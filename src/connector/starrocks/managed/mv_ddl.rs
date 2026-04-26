//! Engine-boundary helpers for CREATE / DROP / SHOW MATERIALIZED VIEW.
//!
//! REFRESH lives in `mv_refresh.rs` because it needs the query executor.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::get_tablet_runtime;
use crate::connector::starrocks::lake::schema::create_lake_tablet_from_req_with_schema_patch;
use crate::formats::starrocks::metadata::load_tablet_snapshot;
use crate::sql::analysis::{OutputColumn, QueryBody, ResolvedQuery};
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, MaterializedViewDistribution, ObjectName,
    ShowMaterializedViewsStmt, SqlType, TableColumnDef, TableKeyDesc, TableKeyKind,
};
use crate::standalone::engine::catalog::normalize_identifier;
use crate::standalone::engine::{record_batch_to_chunk, register_iceberg_tables_for_query};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use prost::Message;

use crate::connector::starrocks::managed::catalog::{
    ManagedLakeCatalog, register_managed_table_in_catalog,
};
use crate::connector::starrocks::managed::ddl::{
    ManagedPhysicalColumn, build_create_tablet_request, build_tablet_schema,
    initialize_global_meta_if_needed, keys_type_name, managed_physical_column,
    patch_tablet_schema_column_flags, reclaim_dropping_table_for_reuse,
    stored_columns_from_physical_columns, table_columns_from_physical_columns,
};
use crate::connector::starrocks::managed::mv_shape::{AggregateMvShape, IncrementalMvShape};
use crate::connector::starrocks::managed::store::{
    IcebergTableRef, ManagedMvRefreshMode, ManagedPartitionState, ManagedTableKind,
    ManagedTableState, ManagedTxnState, StoredManagedIndex, StoredManagedPartition,
    StoredManagedSchema, StoredManagedTable, StoredManagedTablet, StoredManagedTxn,
    StoredMaterializedView,
};
use crate::standalone::engine::{QueryResult, QueryResultColumn, StandaloneState, StatementResult};

/// Resolved base-table reference as the MV analyzer stage returns it.
/// Only the `Iceberg` variant is allowed; anything else fails validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResolvedTableRef {
    Iceberg {
        catalog: String,
        namespace: String,
        table: String,
    },
    ManagedLake {
        database: String,
        table: String,
    },
}

pub(crate) fn extract_base_table_refs(
    resolved: &[ResolvedTableRef],
) -> Result<Vec<IcebergTableRef>, String> {
    let mut out = Vec::new();
    for table_ref in resolved {
        match table_ref {
            ResolvedTableRef::Iceberg {
                catalog,
                namespace,
                table,
            } => {
                let candidate = IcebergTableRef {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                };
                if !out.contains(&candidate) {
                    out.push(candidate);
                }
            }
            ResolvedTableRef::ManagedLake { database, table } => {
                return Err(format!(
                    "materialized view base tables must be Iceberg tables; found managed lake table `{database}.{table}`"
                ));
            }
        }
    }
    if out.is_empty() {
        return Err("materialized view base tables must be Iceberg tables".to_string());
    }
    Ok(out)
}

pub(crate) fn create_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    {
        let catalog = state.catalog.read().expect("standalone catalog read lock");
        if !catalog.database_exists(&db_name)? {
            return Err(format!("unknown database: {db_name}"));
        }
        if catalog.get(&db_name, &mv_name).is_ok() {
            if stmt.if_not_exists {
                return Ok(StatementResult::Ok);
            }
            return Err(format!(
                "materialized view or table already exists: {db_name}.{mv_name}"
            ));
        }
    }

    let managed_config = state
        .managed_lake_config
        .clone()
        .ok_or_else(|| "standalone managed lake config is missing".to_string())?;
    let metadata_store = state.metadata_store.as_ref().ok_or_else(|| {
        "managed lake create materialized view requires sqlite metadata store".to_string()
    })?;

    let analysis = analyze_mv_select(state, current_database, &stmt.select_query)?;
    let base_refs = extract_base_table_refs(&analysis.resolved_refs)?;
    let distribution = stmt
        .distribution
        .as_ref()
        .ok_or_else(|| "CREATE MATERIALIZED VIEW requires DISTRIBUTED BY".to_string())?;
    let bucket_count = distribution.bucket_count.ok_or_else(|| {
        "DISTRIBUTED BY HASH(...) BUCKETS n is required (BUCKETS <n> is mandatory in phase 1)"
            .to_string()
    })?;
    if analysis.output_columns.is_empty() {
        return Err("materialized view SELECT must produce at least one column".to_string());
    }
    let mv_shape = super::mv_shape::classify_incremental_mv_query(&stmt.select_query)?;
    let storage_layout =
        build_mv_storage_layout(&mv_shape, distribution, &analysis.output_columns)?;
    let key_desc = storage_layout.key_desc;
    let physical_columns = storage_layout.physical_columns;

    let mut managed = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    if managed.contains_table(&db_name, &mv_name)? {
        if stmt.if_not_exists {
            return Ok(StatementResult::Ok);
        }
        return Err(format!(
            "materialized view or table already exists: {db_name}.{mv_name}"
        ));
    }

    let mut snapshot = managed.snapshot.clone();
    initialize_global_meta_if_needed(&mut snapshot, &managed_config);
    let database = find_or_create_managed_database(&mut snapshot, &db_name);
    reclaim_dropping_table_for_reuse(&mut snapshot, database.db_id, &mv_name)?;

    let table_id = alloc_id(&mut snapshot.global.next_table_id);
    let schema_id = table_id;
    let partition_id = alloc_id(&mut snapshot.global.next_partition_id);
    let index_id = alloc_id(&mut snapshot.global.next_index_id);
    let bucket_num = i64::from(bucket_count);
    if bucket_num <= 0 {
        return Err("CREATE MATERIALIZED VIEW requires BUCKETS > 0".to_string());
    }

    let table_columns = table_columns_from_physical_columns(&physical_columns);
    let request_schema = build_tablet_schema(&table_columns, &key_desc, schema_id)?;
    let object_store_profile = ObjectStoreProfile::from_s3_store_config(&managed_config.s3)?;
    let mut tablets = Vec::new();
    for bucket_seq in 0..bucket_num {
        let tablet_id = alloc_id(&mut snapshot.global.next_tablet_id);
        let tablet_root_path =
            managed_config.tablet_root_path(database.db_id, table_id, partition_id);
        let request =
            build_create_tablet_request(tablet_id, table_id, partition_id, request_schema.clone());
        create_lake_tablet_from_req_with_schema_patch(
            &request,
            &tablet_root_path,
            Some(managed_config.s3.clone()),
            |schema| patch_tablet_schema_column_flags(schema, &physical_columns),
        )?;
        let runtime_schema = get_tablet_runtime(tablet_id)?.schema;
        let loaded =
            load_tablet_snapshot(tablet_id, 1, &tablet_root_path, Some(&object_store_profile))?;
        if loaded.tablet_schema != runtime_schema {
            return Err(format!(
                "managed tablet schema mismatch after bootstrap: tablet_id={tablet_id}"
            ));
        }
        tablets.push(StoredManagedTablet {
            tablet_id,
            partition_id,
            index_id,
            bucket_seq,
            tablet_root_path,
        });
    }

    snapshot.tables.push(StoredManagedTable {
        table_id,
        db_id: database.db_id,
        name: mv_name.clone(),
        keys_type: keys_type_name(key_desc.kind).to_string(),
        bucket_num,
        current_schema_id: schema_id,
        state: ManagedTableState::Active,
        kind: ManagedTableKind::MaterializedView,
    });
    snapshot.schemas.push(StoredManagedSchema {
        schema_id,
        table_id,
        schema_version: 0,
        tablet_schema_pb: get_tablet_runtime(tablets[0].tablet_id)?
            .schema
            .encode_to_vec(),
    });
    snapshot
        .columns
        .extend(stored_columns_from_physical_columns(
            schema_id,
            &key_desc,
            &physical_columns,
        ));
    snapshot.partitions.push(StoredManagedPartition {
        partition_id,
        table_id,
        name: "p0".to_string(),
        visible_version: 1,
        next_version: 2,
        state: ManagedPartitionState::Active,
    });
    snapshot.indexes.push(StoredManagedIndex {
        index_id,
        table_id,
        partition_id,
        index_type: "BASE".to_string(),
        state: crate::connector::starrocks::managed::store::ManagedIndexState::Active,
    });
    snapshot.tablets.extend(tablets);
    let txn_id = alloc_id(&mut snapshot.global.next_txn_id);
    snapshot.txns.push(StoredManagedTxn {
        txn_id,
        table_id,
        partition_id,
        base_version: 0,
        commit_version: 1,
        state: ManagedTxnState::Visible,
        retry_at_ms: None,
        updated_at_ms: 0,
    });
    snapshot.materialized_views.push(StoredMaterializedView {
        mv_id: table_id,
        select_sql: stmt.select_sql.clone(),
        refresh_mode: ManagedMvRefreshMode::DeferredManual,
        base_table_refs: base_refs,
        last_refresh_ms: None,
        last_refresh_rows: None,
        last_refresh_snapshots: Default::default(),
        created_at_ms: now_ms(),
    });

    let rebuilt = ManagedLakeCatalog::rebuild(Some(managed_config), snapshot.clone())?;
    metadata_store.replace_managed_snapshot(&snapshot)?;
    rebuilt.re_register_active_tablet_runtimes()?;
    let runtime = rebuilt.table(&db_name, &mv_name)?.clone();
    *managed = rebuilt;
    drop(managed);

    let mut catalog = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    register_managed_table_in_catalog(&mut catalog, &runtime)?;
    Ok(StatementResult::Ok)
}

#[derive(Clone, Debug)]
struct MvStorageLayout {
    key_desc: TableKeyDesc,
    physical_columns: Vec<ManagedPhysicalColumn>,
}

fn build_mv_storage_layout(
    mv_shape: &IncrementalMvShape,
    distribution: &MaterializedViewDistribution,
    output_columns: &[OutputColumn],
) -> Result<MvStorageLayout, String> {
    match mv_shape {
        IncrementalMvShape::ProjectionFilter(_) => {
            validate_distribution_columns(distribution, output_columns)?;
            let table_columns = output_columns
                .iter()
                .map(output_column_to_table_column)
                .collect::<Result<Vec<_>, _>>()?;
            let key_desc = TableKeyDesc {
                kind: TableKeyKind::Duplicate,
                columns: distribution.hash_columns.clone(),
            };
            let key_column_set = key_desc
                .columns
                .iter()
                .map(|column| normalize_identifier(column))
                .collect::<Result<HashSet<_>, _>>()?;
            let physical_columns = table_columns
                .iter()
                .map(|column| {
                    let column_name = normalize_identifier(&column.name)?;
                    Ok(managed_physical_column(
                        column.name.clone(),
                        column.data_type.clone(),
                        column.nullable,
                        true,
                        key_column_set.contains(&column_name),
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(MvStorageLayout {
                key_desc,
                physical_columns,
            })
        }
        IncrementalMvShape::Aggregate(shape) => {
            validate_aggregate_distribution_columns(distribution, shape)?;
            let layout = super::mv_agg_state::build_aggregate_mv_layout(shape, output_columns)?;
            validate_unique_aggregate_physical_column_names(&layout.physical_columns)?;
            Ok(MvStorageLayout {
                key_desc: TableKeyDesc {
                    kind: TableKeyKind::Primary,
                    columns: vec![super::mv_agg_state::ROW_ID_COLUMN.to_string()],
                },
                physical_columns: layout.physical_columns,
            })
        }
    }
}

fn validate_unique_aggregate_physical_column_names(
    physical_columns: &[ManagedPhysicalColumn],
) -> Result<(), String> {
    let mut names = HashSet::with_capacity(physical_columns.len());
    for column in physical_columns {
        let normalized = normalize_identifier(&column.column.name)?;
        if !names.insert(normalized.clone()) {
            return Err(format!(
                "aggregate MV physical column name collision: hidden column name collision or duplicate physical column `{normalized}`"
            ));
        }
    }
    Ok(())
}

pub(crate) fn drop_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let runtime = {
        let managed = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock");
        managed.table(&db_name, &mv_name).ok().cloned()
    };
    let Some(runtime) = runtime else {
        if stmt.if_exists {
            return Ok(StatementResult::Ok);
        }
        if state
            .catalog
            .read()
            .expect("standalone catalog read lock")
            .get(&db_name, &mv_name)
            .is_ok()
        {
            return Err(format!(
                "`{db_name}.{mv_name}` is not a materialized view; use DROP TABLE instead"
            ));
        }
        return Err(format!(
            "materialized view does not exist: {db_name}.{mv_name}"
        ));
    };
    if runtime.table.kind != ManagedTableKind::MaterializedView {
        return Err(format!(
            "`{db_name}.{mv_name}` is not a materialized view; use DROP TABLE instead"
        ));
    }
    crate::connector::starrocks::managed::ddl::drop_managed_table(state, &db_name, &mv_name)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn list_mvs(
    state: &Arc<StandaloneState>,
    stmt: &ShowMaterializedViewsStmt,
) -> Result<StatementResult, String> {
    let metadata_store = state.metadata_store.as_ref().ok_or_else(|| {
        "managed lake show materialized views requires sqlite metadata store".to_string()
    })?;
    let snapshot = metadata_store.load_snapshot()?.managed;

    let mut rows = Vec::new();
    for mv in &snapshot.materialized_views {
        let Some(table) = snapshot.tables.iter().find(|table| {
            table.table_id == mv.mv_id && table.kind == ManagedTableKind::MaterializedView
        }) else {
            continue;
        };
        if table.state != ManagedTableState::Active {
            continue;
        }
        let Some(database) = snapshot
            .databases
            .iter()
            .find(|database| database.db_id == table.db_id)
            .map(|database| database.name.clone())
        else {
            continue;
        };
        if let Some(filter_db) = stmt.database.as_deref()
            && !database.eq_ignore_ascii_case(filter_db)
        {
            continue;
        }
        rows.push(ShowMvRow {
            name: table.name.clone(),
            database,
            refresh_mode: mv.refresh_mode.as_sql_str().to_string(),
            last_refresh_time: mv.last_refresh_ms.map(|value| value.to_string()),
            last_refresh_rows: mv.last_refresh_rows.map(|value| value.to_string()),
            base_tables: mv
                .base_table_refs
                .iter()
                .map(IcebergTableRef::fqn)
                .collect::<Vec<_>>()
                .join(", "),
            select_text: mv.select_sql.clone(),
        });
    }
    rows.sort_by(|left, right| {
        left.database
            .cmp(&right.database)
            .then(left.name.cmp(&right.name))
    });

    Ok(StatementResult::Query(build_mv_rows_result(&rows)?))
}

#[derive(Clone, Debug)]
struct MvAnalysis {
    resolved_refs: Vec<ResolvedTableRef>,
    output_columns: Vec<OutputColumn>,
}

fn analyze_mv_select(
    state: &Arc<StandaloneState>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<MvAnalysis, String> {
    let resolved_refs = collect_table_refs_from_query(query, current_database);
    let mut analyzed_query = query.clone();
    register_iceberg_tables_for_query(state, None, current_database, &analyzed_query)?;
    if has_three_part_refs(&resolved_refs) {
        crate::standalone::engine::sqlparse::statement::strip_catalog_from_three_part_names(
            &mut analyzed_query,
        );
    }
    let catalog = state.catalog.read().expect("standalone catalog read lock");
    let (resolved, _) =
        crate::sql::analyzer::analyze(&analyzed_query, &*catalog, current_database)?;
    drop(catalog);

    let mut output_columns = resolved.output_columns.clone();
    if output_columns.is_empty() {
        output_columns = resolved_output_columns_from_body(&resolved);
    }

    Ok(MvAnalysis {
        resolved_refs,
        output_columns,
    })
}

fn resolved_output_columns_from_body(resolved: &ResolvedQuery) -> Vec<OutputColumn> {
    match &resolved.body {
        QueryBody::Select(select) => select
            .projection
            .iter()
            .map(|item| OutputColumn {
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
            })
            .collect(),
        _ => resolved.output_columns.clone(),
    }
}

fn validate_distribution_columns(
    distribution: &MaterializedViewDistribution,
    output_columns: &[OutputColumn],
) -> Result<(), String> {
    for column in &distribution.hash_columns {
        let exists = output_columns
            .iter()
            .any(|output| output.name.eq_ignore_ascii_case(column));
        if !exists {
            return Err(format!(
                "DISTRIBUTED BY column `{column}` not in MV output schema"
            ));
        }
    }
    Ok(())
}

fn validate_aggregate_distribution_columns(
    distribution: &MaterializedViewDistribution,
    shape: &AggregateMvShape,
) -> Result<(), String> {
    let group_key_outputs = shape
        .group_keys
        .iter()
        .map(|group_key| normalize_identifier(&group_key.output_name))
        .collect::<Result<HashSet<_>, _>>()?;
    for column in &distribution.hash_columns {
        let normalized = normalize_identifier(column)?;
        if !group_key_outputs.contains(&normalized) {
            return Err(format!(
                "aggregate MV distribution column `{column}` must be a GROUP BY key output column; DISTRIBUTED BY HASH for aggregate MV can only reference GROUP BY keys"
            ));
        }
    }
    Ok(())
}

fn resolve_mv_name(name: &ObjectName, current_database: &str) -> Result<(String, String), String> {
    match name.parts.as_slice() {
        [table] => Ok((
            normalize_identifier(current_database)?,
            normalize_identifier(table)?,
        )),
        [database, table] => Ok((
            normalize_identifier(database)?,
            normalize_identifier(table)?,
        )),
        _ => Err(format!(
            "materialized view name must be `<name>` or `<db>.<name>`; got `{}`",
            name.parts.join(".")
        )),
    }
}

fn collect_table_refs_from_query(
    query: &sqlparser::ast::Query,
    current_database: &str,
) -> Vec<ResolvedTableRef> {
    let mut refs = Vec::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_table_refs_from_set_expr(cte.query.body.as_ref(), current_database, &mut refs);
        }
    }
    collect_table_refs_from_set_expr(query.body.as_ref(), current_database, &mut refs);
    refs
}

fn collect_table_refs_from_set_expr(
    expr: &sqlparser::ast::SetExpr,
    current_database: &str,
    refs: &mut Vec<ResolvedTableRef>,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &select.from {
                collect_table_refs_from_factor(&from.relation, current_database, refs);
                for join in &from.joins {
                    collect_table_refs_from_factor(&join.relation, current_database, refs);
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            collect_table_refs_from_set_expr(left, current_database, refs);
            collect_table_refs_from_set_expr(right, current_database, refs);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            collect_table_refs_from_set_expr(query.body.as_ref(), current_database, refs);
        }
        _ => {}
    }
}

fn collect_table_refs_from_factor(
    factor: &sqlparser::ast::TableFactor,
    current_database: &str,
    refs: &mut Vec<ResolvedTableRef>,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect();
            let resolved = match parts.as_slice() {
                [catalog, namespace, table] => ResolvedTableRef::Iceberg {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                },
                [table] => ResolvedTableRef::ManagedLake {
                    database: current_database.to_ascii_lowercase(),
                    table: table.clone(),
                },
                [database, table] => ResolvedTableRef::ManagedLake {
                    database: database.clone(),
                    table: table.clone(),
                },
                _ => {
                    let rendered = parts.join(".");
                    ResolvedTableRef::ManagedLake {
                        database: current_database.to_ascii_lowercase(),
                        table: rendered,
                    }
                }
            };
            if !refs.contains(&resolved) {
                refs.push(resolved);
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            if let Some(with) = &subquery.with {
                for cte in &with.cte_tables {
                    collect_table_refs_from_set_expr(
                        cte.query.body.as_ref(),
                        current_database,
                        refs,
                    );
                }
            }
            collect_table_refs_from_set_expr(subquery.body.as_ref(), current_database, refs);
        }
        _ => {}
    }
}

fn has_three_part_refs(resolved_refs: &[ResolvedTableRef]) -> bool {
    resolved_refs
        .iter()
        .any(|table_ref| matches!(table_ref, ResolvedTableRef::Iceberg { .. }))
}

fn output_column_to_table_column(column: &OutputColumn) -> Result<TableColumnDef, String> {
    Ok(TableColumnDef {
        name: column.name.clone(),
        data_type: arrow_data_type_to_sql_type(&column.data_type)?,
        nullable: column.nullable,
        aggregation: None,
    })
}

pub(crate) fn arrow_data_type_to_sql_type(data_type: &DataType) -> Result<SqlType, String> {
    match data_type {
        DataType::Boolean => Ok(SqlType::Boolean),
        DataType::Int8 => Ok(SqlType::TinyInt),
        DataType::Int16 => Ok(SqlType::SmallInt),
        DataType::Int32 => Ok(SqlType::Int),
        DataType::Int64 => Ok(SqlType::BigInt),
        DataType::Float32 => Ok(SqlType::Float),
        DataType::Float64 => Ok(SqlType::Double),
        DataType::Utf8 => Ok(SqlType::String),
        DataType::Binary => Ok(SqlType::Binary),
        DataType::Date32 => Ok(SqlType::Date),
        DataType::Timestamp(_, _) => Ok(SqlType::DateTime),
        DataType::Time64(_) => Ok(SqlType::Time),
        DataType::FixedSizeBinary(width)
            if *width == crate::common::largeint::LARGEINT_BYTE_WIDTH =>
        {
            Ok(SqlType::LargeInt)
        }
        DataType::Decimal128(precision, scale) => Ok(SqlType::Decimal {
            precision: *precision,
            scale: *scale,
        }),
        DataType::List(field) => Ok(SqlType::Array(Box::new(arrow_data_type_to_sql_type(
            field.data_type(),
        )?))),
        DataType::Struct(fields) => Ok(SqlType::Struct(
            fields
                .iter()
                .map(|field| {
                    Ok((
                        field.name().clone(),
                        arrow_data_type_to_sql_type(field.data_type())?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err("MAP output type must use struct entries".to_string());
            };
            let (_, key) = fields
                .find("key")
                .ok_or_else(|| "MAP output type is missing key field".to_string())?;
            let (_, value) = fields
                .find("value")
                .ok_or_else(|| "MAP output type is missing value field".to_string())?;
            Ok(SqlType::Map(
                Box::new(arrow_data_type_to_sql_type(key.data_type())?),
                Box::new(arrow_data_type_to_sql_type(value.data_type())?),
            ))
        }
        other => Err(format!("unsupported MV output type: {other}")),
    }
}

#[derive(Clone, Debug)]
struct ShowMvRow {
    name: String,
    database: String,
    refresh_mode: String,
    last_refresh_time: Option<String>,
    last_refresh_rows: Option<String>,
    base_tables: String,
    select_text: String,
}

fn build_mv_rows_result(rows: &[ShowMvRow]) -> Result<QueryResult, String> {
    let columns = vec![
        QueryResultColumn {
            name: "Name".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "Database".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RefreshMode".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "LastRefreshTime".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "LastRefreshRows".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "BaseTables".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "SelectText".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("Name", DataType::Utf8, false),
        Field::new("Database", DataType::Utf8, false),
        Field::new("RefreshMode", DataType::Utf8, false),
        Field::new("LastRefreshTime", DataType::Utf8, true),
        Field::new("LastRefreshRows", DataType::Utf8, true),
        Field::new("BaseTables", DataType::Utf8, false),
        Field::new("SelectText", DataType::Utf8, false),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.name.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.database.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.refresh_mode.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.last_refresh_time.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.last_refresh_rows.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.base_tables.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.select_text.clone()))
                .collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("build SHOW MATERIALIZED VIEWS batch failed: {e}"))?;
    Ok(QueryResult {
        columns,
        chunks: vec![record_batch_to_chunk(batch)?],
    })
}

fn alloc_id(next_id: &mut i64) -> i64 {
    if *next_id <= 0 {
        *next_id = 1;
    }
    let id = *next_id;
    *next_id += 1;
    id
}

fn find_or_create_managed_database(
    snapshot: &mut crate::connector::starrocks::managed::store::ManagedSnapshot,
    database_name: &str,
) -> crate::connector::starrocks::managed::store::StoredManagedDatabase {
    if let Some(found) = snapshot
        .databases
        .iter()
        .find(|database| database.name == database_name)
        .cloned()
    {
        return found;
    }
    let database = crate::connector::starrocks::managed::store::StoredManagedDatabase {
        db_id: alloc_id(&mut snapshot.global.next_db_id),
        name: database_name.to_string(),
    };
    snapshot.databases.push(database.clone());
    database
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::managed::catalog::ManagedTableRuntime;
    use crate::standalone::engine::catalog::InMemoryCatalog;

    fn parse_create_mv(sql: &str) -> crate::sql::parser::ast::CreateMaterializedViewStmt {
        let stmt = crate::sql::parser::parse_sql(sql).expect("parse").remove(0);
        let crate::sql::parser::ast::Statement::CreateMaterializedView(stmt) = stmt else {
            panic!("not create mv");
        };
        stmt
    }

    #[test]
    fn extract_base_table_refs_rejects_non_iceberg_tables() {
        let err = extract_base_table_refs(&[ResolvedTableRef::ManagedLake {
            database: "analytics".to_string(),
            table: "orders_raw".to_string(),
        }])
        .expect_err("should reject non-iceberg");
        assert!(err.contains("Iceberg"), "err={err}");
    }

    #[test]
    fn extract_base_table_refs_returns_iceberg_fqns() {
        let refs = extract_base_table_refs(&[
            ResolvedTableRef::Iceberg {
                catalog: "iceberg_cat".to_string(),
                namespace: "ns".to_string(),
                table: "orders".to_string(),
            },
            ResolvedTableRef::Iceberg {
                catalog: "iceberg_cat".to_string(),
                namespace: "ns".to_string(),
                table: "items".to_string(),
            },
        ])
        .expect("ok");
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].fqn(), "iceberg_cat.ns.orders");
    }

    #[test]
    fn create_mv_shape_accepts_projection_filter() {
        let stmt = parse_create_mv(
            "create materialized view mv1 distributed by hash(k1) buckets 2 \
             as select k1, v2 from ice.ns.orders where v2 > 10",
        );
        super::super::mv_shape::classify_incremental_mv_query(&stmt.select_query)
            .expect("shape ok");
    }

    #[test]
    fn create_mv_shape_rejects_unsupported_aggregation() {
        let stmt = parse_create_mv(
            "create materialized view mv1 distributed by hash(k1) buckets 2 \
             as select k1, avg(v2) from ice.ns.orders group by k1",
        );
        let err = super::super::mv_shape::classify_incremental_mv_query(&stmt.select_query)
            .expect_err("agg rejected");
        assert!(err.contains("incremental aggregate MV"), "err={err}");
    }

    #[test]
    fn aggregate_mv_physical_schema_has_hidden_row_id_and_state_columns() {
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW analytics.orders_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
AS SELECT k1, count(*) AS c, sum(v2) AS s
FROM ice.ns.orders
GROUP BY k1",
        );
        let mv_shape = super::super::mv_shape::classify_incremental_mv_query(&stmt.select_query)
            .expect("aggregate shape");
        let output_columns = vec![
            OutputColumn {
                name: "k1".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            OutputColumn {
                name: "c".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                name: "s".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
        ];
        let distribution = stmt.distribution.as_ref().expect("distribution");
        let storage_layout =
            build_mv_storage_layout(&mv_shape, distribution, &output_columns).expect("layout");

        assert_eq!(storage_layout.key_desc.kind, TableKeyKind::Primary);
        assert_eq!(
            storage_layout.key_desc.columns,
            vec![super::super::mv_agg_state::ROW_ID_COLUMN.to_string()]
        );
        let table_columns = table_columns_from_physical_columns(&storage_layout.physical_columns);
        let request_schema = build_tablet_schema(&table_columns, &storage_layout.key_desc, 10)
            .expect("request schema");
        assert_eq!(
            request_schema.keys_type,
            crate::types::TKeysType::PRIMARY_KEYS
        );
        let mut tablet_schema =
            crate::connector::starrocks::lake::schema::build_tablet_schema_pb_from_thrift(
                &request_schema,
            )
            .expect("tablet schema pb");
        patch_tablet_schema_column_flags(&mut tablet_schema, &storage_layout.physical_columns)
            .expect("patch flags");
        let stored_columns = stored_columns_from_physical_columns(
            10,
            &storage_layout.key_desc,
            &storage_layout.physical_columns,
        );

        let runtime = ManagedTableRuntime {
            database_name: "analytics".to_string(),
            table: StoredManagedTable {
                table_id: 10,
                db_id: 1,
                name: "orders_mv".to_string(),
                keys_type: keys_type_name(storage_layout.key_desc.kind).to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::MaterializedView,
            },
            tablet_schema,
            columns: stored_columns,
            partitions: Vec::new(),
            indexes: Vec::new(),
            tablets: Vec::new(),
        };
        assert_eq!(runtime.table.keys_type, "PRIMARY_KEYS");
        assert_eq!(
            runtime.tablet_schema.column[0].name.as_deref(),
            Some(super::super::mv_agg_state::ROW_ID_COLUMN)
        );
        assert_eq!(runtime.tablet_schema.column[0].is_key, Some(true));
        assert_eq!(runtime.tablet_schema.column[0].visible, Some(false));
        let state_column = runtime
            .tablet_schema
            .column
            .iter()
            .find(|column| column.name.as_deref() == Some("__agg_state_c"))
            .expect("count state column");
        assert_eq!(state_column.visible, Some(false));

        let mut catalog = InMemoryCatalog::default();
        catalog
            .create_database("analytics")
            .expect("create database");
        register_managed_table_in_catalog(&mut catalog, &runtime).expect("register mv");
        let public_table = catalog.get("analytics", "orders_mv").expect("public table");
        let public_column_names = public_table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(public_column_names, vec!["k1", "c", "s"]);
        assert!(!public_column_names.contains(&super::super::mv_agg_state::ROW_ID_COLUMN));
        assert!(!public_column_names.contains(&"__agg_state_c"));
    }

    #[test]
    fn aggregate_mv_distribution_rejects_non_group_key_output() {
        let stmt = parse_create_mv(
            "create materialized view analytics.orders_mv distributed by hash(c) buckets 2 \
             as select k1, count(*) as c from ice.ns.orders group by k1",
        );
        let mv_shape = super::super::mv_shape::classify_incremental_mv_query(&stmt.select_query)
            .expect("aggregate shape");
        let IncrementalMvShape::Aggregate(shape) = mv_shape else {
            panic!("expected aggregate shape");
        };
        let err = validate_aggregate_distribution_columns(
            stmt.distribution.as_ref().expect("distribution"),
            &shape,
        )
        .expect_err("non-group key distribution should fail");
        assert!(err.contains("aggregate MV distribution"), "err={err}");
        assert!(err.contains("GROUP BY key"), "err={err}");
    }

    #[test]
    fn aggregate_mv_physical_schema_rejects_hidden_name_collision() {
        let stmt = parse_create_mv(
            "create materialized view analytics.orders_mv distributed by hash(__agg_state_c) buckets 2 \
             as select k1 as __agg_state_c, count(*) as c from ice.ns.orders group by k1",
        );
        let mv_shape = super::super::mv_shape::classify_incremental_mv_query(&stmt.select_query)
            .expect("aggregate shape");
        let output_columns = vec![
            OutputColumn {
                name: "__agg_state_c".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            OutputColumn {
                name: "c".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let err = build_mv_storage_layout(
            &mv_shape,
            stmt.distribution.as_ref().expect("distribution"),
            &output_columns,
        )
        .expect_err("hidden physical column collision should fail");
        assert!(
            err.contains("aggregate MV physical column name collision"),
            "err={err}"
        );
        assert!(err.contains("hidden column name collision"), "err={err}");
    }
}
