//! Engine-boundary helpers for CREATE / DROP / SHOW MATERIALIZED VIEW.
//!
//! REFRESH lives in `mv_refresh.rs` because it needs the query executor.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::{get_tablet_runtime, remove_tablet_runtime};
use crate::connector::starrocks::lake::schema::create_lake_tablet_from_req_with_schema_patch;
use crate::connector::starrocks::lake::transactions::delete_tablet;
use crate::engine::catalog::normalize_identifier;
use crate::engine::query_prep::drop_registered_external_table;
use crate::engine::record_batch_to_chunk;
use crate::formats::starrocks::metadata::load_tablet_snapshot;
use crate::meta::repository::managed_lake::{
    CreateManagedColumnRequest, CreateManagedTableLayoutRequest,
    ManagedTableKind as RepoManagedTableKind,
};
use crate::meta::repository::mv::CreateMvDefinitionRequest;
use crate::service::grpc_client::proto::starrocks::DeleteTabletRequest;
use crate::sql::analysis::{ExprKind, OutputColumn, QueryBody, ResolvedQuery};
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, MaterializedViewDistribution, ObjectName,
    ShowMaterializedViewsStmt, SqlType, TableColumnDef, TableKeyDesc, TableKeyKind,
};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use prost::Message;

use crate::connector::starrocks::managed::catalog::{
    ManagedLakeCatalog, register_managed_table_in_catalog,
};
use crate::connector::starrocks::managed::ddl::{
    ManagedPhysicalColumn, build_create_tablet_request, build_tablet_schema, keys_type_name,
    managed_physical_column, patch_tablet_schema_column_flags,
    stored_columns_from_physical_columns, table_columns_from_physical_columns,
};
use crate::connector::starrocks::managed::model::{
    IcebergTableRef, ManagedMvRefreshMode, ManagedMvStorageEngine, ManagedTableKind,
    ManagedTableState,
};
use crate::connector::starrocks::managed::mv_shape::{
    AggregateFunctionKind, AggregateMvShape, IncrementalMvShape, VisibleAggregateOutput,
};
use crate::engine::mv::lifecycle::{MvListRow, MvStorageEngine};
use crate::engine::{QueryResult, QueryResultColumn, StandaloneState, StatementResult};

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

pub(crate) fn resolve_mv_storage_engine(
    properties: &[(String, String)],
    default_from_config: &str,
) -> Result<ManagedMvStorageEngine, String> {
    let property = properties
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("storage_engine"))
        .map(|(_, v)| v.as_str());
    let raw = property.unwrap_or(default_from_config);
    ManagedMvStorageEngine::parse_sql_str(raw)
}

pub(crate) fn create_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    let default_engine = state
        .managed_lake_config
        .as_ref()
        .map(|c| c.mv_default_storage_engine.as_str())
        .unwrap_or("managed_lake");
    let storage_engine = resolve_mv_storage_engine(&stmt.properties, default_engine)?;
    {
        let catalog = state.catalog.read().expect("standalone catalog read lock");
        let database_exists = catalog.database_exists(&db_name)?;
        if !database_exists && storage_engine != ManagedMvStorageEngine::Iceberg {
            return Err(format!("unknown database: {db_name}"));
        }
        if database_exists
            && storage_engine != ManagedMvStorageEngine::Iceberg
            && catalog.get(&db_name, &mv_name).is_ok()
        {
            if stmt.if_not_exists {
                return Ok(StatementResult::Ok);
            }
            return Err(format!(
                "materialized view or table already exists: {db_name}.{mv_name}"
            ));
        }
    }

    if storage_engine == ManagedMvStorageEngine::Iceberg {
        return Err(
            "managed-lake MV backend cannot create storage_engine='iceberg' materialized views"
                .to_string(),
        );
    }

    let managed_config = state
        .managed_lake_config
        .clone()
        .ok_or_else(|| "standalone managed lake config is missing".to_string())?;
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "managed lake create materialized view requires metadata provider".to_string()
    })?;

    let analysis = analyze_mv_select(state, current_catalog, current_database, &stmt.select_query)?;
    validate_mv_partition_columns(stmt.partition_by.as_deref(), &analysis.output_columns)?;
    let base_refs = extract_base_table_refs(&analysis.resolved_refs)?;

    // IVM Phase-2 PRIMARY KEY validation. Only runs when the user opted in
    // by writing `PRIMARY KEY (...)` in the DDL; otherwise behavior is
    // unchanged.
    let primary_key_base_descriptor = if let Some(pk_cols) = stmt.primary_key.as_deref() {
        if base_refs.len() != 1 {
            return Err(
                "PRIMARY KEY on materialized view requires exactly one iceberg base table"
                    .to_string(),
            );
        }
        let base_ref = &base_refs[0];
        let loaded =
            crate::connector::starrocks::managed::mv_refresh::load_current_iceberg_base_table(
                state, base_ref,
            )?;
        let descriptor = descriptor_from_loaded(&loaded);
        validate_ivm_primary_key(pk_cols, &descriptor).map_err(|e| e.to_string())?;
        Some(descriptor)
    } else {
        None
    };

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
    validate_incremental_mv_analyzed_types(&mv_shape, &analysis.resolved_query)?;
    let storage_layout = build_mv_storage_layout(
        &mv_shape,
        distribution,
        &analysis.output_columns,
        stmt.primary_key.as_deref().unwrap_or(&[]),
        primary_key_base_descriptor.as_ref(),
    )?;
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

    let bucket_num = i64::from(bucket_count);
    if bucket_num <= 0 {
        return Err("CREATE MATERIALIZED VIEW requires BUCKETS > 0".to_string());
    }

    let table_columns = table_columns_from_physical_columns(&physical_columns);
    let stored_columns = stored_columns_from_physical_columns(0, &key_desc, &physical_columns)
        .into_iter()
        .map(|column| CreateManagedColumnRequest {
            column_name: column.column_name,
            logical_type: column.logical_type,
            nullable: column.nullable,
            visible: column.visible,
            is_key: column.is_key,
        })
        .collect::<Vec<_>>();

    let mut txn = provider
        .begin_write("create managed lake materialized view")
        .map_err(|e| format!("open managed materialized view create transaction failed: {e}"))?;
    let database = state
        .managed_repo
        .get_or_create_database(txn.as_mut(), &db_name)
        .map_err(|e| format!("create managed database metadata failed: {e}"))?;
    let reclaimed = state
        .managed_repo
        .purge_dropping_table_for_reuse(txn.as_mut(), database.db_id, &mv_name)
        .map_err(|e| format!("reclaim dropping managed table metadata failed: {e}"))?;
    for table_id in &reclaimed {
        state
            .managed_txn_repo
            .delete_for_table(txn.as_mut(), *table_id)
            .map_err(|e| format!("delete reclaimed managed txns failed: {e}"))?;
        state
            .job_repo
            .delete_for_table(txn.as_mut(), *table_id)
            .map_err(|e| format!("delete reclaimed erase jobs failed: {e}"))?;
        state
            .mv_repo
            .drop_by_id(txn.as_mut(), *table_id)
            .map_err(|e| format!("delete reclaimed materialized view definition failed: {e}"))?;
    }

    let created = state
        .managed_repo
        .create_table_layout(
            txn.as_mut(),
            CreateManagedTableLayoutRequest {
                db_id: database.db_id,
                table_name: mv_name.clone(),
                keys_type: keys_type_name(key_desc.kind).to_string(),
                bucket_num,
                kind: RepoManagedTableKind::MaterializedView,
                schema_version: 0,
                tablet_schema_pb: Vec::new(),
                columns: stored_columns,
                partition_name: "p0".to_string(),
                warehouse_uri: managed_config.warehouse_uri.clone(),
            },
        )
        .map_err(|e| format!("create managed materialized view metadata failed: {e}"))?;
    let request_schema = build_tablet_schema(&table_columns, &key_desc, created.schema.schema_id)?;
    let mut tablet_schema_pb =
        crate::connector::starrocks::lake::schema::build_tablet_schema_pb_from_thrift(
            &request_schema,
        )?;
    patch_tablet_schema_column_flags(&mut tablet_schema_pb, &physical_columns)?;
    state
        .managed_repo
        .update_schema_payload(
            txn.as_mut(),
            created.schema.schema_id,
            tablet_schema_pb.encode_to_vec(),
        )
        .map_err(|e| format!("update managed materialized view schema metadata failed: {e}"))?;
    state
        .managed_txn_repo
        .record_visible_bootstrap(
            txn.as_mut(),
            created.table.table_id,
            created.partition.partition_id,
        )
        .map_err(|e| {
            format!("create managed materialized view bootstrap txn metadata failed: {e}")
        })?;
    let created_at_ms = now_ms();
    state
        .mv_repo
        .create_definition_with_id(
            txn.as_mut(),
            created.table.table_id,
            CreateMvDefinitionRequest {
                select_sql: stmt.select_sql.clone(),
                base_table_refs: iceberg_table_ref_fqns(&base_refs),
                primary_key_columns: stmt.primary_key.clone().unwrap_or_default(),
                storage_engine: ManagedMvStorageEngine::ManagedLake.as_sql_str().to_string(),
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                schema_contract: None,
                created_at_ms,
            },
        )
        .map_err(|e| format!("persist materialized view definition failed: {e}"))?;

    let object_store_profile = ObjectStoreProfile::from_s3_store_config(&managed_config.s3)?;
    let mut bootstrapped_tablet_ids = Vec::new();
    for tablet in &created.tablets {
        let request = build_create_tablet_request(
            tablet.tablet_id,
            created.table.table_id,
            created.partition.partition_id,
            request_schema.clone(),
        );
        if let Err(err) = create_lake_tablet_from_req_with_schema_patch(
            &request,
            &tablet.tablet_root_path,
            Some(managed_config.s3.clone()),
            |schema| patch_tablet_schema_column_flags(schema, &physical_columns),
        ) {
            cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
            let _ = txn.abort();
            return Err(err);
        }
        bootstrapped_tablet_ids.push(tablet.tablet_id);
        let runtime_schema = match get_tablet_runtime(tablet.tablet_id) {
            Ok(runtime) => runtime.schema,
            Err(err) => {
                cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
                let _ = txn.abort();
                return Err(err);
            }
        };
        let loaded = match load_tablet_snapshot(
            tablet.tablet_id,
            1,
            &tablet.tablet_root_path,
            Some(&object_store_profile),
        ) {
            Ok(loaded) => loaded,
            Err(err) => {
                cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
                let _ = txn.abort();
                return Err(err);
            }
        };
        if loaded.tablet_schema != runtime_schema {
            cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
            let _ = txn.abort();
            return Err(format!(
                "managed tablet schema mismatch after bootstrap: tablet_id={}",
                tablet.tablet_id
            ));
        }
    }
    if let Err(err) = txn.commit() {
        cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
        return Err(format!(
            "commit managed materialized view metadata failed: {err}"
        ));
    }

    let read = provider
        .begin_read()
        .map_err(|e| format!("open managed materialized view reload transaction failed: {e}"))?;
    let snapshot = state
        .managed_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("reload managed materialized view metadata failed: {e}"))?;
    let rebuilt = ManagedLakeCatalog::rebuild_from_repository(Some(managed_config), snapshot)?;
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
    primary_key_columns: &[String],
    base_descriptor: Option<&BaseTableDescriptor>,
) -> Result<MvStorageLayout, String> {
    match mv_shape {
        IncrementalMvShape::ProjectionFilter(_) => {
            validate_distribution_columns(distribution, output_columns)?;
            let visible_columns = output_columns
                .iter()
                .map(output_column_to_table_column)
                .collect::<Result<Vec<_>, _>>()?;
            let key_columns = if primary_key_columns.is_empty() {
                distribution.hash_columns.clone()
            } else {
                projection_mv_key_columns(primary_key_columns)?
            };
            let key_desc = TableKeyDesc {
                kind: if primary_key_columns.is_empty() {
                    TableKeyKind::Duplicate
                } else {
                    TableKeyKind::Primary
                },
                columns: key_columns,
            };
            let mut physical_columns = Vec::with_capacity(
                primary_key_columns
                    .len()
                    .saturating_add(visible_columns.len()),
            );
            if !primary_key_columns.is_empty() {
                let base = base_descriptor.ok_or_else(|| {
                    "projection/filter materialized view PRIMARY KEY layout requires base table descriptor"
                        .to_string()
                })?;
                physical_columns.extend(projection_mv_hidden_primary_key_columns(
                    output_columns,
                    primary_key_columns,
                    base,
                )?);
            }
            physical_columns.extend(visible_columns.iter().map(|column| {
                managed_physical_column(
                    column.name.clone(),
                    column.data_type.clone(),
                    column.nullable,
                    true,
                    false,
                )
            }));
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

pub(crate) fn projection_mv_hidden_pk_column_name(key: &str) -> Result<String, String> {
    Ok(format!("__mv_pk_{}", normalize_identifier(key)?))
}

fn projection_mv_key_columns(primary_key_columns: &[String]) -> Result<Vec<String>, String> {
    primary_key_columns
        .iter()
        .map(|key| projection_mv_hidden_pk_column_name(key))
        .collect()
}

fn projection_mv_hidden_primary_key_columns(
    output_columns: &[OutputColumn],
    primary_key_columns: &[String],
    base: &BaseTableDescriptor,
) -> Result<Vec<ManagedPhysicalColumn>, String> {
    let output_names = output_columns
        .iter()
        .map(|column| normalize_identifier(&column.name))
        .collect::<Result<HashSet<_>, _>>()?;
    let mut out = Vec::with_capacity(primary_key_columns.len());
    for key in primary_key_columns {
        let hidden_name = projection_mv_hidden_pk_column_name(key)?;
        if output_names.contains(&normalize_identifier(&hidden_name)?) {
            return Err(format!(
                "projection/filter materialized view hidden PRIMARY KEY column `{hidden_name}` collides with SELECT output"
            ));
        }
        let base_col = base
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(key))
            .ok_or_else(|| {
                format!(
                    "projection/filter materialized view PRIMARY KEY column `{key}` does not exist on the iceberg base table"
                )
            })?;
        out.push(managed_physical_column(
            hidden_name,
            arrow_data_type_to_sql_type(&base_col.data_type)?,
            base_col.nullable,
            false,
            true,
        ));
    }
    Ok(out)
}

fn validate_incremental_mv_analyzed_types(
    mv_shape: &IncrementalMvShape,
    resolved: &ResolvedQuery,
) -> Result<(), String> {
    match mv_shape {
        IncrementalMvShape::ProjectionFilter(_) => Ok(()),
        IncrementalMvShape::Aggregate(shape) => {
            validate_aggregate_mv_analyzed_types(shape, resolved)
        }
    }
}

fn validate_aggregate_mv_analyzed_types(
    shape: &AggregateMvShape,
    resolved: &ResolvedQuery,
) -> Result<(), String> {
    let QueryBody::Select(select) = &resolved.body else {
        return Err("incremental aggregate MV analyzer result must be SELECT".to_string());
    };
    if select.projection.len() != shape.visible_outputs.len() {
        return Err(format!(
            "aggregate MV analyzer projection count mismatch: analyzed_projection={} shape_outputs={}",
            select.projection.len(),
            shape.visible_outputs.len()
        ));
    }

    for (projection_index, visible_output) in shape.visible_outputs.iter().enumerate() {
        let VisibleAggregateOutput::Aggregate(aggregate_index) = visible_output else {
            continue;
        };
        let aggregate = shape.aggregates.get(*aggregate_index).ok_or_else(|| {
            format!("aggregate MV aggregate index out of range: aggregate_index={aggregate_index}")
        })?;
        let projection = &select.projection[projection_index];
        let ExprKind::AggregateCall { name, args, .. } = &projection.expr.kind else {
            return Err(format!(
                "aggregate MV analyzed projection `{}` is not an aggregate expression",
                projection.output_name
            ));
        };
        validate_aggregate_mv_input_type(aggregate.function, name, &aggregate.output_name, args)?;
    }

    Ok(())
}

fn validate_aggregate_mv_input_type(
    function: AggregateFunctionKind,
    analyzed_name: &str,
    output_name: &str,
    args: &[crate::sql::analysis::TypedExpr],
) -> Result<(), String> {
    if function != AggregateFunctionKind::Avg {
        return Ok(());
    }
    if !analyzed_name.eq_ignore_ascii_case("avg") {
        return Err(format!(
            "aggregate MV analyzed aggregate mismatch for `{output_name}`: expected AVG, got {analyzed_name}"
        ));
    }
    let input_type = args
        .first()
        .map(|arg| &arg.data_type)
        .ok_or_else(|| "AVG aggregate requires a column expression argument".to_string())?;
    if matches!(
        input_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _)
    ) {
        return Ok(());
    }
    Err(format!(
        "AVG state type is unsupported for aggregate `{output_name}` input: {input_type:?}"
    ))
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

/// Lightweight projection of the iceberg base table that
/// `validate_ivm_primary_key` needs. Built once at the top of `create_mv`
/// from the loaded iceberg table; passing this struct keeps validation
/// pure and easy to unit-test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseColumnDescriptor {
    pub name: String,
    pub data_type: DataType,
    /// Uppercased SQL type as the analyzer/iceberg-schema mapper produced
    /// it (e.g. `BIGINT`, `STRING`, `DECIMAL(18,2)`, `ARRAY<STRING>`).
    pub sql_type: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseTableDescriptor {
    pub format_version: i32,
    pub columns: Vec<BaseColumnDescriptor>,
}

/// Validate that a parsed `PRIMARY KEY (col, ...)` clause on a CREATE
/// MATERIALIZED VIEW statement satisfies the IVM Phase-2 contract:
///
/// 1. The base table is iceberg format-version 2.
/// 2. Every PK column exists on the base table.
/// 3. Every PK column is NOT NULL on the base table.
/// 4. Every PK column has a hashable scalar type.
///
/// Errors fail fast in declared column order — the first mismatch wins.
/// Returns `Ok(())` on success and discards the PK list (PR-1 does not
/// persist it; PR-3 will).
pub(crate) fn validate_ivm_primary_key(
    pk_columns: &[String],
    base: &BaseTableDescriptor,
) -> Result<(), crate::connector::iceberg::changes::ChangeError> {
    use crate::connector::iceberg::changes::ChangeError;

    if base.format_version != 2 && base.format_version != 3 {
        return Err(ChangeError::IcebergFormatUnsupported {
            format_version: base.format_version,
        });
    }
    for pk in pk_columns {
        let col = base
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(pk))
            .ok_or_else(|| ChangeError::PrimaryKeyMissingFromBase { pk_col: pk.clone() })?;
        if col.nullable {
            return Err(ChangeError::PrimaryKeyNullable {
                pk_col: col.name.clone(),
            });
        }
        if !is_hashable_pk_type(&col.sql_type) {
            return Err(ChangeError::PrimaryKeyTypeUnsupported {
                pk_col: col.name.clone(),
                ty: col.sql_type.clone(),
            });
        }
    }
    Ok(())
}

/// Hashable scalar-type predicate for IVM Phase-2 PRIMARY KEY columns.
/// Accepts: BIGINT, INT, SMALLINT, TINYINT, STRING, VARCHAR, DATE,
/// DATETIME, DECIMAL (with or without precision/scale).
/// Rejects: BOOLEAN, FLOAT, DOUBLE, ARRAY, MAP, STRUCT, JSON.
fn is_hashable_pk_type(sql_type: &str) -> bool {
    let upper = sql_type.to_ascii_uppercase();
    let head = upper.split(['(', '<']).next().unwrap_or("").trim();
    matches!(
        head,
        "BIGINT"
            | "INT"
            | "INTEGER"
            | "SMALLINT"
            | "TINYINT"
            | "STRING"
            | "VARCHAR"
            | "CHAR"
            | "DATE"
            | "DATETIME"
            | "TIMESTAMP"
            | "DECIMAL"
    )
}

/// Map an Arrow `DataType` to the SQL head token that
/// `is_hashable_pk_type` recognizes. Returns the token only — no
/// precision/scale or element-type tail. Anything not on the accepted
/// list falls through to the Arrow Debug form (e.g. `Float32`,
/// `List(...)`), which `is_hashable_pk_type` will then reject.
fn arrow_data_type_pk_head(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "STRING".to_string(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "DATETIME".to_string(),
        // Explicitly unsupported as PK: floats (NaN equality), booleans
        // (degenerate cardinality), composites (no stable hash). Fall
        // through to Debug form so is_hashable_pk_type rejects them.
        other => format!("{other:?}"),
    }
}

/// Build the `BaseTableDescriptor` projection from an already-loaded
/// iceberg table. Used by `create_mv` and `create_iceberg_mv` before
/// invoking `validate_ivm_primary_key`.
pub(crate) fn descriptor_from_loaded(
    loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
) -> BaseTableDescriptor {
    let format_version = loaded.table.metadata().format_version() as i32;
    let columns = loaded
        .columns
        .iter()
        .map(|col| BaseColumnDescriptor {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            sql_type: arrow_data_type_pk_head(&col.data_type),
            nullable: col.nullable,
        })
        .collect();
    BaseTableDescriptor {
        format_version,
        columns,
    }
}

pub(crate) fn drop_mv(
    state: &Arc<StandaloneState>,
    _current_catalog: Option<&str>,
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

    crate::connector::starrocks::managed::ddl::drop_managed_table_with_metadata(
        state,
        &db_name,
        &mv_name,
        |txn, table_id| {
            state
                .mv_repo
                .drop_by_id(txn, table_id)
                .map_err(|e| format!("delete materialized view definition failed: {e}"))?;
            Ok(())
        },
    )?;
    Ok(StatementResult::Ok)
}

pub(crate) fn iceberg_table_ref_fqns(base_refs: &[IcebergTableRef]) -> Vec<String> {
    base_refs.iter().map(IcebergTableRef::fqn).collect()
}

pub(crate) fn list_mv_rows(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    stmt: &ShowMaterializedViewsStmt,
    storage_filter: Option<MvStorageEngine>,
) -> Result<Vec<MvListRow>, String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(vec![]);
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open metadata read transaction failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load materialized view definitions failed: {e}"))?;
    let snapshot = state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock")
        .snapshot
        .clone();

    let mut rows = Vec::new();
    for mv in &definitions {
        let engine = MvStorageEngine::from_sql_str(&mv.storage_engine)?;
        if let Some(filter) = storage_filter
            && engine != filter
        {
            continue;
        }
        if engine == MvStorageEngine::Iceberg {
            let Some(target_catalog) = mv.target_catalog.as_deref() else {
                continue;
            };
            if let Some(current_catalog) = current_catalog
                && !target_catalog.eq_ignore_ascii_case(current_catalog)
            {
                continue;
            }
            let Some(target_namespace) = mv.target_namespace.clone() else {
                continue;
            };
            if let Some(filter_db) = stmt.database.as_deref()
                && !target_namespace.eq_ignore_ascii_case(filter_db)
            {
                continue;
            }
            let Some(target_table) = mv.target_table.clone() else {
                continue;
            };
            rows.push(MvListRow {
                name: target_table,
                database: target_namespace,
                storage_engine: mv.storage_engine.clone(),
                refresh_mode: ManagedMvRefreshMode::DeferredManual
                    .as_sql_str()
                    .to_string(),
                last_refresh_time: mv.last_refresh_ms.map(|value| value.to_string()),
                last_refresh_rows: mv.last_refresh_rows.map(|value| value.to_string()),
                base_tables: mv.base_table_refs.join(", "),
                select_text: mv.select_sql.clone(),
            });
            continue;
        }
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
        rows.push(MvListRow {
            name: table.name.clone(),
            database,
            storage_engine: mv.storage_engine.clone(),
            refresh_mode: ManagedMvRefreshMode::DeferredManual
                .as_sql_str()
                .to_string(),
            last_refresh_time: mv.last_refresh_ms.map(|value| value.to_string()),
            last_refresh_rows: mv.last_refresh_rows.map(|value| value.to_string()),
            base_tables: mv.base_table_refs.join(", "),
            select_text: mv.select_sql.clone(),
        });
    }
    Ok(rows)
}

#[derive(Clone, Debug)]
pub(crate) struct MvAnalysis {
    pub resolved_refs: Vec<ResolvedTableRef>,
    pub output_columns: Vec<OutputColumn>,
    pub resolved_query: ResolvedQuery,
}

pub(crate) fn analyze_mv_select(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<MvAnalysis, String> {
    let resolved_refs = collect_table_refs_from_query(query, current_catalog, current_database);
    let mut analyzed_query = query.clone();
    register_iceberg_tables_for_mv_analysis(state, &resolved_refs)?;
    if has_three_part_refs(&resolved_refs) {
        crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut analyzed_query);
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
        resolved_query: resolved,
    })
}

pub(crate) fn canonicalize_iceberg_mv_select_query(
    query: &sqlparser::ast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> sqlparser::ast::Query {
    let mut query = query.clone();
    let Some(catalog) = current_catalog else {
        return query;
    };
    qualify_current_catalog_refs_in_query(
        &mut query,
        &catalog.to_ascii_lowercase(),
        &current_database.to_ascii_lowercase(),
    );
    query
}

fn qualify_current_catalog_refs_in_query(
    query: &mut sqlparser::ast::Query,
    catalog: &str,
    current_database: &str,
) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            qualify_current_catalog_refs_in_set_expr(
                cte.query.body.as_mut(),
                catalog,
                current_database,
            );
        }
    }
    qualify_current_catalog_refs_in_set_expr(query.body.as_mut(), catalog, current_database);
}

fn qualify_current_catalog_refs_in_set_expr(
    expr: &mut sqlparser::ast::SetExpr,
    catalog: &str,
    current_database: &str,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &mut select.from {
                qualify_current_catalog_refs_in_factor(
                    &mut from.relation,
                    catalog,
                    current_database,
                );
                for join in &mut from.joins {
                    qualify_current_catalog_refs_in_factor(
                        &mut join.relation,
                        catalog,
                        current_database,
                    );
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            qualify_current_catalog_refs_in_set_expr(left.as_mut(), catalog, current_database);
            qualify_current_catalog_refs_in_set_expr(right.as_mut(), catalog, current_database);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            qualify_current_catalog_refs_in_set_expr(
                query.body.as_mut(),
                catalog,
                current_database,
            );
        }
        _ => {}
    }
}

fn qualify_current_catalog_refs_in_factor(
    factor: &mut sqlparser::ast::TableFactor,
    catalog: &str,
    current_database: &str,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            let parts = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let qualified = match parts.as_slice() {
                [table] => Some((
                    catalog.to_string(),
                    current_database.to_string(),
                    table.clone(),
                )),
                [namespace, table] => Some((catalog.to_string(), namespace.clone(), table.clone())),
                _ => None,
            };
            if let Some((catalog, namespace, table)) = qualified {
                name.0 = vec![
                    sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(catalog)),
                    sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                        namespace,
                    )),
                    sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(table)),
                ];
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            qualify_current_catalog_refs_in_set_expr(
                subquery.body.as_mut(),
                catalog,
                current_database,
            );
        }
        _ => {}
    }
}

fn register_iceberg_tables_for_mv_analysis(
    state: &Arc<StandaloneState>,
    resolved_refs: &[ResolvedTableRef],
) -> Result<(), String> {
    let (catalog_backend, table_source) = {
        let registry = state
            .connectors
            .read()
            .expect("standalone connector registry read lock");
        (
            registry.catalog_backend("iceberg")?,
            registry.table_source("iceberg")?,
        )
    };

    for table_ref in resolved_refs {
        let ResolvedTableRef::Iceberg {
            catalog,
            namespace,
            table,
        } = table_ref
        else {
            continue;
        };
        drop_registered_external_table(state, namespace, table)?;
        let resolved = catalog_backend
            .load_table(catalog, namespace, table)
            .map_err(|err| {
                format!("load iceberg table {catalog}.{namespace}.{table} failed: {err}")
            })?;
        let table_def = table_source.build_table_def(&resolved)?;
        let mut local_catalog = state
            .catalog
            .write()
            .map_err(|e| format!("standalone catalog write lock: {e}"))?;
        local_catalog.create_database(namespace)?;
        local_catalog.register(namespace, table_def)?;
    }
    Ok(())
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

pub(crate) fn resolve_mv_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<(String, String), String> {
    match name.parts.as_slice() {
        [table] => Ok((
            normalize_identifier(current_database)?,
            normalize_identifier(table)?,
        )),
        [database, table] => Ok((
            normalize_identifier(database)?,
            normalize_identifier(table)?,
        )),
        [catalog, database, table] => {
            let catalog = normalize_identifier(catalog)?;
            if catalog != "default_catalog" {
                return Err(format!(
                    "materialized view name catalog must be `default_catalog`, got `{catalog}`"
                ));
            }
            Ok((
                normalize_identifier(database)?,
                normalize_identifier(table)?,
            ))
        }
        _ => Err(format!(
            "materialized view name must be `<name>`, `<db>.<name>`, or `default_catalog.<db>.<name>`; got `{}`",
            name.parts.join(".")
        )),
    }
}

pub(crate) fn validate_mv_partition_columns(
    partition_by: Option<&[String]>,
    output_columns: &[OutputColumn],
) -> Result<(), String> {
    let Some(partition_by) = partition_by else {
        return Ok(());
    };
    let output_names = output_columns
        .iter()
        .map(|column| normalize_identifier(&column.name))
        .collect::<Result<HashSet<_>, _>>()?;
    for column in partition_by {
        let normalized = normalize_identifier(column)?;
        if !output_names.contains(&normalized) {
            return Err(format!(
                "materialized view PARTITION BY column `{column}` must be an output column"
            ));
        }
    }
    Ok(())
}

fn collect_table_refs_from_query(
    query: &sqlparser::ast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Vec<ResolvedTableRef> {
    let mut refs = Vec::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_table_refs_from_set_expr(
                cte.query.body.as_ref(),
                current_catalog,
                current_database,
                &mut refs,
            );
        }
    }
    collect_table_refs_from_set_expr(
        query.body.as_ref(),
        current_catalog,
        current_database,
        &mut refs,
    );
    refs
}

fn collect_table_refs_from_set_expr(
    expr: &sqlparser::ast::SetExpr,
    current_catalog: Option<&str>,
    current_database: &str,
    refs: &mut Vec<ResolvedTableRef>,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &select.from {
                collect_table_refs_from_factor(
                    &from.relation,
                    current_catalog,
                    current_database,
                    refs,
                );
                for join in &from.joins {
                    collect_table_refs_from_factor(
                        &join.relation,
                        current_catalog,
                        current_database,
                        refs,
                    );
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            collect_table_refs_from_set_expr(left, current_catalog, current_database, refs);
            collect_table_refs_from_set_expr(right, current_catalog, current_database, refs);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            collect_table_refs_from_set_expr(
                query.body.as_ref(),
                current_catalog,
                current_database,
                refs,
            );
        }
        _ => {}
    }
}

fn collect_table_refs_from_factor(
    factor: &sqlparser::ast::TableFactor,
    current_catalog: Option<&str>,
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
                [table] => match current_catalog {
                    Some(catalog) => ResolvedTableRef::Iceberg {
                        catalog: catalog.to_ascii_lowercase(),
                        namespace: current_database.to_ascii_lowercase(),
                        table: table.clone(),
                    },
                    None => ResolvedTableRef::ManagedLake {
                        database: current_database.to_ascii_lowercase(),
                        table: table.clone(),
                    },
                },
                [database, table] => match current_catalog {
                    Some(catalog) => ResolvedTableRef::Iceberg {
                        catalog: catalog.to_ascii_lowercase(),
                        namespace: database.clone(),
                        table: table.clone(),
                    },
                    None => ResolvedTableRef::ManagedLake {
                        database: database.clone(),
                        table: table.clone(),
                    },
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
                        current_catalog,
                        current_database,
                        refs,
                    );
                }
            }
            collect_table_refs_from_set_expr(
                subquery.body.as_ref(),
                current_catalog,
                current_database,
                refs,
            );
        }
        _ => {}
    }
}

fn has_three_part_refs(resolved_refs: &[ResolvedTableRef]) -> bool {
    resolved_refs
        .iter()
        .any(|table_ref| matches!(table_ref, ResolvedTableRef::Iceberg { .. }))
}

pub(crate) fn output_column_to_table_column(
    column: &OutputColumn,
) -> Result<TableColumnDef, String> {
    Ok(TableColumnDef {
        name: column.name.clone(),
        data_type: arrow_data_type_to_sql_type(&column.data_type)?,
        nullable: column.nullable,
        aggregation: None,
        default: None,
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

pub(crate) fn build_mv_rows_result(rows: &[MvListRow]) -> Result<QueryResult, String> {
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
            name: "StorageEngine".to_string(),
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
        Field::new("StorageEngine", DataType::Utf8, false),
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
                .map(|row| Some(row.storage_engine.clone()))
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

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn cleanup_bootstrapped_tablets(tablet_ids: &[i64]) {
    if tablet_ids.is_empty() {
        return;
    }
    if let Err(err) = delete_tablet(&DeleteTabletRequest {
        tablet_ids: tablet_ids.to_vec(),
    }) {
        tracing::warn!(
            "managed materialized view create cleanup failed to delete bootstrapped tablets: tablet_ids={:?} error={}",
            tablet_ids,
            err
        );
        for tablet_id in tablet_ids {
            let _ = remove_tablet_runtime(*tablet_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::model::StoredManagedTable;
    use super::*;
    use crate::connector::starrocks::managed::catalog::ManagedTableRuntime;
    use crate::engine::catalog::InMemoryCatalog;
    use crate::meta::MetaStoreProvider;
    use crate::runtime::starlet_shard_registry::S3StoreConfig;
    use arrow::array::Array;
    use std::sync::RwLock;
    use tempfile::TempDir;

    fn parse_create_mv(sql: &str) -> crate::sql::parser::ast::CreateMaterializedViewStmt {
        let stmt = crate::sql::parser::parse_sql(sql).expect("parse").remove(0);
        let crate::sql::parser::ast::Statement::CreateMaterializedView(stmt) = stmt else {
            panic!("not create mv");
        };
        stmt
    }

    fn open_state_with_sqlite_store() -> (Arc<StandaloneState>, TempDir) {
        let dir = TempDir::new().expect("metadata tempdir");
        let metadata_path = dir.path().join("standalone.sqlite");
        let metadata_provider =
            crate::meta::SqliteMetaStoreProvider::open(&metadata_path).expect("open meta provider");
        let state = Arc::new(StandaloneState {
            metadata_provider: Some(Arc::new(metadata_provider)),
            ..StandaloneState::default()
        });
        (state, dir)
    }

    fn test_managed_config(warehouse_uri: String) -> super::super::config::ManagedLakeConfig {
        super::super::config::ManagedLakeConfig {
            warehouse_uri,
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "test".to_string(),
                root: "warehouse".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "managed_lake".to_string(),
        }
    }

    fn state_with_inflight_managed_mv() -> (Arc<StandaloneState>, TempDir) {
        let dir = TempDir::new().expect("metadata tempdir");
        let metadata_path = dir.path().join("standalone.sqlite");
        let metadata_provider =
            crate::meta::SqliteMetaStoreProvider::open(&metadata_path).expect("open meta provider");
        let warehouse_uri = format!("file://{}", dir.path().join("managed").display());
        let config = test_managed_config(warehouse_uri.clone());
        let request_schema = build_tablet_schema(
            &[
                TableColumnDef {
                    name: "k1".to_string(),
                    data_type: SqlType::Int,
                    nullable: false,
                    aggregation: None,
                    default: None,
                },
                TableColumnDef {
                    name: "v1".to_string(),
                    data_type: SqlType::String,
                    nullable: true,
                    aggregation: None,
                    default: None,
                },
            ],
            &TableKeyDesc {
                kind: TableKeyKind::Duplicate,
                columns: vec!["k1".to_string()],
            },
            0,
        )
        .expect("build request schema");
        let tablet_schema_pb =
            crate::connector::starrocks::lake::schema::build_tablet_schema_pb_from_thrift(
                &request_schema,
            )
            .expect("build tablet schema pb")
            .encode_to_vec();
        let mut write = metadata_provider
            .begin_write("seed inflight managed mv")
            .expect("open write txn");
        let database = crate::meta::repository::managed_lake::ManagedLakeMetaRepository::default()
            .get_or_create_database(write.as_mut(), "analytics")
            .expect("create managed database");
        let created = crate::meta::repository::managed_lake::ManagedLakeMetaRepository::default()
            .create_table_layout(
                write.as_mut(),
                CreateManagedTableLayoutRequest {
                    db_id: database.db_id,
                    table_name: "orders_mv".to_string(),
                    keys_type: "DUP_KEYS".to_string(),
                    bucket_num: 1,
                    kind: RepoManagedTableKind::MaterializedView,
                    schema_version: 0,
                    tablet_schema_pb,
                    columns: vec![
                        CreateManagedColumnRequest {
                            column_name: "k1".to_string(),
                            logical_type: "INT".to_string(),
                            nullable: false,
                            visible: true,
                            is_key: true,
                        },
                        CreateManagedColumnRequest {
                            column_name: "v1".to_string(),
                            logical_type: "STRING".to_string(),
                            nullable: true,
                            visible: true,
                            is_key: false,
                        },
                    ],
                    partition_name: "p0".to_string(),
                    warehouse_uri,
                },
            )
            .expect("create managed mv layout");
        crate::meta::repository::managed_txn::ManagedLakeTxnRepository::default()
            .record_visible_bootstrap(
                write.as_mut(),
                created.table.table_id,
                created.partition.partition_id,
            )
            .expect("record bootstrap txn");
        crate::meta::repository::managed_txn::ManagedLakeTxnRepository::default()
            .prepare(
                &crate::meta::repository::managed_lake::ManagedLakeMetaRepository::default(),
                write.as_mut(),
                created.table.table_id,
                created.partition.partition_id,
            )
            .expect("record inflight txn");
        crate::meta::repository::mv::MvMetaRepository::default()
            .create_definition_with_id(
                write.as_mut(),
                created.table.table_id,
                CreateMvDefinitionRequest {
                    select_sql: "SELECT k1, v1 FROM ice.ns.orders".to_string(),
                    base_table_refs: vec!["ice.ns.orders".to_string()],
                    primary_key_columns: Vec::new(),
                    storage_engine: ManagedMvStorageEngine::ManagedLake.as_sql_str().to_string(),
                    target_catalog: None,
                    target_namespace: None,
                    target_table: None,
                    schema_contract: None,
                    created_at_ms: now_ms(),
                },
            )
            .expect("create mv definition");
        write.commit().expect("commit seeded metadata");
        let read = metadata_provider.begin_read().expect("open read txn");
        let snapshot = crate::meta::repository::managed_lake::ManagedLakeMetaRepository::default()
            .load_snapshot(read.as_ref())
            .expect("load managed snapshot");
        let managed = ManagedLakeCatalog::rebuild_from_repository(Some(config.clone()), snapshot)
            .expect("rebuild managed catalog");
        let state = Arc::new(StandaloneState {
            managed_lake: RwLock::new(managed),
            managed_lake_config: Some(config),
            metadata_provider: Some(Arc::new(metadata_provider)),
            ..StandaloneState::default()
        });
        (state, dir)
    }

    fn mv_definition_exists(state: &Arc<StandaloneState>, mv_id: i64) -> bool {
        let provider = state.metadata_provider.as_ref().expect("metadata provider");
        let txn = provider.begin_read().expect("open read txn");
        state
            .mv_repo
            .load_by_id(txn.as_ref(), mv_id)
            .expect("load mv definition")
            .is_some()
    }

    fn insert_iceberg_mv_relationship(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        select_sql: &str,
    ) {
        let provider = state.metadata_provider.as_ref().expect("metadata provider");
        let mut txn = provider.begin_write("seed iceberg mv").expect("write txn");
        state
            .mv_repo
            .create_definition(
                txn.as_mut(),
                CreateMvDefinitionRequest {
                    select_sql: select_sql.to_string(),
                    base_table_refs: vec![format!("{catalog}.sales.orders")],
                    primary_key_columns: Vec::new(),
                    storage_engine: ManagedMvStorageEngine::Iceberg.as_sql_str().to_string(),
                    target_catalog: Some(catalog.to_string()),
                    target_namespace: Some(namespace.to_string()),
                    target_table: Some(table.to_string()),
                    schema_contract: None,
                    created_at_ms: now_ms(),
                },
            )
            .expect("insert iceberg mv relationship");
        txn.commit().expect("commit iceberg mv relationship");
    }

    fn assert_query_result_contains(result: &QueryResult, expected: &str) {
        for chunk in &result.chunks {
            for column in chunk.batch.columns() {
                if let Some(strings) = column.as_any().downcast_ref::<StringArray>() {
                    for row_idx in 0..strings.len() {
                        if !strings.is_null(row_idx) && strings.value(row_idx).contains(expected) {
                            return;
                        }
                    }
                }
            }
        }
        panic!("expected query result to contain `{expected}`, got {result:?}");
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
    fn show_materialized_views_lists_iceberg_relationship_without_managed_table_row() {
        let (state, _dir) = open_state_with_sqlite_store();
        insert_iceberg_mv_relationship(
            &state,
            "ice",
            "analytics",
            "mv_orders",
            "SELECT id FROM ice.sales.orders",
        );

        let stmt = ShowMaterializedViewsStmt { database: None };
        let rows = list_mv_rows(&state, Some("ice"), &stmt, None).expect("show mvs");
        let result = build_mv_rows_result(&rows).expect("build rows");

        assert_query_result_contains(&result, "mv_orders");
        assert_query_result_contains(&result, "iceberg");
    }

    #[test]
    fn list_mv_rows_filters_managed_and_iceberg_storage_engines() {
        let (state, _dir) = state_with_inflight_managed_mv();
        insert_iceberg_mv_relationship(
            &state,
            "ice",
            "analytics",
            "mv_orders",
            "SELECT id FROM ice.sales.orders",
        );

        let stmt = ShowMaterializedViewsStmt { database: None };
        let managed = list_mv_rows(
            &state,
            Some("ice"),
            &stmt,
            Some(MvStorageEngine::ManagedLake),
        )
        .expect("managed rows");
        let iceberg = list_mv_rows(&state, Some("ice"), &stmt, Some(MvStorageEngine::Iceberg))
            .expect("iceberg rows");

        assert!(!managed.is_empty(), "expected managed MV rows");
        assert!(!iceberg.is_empty(), "expected iceberg MV rows");
        assert!(
            managed
                .iter()
                .all(|row| row.storage_engine == "managed_lake")
        );
        assert!(iceberg.iter().all(|row| row.storage_engine == "iceberg"));
    }

    #[test]
    fn drop_managed_mv_preserves_repo_definition_when_legacy_drop_rejects() {
        let (state, _dir) = state_with_inflight_managed_mv();
        let mv_id = state
            .managed_lake
            .read()
            .expect("managed lake read lock")
            .table("analytics", "orders_mv")
            .expect("managed mv runtime")
            .table
            .table_id;
        assert!(mv_definition_exists(&state, mv_id));

        let stmt = DropMaterializedViewStmt {
            name: ObjectName {
                parts: vec!["analytics".to_string(), "orders_mv".to_string()],
            },
            if_exists: false,
        };
        let err = drop_mv(&state, None, "analytics", &stmt).expect_err("drop should reject");

        assert!(err.contains("inflight managed txns"), "err={err}");
        assert!(
            mv_definition_exists(&state, mv_id),
            "legacy-owned MV must remain visible through repository reads when legacy drop rejects"
        );
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
             as select k1, stddev(v2) from ice.ns.orders group by k1",
        );
        let err = super::super::mv_shape::classify_incremental_mv_query(&stmt.select_query)
            .expect_err("agg rejected");
        assert!(err.contains("incremental aggregate MV"), "err={err}");
    }

    fn resolved_avg_query(arg_type: DataType) -> ResolvedQuery {
        use crate::sql::analysis::{ExprKind, ProjectItem, ResolvedSelect, TypedExpr};

        let group_key = TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: "k".to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        };
        let avg_arg = TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: "v".to_string(),
            },
            data_type: arg_type,
            nullable: true,
        };
        let avg = TypedExpr {
            kind: ExprKind::AggregateCall {
                name: "avg".to_string(),
                args: vec![avg_arg],
                distinct: false,
                order_by: Vec::new(),
            },
            data_type: DataType::Float64,
            nullable: true,
        };

        ResolvedQuery {
            body: QueryBody::Select(ResolvedSelect {
                from: None,
                filter: None,
                group_by: vec![group_key.clone()],
                having: None,
                projection: vec![
                    ProjectItem {
                        expr: group_key,
                        output_name: "k".to_string(),
                    },
                    ProjectItem {
                        expr: avg,
                        output_name: "a".to_string(),
                    },
                ],
                has_aggregation: true,
                distinct: false,
                repeat: None,
            }),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            output_columns: Vec::new(),
            local_cte_ids: Vec::new(),
        }
    }

    fn avg_shape() -> IncrementalMvShape {
        let stmt = parse_create_mv(
            "create materialized view mv1 distributed by hash(k) buckets 2 \
             as select k, avg(v) as a from ice.ns.orders group by k",
        );
        super::super::mv_shape::classify_incremental_mv_query(&stmt.select_query)
            .expect("avg shape")
    }

    #[test]
    fn aggregate_mv_analyzed_types_accepts_avg_integer_input() {
        let shape = avg_shape();
        let resolved = resolved_avg_query(DataType::Int64);
        validate_incremental_mv_analyzed_types(&shape, &resolved).expect("AVG(Int64) is supported");
    }

    #[test]
    fn aggregate_mv_analyzed_types_rejects_avg_float_and_string_inputs() {
        let shape = avg_shape();
        for data_type in [DataType::Float64, DataType::Utf8] {
            let resolved = resolved_avg_query(data_type.clone());
            let err = validate_incremental_mv_analyzed_types(&shape, &resolved)
                .expect_err("unsupported AVG input should be rejected");
            assert!(err.contains("AVG state type is unsupported"), "err={err}");
            assert!(err.contains(&format!("{data_type:?}")), "err={err}");
        }
    }

    #[test]
    fn projection_mv_primary_key_uses_primary_key_storage() {
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW analytics.orders_mv
DISTRIBUTED BY HASH(id) BUCKETS 2
PRIMARY KEY (id)
AS SELECT id, customer, amount
FROM ice.ns.orders
WHERE amount > 0",
        );
        let mv_shape = super::super::mv_shape::classify_incremental_mv_query(&stmt.select_query)
            .expect("projection shape");
        let output_columns = vec![
            OutputColumn {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                name: "customer".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            OutputColumn {
                name: "amount".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
        ];
        let layout = build_mv_storage_layout(
            &mv_shape,
            stmt.distribution.as_ref().expect("distribution"),
            &output_columns,
            stmt.primary_key.as_deref().expect("primary key"),
            Some(&descriptor(
                2,
                &[
                    ("id", "BIGINT", false),
                    ("customer", "STRING", true),
                    ("amount", "BIGINT", true),
                ],
            )),
        )
        .expect("layout");

        assert_eq!(layout.key_desc.kind, TableKeyKind::Primary);
        assert_eq!(layout.key_desc.columns, vec!["__mv_pk_id".to_string()]);
        let hidden_key = &layout.physical_columns[0];
        assert_eq!(hidden_key.column.name, "__mv_pk_id");
        assert!(!hidden_key.visible);
        assert!(hidden_key.is_key);
        let id_column = layout
            .physical_columns
            .iter()
            .find(|column| column.column.name == "id")
            .expect("id column");
        assert!(id_column.visible);
        assert!(!id_column.is_key);
    }

    #[test]
    fn projection_mv_primary_key_missing_output_becomes_hidden_key_column() {
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW analytics.orders_mv
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (id)
AS SELECT customer, amount
FROM ice.ns.orders
WHERE amount > 0",
        );
        let mv_shape = super::super::mv_shape::classify_incremental_mv_query(&stmt.select_query)
            .expect("projection shape");
        let output_columns = vec![
            OutputColumn {
                name: "customer".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            OutputColumn {
                name: "amount".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
        ];

        let layout = build_mv_storage_layout(
            &mv_shape,
            stmt.distribution.as_ref().expect("distribution"),
            &output_columns,
            stmt.primary_key.as_deref().expect("primary key"),
            Some(&descriptor(
                2,
                &[
                    ("id", "BIGINT", false),
                    ("customer", "STRING", true),
                    ("amount", "BIGINT", true),
                ],
            )),
        )
        .expect("layout");

        assert_eq!(layout.key_desc.kind, TableKeyKind::Primary);
        assert_eq!(layout.key_desc.columns, vec!["__mv_pk_id".to_string()]);
        assert_eq!(layout.physical_columns[0].column.name, "__mv_pk_id");
        assert!(!layout.physical_columns[0].visible);
        assert!(layout.physical_columns[0].is_key);
        assert_eq!(layout.physical_columns[1].column.name, "customer");
        assert!(layout.physical_columns[1].visible);
        assert_eq!(layout.physical_columns[2].column.name, "amount");
        assert!(layout.physical_columns[2].visible);
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
            build_mv_storage_layout(&mv_shape, distribution, &output_columns, &[], None)
                .expect("layout");

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
            &[],
            None,
        )
        .expect_err("hidden physical column collision should fail");
        assert!(
            err.contains("aggregate MV physical column name collision"),
            "err={err}"
        );
        assert!(err.contains("hidden column name collision"), "err={err}");
    }

    #[test]
    fn create_mv_routes_iceberg_storage_engine_to_phase4_path() {
        let stmt_sql = "CREATE MATERIALIZED VIEW analytics.mv1 \
            DISTRIBUTED BY HASH(k) BUCKETS 2 \
            PROPERTIES('storage_engine' = 'iceberg') \
            AS SELECT k FROM ice.ns.t";
        let stmt = parse_create_mv(stmt_sql);
        // resolve_storage_engine takes (PROPERTIES, default_from_config) and returns the resolved enum.
        let resolved =
            resolve_mv_storage_engine(&stmt.properties, "managed_lake").expect("resolve");
        assert_eq!(resolved, ManagedMvStorageEngine::Iceberg);
    }

    #[test]
    fn create_mv_uses_default_when_property_missing() {
        let stmt_sql = "CREATE MATERIALIZED VIEW analytics.mv1 \
            DISTRIBUTED BY HASH(k) BUCKETS 2 \
            AS SELECT k FROM ice.ns.t";
        let stmt = parse_create_mv(stmt_sql);
        let resolved = resolve_mv_storage_engine(&stmt.properties, "iceberg").expect("resolve");
        assert_eq!(resolved, ManagedMvStorageEngine::Iceberg);
    }

    #[test]
    fn create_mv_rejects_unknown_storage_engine() {
        let stmt_sql = "CREATE MATERIALIZED VIEW analytics.mv1 \
            DISTRIBUTED BY HASH(k) BUCKETS 2 \
            PROPERTIES('storage_engine' = 'duckdb') \
            AS SELECT k FROM ice.ns.t";
        let stmt = parse_create_mv(stmt_sql);
        let err = resolve_mv_storage_engine(&stmt.properties, "managed_lake").unwrap_err();
        assert!(err.contains("duckdb"));
    }

    use crate::connector::iceberg::changes::ChangeError;

    /// Build a `BaseTableDescriptor` directly without touching iceberg-rust.
    /// Mirrors the production projection done by the caller in `create_mv`.
    fn descriptor(
        format_version: i32,
        cols: &[(&str, &str, bool)], // name, type, nullable
    ) -> super::BaseTableDescriptor {
        super::BaseTableDescriptor {
            format_version,
            columns: cols
                .iter()
                .map(|(n, t, nullable)| super::BaseColumnDescriptor {
                    name: (*n).to_string(),
                    data_type: descriptor_data_type(t),
                    sql_type: (*t).to_string(),
                    nullable: *nullable,
                })
                .collect(),
        }
    }

    fn descriptor_data_type(sql_type: &str) -> DataType {
        let head = sql_type
            .split(['(', '<'])
            .next()
            .unwrap_or(sql_type)
            .to_ascii_uppercase();
        match head.as_str() {
            "BIGINT" => DataType::Int64,
            "INT" | "INTEGER" => DataType::Int32,
            "STRING" | "VARCHAR" | "CHAR" => DataType::Utf8,
            "DECIMAL" => DataType::Decimal128(18, 2),
            "DOUBLE" => DataType::Float64,
            "ARRAY" => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            other => panic!("unsupported descriptor test type {other}"),
        }
    }

    #[test]
    fn validate_ivm_pk_happy_path() {
        let base = descriptor(
            2,
            &[("order_id", "BIGINT", false), ("customer", "STRING", true)],
        );
        validate_ivm_primary_key(&["order_id".to_string()], &base).expect("ok");
    }

    #[test]
    fn validate_ivm_pk_rejects_v1_base_table() {
        let base = descriptor(1, &[("order_id", "BIGINT", false)]);
        let err = validate_ivm_primary_key(&["order_id".to_string()], &base).expect_err("err");
        assert!(matches!(
            err,
            ChangeError::IcebergFormatUnsupported { format_version: 1 }
        ));
    }

    #[test]
    fn validate_ivm_primary_key_accepts_v3_base() {
        let base = descriptor(3, &[("id", "BIGINT", false)]);
        super::validate_ivm_primary_key(&["id".to_string()], &base).expect("v3 must be accepted");
    }

    #[test]
    fn validate_ivm_pk_rejects_missing_column() {
        let base = descriptor(2, &[("customer", "STRING", true)]);
        let err = validate_ivm_primary_key(&["order_id".to_string()], &base).expect_err("err");
        assert!(matches!(
            err,
            ChangeError::PrimaryKeyMissingFromBase { pk_col } if pk_col == "order_id"
        ));
    }

    #[test]
    fn validate_ivm_pk_rejects_nullable_column() {
        let base = descriptor(2, &[("order_id", "BIGINT", true)]);
        let err = validate_ivm_primary_key(&["order_id".to_string()], &base).expect_err("err");
        assert!(matches!(
            err,
            ChangeError::PrimaryKeyNullable { pk_col } if pk_col == "order_id"
        ));
    }

    #[test]
    fn validate_ivm_pk_rejects_unhashable_type_double() {
        let base = descriptor(2, &[("order_id", "DOUBLE", false)]);
        let err = validate_ivm_primary_key(&["order_id".to_string()], &base).expect_err("err");
        assert!(matches!(
            err,
            ChangeError::PrimaryKeyTypeUnsupported { pk_col, .. } if pk_col == "order_id"
        ));
    }

    #[test]
    fn validate_ivm_pk_rejects_unhashable_type_array() {
        let base = descriptor(2, &[("tags", "ARRAY<STRING>", false)]);
        let err = validate_ivm_primary_key(&["tags".to_string()], &base).expect_err("err");
        assert!(matches!(
            err,
            ChangeError::PrimaryKeyTypeUnsupported { pk_col, .. } if pk_col == "tags"
        ));
    }

    #[test]
    fn validate_ivm_pk_accepts_decimal_and_string() {
        let base = descriptor(
            2,
            &[("k1", "DECIMAL(18,2)", false), ("k2", "STRING", false)],
        );
        validate_ivm_primary_key(&["k1".to_string(), "k2".to_string()], &base).expect("ok");
    }

    #[test]
    fn validate_ivm_pk_first_failure_wins_per_column_order() {
        // missing comes before nullable in column order; expect missing.
        let base = descriptor(2, &[("present_but_nullable", "BIGINT", true)]);
        let err = validate_ivm_primary_key(
            &["absent".to_string(), "present_but_nullable".to_string()],
            &base,
        )
        .expect_err("err");
        assert!(matches!(
            err,
            ChangeError::PrimaryKeyMissingFromBase { pk_col } if pk_col == "absent"
        ));
    }

    #[test]
    fn arrow_data_type_pk_head_maps_supported_scalars() {
        use arrow::datatypes::{DataType, TimeUnit};
        assert_eq!(super::arrow_data_type_pk_head(&DataType::Int8), "TINYINT");
        assert_eq!(super::arrow_data_type_pk_head(&DataType::Int16), "SMALLINT");
        assert_eq!(super::arrow_data_type_pk_head(&DataType::Int32), "INT");
        assert_eq!(super::arrow_data_type_pk_head(&DataType::Int64), "BIGINT");
        assert_eq!(super::arrow_data_type_pk_head(&DataType::Utf8), "STRING");
        assert_eq!(
            super::arrow_data_type_pk_head(&DataType::LargeUtf8),
            "STRING"
        );
        assert_eq!(
            super::arrow_data_type_pk_head(&DataType::Decimal128(18, 2)),
            "DECIMAL"
        );
        assert_eq!(super::arrow_data_type_pk_head(&DataType::Date32), "DATE");
        assert_eq!(
            super::arrow_data_type_pk_head(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            "DATETIME"
        );
    }

    #[test]
    fn arrow_data_type_pk_head_rejects_unsupported_via_debug_fallback() {
        use arrow::datatypes::DataType;
        // Floats are intentionally rejected (NaN equality). The fallback
        // returns the Debug form which is_hashable_pk_type does not match.
        let head = super::arrow_data_type_pk_head(&DataType::Float64);
        assert!(!super::is_hashable_pk_type(&head), "head={head}");
        let head = super::arrow_data_type_pk_head(&DataType::Boolean);
        assert!(!super::is_hashable_pk_type(&head), "head={head}");
    }
}
