use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use iceberg::Catalog;
use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::connector::iceberg::commit::expire_snapshots::{ExpireParams, run_expire_snapshots};
use crate::connector::iceberg::commit::remove_orphan_files::run_remove_orphan_files;
use crate::connector::iceberg::commit::rewrite_manifests::run_rewrite_manifests;
use crate::engine::catalog::normalize_identifier;
use crate::engine::procedure::{CallProcedureStmt, ProcedureArgMode, ProcedureArgValue};
use crate::engine::{
    QueryResult, QueryResultColumn, StandaloneState, StatementResult, record_batch_to_chunk,
};
use crate::fs::object_store::ObjectStoreConfig;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceActionSource {
    SparkProcedure,
    LegacyAlter,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceActionKind {
    RewriteDataFiles,
    RewriteManifests,
    ExpireSnapshots,
    RemoveOrphanFiles,
    RewritePositionDeleteFiles,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct MaintenanceActionOptions {
    pub(crate) values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MaintenanceActionRequest {
    pub(crate) source: MaintenanceActionSource,
    pub(crate) kind: MaintenanceActionKind,
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) options: MaintenanceActionOptions,
    pub(crate) older_than_ms: Option<i64>,
    pub(crate) retain_last: Option<u32>,
    pub(crate) use_caching: Option<bool>,
    pub(crate) spec_id: Option<i32>,
    pub(crate) branch: Option<String>,
    pub(crate) where_clause: Option<String>,
}

impl MaintenanceActionRequest {
    pub(crate) fn from_call(
        stmt: &CallProcedureStmt,
        current_database: &str,
    ) -> Result<Self, String> {
        let kind = match stmt.procedure.as_str() {
            "rewrite_data_files" => MaintenanceActionKind::RewriteDataFiles,
            "rewrite_manifests" => MaintenanceActionKind::RewriteManifests,
            "expire_snapshots" => MaintenanceActionKind::ExpireSnapshots,
            "remove_orphan_files" => MaintenanceActionKind::RemoveOrphanFiles,
            "rewrite_position_delete_files" => MaintenanceActionKind::RewritePositionDeleteFiles,
            other => return Err(format!("unsupported Iceberg system procedure `{other}`")),
        };
        let named = normalize_procedure_args(stmt)?;
        let table = required_string_arg(&named, "table")?;
        let (catalog, namespace, table) =
            resolve_procedure_table_name(&stmt.catalog, current_database, &table)?;
        let mut req = Self {
            source: MaintenanceActionSource::SparkProcedure,
            kind,
            catalog,
            namespace,
            table,
            options: MaintenanceActionOptions::default(),
            older_than_ms: optional_timestamp_arg(&named, "older_than")?,
            retain_last: optional_u32_arg(&named, "retain_last")?,
            use_caching: optional_bool_arg(&named, "use_caching")?,
            spec_id: optional_i32_arg(&named, "spec_id")?,
            branch: optional_string_arg(&named, "branch")?,
            where_clause: optional_string_arg(&named, "where")?,
        };
        if let Some(options) = optional_string_map_arg(&named, "options")? {
            req.options = MaintenanceActionOptions { values: options };
        }
        validate_supported_args(stmt.procedure.as_str(), named.keys())?;
        validate_current_task_args(stmt.procedure.as_str(), named.keys())?;
        validate_call_request_semantics(&req)?;
        Ok(req)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MaintenanceActionOutcome {
    RewriteManifests {
        rewritten_manifests_count: i32,
        added_manifests_count: i32,
    },
    ExpireSnapshots {
        deleted_data_files_count: Option<i64>,
        deleted_position_delete_files_count: Option<i64>,
        deleted_equality_delete_files_count: Option<i64>,
        deleted_manifest_files_count: Option<i64>,
        deleted_manifest_lists_count: Option<i64>,
        deleted_statistics_files_count: Option<i64>,
    },
    RemoveOrphanFiles {
        orphan_file_locations: Vec<String>,
    },
    RewriteDataFiles {
        rewritten_data_files_count: i32,
        added_data_files_count: i32,
        rewritten_bytes_count: i64,
        failed_data_files_count: i32,
        removed_delete_files_count: i32,
    },
    RewritePositionDeleteFiles {
        rewritten_delete_files_count: i32,
        added_delete_files_count: i32,
        rewritten_bytes_count: i64,
        added_bytes_count: i64,
    },
}

pub(crate) fn execute_maintenance_action(
    state: &Arc<StandaloneState>,
    request: MaintenanceActionRequest,
) -> Result<StatementResult, String> {
    match request.source {
        MaintenanceActionSource::SparkProcedure => {
            let outcome = execute_maintenance_action_outcome(state, &request)?;
            Ok(StatementResult::Query(outcome.to_spark_query_result()?))
        }
        MaintenanceActionSource::LegacyAlter => execute_legacy_maintenance_action(state, request),
    }
}

fn execute_maintenance_action_outcome(
    state: &Arc<StandaloneState>,
    request: &MaintenanceActionRequest,
) -> Result<MaintenanceActionOutcome, String> {
    match request.kind {
        MaintenanceActionKind::RewriteManifests => run_rewrite_manifests_action(state, request),
        MaintenanceActionKind::ExpireSnapshots => run_expire_snapshots_action(state, request),
        MaintenanceActionKind::RemoveOrphanFiles => run_remove_orphan_files_action(state, request),
        MaintenanceActionKind::RewriteDataFiles => run_rewrite_data_files_action(state, request),
        MaintenanceActionKind::RewritePositionDeleteFiles => {
            run_rewrite_position_delete_files_action(state, request)
        }
    }
}

fn execute_legacy_maintenance_action(
    state: &Arc<StandaloneState>,
    request: MaintenanceActionRequest,
) -> Result<StatementResult, String> {
    match request.kind {
        MaintenanceActionKind::RewriteDataFiles => create_legacy_optimize_job(state, &request),
        _ => {
            let _ = execute_maintenance_action_outcome(state, &request)?;
            Ok(StatementResult::Ok)
        }
    }
}

fn run_rewrite_manifests_action(
    state: &Arc<StandaloneState>,
    request: &MaintenanceActionRequest,
) -> Result<MaintenanceActionOutcome, String> {
    if request.spec_id.is_some() {
        return Err("rewrite_manifests `spec_id` is not implemented in NovaRocks yet".to_string());
    }
    let (catalog, table_ident, _) = build_action_catalog(state, request)?;
    block_on_iceberg(async move { run_rewrite_manifests(catalog, table_ident).await })?.map_err(
        |e| {
            format!(
                "REWRITE MANIFESTS failed for {}: {e}",
                action_target(request)
            )
        },
    )?;

    Ok(MaintenanceActionOutcome::RewriteManifests {
        rewritten_manifests_count: 0,
        added_manifests_count: 0,
    })
}

fn run_expire_snapshots_action(
    state: &Arc<StandaloneState>,
    request: &MaintenanceActionRequest,
) -> Result<MaintenanceActionOutcome, String> {
    let (catalog, table_ident, _) = build_action_catalog(state, request)?;
    let params = ExpireParams {
        older_than_ms: request.older_than_ms,
        retain_last: request.retain_last,
    };
    let outcome =
        block_on_iceberg(async move { run_expire_snapshots(catalog, table_ident, params).await })?
            .map_err(|e| {
                format!(
                    "EXPIRE SNAPSHOTS failed for {}: {e}",
                    action_target(request)
                )
            })?;

    tracing::info!(
        expired_snapshot_count = outcome.expired_snapshot_count,
        deleted_file_count = outcome.deleted_file_count,
        catalog = %request.catalog,
        namespace = %request.namespace,
        table = %request.table,
        "expire_snapshots: completed"
    );

    Ok(MaintenanceActionOutcome::ExpireSnapshots {
        deleted_data_files_count: None,
        deleted_position_delete_files_count: None,
        deleted_equality_delete_files_count: None,
        deleted_manifest_files_count: None,
        deleted_manifest_lists_count: None,
        deleted_statistics_files_count: None,
    })
}

fn run_remove_orphan_files_action(
    state: &Arc<StandaloneState>,
    request: &MaintenanceActionRequest,
) -> Result<MaintenanceActionOutcome, String> {
    let older_than_ms = request.older_than_ms.ok_or_else(|| {
        "remove_orphan_files requires `older_than` TIMESTAMP argument".to_string()
    })?;
    let (catalog, table_ident, object_store_config) = build_action_catalog(state, request)?;
    let outcome = block_on_iceberg(async move {
        run_remove_orphan_files(
            catalog,
            table_ident,
            older_than_ms,
            object_store_config.as_ref(),
        )
        .await
    })?
    .map_err(|e| {
        format!(
            "REMOVE ORPHAN FILES failed for {}: {e}",
            action_target(request)
        )
    })?;

    tracing::info!(
        deleted_count = outcome.deleted_count,
        scanned_count = outcome.scanned_count,
        catalog = %request.catalog,
        namespace = %request.namespace,
        table = %request.table,
        older_than_ms = older_than_ms,
        "remove_orphan_files: completed"
    );

    Ok(MaintenanceActionOutcome::RemoveOrphanFiles {
        orphan_file_locations: Vec::new(),
    })
}

fn run_rewrite_data_files_action(
    _state: &Arc<StandaloneState>,
    _request: &MaintenanceActionRequest,
) -> Result<MaintenanceActionOutcome, String> {
    Err("rewrite_data_files execution is not implemented in this task".to_string())
}

fn run_rewrite_position_delete_files_action(
    _state: &Arc<StandaloneState>,
    _request: &MaintenanceActionRequest,
) -> Result<MaintenanceActionOutcome, String> {
    Err("rewrite_position_delete_files execution is not implemented in this task".to_string())
}

fn create_legacy_optimize_job(
    _state: &Arc<StandaloneState>,
    _request: &MaintenanceActionRequest,
) -> Result<StatementResult, String> {
    Err(
        "ALTER TABLE OPTIMIZE routing through maintenance actions is not implemented in this task"
            .to_string(),
    )
}

fn build_action_catalog(
    state: &Arc<StandaloneState>,
    request: &MaintenanceActionRequest,
) -> Result<(Arc<dyn Catalog>, TableIdent, Option<ObjectStoreConfig>), String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&request.catalog)?
    };
    entry.invalidate_table_cache(&request.namespace, &request.table);
    let object_store_config = entry.object_store_config().cloned();
    let hadoop_catalog = build_hadoop_catalog(&entry)?;
    let catalog: Arc<dyn Catalog> = Arc::new(hadoop_catalog);
    let table_ident = TableIdent::new(
        NamespaceIdent::new(request.namespace.clone()),
        request.table.clone(),
    );
    Ok((catalog, table_ident, object_store_config))
}

fn action_target(request: &MaintenanceActionRequest) -> String {
    format!(
        "{}.{}.{}",
        request.catalog, request.namespace, request.table
    )
}

fn normalize_procedure_args(
    stmt: &CallProcedureStmt,
) -> Result<BTreeMap<String, ProcedureArgValue>, String> {
    let mut named = BTreeMap::new();
    match stmt.mode {
        ProcedureArgMode::Empty => {}
        ProcedureArgMode::Named => {
            for arg in &stmt.args {
                let Some(name) = &arg.name else {
                    return Err(
                        "CALL procedure cannot mix positional and named arguments".to_string()
                    );
                };
                insert_procedure_arg(&mut named, name, arg.value.clone())?;
            }
        }
        ProcedureArgMode::Positional => {
            let names = positional_names(&stmt.procedure)?;
            if stmt.args.len() > names.len() {
                return Err(format!(
                    "Iceberg system procedure `{}` accepts at most {} positional arguments, got {}",
                    stmt.procedure,
                    names.len(),
                    stmt.args.len()
                ));
            }
            for (arg, name) in stmt.args.iter().zip(names.iter()) {
                if arg.name.is_some() {
                    return Err(
                        "CALL procedure cannot mix positional and named arguments".to_string()
                    );
                }
                insert_procedure_arg(&mut named, name, arg.value.clone())?;
            }
        }
    }
    Ok(named)
}

fn insert_procedure_arg(
    named: &mut BTreeMap<String, ProcedureArgValue>,
    name: &str,
    value: ProcedureArgValue,
) -> Result<(), String> {
    if named.insert(name.to_string(), value).is_some() {
        return Err(format!("duplicate CALL procedure argument `{name}`"));
    }
    Ok(())
}

fn positional_names(procedure: &str) -> Result<&'static [&'static str], String> {
    match procedure {
        "rewrite_data_files" => Ok(&[
            "table",
            "strategy",
            "sort_order",
            "options",
            "where",
            "branch",
        ]),
        "rewrite_manifests" => Ok(&["table", "use_caching", "spec_id"]),
        "expire_snapshots" => Ok(&[
            "table",
            "older_than",
            "retain_last",
            "max_concurrent_deletes",
            "stream_results",
            "snapshot_ids",
            "clean_expired_metadata",
        ]),
        "remove_orphan_files" => Ok(&[
            "table",
            "older_than",
            "location",
            "dry_run",
            "max_concurrent_deletes",
            "file_list_view",
            "equal_schemes",
            "equal_authorities",
            "prefix_mismatch_mode",
            "prefix_listing",
            "stream_results",
        ]),
        "rewrite_position_delete_files" => Ok(&["table", "options", "where"]),
        other => Err(format!("unsupported Iceberg system procedure `{other}`")),
    }
}

fn validate_supported_args<'a>(
    procedure: &str,
    keys: impl IntoIterator<Item = &'a String>,
) -> Result<(), String> {
    let allowed = positional_names(procedure)?;
    for key in keys {
        if !allowed.contains(&key.as_str()) {
            return Err(format!(
                "unsupported argument `{key}` for Iceberg system procedure `{procedure}`"
            ));
        }
    }
    Ok(())
}

fn validate_current_task_args<'a>(
    procedure: &str,
    keys: impl IntoIterator<Item = &'a String>,
) -> Result<(), String> {
    let implemented = match procedure {
        "rewrite_data_files" => &["table", "options", "where", "branch"][..],
        "rewrite_manifests" => &["table", "use_caching", "spec_id"],
        "expire_snapshots" => &["table", "older_than", "retain_last"],
        "remove_orphan_files" => &["table", "older_than"],
        "rewrite_position_delete_files" => &["table", "options", "where"],
        other => return Err(format!("unsupported Iceberg system procedure `{other}`")),
    };
    for key in keys {
        if !implemented.contains(&key.as_str()) {
            return Err(format!(
                "argument `{key}` for Iceberg system procedure `{procedure}` is not implemented in NovaRocks yet"
            ));
        }
    }
    Ok(())
}

fn validate_call_request_semantics(request: &MaintenanceActionRequest) -> Result<(), String> {
    match request.kind {
        MaintenanceActionKind::ExpireSnapshots => {
            if request.older_than_ms.is_none() && request.retain_last.is_none() {
                return Err("expire_snapshots requires `older_than` or `retain_last`".to_string());
            }
        }
        MaintenanceActionKind::RemoveOrphanFiles => {
            return Err(
                "remove_orphan_files Spark procedure cannot return precise orphan file locations yet"
                    .to_string(),
            );
        }
        _ => {}
    }
    Ok(())
}

fn required_string_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<String, String> {
    match named.get(name) {
        Some(ProcedureArgValue::String(value)) => Ok(value.clone()),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a string, got {}",
            procedure_arg_type(value)
        )),
        None => Err(format!("CALL procedure requires `{name}` argument")),
    }
}

fn optional_string_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<String>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::String(value)) => Ok(Some(value.clone())),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a string, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_bool_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<bool>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::Boolean(value)) => Ok(Some(*value)),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a boolean, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_timestamp_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<i64>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::TimestampMillis(value)) => Ok(Some(*value)),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a TIMESTAMP literal, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_u32_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<u32>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::Integer(value)) => {
            if *value <= 0 {
                return Err(format!("CALL procedure argument `{name}` must be >= 1"));
            }
            u32::try_from(*value)
                .map(Some)
                .map_err(|_| format!("CALL procedure argument `{name}` is too large"))
        }
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be an integer, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_i32_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<i32>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::Integer(value)) => i32::try_from(*value)
            .map(Some)
            .map_err(|_| format!("CALL procedure argument `{name}` does not fit i32")),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be an integer, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_string_map_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<BTreeMap<String, String>>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::StringMap(value)) => Ok(Some(value.clone())),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a string map, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn procedure_arg_type(value: &ProcedureArgValue) -> &'static str {
    match value {
        ProcedureArgValue::String(_) => "string",
        ProcedureArgValue::Boolean(_) => "boolean",
        ProcedureArgValue::Integer(_) => "integer",
        ProcedureArgValue::TimestampMillis(_) => "timestamp",
        ProcedureArgValue::StringMap(_) => "string map",
        ProcedureArgValue::Null => "null",
    }
}

fn resolve_procedure_table_name(
    call_catalog: &str,
    current_database: &str,
    raw_table: &str,
) -> Result<(String, String, String), String> {
    let parts = raw_table
        .split('.')
        .map(normalize_identifier)
        .collect::<Result<Vec<_>, _>>()?;
    match parts.as_slice() {
        [table] => Ok((
            normalize_identifier(call_catalog)?,
            normalize_identifier(current_database)?,
            table.clone(),
        )),
        [namespace, table] => Ok((
            normalize_identifier(call_catalog)?,
            namespace.clone(),
            table.clone(),
        )),
        [catalog, namespace, table] => {
            let call_catalog = normalize_identifier(call_catalog)?;
            if catalog != &call_catalog {
                return Err(format!(
                    "CALL procedure table catalog `{catalog}` does not match procedure catalog `{call_catalog}`"
                ));
            }
            Ok((call_catalog, namespace.clone(), table.clone()))
        }
        _ => Err(format!(
            "CALL procedure table must be `table`, `namespace.table`, or `catalog.namespace.table`, got `{raw_table}`"
        )),
    }
}

impl MaintenanceActionOutcome {
    pub(crate) fn to_spark_query_result(&self) -> Result<QueryResult, String> {
        match self {
            Self::RewriteManifests {
                rewritten_manifests_count,
                added_manifests_count,
            } => build_rewrite_manifests_result(*rewritten_manifests_count, *added_manifests_count),
            Self::ExpireSnapshots {
                deleted_data_files_count,
                deleted_position_delete_files_count,
                deleted_equality_delete_files_count,
                deleted_manifest_files_count,
                deleted_manifest_lists_count,
                deleted_statistics_files_count,
            } => build_expire_snapshots_result([
                *deleted_data_files_count,
                *deleted_position_delete_files_count,
                *deleted_equality_delete_files_count,
                *deleted_manifest_files_count,
                *deleted_manifest_lists_count,
                *deleted_statistics_files_count,
            ]),
            Self::RemoveOrphanFiles {
                orphan_file_locations,
            } => build_string_rows_result("orphan_file_location", orphan_file_locations),
            Self::RewriteDataFiles {
                rewritten_data_files_count,
                added_data_files_count,
                rewritten_bytes_count,
                failed_data_files_count,
                removed_delete_files_count,
            } => build_rewrite_data_files_result(
                *rewritten_data_files_count,
                *added_data_files_count,
                *rewritten_bytes_count,
                *failed_data_files_count,
                *removed_delete_files_count,
            ),
            Self::RewritePositionDeleteFiles {
                rewritten_delete_files_count,
                added_delete_files_count,
                rewritten_bytes_count,
                added_bytes_count,
            } => build_rewrite_position_delete_files_result(
                *rewritten_delete_files_count,
                *added_delete_files_count,
                *rewritten_bytes_count,
                *added_bytes_count,
            ),
        }
    }
}

fn build_rewrite_manifests_result(
    rewritten_manifests_count: i32,
    added_manifests_count: i32,
) -> Result<QueryResult, String> {
    build_query_result(
        vec![
            column("rewritten_manifests_count", DataType::Int32, false),
            column("added_manifests_count", DataType::Int32, false),
        ],
        vec![
            Arc::new(Int32Array::from(vec![rewritten_manifests_count])) as ArrayRef,
            Arc::new(Int32Array::from(vec![added_manifests_count])) as ArrayRef,
        ],
    )
}

fn build_expire_snapshots_result(values: [Option<i64>; 6]) -> Result<QueryResult, String> {
    let names = [
        "deleted_data_files_count",
        "deleted_position_delete_files_count",
        "deleted_equality_delete_files_count",
        "deleted_manifest_files_count",
        "deleted_manifest_lists_count",
        "deleted_statistics_files_count",
    ];
    let columns = names
        .iter()
        .map(|name| column(name, DataType::Int64, true))
        .collect::<Vec<_>>();
    let arrays = values
        .iter()
        .map(|value| Arc::new(Int64Array::from(vec![*value])) as ArrayRef)
        .collect::<Vec<_>>();
    build_query_result(columns, arrays)
}

fn build_string_rows_result(column_name: &str, rows: &[String]) -> Result<QueryResult, String> {
    build_query_result(
        vec![column(column_name, DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(rows.to_vec())) as ArrayRef],
    )
}

fn build_rewrite_data_files_result(
    rewritten_data_files_count: i32,
    added_data_files_count: i32,
    rewritten_bytes_count: i64,
    failed_data_files_count: i32,
    removed_delete_files_count: i32,
) -> Result<QueryResult, String> {
    build_query_result(
        vec![
            column("rewritten_data_files_count", DataType::Int32, false),
            column("added_data_files_count", DataType::Int32, false),
            column("rewritten_bytes_count", DataType::Int64, false),
            column("failed_data_files_count", DataType::Int32, false),
            column("removed_delete_files_count", DataType::Int32, false),
        ],
        vec![
            Arc::new(Int32Array::from(vec![rewritten_data_files_count])) as ArrayRef,
            Arc::new(Int32Array::from(vec![added_data_files_count])) as ArrayRef,
            Arc::new(Int64Array::from(vec![rewritten_bytes_count])) as ArrayRef,
            Arc::new(Int32Array::from(vec![failed_data_files_count])) as ArrayRef,
            Arc::new(Int32Array::from(vec![removed_delete_files_count])) as ArrayRef,
        ],
    )
}

fn build_rewrite_position_delete_files_result(
    rewritten_delete_files_count: i32,
    added_delete_files_count: i32,
    rewritten_bytes_count: i64,
    added_bytes_count: i64,
) -> Result<QueryResult, String> {
    build_query_result(
        vec![
            column("rewritten_delete_files_count", DataType::Int32, false),
            column("added_delete_files_count", DataType::Int32, false),
            column("rewritten_bytes_count", DataType::Int64, false),
            column("added_bytes_count", DataType::Int64, false),
        ],
        vec![
            Arc::new(Int32Array::from(vec![rewritten_delete_files_count])) as ArrayRef,
            Arc::new(Int32Array::from(vec![added_delete_files_count])) as ArrayRef,
            Arc::new(Int64Array::from(vec![rewritten_bytes_count])) as ArrayRef,
            Arc::new(Int64Array::from(vec![added_bytes_count])) as ArrayRef,
        ],
    )
}

fn build_query_result(
    columns: Vec<QueryResultColumn>,
    arrays: Vec<ArrayRef>,
) -> Result<QueryResult, String> {
    let fields = columns
        .iter()
        .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("build Iceberg maintenance result failed: {e}"))?;
    Ok(QueryResult {
        columns,
        chunks: vec![record_batch_to_chunk(batch)?],
    })
}

fn column(name: &str, data_type: DataType, nullable: bool) -> QueryResultColumn {
    QueryResultColumn {
        name: name.to_string(),
        data_type,
        nullable,
        logical_type: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::procedure::parse_call_procedure_sql;

    fn request_from_call(sql: &str) -> Result<MaintenanceActionRequest, String> {
        let stmt = parse_call_procedure_sql(sql).unwrap();
        MaintenanceActionRequest::from_call(&stmt, "db")
    }

    #[test]
    fn expire_snapshots_requires_retention_boundary() {
        let err =
            request_from_call("CALL ice.system.expire_snapshots(table => 'db.t')").unwrap_err();
        assert!(err.contains("requires `older_than` or `retain_last`"));
    }

    #[test]
    fn rewrite_data_files_rejects_unimplemented_strategy_arg() {
        let err = request_from_call(
            "CALL ice.system.rewrite_data_files(table => 'db.t', strategy => 'binpack')",
        )
        .unwrap_err();
        assert!(err.contains("not implemented"));
    }

    #[test]
    fn remove_orphan_files_spark_call_rejected_until_locations_available() {
        let err = request_from_call(
            "CALL ice.system.remove_orphan_files(table => 'db.t', older_than => TIMESTAMP '2026-01-01 00:00:00')",
        )
        .unwrap_err();
        assert!(err.contains("orphan file locations"));
    }

    #[test]
    fn rewrite_position_delete_files_schema_matches_spark() {
        let outcome = MaintenanceActionOutcome::RewritePositionDeleteFiles {
            rewritten_delete_files_count: 2,
            added_delete_files_count: 1,
            rewritten_bytes_count: 128,
            added_bytes_count: 96,
        };
        let result = outcome.to_spark_query_result().unwrap();
        let names = result
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "rewritten_delete_files_count",
                "added_delete_files_count",
                "rewritten_bytes_count",
                "added_bytes_count"
            ]
        );
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn remove_orphan_files_returns_one_row_per_location() {
        let outcome = MaintenanceActionOutcome::RemoveOrphanFiles {
            orphan_file_locations: vec![
                "s3://bucket/table/data/a.parquet".to_string(),
                "s3://bucket/table/metadata/old.avro".to_string(),
            ],
        };
        let result = outcome.to_spark_query_result().unwrap();
        assert_eq!(result.columns[0].name, "orphan_file_location");
        assert_eq!(result.row_count(), 2);
    }
}
