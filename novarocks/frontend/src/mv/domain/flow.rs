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

//! Iceberg materialized-view metadata statement dispatch.

use std::sync::Arc;

use crate::mv::domain::application::MvApplicationService;
use crate::mv::domain::iceberg_backend::IcebergMvBackend;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::lifecycle::{CreateMvRequest, DropMvRequest, ListMvsRequest};
use crate::mv::domain::model::{MvStorageEngine, MvTarget};
use crate::mv::domain::persistence::definition::{
    StoredMvDefinition, StoredMvRefreshPolicy, UpdateMvRefreshMetadataRequest,
};
use crate::mv::domain::refresh::target::{IcebergMvTarget, resolve_refresh_target};
use crate::mv::domain::repository::MvRepository;
use novarocks::runtime::statement_result::StatementResult;
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_sql::syntax::three_part_table_ref_occurrences;
use novarocks_sql::syntax::{
    AlterMaterializedViewAction, AlterMaterializedViewStmt, CreateMaterializedViewStmt,
    DropMaterializedViewStmt, MaterializedViewRefreshPolicy, ShowMaterializedViewsStmt,
};

fn default_mv_storage_engine() -> &'static str {
    "iceberg"
}

fn storage_engine_for_create(stmt: &CreateMaterializedViewStmt) -> Result<MvStorageEngine, String> {
    let configured = stmt
        .properties
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("storage_engine"))
        .map(|(_, value)| value.clone());
    let raw = match configured.as_deref() {
        Some(value) => value.trim(),
        None => default_mv_storage_engine(),
    };
    match raw.to_ascii_lowercase().as_str() {
        "iceberg" => Ok(MvStorageEngine::Iceberg),
        "starrocks" => Err(
            "storage_engine='starrocks' is no longer supported for standalone materialized views; use storage_engine='iceberg'"
                .to_string(),
        ),
        _ => Err(format!(
            "unknown materialized view storage_engine `{raw}`"
        )),
    }
}

fn existing_mv_storage_engine_by_target(
    repository: &dyn MvRepository,
    target: &IcebergMvTarget,
) -> Result<Option<MvStorageEngine>, String> {
    let Some(definition) = repository
        .find_by_target(&MvTarget {
            catalog: Some(target.catalog.clone()),
            database: target.namespace.clone(),
            name: target.table.clone(),
        })
        .map_err(|e| format!("load MV definition by target failed: {e}"))?
    else {
        return Ok(None);
    };
    MvStorageEngine::from_sql_str(&definition.storage_engine).map(Some)
}

fn stored_refresh_policy(
    policy: &MaterializedViewRefreshPolicy,
) -> (StoredMvRefreshPolicy, Option<i64>) {
    match policy {
        MaterializedViewRefreshPolicy::Manual => (StoredMvRefreshPolicy::Manual, None),
        MaterializedViewRefreshPolicy::AsyncOnChange => {
            (StoredMvRefreshPolicy::AsyncOnChange, None)
        }
        MaterializedViewRefreshPolicy::AsyncInterval { interval_ms } => {
            (StoredMvRefreshPolicy::AsyncInterval, Some(*interval_ms))
        }
    }
}

pub(crate) fn initial_refresh_configuration_for_create(
    policy: &MaterializedViewRefreshPolicy,
) -> crate::mv::domain::repository::InitialMvRefreshConfiguration {
    let (policy, interval_ms) = stored_refresh_policy(policy);
    crate::mv::domain::repository::InitialMvRefreshConfiguration {
        policy,
        paused: false,
        interval_ms,
        max_staleness_ms: None,
        next_refresh_after_ms: None,
    }
}

pub(crate) fn refresh_metadata_request_for_create(
    mv_id: i64,
    policy: &MaterializedViewRefreshPolicy,
) -> UpdateMvRefreshMetadataRequest {
    let initial = initial_refresh_configuration_for_create(policy);
    UpdateMvRefreshMetadataRequest {
        mv_id,
        refresh_policy: initial.policy,
        refresh_paused: false,
        refresh_interval_ms: initial.interval_ms,
        max_staleness_ms: initial.max_staleness_ms,
        last_scheduler_error: None,
        next_refresh_after_ms: None,
    }
}

fn refresh_metadata_request_for_policy(
    definition: &StoredMvDefinition,
    policy: &MaterializedViewRefreshPolicy,
    refresh_paused: bool,
) -> UpdateMvRefreshMetadataRequest {
    let (refresh_policy, refresh_interval_ms) = stored_refresh_policy(policy);
    UpdateMvRefreshMetadataRequest {
        mv_id: definition.mv_id,
        refresh_policy,
        refresh_paused,
        refresh_interval_ms,
        max_staleness_ms: definition.max_staleness_ms,
        last_scheduler_error: None,
        next_refresh_after_ms: None,
    }
}

fn load_definition_for_alter(
    repository: &dyn MvRepository,
    current_catalog: Option<&str>,
    db: &str,
    name: &novarocks_sql::syntax::ObjectName,
) -> Result<StoredMvDefinition, String> {
    let target = resolve_refresh_target(current_catalog, db, name)?;
    let Some(definition) = repository
        .find_by_target(&MvTarget {
            catalog: Some(target.catalog.clone()),
            database: target.namespace.clone(),
            name: target.table.clone(),
        })
        .map_err(|e| format!("load MV definition by target failed: {e}"))?
    else {
        return Err(format!(
            "materialized view does not exist: {}.{}.{}",
            target.catalog, target.namespace, target.table
        ));
    };
    if MvStorageEngine::from_sql_str(&definition.storage_engine)? != MvStorageEngine::Iceberg {
        return Err(
            "ALTER MATERIALIZED VIEW is only supported for Iceberg-backed materialized views"
                .to_string(),
        );
    }
    Ok(definition)
}

/// Create an MV from the explicit MV ports composed by the frontend.
///
/// The SQL surface admits only Iceberg-backed MVs, so this uses the single
/// injected backend rather than a string-keyed connector registry lookup.
pub fn create_mv_with_ports(
    ports: &IcebergMvCorePorts,
    application: &dyn MvApplicationService,
    mv_backend: &IcebergMvBackend,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &CreateMaterializedViewStmt,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    novarocks::connector::validate_request_context(connector_context)?;
    if storage_engine_for_create(stmt)? != MvStorageEngine::Iceberg {
        return Err("materialized view backend must be Iceberg".to_string());
    }
    let engine = crate::mv::domain::iceberg_refresh::StandaloneMvEngine::new_with_ports(
        ports.clone(),
        connector_context.clone(),
    );
    let application_statement = crate::mv::domain::application::MvApplicationStatement::Create(
        crate::mv::domain::application::MvCreateStatement::from(stmt),
    );
    match application.try_handle_statement(
        &engine,
        &application_statement,
        crate::mv::domain::application::MvRequestContext {
            current_catalog,
            current_database: db,
        },
    ) {
        Ok(Some(crate::mv::domain::application::MvStatementResult::Ok)) => {
            return Ok(StatementResult::Ok);
        }
        Ok(Some(crate::mv::domain::application::MvStatementResult::Query(result))) => {
            return Ok(StatementResult::Query(result));
        }
        Ok(None) => {}
        Err(error) => return Err(error.to_string()),
    }
    mv_backend.create_mv(CreateMvRequest {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
        current_database: db.to_string(),
        connector_context: connector_context.clone(),
    })?;
    Ok(StatementResult::Ok)
}

/// Drop an MV from the durable MV repository and the injected MV backend.
pub fn drop_mv_with_ports(
    repository: &dyn MvRepository,
    mv_backend: &IcebergMvBackend,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &DropMaterializedViewStmt,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    novarocks::connector::validate_request_context(connector_context)?;
    let target = resolve_refresh_target(current_catalog, db, &stmt.name)?;
    if let Some(engine) = existing_mv_storage_engine_by_target(repository, &target)?
        && engine != MvStorageEngine::Iceberg
    {
        return Err(
            "DROP MATERIALIZED VIEW is only supported for Iceberg-backed materialized views"
                .to_string(),
        );
    }
    mv_backend.drop_mv(DropMvRequest {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
        current_database: db.to_string(),
        connector_context: connector_context.clone(),
    })?;
    Ok(StatementResult::Ok)
}

/// Alter Iceberg MV metadata through the explicit frontend-composed MV ports.
/// Repartition remains a request-frozen frontend refresh operation and is
/// deliberately rejected here so its lifecycle cannot fall back to a generic
/// command route.
pub fn alter_mv_with_ports(
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &AlterMaterializedViewStmt,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    novarocks::connector::validate_request_context(connector_context)?;
    if matches!(stmt.action, AlterMaterializedViewAction::Repartition(_)) {
        return Err(
            "ALTER MATERIALIZED VIEW ... REPARTITION requires the frontend MV lifecycle"
                .to_string(),
        );
    }
    if matches!(stmt.action, AlterMaterializedViewAction::SetProperties(_)) {
        let current_catalog = current_catalog.ok_or_else(|| {
            "ALTER MATERIALIZED VIEW requires current Iceberg catalog".to_string()
        })?;
        let target = resolve_refresh_target(Some(current_catalog), db, &stmt.name)?;
        let engine = existing_mv_storage_engine_by_target(ports.repository().as_ref(), &target)?
            .ok_or_else(|| {
                format!(
                    "materialized view {}.{}.{} not found",
                    target.catalog, target.namespace, target.table
                )
            })?;
        if engine != MvStorageEngine::Iceberg {
            return Err(
                "ALTER MATERIALIZED VIEW is only supported for Iceberg-backed materialized views"
                    .to_string(),
            );
        }
        let AlterMaterializedViewAction::SetProperties(entries) = &stmt.action else {
            unreachable!("properties branch was checked above")
        };
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        novarocks::connector::mutation::execute_catalog_mutation(
            ports.connector_control(),
            &instance_id,
            novarocks_spi::connector::ConnectorCatalogMutationOperation::AlterProperties {
                table: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: instance_id.clone(),
                    namespace: Arc::from(target.namespace.as_str()),
                    table: Arc::from(target.table.as_str()),
                },
                changes: entries
                    .iter()
                    .map(
                        |(key, value)| novarocks_spi::connector::ConnectorPropertyChange::Set {
                            key: Arc::from(key.as_str()),
                            value: Arc::from(value.as_str()),
                        },
                    )
                    .collect(),
                authority: novarocks_spi::connector::ConnectorPropertyAuthority::UserStatement,
                expected_committed_partitioning: None,
            },
            connector_context.clone(),
        )?;
        return Ok(StatementResult::Ok);
    }
    let definition =
        load_definition_for_alter(ports.repository().as_ref(), current_catalog, db, &stmt.name)?;
    let req = match &stmt.action {
        AlterMaterializedViewAction::SetRefresh(policy) => {
            refresh_metadata_request_for_policy(&definition, policy, definition.refresh_paused)
        }
        AlterMaterializedViewAction::PauseRefresh => UpdateMvRefreshMetadataRequest {
            mv_id: definition.mv_id,
            refresh_policy: definition.refresh_policy.clone(),
            refresh_paused: true,
            refresh_interval_ms: definition.refresh_interval_ms,
            max_staleness_ms: definition.max_staleness_ms,
            last_scheduler_error: definition.last_scheduler_error.clone(),
            next_refresh_after_ms: definition.next_refresh_after_ms,
        },
        AlterMaterializedViewAction::ResumeRefresh => UpdateMvRefreshMetadataRequest {
            mv_id: definition.mv_id,
            refresh_policy: definition.refresh_policy.clone(),
            refresh_paused: false,
            refresh_interval_ms: definition.refresh_interval_ms,
            max_staleness_ms: definition.max_staleness_ms,
            last_scheduler_error: definition.last_scheduler_error.clone(),
            next_refresh_after_ms: definition.next_refresh_after_ms,
        },
        AlterMaterializedViewAction::Repartition(_)
        | AlterMaterializedViewAction::SetProperties(_) => {
            unreachable!("repartition and properties returned before metadata update")
        }
    };
    // SET REFRESH, PAUSE and RESUME are user-owned configuration, not a refresh
    // lifecycle transition, so they go through the definition-DDL write. Using
    // the lifecycle write would put them inside the refresh fence domain and
    // fail closed whenever another frontend happened to own the active refresh.
    ports
        .repository()
        .update_definition_refresh_metadata(req.clone())
        .map_err(|e| format!("update MV refresh metadata failed: {e}"))?;
    crate::mv::domain::iceberg_refresh::sync_iceberg_mv_descriptor_with_ports(
        ports,
        &definition,
        &req.refresh_policy,
        req.refresh_paused,
        req.refresh_interval_ms,
        None,
        connector_context,
    )
    .map_err(|e| format!("sync Iceberg MV descriptor refresh metadata failed: {e}"))?;
    Ok(StatementResult::Ok)
}

/// List MVs through the injected backend, with no registry lookup. Sorting is
/// retained here because it is part of the SQL presentation contract.
pub fn list_mvs_with_backend(
    mv_backend: &IcebergMvBackend,
    current_catalog: Option<&str>,
    stmt: &ShowMaterializedViewsStmt,
) -> Result<StatementResult, String> {
    let req = ListMvsRequest {
        stmt: stmt.clone(),
        current_catalog: current_catalog.map(str::to_string),
    };
    let mut rows = mv_backend.list_mvs(req)?;
    rows.sort_by(|left, right| {
        left.database
            .cmp(&right.database)
            .then(left.name.cmp(&right.name))
    });
    Ok(StatementResult::Query(
        crate::mv::domain::analysis_adapter::build_mv_rows_result(&rows)?,
    ))
}

/// Analyze the output column types of a MV SELECT SQL without executing it.
///
/// Runs the semantic analyzer on the ORIGINAL (un-rewritten) SQL and returns
/// the visible output columns. This is used by the aggregate MV refresh path
/// to obtain visible-shaped types for `build_aggregate_mv_layout`, which expects
/// types matching `shape.visible_outputs` — not the state-shaped columns that
/// the rewritten SELECT (AVG → SUM + COUNT) produces.
fn normalize_incremental_mv_base_ref(
    base_ref: &novarocks_catalog::identifier::TableIdentity,
) -> Result<(String, String, String), String> {
    Ok((
        normalize_identifier(&base_ref.catalog)?,
        normalize_identifier(&base_ref.namespace)?,
        normalize_identifier(&base_ref.table)?,
    ))
}

pub(crate) fn validate_incremental_mv_base_ref(
    query: &sqlparser::ast::Query,
    base_ref: &novarocks_catalog::identifier::TableIdentity,
) -> Result<(String, String, String), String> {
    let refs = three_part_table_ref_occurrences(&query.to_string())?;
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
            novarocks_sql::syntax::normalize_for_raw_parse(sql).expect("normalize sql");
        let statement =
            novarocks_sql::syntax::parse_normalized_sql_raw(&normalized).expect("parse sql");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query");
        };
        *query
    }

    fn base_ref() -> novarocks_catalog::identifier::TableIdentity {
        novarocks_catalog::identifier::TableIdentity {
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
