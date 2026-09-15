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

use crate::mv::domain::application::{
    MvAlterAction, MvAlterStatement, MvCreateRefreshPolicy, MvCreateStatement, MvDropStatement,
    MvShowStatement,
};
use crate::mv::domain::iceberg_backend::IcebergMvBackend;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::lifecycle::ListMvsRequest;
use crate::mv::domain::model::MvStorageEngine;
use crate::mv::domain::readiness::MvReadinessPort;
use crate::mv::domain::refresh::target::{IcebergMvTarget, resolve_refresh_target};
use novarocks_mv_application::persistence::definition::{
    MvDesiredRefreshPolicy, StoredMvDefinition,
};
use novarocks_parser::ast::Visit;
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;
use novarocks_types::naming::normalize_identifier;

fn default_mv_storage_engine() -> &'static str {
    "iceberg"
}

fn storage_engine_for_create(stmt: &MvCreateStatement) -> Result<MvStorageEngine, String> {
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
    readiness: &MvReadinessPort,
    target: &IcebergMvTarget,
) -> Result<Option<MvStorageEngine>, String> {
    let Some(definition) = readiness
        .load_ready(&MvTarget {
            catalog: Some(target.catalog.clone()),
            database: target.namespace.clone(),
            name: target.table.clone(),
        })
        .map_err(|e| format!("load MV definition by target failed: {e}"))?
    else {
        return Ok(None);
    };
    MvStorageEngine::from_sql_str(&definition.definition.storage_engine).map(Some)
}

fn stored_refresh_policy(policy: &MvCreateRefreshPolicy) -> (MvDesiredRefreshPolicy, Option<i64>) {
    match policy {
        MvCreateRefreshPolicy::Manual => (MvDesiredRefreshPolicy::Manual, None),
        MvCreateRefreshPolicy::AsyncOnChange => (MvDesiredRefreshPolicy::AsyncOnChange, None),
        MvCreateRefreshPolicy::AsyncInterval { interval_ms } => {
            (MvDesiredRefreshPolicy::AsyncInterval, Some(*interval_ms))
        }
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn initial_refresh_configuration_for_create(
    policy: &MvCreateRefreshPolicy,
) -> novarocks_mv_application::repository::InitialMvRefreshConfiguration {
    let (policy, interval_ms) = stored_refresh_policy(policy);
    novarocks_mv_application::repository::InitialMvRefreshConfiguration {
        policy,
        paused: false,
        interval_ms,
        max_staleness_ms: None,
    }
}

fn load_definition_for_alter(
    readiness: &MvReadinessPort,
    current_catalog: Option<&str>,
    db: &str,
    name_parts: &[String],
) -> Result<StoredMvDefinition, String> {
    let target = resolve_refresh_target(current_catalog, db, name_parts)?;
    let Some(definition) = readiness
        .load_ready(&MvTarget {
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
    let definition = definition.definition;
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
    service: &crate::mv::FrontendMvProductAdapter,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &MvCreateStatement,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    if storage_engine_for_create(stmt)? != MvStorageEngine::Iceberg {
        return Err("materialized view backend must be Iceberg".to_string());
    }
    let engine = crate::mv::domain::iceberg_refresh::IcebergMvCreateProviderAdapter::new_with_ports(
        ports.clone(),
        connector_context.clone(),
    );
    service
        .execute_create(
            &engine,
            stmt,
            crate::mv::domain::application::MvRequestContext {
                current_catalog,
                current_database: db,
            },
        )
        .map_err(|error| error.to_string())?;
    Ok(StatementResult::Ok)
}

/// Drop an MV through the readiness-aware Accelerator view and the injected
/// MV backend.
pub fn drop_mv_with_ports(
    product: &novarocks_mv_application::service::MvProductService,
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &MvDropStatement,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    crate::mv::domain::iceberg_refresh::drop_iceberg_mv_with_product(
        product,
        ports,
        current_catalog,
        db,
        stmt,
        connector_context,
    )
}

/// Alter Iceberg MV metadata through the explicit frontend-composed MV ports.
/// Repartition remains a request-frozen frontend refresh operation and is
/// deliberately rejected here so its lifecycle cannot fall back to a generic
/// command route.
pub fn alter_mv_with_ports(
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &MvAlterStatement,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    if matches!(stmt.action, MvAlterAction::Repartition(_)) {
        return Err(
            "ALTER MATERIALIZED VIEW ... REPARTITION requires the frontend MV lifecycle"
                .to_string(),
        );
    }
    if matches!(stmt.action, MvAlterAction::SetProperties(_)) {
        let current_catalog = current_catalog.ok_or_else(|| {
            "ALTER MATERIALIZED VIEW requires current Iceberg catalog".to_string()
        })?;
        let target = resolve_refresh_target(Some(current_catalog), db, &stmt.name_parts)?;
        let engine = existing_mv_storage_engine_by_target(ports.readiness().as_ref(), &target)?
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
        let MvAlterAction::SetProperties(entries) = &stmt.action else {
            unreachable!("properties branch was checked above")
        };
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        crate::connector::mutation::execute_catalog_mutation(
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
        crate::mv::domain::iceberg_refresh::reobserve_and_project_iceberg_mv_with_ports(
            ports,
            &target,
            connector_context.clone(),
        )
        .map_err(|error| format!("reobserve Iceberg MV after property update failed: {error}"))?;
        return Ok(StatementResult::Ok);
    }
    let definition = load_definition_for_alter(
        ports.readiness().as_ref(),
        current_catalog,
        db,
        &stmt.name_parts,
    )?;
    let current_refresh =
        novarocks_mv_application::persistence::semantic::MvRefreshDesiredConfiguration::new(
            definition.refresh_policy.clone(),
            definition.refresh_paused,
            definition.refresh_interval_ms,
            definition.max_staleness_ms,
        )
        .map_err(|error| format!("stored Iceberg MV refresh configuration is invalid: {error}"))?;
    let refresh = match &stmt.action {
        MvAlterAction::SetRefresh(policy) => {
            let (policy, interval_ms) = stored_refresh_policy(policy);
            current_refresh.with_policy(policy, interval_ms)
        }
        MvAlterAction::PauseRefresh => current_refresh.with_paused(true),
        MvAlterAction::ResumeRefresh => current_refresh.with_paused(false),
        MvAlterAction::Repartition(_) | MvAlterAction::SetProperties(_) => {
            unreachable!("repartition and properties returned before metadata update")
        }
    }
    .map_err(|error| format!("invalid Iceberg MV refresh transition: {error}"))?;
    crate::mv::domain::iceberg_refresh::sync_iceberg_mv_descriptor_with_ports(
        ports,
        &definition,
        &refresh.policy,
        refresh.paused,
        refresh.interval_ms,
        None,
        connector_context,
    )
    .map_err(|e| format!("sync Iceberg MV descriptor refresh metadata failed: {e}"))?;
    let target = resolve_refresh_target(current_catalog, db, &stmt.name_parts)?;
    crate::mv::domain::iceberg_refresh::reobserve_and_project_iceberg_mv_with_ports(
        ports,
        &target,
        connector_context.clone(),
    )
    .map_err(|error| format!("reobserve Iceberg MV after descriptor update failed: {error}"))?;
    Ok(StatementResult::Ok)
}

/// List MVs through the injected backend, with no registry lookup. Sorting is
/// retained here because it is part of the SQL presentation contract.
pub fn list_mvs_with_backend(
    mv_backend: &IcebergMvBackend,
    current_catalog: Option<&str>,
    stmt: &MvShowStatement,
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
/// to obtain visible-shaped types for SQL's aggregate physical-layout builder, which expects
/// types matching `shape.visible_outputs` — not the state-shaped columns that
/// the rewritten SELECT (AVG → SUM + COUNT) produces.
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn normalize_incremental_mv_base_ref(
    base_ref: &novarocks_types::naming::TableIdentity,
) -> Result<(String, String, String), String> {
    Ok((
        normalize_identifier(&base_ref.catalog)?,
        normalize_identifier(&base_ref.namespace)?,
        normalize_identifier(&base_ref.table)?,
    ))
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn validate_incremental_mv_base_ref(
    query: &novarocks_parser::ast::Query,
    base_ref: &novarocks_types::naming::TableIdentity,
) -> Result<(String, String, String), String> {
    let refs = query_three_part_table_refs(query);
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

fn query_three_part_table_refs(
    query: &novarocks_parser::ast::Query,
) -> Vec<(String, String, String)> {
    struct Collector(Vec<(String, String, String)>);

    impl novarocks_parser::ast::Visit for Collector {
        fn visit_table_factor(&mut self, factor: &novarocks_parser::ast::TableFactor) {
            if let novarocks_parser::ast::TableFactor::Table { name, .. } = factor
                && let [catalog, namespace, table] = name.parts.as_slice()
            {
                self.0.push((
                    catalog.value.clone(),
                    namespace.value.clone(),
                    table.value.clone(),
                ));
            }
            novarocks_parser::ast::walk_table_factor(self, factor);
        }
    }

    let mut collector = Collector(Vec::new());
    collector.visit_query(query);
    collector.0
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
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

    fn parse_query(sql: &str) -> novarocks_parser::ast::Query {
        let statements = novarocks_parser::parse(sql).expect("parse query");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        query.clone()
    }

    fn base_ref() -> novarocks_types::naming::TableIdentity {
        novarocks_types::naming::TableIdentity {
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
