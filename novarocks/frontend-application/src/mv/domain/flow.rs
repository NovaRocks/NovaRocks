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

use crate::mv::domain::application::{MvAlterAction, MvCreateRefreshPolicy};
use novarocks_mv_application::persistence::codec::{ConfigurationDocument, RefreshPolicy};
use novarocks_mv_application::persistence::definition::MvDesiredRefreshPolicy;
use novarocks_parser::ast::Visit;
use novarocks_types::naming::normalize_identifier;

fn stored_refresh_policy(policy: &MvCreateRefreshPolicy) -> (MvDesiredRefreshPolicy, Option<i64>) {
    match policy {
        MvCreateRefreshPolicy::Manual => (MvDesiredRefreshPolicy::Manual, None),
        MvCreateRefreshPolicy::AsyncOnChange => (MvDesiredRefreshPolicy::AsyncOnChange, None),
        MvCreateRefreshPolicy::AsyncInterval { interval_ms } => {
            (MvDesiredRefreshPolicy::AsyncInterval, Some(*interval_ms))
        }
    }
}

fn desired_refresh_configuration(
    configuration: &ConfigurationDocument,
) -> Result<novarocks_mv_application::persistence::semantic::MvRefreshDesiredConfiguration, String>
{
    novarocks_mv_application::persistence::semantic::MvRefreshDesiredConfiguration::new(
        match configuration.refresh_policy {
            RefreshPolicy::Manual => MvDesiredRefreshPolicy::Manual,
            RefreshPolicy::AsyncOnChange => MvDesiredRefreshPolicy::AsyncOnChange,
            RefreshPolicy::AsyncInterval => MvDesiredRefreshPolicy::AsyncInterval,
        },
        configuration.paused,
        configuration
            .refresh_interval_ms
            .map(i64::try_from)
            .transpose()
            .map_err(|_| "MV refresh interval exceeds the SQL duration range".to_string())?,
        configuration
            .max_staleness_ms
            .map(i64::try_from)
            .transpose()
            .map_err(|_| "MV maximum staleness exceeds the SQL duration range".to_string())?,
    )
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

/// The pure configuration transition used by original control preparation.
pub(crate) fn configuration_after_alter(
    configuration: &ConfigurationDocument,
    action: &MvAlterAction,
) -> Result<novarocks_mv_application::persistence::semantic::MvRefreshDesiredConfiguration, String>
{
    let current = desired_refresh_configuration(configuration)
        .map_err(|error| format!("stored Iceberg MV refresh configuration is invalid: {error}"))?;
    match action {
        MvAlterAction::SetRefresh(policy) => {
            let (policy, interval_ms) = stored_refresh_policy(policy);
            current.with_policy(policy, interval_ms)
        }
        MvAlterAction::PauseRefresh => current.with_paused(true),
        MvAlterAction::ResumeRefresh => current.with_paused(false),
        MvAlterAction::Repartition(_) | MvAlterAction::SetProperties(_) => {
            unreachable!("repartition and properties returned before metadata update")
        }
    }
    .map_err(|error| format!("invalid Iceberg MV refresh transition: {error}"))
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

    #[test]
    fn alter_refresh_reads_every_desired_field_from_configuration() {
        let configuration = super::ConfigurationDocument {
            refresh_policy: super::RefreshPolicy::AsyncInterval,
            paused: true,
            refresh_interval_ms: Some(123),
            max_staleness_ms: Some(456),
        };
        let desired = super::desired_refresh_configuration(&configuration).unwrap();
        assert_eq!(desired.policy, super::MvDesiredRefreshPolicy::AsyncInterval);
        assert!(desired.paused);
        assert_eq!(desired.interval_ms, Some(123));
        assert_eq!(desired.max_staleness_ms, Some(456));
    }

    #[test]
    fn alter_refresh_rejects_duration_overflow_instead_of_truncating() {
        let mut configuration = super::ConfigurationDocument {
            refresh_policy: super::RefreshPolicy::AsyncInterval,
            paused: false,
            refresh_interval_ms: Some(u64::MAX),
            max_staleness_ms: None,
        };
        assert!(super::desired_refresh_configuration(&configuration).is_err());
        configuration.refresh_interval_ms = Some(123);
        configuration.max_staleness_ms = Some(u64::MAX);
        assert!(super::desired_refresh_configuration(&configuration).is_err());
    }

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
