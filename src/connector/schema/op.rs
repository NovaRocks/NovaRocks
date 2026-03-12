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
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::NaiveDateTime;
use regex::Regex;

use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{ScanMorsel, ScanMorsels, ScanOp};
use crate::novarocks_config::config as novarocks_app_config;
use crate::runtime::backend_id;
use crate::types;

use super::be_compaction_stats_store;
use super::be_tablet_write_log_store;
use super::be_txn_store;
use super::chunk_builder::{SchemaRow, SchemaValue, build_chunk, normalize_column_key};
use super::fe_tables;
use super::load_tracking_logs;
use super::loads;
use super::{BeSchemaTable, SchemaScanContext, SchemaTable};

const DEFAULT_CHUNK_ROWS: usize = 4096;

#[derive(Clone)]
pub(crate) struct SchemaScanOp {
    table: SchemaTable,
    context: SchemaScanContext,
    output_chunk_schema: ChunkSchemaRef,
    should_scan: bool,
    fe_addr: Option<types::TNetworkAddress>,
}

impl SchemaScanOp {
    pub(crate) fn new(
        table: SchemaTable,
        context: SchemaScanContext,
        output_chunk_schema: ChunkSchemaRef,
        should_scan: bool,
        fe_addr: Option<types::TNetworkAddress>,
    ) -> Self {
        Self {
            table,
            context,
            output_chunk_schema,
            should_scan,
            fe_addr,
        }
    }

    fn collect_rows(&self) -> Result<Vec<SchemaRow>, String> {
        let mut rows = match &self.table {
            SchemaTable::Loads => loads::fetch_rows(&self.context, self.fe_addr.as_ref())?,
            SchemaTable::LoadTrackingLogs => {
                load_tracking_logs::fetch_rows(&self.context, self.fe_addr.as_ref())?
            }
            SchemaTable::AnalyzeStatus
            | SchemaTable::CharacterSets
            | SchemaTable::Collations
            | SchemaTable::ColumnPrivileges
            | SchemaTable::ColumnStatsUsage
            | SchemaTable::Events
            | SchemaTable::FeMetrics
            | SchemaTable::Keywords
            | SchemaTable::ApplicableRoles
            | SchemaTable::Engines
            | SchemaTable::FeThreads
            | SchemaTable::FeTabletSchedules
            | SchemaTable::GlobalVariables
            | SchemaTable::KeyColumnUsage
            | SchemaTable::Partitions
            | SchemaTable::PartitionsMeta
            | SchemaTable::PipeFiles
            | SchemaTable::Pipes
            | SchemaTable::ReferentialConstraints
            | SchemaTable::RoutineLoadJobs
            | SchemaTable::Routines
            | SchemaTable::SessionVariables
            | SchemaTable::Statistics
            | SchemaTable::StreamLoads
            | SchemaTable::TableConstraints
            | SchemaTable::Tables
            | SchemaTable::TablesConfig
            | SchemaTable::TablePrivileges
            | SchemaTable::Tasks
            | SchemaTable::TempTables
            | SchemaTable::Triggers
            | SchemaTable::UserPrivileges
            | SchemaTable::VerboseSessionVariables
            | SchemaTable::Views
            | SchemaTable::Columns
            | SchemaTable::Schemata
            | SchemaTable::MaterializedViews
            | SchemaTable::TaskRuns
            | SchemaTable::WarehouseMetrics
            | SchemaTable::WarehouseQueries
            | SchemaTable::TabletReshardJobs
            | SchemaTable::ClusterSnapshots
            | SchemaTable::ClusterSnapshotJobs
            | SchemaTable::RecyclebinCatalogs
            | SchemaTable::ObjectDependencies
            | SchemaTable::FeLocks
            | SchemaTable::FeMemoryUsage
            | SchemaTable::SchemaPrivileges
            | SchemaTable::GrantsToRoles
            | SchemaTable::GrantsToUsers
            | SchemaTable::RoleEdges => {
                fe_tables::fetch_rows(&self.table, &self.context, self.fe_addr.as_ref())?
            }
            SchemaTable::Be(BeSchemaTable::TabletWriteLog) => {
                be_tablet_write_log_store::snapshot(&self.context)
                    .into_iter()
                    .map(|entry| {
                        let mut row = SchemaRow::new();
                        row.insert(
                            normalize_column_key("BE_ID"),
                            SchemaValue::Int64(entry.backend_id),
                        );
                        row.insert(
                            normalize_column_key("BEGIN_TIME"),
                            SchemaValue::TimestampMicrosecond(
                                entry.begin_time_ms.saturating_mul(1000),
                            ),
                        );
                        row.insert(
                            normalize_column_key("FINISH_TIME"),
                            SchemaValue::TimestampMicrosecond(
                                entry.finish_time_ms.saturating_mul(1000),
                            ),
                        );
                        row.insert(
                            normalize_column_key("TXN_ID"),
                            SchemaValue::Int64(entry.txn_id),
                        );
                        row.insert(
                            normalize_column_key("TABLET_ID"),
                            SchemaValue::Int64(entry.tablet_id),
                        );
                        row.insert(
                            normalize_column_key("TABLE_ID"),
                            SchemaValue::Int64(entry.table_id),
                        );
                        row.insert(
                            normalize_column_key("PARTITION_ID"),
                            SchemaValue::Int64(entry.partition_id),
                        );
                        row.insert(
                            normalize_column_key("LOG_TYPE"),
                            SchemaValue::Utf8(entry.log_type.as_str().to_string()),
                        );
                        row.insert(
                            normalize_column_key("INPUT_ROWS"),
                            SchemaValue::Int64(entry.input_rows),
                        );
                        row.insert(
                            normalize_column_key("INPUT_BYTES"),
                            SchemaValue::Int64(entry.input_bytes),
                        );
                        row.insert(
                            normalize_column_key("OUTPUT_ROWS"),
                            SchemaValue::Int64(entry.output_rows),
                        );
                        row.insert(
                            normalize_column_key("OUTPUT_BYTES"),
                            SchemaValue::Int64(entry.output_bytes),
                        );
                        if let Some(input_segments) = entry.input_segments {
                            row.insert(
                                normalize_column_key("INPUT_SEGMENTS"),
                                SchemaValue::Int32(input_segments),
                            );
                        }
                        row.insert(
                            normalize_column_key("OUTPUT_SEGMENTS"),
                            SchemaValue::Int32(entry.output_segments),
                        );
                        if let Some(label) = entry.label {
                            row.insert(normalize_column_key("LABEL"), SchemaValue::Utf8(label));
                        }
                        if let Some(compaction_score) = entry.compaction_score {
                            row.insert(
                                normalize_column_key("COMPACTION_SCORE"),
                                SchemaValue::Int64(compaction_score),
                            );
                        }
                        if let Some(compaction_type) = entry.compaction_type {
                            row.insert(
                                normalize_column_key("COMPACTION_TYPE"),
                                SchemaValue::Utf8(compaction_type),
                            );
                        }
                        row
                    })
                    .collect()
            }
            SchemaTable::Be(BeSchemaTable::Txns) => be_txn_store::snapshot(&self.context)
                .into_iter()
                .map(|entry| {
                    let mut row = SchemaRow::new();
                    row.insert(
                        normalize_column_key("BE_ID"),
                        SchemaValue::Int64(entry.backend_id),
                    );
                    row.insert(
                        normalize_column_key("LOAD_ID"),
                        SchemaValue::Utf8(entry.load_id),
                    );
                    row.insert(
                        normalize_column_key("TXN_ID"),
                        SchemaValue::Int64(entry.txn_id),
                    );
                    row.insert(
                        normalize_column_key("PARTITION_ID"),
                        SchemaValue::Int64(entry.partition_id),
                    );
                    row.insert(
                        normalize_column_key("TABLET_ID"),
                        SchemaValue::Int64(entry.tablet_id),
                    );
                    row.insert(
                        normalize_column_key("CREATE_TIME"),
                        SchemaValue::Int64(entry.create_time),
                    );
                    row.insert(
                        normalize_column_key("COMMIT_TIME"),
                        SchemaValue::Int64(entry.commit_time),
                    );
                    row.insert(
                        normalize_column_key("PUBLISH_TIME"),
                        SchemaValue::Int64(entry.publish_time),
                    );
                    row.insert(
                        normalize_column_key("ROWSET_ID"),
                        SchemaValue::Utf8(entry.rowset_id),
                    );
                    row.insert(
                        normalize_column_key("NUM_SEGMENT"),
                        SchemaValue::Int64(entry.num_segment),
                    );
                    row.insert(
                        normalize_column_key("NUM_DELFILE"),
                        SchemaValue::Int64(entry.num_delfile),
                    );
                    row.insert(
                        normalize_column_key("NUM_ROW"),
                        SchemaValue::Int64(entry.num_row),
                    );
                    row.insert(
                        normalize_column_key("DATA_SIZE"),
                        SchemaValue::Int64(entry.data_size),
                    );
                    row.insert(
                        normalize_column_key("VERSION"),
                        SchemaValue::Int64(entry.version),
                    );
                    row
                })
                .collect(),
            SchemaTable::Be(BeSchemaTable::Compactions) => {
                let be_id = backend_id::backend_id().unwrap_or(-1);
                let stats = be_compaction_stats_store::snapshot();
                let mut row = SchemaRow::new();
                row.insert(normalize_column_key("BE_ID"), SchemaValue::Int64(be_id));
                row.insert(
                    normalize_column_key("CANDIDATES_NUM"),
                    SchemaValue::Int64(stats.candidates_num),
                );
                row.insert(
                    normalize_column_key("BASE_COMPACTION_CONCURRENCY"),
                    SchemaValue::Int64(stats.base_compaction_concurrency),
                );
                row.insert(
                    normalize_column_key("CUMULATIVE_COMPACTION_CONCURRENCY"),
                    SchemaValue::Int64(stats.cumulative_compaction_concurrency),
                );
                row.insert(
                    normalize_column_key("LATEST_COMPACTION_SCORE"),
                    SchemaValue::Float64(stats.latest_compaction_score),
                );
                row.insert(
                    normalize_column_key("CANDIDATE_MAX_SCORE"),
                    SchemaValue::Float64(stats.candidate_max_score),
                );
                row.insert(
                    normalize_column_key("MANUAL_COMPACTION_CONCURRENCY"),
                    SchemaValue::Int64(stats.manual_compaction_concurrency),
                );
                row.insert(
                    normalize_column_key("MANUAL_COMPACTION_CANDIDATES_NUM"),
                    SchemaValue::Int64(stats.manual_compaction_candidates_num),
                );
                vec![row]
            }
            SchemaTable::Be(BeSchemaTable::CloudNativeCompactions) => Vec::new(),
            SchemaTable::Be(BeSchemaTable::Configs) => build_be_config_rows()?,
            SchemaTable::Be(BeSchemaTable::DatacacheMetrics) => Vec::new(),
            SchemaTable::Be(BeSchemaTable::Logs) => build_be_log_rows(&self.context)?,
            SchemaTable::Be(BeSchemaTable::Tablets) => Vec::new(),
            SchemaTable::Be(BeSchemaTable::Threads) => Vec::new(),
            SchemaTable::Be(BeSchemaTable::Bvars) => Vec::new(),
            SchemaTable::Be(BeSchemaTable::Unsupported(_)) => Vec::new(),
        };

        if let Some(limit) = self.context.limit_as_usize()
            && rows.len() > limit
        {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

fn build_be_config_rows() -> Result<Vec<SchemaRow>, String> {
    let Some(config_path) = active_config_path() else {
        return Ok(Vec::new());
    };
    if !config_path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read_to_string(&config_path)
        .map_err(|err| format!("read config {} failed: {err}", config_path.display()))?;
    let value = toml::from_str::<toml::Value>(&raw)
        .map_err(|err| format!("parse config {} failed: {err}", config_path.display()))?;
    let mut rows = Vec::new();
    let be_id = backend_id::backend_id().unwrap_or(-1);
    collect_toml_rows("", &value, be_id, &mut rows);
    rows.sort_by(|left, right| {
        schema_row_string(left, "NAME").cmp(&schema_row_string(right, "NAME"))
    });
    Ok(rows)
}

fn active_config_path() -> Option<PathBuf> {
    env::var("NOVAROCKS_CONFIG")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| Some(PathBuf::from("novarocks.toml")))
}

fn collect_toml_rows(prefix: &str, value: &toml::Value, be_id: i64, rows: &mut Vec<SchemaRow>) {
    match value {
        toml::Value::Table(entries) => {
            for (name, child) in entries {
                let child_prefix = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{prefix}.{name}")
                };
                collect_toml_rows(&child_prefix, child, be_id, rows);
            }
        }
        _ => {
            if prefix.is_empty() {
                return;
            }
            let mut row = SchemaRow::new();
            row.insert(normalize_column_key("BE_ID"), SchemaValue::Int64(be_id));
            row.insert(
                normalize_column_key("NAME"),
                SchemaValue::Utf8(prefix.to_string()),
            );
            row.insert(
                normalize_column_key("VALUE"),
                SchemaValue::Utf8(toml_value_to_string(value)),
            );
            row.insert(
                normalize_column_key("TYPE"),
                SchemaValue::Utf8(toml_value_type_name(value).to_string()),
            );
            row.insert(normalize_column_key("MUTABLE"), SchemaValue::Boolean(false));
            rows.push(row);
        }
    }
}

fn toml_value_type_name(value: &toml::Value) -> &'static str {
    match value {
        toml::Value::String(_) => "string",
        toml::Value::Integer(_) => "integer",
        toml::Value::Float(_) => "float",
        toml::Value::Boolean(_) => "boolean",
        toml::Value::Datetime(_) => "datetime",
        toml::Value::Array(_) => "array",
        toml::Value::Table(_) => "table",
    }
}

fn toml_value_to_string(value: &toml::Value) -> String {
    match value {
        toml::Value::String(value) => value.clone(),
        toml::Value::Integer(value) => value.to_string(),
        toml::Value::Float(value) => value.to_string(),
        toml::Value::Boolean(value) => value.to_string(),
        toml::Value::Datetime(value) => value.to_string(),
        toml::Value::Array(_) | toml::Value::Table(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
        }
    }
}

fn build_be_log_rows(ctx: &SchemaScanContext) -> Result<Vec<SchemaRow>, String> {
    let (log_dir, log_basename) = active_log_settings();
    if !log_dir.is_dir() {
        return Ok(Vec::new());
    }
    let log_pattern = ctx
        .log_pattern
        .as_deref()
        .map(Regex::new)
        .transpose()
        .map_err(|err| format!("invalid be log regex pattern: {err}"))?;
    let mut entries = Vec::new();
    for path in list_be_log_paths(&log_dir, &log_basename, ctx.log_level.as_deref())? {
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(_) => continue,
        };
        for line in raw.lines() {
            let Some(entry) = parse_be_log_line(line) else {
                continue;
            };
            if let Some(start_ts) = normalize_log_filter_ts(ctx.log_start_ts)
                && entry.timestamp < start_ts
            {
                continue;
            }
            if let Some(end_ts) = normalize_log_filter_ts(ctx.log_end_ts)
                && entry.timestamp >= end_ts
            {
                continue;
            }
            if let Some(pattern) = log_pattern.as_ref()
                && !pattern.is_match(&entry.log)
            {
                continue;
            }
            entries.push(entry);
        }
    }
    entries.sort_by(|left, right| {
        right
            .timestamp
            .cmp(&left.timestamp)
            .then_with(|| right.log.cmp(&left.log))
    });
    if let Some(limit) = ctx
        .log_limit
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        && entries.len() > limit
    {
        entries.truncate(limit);
    }
    let be_id = backend_id::backend_id().unwrap_or(-1);
    Ok(entries
        .into_iter()
        .map(|entry| {
            let mut row = SchemaRow::new();
            row.insert(normalize_column_key("BE_ID"), SchemaValue::Int64(be_id));
            row.insert(
                normalize_column_key("LEVEL"),
                SchemaValue::Utf8(entry.level),
            );
            row.insert(
                normalize_column_key("TIMESTAMP"),
                SchemaValue::Int64(entry.timestamp),
            );
            row.insert(normalize_column_key("TID"), SchemaValue::Int64(entry.tid));
            row.insert(normalize_column_key("LOG"), SchemaValue::Utf8(entry.log));
            row
        })
        .collect())
}

fn active_log_settings() -> (PathBuf, String) {
    let log_dir = env::var("NOVAROCKS_LOG_DIR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            env::var("LOG_DIR")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .or_else(|| {
            novarocks_app_config()
                .ok()
                .map(|cfg| cfg.sys_log_dir.clone())
        })
        .unwrap_or_else(|| "log".to_string());
    let log_basename = env::var("NOVAROCKS_LOG_BASENAME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            env::var("NOVAROCKS_LOG_FILE")
                .ok()
                .and_then(|value| derive_log_basename(Path::new(value.trim())))
        })
        .unwrap_or_else(|| "novarocks".to_string());
    (PathBuf::from(log_dir), log_basename)
}

fn derive_log_basename(path: &Path) -> Option<String> {
    path.file_stem()
        .and_then(|value| value.to_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn list_be_log_paths(
    log_dir: &Path,
    log_basename: &str,
    log_level: Option<&str>,
) -> Result<Vec<PathBuf>, String> {
    let level = log_level
        .and_then(|value| value.chars().next())
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or('I');
    let families = match level {
        'W' => ["WARNING", "ERROR"].as_slice(),
        'E' | 'F' => ["ERROR"].as_slice(),
        _ => ["INFO"].as_slice(),
    };
    let mut paths = Vec::new();
    for entry in fs::read_dir(log_dir)
        .map_err(|err| format!("list log dir {} failed: {err}", log_dir.display()))?
    {
        let entry = entry.map_err(|err| err.to_string())?;
        let file_type = entry.file_type().map_err(|err| err.to_string())?;
        if !file_type.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if families.iter().any(|family| {
            file_name == format!("{log_basename}.{family}")
                || file_name.starts_with(&format!("{log_basename}.{family}.log."))
        }) {
            paths.push(entry.path());
        }
    }
    paths.sort_by(|left, right| {
        right
            .file_name()
            .and_then(|name| name.to_str())
            .cmp(&left.file_name().and_then(|name| name.to_str()))
    });
    Ok(paths)
}

fn normalize_log_filter_ts(value: Option<i64>) -> Option<i64> {
    value.map(|value| {
        if value > 100_000_000_000 {
            value / 1000
        } else {
            value
        }
    })
}

fn parse_be_log_line(line: &str) -> Option<ParsedBeLogEntry> {
    let line = line.trim_end();
    let level = line.chars().next()?;
    if !matches!(level, 'I' | 'W' | 'E' | 'F') || line.len() < 27 {
        return None;
    }
    let timestamp_text = line.get(1..25)?;
    let tid_text = line.get(26..)?.split_once(' ')?.0;
    let timestamp = NaiveDateTime::parse_from_str(timestamp_text, "%Y%m%d %H:%M:%S%.f")
        .ok()?
        .and_utc()
        .timestamp();
    let tid = tid_text.parse::<i64>().ok()?;
    Some(ParsedBeLogEntry {
        level: level.to_string(),
        timestamp,
        tid,
        log: line.to_string(),
    })
}

fn schema_row_string<'a>(row: &'a SchemaRow, name: &str) -> &'a str {
    match row.get(&normalize_column_key(name)) {
        Some(SchemaValue::Utf8(value)) => value.as_str(),
        _ => "",
    }
}

struct ParsedBeLogEntry {
    level: String,
    timestamp: i64,
    tid: i64,
    log: String,
}

impl ScanOp for SchemaScanOp {
    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        _profile: Option<crate::runtime::profile::RuntimeProfile>,
        _runtime_filters: Option<&crate::exec::node::scan::RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        match morsel {
            ScanMorsel::Schema { .. } => {}
            _ => return Err("schema scan received unexpected morsel".to_string()),
        }
        if !self.should_scan {
            return Ok(Box::new(std::iter::empty()));
        }

        let rows = self.collect_rows()?;
        if rows.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }

        let mut chunks = Vec::new();
        for batch_rows in rows.chunks(DEFAULT_CHUNK_ROWS.max(1)) {
            let chunk = build_chunk(Arc::clone(&self.output_chunk_schema), batch_rows)?;
            chunks.push(chunk);
        }

        Ok(Box::new(chunks.into_iter().map(Ok)))
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        Ok(ScanMorsels::new(
            vec![ScanMorsel::Schema {
                table_name: self.table.table_name().to_string(),
            }],
            false,
        ))
    }

    fn profile_name(&self) -> Option<String> {
        Some(format!("SCHEMA_SCAN (table={})", self.table.table_name()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::ChunkSchema;

    fn ctx(table_name: &str) -> SchemaScanContext {
        SchemaScanContext {
            table_name: table_name.to_string(),
            db: None,
            table: None,
            wild: None,
            user: None,
            ip: None,
            port: None,
            thread_id: None,
            user_ip: None,
            current_user_ident: None,
            catalog_name: None,
            table_id: None,
            partition_id: None,
            tablet_id: None,
            txn_id: None,
            job_id: None,
            label: None,
            type_: None,
            state: None,
            limit: None,
            log_start_ts: None,
            log_end_ts: None,
            log_level: None,
            log_pattern: None,
            log_limit: None,
            frontends: Vec::new(),
        }
    }

    #[test]
    fn schema_scan_op_respects_slot_projection_order_and_nullable_columns() {
        let _guard = crate::connector::schema::test_lock()
            .lock()
            .expect("schema scan test lock");
        super::be_tablet_write_log_store::clear_for_test();
        super::be_tablet_write_log_store::set_options_for_test(true, 16);
        super::be_tablet_write_log_store::record_load(
            crate::connector::schema::BeTabletWriteLoadLogRecord {
                backend_id: 1,
                begin_time_ms: 1000,
                finish_time_ms: 2000,
                txn_id: 888,
                tablet_id: 9,
                table_id: 7,
                partition_id: 8,
                input_rows: 10,
                input_bytes: 20,
                output_rows: 10,
                output_bytes: 20,
                output_segments: 1,
                label: None,
            },
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("LABEL", DataType::Utf8, true),
            Field::new("TXN_ID", DataType::Int64, false),
        ]));
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            schema.as_ref(),
            &[SlotId::new(2), SlotId::new(1)],
        )
        .expect("chunk schema");
        let op = SchemaScanOp::new(
            SchemaTable::Be(BeSchemaTable::TabletWriteLog),
            ctx("be_tablet_write_log"),
            chunk_schema,
            true,
            None,
        );
        let mut iter = op
            .execute_iter(
                ScanMorsel::Schema {
                    table_name: "be_tablet_write_log".to_string(),
                },
                None,
                None,
            )
            .expect("schema scan execute");
        let chunk = iter.next().expect("one chunk").expect("chunk should be ok");
        assert_eq!(
            chunk
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect::<Vec<_>>(),
            vec!["LABEL".to_string(), "TXN_ID".to_string()]
        );
        let label_col = chunk.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("label as utf8");
        assert!(label_col.is_null(0));
        let txn_col = chunk.columns()[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("txn as int64");
        assert_eq!(txn_col.value(0), 888);

        super::be_tablet_write_log_store::clear_for_test();
    }
}
