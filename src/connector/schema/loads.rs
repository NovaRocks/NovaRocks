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
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use chrono::NaiveDateTime;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel};

use crate::frontend_service::{self, FrontendServiceSyncClient, TFrontendServiceSyncClient};
use crate::service::disk_report;
use crate::types;

use super::SchemaScanContext;
use super::chunk_builder::{SchemaRow, SchemaValue, normalize_column_key};

const FE_LOADS_TIMEOUT_SECS: u64 = 5;

pub(crate) fn fetch_rows(
    ctx: &SchemaScanContext,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SchemaRow>, String> {
    let fe_addr = resolve_frontend_addr(fe_addr).ok_or_else(|| {
        "missing FE address for schema scan loads (coord is absent and heartbeat cache is empty)"
            .to_string()
    })?;
    let request = frontend_service::TGetLoadsParams::new(
        ctx.db.clone(),
        ctx.job_id,
        None::<i64>,
        ctx.label.clone(),
        None::<String>,
    );
    let response = with_frontend_client(&fe_addr, |client| {
        client.get_loads(request).map_err(|err| err.to_string())
    })?;
    Ok(response
        .loads
        .unwrap_or_default()
        .iter()
        .map(build_load_row)
        .collect())
}

fn resolve_frontend_addr(
    fe_addr: Option<&types::TNetworkAddress>,
) -> Option<types::TNetworkAddress> {
    fe_addr.cloned().or_else(disk_report::latest_fe_addr)
}

fn with_frontend_client<T>(
    fe_addr: &types::TNetworkAddress,
    f: impl FnOnce(&mut dyn TFrontendServiceSyncClient) -> Result<T, String>,
) -> Result<T, String> {
    let addr: SocketAddr = format!("{}:{}", fe_addr.hostname, fe_addr.port)
        .parse()
        .map_err(|e| format!("invalid FE address: {e}"))?;
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(FE_LOADS_TIMEOUT_SECS))
        .map_err(|e| format!("connect FE failed: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(FE_LOADS_TIMEOUT_SECS)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(FE_LOADS_TIMEOUT_SECS)));
    let _ = stream.set_nodelay(true);

    let channel = TTcpChannel::with_stream(stream);
    let (i_chan, o_chan) = channel
        .split()
        .map_err(|e| format!("split FE thrift channel failed: {e}"))?;
    let i_trans = TBufferedReadTransport::new(i_chan);
    let o_trans = TBufferedWriteTransport::new(o_chan);
    let i_prot = TBinaryInputProtocol::new(i_trans, true);
    let o_prot = TBinaryOutputProtocol::new(o_trans, true);
    let mut client = FrontendServiceSyncClient::new(i_prot, o_prot);
    f(&mut client)
}

fn build_load_row(info: &frontend_service::TLoadInfo) -> SchemaRow {
    let mut row = SchemaRow::new();
    let job_id = info.job_id.unwrap_or_default();
    row.insert(normalize_column_key("ID"), SchemaValue::Int64(job_id));
    row.insert(
        normalize_column_key("LABEL"),
        SchemaValue::Utf8(info.label.clone().unwrap_or_default()),
    );
    if let Some(profile_id) = optional_non_empty(info.profile_id.as_deref()) {
        row.insert(
            normalize_column_key("PROFILE_ID"),
            SchemaValue::Utf8(profile_id.to_string()),
        );
    }
    row.insert(
        normalize_column_key("DB_NAME"),
        SchemaValue::Utf8(info.db.clone().unwrap_or_default()),
    );
    row.insert(
        normalize_column_key("TABLE_NAME"),
        SchemaValue::Utf8(info.table.clone().unwrap_or_default()),
    );
    row.insert(
        normalize_column_key("USER"),
        SchemaValue::Utf8(info.user.clone().unwrap_or_default()),
    );
    if let Some(warehouse) = optional_non_empty(info.warehouse.as_deref()) {
        row.insert(
            normalize_column_key("WAREHOUSE"),
            SchemaValue::Utf8(warehouse.to_string()),
        );
    }
    row.insert(
        normalize_column_key("STATE"),
        SchemaValue::Utf8(info.state.clone().unwrap_or_default()),
    );
    row.insert(
        normalize_column_key("PROGRESS"),
        SchemaValue::Utf8(info.progress.clone().unwrap_or_default()),
    );
    row.insert(
        normalize_column_key("TYPE"),
        SchemaValue::Utf8(info.type_.clone().unwrap_or_default()),
    );
    row.insert(
        normalize_column_key("PRIORITY"),
        SchemaValue::Utf8(info.priority.clone().unwrap_or_default()),
    );
    row.insert(
        normalize_column_key("SCAN_ROWS"),
        SchemaValue::Int64(info.num_scan_rows.unwrap_or_default()),
    );
    row.insert(
        normalize_column_key("SCAN_BYTES"),
        SchemaValue::Int64(info.num_scan_bytes.unwrap_or_default()),
    );
    row.insert(
        normalize_column_key("FILTERED_ROWS"),
        SchemaValue::Int64(info.num_filtered_rows.unwrap_or_default()),
    );
    row.insert(
        normalize_column_key("UNSELECTED_ROWS"),
        SchemaValue::Int64(info.num_unselected_rows.unwrap_or_default()),
    );
    row.insert(
        normalize_column_key("SINK_ROWS"),
        SchemaValue::Int64(info.num_sink_rows.unwrap_or_default()),
    );
    if let Some(runtime_details) = valid_json_text(info.runtime_details.as_deref()) {
        row.insert(
            normalize_column_key("RUNTIME_DETAILS"),
            SchemaValue::Utf8(runtime_details.to_string()),
        );
    }
    insert_optional_datetime(&mut row, "CREATE_TIME", info.create_time.as_deref());
    insert_optional_datetime(&mut row, "LOAD_START_TIME", info.load_start_time.as_deref());
    insert_optional_datetime(
        &mut row,
        "LOAD_COMMIT_TIME",
        info.load_commit_time.as_deref(),
    );
    insert_optional_datetime(
        &mut row,
        "LOAD_FINISH_TIME",
        info.load_finish_time.as_deref(),
    );
    if let Some(properties) = valid_json_text(info.properties.as_deref()) {
        row.insert(
            normalize_column_key("PROPERTIES"),
            SchemaValue::Utf8(properties.to_string()),
        );
    }
    if let Some(error_msg) = optional_non_empty(info.error_msg.as_deref()) {
        row.insert(
            normalize_column_key("ERROR_MSG"),
            SchemaValue::Utf8(error_msg.to_string()),
        );
    }
    if let Some(tracking_sql) = optional_non_empty(info.tracking_sql.as_deref()) {
        row.insert(
            normalize_column_key("TRACKING_SQL"),
            SchemaValue::Utf8(tracking_sql.to_string()),
        );
    }
    if let Some(rejected_record_path) = optional_non_empty(info.rejected_record_path.as_deref()) {
        row.insert(
            normalize_column_key("REJECTED_RECORD_PATH"),
            SchemaValue::Utf8(rejected_record_path.to_string()),
        );
    }
    row.insert(normalize_column_key("JOB_ID"), SchemaValue::Int64(job_id));
    row
}

fn insert_optional_datetime(row: &mut SchemaRow, column: &str, raw: Option<&str>) {
    let Some(raw) = optional_non_empty(raw) else {
        return;
    };
    let Some(value) = parse_datetime_to_micros(raw) else {
        return;
    };
    row.insert(
        normalize_column_key(column),
        SchemaValue::TimestampMicrosecond(value),
    );
}

fn parse_datetime_to_micros(raw: &str) -> Option<i64> {
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"))
        .map(|dt| dt.and_utc().timestamp_micros())
        .ok()
}

fn valid_json_text(raw: Option<&str>) -> Option<&str> {
    let raw = optional_non_empty(raw)?;
    serde_json::from_str::<serde_json::Value>(raw).ok()?;
    Some(raw)
}

fn optional_non_empty(raw: Option<&str>) -> Option<&str> {
    raw.map(str::trim).filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_load_row_populates_expected_columns() {
        let info = frontend_service::TLoadInfo {
            job_id: Some(42),
            label: Some("insert_1".to_string()),
            state: Some("FINISHED".to_string()),
            progress: Some("ETL:100%; LOAD:100%".to_string()),
            type_: Some("INSERT".to_string()),
            priority: Some("NORMAL".to_string()),
            etl_info: None,
            task_info: None,
            create_time: Some("2026-03-06 10:30:40".to_string()),
            etl_start_time: None,
            etl_finish_time: None,
            load_start_time: Some("2026-03-06 10:31:40".to_string()),
            load_finish_time: Some("2026-03-06 10:32:40".to_string()),
            url: None,
            job_details: None,
            error_msg: Some("".to_string()),
            db: Some("test_db".to_string()),
            txn_id: None,
            tracking_sql: Some("SELECT 1".to_string()),
            num_scan_rows: Some(10),
            num_filtered_rows: Some(1),
            num_unselected_rows: Some(2),
            num_sink_rows: Some(7),
            rejected_record_path: None,
            load_id: None,
            profile_id: Some("profile-1".to_string()),
            table: Some("test_tbl".to_string()),
            user: Some("root".to_string()),
            load_commit_time: Some("2026-03-06 10:32:00".to_string()),
            warehouse: Some("default_warehouse".to_string()),
            runtime_details: Some("{\"txn_id\":1}".to_string()),
            properties: Some("{\"timeout\":3600}".to_string()),
            num_scan_bytes: Some(1024),
        };

        let row = build_load_row(&info);
        assert_eq!(
            row.get(&normalize_column_key("ID")),
            Some(&SchemaValue::Int64(42))
        );
        assert_eq!(
            row.get(&normalize_column_key("JOB_ID")),
            Some(&SchemaValue::Int64(42))
        );
        assert_eq!(
            row.get(&normalize_column_key("TYPE")),
            Some(&SchemaValue::Utf8("INSERT".to_string()))
        );
        assert!(matches!(
            row.get(&normalize_column_key("CREATE_TIME")),
            Some(SchemaValue::TimestampMicrosecond(_))
        ));
        assert_eq!(
            row.get(&normalize_column_key("RUNTIME_DETAILS")),
            Some(&SchemaValue::Utf8("{\"txn_id\":1}".to_string()))
        );
        assert!(!row.contains_key(&normalize_column_key("ERROR_MSG")));
    }

    #[test]
    fn build_load_row_nulls_invalid_json_fields() {
        let info = frontend_service::TLoadInfo {
            runtime_details: Some("not-json".to_string()),
            properties: Some("".to_string()),
            ..Default::default()
        };
        let row = build_load_row(&info);
        assert!(!row.contains_key(&normalize_column_key("RUNTIME_DETAILS")));
        assert!(!row.contains_key(&normalize_column_key("PROPERTIES")));
    }
}
