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
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use serde_json::{Map, Value, json};
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel};

use crate::frontend_service::{
    FrontendServiceSyncClient, TFrontendServiceSyncClient, TLoadTxnBeginRequest,
    TLoadTxnBeginResult, TLoadTxnCommitRequest, TLoadTxnCommitResult, TLoadTxnRollbackRequest,
    TLoadTxnRollbackResult, TStreamLoadPutRequest, TStreamLoadPutResult,
};
use crate::plan_nodes::TFileFormatType;
use crate::runtime::{backend_id, sink_commit};
use crate::service::disk_report;
use crate::service::internal_service;
use crate::status::TStatus;
use crate::status_code::TStatusCode;
use crate::types::{self, TPartialUpdateMode, TUniqueId};

pub(crate) type HttpHeaders = HashMap<String, String>;

#[derive(Clone, Debug)]
struct ApiError {
    code: TStatusCode,
    message: String,
    existing_job_status: Option<String>,
}

impl ApiError {
    fn new(code: TStatusCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            existing_job_status: None,
        }
    }

    fn with_existing_job_status(mut self, job_status: Option<String>) -> Self {
        self.existing_job_status = job_status;
        self
    }
}

#[derive(Clone, Debug)]
struct BasicAuth {
    user: String,
    passwd: String,
    user_ip: Option<String>,
}

#[derive(Clone, Debug)]
struct LoadHeaderOptions {
    format_type: TFileFormatType,
    columns: Option<String>,
    where_clause: Option<String>,
    column_separator: Option<String>,
    row_delimiter: Option<String>,
    skip_header: Option<i64>,
    trim_space: Option<bool>,
    enclose: Option<i8>,
    escape: Option<i8>,
    partitions: Option<String>,
    is_temp_partition: Option<bool>,
    negative: Option<bool>,
    strict_mode: Option<bool>,
    timezone: Option<String>,
    load_mem_limit: Option<i64>,
    jsonpaths: Option<String>,
    json_root: Option<String>,
    strip_outer_array: Option<bool>,
    partial_update: Option<bool>,
    partial_update_mode: Option<TPartialUpdateMode>,
    transmission_compression_type: Option<String>,
    load_dop: Option<i32>,
    merge_condition: Option<String>,
    log_rejected_record_num: Option<i64>,
    payload_compression_type: Option<String>,
    timeout: Option<i64>,
    prepared_timeout: Option<i32>,
    idle_transaction_timeout: Option<i64>,
    label: Option<String>,
    channel_id: Option<i32>,
    warehouse: Option<String>,
    enable_merge_commit: Option<bool>,
    transaction_type: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct LoadStats {
    number_total_rows: i64,
    number_loaded_rows: i64,
    number_filtered_rows: i64,
    number_unselected_rows: i64,
    load_bytes: i64,
    begin_txn_time_ms: i64,
    stream_load_plan_time_ms: i64,
    read_data_time_ms: i64,
    write_data_time_ms: i64,
    load_time_ms: i64,
    commit_and_publish_time_ms: i64,
}

#[derive(Clone, Debug)]
struct TxnContext {
    label: String,
    db: String,
    table: String,
    auth: BasicAuth,
    warehouse: Option<String>,
    txn_id: i64,
    timeout: Option<i64>,
    prepared_timeout: Option<i32>,
    idle_timeout: Option<i64>,
    commit_infos: Vec<types::TTabletCommitInfo>,
    fail_infos: Vec<types::TTabletFailInfo>,
    stats: LoadStats,
    created_at: Instant,
    last_active: Instant,
    finished_channels: HashSet<i32>,
}

impl TxnContext {
    fn is_idle_expired(&self) -> bool {
        let Some(timeout_sec) = self.idle_timeout else {
            return false;
        };
        if timeout_sec <= 0 {
            return false;
        }
        self.last_active.elapsed() >= Duration::from_secs(timeout_sec as u64)
    }
}

fn txn_contexts() -> &'static Mutex<HashMap<String, Arc<Mutex<TxnContext>>>> {
    static CONTEXTS: OnceLock<Mutex<HashMap<String, Arc<Mutex<TxnContext>>>>> = OnceLock::new();
    CONTEXTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn stream_load_file_paths() -> &'static Mutex<HashMap<(i64, i64), String>> {
    static FILE_PATHS: OnceLock<Mutex<HashMap<(i64, i64), String>>> = OnceLock::new();
    FILE_PATHS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn register_stream_load_file(load_id: &TUniqueId, path: &Path) {
    let mut guard = stream_load_file_paths()
        .lock()
        .expect("stream load file path lock");
    guard.insert(
        (load_id.hi, load_id.lo),
        path.as_os_str().to_string_lossy().to_string(),
    );
}

pub(crate) fn resolve_stream_load_file_path(load_id: &TUniqueId) -> Option<String> {
    let guard = stream_load_file_paths()
        .lock()
        .expect("stream load file path lock");
    guard.get(&(load_id.hi, load_id.lo)).cloned()
}

pub(crate) fn unregister_stream_load_file(load_id: &TUniqueId) {
    let mut guard = stream_load_file_paths()
        .lock()
        .expect("stream load file path lock");
    guard.remove(&(load_id.hi, load_id.lo));
}

fn get_header<'a>(headers: &'a HttpHeaders, key: &str) -> Option<&'a str> {
    headers
        .get(key)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn parse_bool_header(headers: &HttpHeaders, key: &str) -> Result<Option<bool>, ApiError> {
    let Some(raw) = get_header(headers, key) else {
        return Ok(None);
    };
    if raw.eq_ignore_ascii_case("true") {
        return Ok(Some(true));
    }
    if raw.eq_ignore_ascii_case("false") {
        return Ok(Some(false));
    }
    Err(ApiError::new(
        TStatusCode::INVALID_ARGUMENT,
        format!("invalid boolean header `{key}`"),
    ))
}

fn parse_i64_header(headers: &HttpHeaders, key: &str) -> Result<Option<i64>, ApiError> {
    let Some(raw) = get_header(headers, key) else {
        return Ok(None);
    };
    raw.parse::<i64>().map(Some).map_err(|_| {
        ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            format!("invalid integer header `{key}`"),
        )
    })
}

fn parse_i32_header(headers: &HttpHeaders, key: &str) -> Result<Option<i32>, ApiError> {
    let Some(raw) = get_header(headers, key) else {
        return Ok(None);
    };
    raw.parse::<i32>().map(Some).map_err(|_| {
        ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            format!("invalid integer header `{key}`"),
        )
    })
}

fn parse_format(headers: &HttpHeaders) -> Result<TFileFormatType, ApiError> {
    let Some(format) = get_header(headers, "format") else {
        return Ok(TFileFormatType::FORMAT_CSV_PLAIN);
    };
    if format.eq_ignore_ascii_case("csv") {
        return Ok(TFileFormatType::FORMAT_CSV_PLAIN);
    }
    if format.eq_ignore_ascii_case("json") {
        return Ok(TFileFormatType::FORMAT_JSON);
    }
    if format.eq_ignore_ascii_case("gzip") {
        return Ok(TFileFormatType::FORMAT_CSV_GZ);
    }
    if format.eq_ignore_ascii_case("bzip2") {
        return Ok(TFileFormatType::FORMAT_CSV_BZ2);
    }
    if format.eq_ignore_ascii_case("lz4") {
        return Ok(TFileFormatType::FORMAT_CSV_LZ4_FRAME);
    }
    if format.eq_ignore_ascii_case("deflate") {
        return Ok(TFileFormatType::FORMAT_CSV_DEFLATE);
    }
    if format.eq_ignore_ascii_case("zstd") {
        return Ok(TFileFormatType::FORMAT_CSV_ZSTD);
    }
    Err(ApiError::new(
        TStatusCode::INVALID_ARGUMENT,
        format!("invalid `format` value: {format}"),
    ))
}

fn parse_partial_update_mode(
    headers: &HttpHeaders,
) -> Result<Option<TPartialUpdateMode>, ApiError> {
    let Some(mode) = get_header(headers, "partial_update_mode") else {
        return Ok(None);
    };
    if mode.eq_ignore_ascii_case("row") {
        return Ok(Some(TPartialUpdateMode::ROW_MODE));
    }
    if mode.eq_ignore_ascii_case("auto") {
        return Ok(Some(TPartialUpdateMode::AUTO_MODE));
    }
    if mode.eq_ignore_ascii_case("column") {
        return Ok(Some(TPartialUpdateMode::COLUMN_UPSERT_MODE));
    }
    if mode.eq_ignore_ascii_case("column_update") {
        return Ok(Some(TPartialUpdateMode::COLUMN_UPDATE_MODE));
    }
    Err(ApiError::new(
        TStatusCode::INVALID_ARGUMENT,
        format!("invalid `partial_update_mode` value: {mode}"),
    ))
}

fn parse_enclose_or_escape(headers: &HttpHeaders, key: &str) -> Result<Option<i8>, ApiError> {
    let Some(raw) = get_header(headers, key) else {
        return Ok(None);
    };
    let mut bytes = raw.as_bytes().iter();
    let Some(first) = bytes.next() else {
        return Ok(None);
    };
    if bytes.next().is_some() {
        return Err(ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            format!("`{key}` must be exactly one byte"),
        ));
    }
    Ok(Some(*first as i8))
}

fn parse_load_headers(headers: &HttpHeaders) -> Result<LoadHeaderOptions, ApiError> {
    let format_type = parse_format(headers)?;
    let partitions = get_header(headers, "partitions").map(ToString::to_string);
    let temp_partitions = get_header(headers, "temporary_partitions")
        .or_else(|| get_header(headers, "temp_partitions"))
        .map(ToString::to_string);
    let has_partitions = partitions.is_some();
    let has_temp_partitions = temp_partitions.is_some();
    if partitions.is_some() && temp_partitions.is_some() {
        return Err(ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            "cannot specify both `partitions` and `temporary_partitions`",
        ));
    }
    let content_encoding = get_header(headers, "content-encoding").map(ToString::to_string);
    let compression = get_header(headers, "compression").map(ToString::to_string);
    if content_encoding.is_some() && compression.is_some() {
        return Err(ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            "only one of `content-encoding` and `compression` can be set",
        ));
    }
    let timeout = parse_i64_header(headers, "timeout")?;
    if let Some(value) = timeout
        && value < 0
    {
        return Err(ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            "`timeout` must be >= 0",
        ));
    }
    let skip_header = parse_i64_header(headers, "skip_header")?;
    if let Some(value) = skip_header
        && value < 0
    {
        return Err(ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            "`skip_header` must be >= 0",
        ));
    }
    let load_mem_limit = parse_i64_header(headers, "load_mem_limit")?;
    if let Some(value) = load_mem_limit
        && value < 0
    {
        return Err(ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            "`load_mem_limit` must be >= 0",
        ));
    }
    let log_rejected_record_num = parse_i64_header(headers, "log_rejected_record_num")?;
    if let Some(value) = log_rejected_record_num
        && value < -1
    {
        return Err(ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            "`log_rejected_record_num` must be >= -1",
        ));
    }
    let load_dop = parse_i32_header(headers, "load_dop")?;
    let prepared_timeout = parse_i32_header(headers, "prepared_timeout")?;
    let idle_transaction_timeout = parse_i64_header(headers, "idle_transaction_timeout")?;
    let channel_id = parse_i32_header(headers, "channel_id")?;

    Ok(LoadHeaderOptions {
        format_type,
        columns: get_header(headers, "columns").map(ToString::to_string),
        where_clause: get_header(headers, "where").map(ToString::to_string),
        column_separator: get_header(headers, "column_separator").map(ToString::to_string),
        row_delimiter: get_header(headers, "row_delimiter").map(ToString::to_string),
        skip_header,
        trim_space: parse_bool_header(headers, "trim_space")?,
        enclose: parse_enclose_or_escape(headers, "enclose")?,
        escape: parse_enclose_or_escape(headers, "escape")?,
        partitions: partitions.or(temp_partitions),
        is_temp_partition: if has_partitions {
            Some(false)
        } else if has_temp_partitions {
            Some(true)
        } else {
            None
        },
        negative: parse_bool_header(headers, "negative")?,
        strict_mode: parse_bool_header(headers, "strict_mode")?,
        timezone: get_header(headers, "timezone").map(ToString::to_string),
        load_mem_limit,
        jsonpaths: get_header(headers, "jsonpaths").map(ToString::to_string),
        json_root: get_header(headers, "json_root").map(ToString::to_string),
        strip_outer_array: parse_bool_header(headers, "strip_outer_array")?,
        partial_update: parse_bool_header(headers, "partial_update")?,
        partial_update_mode: parse_partial_update_mode(headers)?,
        transmission_compression_type: get_header(headers, "transmission_compression_type")
            .map(ToString::to_string),
        load_dop,
        merge_condition: get_header(headers, "merge_condition").map(ToString::to_string),
        log_rejected_record_num,
        payload_compression_type: content_encoding.or(compression),
        timeout,
        prepared_timeout,
        idle_transaction_timeout,
        label: get_header(headers, "label").map(ToString::to_string),
        channel_id,
        warehouse: get_header(headers, "warehouse").map(ToString::to_string),
        enable_merge_commit: parse_bool_header(headers, "enable_merge_commit")?,
        transaction_type: get_header(headers, "transaction_type").map(ToString::to_string),
    })
}

fn parse_basic_auth(headers: &HttpHeaders) -> Result<BasicAuth, ApiError> {
    let auth = get_header(headers, "authorization").ok_or_else(|| {
        ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            "missing HTTP Basic Authorization header",
        )
    })?;
    let encoded = auth
        .strip_prefix("Basic ")
        .or_else(|| auth.strip_prefix("basic "))
        .ok_or_else(|| {
            ApiError::new(
                TStatusCode::INVALID_ARGUMENT,
                "invalid Authorization scheme, expected Basic",
            )
        })?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| ApiError::new(TStatusCode::INVALID_ARGUMENT, "invalid basic auth payload"))?;
    let decoded = String::from_utf8(decoded)
        .map_err(|_| ApiError::new(TStatusCode::INVALID_ARGUMENT, "invalid utf8 basic auth"))?;
    let (user, passwd) = decoded.split_once(':').ok_or_else(|| {
        ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            "invalid basic auth payload, expected user:password",
        )
    })?;
    if user.is_empty() {
        return Err(ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            "basic auth user is empty",
        ));
    }
    Ok(BasicAuth {
        user: user.to_string(),
        passwd: passwd.to_string(),
        user_ip: None,
    })
}

fn status_code_name(code: TStatusCode) -> &'static str {
    if code == TStatusCode::OK {
        "OK"
    } else if code == TStatusCode::CANCELLED {
        "CANCELLED"
    } else if code == TStatusCode::ANALYSIS_ERROR {
        "ANALYSIS_ERROR"
    } else if code == TStatusCode::NOT_IMPLEMENTED_ERROR {
        "NOT_IMPLEMENTED_ERROR"
    } else if code == TStatusCode::RUNTIME_ERROR {
        "RUNTIME_ERROR"
    } else if code == TStatusCode::MEM_LIMIT_EXCEEDED {
        "MEM_LIMIT_EXCEEDED"
    } else if code == TStatusCode::INTERNAL_ERROR {
        "INTERNAL_ERROR"
    } else if code == TStatusCode::THRIFT_RPC_ERROR {
        "THRIFT_RPC_ERROR"
    } else if code == TStatusCode::TIMEOUT {
        "TIMEOUT"
    } else if code == TStatusCode::PUBLISH_TIMEOUT {
        "PUBLISH_TIMEOUT"
    } else if code == TStatusCode::LABEL_ALREADY_EXISTS {
        "LABEL_ALREADY_EXISTS"
    } else if code == TStatusCode::INVALID_ARGUMENT {
        "INVALID_ARGUMENT"
    } else if code == TStatusCode::TXN_NOT_EXISTS {
        "TXN_NOT_EXISTS"
    } else if code == TStatusCode::TXN_IN_PROCESSING {
        "TXN_IN_PROCESSING"
    } else if code == TStatusCode::SERVICE_UNAVAILABLE {
        "SERVICE_UNAVAILABLE"
    } else if code == TStatusCode::SR_EAGAIN {
        "SR_EAGAIN"
    } else {
        "UNKNOWN"
    }
}

fn stream_load_status_text(code: TStatusCode) -> &'static str {
    if code == TStatusCode::OK {
        "Success"
    } else if code == TStatusCode::PUBLISH_TIMEOUT {
        "Publish Timeout"
    } else if code == TStatusCode::LABEL_ALREADY_EXISTS {
        "Label Already Exists"
    } else {
        "Fail"
    }
}

fn status_message(status: &TStatus) -> String {
    status
        .error_msgs
        .as_ref()
        .and_then(|messages| messages.first())
        .cloned()
        .unwrap_or_else(|| status_code_name(status.status_code).to_string())
}

fn api_error_from_status(status: &TStatus) -> ApiError {
    ApiError::new(status.status_code, status_message(status))
}

fn ensure_ok_status(status: &TStatus) -> Result<(), ApiError> {
    if status.status_code == TStatusCode::OK {
        Ok(())
    } else {
        Err(api_error_from_status(status))
    }
}

fn with_frontend_client<T>(
    f: impl FnOnce(&mut dyn TFrontendServiceSyncClient) -> Result<T, String>,
) -> Result<T, ApiError> {
    let fe_addr = disk_report::latest_fe_addr().ok_or_else(|| {
        ApiError::new(
            TStatusCode::INTERNAL_ERROR,
            "missing FE address (heartbeat not established yet)",
        )
    })?;
    let addr: SocketAddr = format!("{}:{}", fe_addr.hostname, fe_addr.port)
        .parse()
        .map_err(|e| {
            ApiError::new(
                TStatusCode::THRIFT_RPC_ERROR,
                format!("invalid FE address: {e}"),
            )
        })?;
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).map_err(|e| {
        ApiError::new(
            TStatusCode::THRIFT_RPC_ERROR,
            format!("connect FE failed: {e}"),
        )
    })?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_nodelay(true);

    let channel = TTcpChannel::with_stream(stream);
    let (i_chan, o_chan) = channel.split().map_err(|e| {
        ApiError::new(
            TStatusCode::THRIFT_RPC_ERROR,
            format!("split FE thrift channel failed: {e}"),
        )
    })?;
    let i_trans = TBufferedReadTransport::new(i_chan);
    let o_trans = TBufferedWriteTransport::new(o_chan);
    let i_prot = TBinaryInputProtocol::new(i_trans, true);
    let o_prot = TBinaryOutputProtocol::new(o_trans, true);
    let mut client = FrontendServiceSyncClient::new(i_prot, o_prot);
    f(&mut client).map_err(|e| ApiError::new(TStatusCode::THRIFT_RPC_ERROR, e))
}

fn random_unique_id() -> TUniqueId {
    TUniqueId::new(rand::random::<i64>(), rand::random::<i64>())
}

fn generate_label() -> String {
    let epoch_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!(
        "novarocks_stream_load_{epoch_ms}_{:08x}",
        rand::random::<u32>()
    )
}

fn write_request_body_to_temp_file(body: &[u8]) -> Result<(PathBuf, i64), ApiError> {
    let path = std::env::temp_dir().join(format!(
        "novarocks_stream_load_{}_{}.data",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        rand::random::<u32>()
    ));
    let started = Instant::now();
    let mut file = File::create(&path).map_err(|e| {
        ApiError::new(
            TStatusCode::IO_ERROR,
            format!("create temp stream load file failed: {e}"),
        )
    })?;
    file.write_all(body).map_err(|e| {
        ApiError::new(
            TStatusCode::IO_ERROR,
            format!("write temp stream load file failed: {e}"),
        )
    })?;
    let elapsed_ms = started.elapsed().as_millis() as i64;
    Ok((path, elapsed_ms))
}

fn begin_txn(
    auth: &BasicAuth,
    db: &str,
    table: &str,
    label: &str,
    timeout: Option<i64>,
    warehouse: Option<&str>,
) -> Result<TLoadTxnBeginResult, ApiError> {
    let request = TLoadTxnBeginRequest::new(
        Option::<String>::None,
        auth.user.clone(),
        auth.passwd.clone(),
        db.to_string(),
        table.to_string(),
        auth.user_ip.clone(),
        label.to_string(),
        Option::<i64>::None,
        Option::<i64>::None,
        timeout,
        Some(random_unique_id()),
        warehouse.map(ToString::to_string),
        backend_id::backend_id(),
    );
    with_frontend_client(|client| client.load_txn_begin(request).map_err(|e| e.to_string()))
}

fn build_put_request(
    auth: &BasicAuth,
    db: &str,
    table: &str,
    txn_id: i64,
    load_id: TUniqueId,
    _local_path: &str,
    options: &LoadHeaderOptions,
    warehouse: Option<&str>,
) -> Result<TStreamLoadPutRequest, ApiError> {
    let timeout =
        match options.timeout {
            Some(value) => Some(i32::try_from(value).map_err(|_| {
                ApiError::new(TStatusCode::INVALID_ARGUMENT, "`timeout` is too large")
            })?),
            None => None,
        };
    Ok(TStreamLoadPutRequest {
        cluster: None,
        user: auth.user.clone(),
        passwd: auth.passwd.clone(),
        db: db.to_string(),
        tbl: table.to_string(),
        user_ip: auth.user_ip.clone(),
        load_id,
        txn_id,
        file_type: types::TFileType::FILE_STREAM,
        format_type: options.format_type,
        path: None,
        columns: options.columns.clone(),
        where_: options.where_clause.clone(),
        column_separator: options.column_separator.clone(),
        partitions: options.partitions.clone(),
        auth_code: None,
        negative: options.negative,
        timeout,
        strict_mode: options.strict_mode,
        timezone: options.timezone.clone(),
        load_mem_limit: options.load_mem_limit,
        is_temp_partition: options.is_temp_partition,
        strip_outer_array: options.strip_outer_array,
        jsonpaths: options.jsonpaths.clone(),
        thrift_rpc_timeout_ms: Some(5_000),
        json_root: options.json_root.clone(),
        partial_update: options.partial_update,
        transmission_compression_type: options.transmission_compression_type.clone(),
        load_dop: options.load_dop,
        enable_replicated_storage: None,
        merge_condition: options.merge_condition.clone(),
        log_rejected_record_num: options.log_rejected_record_num,
        row_delimiter: options.row_delimiter.clone(),
        skip_header: options.skip_header,
        trim_space: options.trim_space,
        enclose: options.enclose,
        escape: options.escape,
        partial_update_mode: options.partial_update_mode,
        payload_compression_type: options.payload_compression_type.clone(),
        warehouse: warehouse.map(ToString::to_string),
        backend_id: backend_id::backend_id(),
    })
}

fn stream_load_put(request: TStreamLoadPutRequest) -> Result<TStreamLoadPutResult, ApiError> {
    with_frontend_client(|client| client.stream_load_put(request).map_err(|e| e.to_string()))
}

fn commit_txn(
    auth: &BasicAuth,
    db: &str,
    table: &str,
    txn_id: i64,
    commit_infos: Vec<types::TTabletCommitInfo>,
    fail_infos: Vec<types::TTabletFailInfo>,
    prepared_timeout: Option<i32>,
    prepare_only: bool,
    stats: Option<&LoadStats>,
) -> Result<TLoadTxnCommitResult, ApiError> {
    let txn_commit_attachment = stats.map(|stats| crate::frontend_service::TTxnCommitAttachment {
        load_type: types::TLoadType::MANUAL_LOAD,
        rl_task_txn_commit_attachment: None,
        ml_txn_commit_attachment: None,
        manual_load_txn_commit_attachment: Some(
            crate::frontend_service::TManualLoadTxnCommitAttachment::new(
                Some(stats.number_loaded_rows),
                Some(stats.number_filtered_rows),
                Option::<String>::None,
                Some(stats.load_bytes),
                Some(stats.load_bytes),
                Some(stats.number_unselected_rows),
                Some(stats.stream_load_plan_time_ms),
                Some(stats.read_data_time_ms),
                Some(stats.begin_txn_time_ms),
            ),
        ),
    });
    let request = TLoadTxnCommitRequest::new(
        Option::<String>::None,
        auth.user.clone(),
        auth.passwd.clone(),
        db.to_string(),
        table.to_string(),
        auth.user_ip.clone(),
        txn_id,
        true,
        Some(commit_infos),
        Option::<i64>::None,
        txn_commit_attachment,
        Some(5_000),
        Some(fail_infos),
        prepared_timeout,
    );
    if prepare_only {
        with_frontend_client(|client| client.load_txn_prepare(request).map_err(|e| e.to_string()))
    } else {
        with_frontend_client(|client| client.load_txn_commit(request).map_err(|e| e.to_string()))
    }
}

fn rollback_txn(
    auth: &BasicAuth,
    db: &str,
    table: &str,
    txn_id: i64,
    reason: &str,
    commit_infos: Vec<types::TTabletCommitInfo>,
    fail_infos: Vec<types::TTabletFailInfo>,
) -> Result<TLoadTxnRollbackResult, ApiError> {
    let request = TLoadTxnRollbackRequest::new(
        Option::<String>::None,
        auth.user.clone(),
        auth.passwd.clone(),
        db.to_string(),
        table.to_string(),
        auth.user_ip.clone(),
        txn_id,
        Some(reason.to_string()),
        Option::<i64>::None,
        Option::<crate::frontend_service::TTxnCommitAttachment>::None,
        Some(fail_infos),
        Some(commit_infos),
    );
    with_frontend_client(|client| client.load_txn_rollback(request).map_err(|e| e.to_string()))
}

fn dedup_extend_commit_infos(
    target: &mut Vec<types::TTabletCommitInfo>,
    source: Vec<types::TTabletCommitInfo>,
) {
    for info in source {
        let exists = target.iter().any(|current| {
            current.tablet_id == info.tablet_id && current.backend_id == info.backend_id
        });
        if !exists {
            target.push(info);
        }
    }
}

fn dedup_extend_fail_infos(
    target: &mut Vec<types::TTabletFailInfo>,
    source: Vec<types::TTabletFailInfo>,
) {
    for info in source {
        let exists = target.iter().any(|current| {
            current.tablet_id == info.tablet_id && current.backend_id == info.backend_id
        });
        if !exists {
            target.push(info);
        }
    }
}

fn parse_db_table_from_headers(headers: &HttpHeaders) -> Result<(String, String), ApiError> {
    let db = get_header(headers, "db")
        .ok_or_else(|| ApiError::new(TStatusCode::INVALID_ARGUMENT, "missing header `db`"))?;
    let table = get_header(headers, "table")
        .ok_or_else(|| ApiError::new(TStatusCode::INVALID_ARGUMENT, "missing header `table`"))?;
    Ok((db.to_string(), table.to_string()))
}

fn parse_required_label(options: &LoadHeaderOptions) -> Result<String, ApiError> {
    options
        .label
        .clone()
        .ok_or_else(|| ApiError::new(TStatusCode::INVALID_ARGUMENT, "missing header `label`"))
}

fn ensure_txn_extensions_supported(options: &LoadHeaderOptions) -> Result<(), ApiError> {
    if options.enable_merge_commit.unwrap_or(false) {
        return Err(ApiError::new(
            TStatusCode::NOT_IMPLEMENTED_ERROR,
            "stream load `enable_merge_commit=true` is not implemented",
        ));
    }
    if let Some(transaction_type) = options.transaction_type.as_deref()
        && transaction_type.eq_ignore_ascii_case("multi")
    {
        return Err(ApiError::new(
            TStatusCode::INVALID_ARGUMENT,
            "stream load `transaction_type=multi` is not supported on BE",
        ));
    }
    Ok(())
}

fn with_txn_context_for_update<T>(
    label: &str,
    f: impl FnOnce(&mut TxnContext) -> Result<T, ApiError>,
) -> Result<T, ApiError> {
    let context = {
        let guard = txn_contexts().lock().expect("txn context store lock");
        guard.get(label).cloned()
    }
    .ok_or_else(|| {
        ApiError::new(
            TStatusCode::TXN_NOT_EXISTS,
            format!("transaction with label `{label}` does not exist"),
        )
    })?;

    let mut context_guard = context.try_lock().map_err(|_| {
        ApiError::new(
            TStatusCode::TXN_IN_PROCESSING,
            format!("transaction `{label}` is in processing"),
        )
    })?;
    if context_guard.is_idle_expired() {
        drop(context_guard);
        let mut store = txn_contexts().lock().expect("txn context store lock");
        store.remove(label);
        return Err(ApiError::new(
            TStatusCode::TXN_NOT_EXISTS,
            format!("transaction `{label}` is expired"),
        ));
    }
    f(&mut context_guard)
}

fn txn_response(
    txn_op: &str,
    code: TStatusCode,
    message: &str,
    db: &str,
    table: &str,
    label: &str,
    txn_id: Option<i64>,
    stats: Option<&LoadStats>,
    begin_txn_time_ms: Option<i64>,
    existing_job_status: Option<&str>,
) -> Value {
    let mut root = Map::new();
    root.insert("Status".to_string(), json!(status_code_name(code)));
    if code == TStatusCode::LABEL_ALREADY_EXISTS
        && let Some(job_status) = existing_job_status
    {
        root.insert("ExistingJobStatus".to_string(), json!(job_status));
    }
    if code == TStatusCode::TXN_IN_PROCESSING {
        root.insert("Label".to_string(), json!(label));
    }
    root.insert("Message".to_string(), json!(message));
    root.insert("Db".to_string(), json!(db));
    root.insert("Table".to_string(), json!(table));

    if code == TStatusCode::OK {
        root.insert("Label".to_string(), json!(label));
        if let Some(txn_id) = txn_id {
            root.insert("TxnId".to_string(), json!(txn_id));
        }
        match txn_op {
            "begin" => {
                root.insert(
                    "BeginTxnTimeMs".to_string(),
                    json!(begin_txn_time_ms.unwrap_or_default()),
                );
            }
            "load" => {
                if let Some(stats) = stats {
                    root.insert("LoadBytes".to_string(), json!(stats.load_bytes));
                    root.insert(
                        "StreamLoadPlanTimeMs".to_string(),
                        json!(stats.stream_load_plan_time_ms),
                    );
                    root.insert(
                        "ReceivedDataTimeMs".to_string(),
                        json!(stats.read_data_time_ms),
                    );
                }
            }
            "prepare" | "commit" => {
                if let Some(stats) = stats {
                    root.insert(
                        "NumberTotalRows".to_string(),
                        json!(stats.number_total_rows),
                    );
                    root.insert(
                        "NumberLoadedRows".to_string(),
                        json!(stats.number_loaded_rows),
                    );
                    root.insert(
                        "NumberFilteredRows".to_string(),
                        json!(stats.number_filtered_rows),
                    );
                    root.insert(
                        "NumberUnselectedRows".to_string(),
                        json!(stats.number_unselected_rows),
                    );
                    root.insert("LoadBytes".to_string(), json!(stats.load_bytes));
                    root.insert("LoadTimeMs".to_string(), json!(stats.load_time_ms));
                    root.insert(
                        "StreamLoadPlanTimeMs".to_string(),
                        json!(stats.stream_load_plan_time_ms),
                    );
                    root.insert(
                        "ReceivedDataTimeMs".to_string(),
                        json!(stats.read_data_time_ms),
                    );
                    root.insert(
                        "WriteDataTimeMs".to_string(),
                        json!(stats.write_data_time_ms),
                    );
                    root.insert(
                        "CommitAndPublishTimeMs".to_string(),
                        json!(stats.commit_and_publish_time_ms),
                    );
                }
            }
            _ => {}
        }
    }
    Value::Object(root)
}

fn stream_load_response(
    code: TStatusCode,
    message: &str,
    db: &str,
    table: &str,
    label: &str,
    txn_id: i64,
    stats: &LoadStats,
    existing_job_status: Option<&str>,
) -> Value {
    let mut root = Map::new();
    root.insert("TxnId".to_string(), json!(txn_id));
    root.insert("Label".to_string(), json!(label));
    root.insert("Db".to_string(), json!(db));
    root.insert("Table".to_string(), json!(table));
    root.insert("Status".to_string(), json!(stream_load_status_text(code)));
    if code == TStatusCode::LABEL_ALREADY_EXISTS
        && let Some(job_status) = existing_job_status
    {
        root.insert("ExistingJobStatus".to_string(), json!(job_status));
    }
    if code == TStatusCode::OK {
        root.insert("Message".to_string(), json!("OK"));
    } else {
        root.insert("Message".to_string(), json!(message));
    }
    root.insert(
        "NumberTotalRows".to_string(),
        json!(stats.number_total_rows),
    );
    root.insert(
        "NumberLoadedRows".to_string(),
        json!(stats.number_loaded_rows),
    );
    root.insert(
        "NumberFilteredRows".to_string(),
        json!(stats.number_filtered_rows),
    );
    root.insert(
        "NumberUnselectedRows".to_string(),
        json!(stats.number_unselected_rows),
    );
    root.insert("LoadBytes".to_string(), json!(stats.load_bytes));
    root.insert("LoadTimeMs".to_string(), json!(stats.load_time_ms));
    root.insert("BeginTxnTimeMs".to_string(), json!(stats.begin_txn_time_ms));
    root.insert(
        "StreamLoadPlanTimeMs".to_string(),
        json!(stats.stream_load_plan_time_ms),
    );
    root.insert("ReadDataTimeMs".to_string(), json!(stats.read_data_time_ms));
    root.insert(
        "WriteDataTimeMs".to_string(),
        json!(stats.write_data_time_ms),
    );
    root.insert(
        "CommitAndPublishTimeMs".to_string(),
        json!(stats.commit_and_publish_time_ms),
    );
    Value::Object(root)
}

pub(crate) fn handle_stream_load(
    db: String,
    table: String,
    headers: HttpHeaders,
    body: Vec<u8>,
) -> Value {
    let started = Instant::now();
    let auth = match parse_basic_auth(&headers) {
        Ok(v) => v,
        Err(e) => {
            return stream_load_response(
                e.code,
                &e.message,
                &db,
                &table,
                get_header(&headers, "label").unwrap_or(""),
                0,
                &LoadStats::default(),
                e.existing_job_status.as_deref(),
            );
        }
    };
    let options = match parse_load_headers(&headers).and_then(|opts| {
        ensure_txn_extensions_supported(&opts)?;
        Ok(opts)
    }) {
        Ok(v) => v,
        Err(e) => {
            return stream_load_response(
                e.code,
                &e.message,
                &db,
                &table,
                get_header(&headers, "label").unwrap_or(""),
                0,
                &LoadStats::default(),
                e.existing_job_status.as_deref(),
            );
        }
    };
    let label = options.label.clone().unwrap_or_else(generate_label);
    let mut stats = LoadStats::default();
    let mut txn_id = 0_i64;
    let mut begin_ok = false;
    let mut temp_path: Option<PathBuf> = None;
    let mut stream_load_id: Option<TUniqueId> = None;
    let mut finst_id: Option<crate::common::types::UniqueId> = None;
    let mut commit_infos: Vec<types::TTabletCommitInfo> = Vec::new();
    let mut fail_infos: Vec<types::TTabletFailInfo> = Vec::new();
    let mut final_error: Option<ApiError> = None;

    let work = (|| -> Result<(), ApiError> {
        let begin_started = Instant::now();
        let begin_result = begin_txn(
            &auth,
            &db,
            &table,
            &label,
            options.timeout,
            options.warehouse.as_deref(),
        )?;
        stats.begin_txn_time_ms = begin_started.elapsed().as_millis() as i64;
        if begin_result.status.status_code == TStatusCode::LABEL_ALREADY_EXISTS {
            return Err(api_error_from_status(&begin_result.status)
                .with_existing_job_status(begin_result.job_status));
        }
        ensure_ok_status(&begin_result.status)?;
        txn_id = begin_result.txn_id.ok_or_else(|| {
            ApiError::new(
                TStatusCode::INTERNAL_ERROR,
                "missing txn id from loadTxnBegin",
            )
        })?;
        begin_ok = true;

        let (path, read_ms) = write_request_body_to_temp_file(&body)?;
        stats.read_data_time_ms = read_ms;
        temp_path = Some(path.clone());
        let load_id = random_unique_id();
        register_stream_load_file(&load_id, &path);
        stream_load_id = Some(load_id.clone());

        let put_request = build_put_request(
            &auth,
            &db,
            &table,
            txn_id,
            load_id,
            path.to_string_lossy().as_ref(),
            &options,
            options.warehouse.as_deref(),
        )?;
        let plan_started = Instant::now();
        let put_result = stream_load_put(put_request)?;
        stats.stream_load_plan_time_ms = plan_started.elapsed().as_millis() as i64;
        ensure_ok_status(&put_result.status)?;
        let plan_params = put_result.params.ok_or_else(|| {
            ApiError::new(
                TStatusCode::INTERNAL_ERROR,
                "streamLoadPut succeeded but returned no exec params",
            )
        })?;

        let execute_started = Instant::now();
        let execute_result =
            internal_service::execute_plan_fragment_sync(plan_params).map_err(|e| {
                ApiError::new(
                    TStatusCode::RUNTIME_ERROR,
                    format!("execute plan fragment failed: {e}"),
                )
            })?;
        stats.write_data_time_ms = execute_started.elapsed().as_millis() as i64;
        finst_id = Some(execute_result.finst_id);
        commit_infos = sink_commit::list_tablet_commit_infos(execute_result.finst_id);
        fail_infos = sink_commit::list_tablet_fail_infos(execute_result.finst_id);
        let (loaded_rows, _loaded_bytes) = sink_commit::get_load_counters(execute_result.finst_id);
        let loaded_rows = loaded_rows.max(0);
        stats.number_loaded_rows = stats.number_loaded_rows.saturating_add(loaded_rows);
        stats.number_total_rows = stats.number_total_rows.saturating_add(loaded_rows);

        stats.load_bytes = body.len() as i64;
        let commit_started = Instant::now();
        let commit_result = commit_txn(
            &auth,
            &db,
            &table,
            txn_id,
            commit_infos.clone(),
            fail_infos.clone(),
            options.prepared_timeout,
            false,
            Some(&stats),
        )?;
        stats.commit_and_publish_time_ms = commit_started.elapsed().as_millis() as i64;
        ensure_ok_status(&commit_result.status)?;
        stats.load_time_ms = started.elapsed().as_millis() as i64;
        Ok(())
    })();

    if let Some(path) = temp_path {
        let _ = std::fs::remove_file(path);
    }
    if let Some(load_id) = stream_load_id.as_ref() {
        unregister_stream_load_file(load_id);
    }
    if let Some(finst_id) = finst_id {
        sink_commit::unregister(finst_id);
    }
    if let Err(err) = work {
        final_error = Some(err);
    }
    if let Some(err) = final_error {
        if begin_ok {
            let _ = rollback_txn(
                &auth,
                &db,
                &table,
                txn_id,
                &err.message,
                commit_infos,
                fail_infos,
            );
        }
        stats.load_time_ms = started.elapsed().as_millis() as i64;
        return stream_load_response(
            err.code,
            &err.message,
            &db,
            &table,
            &label,
            txn_id,
            &stats,
            err.existing_job_status.as_deref(),
        );
    }
    stream_load_response(
        TStatusCode::OK,
        "OK",
        &db,
        &table,
        &label,
        txn_id,
        &stats,
        None,
    )
}

pub(crate) fn handle_transaction_load(headers: HttpHeaders, body: Vec<u8>) -> Value {
    let options = match parse_load_headers(&headers).and_then(|opts| {
        ensure_txn_extensions_supported(&opts)?;
        Ok(opts)
    }) {
        Ok(v) => v,
        Err(e) => {
            let (db, table) = (
                get_header(&headers, "db").unwrap_or("").to_string(),
                get_header(&headers, "table").unwrap_or("").to_string(),
            );
            return txn_response(
                "load",
                e.code,
                &e.message,
                &db,
                &table,
                get_header(&headers, "label").unwrap_or(""),
                None,
                None,
                None,
                e.existing_job_status.as_deref(),
            );
        }
    };
    let label = match parse_required_label(&options) {
        Ok(v) => v,
        Err(e) => {
            let (db, table) = (
                get_header(&headers, "db").unwrap_or("").to_string(),
                get_header(&headers, "table").unwrap_or("").to_string(),
            );
            return txn_response(
                "load", e.code, &e.message, &db, &table, "", None, None, None, None,
            );
        }
    };
    let (db, table) = match parse_db_table_from_headers(&headers) {
        Ok(v) => v,
        Err(e) => {
            return txn_response(
                "load", e.code, &e.message, "", "", &label, None, None, None, None,
            );
        }
    };

    let result = with_txn_context_for_update(&label, |ctx| {
        if ctx.db != db {
            return Err(ApiError::new(
                TStatusCode::INVALID_ARGUMENT,
                format!(
                    "request db `{db}` does not match transaction db `{}`",
                    ctx.db
                ),
            ));
        }
        if ctx.table != table {
            return Err(ApiError::new(
                TStatusCode::INVALID_ARGUMENT,
                format!(
                    "request table `{table}` does not match transaction table `{}`",
                    ctx.table
                ),
            ));
        }

        let (temp_path, read_ms) = write_request_body_to_temp_file(&body)?;
        let mut finst_id: Option<crate::common::types::UniqueId> = None;
        let mut stream_load_id: Option<TUniqueId> = None;
        let load_work = (|| -> Result<(), ApiError> {
            let load_id = random_unique_id();
            register_stream_load_file(&load_id, &temp_path);
            stream_load_id = Some(load_id.clone());
            let put_request = build_put_request(
                &ctx.auth,
                &ctx.db,
                &ctx.table,
                ctx.txn_id,
                load_id,
                temp_path.to_string_lossy().as_ref(),
                &options,
                ctx.warehouse.as_deref().or(options.warehouse.as_deref()),
            )?;
            let plan_started = Instant::now();
            let put_result = stream_load_put(put_request)?;
            let plan_ms = plan_started.elapsed().as_millis() as i64;
            ensure_ok_status(&put_result.status)?;
            let plan_params = put_result.params.ok_or_else(|| {
                ApiError::new(
                    TStatusCode::INTERNAL_ERROR,
                    "streamLoadPut succeeded but returned no exec params",
                )
            })?;
            let execute_started = Instant::now();
            let execute_result = internal_service::execute_plan_fragment_sync(plan_params)
                .map_err(|e| {
                    ApiError::new(
                        TStatusCode::RUNTIME_ERROR,
                        format!("execute plan fragment failed: {e}"),
                    )
                })?;
            let write_ms = execute_started.elapsed().as_millis() as i64;
            finst_id = Some(execute_result.finst_id);
            let commit_infos = sink_commit::list_tablet_commit_infos(execute_result.finst_id);
            let fail_infos = sink_commit::list_tablet_fail_infos(execute_result.finst_id);
            let (loaded_rows, _loaded_bytes) =
                sink_commit::get_load_counters(execute_result.finst_id);
            dedup_extend_commit_infos(&mut ctx.commit_infos, commit_infos);
            dedup_extend_fail_infos(&mut ctx.fail_infos, fail_infos);
            let loaded_rows = loaded_rows.max(0);
            ctx.stats.number_loaded_rows = ctx.stats.number_loaded_rows.saturating_add(loaded_rows);
            ctx.stats.number_total_rows = ctx.stats.number_total_rows.saturating_add(loaded_rows);
            ctx.stats.read_data_time_ms += read_ms;
            ctx.stats.stream_load_plan_time_ms += plan_ms;
            ctx.stats.write_data_time_ms += write_ms;
            ctx.stats.load_bytes += body.len() as i64;
            ctx.stats.load_time_ms += read_ms + plan_ms + write_ms;
            ctx.last_active = Instant::now();
            if let Some(channel_id) = options.channel_id {
                ctx.finished_channels.insert(channel_id);
            }
            Ok(())
        })();

        let _ = std::fs::remove_file(&temp_path);
        if let Some(load_id) = stream_load_id.as_ref() {
            unregister_stream_load_file(load_id);
        }
        if let Some(finst_id) = finst_id {
            sink_commit::unregister(finst_id);
        }
        load_work?;
        Ok(())
    });

    match result {
        Ok(()) => {
            let (txn_id, stats) =
                with_txn_context_for_update(&label, |ctx| Ok((ctx.txn_id, ctx.stats.clone())))
                    .unwrap_or((0, LoadStats::default()));
            txn_response(
                "load",
                TStatusCode::OK,
                "OK",
                &db,
                &table,
                &label,
                Some(txn_id),
                Some(&stats),
                None,
                None,
            )
        }
        Err(err) => txn_response(
            "load",
            err.code,
            &err.message,
            &db,
            &table,
            &label,
            None,
            None,
            None,
            err.existing_job_status.as_deref(),
        ),
    }
}

fn handle_transaction_begin(headers: &HttpHeaders, options: LoadHeaderOptions) -> Value {
    let label = match parse_required_label(&options) {
        Ok(v) => v,
        Err(e) => {
            return txn_response(
                "begin", e.code, &e.message, "", "", "", None, None, None, None,
            );
        }
    };
    let (db, table) = match parse_db_table_from_headers(headers) {
        Ok(v) => v,
        Err(e) => {
            return txn_response(
                "begin", e.code, &e.message, "", "", &label, None, None, None, None,
            );
        }
    };
    let auth = match parse_basic_auth(headers) {
        Ok(v) => v,
        Err(e) => {
            return txn_response(
                "begin", e.code, &e.message, &db, &table, &label, None, None, None, None,
            );
        }
    };

    {
        let contexts = txn_contexts().lock().expect("txn context store lock");
        if contexts.contains_key(&label) {
            return txn_response(
                "begin",
                TStatusCode::LABEL_ALREADY_EXISTS,
                "label already exists",
                &db,
                &table,
                &label,
                None,
                None,
                None,
                Some("RUNNING"),
            );
        }
    }

    let begin_started = Instant::now();
    let begin_result = match begin_txn(
        &auth,
        &db,
        &table,
        &label,
        options.timeout,
        options.warehouse.as_deref(),
    ) {
        Ok(v) => v,
        Err(e) => {
            return txn_response(
                "begin",
                e.code,
                &e.message,
                &db,
                &table,
                &label,
                None,
                None,
                None,
                e.existing_job_status.as_deref(),
            );
        }
    };
    if begin_result.status.status_code == TStatusCode::LABEL_ALREADY_EXISTS {
        return txn_response(
            "begin",
            TStatusCode::LABEL_ALREADY_EXISTS,
            &status_message(&begin_result.status),
            &db,
            &table,
            &label,
            None,
            None,
            None,
            begin_result.job_status.as_deref(),
        );
    }
    if let Err(err) = ensure_ok_status(&begin_result.status) {
        return txn_response(
            "begin",
            err.code,
            &err.message,
            &db,
            &table,
            &label,
            None,
            None,
            None,
            err.existing_job_status.as_deref(),
        );
    }
    let txn_id = match begin_result.txn_id {
        Some(v) => v,
        None => {
            return txn_response(
                "begin",
                TStatusCode::INTERNAL_ERROR,
                "missing txn id from loadTxnBegin",
                &db,
                &table,
                &label,
                None,
                None,
                None,
                None,
            );
        }
    };
    let context = TxnContext {
        label: label.clone(),
        db: db.clone(),
        table: table.clone(),
        auth,
        warehouse: options.warehouse.clone(),
        txn_id,
        timeout: options.timeout,
        prepared_timeout: options.prepared_timeout,
        idle_timeout: options.idle_transaction_timeout,
        commit_infos: Vec::new(),
        fail_infos: Vec::new(),
        stats: LoadStats::default(),
        created_at: Instant::now(),
        last_active: Instant::now(),
        finished_channels: HashSet::new(),
    };
    let mut contexts = txn_contexts().lock().expect("txn context store lock");
    if contexts.contains_key(&label) {
        let _ = rollback_txn(
            &context.auth,
            &context.db,
            &context.table,
            context.txn_id,
            "duplicate label",
            Vec::new(),
            Vec::new(),
        );
        return txn_response(
            "begin",
            TStatusCode::LABEL_ALREADY_EXISTS,
            "label already exists",
            &db,
            &table,
            &label,
            None,
            None,
            None,
            Some("RUNNING"),
        );
    }
    contexts.insert(label.clone(), Arc::new(Mutex::new(context)));
    drop(contexts);
    txn_response(
        "begin",
        TStatusCode::OK,
        "OK",
        &db,
        &table,
        &label,
        Some(txn_id),
        None,
        Some(begin_started.elapsed().as_millis() as i64),
        None,
    )
}

fn handle_transaction_prepare_or_commit(
    headers: &HttpHeaders,
    options: &LoadHeaderOptions,
    prepare_only: bool,
) -> Value {
    let op = if prepare_only { "prepare" } else { "commit" };
    let label = match parse_required_label(options) {
        Ok(v) => v,
        Err(e) => {
            return txn_response(op, e.code, &e.message, "", "", "", None, None, None, None);
        }
    };
    let (db, table) = match parse_db_table_from_headers(headers) {
        Ok(v) => v,
        Err(e) => {
            return txn_response(
                op, e.code, &e.message, "", "", &label, None, None, None, None,
            );
        }
    };
    let result = with_txn_context_for_update(&label, |ctx| {
        if ctx.db != db {
            return Err(ApiError::new(
                TStatusCode::INVALID_ARGUMENT,
                format!(
                    "request db `{db}` does not match transaction db `{}`",
                    ctx.db
                ),
            ));
        }
        if ctx.table != table {
            return Err(ApiError::new(
                TStatusCode::INVALID_ARGUMENT,
                format!(
                    "request table `{table}` does not match transaction table `{}`",
                    ctx.table
                ),
            ));
        }
        let prepared_timeout = options.prepared_timeout.or(ctx.prepared_timeout);
        let commit_started = Instant::now();
        let commit_result = commit_txn(
            &ctx.auth,
            &ctx.db,
            &ctx.table,
            ctx.txn_id,
            ctx.commit_infos.clone(),
            ctx.fail_infos.clone(),
            prepared_timeout,
            prepare_only,
            Some(&ctx.stats),
        )?;
        ensure_ok_status(&commit_result.status)?;
        ctx.stats.commit_and_publish_time_ms = commit_started.elapsed().as_millis() as i64;
        ctx.last_active = Instant::now();
        Ok((ctx.txn_id, ctx.stats.clone()))
    });

    match result {
        Ok((txn_id, stats)) => {
            if !prepare_only {
                let mut contexts = txn_contexts().lock().expect("txn context store lock");
                contexts.remove(&label);
            }
            txn_response(
                op,
                TStatusCode::OK,
                "OK",
                &db,
                &table,
                &label,
                Some(txn_id),
                Some(&stats),
                None,
                None,
            )
        }
        Err(err) => txn_response(
            op,
            err.code,
            &err.message,
            &db,
            &table,
            &label,
            None,
            None,
            None,
            err.existing_job_status.as_deref(),
        ),
    }
}

fn handle_transaction_rollback(headers: &HttpHeaders, options: &LoadHeaderOptions) -> Value {
    let label = match parse_required_label(options) {
        Ok(v) => v,
        Err(e) => {
            return txn_response(
                "rollback", e.code, &e.message, "", "", "", None, None, None, None,
            );
        }
    };
    let requested_db = get_header(headers, "db").map(ToString::to_string);
    let requested_table = get_header(headers, "table").map(ToString::to_string);
    let result = with_txn_context_for_update(&label, |ctx| {
        if let Some(db) = requested_db.as_deref() {
            if ctx.db != db {
                return Err(ApiError::new(
                    TStatusCode::INVALID_ARGUMENT,
                    format!(
                        "request db `{db}` does not match transaction db `{}`",
                        ctx.db
                    ),
                ));
            }
        }
        if let Some(table) = requested_table.as_deref() {
            if ctx.table != table {
                return Err(ApiError::new(
                    TStatusCode::INVALID_ARGUMENT,
                    format!(
                        "request table `{table}` does not match transaction table `{}`",
                        ctx.table
                    ),
                ));
            }
        }
        let rollback_result = rollback_txn(
            &ctx.auth,
            &ctx.db,
            &ctx.table,
            ctx.txn_id,
            "rollback requested",
            ctx.commit_infos.clone(),
            ctx.fail_infos.clone(),
        )?;
        ensure_ok_status(&rollback_result.status)?;
        Ok(ctx.txn_id)
    });

    match result {
        Ok(_txn_id) => {
            let mut contexts = txn_contexts().lock().expect("txn context store lock");
            contexts.remove(&label);
            txn_response(
                "rollback",
                TStatusCode::OK,
                "",
                "",
                "",
                &label,
                None,
                None,
                None,
                None,
            )
        }
        Err(err) => txn_response(
            "rollback",
            err.code,
            &err.message,
            requested_db.as_deref().unwrap_or(""),
            requested_table.as_deref().unwrap_or(""),
            &label,
            None,
            None,
            None,
            err.existing_job_status.as_deref(),
        ),
    }
}

fn handle_transaction_list() -> Value {
    let contexts = {
        let guard = txn_contexts().lock().expect("txn context store lock");
        guard.values().cloned().collect::<Vec<_>>()
    };
    let mut entries = Vec::with_capacity(contexts.len());
    for context in contexts {
        match context.try_lock() {
            Ok(ctx) => {
                let status = if ctx.is_idle_expired() {
                    "TXN_NOT_EXISTS".to_string()
                } else {
                    "OK".to_string()
                };
                entries.push(json!({
                    "Status": status,
                    "InProcessing": false,
                    "Label": ctx.label,
                    "TxnId": ctx.txn_id,
                    "NumberTotalRows": ctx.stats.number_total_rows,
                    "NumberLoadedRows": ctx.stats.number_loaded_rows,
                    "NumberFilteredRows": ctx.stats.number_filtered_rows,
                    "NumberUnselectedRows": ctx.stats.number_unselected_rows,
                    "LoadBytes": ctx.stats.load_bytes,
                    "StreamLoadPlanTimeMs": ctx.stats.stream_load_plan_time_ms,
                    "ReceivedDataTimeMs": ctx.stats.read_data_time_ms,
                    "WriteDataTimeMs": ctx.stats.write_data_time_ms,
                    "BeginTimestamp": ctx.created_at.elapsed().as_millis() as i64,
                    "LastActiveTimestamp": ctx.last_active.elapsed().as_millis() as i64,
                    "TransactionTimeoutSec": ctx.timeout.unwrap_or_default(),
                    "IdleTransactionTimeoutSec": ctx.idle_timeout.unwrap_or_default(),
                }));
            }
            Err(_) => entries.push(json!({
                "InProcessing": true,
            })),
        }
    }
    Value::Array(entries)
}

pub(crate) fn handle_transaction_op(txn_op: String, headers: HttpHeaders) -> Value {
    let txn_op = txn_op.to_ascii_lowercase();
    let options = match parse_load_headers(&headers).and_then(|opts| {
        ensure_txn_extensions_supported(&opts)?;
        Ok(opts)
    }) {
        Ok(v) => v,
        Err(e) => {
            return txn_response(
                &txn_op,
                e.code,
                &e.message,
                get_header(&headers, "db").unwrap_or(""),
                get_header(&headers, "table").unwrap_or(""),
                get_header(&headers, "label").unwrap_or(""),
                None,
                None,
                None,
                e.existing_job_status.as_deref(),
            );
        }
    };
    match txn_op.as_str() {
        "begin" => handle_transaction_begin(&headers, options),
        "prepare" => handle_transaction_prepare_or_commit(&headers, &options, true),
        "commit" => handle_transaction_prepare_or_commit(&headers, &options, false),
        "rollback" => handle_transaction_rollback(&headers, &options),
        "list" => handle_transaction_list(),
        other => txn_response(
            other,
            TStatusCode::INVALID_ARGUMENT,
            &format!("unsupported transaction operation `{other}`"),
            get_header(&headers, "db").unwrap_or(""),
            get_header(&headers, "table").unwrap_or(""),
            get_header(&headers, "label").unwrap_or(""),
            None,
            None,
            None,
            None,
        ),
    }
}

pub(crate) fn finish_stream_load_channel(
    label: Option<&str>,
    table_name: Option<&str>,
    channel_id: Option<i32>,
) -> TStatus {
    let Some(label) = label.map(str::trim).filter(|v| !v.is_empty()) else {
        return TStatus::new(
            TStatusCode::TXN_NOT_EXISTS,
            Some(vec!["missing stream load label".to_string()]),
        );
    };
    let Some(table_name) = table_name.map(str::trim).filter(|v| !v.is_empty()) else {
        return TStatus::new(
            TStatusCode::TXN_NOT_EXISTS,
            Some(vec!["missing stream load table name".to_string()]),
        );
    };
    let context = {
        let contexts = txn_contexts().lock().expect("txn context store lock");
        contexts.get(label).cloned()
    };
    let Some(context) = context else {
        return TStatus::new(
            TStatusCode::TXN_NOT_EXISTS,
            Some(vec![format!(
                "stream load transaction `{label}` does not exist"
            )]),
        );
    };
    let mut context = context.lock().expect("txn context lock");
    if context.table != table_name {
        return TStatus::new(
            TStatusCode::TXN_NOT_EXISTS,
            Some(vec![format!(
                "stream load transaction `{label}` does not match table `{table_name}`"
            )]),
        );
    }
    if let Some(channel_id) = channel_id {
        context.finished_channels.insert(channel_id);
    }
    context.last_active = Instant::now();
    TStatus::new(TStatusCode::OK, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    fn test_guard() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("test lock")
    }

    fn test_headers(pairs: &[(&str, &str)]) -> HttpHeaders {
        let mut headers = HttpHeaders::new();
        for (k, v) in pairs {
            headers.insert((*k).to_string(), (*v).to_string());
        }
        headers
    }

    fn clear_contexts() {
        let mut contexts = txn_contexts().lock().expect("txn context store lock");
        contexts.clear();
    }

    #[test]
    fn parse_basic_auth_success() {
        let _guard = test_guard();
        let encoded = base64::engine::general_purpose::STANDARD.encode("alice:secret");
        let headers = test_headers(&[("authorization", &format!("Basic {encoded}"))]);
        let auth = parse_basic_auth(&headers).expect("parse auth");
        assert_eq!(auth.user, "alice");
        assert_eq!(auth.passwd, "secret");
    }

    #[test]
    fn parse_basic_auth_invalid_payload() {
        let _guard = test_guard();
        let headers = test_headers(&[("authorization", "Basic not_base64")]);
        let err = parse_basic_auth(&headers).expect_err("should fail");
        assert_eq!(err.code, TStatusCode::INVALID_ARGUMENT);
    }

    #[test]
    fn parse_headers_reject_conflicts() {
        let _guard = test_guard();
        let headers = test_headers(&[
            ("partitions", "p1"),
            ("temporary_partitions", "p2"),
            ("format", "csv"),
        ]);
        let err = parse_load_headers(&headers).expect_err("should reject partition conflict");
        assert_eq!(err.code, TStatusCode::INVALID_ARGUMENT);

        let headers = test_headers(&[
            ("compression", "gzip"),
            ("content-encoding", "gzip"),
            ("format", "csv"),
        ]);
        let err = parse_load_headers(&headers).expect_err("should reject compression conflict");
        assert_eq!(err.code, TStatusCode::INVALID_ARGUMENT);
    }

    #[test]
    fn parse_headers_reject_invalid_bool_and_number() {
        let _guard = test_guard();
        let headers = test_headers(&[("strict_mode", "maybe"), ("format", "csv")]);
        let err = parse_load_headers(&headers).expect_err("should reject bool");
        assert_eq!(err.code, TStatusCode::INVALID_ARGUMENT);

        let headers = test_headers(&[("skip_header", "x"), ("format", "csv")]);
        let err = parse_load_headers(&headers).expect_err("should reject number");
        assert_eq!(err.code, TStatusCode::INVALID_ARGUMENT);
    }

    #[test]
    fn rollback_without_table_header_is_not_rejected() {
        let _guard = test_guard();
        clear_contexts();
        let encoded = base64::engine::general_purpose::STANDARD.encode("root:");
        let headers = test_headers(&[
            ("authorization", &format!("Basic {encoded}")),
            ("db", "db1"),
            ("label", "missing_txn"),
        ]);
        let response = handle_transaction_op("rollback".to_string(), headers);
        assert_eq!(response["Status"], "TXN_NOT_EXISTS");
    }

    #[test]
    fn txn_response_includes_existing_job_status() {
        let _guard = test_guard();
        let resp = txn_response(
            "begin",
            TStatusCode::LABEL_ALREADY_EXISTS,
            "label exists",
            "db1",
            "tbl1",
            "label_1",
            None,
            None,
            None,
            Some("RUNNING"),
        );
        assert_eq!(resp["Status"], "LABEL_ALREADY_EXISTS");
        assert_eq!(resp["ExistingJobStatus"], "RUNNING");
    }

    #[test]
    fn context_state_machine_duplicate_in_processing_and_timeout() {
        let _guard = test_guard();
        clear_contexts();

        let context = TxnContext {
            label: "label_1".to_string(),
            db: "db1".to_string(),
            table: "tbl1".to_string(),
            auth: BasicAuth {
                user: "u".to_string(),
                passwd: "p".to_string(),
                user_ip: None,
            },
            warehouse: None,
            txn_id: 100,
            timeout: None,
            prepared_timeout: None,
            idle_timeout: Some(1),
            commit_infos: Vec::new(),
            fail_infos: Vec::new(),
            stats: LoadStats::default(),
            created_at: Instant::now(),
            last_active: Instant::now() - Duration::from_secs(3),
            finished_channels: HashSet::new(),
        };
        {
            let mut contexts = txn_contexts().lock().expect("txn context store lock");
            contexts.insert("label_1".to_string(), Arc::new(Mutex::new(context)));
        }

        let duplicate = {
            let contexts = txn_contexts().lock().expect("txn context store lock");
            contexts.contains_key("label_1")
        };
        assert!(duplicate);

        let context_arc = {
            let contexts = txn_contexts().lock().expect("txn context store lock");
            contexts.get("label_1").cloned().expect("context")
        };
        let lock_guard = context_arc.lock().expect("ctx lock");
        let processing_err = with_txn_context_for_update("label_1", |_ctx| Ok(()))
            .expect_err("lock should fail while in processing");
        assert_eq!(processing_err.code, TStatusCode::TXN_IN_PROCESSING);
        drop(lock_guard);

        let timeout_err = with_txn_context_for_update("label_1", |_ctx| Ok(()))
            .expect_err("context should be expired");
        assert_eq!(timeout_err.code, TStatusCode::TXN_NOT_EXISTS);

        let unknown_err = with_txn_context_for_update("label_not_exists", |_ctx| Ok(()))
            .expect_err("unknown label");
        assert_eq!(unknown_err.code, TStatusCode::TXN_NOT_EXISTS);
    }
}
