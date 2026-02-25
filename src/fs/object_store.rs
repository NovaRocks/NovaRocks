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
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::net::IpAddr;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use opendal::Operator;
use opendal::layers::{RetryInterceptor, RetryLayer, TimeoutLayer};

use crate::novarocks_config::config as novarocks_app_config;
use crate::novarocks_logging::{debug, warn};

const DEFAULT_OSS_RETRY_MAX_TIMES: usize = 6;
const DEFAULT_OSS_RETRY_MIN_DELAY_MS: u64 = 100;
const DEFAULT_OSS_RETRY_MAX_DELAY_MS: u64 = 2_000;
const DEFAULT_RETRY_LOG_SUMMARY_INTERVAL_MS: u64 = 30_000;
const DEFAULT_RETRY_LOG_FIRST_N: u32 = 3;
const DEFAULT_RETRY_LOG_MIN_INTERVAL_MS: u64 = 1_000;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RetryLogControl {
    summary_interval_ms: u64,
    first_n: u32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectStoreRetrySettings {
    pub retry_max_times: Option<usize>,
    pub retry_min_delay_ms: Option<u64>,
    pub retry_max_delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub io_timeout_ms: Option<u64>,
}

impl ObjectStoreRetrySettings {
    pub fn from_aws_s3_props<S>(props: Option<&BTreeMap<S, S>>) -> Self
    where
        S: Borrow<str> + Ord,
    {
        let Some(props) = props else {
            return Self::default();
        };
        let mut settings = Self::default();
        if let Some((key, value)) =
            first_nonempty_property(props, &["aws.s3.max_retries", "aws.s3.retry_max_times"])
        {
            settings.retry_max_times = parse_usize_property(&key, value);
        }
        if let Some((key, value)) = first_nonempty_property(props, &["aws.s3.retry_min_delay_ms"]) {
            settings.retry_min_delay_ms = parse_u64_property(&key, value);
        }
        if let Some((key, value)) = first_nonempty_property(props, &["aws.s3.retry_max_delay_ms"]) {
            settings.retry_max_delay_ms = parse_u64_property(&key, value);
        }
        if let Some((key, value)) =
            first_nonempty_property(props, &["aws.s3.request_timeout_ms", "aws.s3.timeout_ms"])
        {
            settings.timeout_ms = parse_u64_property(&key, value);
        }
        if let Some((key, value)) = first_nonempty_property(props, &["aws.s3.io_timeout_ms"]) {
            settings.io_timeout_ms = parse_u64_property(&key, value);
        }
        settings
    }

    fn from_runtime_config() -> Self {
        novarocks_app_config()
            .ok()
            .map(|cfg| Self {
                retry_max_times: cfg.runtime.object_storage.retry_max_times,
                retry_min_delay_ms: cfg.runtime.object_storage.retry_min_delay_ms,
                retry_max_delay_ms: cfg.runtime.object_storage.retry_max_delay_ms,
                timeout_ms: cfg.runtime.object_storage.timeout_ms,
                io_timeout_ms: cfg.runtime.object_storage.io_timeout_ms,
            })
            .unwrap_or_default()
    }

    fn apply_if_absent(&self, cfg: &mut ObjectStoreConfig) {
        if cfg.retry_max_times.is_none() {
            cfg.retry_max_times = self.retry_max_times;
        }
        if cfg.retry_min_delay_ms.is_none() {
            cfg.retry_min_delay_ms = self.retry_min_delay_ms;
        }
        if cfg.retry_max_delay_ms.is_none() {
            cfg.retry_max_delay_ms = self.retry_max_delay_ms;
        }
        if cfg.timeout_ms.is_none() {
            cfg.timeout_ms = self.timeout_ms;
        }
        if cfg.io_timeout_ms.is_none() {
            cfg.io_timeout_ms = self.io_timeout_ms;
        }
    }
}

pub fn apply_object_store_runtime_defaults(cfg: &mut ObjectStoreConfig) {
    ObjectStoreRetrySettings::from_runtime_config().apply_if_absent(cfg);
}

fn first_nonempty_property<'a, S>(
    props: &'a BTreeMap<S, S>,
    keys: &[&str],
) -> Option<(String, &'a str)>
where
    S: Borrow<str> + Ord,
{
    for key in keys {
        if let Some(value) = props
            .get(*key)
            .map(|v| v.borrow().trim())
            .filter(|v| !v.is_empty())
        {
            return Some(((*key).to_string(), value));
        }
    }
    None
}

fn parse_u64_property(key: &str, value: &str) -> Option<u64> {
    match value.parse::<u64>() {
        Ok(v) => Some(v),
        Err(err) => {
            warn!(
                "ignore invalid object store property {}='{}': {}",
                key, value, err
            );
            None
        }
    }
}

fn parse_usize_property(key: &str, value: &str) -> Option<usize> {
    match value.parse::<usize>() {
        Ok(v) => Some(v),
        Err(err) => {
            warn!(
                "ignore invalid object store property {}='{}': {}",
                key, value, err
            );
            None
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ObjectStoreConfig {
    pub endpoint: String,
    pub bucket: String,
    pub root: String,
    pub access_key_id: String,
    pub access_key_secret: String,
    pub session_token: Option<String>,
    pub enable_path_style_access: Option<bool>,
    pub region: Option<String>,
    pub retry_max_times: Option<usize>,
    pub retry_min_delay_ms: Option<u64>,
    pub retry_max_delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub io_timeout_ms: Option<u64>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct OssOperatorCacheKey {
    endpoint: String,
    bucket: String,
    root: String,
    access_key_id: String,
    access_key_secret: String,
    session_token: Option<String>,
    enable_path_style_access: Option<bool>,
    region: Option<String>,
    retry_max_times: Option<usize>,
    retry_min_delay_ms: Option<u64>,
    retry_max_delay_ms: Option<u64>,
    timeout_ms: Option<u64>,
    io_timeout_ms: Option<u64>,
}

impl OssOperatorCacheKey {
    fn from_config(cfg: &ObjectStoreConfig) -> Self {
        Self {
            endpoint: cfg.endpoint.clone(),
            bucket: cfg.bucket.clone(),
            root: cfg.root.clone(),
            access_key_id: cfg.access_key_id.clone(),
            access_key_secret: cfg.access_key_secret.clone(),
            session_token: cfg.session_token.clone(),
            enable_path_style_access: cfg.enable_path_style_access,
            region: cfg.region.clone(),
            retry_max_times: cfg.retry_max_times,
            retry_min_delay_ms: cfg.retry_min_delay_ms,
            retry_max_delay_ms: cfg.retry_max_delay_ms,
            timeout_ms: cfg.timeout_ms,
            io_timeout_ms: cfg.io_timeout_ms,
        }
    }
}

static OSS_OPERATOR_CACHE: OnceLock<Mutex<HashMap<OssOperatorCacheKey, Operator>>> =
    OnceLock::new();
static OSS_RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();
static RETRY_WARNING_STATE: OnceLock<Mutex<HashMap<RetryWarningKey, RetryWarningWindow>>> =
    OnceLock::new();
static RETRY_LOG_CONTROL: OnceLock<RetryLogControl> = OnceLock::new();

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RetryWarningKey {
    endpoint: String,
    bucket: String,
    stage: String,
    reason: String,
}

#[derive(Clone, Debug)]
struct RetryWarningWindow {
    window_start: Instant,
    emitted_in_window: u32,
    suppressed_in_window: u64,
    retries_in_window: u64,
    backoff_ms_in_window: u64,
    total_retries: u64,
}

impl RetryWarningWindow {
    fn new(now: Instant) -> Self {
        Self {
            window_start: now,
            emitted_in_window: 0,
            suppressed_in_window: 0,
            retries_in_window: 0,
            backoff_ms_in_window: 0,
            total_retries: 0,
        }
    }

    fn reset_window(&mut self, now: Instant) {
        self.window_start = now;
        self.emitted_in_window = 0;
        self.suppressed_in_window = 0;
        self.retries_in_window = 0;
        self.backoff_ms_in_window = 0;
    }
}

#[derive(Clone, Debug)]
struct ObjectStoreRetryInterceptor {
    endpoint: String,
    bucket: String,
}

impl ObjectStoreRetryInterceptor {
    fn from_config(cfg: &ObjectStoreConfig) -> Self {
        Self {
            endpoint: endpoint_host(&cfg.endpoint),
            bucket: cfg.bucket.clone(),
        }
    }
}

impl RetryInterceptor for ObjectStoreRetryInterceptor {
    fn intercept(&self, err: &opendal::Error, dur: Duration) {
        let control = retry_log_control();
        let err_text = err.to_string();
        let err_lower = err_text.to_ascii_lowercase();
        let stage = classify_retry_stage(&err_lower).to_string();
        let reason = classify_retry_reason(&err_lower).to_string();
        let detail = compact_retry_detail(&err_text);
        let now = Instant::now();
        let backoff_ms = clamp_u128_to_u64(dur.as_millis());
        let mut summary: Option<(u64, u64, u64, u64)> = None;
        let mut emit_detail = false;
        let total_retries;
        let key = RetryWarningKey {
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            stage: stage.clone(),
            reason: reason.clone(),
        };
        let Ok(mut guard) = retry_warning_state().lock() else {
            return;
        };
        let window = guard
            .entry(key)
            .or_insert_with(|| RetryWarningWindow::new(now));
        if clamp_u128_to_u64(now.duration_since(window.window_start).as_millis())
            >= control.summary_interval_ms
        {
            if window.suppressed_in_window > 0 {
                summary = Some((
                    window.suppressed_in_window,
                    window.retries_in_window,
                    window.backoff_ms_in_window,
                    window.total_retries,
                ));
            }
            window.reset_window(now);
        }
        window.retries_in_window = window.retries_in_window.saturating_add(1);
        window.backoff_ms_in_window = window.backoff_ms_in_window.saturating_add(backoff_ms);
        window.total_retries = window.total_retries.saturating_add(1);
        total_retries = window.total_retries;
        if window.emitted_in_window < control.first_n {
            window.emitted_in_window += 1;
            emit_detail = true;
        } else {
            window.suppressed_in_window = window.suppressed_in_window.saturating_add(1);
        }
        drop(guard);

        if let Some((suppressed, retries, backoff_sum_ms, total)) = summary {
            let avg_backoff_ms = if retries > 0 {
                backoff_sum_ms / retries
            } else {
                0
            };
            warn!(
                "object store retry summary: endpoint={} bucket={} stage={} reason={} suppressed={} retries={} avg_backoff_ms={} window_ms={} total_retries={}",
                self.endpoint,
                self.bucket,
                stage,
                reason,
                suppressed,
                retries,
                avg_backoff_ms,
                control.summary_interval_ms,
                total
            );
        }
        if emit_detail {
            warn!(
                "object store temporary error, will retry: endpoint={} bucket={} stage={} reason={} backoff_ms={} total_retries={} detail={}",
                self.endpoint, self.bucket, stage, reason, backoff_ms, total_retries, detail
            );
        }
    }
}

fn oss_operator_cache() -> &'static Mutex<HashMap<OssOperatorCacheKey, Operator>> {
    OSS_OPERATOR_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn retry_warning_state() -> &'static Mutex<HashMap<RetryWarningKey, RetryWarningWindow>> {
    RETRY_WARNING_STATE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn retry_log_control() -> RetryLogControl {
    *RETRY_LOG_CONTROL.get_or_init(|| {
        let mut control = RetryLogControl {
            summary_interval_ms: DEFAULT_RETRY_LOG_SUMMARY_INTERVAL_MS,
            first_n: DEFAULT_RETRY_LOG_FIRST_N,
        };
        if let Ok(cfg) = novarocks_app_config() {
            control.summary_interval_ms = cfg
                .runtime
                .object_storage
                .retry_log_summary_interval_ms
                .max(DEFAULT_RETRY_LOG_MIN_INTERVAL_MS);
            control.first_n = cfg.runtime.object_storage.retry_log_first_n.max(1);
        }
        control
    })
}

fn clamp_u128_to_u64(v: u128) -> u64 {
    u64::try_from(v).unwrap_or(u64::MAX)
}

fn classify_retry_stage(err_lower: &str) -> &'static str {
    if err_lower.contains("reader::read") {
        "reader_read"
    } else if err_lower.contains("writer::close") {
        "writer_close"
    } else if err_lower.contains("writer::write") || err_lower.contains(" at write") {
        "write"
    } else if err_lower.contains(" at read") {
        "read"
    } else if err_lower.contains(" at stat") {
        "stat"
    } else if err_lower.contains(" at list") {
        "list"
    } else {
        "other"
    }
}

fn classify_retry_reason(err_lower: &str) -> &'static str {
    if err_lower.contains("too little data") {
        "short_read"
    } else if err_lower.contains("timed out")
        || err_lower.contains("timeout")
        || err_lower.contains("deadline exceeded")
    {
        "timeout"
    } else if err_lower.contains("connection reset") {
        "connection_reset"
    } else if err_lower.contains("connection refused") {
        "connection_refused"
    } else if err_lower.contains("429") || err_lower.contains("too many requests") {
        "http_429"
    } else if err_lower.contains("503")
        || err_lower.contains("502")
        || err_lower.contains("500")
        || err_lower.contains("status code: 5")
    {
        "http_5xx"
    } else if err_lower.contains("error sending request") {
        "send_request"
    } else {
        "temporary"
    }
}

fn compact_retry_detail(err_text: &str) -> String {
    let compact = err_text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out = String::new();
    for (idx, ch) in compact.chars().enumerate() {
        if idx >= 200 {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

fn build_retry_layer(cfg: &ObjectStoreConfig) -> RetryLayer<ObjectStoreRetryInterceptor> {
    let max_times = cfg.retry_max_times.unwrap_or(DEFAULT_OSS_RETRY_MAX_TIMES);
    let min_delay_ms = cfg
        .retry_min_delay_ms
        .unwrap_or(DEFAULT_OSS_RETRY_MIN_DELAY_MS);
    let max_delay_ms = cfg
        .retry_max_delay_ms
        .unwrap_or(DEFAULT_OSS_RETRY_MAX_DELAY_MS)
        .max(min_delay_ms);

    RetryLayer::new()
        .with_notify(ObjectStoreRetryInterceptor::from_config(cfg))
        .with_jitter()
        .with_min_delay(Duration::from_millis(min_delay_ms))
        .with_max_delay(Duration::from_millis(max_delay_ms))
        .with_max_times(max_times)
}

fn build_timeout_layer(cfg: &ObjectStoreConfig) -> Option<TimeoutLayer> {
    if cfg.timeout_ms.is_none() && cfg.io_timeout_ms.is_none() {
        return None;
    }
    let mut layer = TimeoutLayer::new();
    if let Some(timeout_ms) = cfg.timeout_ms.filter(|v| *v > 0) {
        layer = layer.with_timeout(Duration::from_millis(timeout_ms));
    }
    if let Some(io_timeout_ms) = cfg.io_timeout_ms.filter(|v| *v > 0) {
        layer = layer.with_io_timeout(Duration::from_millis(io_timeout_ms));
    }
    Some(layer)
}

fn build_raw_object_store_operator(cfg: &ObjectStoreConfig) -> Result<Operator> {
    let endpoint = normalize_s3_endpoint(&cfg.endpoint)?;
    let local_endpoint = is_local_endpoint(&endpoint);
    let use_path_style = should_use_path_style(cfg);

    let mut builder = opendal::services::S3::default()
        .endpoint(&endpoint)
        .bucket(&cfg.bucket)
        .region(cfg.region.as_deref().unwrap_or("us-east-1"))
        .access_key_id(&cfg.access_key_id)
        .secret_access_key(&cfg.access_key_secret);
    if !use_path_style {
        builder = builder.enable_virtual_host_style();
    }
    if let Some(token) = cfg.session_token.as_deref() {
        builder = builder.session_token(token);
    }
    if local_endpoint {
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .context("build reqwest client without proxy for local object endpoint")?;
        builder = builder.http_client(opendal::raw::HttpClient::with(client));
    }
    if !cfg.root.is_empty() {
        builder = builder.root(&cfg.root);
    }
    let init_context = if use_path_style {
        "init opendal s3 operator for path-style endpoint"
    } else {
        "init opendal s3 operator for virtual-host endpoint"
    };
    let mut op = Operator::new(builder).context(init_context)?.finish();

    if let Some(timeout_layer) = build_timeout_layer(cfg) {
        op = op.layer(timeout_layer);
    }
    op = op.layer(build_retry_layer(cfg));
    Ok(op)
}

fn endpoint_host(endpoint: &str) -> String {
    let mut view = endpoint.trim();
    if let Some(rest) = view.strip_prefix("http://") {
        view = rest;
    } else if let Some(rest) = view.strip_prefix("https://") {
        view = rest;
    }
    if let Some((authority, _)) = view.split_once('/') {
        view = authority;
    }
    if let Some(rest) = view.strip_prefix('[') {
        if let Some((host, _)) = rest.split_once(']') {
            return host.to_ascii_lowercase();
        }
    }
    view.split(':').next().unwrap_or(view).to_ascii_lowercase()
}

fn is_local_endpoint(endpoint: &str) -> bool {
    let host = endpoint_host(endpoint);
    host == "localhost" || host.parse::<IpAddr>().is_ok()
}

fn prefer_virtual_host_style(endpoint: &str) -> bool {
    let host = endpoint_host(endpoint);
    let suffixes = [
        ".amazonaws.com",
        ".aliyuncs.com",
        ".myhuaweicloud.com",
        ".myqcloud.com",
        ".volces.com",
        ".ivolces.com",
        ".ksyuncs.com",
        "storage.googleapis.com",
    ];
    suffixes.iter().any(|suffix| host.ends_with(suffix))
}

fn should_use_path_style(cfg: &ObjectStoreConfig) -> bool {
    if let Some(v) = cfg.enable_path_style_access {
        return v;
    }
    !prefer_virtual_host_style(&cfg.endpoint)
}

fn normalize_s3_endpoint(raw_endpoint: &str) -> Result<String> {
    let endpoint = raw_endpoint.trim().trim_end_matches('/');
    if endpoint.is_empty() {
        return Err(anyhow!("empty oss endpoint"));
    }
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return Ok(endpoint.to_string());
    }
    let scheme = if is_local_endpoint(endpoint) {
        "http"
    } else {
        "https"
    };
    Ok(format!("{scheme}://{endpoint}"))
}

pub fn build_oss_operator(cfg: &ObjectStoreConfig) -> Result<Operator> {
    let mut effective_cfg = cfg.clone();
    apply_object_store_runtime_defaults(&mut effective_cfg);
    let key = OssOperatorCacheKey::from_config(&effective_cfg);
    if let Some(op) = {
        let guard = oss_operator_cache()
            .lock()
            .map_err(|_| anyhow!("lock oss operator cache failed"))?;
        guard.get(&key).cloned()
    } {
        return Ok(op);
    }

    debug!(
        "init object store operator: endpoint={} bucket={} retry_max_times={:?} retry_min_delay_ms={:?} retry_max_delay_ms={:?} timeout_ms={:?} io_timeout_ms={:?}",
        effective_cfg.endpoint,
        effective_cfg.bucket,
        effective_cfg.retry_max_times,
        effective_cfg.retry_min_delay_ms,
        effective_cfg.retry_max_delay_ms,
        effective_cfg.timeout_ms,
        effective_cfg.io_timeout_ms
    );
    let op = build_raw_object_store_operator(&effective_cfg)?;
    let mut guard = oss_operator_cache()
        .lock()
        .map_err(|_| anyhow!("lock oss operator cache failed"))?;
    let cached = guard.entry(key).or_insert_with(|| op.clone());
    Ok(cached.clone())
}

fn oss_runtime() -> Result<&'static tokio::runtime::Runtime, String> {
    match OSS_RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().map_err(|e| format!("init tokio runtime failed: {e}"))
    }) {
        Ok(rt) => Ok(rt),
        Err(err) => Err(err.clone()),
    }
}

pub fn oss_block_on<F>(future: F) -> Result<F::Output, String>
where
    F: Future,
{
    let rt = oss_runtime()?;
    Ok(rt.block_on(future))
}

pub fn resolve_oss_operator_and_path_with_config(
    full_path: &str,
    cfg: &ObjectStoreConfig,
) -> Result<(Operator, String), String> {
    let op = build_oss_operator(&cfg).map_err(|e| e.to_string())?;
    let rel = normalize_oss_path(full_path, &cfg.bucket, &cfg.root)?;
    Ok((op, rel))
}

pub fn normalize_oss_path(full: &str, bucket: &str, root: &str) -> Result<String, String> {
    // Expected full path formats:
    // - oss://<bucket>/<key>
    // - s3://<bucket>/<key> (sometimes used with OSS S3-compat)
    // - <key> (already relative to bucket)
    // Return value must be path relative to configured OpenDAL root.
    let mut s = full.trim().to_string();

    for scheme in ["oss://", "s3://"] {
        if let Some(rest) = s.strip_prefix(scheme) {
            let (b, key) = rest
                .split_once('/')
                .ok_or_else(|| format!("invalid object url: {full}"))?;
            if b != bucket {
                return Err(format!(
                    "bucket mismatch: url bucket={b} config bucket={bucket}"
                ));
            }
            s = key.to_string();
            break;
        }
    }

    s = s.trim_start_matches('/').to_string();

    let root_trim = root.trim_matches('/');
    if !root_trim.is_empty() {
        let prefix = format!("{root_trim}/");
        if let Some(rest) = s.strip_prefix(&prefix) {
            s = rest.to_string();
        } else if s == root_trim {
            s.clear();
        }
    }

    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::{
        ObjectStoreRetrySettings, normalize_oss_path, normalize_s3_endpoint,
        prefer_virtual_host_style, should_use_path_style,
    };
    use crate::fs::object_store::ObjectStoreConfig;
    use std::collections::BTreeMap;

    #[test]
    fn normalize_oss_path_strips_bucket_and_root_prefix() {
        let got = normalize_oss_path(
            "oss://my-bucket/my-prefix/a/b.parquet",
            "my-bucket",
            "/my-prefix",
        )
        .expect("normalize oss path");
        assert_eq!(got, "a/b.parquet");
    }

    #[test]
    fn normalize_oss_path_rejects_bucket_mismatch() {
        let err = normalize_oss_path("oss://bucket-a/a/b.parquet", "bucket-b", "")
            .expect_err("bucket mismatch should fail");
        assert!(err.contains("bucket mismatch"));
    }

    #[test]
    fn prefer_virtual_host_style_for_aliyun_endpoint() {
        assert!(prefer_virtual_host_style(
            "https://oss-cn-zhangjiakou.aliyuncs.com"
        ));
    }

    #[test]
    fn default_to_path_style_for_local_endpoint() {
        let cfg = ObjectStoreConfig {
            endpoint: "http://localhost:9000".to_string(),
            bucket: "bucket".to_string(),
            root: String::new(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            session_token: None,
            enable_path_style_access: None,
            region: None,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        };
        assert!(should_use_path_style(&cfg));
    }

    #[test]
    fn explicit_path_style_flag_overrides_default() {
        let cfg = ObjectStoreConfig {
            endpoint: "https://oss-cn-zhangjiakou.aliyuncs.com".to_string(),
            bucket: "bucket".to_string(),
            root: String::new(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            session_token: None,
            enable_path_style_access: Some(true),
            region: None,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        };
        assert!(should_use_path_style(&cfg));
    }

    #[test]
    fn normalize_s3_endpoint_defaults_local_to_http() {
        let endpoint = normalize_s3_endpoint("localhost:9000").expect("normalize endpoint");
        assert_eq!(endpoint, "http://localhost:9000");
    }

    #[test]
    fn parse_retry_settings_from_aws_props() {
        let mut props = BTreeMap::new();
        props.insert("aws.s3.max_retries".to_string(), "9".to_string());
        props.insert("aws.s3.retry_min_delay_ms".to_string(), "120".to_string());
        props.insert("aws.s3.retry_max_delay_ms".to_string(), "2800".to_string());
        props.insert("aws.s3.request_timeout_ms".to_string(), "3500".to_string());
        props.insert("aws.s3.io_timeout_ms".to_string(), "4000".to_string());

        let settings = ObjectStoreRetrySettings::from_aws_s3_props(Some(&props));
        assert_eq!(settings.retry_max_times, Some(9));
        assert_eq!(settings.retry_min_delay_ms, Some(120));
        assert_eq!(settings.retry_max_delay_ms, Some(2800));
        assert_eq!(settings.timeout_ms, Some(3500));
        assert_eq!(settings.io_timeout_ms, Some(4000));
    }

    #[test]
    fn parse_retry_settings_ignores_invalid_values() {
        let mut props = BTreeMap::new();
        props.insert("aws.s3.max_retries".to_string(), "x".to_string());
        props.insert("aws.s3.retry_min_delay_ms".to_string(), "abc".to_string());
        let settings = ObjectStoreRetrySettings::from_aws_s3_props(Some(&props));
        assert_eq!(settings.retry_max_times, None);
        assert_eq!(settings.retry_min_delay_ms, None);
    }
}
