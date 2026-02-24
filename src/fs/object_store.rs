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
use std::collections::HashMap;
use std::future::Future;
use std::net::IpAddr;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use opendal::Operator;
use opendal::layers::{RetryLayer, TimeoutLayer};

const DEFAULT_OSS_RETRY_MAX_TIMES: usize = 6;
const DEFAULT_OSS_RETRY_MIN_DELAY_MS: u64 = 100;
const DEFAULT_OSS_RETRY_MAX_DELAY_MS: u64 = 2_000;

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

fn oss_operator_cache() -> &'static Mutex<HashMap<OssOperatorCacheKey, Operator>> {
    OSS_OPERATOR_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn build_retry_layer(cfg: &ObjectStoreConfig) -> RetryLayer {
    let max_times = cfg.retry_max_times.unwrap_or(DEFAULT_OSS_RETRY_MAX_TIMES);
    let min_delay_ms = cfg
        .retry_min_delay_ms
        .unwrap_or(DEFAULT_OSS_RETRY_MIN_DELAY_MS);
    let max_delay_ms = cfg
        .retry_max_delay_ms
        .unwrap_or(DEFAULT_OSS_RETRY_MAX_DELAY_MS)
        .max(min_delay_ms);

    RetryLayer::new()
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
    let key = OssOperatorCacheKey::from_config(cfg);
    if let Some(op) = {
        let guard = oss_operator_cache()
            .lock()
            .map_err(|_| anyhow!("lock oss operator cache failed"))?;
        guard.get(&key).cloned()
    } {
        return Ok(op);
    }

    let op = build_raw_object_store_operator(cfg)?;
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

pub fn resolve_oss_operator_and_path(full_path: &str) -> Result<(Operator, String), String> {
    let cfg = resolve_oss_config_for_path(full_path)?;
    resolve_oss_operator_and_path_with_config(full_path, &cfg)
}

pub fn resolve_oss_config_for_path(full_path: &str) -> Result<ObjectStoreConfig, String> {
    crate::runtime::starlet_shard_registry::find_s3_config_for_path(full_path)
        .ok_or_else(|| {
            format!(
                "missing object store config from FE for path={full_path}; \
                expected AddShard or persisted tablet runtime to provide S3 credentials"
            )
        })
        .map(|cfg| cfg.to_object_store_config())
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
        normalize_oss_path, normalize_s3_endpoint, prefer_virtual_host_style, should_use_path_style,
    };
    use crate::fs::object_store::ObjectStoreConfig;

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
}
