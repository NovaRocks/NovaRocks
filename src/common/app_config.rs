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
use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

static CONFIG: OnceLock<NovaRocksConfig> = OnceLock::new();

fn default_log_level() -> String {
    "info".to_string()
}

pub fn init_from_path(path: impl AsRef<Path>) -> Result<&'static NovaRocksConfig> {
    if let Some(cfg) = CONFIG.get() {
        return Ok(cfg);
    }
    let path = path.as_ref().to_path_buf();
    let cfg = NovaRocksConfig::load_from_file(&path)?;
    let _ = CONFIG.set(cfg);
    Ok(CONFIG.get().expect("CONFIG set"))
}

pub fn init_from_env_or_default() -> Result<&'static NovaRocksConfig> {
    if let Some(cfg) = CONFIG.get() {
        return Ok(cfg);
    }
    let path = config_path_from_env_or_default()?;
    let cfg = NovaRocksConfig::load_from_file(&path)?;
    let _ = CONFIG.set(cfg);
    Ok(CONFIG.get().expect("CONFIG set"))
}

pub fn config() -> Result<&'static NovaRocksConfig> {
    init_from_env_or_default()
}

fn config_path_from_env_or_default() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("NOVAROCKS_CONFIG") {
        if !p.trim().is_empty() {
            return Ok(PathBuf::from(p));
        }
    }

    let candidates = [PathBuf::from("novarocks.toml")];
    for p in candidates {
        if p.exists() {
            return Ok(p);
        }
    }

    Err(anyhow!(
        "missing config file: set $NOVAROCKS_CONFIG or create ./novarocks.toml"
    ))
}

#[derive(Clone, Deserialize)]
pub struct NovaRocksConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,

    /// Optional full tracing EnvFilter expression.
    /// If set, this takes precedence over `log_level`.
    /// Example: "novarocks=debug,h2=off,hyper=off,tonic=off"
    #[serde(default)]
    pub log_filter: Option<String>,

    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub runtime: RuntimeConfig,

    #[serde(default)]
    pub debug: DebugConfig,

    #[serde(default)]
    pub jdbc: Option<JdbcConfig>,

    #[serde(default)]
    pub spill: SpillStorageConfig,

    #[serde(default)]
    pub starrocks: StarRocksConfig,
}

impl NovaRocksConfig {
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("read config file: {}", path.display()))?;
        let cfg: NovaRocksConfig =
            toml::from_str(&s).with_context(|| format!("parse toml: {}", path.display()))?;
        Ok(cfg)
    }

    pub fn jdbc_config(&self) -> Option<&JdbcConfig> {
        self.jdbc.as_ref()
    }
}

impl Default for NovaRocksConfig {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            log_filter: None,
            server: ServerConfig::default(),
            runtime: RuntimeConfig::default(),
            debug: DebugConfig::default(),
            jdbc: None,
            spill: SpillStorageConfig::default(),
            starrocks: StarRocksConfig::default(),
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_server_host")]
    pub host: String,
    #[serde(default = "default_heartbeat_port")]
    pub heartbeat_port: u16,
    #[serde(default = "default_be_port")]
    pub be_port: u16,
    #[serde(default = "default_brpc_port")]
    pub brpc_port: u16,
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    #[serde(default = "default_starlet_port")]
    pub starlet_port: u16,
}

fn default_server_host() -> String {
    "0.0.0.0".to_string()
}
fn default_heartbeat_port() -> u16 {
    9050
}
fn default_be_port() -> u16 {
    9060
}
fn default_brpc_port() -> u16 {
    8060
}
fn default_http_port() -> u16 {
    8040
}
fn default_starlet_port() -> u16 {
    9070
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_server_host(),
            heartbeat_port: default_heartbeat_port(),
            be_port: default_be_port(),
            brpc_port: default_brpc_port(),
            http_port: default_http_port(),
            starlet_port: default_starlet_port(),
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_exchange_wait_ms")]
    pub exchange_wait_ms: u64,
    #[serde(default = "default_exchange_max_transmit_batched_bytes")]
    pub exchange_max_transmit_batched_bytes: usize,
    #[serde(default = "default_exchange_io_threads")]
    pub exchange_io_threads: usize,
    #[serde(default = "default_exchange_io_max_inflight_bytes")]
    pub exchange_io_max_inflight_bytes: usize,
    #[serde(default = "default_local_exchange_buffer_mem_limit_per_driver")]
    pub local_exchange_buffer_mem_limit_per_driver: usize,
    #[serde(default = "default_local_exchange_max_buffered_rows")]
    pub local_exchange_max_buffered_rows: i64,
    #[serde(default = "default_operator_buffer_chunks")]
    pub operator_buffer_chunks: usize,
    #[serde(default = "default_olap_sink_write_buffer_size_bytes")]
    pub olap_sink_write_buffer_size_bytes: usize,
    #[serde(default = "default_olap_sink_max_tablet_write_chunk_bytes")]
    pub olap_sink_max_tablet_write_chunk_bytes: usize,
    #[serde(default = "default_pipeline_scan_thread_pool_thread_num")]
    pub pipeline_scan_thread_pool_thread_num: usize,
    #[serde(default = "default_pipeline_scan_thread_pool_queue_size")]
    pub pipeline_scan_thread_pool_queue_size: usize,
    #[serde(default = "default_pipeline_exec_thread_pool_thread_num")]
    pub pipeline_exec_thread_pool_thread_num: usize,
    #[serde(default = "default_spill_io_threads")]
    pub spill_io_threads: usize,
    #[serde(default = "default_spill_io_queue_size")]
    pub spill_io_queue_size: usize,
    #[serde(default = "default_scan_submit_fail_max")]
    pub scan_submit_fail_max: usize,
    #[serde(default = "default_scan_submit_fail_timeout_ms")]
    pub scan_submit_fail_timeout_ms: u64,
    #[serde(default = "default_profile_report_interval")]
    pub profile_report_interval: i64,
    #[serde(default)]
    pub runtime_filter_scan_wait_time_ms_override: Option<i64>,
    #[serde(default)]
    pub runtime_filter_wait_timeout_ms_override: Option<i64>,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub path_rewrite: PathRewriteConfig,
}

#[derive(Clone, Deserialize)]
pub struct SpillStorageConfig {
    #[serde(default = "default_spill_enable")]
    pub enable: bool,
    #[serde(default)]
    pub local_dirs: Vec<String>,
    #[serde(default = "default_spill_dir_max_bytes")]
    pub dir_max_bytes: u64,
    #[serde(default = "default_spill_block_size_bytes")]
    pub block_size_bytes: u64,
    #[serde(default = "default_spill_ipc_compression")]
    pub ipc_compression: String,
}

fn default_spill_enable() -> bool {
    true
}

fn default_spill_dir_max_bytes() -> u64 {
    0
}

fn default_spill_block_size_bytes() -> u64 {
    134_217_728
}

fn default_spill_ipc_compression() -> String {
    "lz4".to_string()
}

impl Default for SpillStorageConfig {
    fn default() -> Self {
        Self {
            enable: default_spill_enable(),
            local_dirs: Vec::new(),
            dir_max_bytes: default_spill_dir_max_bytes(),
            block_size_bytes: default_spill_block_size_bytes(),
            ipc_compression: default_spill_ipc_compression(),
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct StarRocksConfig {
    #[serde(default)]
    pub fe_http_endpoint: Option<String>,
    #[serde(default = "default_fe_catalog")]
    pub fe_catalog: String,
    #[serde(default)]
    pub auth_mode: Option<String>,
    #[serde(default)]
    pub basic_user: Option<String>,
    #[serde(default)]
    pub basic_password: Option<String>,
    #[serde(default)]
    pub auth_token: Option<String>,
    #[serde(default = "default_starrocks_meta_cache_ttl_ms")]
    pub meta_cache_ttl_ms: u64,
    #[serde(default = "default_starrocks_lake_data_write_format")]
    pub lake_data_write_format: String,
}

fn default_fe_catalog() -> String {
    "default_catalog".to_string()
}

fn default_starrocks_meta_cache_ttl_ms() -> u64 {
    0
}

fn default_starrocks_lake_data_write_format() -> String {
    "native".to_string()
}

impl Default for StarRocksConfig {
    fn default() -> Self {
        Self {
            fe_http_endpoint: None,
            fe_catalog: default_fe_catalog(),
            auth_mode: None,
            basic_user: None,
            basic_password: None,
            auth_token: None,
            meta_cache_ttl_ms: default_starrocks_meta_cache_ttl_ms(),
            lake_data_write_format: default_starrocks_lake_data_write_format(),
        }
    }
}

fn default_exchange_wait_ms() -> u64 {
    120_000
}

fn default_exchange_max_transmit_batched_bytes() -> usize {
    262_144 // 256KB, aligned with StarRocks `max_transmit_batched_bytes`
}

fn default_exchange_io_threads() -> usize {
    4
}

fn default_exchange_io_max_inflight_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_local_exchange_buffer_mem_limit_per_driver() -> usize {
    128 * 1024 * 1024
}

fn default_local_exchange_max_buffered_rows() -> i64 {
    -1
}

fn default_operator_buffer_chunks() -> usize {
    8
}

fn default_olap_sink_write_buffer_size_bytes() -> usize {
    100 * 1024 * 1024 // 100MB, aligned with StarRocks `write_buffer_size`
}

fn default_olap_sink_max_tablet_write_chunk_bytes() -> usize {
    512 * 1024 * 1024 // 512MB, aligned with StarRocks `max_tablet_write_chunk_bytes`
}

fn default_pipeline_exec_thread_pool_thread_num() -> usize {
    0 // 0 means use CPU cores
}

fn default_spill_io_threads() -> usize {
    0 // 0 means use actual exec thread count
}

fn default_spill_io_queue_size() -> usize {
    1024
}

fn default_pipeline_scan_thread_pool_thread_num() -> usize {
    0 // 0 means use CPU cores, aligned with StarRocks pipeline_scan_thread_pool_thread_num
}

fn default_pipeline_scan_thread_pool_queue_size() -> usize {
    102_400 // Aligned with StarRocks pipeline_scan_thread_pool_queue_size
}

fn default_scan_submit_fail_max() -> usize {
    128
}

fn default_scan_submit_fail_timeout_ms() -> u64 {
    2000
}

fn default_profile_report_interval() -> i64 {
    30
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            exchange_wait_ms: default_exchange_wait_ms(),
            exchange_max_transmit_batched_bytes: default_exchange_max_transmit_batched_bytes(),
            exchange_io_threads: default_exchange_io_threads(),
            exchange_io_max_inflight_bytes: default_exchange_io_max_inflight_bytes(),
            local_exchange_buffer_mem_limit_per_driver:
                default_local_exchange_buffer_mem_limit_per_driver(),
            local_exchange_max_buffered_rows: default_local_exchange_max_buffered_rows(),
            operator_buffer_chunks: default_operator_buffer_chunks(),
            olap_sink_write_buffer_size_bytes: default_olap_sink_write_buffer_size_bytes(),
            olap_sink_max_tablet_write_chunk_bytes: default_olap_sink_max_tablet_write_chunk_bytes(
            ),
            pipeline_scan_thread_pool_thread_num: default_pipeline_scan_thread_pool_thread_num(),
            pipeline_scan_thread_pool_queue_size: default_pipeline_scan_thread_pool_queue_size(),
            pipeline_exec_thread_pool_thread_num: default_pipeline_exec_thread_pool_thread_num(),
            spill_io_threads: default_spill_io_threads(),
            spill_io_queue_size: default_spill_io_queue_size(),
            scan_submit_fail_max: default_scan_submit_fail_max(),
            scan_submit_fail_timeout_ms: default_scan_submit_fail_timeout_ms(),
            profile_report_interval: default_profile_report_interval(),
            runtime_filter_scan_wait_time_ms_override: None,
            runtime_filter_wait_timeout_ms_override: None,
            cache: CacheConfig::default(),
            path_rewrite: PathRewriteConfig::default(),
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct PathRewriteConfig {
    #[serde(default)]
    pub enable: bool,
    #[serde(default)]
    pub from_prefix: String,
    #[serde(default)]
    pub to_prefix: String,
}

impl Default for PathRewriteConfig {
    fn default() -> Self {
        Self {
            enable: false,
            from_prefix: String::new(),
            to_prefix: String::new(),
        }
    }
}

impl RuntimeConfig {
    /// Get the actual number of executor threads.
    /// Returns CPU cores if configured as 0.
    pub fn actual_exec_threads(&self) -> usize {
        if self.pipeline_exec_thread_pool_thread_num > 0 {
            self.pipeline_exec_thread_pool_thread_num
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        }
    }

    /// Get the actual number of scan threads.
    /// Returns CPU cores if configured as 0.
    pub fn actual_scan_threads(&self) -> usize {
        if self.pipeline_scan_thread_pool_thread_num > 0 {
            self.pipeline_scan_thread_pool_thread_num
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_page_cache_enable")]
    pub page_cache_enable: bool,
    #[serde(default = "default_page_cache_capacity")]
    pub page_cache_capacity: usize,
    #[serde(default = "default_page_cache_evict_probability")]
    pub page_cache_evict_probability: u32,
    #[serde(default = "default_parquet_meta_cache_enable")]
    pub parquet_meta_cache_enable: bool,
    #[serde(default = "default_parquet_meta_cache_capacity")]
    pub parquet_meta_cache_capacity: usize,
    #[serde(default = "default_parquet_meta_cache_ttl_seconds")]
    pub parquet_meta_cache_ttl_seconds: u64,
    #[serde(default = "default_parquet_page_cache_enable")]
    pub parquet_page_cache_enable: bool,
    #[serde(default = "default_parquet_page_cache_capacity")]
    pub parquet_page_cache_capacity: usize,
    #[serde(default = "default_parquet_page_cache_ttl_seconds")]
    pub parquet_page_cache_ttl_seconds: u64,
    #[serde(default = "default_parquet_page_cache_decompress_threshold")]
    pub parquet_page_cache_decompress_threshold: f64,
    #[serde(default = "default_datacache_enable")]
    pub datacache_enable: bool,
    #[serde(default = "default_datacache_disk_path")]
    pub datacache_disk_path: String,
    #[serde(default = "default_datacache_disk_size")]
    pub datacache_disk_size: u64,
    #[serde(default = "default_datacache_block_size")]
    pub datacache_block_size: u64,
    #[serde(default = "default_datacache_checksum_enable")]
    pub datacache_checksum_enable: bool,
    #[serde(default = "default_datacache_direct_io_enable")]
    pub datacache_direct_io_enable: bool,
    #[serde(default = "default_datacache_io_align_unit_size")]
    pub datacache_io_align_unit_size: u64,
}

fn default_parquet_meta_cache_enable() -> bool {
    true
}

fn default_page_cache_enable() -> bool {
    true
}

fn default_page_cache_capacity() -> usize {
    11_000
}

fn default_page_cache_evict_probability() -> u32 {
    100
}

fn default_parquet_meta_cache_capacity() -> usize {
    1000 // Cache up to 1000 file metadata entries
}

fn default_parquet_meta_cache_ttl_seconds() -> u64 {
    3600 // 1 hour TTL
}

fn default_parquet_page_cache_enable() -> bool {
    true
}

fn default_parquet_page_cache_capacity() -> usize {
    10000 // Cache up to 10000 page entries (each entry can be several KB to MB)
}

fn default_parquet_page_cache_ttl_seconds() -> u64 {
    1800 // 30 minutes TTL (shorter than metadata cache)
}

fn default_parquet_page_cache_decompress_threshold() -> f64 {
    2.0 // Cache decompressed data if uncompressed_size <= 2.0 * compressed_size
}

fn default_datacache_enable() -> bool {
    false
}

fn default_datacache_disk_path() -> String {
    "/tmp/novarocks_datacache".to_string()
}

fn default_datacache_disk_size() -> u64 {
    0
}

fn default_datacache_block_size() -> u64 {
    1 * 1024 * 1024
}

fn default_datacache_checksum_enable() -> bool {
    true
}

fn default_datacache_direct_io_enable() -> bool {
    false
}

fn default_datacache_io_align_unit_size() -> u64 {
    4096
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            page_cache_enable: default_page_cache_enable(),
            page_cache_capacity: default_page_cache_capacity(),
            page_cache_evict_probability: default_page_cache_evict_probability(),
            parquet_meta_cache_enable: default_parquet_meta_cache_enable(),
            parquet_meta_cache_capacity: default_parquet_meta_cache_capacity(),
            parquet_meta_cache_ttl_seconds: default_parquet_meta_cache_ttl_seconds(),
            parquet_page_cache_enable: default_parquet_page_cache_enable(),
            parquet_page_cache_capacity: default_parquet_page_cache_capacity(),
            parquet_page_cache_ttl_seconds: default_parquet_page_cache_ttl_seconds(),
            parquet_page_cache_decompress_threshold:
                default_parquet_page_cache_decompress_threshold(),
            datacache_enable: default_datacache_enable(),
            datacache_disk_path: default_datacache_disk_path(),
            datacache_disk_size: default_datacache_disk_size(),
            datacache_block_size: default_datacache_block_size(),
            datacache_checksum_enable: default_datacache_checksum_enable(),
            datacache_direct_io_enable: default_datacache_direct_io_enable(),
            datacache_io_align_unit_size: default_datacache_io_align_unit_size(),
        }
    }
}

#[derive(Clone, Default, Deserialize)]
pub struct DebugConfig {
    #[serde(default)]
    pub exec_node_output: bool,
    #[serde(default)]
    /// Dump RPC inputs as "named_json" for `exec_plan_fragment` / `exec_batch_plan_fragments`.
    /// This is config-only (no env var fallback).
    pub exec_batch_plan_json: bool,
}

#[derive(Clone, Deserialize)]
pub struct JdbcConfig {
    pub url: String,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub default_db: Option<String>,
}

impl std::fmt::Debug for JdbcConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JdbcConfig")
            .field("url", &self.url)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("default_db", &self.default_db)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::NovaRocksConfig;

    #[test]
    fn test_server_starlet_port_default_is_9070() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[server]
http_port = 8040
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.server.starlet_port, 9070);
    }

    #[test]
    fn test_server_starlet_port_can_be_overridden() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[server]
http_port = 8040
starlet_port = 19070
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.server.starlet_port, 19070);
    }

    #[test]
    fn test_runtime_olap_sink_threshold_defaults() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.runtime.olap_sink_write_buffer_size_bytes, 104_857_600);
        assert_eq!(
            cfg.runtime.olap_sink_max_tablet_write_chunk_bytes,
            536_870_912
        );
    }

    #[test]
    fn test_runtime_olap_sink_threshold_can_be_overridden() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
olap_sink_write_buffer_size_bytes = 33554432
olap_sink_max_tablet_write_chunk_bytes = 67108864
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.runtime.olap_sink_write_buffer_size_bytes, 33_554_432);
        assert_eq!(
            cfg.runtime.olap_sink_max_tablet_write_chunk_bytes,
            67_108_864
        );
    }
}
