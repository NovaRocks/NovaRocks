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
use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::{OnceLock, RwLock};

static CONFIG: OnceLock<RwLock<&'static NovaRocksConfig>> = OnceLock::new();

fn install_config(cfg: NovaRocksConfig) -> &'static NovaRocksConfig {
    let leaked: &'static NovaRocksConfig = Box::leak(Box::new(cfg));
    let lock = CONFIG.get_or_init(|| RwLock::new(leaked));
    *lock.write().expect("novarocks config lock poisoned") = leaked;
    leaked
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_sys_log_dir() -> String {
    "log".to_string()
}

fn default_sys_log_roll_mode() -> String {
    "SIZE-MB-1024".to_string()
}

fn default_sys_log_roll_num() -> usize {
    10
}

/// Cluster role for distributed deployments.
/// The default is `AllInOne`, which preserves existing single-process behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterRole {
    Fe,
    Be,
    AllInOne,
}

impl Default for ClusterRole {
    fn default() -> Self {
        ClusterRole::AllInOne
    }
}

/// Configuration for the `[cluster]` TOML section.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub role: ClusterRole,
    pub backends: Vec<String>,
    pub advertise_host: String,
    pub advertise_port: u16,
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_heartbeat_timeout_retries")]
    pub heartbeat_timeout_retries: u32,
    #[serde(default = "default_decommission_timeout_secs")]
    pub decommission_timeout_secs: u64,
}

fn default_heartbeat_interval_ms() -> u64 {
    5000
}

fn default_heartbeat_timeout_retries() -> u32 {
    3
}

fn default_decommission_timeout_secs() -> u64 {
    300
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            role: ClusterRole::default(),
            backends: Vec::new(),
            advertise_host: String::new(),
            advertise_port: 0,
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            heartbeat_timeout_retries: default_heartbeat_timeout_retries(),
            decommission_timeout_secs: default_decommission_timeout_secs(),
        }
    }
}

impl ClusterConfig {
    /// Validate cluster config consistency. Called at startup after parsing.
    pub fn validate(&self) -> Result<(), String> {
        match self.role {
            ClusterRole::Fe => {
                let mut seen = std::collections::HashSet::new();
                for b in &self.backends {
                    let canonical = b
                        .parse::<std::net::SocketAddr>()
                        .map_err(|e| format!("invalid backend addr '{}': {}", b, e))?
                        .to_string();
                    if !seen.insert(canonical) {
                        return Err(format!("duplicate backend in [cluster].backends: {}", b));
                    }
                }
            }
            ClusterRole::Be => {
                if !self.backends.is_empty() {
                    return Err(format!(
                        "role=be must not configure [cluster].backends (got {} entries)",
                        self.backends.len()
                    ));
                }
            }
            ClusterRole::AllInOne => {
                if !self.backends.is_empty() {
                    return Err(format!(
                        "role=all-in-one must not configure [cluster].backends (got {} entries)",
                        self.backends.len()
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod cluster_hb_tests {
    use super::ClusterConfig;

    #[test]
    fn cluster_config_heartbeat_defaults() {
        let c = ClusterConfig::default();
        assert_eq!(c.heartbeat_interval_ms, 5000);
        assert_eq!(c.heartbeat_timeout_retries, 3);
        assert_eq!(c.decommission_timeout_secs, 300);
    }

    #[test]
    fn cluster_config_parses_heartbeat_overrides() {
        let toml = r#"
            role = "fe"
            backends = ["127.0.0.1:9070"]
            heartbeat_interval_ms = 2000
            heartbeat_timeout_retries = 5
        "#;
        let c: ClusterConfig = toml::from_str(toml).unwrap();
        assert_eq!(c.heartbeat_interval_ms, 2000);
        assert_eq!(c.heartbeat_timeout_retries, 5);
        assert_eq!(c.decommission_timeout_secs, 300);
    }
}

/// Resolve the config file path using the standard search order:
/// 1. `explicit` – a path supplied directly by the caller (e.g. `--config`).
/// 2. `NOVAROCKS_CONFIG` environment variable.
/// 3. `./novarocks.toml` in the current working directory (only if the file exists).
/// 4. `None` – the caller should fall back to built-in defaults.
pub fn resolve_config_path(explicit: Option<&Path>) -> Option<PathBuf> {
    explicit
        .map(Path::to_path_buf)
        .or_else(|| {
            std::env::var("NOVAROCKS_CONFIG")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
        })
        .or_else(|| {
            let default_path = PathBuf::from("novarocks.toml");
            default_path.exists().then_some(default_path)
        })
}

pub fn init_from_path(path: impl AsRef<Path>) -> Result<&'static NovaRocksConfig> {
    let path = path.as_ref().to_path_buf();
    let cfg = if !path.exists() {
        eprintln!(
            "WARNING: config file '{}' not found, using built-in defaults",
            path.display()
        );
        NovaRocksConfig::default()
    } else {
        NovaRocksConfig::load_from_file(&path)?
    };
    Ok(install_config(cfg))
}

pub fn init_from_env_or_default() -> Result<&'static NovaRocksConfig> {
    if let Some(lock) = CONFIG.get() {
        return Ok(*lock.read().expect("novarocks config lock poisoned"));
    }
    if let Ok(p) = std::env::var("NOVAROCKS_CONFIG") {
        let p = p.trim();
        if !p.is_empty() {
            return init_from_path(PathBuf::from(p));
        }
    }

    let default_path = PathBuf::from("novarocks.toml");
    if default_path.exists() {
        let cfg = NovaRocksConfig::load_from_file(&default_path)?;
        return Ok(install_config(cfg));
    }

    eprintln!("WARNING: config file 'novarocks.toml' not found, using built-in defaults");
    Ok(install_config(NovaRocksConfig::default()))
}

/// Install an already-loaded config as the process-wide active config, replacing
/// any existing global config.  Use this when the caller has already loaded and
/// validated a [`NovaRocksConfig`] and wants to guarantee that the engine uses
/// exactly that instance rather than performing a second disk read.
pub fn install_preloaded_config(cfg: NovaRocksConfig) -> &'static NovaRocksConfig {
    install_config(cfg)
}

/// Force-install the built-in default config, replacing any existing global config.
/// Intended for test setup where each test must start from a known-clean config.
#[cfg(test)]
pub fn install_default_for_test() -> &'static NovaRocksConfig {
    install_config(NovaRocksConfig::default())
}

pub fn config() -> Result<&'static NovaRocksConfig> {
    if let Some(lock) = CONFIG.get() {
        return Ok(*lock.read().expect("novarocks config lock poisoned"));
    }
    init_from_env_or_default()
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

    #[serde(default = "default_sys_log_dir")]
    pub sys_log_dir: String,

    #[serde(default = "default_sys_log_roll_mode")]
    pub sys_log_roll_mode: String,

    #[serde(default = "default_sys_log_roll_num")]
    pub sys_log_roll_num: usize,

    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub runtime: RuntimeConfig,

    #[serde(default)]
    pub debug: DebugConfig,

    #[serde(default)]
    pub jdbc: Option<JdbcConfig>,

    #[serde(default)]
    pub metadata: Option<MetadataConfig>,

    #[serde(default)]
    pub standalone_server: Option<StandaloneServerConfig>,

    #[serde(default)]
    pub spill: SpillStorageConfig,

    #[serde(default)]
    pub starrocks: StarRocksConfig,

    #[serde(default)]
    pub cluster: ClusterConfig,
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
            sys_log_dir: default_sys_log_dir(),
            sys_log_roll_mode: default_sys_log_roll_mode(),
            sys_log_roll_num: default_sys_log_roll_num(),
            server: ServerConfig::default(),
            runtime: RuntimeConfig::default(),
            debug: DebugConfig::default(),
            jdbc: None,
            metadata: None,
            standalone_server: None,
            spill: SpillStorageConfig::default(),
            starrocks: StarRocksConfig::default(),
            cluster: ClusterConfig::default(),
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_server_host")]
    pub host: String,
    #[serde(default)]
    pub priority_networks: String,
    #[serde(default = "default_heartbeat_port")]
    pub heartbeat_port: u16,
    #[serde(default = "default_be_port")]
    pub be_port: u16,
    #[serde(default = "default_brpc_port")]
    pub brpc_port: u16,
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    #[serde(default = "default_grpc_port")]
    pub grpc_port: u16,
    #[serde(default = "default_starlet_port")]
    pub starlet_port: u16,
}

fn default_server_host() -> String {
    "127.0.0.1".to_string()
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
fn default_grpc_port() -> u16 {
    9080
}
fn default_starlet_port() -> u16 {
    9070
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_server_host(),
            priority_networks: String::new(),
            heartbeat_port: default_heartbeat_port(),
            be_port: default_be_port(),
            brpc_port: default_brpc_port(),
            http_port: default_http_port(),
            grpc_port: default_grpc_port(),
            starlet_port: default_starlet_port(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct MetadataConfig {
    #[serde(default)]
    pub provider: MetadataProviderConfig,
    pub path: PathBuf,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataProviderConfig {
    #[default]
    Sqlite,
}

#[derive(Clone, Debug, Deserialize, Default, PartialEq, Eq)]
pub struct StandaloneObjectStoreConfig {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub access_key_secret: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub enable_path_style_access: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct StandaloneServerConfig {
    #[serde(default = "default_standalone_server_mysql_port")]
    pub mysql_port: u16,
    #[serde(default = "default_standalone_server_user")]
    pub user: String,
    #[serde(default)]
    pub warehouse_uri: Option<String>,
    #[serde(default)]
    pub object_store: Option<StandaloneObjectStoreConfig>,
    #[serde(default)]
    pub mv_default_storage_engine: Option<String>,
    #[serde(default)]
    pub mv_refresh_scheduler_enabled: bool,
    #[serde(default = "default_standalone_mv_refresh_scheduler_interval_ms")]
    pub mv_refresh_scheduler_interval_ms: u64,
    #[serde(default = "default_standalone_mv_refresh_scheduler_max_concurrent")]
    pub mv_refresh_scheduler_max_concurrent: usize,
    #[serde(default = "default_standalone_mv_refresh_scheduler_failure_backoff_ms")]
    pub mv_refresh_scheduler_failure_backoff_ms: i64,
    #[serde(default = "default_standalone_mv_refresh_scheduler_max_failure_backoff_ms")]
    pub mv_refresh_scheduler_max_failure_backoff_ms: i64,
    #[serde(default = "default_standalone_mv_refresh_max_touched_groups")]
    pub mv_refresh_max_touched_groups: usize,
    #[serde(default = "default_standalone_mv_refresh_max_affected_partitions")]
    pub mv_refresh_max_affected_partitions: usize,
    #[serde(default = "default_standalone_mv_partition_state_max_entries")]
    pub mv_partition_state_max_entries: usize,
    #[serde(default = "default_standalone_iceberg_maintenance_enabled")]
    pub iceberg_maintenance_enabled: bool,
    #[serde(default = "default_standalone_iceberg_maintenance_tick_interval_ms")]
    pub iceberg_maintenance_tick_interval_ms: u64,
    #[serde(default = "default_standalone_iceberg_maintenance_max_concurrent")]
    pub iceberg_maintenance_max_concurrent: usize,
    #[serde(default = "default_standalone_iceberg_maintenance_compaction_min_data_files")]
    pub iceberg_maintenance_compaction_min_data_files: u64,
    #[serde(default = "default_standalone_iceberg_maintenance_dv_min_delete_files")]
    pub iceberg_maintenance_dv_min_delete_files: u64,
    #[serde(default = "default_standalone_iceberg_maintenance_action_cooldown_ms")]
    pub iceberg_maintenance_action_cooldown_ms: i64,
    #[serde(default = "default_standalone_iceberg_maintenance_max_consecutive_failures")]
    pub iceberg_maintenance_max_consecutive_failures: u32,
}

fn default_standalone_server_mysql_port() -> u16 {
    9030
}

fn default_standalone_server_user() -> String {
    "root".to_string()
}

fn default_standalone_mv_refresh_scheduler_interval_ms() -> u64 {
    30_000
}

fn default_standalone_mv_refresh_scheduler_max_concurrent() -> usize {
    1
}

fn default_standalone_mv_refresh_scheduler_failure_backoff_ms() -> i64 {
    60_000
}

fn default_standalone_mv_refresh_scheduler_max_failure_backoff_ms() -> i64 {
    1_800_000
}

fn default_standalone_mv_refresh_max_touched_groups() -> usize {
    100_000
}

fn default_standalone_mv_refresh_max_affected_partitions() -> usize {
    4_096
}

fn default_standalone_mv_partition_state_max_entries() -> usize {
    10_000
}

fn default_standalone_iceberg_maintenance_enabled() -> bool {
    true
}

fn default_standalone_iceberg_maintenance_tick_interval_ms() -> u64 {
    600_000
}

fn default_standalone_iceberg_maintenance_max_concurrent() -> usize {
    1
}

fn default_standalone_iceberg_maintenance_compaction_min_data_files() -> u64 {
    100
}

fn default_standalone_iceberg_maintenance_dv_min_delete_files() -> u64 {
    10
}

fn default_standalone_iceberg_maintenance_action_cooldown_ms() -> i64 {
    3_600_000
}

fn default_standalone_iceberg_maintenance_max_consecutive_failures() -> u32 {
    4
}

impl Default for StandaloneServerConfig {
    fn default() -> Self {
        Self {
            mysql_port: default_standalone_server_mysql_port(),
            user: default_standalone_server_user(),
            warehouse_uri: None,
            object_store: None,
            mv_default_storage_engine: None,
            mv_refresh_scheduler_enabled: false,
            mv_refresh_scheduler_interval_ms: default_standalone_mv_refresh_scheduler_interval_ms(),
            mv_refresh_scheduler_max_concurrent:
                default_standalone_mv_refresh_scheduler_max_concurrent(),
            mv_refresh_scheduler_failure_backoff_ms:
                default_standalone_mv_refresh_scheduler_failure_backoff_ms(),
            mv_refresh_scheduler_max_failure_backoff_ms:
                default_standalone_mv_refresh_scheduler_max_failure_backoff_ms(),
            mv_refresh_max_touched_groups: default_standalone_mv_refresh_max_touched_groups(),
            mv_refresh_max_affected_partitions:
                default_standalone_mv_refresh_max_affected_partitions(),
            mv_partition_state_max_entries: default_standalone_mv_partition_state_max_entries(),
            iceberg_maintenance_enabled: default_standalone_iceberg_maintenance_enabled(),
            iceberg_maintenance_tick_interval_ms:
                default_standalone_iceberg_maintenance_tick_interval_ms(),
            iceberg_maintenance_max_concurrent:
                default_standalone_iceberg_maintenance_max_concurrent(),
            iceberg_maintenance_compaction_min_data_files:
                default_standalone_iceberg_maintenance_compaction_min_data_files(),
            iceberg_maintenance_dv_min_delete_files:
                default_standalone_iceberg_maintenance_dv_min_delete_files(),
            iceberg_maintenance_action_cooldown_ms:
                default_standalone_iceberg_maintenance_action_cooldown_ms(),
            iceberg_maintenance_max_consecutive_failures:
                default_standalone_iceberg_maintenance_max_consecutive_failures(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandaloneStarRocksTableConfig {
    pub warehouse_uri: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub access_key_secret: String,
    pub region: Option<String>,
    pub enable_path_style_access: Option<bool>,
    pub mv_default_storage_engine: Option<String>,
}

impl StandaloneServerConfig {
    pub fn starrocks_table_config(
        &self,
    ) -> std::result::Result<Option<StandaloneStarRocksTableConfig>, String> {
        let Some(warehouse_uri) = self
            .warehouse_uri
            .as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
        else {
            return Ok(None);
        };

        let object_store = self.object_store.as_ref().ok_or_else(|| {
            "standalone StarRocks table requires [standalone_server.object_store]".to_string()
        })?;
        let endpoint = object_store
            .endpoint
            .as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                "standalone StarRocks table requires object_store.endpoint".to_string()
            })?;
        let access_key_id = object_store
            .access_key_id
            .as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                "standalone StarRocks table requires object_store.access_key_id".to_string()
            })?;
        let access_key_secret = object_store
            .access_key_secret
            .as_ref()
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                "standalone StarRocks table requires object_store.access_key_secret".to_string()
            })?;

        Ok(Some(StandaloneStarRocksTableConfig {
            warehouse_uri: warehouse_uri.to_string(),
            endpoint: endpoint.to_string(),
            access_key_id: access_key_id.to_string(),
            access_key_secret: access_key_secret.to_string(),
            region: object_store.region.clone(),
            enable_path_style_access: object_store.enable_path_style_access,
            mv_default_storage_engine: self.mv_default_storage_engine.clone(),
        }))
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
    #[serde(default = "default_enable_tablet_write_log")]
    pub enable_tablet_write_log: bool,
    #[serde(default = "default_tablet_write_log_buffer_size")]
    pub tablet_write_log_buffer_size: usize,
    #[serde(default = "default_be_txn_info_history_size")]
    pub be_txn_info_history_size: usize,
    #[serde(default = "default_connector_io_tasks_per_scan_operator")]
    pub connector_io_tasks_per_scan_operator: i32,
    #[serde(default = "default_io_coalesce_read_enable")]
    pub io_coalesce_read_enable: bool,
    #[serde(default = "default_io_coalesce_read_max_buffer_size")]
    pub io_coalesce_read_max_buffer_size: u64,
    #[serde(default = "default_io_coalesce_read_max_distance_size")]
    pub io_coalesce_read_max_distance_size: u64,
    #[serde(default = "default_io_coalesce_adaptive_lazy_active")]
    pub io_coalesce_adaptive_lazy_active: bool,
    #[serde(default = "default_pipeline_scan_thread_pool_queue_size")]
    pub pipeline_scan_thread_pool_queue_size: usize,
    #[serde(default = "default_pipeline_exec_thread_pool_thread_num")]
    pub pipeline_exec_thread_pool_thread_num: usize,
    #[serde(default = "default_internal_service_query_rpc_thread_num")]
    pub internal_service_query_rpc_thread_num: usize,
    #[serde(default = "default_data_runtime_worker_threads")]
    pub data_runtime_worker_threads: usize,
    #[serde(default = "default_data_runtime_max_blocking_threads")]
    pub data_runtime_max_blocking_threads: usize,
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
    #[serde(default = "default_fe_rpc_connect_timeout_ms")]
    pub fe_rpc_connect_timeout_ms: u64,
    #[serde(default = "default_fe_rpc_timeout_ms")]
    pub fe_rpc_timeout_ms: u64,
    #[serde(default = "default_fe_rpc_retry_interval_ms")]
    pub fe_rpc_retry_interval_ms: u64,
    #[serde(default = "default_fe_rpc_pool_max_idle_per_host")]
    pub fe_rpc_pool_max_idle_per_host: usize,
    #[serde(default = "default_fe_rpc_max_inflight_total")]
    pub fe_rpc_max_inflight_total: usize,
    #[serde(default = "default_fe_rpc_max_inflight_schema")]
    pub fe_rpc_max_inflight_schema: usize,
    #[serde(default = "default_fe_rpc_max_inflight_exec_status")]
    pub fe_rpc_max_inflight_exec_status: usize,
    #[serde(default = "default_fe_rpc_max_inflight_control")]
    pub fe_rpc_max_inflight_control: usize,
    #[serde(default = "default_fe_rpc_max_inflight_schema_query")]
    pub fe_rpc_max_inflight_schema_query: usize,
    #[serde(default = "default_table_schema_service_max_retries")]
    pub table_schema_service_max_retries: usize,
    #[serde(default = "default_table_schema_service_cache_capacity")]
    pub table_schema_service_cache_capacity: u64,
    #[serde(default = "default_exec_state_report_max_threads")]
    pub exec_state_report_max_threads: usize,
    #[serde(default = "default_priority_exec_state_report_max_threads")]
    pub priority_exec_state_report_max_threads: usize,
    #[serde(default = "default_report_exec_rpc_request_retry_num")]
    pub report_exec_rpc_request_retry_num: usize,
    #[serde(default = "default_report_exec_batch_flush_interval_ms")]
    pub report_exec_batch_flush_interval_ms: u64,
    #[serde(default = "default_report_exec_batch_max_size")]
    pub report_exec_batch_max_size: usize,
    #[serde(default)]
    pub runtime_filter_scan_wait_time_ms_override: Option<i64>,
    #[serde(default)]
    pub runtime_filter_wait_timeout_ms_override: Option<i64>,
    #[serde(default)]
    pub object_storage: ObjectStorageConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub path_rewrite: PathRewriteConfig,
    #[serde(default)]
    pub execution_services: ExecutionServicesConfig,
}

#[derive(Clone, Deserialize)]
pub struct ObjectStorageConfig {
    #[serde(default)]
    pub retry_max_times: Option<usize>,
    #[serde(default)]
    pub retry_min_delay_ms: Option<u64>,
    #[serde(default)]
    pub retry_max_delay_ms: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub io_timeout_ms: Option<u64>,
    #[serde(default = "default_object_storage_retry_log_summary_interval_ms")]
    pub retry_log_summary_interval_ms: u64,
    #[serde(default = "default_object_storage_retry_log_first_n")]
    pub retry_log_first_n: u32,
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

fn default_internal_service_query_rpc_thread_num() -> usize {
    0 // 0 means use CPU cores, aligned with StarRocks internal_service_query_rpc_thread_num
}

fn default_data_runtime_worker_threads() -> usize {
    0 // 0 means use CPU cores for global data runtime
}

fn default_data_runtime_max_blocking_threads() -> usize {
    64
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

fn default_enable_tablet_write_log() -> bool {
    false // aligned with StarRocks enable_tablet_write_log
}

fn default_tablet_write_log_buffer_size() -> usize {
    100_000 // aligned with StarRocks tablet_write_log_buffer_size
}

fn default_be_txn_info_history_size() -> usize {
    20_000 // aligned with StarRocks txn_info_history_size
}

fn default_connector_io_tasks_per_scan_operator() -> i32 {
    16 // aligned with StarRocks BE config::connector_io_tasks_per_scan_operator
}

fn default_io_coalesce_read_enable() -> bool {
    true
}

fn default_io_coalesce_read_max_buffer_size() -> u64 {
    8 * 1024 * 1024 // aligned with StarRocks io_coalesce_read_max_buffer_size
}

fn default_io_coalesce_read_max_distance_size() -> u64 {
    1024 * 1024 // aligned with StarRocks io_coalesce_read_max_distance_size
}

fn default_io_coalesce_adaptive_lazy_active() -> bool {
    true // aligned with StarRocks io_coalesce_adaptive_lazy_active
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

fn default_fe_rpc_connect_timeout_ms() -> u64 {
    5_000
}

fn default_fe_rpc_timeout_ms() -> u64 {
    5_000
}

fn default_fe_rpc_retry_interval_ms() -> u64 {
    100
}

fn default_fe_rpc_pool_max_idle_per_host() -> usize {
    10
}

fn default_fe_rpc_max_inflight_total() -> usize {
    32
}

fn default_fe_rpc_max_inflight_schema() -> usize {
    8
}

fn default_fe_rpc_max_inflight_exec_status() -> usize {
    4
}

fn default_fe_rpc_max_inflight_control() -> usize {
    4
}

fn default_fe_rpc_max_inflight_schema_query() -> usize {
    4
}

fn default_table_schema_service_max_retries() -> usize {
    3
}

fn default_table_schema_service_cache_capacity() -> u64 {
    4_096
}

fn default_exec_state_report_max_threads() -> usize {
    2
}

fn default_priority_exec_state_report_max_threads() -> usize {
    2
}

fn default_report_exec_rpc_request_retry_num() -> usize {
    10
}

fn default_report_exec_batch_flush_interval_ms() -> u64 {
    50
}

fn default_report_exec_batch_max_size() -> usize {
    32
}

fn default_object_storage_retry_log_summary_interval_ms() -> u64 {
    30_000
}

fn default_object_storage_retry_log_first_n() -> u32 {
    3
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
            enable_tablet_write_log: default_enable_tablet_write_log(),
            tablet_write_log_buffer_size: default_tablet_write_log_buffer_size(),
            be_txn_info_history_size: default_be_txn_info_history_size(),
            connector_io_tasks_per_scan_operator: default_connector_io_tasks_per_scan_operator(),
            io_coalesce_read_enable: default_io_coalesce_read_enable(),
            io_coalesce_read_max_buffer_size: default_io_coalesce_read_max_buffer_size(),
            io_coalesce_read_max_distance_size: default_io_coalesce_read_max_distance_size(),
            io_coalesce_adaptive_lazy_active: default_io_coalesce_adaptive_lazy_active(),
            pipeline_scan_thread_pool_queue_size: default_pipeline_scan_thread_pool_queue_size(),
            pipeline_exec_thread_pool_thread_num: default_pipeline_exec_thread_pool_thread_num(),
            internal_service_query_rpc_thread_num: default_internal_service_query_rpc_thread_num(),
            data_runtime_worker_threads: default_data_runtime_worker_threads(),
            data_runtime_max_blocking_threads: default_data_runtime_max_blocking_threads(),
            spill_io_threads: default_spill_io_threads(),
            spill_io_queue_size: default_spill_io_queue_size(),
            scan_submit_fail_max: default_scan_submit_fail_max(),
            scan_submit_fail_timeout_ms: default_scan_submit_fail_timeout_ms(),
            profile_report_interval: default_profile_report_interval(),
            fe_rpc_connect_timeout_ms: default_fe_rpc_connect_timeout_ms(),
            fe_rpc_timeout_ms: default_fe_rpc_timeout_ms(),
            fe_rpc_retry_interval_ms: default_fe_rpc_retry_interval_ms(),
            fe_rpc_pool_max_idle_per_host: default_fe_rpc_pool_max_idle_per_host(),
            fe_rpc_max_inflight_total: default_fe_rpc_max_inflight_total(),
            fe_rpc_max_inflight_schema: default_fe_rpc_max_inflight_schema(),
            fe_rpc_max_inflight_exec_status: default_fe_rpc_max_inflight_exec_status(),
            fe_rpc_max_inflight_control: default_fe_rpc_max_inflight_control(),
            fe_rpc_max_inflight_schema_query: default_fe_rpc_max_inflight_schema_query(),
            table_schema_service_max_retries: default_table_schema_service_max_retries(),
            table_schema_service_cache_capacity: default_table_schema_service_cache_capacity(),
            exec_state_report_max_threads: default_exec_state_report_max_threads(),
            priority_exec_state_report_max_threads: default_priority_exec_state_report_max_threads(
            ),
            report_exec_rpc_request_retry_num: default_report_exec_rpc_request_retry_num(),
            report_exec_batch_flush_interval_ms: default_report_exec_batch_flush_interval_ms(),
            report_exec_batch_max_size: default_report_exec_batch_max_size(),
            runtime_filter_scan_wait_time_ms_override: None,
            runtime_filter_wait_timeout_ms_override: None,
            object_storage: ObjectStorageConfig::default(),
            cache: CacheConfig::default(),
            path_rewrite: PathRewriteConfig::default(),
            execution_services: ExecutionServicesConfig::default(),
        }
    }
}

impl Default for ObjectStorageConfig {
    fn default() -> Self {
        Self {
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
            retry_log_summary_interval_ms: default_object_storage_retry_log_summary_interval_ms(),
            retry_log_first_n: default_object_storage_retry_log_first_n(),
        }
    }
}

#[derive(Clone, Default, Deserialize)]
pub struct PathRewriteConfig {
    #[serde(default)]
    pub enable: bool,
    #[serde(default)]
    pub from_prefix: String,
    #[serde(default)]
    pub to_prefix: String,
}

/// Execution-service resource boundaries (IW-1).
///
/// These knobs size the dedicated `sink_io` runtime and the async-sink queue.
/// Defaults add only a few (mostly idle) threads and do not change all-in-one
/// behavior. `metadata_io` / `commit` / `scan_io` currently alias `data_runtime`
/// and therefore have no size knobs yet.
#[derive(Clone, Deserialize)]
pub struct ExecutionServicesConfig {
    /// Worker threads for the dedicated sink I/O runtime. 0 = min(4, cores).
    #[serde(default = "default_sink_io_worker_threads")]
    pub sink_io_worker_threads: usize,
    /// Max blocking threads for the dedicated sink I/O runtime.
    #[serde(default = "default_sink_io_max_blocking_threads")]
    pub sink_io_max_blocking_threads: usize,
    /// Bounded queue capacity (chunks) for `AsyncSinkOperator` backpressure.
    #[serde(default = "default_async_sink_queue_capacity")]
    pub async_sink_queue_capacity: usize,
}

fn default_sink_io_worker_threads() -> usize {
    0
}

fn default_sink_io_max_blocking_threads() -> usize {
    16
}

fn default_async_sink_queue_capacity() -> usize {
    8
}

impl Default for ExecutionServicesConfig {
    fn default() -> Self {
        Self {
            sink_io_worker_threads: default_sink_io_worker_threads(),
            sink_io_max_blocking_threads: default_sink_io_max_blocking_threads(),
            async_sink_queue_capacity: default_async_sink_queue_capacity(),
        }
    }
}

impl ExecutionServicesConfig {
    /// Resolve sink I/O worker threads; 0 means min(4, cores).
    pub fn actual_sink_io_worker_threads(&self) -> usize {
        if self.sink_io_worker_threads > 0 {
            self.sink_io_worker_threads
        } else {
            let cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            cores.clamp(1, 4)
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

    /// Get the actual number of data-runtime worker threads.
    /// Returns CPU cores if configured as 0.
    pub fn actual_data_runtime_threads(&self) -> usize {
        if self.data_runtime_worker_threads > 0 {
            self.data_runtime_worker_threads
        } else {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        }
    }

    /// Get the actual number of internal-service query RPC threads.
    /// Returns CPU cores if configured as 0.
    pub fn actual_internal_service_query_rpc_threads(&self) -> usize {
        if self.internal_service_query_rpc_thread_num > 0 {
            self.internal_service_query_rpc_thread_num
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
    1024 * 1024
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

#[derive(Clone, Default)]
pub struct DebugConfig {
    pub exec_node_output: bool,
    /// Dump RPC inputs as "named_json" for `exec_plan_fragment` / `exec_batch_plan_fragments`.
    /// This is config-only (no env var fallback).
    pub exec_batch_plan_json: bool,
    #[cfg(debug_assertions)]
    pub fault_inject_submit_fail_after: Option<usize>,
    #[cfg(debug_assertions)]
    pub fault_inject_fetch_not_ready_count: Option<usize>,
    #[cfg(debug_assertions)]
    pub emit_cancel_marker: bool,
}

#[cfg(debug_assertions)]
#[derive(Deserialize, Default)]
#[serde(default)]
struct DebugConfigToml {
    exec_node_output: bool,
    exec_batch_plan_json: bool,
    fault_inject_submit_fail_after: Option<usize>,
    fault_inject_fetch_not_ready_count: Option<usize>,
    emit_cancel_marker: bool,
}

#[cfg(not(debug_assertions))]
#[derive(Deserialize, Default)]
#[serde(default)]
struct DebugConfigToml {
    exec_node_output: bool,
    exec_batch_plan_json: bool,
    fault_inject_submit_fail_after: Option<usize>,
    fault_inject_fetch_not_ready_count: Option<usize>,
    emit_cancel_marker: Option<bool>,
}

impl<'de> Deserialize<'de> for DebugConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = DebugConfigToml::deserialize(deserializer)?;
        #[cfg(debug_assertions)]
        {
            Ok(Self {
                exec_node_output: raw.exec_node_output,
                exec_batch_plan_json: raw.exec_batch_plan_json,
                fault_inject_submit_fail_after: raw.fault_inject_submit_fail_after,
                fault_inject_fetch_not_ready_count: raw.fault_inject_fetch_not_ready_count,
                emit_cancel_marker: raw.emit_cancel_marker,
            })
        }
        #[cfg(not(debug_assertions))]
        {
            if raw.fault_inject_submit_fail_after.is_some() {
                return Err(serde::de::Error::custom(
                    "debug.fault_inject_submit_fail_after is only available in debug builds",
                ));
            }
            if raw.fault_inject_fetch_not_ready_count.is_some() {
                return Err(serde::de::Error::custom(
                    "debug.fault_inject_fetch_not_ready_count is only available in debug builds",
                ));
            }
            if raw.emit_cancel_marker.is_some() {
                return Err(serde::de::Error::custom(
                    "debug.emit_cancel_marker is only available in debug builds",
                ));
            }
            Ok(Self {
                exec_node_output: raw.exec_node_output,
                exec_batch_plan_json: raw.exec_batch_plan_json,
            })
        }
    }
}

impl DebugConfig {
    #[cfg(debug_assertions)]
    pub fn fault_inject_submit_fail_after(&self) -> Option<usize> {
        self.fault_inject_submit_fail_after
    }

    #[cfg(not(debug_assertions))]
    pub fn fault_inject_submit_fail_after(&self) -> Option<usize> {
        None
    }

    #[cfg(debug_assertions)]
    pub fn fault_inject_fetch_not_ready_count(&self) -> Option<usize> {
        self.fault_inject_fetch_not_ready_count
    }

    #[cfg(not(debug_assertions))]
    pub fn fault_inject_fetch_not_ready_count(&self) -> Option<usize> {
        None
    }

    #[cfg(debug_assertions)]
    pub fn emit_cancel_marker(&self) -> bool {
        self.emit_cancel_marker
    }

    #[cfg(not(debug_assertions))]
    pub fn emit_cancel_marker(&self) -> bool {
        false
    }
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
    use std::path::PathBuf;

    use super::{
        MetadataProviderConfig, NovaRocksConfig, RuntimeConfig, StandaloneObjectStoreConfig,
        StandaloneServerConfig, StandaloneStarRocksTableConfig,
    };

    #[test]
    fn test_server_priority_networks_default_is_empty() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[server]
http_port = 8040
"#,
        )
        .expect("parse config");
        assert!(cfg.server.priority_networks.is_empty());
    }

    #[test]
    fn test_server_host_default_is_loopback() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[server]
http_port = 8040
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.server.host, "127.0.0.1");
    }

    #[test]
    fn test_server_priority_networks_can_be_overridden() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[server]
http_port = 8040
priority_networks = "10.10.10.0/24;192.168.0.0/16"
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.server.priority_networks, "10.10.10.0/24;192.168.0.0/16");
    }

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
    fn test_server_grpc_port_default_is_9080() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[server]
http_port = 8040
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.server.grpc_port, 9080);
    }

    #[test]
    fn test_server_grpc_port_can_be_overridden() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[server]
grpc_port = 19080
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.server.grpc_port, 19080);
    }

    #[test]
    fn test_standalone_server_defaults() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[standalone_server]
"#,
        )
        .expect("parse config");
        assert_eq!(
            cfg.standalone_server,
            Some(StandaloneServerConfig {
                mysql_port: 9030,
                user: "root".to_string(),
                warehouse_uri: None,
                object_store: None,
                mv_default_storage_engine: None,
                mv_refresh_scheduler_enabled: false,
                mv_refresh_scheduler_interval_ms: 30_000,
                mv_refresh_scheduler_max_concurrent: 1,
                mv_refresh_scheduler_failure_backoff_ms: 60_000,
                mv_refresh_scheduler_max_failure_backoff_ms: 1_800_000,
                mv_refresh_max_touched_groups: 100_000,
                mv_refresh_max_affected_partitions: 4_096,
                mv_partition_state_max_entries: 10_000,
                iceberg_maintenance_enabled: true,
                iceberg_maintenance_tick_interval_ms: 600_000,
                iceberg_maintenance_max_concurrent: 1,
                iceberg_maintenance_compaction_min_data_files: 100,
                iceberg_maintenance_dv_min_delete_files: 10,
                iceberg_maintenance_action_cooldown_ms: 3_600_000,
                iceberg_maintenance_max_consecutive_failures: 4,
            })
        );
    }

    #[test]
    fn standalone_server_config_iceberg_maintenance_defaults() {
        let cfg: StandaloneServerConfig =
            toml::from_str("").expect("empty standalone_server section parses");
        assert!(cfg.iceberg_maintenance_enabled);
        assert_eq!(cfg.iceberg_maintenance_tick_interval_ms, 600_000);
        assert_eq!(cfg.iceberg_maintenance_max_concurrent, 1);
        assert_eq!(cfg.iceberg_maintenance_compaction_min_data_files, 100);
        assert_eq!(cfg.iceberg_maintenance_dv_min_delete_files, 10);
        assert_eq!(cfg.iceberg_maintenance_action_cooldown_ms, 3_600_000);
        assert_eq!(cfg.iceberg_maintenance_max_consecutive_failures, 4);
        assert_eq!(cfg.mv_refresh_max_touched_groups, 100_000);
        assert_eq!(cfg.mv_refresh_max_affected_partitions, 4_096);
        assert_eq!(cfg.mv_partition_state_max_entries, 10_000);
        assert_eq!(cfg, StandaloneServerConfig::default());
    }

    #[test]
    fn standalone_server_config_mv_refresh_pruning_overrides() {
        let cfg: StandaloneServerConfig = toml::from_str(
            r#"
mv_refresh_max_touched_groups = 7
mv_refresh_max_affected_partitions = 3
mv_partition_state_max_entries = 11
"#,
        )
        .expect("standalone_server pruning thresholds parse");

        assert_eq!(cfg.mv_refresh_max_touched_groups, 7);
        assert_eq!(cfg.mv_refresh_max_affected_partitions, 3);
        assert_eq!(cfg.mv_partition_state_max_entries, 11);
    }

    #[test]
    fn test_metadata_config_parses_sqlite_provider() {
        let toml = r#"
[metadata]
provider = "sqlite"
path = "meta/catalog.db"

[standalone_server]
mysql_port = 19030
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse config");
        let metadata = cfg.metadata.expect("metadata config");
        assert_eq!(metadata.provider, MetadataProviderConfig::Sqlite);
        assert_eq!(metadata.path, PathBuf::from("meta/catalog.db"));
    }

    #[test]
    fn test_standalone_server_parses_starrocks_table_object_store() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[standalone_server]
mysql_port = 9030
user = "root"
warehouse_uri = "s3://novarocks/standalone"

[standalone_server.object_store]
endpoint = "http://127.0.0.1:9000"
access_key_id = "admin"
access_key_secret = "admin123"
enable_path_style_access = true
"#,
        )
        .expect("parse config");

        let standalone = cfg.standalone_server.expect("standalone config");
        assert_eq!(
            standalone.warehouse_uri.as_deref(),
            Some("s3://novarocks/standalone")
        );
        let object_store = standalone.object_store.expect("object_store");
        assert_eq!(
            object_store.endpoint.as_deref(),
            Some("http://127.0.0.1:9000")
        );
        assert_eq!(object_store.access_key_id.as_deref(), Some("admin"));
        assert_eq!(object_store.enable_path_style_access, Some(true));
    }

    #[test]
    fn test_standalone_starrocks_table_fixture_loads() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/sql-test-runner/conf/standalone_starrocks_table.toml");
        let cfg = NovaRocksConfig::load_from_file(&path).expect("load fixture config");
        let metadata = cfg.metadata.expect("metadata config");
        assert_eq!(
            metadata.path,
            PathBuf::from("tmp/standalone-starrocks-table.sqlite")
        );

        let standalone = cfg.standalone_server.expect("standalone config");
        assert_eq!(
            standalone.warehouse_uri.as_deref(),
            Some("s3://novarocks/sql-tests-starrocks-table")
        );
        assert_eq!(standalone.mysql_port, 9030);
        assert!(standalone.object_store.is_some());
    }

    #[test]
    #[cfg(debug_assertions)]
    fn test_debug_fault_injection_knobs_parse_in_debug_builds() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[debug]
fault_inject_submit_fail_after = 1
fault_inject_fetch_not_ready_count = 2
emit_cancel_marker = true
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.debug.fault_inject_submit_fail_after, Some(1));
        assert_eq!(cfg.debug.fault_inject_fetch_not_ready_count, Some(2));
        assert!(cfg.debug.emit_cancel_marker);
    }

    #[test]
    #[cfg(not(debug_assertions))]
    fn test_debug_fault_injection_knobs_are_rejected_in_release_builds() {
        let err = match toml::from_str::<NovaRocksConfig>(
            r#"
[debug]
fault_inject_submit_fail_after = 1
"#,
        ) {
            Ok(_) => panic!("release config must reject fault injection knobs"),
            Err(err) => err,
        };
        let err = err.to_string();
        assert!(
            err.contains("fault_inject_submit_fail_after"),
            "unexpected parse error: {err}"
        );

        let err = match toml::from_str::<NovaRocksConfig>(
            r#"
[debug]
emit_cancel_marker = false
"#,
        ) {
            Ok(_) => panic!("release config must reject emit_cancel_marker knob"),
            Err(err) => err,
        };
        let err = err.to_string();
        assert!(
            err.contains("emit_cancel_marker"),
            "unexpected parse error: {err}"
        );
    }

    #[test]
    fn test_standalone_server_defaults_without_starrocks_table_section() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[standalone_server]
mysql_port = 9030
user = "root"
"#,
        )
        .expect("parse config");

        let standalone = cfg.standalone_server.expect("standalone config");
        assert_eq!(standalone.warehouse_uri, None);
        assert!(standalone.object_store.is_none());
    }

    #[test]
    fn test_standalone_server_starrocks_table_config_normalizes_required_fields() {
        let standalone = StandaloneServerConfig {
            warehouse_uri: Some(" s3://novarocks/standalone ".to_string()),
            object_store: Some(StandaloneObjectStoreConfig {
                endpoint: Some(" http://127.0.0.1:9000 ".to_string()),
                access_key_id: Some(" admin ".to_string()),
                access_key_secret: Some(" admin123 ".to_string()),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            }),
            ..StandaloneServerConfig::default()
        };

        assert_eq!(
            standalone
                .starrocks_table_config()
                .expect("StarRocks table config"),
            Some(StandaloneStarRocksTableConfig {
                warehouse_uri: "s3://novarocks/standalone".to_string(),
                endpoint: "http://127.0.0.1:9000".to_string(),
                access_key_id: "admin".to_string(),
                access_key_secret: "admin123".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
                mv_default_storage_engine: None,
            })
        );
    }

    #[test]
    fn test_standalone_server_starrocks_table_config_propagates_mv_default_storage_engine() {
        let standalone = StandaloneServerConfig {
            warehouse_uri: Some("s3://bucket/wh".to_string()),
            object_store: Some(StandaloneObjectStoreConfig {
                endpoint: Some("http://localhost:9000".to_string()),
                access_key_id: Some("ak".to_string()),
                access_key_secret: Some("sk".to_string()),
                region: None,
                enable_path_style_access: Some(true),
            }),
            mv_default_storage_engine: Some("iceberg".to_string()),
            ..StandaloneServerConfig::default()
        };
        let cfg = standalone
            .starrocks_table_config()
            .expect("ok")
            .expect("some");
        assert_eq!(cfg.mv_default_storage_engine.as_deref(), Some("iceberg"));
    }

    #[test]
    fn test_standalone_server_starrocks_table_config_returns_none_without_warehouse_uri() {
        let standalone = StandaloneServerConfig::default();
        assert_eq!(
            standalone
                .starrocks_table_config()
                .expect("StarRocks table config"),
            None
        );

        let standalone = StandaloneServerConfig {
            warehouse_uri: Some("   ".to_string()),
            ..StandaloneServerConfig::default()
        };
        assert_eq!(
            standalone
                .starrocks_table_config()
                .expect("StarRocks table config"),
            None
        );
    }

    #[test]
    fn test_standalone_server_starrocks_table_config_validates_required_object_store_fields() {
        let base = StandaloneServerConfig {
            warehouse_uri: Some("s3://novarocks/standalone".to_string()),
            object_store: Some(StandaloneObjectStoreConfig {
                endpoint: Some("http://127.0.0.1:9000".to_string()),
                access_key_id: Some("admin".to_string()),
                access_key_secret: Some("admin123".to_string()),
                region: None,
                enable_path_style_access: None,
            }),
            ..StandaloneServerConfig::default()
        };

        let cases = vec![
            (
                StandaloneServerConfig {
                    object_store: None,
                    ..base.clone()
                },
                "standalone StarRocks table requires [standalone_server.object_store]",
            ),
            (
                StandaloneServerConfig {
                    object_store: Some(StandaloneObjectStoreConfig {
                        endpoint: None,
                        ..base.object_store.clone().expect("object store")
                    }),
                    ..base.clone()
                },
                "standalone StarRocks table requires object_store.endpoint",
            ),
            (
                StandaloneServerConfig {
                    object_store: Some(StandaloneObjectStoreConfig {
                        access_key_id: None,
                        ..base.object_store.clone().expect("object store")
                    }),
                    ..base.clone()
                },
                "standalone StarRocks table requires object_store.access_key_id",
            ),
            (
                StandaloneServerConfig {
                    object_store: Some(StandaloneObjectStoreConfig {
                        access_key_secret: None,
                        ..base.object_store.clone().expect("object store")
                    }),
                    ..base
                },
                "standalone StarRocks table requires object_store.access_key_secret",
            ),
        ];

        for (standalone, expected) in cases {
            assert_eq!(
                standalone
                    .starrocks_table_config()
                    .expect_err("StarRocks table error"),
                expected
            );
        }
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

    #[test]
    fn test_runtime_data_runtime_defaults() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.runtime.data_runtime_worker_threads, 0);
        assert_eq!(cfg.runtime.data_runtime_max_blocking_threads, 64);
    }

    #[test]
    fn test_runtime_data_runtime_can_be_overridden() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
data_runtime_worker_threads = 6
data_runtime_max_blocking_threads = 99
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.runtime.data_runtime_worker_threads, 6);
        assert_eq!(cfg.runtime.data_runtime_max_blocking_threads, 99);
    }

    #[test]
    fn test_actual_data_runtime_threads_behavior() {
        let mut runtime = RuntimeConfig {
            data_runtime_worker_threads: 0,
            ..Default::default()
        };
        let expected = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert_eq!(runtime.actual_data_runtime_threads(), expected);
        runtime.data_runtime_worker_threads = 3;
        assert_eq!(runtime.actual_data_runtime_threads(), 3);
    }

    #[test]
    fn test_runtime_internal_service_query_rpc_threads_default_to_auto() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.runtime.internal_service_query_rpc_thread_num, 0);
    }

    #[test]
    fn test_runtime_internal_service_query_rpc_threads_can_be_overridden() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
internal_service_query_rpc_thread_num = 7
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.runtime.internal_service_query_rpc_thread_num, 7);
    }

    #[test]
    fn test_actual_internal_service_query_rpc_threads_behavior() {
        let mut runtime = RuntimeConfig {
            internal_service_query_rpc_thread_num: 0,
            ..Default::default()
        };
        let expected = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert_eq!(
            runtime.actual_internal_service_query_rpc_threads(),
            expected
        );
        runtime.internal_service_query_rpc_thread_num = 5;
        assert_eq!(runtime.actual_internal_service_query_rpc_threads(), 5);
    }

    #[test]
    fn test_cluster_default_is_all_in_one() {
        let toml = r#"
[server]
host = "127.0.0.1"
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse default");
        assert_eq!(cfg.cluster.role, super::ClusterRole::AllInOne);
        assert!(cfg.cluster.backends.is_empty());
    }

    #[test]
    fn test_cluster_role_fe_with_single_backend() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["127.0.0.1:9070"]
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse fe");
        assert_eq!(cfg.cluster.role, super::ClusterRole::Fe);
        assert_eq!(cfg.cluster.backends, vec!["127.0.0.1:9070".to_string()]);
    }

    #[test]
    fn test_cluster_role_be_rejects_backends() {
        let toml = r#"
[cluster]
role = "be"
backends = ["127.0.0.1:9070"]
"#;
        let parsed: NovaRocksConfig = toml::from_str(toml).expect("parse be with backends");
        let err = parsed
            .cluster
            .validate()
            .expect_err("be with backends should fail");
        assert!(err.contains("backends"));
    }

    #[test]
    fn test_cluster_role_fe_with_three_backends_passes() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["10.0.0.1:9070", "10.0.0.2:9070", "10.0.0.3:9070"]
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse fe with 3 backends");
        cfg.cluster
            .validate()
            .expect("3 backends should pass D2 validate");
    }

    #[test]
    fn test_cluster_role_fe_rejects_duplicate_backends() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["10.0.0.1:9070", "10.0.0.1:9070"]
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
        let err = cfg
            .cluster
            .validate()
            .expect_err("duplicate backends should fail");
        assert!(err.contains("duplicate") || err.contains("10.0.0.1:9070"));
    }

    #[test]
    fn test_cluster_role_fe_rejects_malformed_backend() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["not-a-socket-addr"]
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
        let err = cfg
            .cluster
            .validate()
            .expect_err("malformed addr should fail");
        assert!(err.contains("not-a-socket-addr") || err.contains("invalid"));
    }

    #[test]
    fn test_cluster_role_fe_empty_backends_allowed() {
        let toml = r#"
[cluster]
role = "fe"
backends = []
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
        cfg.cluster
            .validate()
            .expect("role=fe may start with no configured backends");
    }

    #[test]
    fn test_cluster_role_invalid_rejected() {
        let toml = r#"
[cluster]
role = "leader"
"#;
        let result: Result<NovaRocksConfig, _> = toml::from_str(toml);
        assert!(result.is_err(), "invalid role string should fail parse");
    }

    #[test]
    fn test_all_in_one_rejects_non_empty_backends() {
        // I2: role=all-in-one with non-empty backends must be rejected.
        let toml = r#"
[cluster]
role = "all-in-one"
backends = ["127.0.0.1:9070"]
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse all-in-one with backends");
        let err = cfg
            .cluster
            .validate()
            .expect_err("all-in-one with backends should fail");
        assert!(
            err.contains("all-in-one") && err.contains("backends"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_all_in_one_with_no_backends_passes_validation() {
        // Default all-in-one with no backends must still pass.
        let toml = r#"
[cluster]
role = "all-in-one"
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse all-in-one");
        cfg.cluster
            .validate()
            .expect("all-in-one with no backends should pass");
    }

    #[test]
    fn test_all_in_one_rejects_multiple_backends() {
        // I2: multiple backends should also be rejected for all-in-one.
        let toml = r#"
[cluster]
role = "all-in-one"
backends = ["127.0.0.1:9070", "127.0.0.1:9071"]
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
        let err = cfg
            .cluster
            .validate()
            .expect_err("all-in-one with 2 backends should fail");
        assert!(
            err.contains("all-in-one") && err.contains("backends"),
            "unexpected error: {err}"
        );
        // Error message should contain the count.
        assert!(err.contains('2'), "expected count 2 in error: {err}");
    }

    #[test]
    fn execution_services_defaults_are_sane() {
        let cfg = RuntimeConfig::default();
        assert_eq!(cfg.execution_services.sink_io_max_blocking_threads, 16);
        assert_eq!(cfg.execution_services.async_sink_queue_capacity, 8);
        // 0 means "derive from cores"; resolved value must be >= 1.
        assert!(cfg.execution_services.actual_sink_io_worker_threads() >= 1);
        assert!(cfg.execution_services.actual_sink_io_worker_threads() <= 4);
    }
}
