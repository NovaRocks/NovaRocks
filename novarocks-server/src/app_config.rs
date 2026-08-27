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
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer};
use std::path::{Path, PathBuf};

use crate::catalog_source_config::{CatalogSourceConfig, preflight_catalog_source};
use crate::env_reference::resolve_env_references;
use crate::state_store_config::{StateStoreAppConfig, StateStoreConfig};
use crate::state_store_limits::StateStoreLimitOverrides;
use novarocks_native_trust::NativeTransportMode;
use novarocks_secret::SecretValue;
use novarocks_state_store_sqlite::SqliteHistoryRetentionConfig;
use novarocks_types::{ClusterRole, NativeEndpoint};

pub use crate::memory_limit::DEFAULT_MEM_LIMIT_SPEC;

pub const DEFAULT_FRONTEND_DRAIN_TIMEOUT_MS: u64 = 300_000;
pub const DEFAULT_FRONTEND_CLEANUP_TIMEOUT_MS: u64 = 30_000;

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

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StateStoreProviderKindWire {
    Sqlite,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreAppConfigWire {
    provider: StateStoreProviderKindWire,
    cluster_id: String,
    path: PathBuf,
    #[serde(default)]
    limits: StateStoreLimitOverridesWire,
    #[serde(default)]
    history_retention: SqliteHistoryRetentionConfigWire,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateStoreLimitOverridesWire {
    max_key_bytes: Option<usize>,
    max_value_bytes: Option<usize>,
    max_page_size: Option<usize>,
    max_transaction_operations: Option<usize>,
    max_transaction_bytes: Option<usize>,
    transaction_deadline_ms: Option<u64>,
    runner_max_attempts: Option<usize>,
}
impl From<StateStoreLimitOverridesWire> for StateStoreLimitOverrides {
    fn from(w: StateStoreLimitOverridesWire) -> Self {
        Self {
            max_key_bytes: w.max_key_bytes,
            max_value_bytes: w.max_value_bytes,
            max_page_size: w.max_page_size,
            max_transaction_operations: w.max_transaction_operations,
            max_transaction_bytes: w.max_transaction_bytes,
            transaction_deadline_ms: w.transaction_deadline_ms,
            runner_max_attempts: w.runner_max_attempts,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SqliteHistoryRetentionConfigWire {
    max_age_secs: Option<u64>,
    max_change_rows: Option<usize>,
    max_commit_receipts: Option<usize>,
    maintenance_interval_commits: Option<usize>,
    incremental_vacuum_pages: Option<usize>,
}
impl From<SqliteHistoryRetentionConfigWire> for SqliteHistoryRetentionConfig {
    fn from(w: SqliteHistoryRetentionConfigWire) -> Self {
        let defaults = SqliteHistoryRetentionConfig::default();
        Self {
            max_age_secs: w.max_age_secs.unwrap_or(defaults.max_age_secs),
            max_change_rows: w.max_change_rows.unwrap_or(defaults.max_change_rows),
            max_commit_receipts: w
                .max_commit_receipts
                .unwrap_or(defaults.max_commit_receipts),
            maintenance_interval_commits: w
                .maintenance_interval_commits
                .unwrap_or(defaults.maintenance_interval_commits),
            incremental_vacuum_pages: w
                .incremental_vacuum_pages
                .unwrap_or(defaults.incremental_vacuum_pages),
        }
    }
}

fn state_store_from_wire(wire: StateStoreAppConfigWire) -> StateStoreAppConfig {
    let StateStoreProviderKindWire::Sqlite = wire.provider;
    StateStoreAppConfig {
        store: StateStoreConfig {
            cluster_id: wire.cluster_id,
            path: wire.path,
            limits: wire.limits.into(),
            history_retention: wire.history_retention.into(),
        },
    }
}
fn deserialize_state_store<'de, D: Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<StateStoreAppConfig>, D::Error> {
    Ok(Option::<StateStoreAppConfigWire>::deserialize(d)?.map(state_store_from_wire))
}

/// Configuration for the `[cluster]` TOML section.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    pub role: ClusterRole,
    /// Exact logical FE native endpoint used by a BE to announce itself.
    /// FE membership is self-registration only; no role accepts a BE seed list.
    pub frontend_endpoint: Option<String>,
    pub advertise_host: String,
    pub advertise_port: u16,
    pub heartbeat_interval_ms: Option<u64>,
    pub heartbeat_timeout_retries: Option<u32>,
    pub backend_announce_lease_ttl_ms: Option<u64>,
    pub backend_announce_interval_ms: Option<u64>,
    pub backend_announce_initial_backoff_ms: Option<u64>,
    pub backend_announce_max_backoff_ms: Option<u64>,
}

fn default_heartbeat_interval_ms() -> u64 {
    1000
}

fn default_heartbeat_timeout_retries() -> u32 {
    3
}

fn default_backend_announce_lease_ttl_ms() -> u64 {
    5000
}

fn default_backend_announce_interval_ms() -> u64 {
    1000
}

fn default_backend_announce_initial_backoff_ms() -> u64 {
    100
}

fn default_backend_announce_max_backoff_ms() -> u64 {
    2000
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            role: ClusterRole::Fe,
            frontend_endpoint: None,
            advertise_host: String::new(),
            advertise_port: 0,
            heartbeat_interval_ms: None,
            heartbeat_timeout_retries: None,
            backend_announce_lease_ttl_ms: None,
            backend_announce_interval_ms: None,
            backend_announce_initial_backoff_ms: None,
            backend_announce_max_backoff_ms: None,
        }
    }
}

impl ClusterConfig {
    /// Validate cluster config consistency. Called at startup after parsing.
    pub fn validate(&self) -> Result<(), String> {
        match self.role {
            ClusterRole::Fe if self.frontend_endpoint.is_some() => {
                return Err("role=fe must not configure [cluster].frontend_endpoint".to_string());
            }
            ClusterRole::Fe => {
                if self.backend_announce_interval_ms.is_some()
                    || self.backend_announce_initial_backoff_ms.is_some()
                    || self.backend_announce_max_backoff_ms.is_some()
                {
                    return Err(
                        "role=fe must not configure BE announce cadence or backoff".to_string()
                    );
                }
                if self.heartbeat_interval_ms() == 0
                    || self.heartbeat_timeout_retries() == 0
                    || self.backend_announce_lease_ttl_ms() == 0
                {
                    return Err(
                        "FE heartbeat and announce lease settings must be nonzero".to_string()
                    );
                }
            }
            ClusterRole::Be => {
                self.frontend_endpoint
                    .as_deref()
                    .ok_or_else(|| {
                        "role=be requires [cluster].frontend_endpoint for authenticated self-registration"
                            .to_string()
                    })?
                    .parse::<NativeEndpoint>()
                    .map_err(|error| {
                        format!("invalid [cluster].frontend_endpoint: {error}")
                    })?;
                if self.heartbeat_interval_ms.is_some()
                    || self.heartbeat_timeout_retries.is_some()
                    || self.backend_announce_lease_ttl_ms.is_some()
                {
                    return Err(
                        "role=be must not configure FE heartbeat or announce lease settings"
                            .to_string(),
                    );
                }
                if self.backend_announce_initial_backoff_ms()
                    > self.backend_announce_max_backoff_ms()
                {
                    return Err(
                        "[cluster].backend_announce_max_backoff_ms must be at least backend_announce_initial_backoff_ms"
                            .to_string(),
                    );
                }
                if self.backend_announce_interval_ms() == 0
                    || self.backend_announce_initial_backoff_ms() == 0
                    || self.backend_announce_max_backoff_ms() == 0
                {
                    return Err(
                        "BE announce cadence and backoff settings must be nonzero".to_string()
                    );
                }
            }
        }
        Ok(())
    }

    pub fn heartbeat_interval_ms(&self) -> u64 {
        self.heartbeat_interval_ms
            .unwrap_or_else(default_heartbeat_interval_ms)
    }

    pub fn heartbeat_timeout_retries(&self) -> u32 {
        self.heartbeat_timeout_retries
            .unwrap_or_else(default_heartbeat_timeout_retries)
    }

    pub fn backend_announce_lease_ttl_ms(&self) -> u64 {
        self.backend_announce_lease_ttl_ms
            .unwrap_or_else(default_backend_announce_lease_ttl_ms)
    }

    pub fn backend_announce_interval_ms(&self) -> u64 {
        self.backend_announce_interval_ms
            .unwrap_or_else(default_backend_announce_interval_ms)
    }

    pub fn backend_announce_initial_backoff_ms(&self) -> u64 {
        self.backend_announce_initial_backoff_ms
            .unwrap_or_else(default_backend_announce_initial_backoff_ms)
    }

    pub fn backend_announce_max_backoff_ms(&self) -> u64 {
        self.backend_announce_max_backoff_ms
            .unwrap_or_else(default_backend_announce_max_backoff_ms)
    }
}

#[cfg(test)]
mod cluster_hb_tests {
    use super::ClusterConfig;

    #[test]
    fn cluster_config_heartbeat_defaults() {
        let c = ClusterConfig::default();
        assert_eq!(c.heartbeat_interval_ms(), 1000);
        assert_eq!(c.heartbeat_timeout_retries(), 3);
        assert_eq!(c.backend_announce_lease_ttl_ms(), 5000);
        assert_eq!(c.backend_announce_interval_ms(), 1000);
        assert_eq!(c.backend_announce_initial_backoff_ms(), 100);
        assert_eq!(c.backend_announce_max_backoff_ms(), 2000);
    }

    #[test]
    fn cluster_config_parses_heartbeat_overrides() {
        let toml = r#"
            role = "fe"
            heartbeat_interval_ms = 2000
            heartbeat_timeout_retries = 5
        "#;
        let c: ClusterConfig = toml::from_str(toml).unwrap();
        assert_eq!(c.heartbeat_interval_ms(), 2000);
        assert_eq!(c.heartbeat_timeout_retries(), 5);
        assert_eq!(c.backend_announce_lease_ttl_ms(), 5000);
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

/// Loads the config at `path`, falling back to built-in defaults when the file
/// is absent.
///
/// The result is a value the caller owns. There is no process-wide active
/// config: whoever loads the config hands it to the components that need it.
pub fn load_from_path(path: impl AsRef<Path>) -> Result<NovaRocksConfig> {
    let path = path.as_ref().to_path_buf();
    if !path.exists() {
        eprintln!(
            "WARNING: config file '{}' not found, using built-in defaults",
            path.display()
        );
        return Ok(NovaRocksConfig::default());
    }
    NovaRocksConfig::load_from_file(&path)
}

/// Loads the config named by `NOVAROCKS_CONFIG`, else `./novarocks.toml`, else
/// the built-in defaults.
pub fn load_from_env_or_default() -> Result<NovaRocksConfig> {
    if let Ok(p) = std::env::var("NOVAROCKS_CONFIG") {
        let p = p.trim();
        if !p.is_empty() {
            return load_from_path(PathBuf::from(p));
        }
    }

    let default_path = PathBuf::from("novarocks.toml");
    if default_path.exists() {
        return NovaRocksConfig::load_from_file(&default_path);
    }

    eprintln!("WARNING: config file 'novarocks.toml' not found, using built-in defaults");
    Ok(NovaRocksConfig::default())
}

/// Load only the resolved object-store input needed by offline tooling.
pub fn load_object_store_config(
    explicit: Option<&Path>,
) -> Result<novarocks_fs::ObjectStoreConfig> {
    let config = match explicit {
        Some(path) => load_from_path(path)?,
        None => load_from_env_or_default()?,
    };
    config
        .connector
        .object_store_config(&config.runtime.object_storage.retry_settings())
        .map_err(anyhow::Error::msg)?
        .context("missing [connector.object_store] config")
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

    /// FE-only closed desired-state source selection. It is validated and, for
    /// StaticFile, fully parsed by deployable-role preflight.
    #[serde(default)]
    pub catalog_source: Option<CatalogSourceConfig>,

    #[serde(default)]
    pub runtime: RuntimeConfig,

    #[serde(default, deserialize_with = "deserialize_state_store")]
    pub state_store: Option<StateStoreAppConfig>,

    #[serde(default, rename = "foundationdb_client")]
    rejected_foundationdb_client: Option<toml::Value>,

    #[serde(default)]
    pub standalone_server: Option<StandaloneServerConfig>,

    #[serde(default, deserialize_with = "deserialize_connector_config")]
    pub connector: ConnectorConfig,

    #[serde(default)]
    pub spill: SpillStorageConfig,

    #[serde(default)]
    pub cluster: ClusterConfig,

    #[serde(default, deserialize_with = "deserialize_native_trust_config")]
    pub native_trust: Option<NativeTrustConfig>,
}

impl NovaRocksConfig {
    // Design: ADR-0107 (docs/adr/ADR-0107-static-startup-secret-resolution.md)
    pub fn load_from_file(path: &Path) -> Result<Self> {
        deserialize_loaded_config(path, load_resolved_config_value(path)?)
    }

    /// Load a deployable role configuration. Unlike the generic deserializer
    /// used by in-process test builders, a server config must name its role
    /// explicitly and may only describe one application role.
    pub fn load_deployable_from_file(path: &Path) -> Result<Self> {
        let document = load_resolved_config_value(path)?;
        let cluster = document
            .get("cluster")
            .and_then(toml::Value::as_table)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "config {}: missing required [cluster] table",
                    path.display()
                )
            })?;
        let role = cluster
            .get("role")
            .and_then(toml::Value::as_str)
            .ok_or_else(|| {
                anyhow::anyhow!("config {}: missing required [cluster].role", path.display())
            })?;
        if !matches!(role, "fe" | "be") {
            bail!(
                "config {}: [cluster].role must be `fe` or `be`, got `{role}`",
                path.display()
            );
        }

        let mut cfg = deserialize_loaded_config(path, document)?;
        cfg.cluster
            .validate()
            .map_err(anyhow::Error::msg)
            .with_context(|| format!("validate [cluster]: {}", path.display()))?;
        if cfg.native_trust.is_none() {
            bail!(
                "config {}: missing required [native_trust] table",
                path.display()
            );
        }
        preflight_catalog_source(&mut cfg, path)?;
        Ok(cfg)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeTrustConfig {
    pub deployment_id: String,
    pub shared_secret: SecretValue,
    pub transport: NativeTrustTransportConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeTrustTransportConfig {
    pub mode: NativeTransportMode,
    pub certificate_chain_path: Option<PathBuf>,
    pub private_key_path: Option<PathBuf>,
    pub trust_roots_path: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeTrustConfigWire {
    deployment_id: String,
    shared_secret: String,
    #[serde(default)]
    transport: NativeTrustTransportConfigWire,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct NativeTrustTransportConfigWire {
    mode: Option<String>,
    certificate_chain_path: Option<PathBuf>,
    private_key_path: Option<PathBuf>,
    trust_roots_path: Option<PathBuf>,
}

impl Default for NativeTrustTransportConfigWire {
    fn default() -> Self {
        Self {
            mode: None,
            certificate_chain_path: None,
            private_key_path: None,
            trust_roots_path: None,
        }
    }
}

fn deserialize_native_trust_config<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<NativeTrustConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let wire = Option::<NativeTrustConfigWire>::deserialize(deserializer)?;
    wire.map(|wire| {
        let mode = match wire.transport.mode.as_deref().unwrap_or("disabled") {
            "disabled" => NativeTransportMode::Disabled,
            "automatic" => NativeTransportMode::Automatic,
            "pem" => NativeTransportMode::Pem,
            _ => return Err(serde::de::Error::custom("native_trust.transport.mode must be disabled, automatic, or pem")),
        };
        let transport = NativeTrustTransportConfig {
            mode,
            certificate_chain_path: wire.transport.certificate_chain_path,
            private_key_path: wire.transport.private_key_path,
            trust_roots_path: wire.transport.trust_roots_path,
        };
        let has_pem_paths = transport.certificate_chain_path.is_some()
            || transport.private_key_path.is_some()
            || transport.trust_roots_path.is_some();
        if mode == NativeTransportMode::Pem {
            if transport.certificate_chain_path.is_none()
                || transport.private_key_path.is_none()
                || transport.trust_roots_path.is_none()
            {
                return Err(serde::de::Error::custom("native_trust.transport.mode=pem requires certificate_chain_path, private_key_path, and trust_roots_path"));
            }
        } else if has_pem_paths {
            return Err(serde::de::Error::custom("native_trust PEM paths are only valid when transport.mode=pem"));
        }
        Ok(NativeTrustConfig {
            deployment_id: wire.deployment_id,
            shared_secret: SecretValue::new(wire.shared_secret),
            transport,
        })
    }).transpose()
}

fn load_resolved_config_value(path: &Path) -> Result<toml::Value> {
    let source = std::fs::read_to_string(path)
        .with_context(|| format!("read config file: {}", path.display()))?;
    let mut value: toml::Value = toml::from_str(&source)
        .with_context(|| format!("parse config TOML: {}", path.display()))?;
    resolve_env_references(&mut value)
        .with_context(|| format!("resolve config environment references: {}", path.display()))?;
    Ok(value)
}

fn deserialize_loaded_config(path: &Path, value: toml::Value) -> Result<NovaRocksConfig> {
    let cfg: NovaRocksConfig = value
        .try_into()
        .with_context(|| format!("deserialize config TOML: {}", path.display()))?;
    validate_state_store_configuration(&cfg)?;
    validate_query_control_config(&cfg.runtime)?;
    validate_lake_publication_runtime_policy(&cfg.runtime)?;
    #[cfg(not(debug_assertions))]
    reject_fault_injection_environment()?;
    Ok(cfg)
}

/// Reject runner-owned fault-injection environment variables in release builds.
///
/// The fault hooks themselves read these variables directly (see
/// `novarocks-failpoint`) and are compiled
/// out of release builds. Failing startup here keeps a release binary from
/// silently ignoring an armed fault and letting a cross-process test pass
/// vacuously.
#[cfg(not(debug_assertions))]
fn reject_fault_injection_environment() -> Result<()> {
    for name in [
        novarocks_failpoint::QUERY_LIFECYCLE_FAULT_DIR_ENV,
        novarocks_failpoint::CLEANUP_FAULT_DIR_ENV,
        "NOVAROCKS_SQL_TEST_FAULT_INJECT_FETCH_NOT_READY_COUNT",
        "NOVAROCKS_SQL_TEST_EMIT_CANCEL_MARKER",
        "NOVAROCKS_SQL_TEST_EMIT_GRPC_FRAGMENT_MARKER",
        "NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER",
        "NOVAROCKS_DEBUG_EXEC_NODE_OUTPUT",
    ] {
        if std::env::var_os(name).is_some() {
            bail!("{name} is only available in debug builds");
        }
    }
    Ok(())
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
            catalog_source: None,
            runtime: RuntimeConfig::default(),
            state_store: None,
            rejected_foundationdb_client: None,
            standalone_server: None,
            connector: ConnectorConfig::default(),
            spill: SpillStorageConfig::default(),
            cluster: ClusterConfig::default(),
            native_trust: None,
        }
    }
}

fn validate_state_store_configuration(config: &NovaRocksConfig) -> Result<()> {
    if config.rejected_foundationdb_client.is_some() {
        bail!("InvalidStateStoreConfig: [foundationdb_client] is not supported by the server");
    }
    if let Some(state_store) = &config.state_store {
        state_store.validate()?;
    }
    Ok(())
}

#[derive(Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_server_host")]
    pub host: String,
    #[serde(default)]
    pub priority_networks: String,
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    #[serde(default = "default_grpc_port")]
    pub grpc_port: u16,
    #[serde(default = "default_frontend_drain_timeout_ms")]
    pub frontend_drain_timeout_ms: u64,
    #[serde(default = "default_frontend_cleanup_timeout_ms")]
    pub frontend_cleanup_timeout_ms: u64,
}

fn default_server_host() -> String {
    "127.0.0.1".to_string()
}
fn default_http_port() -> u16 {
    8040
}
fn default_grpc_port() -> u16 {
    9080
}
fn default_frontend_drain_timeout_ms() -> u64 {
    DEFAULT_FRONTEND_DRAIN_TIMEOUT_MS
}
fn default_frontend_cleanup_timeout_ms() -> u64 {
    DEFAULT_FRONTEND_CLEANUP_TIMEOUT_MS
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_server_host(),
            priority_networks: String::new(),
            http_port: default_http_port(),
            grpc_port: default_grpc_port(),
            frontend_drain_timeout_ms: default_frontend_drain_timeout_ms(),
            frontend_cleanup_timeout_ms: default_frontend_cleanup_timeout_ms(),
        }
    }
}

/// Shared object-store credentials loaded independently by every backend at
/// startup. Native plans may reference this binding but must never carry its
/// values.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConnectorObjectStoreConfig {
    pub endpoint: Option<String>,
    pub access_key_id: Option<SecretValue>,
    pub access_key_secret: Option<SecretValue>,
    pub region: Option<String>,
    pub enable_path_style_access: Option<bool>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConnectorConfig {
    pub object_store: Option<ConnectorObjectStoreConfig>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ConnectorObjectStoreConfigWire {
    endpoint: Option<String>,
    access_key_id: Option<String>,
    access_key_secret: Option<String>,
    region: Option<String>,
    enable_path_style_access: Option<bool>,
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct ConnectorConfigWire {
    object_store: Option<ConnectorObjectStoreConfigWire>,
}

fn deserialize_connector_config<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<ConnectorConfig, D::Error> {
    let wire = ConnectorConfigWire::deserialize(deserializer)?;
    Ok(ConnectorConfig {
        object_store: wire
            .object_store
            .map(|object_store| ConnectorObjectStoreConfig {
                endpoint: object_store.endpoint,
                access_key_id: object_store.access_key_id.map(SecretValue::new),
                access_key_secret: object_store.access_key_secret.map(SecretValue::new),
                region: object_store.region,
                enable_path_style_access: object_store.enable_path_style_access,
            }),
    })
}

impl ConnectorConfig {
    /// Project the `[connector.object_store]` credentials onto a neutral
    /// filesystem config, filling unset retry knobs from `retry`.
    ///
    /// The retry defaults arrive as an argument rather than being read from a
    /// process-global config, so that `novarocks-fs` owns no configuration
    /// source of its own.
    pub fn object_store_config(
        &self,
        retry: &novarocks_fs::ObjectStoreRetrySettings,
    ) -> std::result::Result<Option<novarocks_fs::ObjectStoreConfig>, String> {
        let Some(object_store) = self.object_store.as_ref() else {
            return Ok(None);
        };
        let credentials = novarocks_fs::ObjectStoreCredentials::from_parts(
            novarocks_fs::ObjectStoreCredentialsSource::ConnectorStartupConfig,
            object_store.endpoint.as_deref().unwrap_or_default(),
            object_store
                .access_key_id
                .clone()
                .unwrap_or_else(|| SecretValue::new("")),
            object_store
                .access_key_secret
                .clone()
                .unwrap_or_else(|| SecretValue::new("")),
            object_store.region.as_deref(),
            object_store.enable_path_style_access,
        )?;
        let mut config = credentials.to_object_store_config();
        retry.apply_if_absent(&mut config);
        Ok(Some(config))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct StandaloneServerConfig {
    #[serde(default = "default_standalone_server_mysql_port")]
    pub mysql_port: u16,
    #[serde(default = "default_standalone_server_user")]
    pub user: String,
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
    #[serde(default = "default_query_control_heartbeat_interval_ms")]
    pub query_control_heartbeat_interval_ms: u64,
    #[serde(default = "default_query_control_heartbeat_timeout_ms")]
    pub query_control_heartbeat_timeout_ms: u64,
    #[serde(default = "default_query_control_init_rpc_timeout_ms")]
    pub query_control_init_rpc_timeout_ms: u64,
    #[serde(default = "default_query_control_attach_timeout_ms")]
    pub query_control_attach_timeout_ms: u64,
    #[serde(default = "default_query_control_stage_rpc_timeout_ms")]
    pub query_control_stage_rpc_timeout_ms: u64,
    #[serde(default = "default_query_control_start_rpc_timeout_ms")]
    pub query_control_start_rpc_timeout_ms: u64,
    #[serde(default = "default_query_control_pre_start_timeout_ms")]
    pub query_control_pre_start_timeout_ms: u64,
    #[serde(default = "default_query_control_tombstone_retention_ms")]
    pub query_control_tombstone_retention_ms: u64,
    #[serde(default = "default_query_control_tombstone_capacity")]
    pub query_control_tombstone_capacity: usize,
    #[serde(default = "default_query_control_terminal_drain_timeout_ms")]
    pub query_control_terminal_drain_timeout_ms: u64,
    #[serde(default = "default_query_control_terminal_ack_timeout_ms")]
    pub query_control_terminal_ack_timeout_ms: u64,
    #[serde(default = "default_query_control_terminal_fallback_rpc_timeout_ms")]
    pub query_control_terminal_fallback_rpc_timeout_ms: u64,
    #[serde(default = "default_query_control_terminal_fallback_max_attempts")]
    pub query_control_terminal_fallback_max_attempts: usize,
    #[serde(default = "default_query_control_terminal_fallback_initial_backoff_ms")]
    pub query_control_terminal_fallback_initial_backoff_ms: u64,
    #[serde(default = "default_query_control_terminal_fallback_max_backoff_ms")]
    pub query_control_terminal_fallback_max_backoff_ms: u64,
    #[serde(default = "default_query_control_terminal_max_encoded_bytes")]
    pub query_control_terminal_max_encoded_bytes: usize,
    #[serde(default = "default_query_control_terminal_max_retained_bytes")]
    pub query_control_terminal_max_retained_bytes: usize,
    #[serde(default = "default_query_control_terminal_retained_capacity")]
    pub query_control_terminal_retained_capacity: usize,
    #[serde(default = "default_query_control_terminal_retention_ms")]
    pub query_control_terminal_retention_ms: u64,
    #[serde(default = "default_query_control_max_active_entries")]
    pub query_control_max_active_entries: usize,
    #[serde(default = "default_query_control_stage_max_encoded_bytes")]
    pub query_control_stage_max_encoded_bytes: usize,
    #[serde(default = "default_query_control_stage_max_fragments")]
    pub query_control_stage_max_fragments: usize,
    #[serde(default = "default_query_control_max_active_staging")]
    pub query_control_max_active_staging: usize,
    #[serde(default = "default_query_control_stage_max_inflight_encoded_bytes")]
    pub query_control_stage_max_inflight_encoded_bytes: usize,
    #[serde(default = "default_query_control_stage_max_dormant_workers")]
    pub query_control_stage_max_dormant_workers: usize,
    #[serde(default = "default_write_commit_evidence_max_bytes")]
    pub write_commit_evidence_max_bytes: usize,
    #[serde(default = "default_write_commit_evidence_max_entries")]
    pub write_commit_evidence_max_entries: usize,
    #[serde(default = "default_lake_publication_max_attempt_duration_ms")]
    pub lake_publication_max_attempt_duration_ms: u64,
    #[serde(default = "default_lake_publication_safe_gc_age_ms")]
    pub lake_publication_safe_gc_age_ms: u64,
    #[serde(default = "default_lake_publication_max_clock_skew_ms")]
    pub lake_publication_max_clock_skew_ms: u64,
    #[serde(default = "default_lake_publication_listing_visibility_delay_ms")]
    pub lake_publication_listing_visibility_delay_ms: u64,
    #[serde(default = "default_lake_publication_scheduler_margin_ms")]
    pub lake_publication_scheduler_margin_ms: u64,
    #[serde(default = "default_mem_limit")]
    pub mem_limit: String,
    #[serde(default = "default_be_mem_limit_bytes")]
    pub be_mem_limit_bytes: u64,
    #[serde(default = "default_optimizer_query_mem_limit_bytes")]
    pub optimizer_query_mem_limit_bytes: u64,
    /// `0` means derive the backend count from the live BE registry (the normal path).
    #[serde(default = "default_optimizer_effective_backend_count")]
    pub optimizer_effective_backend_count: u64,
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
    #[serde(default = "default_table_schema_service_max_retries")]
    pub table_schema_service_max_retries: usize,
    #[serde(default = "default_table_schema_service_cache_capacity")]
    pub table_schema_service_cache_capacity: u64,
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

impl ObjectStorageConfig {
    /// The retry knobs this section contributes to filesystem resources.
    pub fn retry_settings(&self) -> novarocks_fs::ObjectStoreRetrySettings {
        novarocks_fs::ObjectStoreRetrySettings {
            retry_max_times: self.retry_max_times,
            retry_min_delay_ms: self.retry_min_delay_ms,
            retry_max_delay_ms: self.retry_max_delay_ms,
            timeout_ms: self.timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
        }
    }
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

fn default_query_control_heartbeat_interval_ms() -> u64 {
    1_000
}

fn default_query_control_heartbeat_timeout_ms() -> u64 {
    5_000
}

fn default_query_control_init_rpc_timeout_ms() -> u64 {
    5_000
}

fn default_query_control_attach_timeout_ms() -> u64 {
    5_000
}

fn default_query_control_stage_rpc_timeout_ms() -> u64 {
    5_000
}

fn default_query_control_start_rpc_timeout_ms() -> u64 {
    2_000
}

fn default_query_control_pre_start_timeout_ms() -> u64 {
    30_000
}

fn default_query_control_tombstone_retention_ms() -> u64 {
    120_000
}

fn default_query_control_tombstone_capacity() -> usize {
    16_384
}

fn default_query_control_terminal_drain_timeout_ms() -> u64 {
    30_000
}
fn default_query_control_terminal_ack_timeout_ms() -> u64 {
    5_000
}
fn default_query_control_terminal_fallback_rpc_timeout_ms() -> u64 {
    5_000
}
fn default_query_control_terminal_fallback_max_attempts() -> usize {
    5
}
fn default_query_control_terminal_fallback_initial_backoff_ms() -> u64 {
    100
}
fn default_query_control_terminal_fallback_max_backoff_ms() -> u64 {
    1_000
}
fn default_query_control_terminal_max_encoded_bytes() -> usize {
    48 * 1024 * 1024
}
fn default_query_control_terminal_max_retained_bytes() -> usize {
    256 * 1024 * 1024
}
fn default_query_control_terminal_retained_capacity() -> usize {
    4_096
}
fn default_query_control_terminal_retention_ms() -> u64 {
    120_000
}

fn default_query_control_max_active_entries() -> usize {
    4_096
}

fn default_query_control_stage_max_encoded_bytes() -> usize {
    48 * 1024 * 1024
}

fn default_query_control_stage_max_fragments() -> usize {
    256
}

fn default_query_control_max_active_staging() -> usize {
    32
}

fn default_query_control_stage_max_inflight_encoded_bytes() -> usize {
    256 * 1024 * 1024
}

fn default_query_control_stage_max_dormant_workers() -> usize {
    512
}

fn validate_query_control_config(runtime: &RuntimeConfig) -> Result<()> {
    let nonzero_durations = [
        (
            "runtime.query_control_heartbeat_interval_ms",
            runtime.query_control_heartbeat_interval_ms,
        ),
        (
            "runtime.query_control_heartbeat_timeout_ms",
            runtime.query_control_heartbeat_timeout_ms,
        ),
        (
            "runtime.query_control_init_rpc_timeout_ms",
            runtime.query_control_init_rpc_timeout_ms,
        ),
        (
            "runtime.query_control_attach_timeout_ms",
            runtime.query_control_attach_timeout_ms,
        ),
        (
            "runtime.query_control_stage_rpc_timeout_ms",
            runtime.query_control_stage_rpc_timeout_ms,
        ),
        (
            "runtime.query_control_start_rpc_timeout_ms",
            runtime.query_control_start_rpc_timeout_ms,
        ),
        (
            "runtime.query_control_pre_start_timeout_ms",
            runtime.query_control_pre_start_timeout_ms,
        ),
        (
            "runtime.query_control_tombstone_retention_ms",
            runtime.query_control_tombstone_retention_ms,
        ),
        (
            "runtime.query_control_terminal_drain_timeout_ms",
            runtime.query_control_terminal_drain_timeout_ms,
        ),
        (
            "runtime.query_control_terminal_ack_timeout_ms",
            runtime.query_control_terminal_ack_timeout_ms,
        ),
        (
            "runtime.query_control_terminal_fallback_rpc_timeout_ms",
            runtime.query_control_terminal_fallback_rpc_timeout_ms,
        ),
        (
            "runtime.query_control_terminal_fallback_initial_backoff_ms",
            runtime.query_control_terminal_fallback_initial_backoff_ms,
        ),
        (
            "runtime.query_control_terminal_fallback_max_backoff_ms",
            runtime.query_control_terminal_fallback_max_backoff_ms,
        ),
    ];
    for (field, value) in nonzero_durations {
        if value == 0 {
            bail!("{field} must be greater than 0");
        }
    }
    if runtime.query_control_tombstone_capacity == 0 {
        bail!("runtime.query_control_tombstone_capacity must be greater than 0");
    }
    if runtime.query_control_max_active_entries == 0 {
        bail!("runtime.query_control_max_active_entries must be greater than 0");
    }
    let terminal_limits = [
        (
            "runtime.query_control_terminal_fallback_max_attempts",
            runtime.query_control_terminal_fallback_max_attempts,
        ),
        (
            "runtime.query_control_terminal_max_encoded_bytes",
            runtime.query_control_terminal_max_encoded_bytes,
        ),
        (
            "runtime.query_control_terminal_max_retained_bytes",
            runtime.query_control_terminal_max_retained_bytes,
        ),
        (
            "runtime.query_control_terminal_retained_capacity",
            runtime.query_control_terminal_retained_capacity,
        ),
    ];
    for (field, value) in terminal_limits {
        if value == 0 {
            bail!("{field} must be greater than 0");
        }
    }
    if runtime.query_control_terminal_fallback_initial_backoff_ms
        > runtime.query_control_terminal_fallback_max_backoff_ms
    {
        bail!(
            "runtime.query_control_terminal_fallback_initial_backoff_ms must not exceed runtime.query_control_terminal_fallback_max_backoff_ms"
        );
    }
    let nonzero_limits = [
        (
            "runtime.query_control_stage_max_encoded_bytes",
            runtime.query_control_stage_max_encoded_bytes,
        ),
        (
            "runtime.query_control_stage_max_fragments",
            runtime.query_control_stage_max_fragments,
        ),
        (
            "runtime.query_control_max_active_staging",
            runtime.query_control_max_active_staging,
        ),
        (
            "runtime.query_control_stage_max_inflight_encoded_bytes",
            runtime.query_control_stage_max_inflight_encoded_bytes,
        ),
        (
            "runtime.query_control_stage_max_dormant_workers",
            runtime.query_control_stage_max_dormant_workers,
        ),
        (
            "runtime.write_commit_evidence_max_bytes",
            runtime.write_commit_evidence_max_bytes,
        ),
        (
            "runtime.write_commit_evidence_max_entries",
            runtime.write_commit_evidence_max_entries,
        ),
    ];
    for (field, value) in nonzero_limits {
        if value == 0 {
            bail!("{field} must be greater than 0");
        }
    }
    const TONIC_MAX_STAGE_REQUEST_BYTES: usize = 64 * 1024 * 1024;
    if runtime.query_control_stage_max_encoded_bytes >= TONIC_MAX_STAGE_REQUEST_BYTES {
        bail!(
            "runtime.query_control_stage_max_encoded_bytes must be smaller than the 64MiB gRPC limit"
        );
    }
    if runtime.query_control_stage_max_inflight_encoded_bytes
        < runtime.query_control_stage_max_encoded_bytes
    {
        bail!(
            "runtime.query_control_stage_max_inflight_encoded_bytes must be at least runtime.query_control_stage_max_encoded_bytes"
        );
    }
    if runtime.query_control_stage_max_dormant_workers < runtime.query_control_stage_max_fragments {
        bail!(
            "runtime.query_control_stage_max_dormant_workers must be at least runtime.query_control_stage_max_fragments"
        );
    }
    let minimum_timeout = runtime
        .query_control_heartbeat_interval_ms
        .checked_mul(3)
        .ok_or_else(|| {
            anyhow::anyhow!("runtime.query_control_heartbeat_interval_ms is too large to validate")
        })?;
    if runtime.query_control_heartbeat_timeout_ms < minimum_timeout {
        bail!(
            "runtime.query_control_heartbeat_timeout_ms must be at least 3 times runtime.query_control_heartbeat_interval_ms"
        );
    }
    Ok(())
}

fn validate_lake_publication_runtime_policy(runtime: &RuntimeConfig) -> Result<()> {
    let fields = [
        (
            "runtime.lake_publication_max_attempt_duration_ms",
            runtime.lake_publication_max_attempt_duration_ms,
        ),
        (
            "runtime.lake_publication_safe_gc_age_ms",
            runtime.lake_publication_safe_gc_age_ms,
        ),
        (
            "runtime.lake_publication_max_clock_skew_ms",
            runtime.lake_publication_max_clock_skew_ms,
        ),
        (
            "runtime.lake_publication_listing_visibility_delay_ms",
            runtime.lake_publication_listing_visibility_delay_ms,
        ),
        (
            "runtime.lake_publication_scheduler_margin_ms",
            runtime.lake_publication_scheduler_margin_ms,
        ),
    ];
    for (field, value) in fields {
        if value == 0 {
            bail!("{field} must be greater than 0");
        }
    }
    let required_safe_age = runtime
        .lake_publication_max_attempt_duration_ms
        .checked_add(runtime.lake_publication_max_clock_skew_ms)
        .and_then(|value| value.checked_add(runtime.lake_publication_listing_visibility_delay_ms))
        .and_then(|value| value.checked_add(runtime.lake_publication_scheduler_margin_ms))
        .ok_or_else(|| {
            anyhow::anyhow!("runtime lake publication safe GC age calculation overflows")
        })?;
    if runtime.lake_publication_safe_gc_age_ms <= required_safe_age {
        bail!(
            "runtime.lake_publication_safe_gc_age_ms must exceed max attempt duration plus clock skew, listing visibility delay, and scheduler margin"
        );
    }
    Ok(())
}

fn default_mem_limit() -> String {
    DEFAULT_MEM_LIMIT_SPEC.to_string()
}

fn default_be_mem_limit_bytes() -> u64 {
    0
}

fn default_optimizer_query_mem_limit_bytes() -> u64 {
    2 * 1024 * 1024 * 1024
}

fn default_optimizer_effective_backend_count() -> u64 {
    0
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

fn default_write_commit_evidence_max_bytes() -> usize {
    novarocks_spi::connector::DEFAULT_WRITE_COMMIT_EVIDENCE_MAX_BYTES
}

fn default_write_commit_evidence_max_entries() -> usize {
    novarocks_spi::connector::DEFAULT_WRITE_COMMIT_EVIDENCE_MAX_ENTRIES
}

fn default_lake_publication_max_attempt_duration_ms() -> u64 {
    30 * 60 * 1_000
}

fn default_lake_publication_safe_gc_age_ms() -> u64 {
    45 * 60 * 1_000
}

fn default_lake_publication_max_clock_skew_ms() -> u64 {
    60 * 1_000
}

fn default_lake_publication_listing_visibility_delay_ms() -> u64 {
    5 * 60 * 1_000
}

fn default_lake_publication_scheduler_margin_ms() -> u64 {
    60 * 1_000
}

fn default_pipeline_exec_thread_pool_thread_num() -> usize {
    0 // 0 means use CPU cores
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

fn default_table_schema_service_max_retries() -> usize {
    3
}

fn default_table_schema_service_cache_capacity() -> u64 {
    4_096
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
            query_control_heartbeat_interval_ms: default_query_control_heartbeat_interval_ms(),
            query_control_heartbeat_timeout_ms: default_query_control_heartbeat_timeout_ms(),
            query_control_init_rpc_timeout_ms: default_query_control_init_rpc_timeout_ms(),
            query_control_attach_timeout_ms: default_query_control_attach_timeout_ms(),
            query_control_stage_rpc_timeout_ms: default_query_control_stage_rpc_timeout_ms(),
            query_control_start_rpc_timeout_ms: default_query_control_start_rpc_timeout_ms(),
            query_control_pre_start_timeout_ms: default_query_control_pre_start_timeout_ms(),
            query_control_tombstone_retention_ms: default_query_control_tombstone_retention_ms(),
            query_control_tombstone_capacity: default_query_control_tombstone_capacity(),
            query_control_terminal_drain_timeout_ms:
                default_query_control_terminal_drain_timeout_ms(),
            query_control_terminal_ack_timeout_ms: default_query_control_terminal_ack_timeout_ms(),
            query_control_terminal_fallback_rpc_timeout_ms:
                default_query_control_terminal_fallback_rpc_timeout_ms(),
            query_control_terminal_fallback_max_attempts:
                default_query_control_terminal_fallback_max_attempts(),
            query_control_terminal_fallback_initial_backoff_ms:
                default_query_control_terminal_fallback_initial_backoff_ms(),
            query_control_terminal_fallback_max_backoff_ms:
                default_query_control_terminal_fallback_max_backoff_ms(),
            query_control_terminal_max_encoded_bytes:
                default_query_control_terminal_max_encoded_bytes(),
            query_control_terminal_max_retained_bytes:
                default_query_control_terminal_max_retained_bytes(),
            query_control_terminal_retained_capacity:
                default_query_control_terminal_retained_capacity(),
            query_control_terminal_retention_ms: default_query_control_terminal_retention_ms(),
            query_control_max_active_entries: default_query_control_max_active_entries(),
            query_control_stage_max_encoded_bytes: default_query_control_stage_max_encoded_bytes(),
            query_control_stage_max_fragments: default_query_control_stage_max_fragments(),
            query_control_max_active_staging: default_query_control_max_active_staging(),
            query_control_stage_max_inflight_encoded_bytes:
                default_query_control_stage_max_inflight_encoded_bytes(),
            query_control_stage_max_dormant_workers:
                default_query_control_stage_max_dormant_workers(),
            write_commit_evidence_max_bytes: default_write_commit_evidence_max_bytes(),
            write_commit_evidence_max_entries: default_write_commit_evidence_max_entries(),
            lake_publication_max_attempt_duration_ms:
                default_lake_publication_max_attempt_duration_ms(),
            lake_publication_safe_gc_age_ms: default_lake_publication_safe_gc_age_ms(),
            lake_publication_max_clock_skew_ms: default_lake_publication_max_clock_skew_ms(),
            lake_publication_listing_visibility_delay_ms:
                default_lake_publication_listing_visibility_delay_ms(),
            lake_publication_scheduler_margin_ms: default_lake_publication_scheduler_margin_ms(),
            mem_limit: default_mem_limit(),
            be_mem_limit_bytes: default_be_mem_limit_bytes(),
            optimizer_query_mem_limit_bytes: default_optimizer_query_mem_limit_bytes(),
            optimizer_effective_backend_count: default_optimizer_effective_backend_count(),
            local_exchange_buffer_mem_limit_per_driver:
                default_local_exchange_buffer_mem_limit_per_driver(),
            local_exchange_max_buffered_rows: default_local_exchange_max_buffered_rows(),
            operator_buffer_chunks: default_operator_buffer_chunks(),
            olap_sink_write_buffer_size_bytes: default_olap_sink_write_buffer_size_bytes(),
            olap_sink_max_tablet_write_chunk_bytes: default_olap_sink_max_tablet_write_chunk_bytes(
            ),
            pipeline_scan_thread_pool_thread_num: default_pipeline_scan_thread_pool_thread_num(),
            connector_io_tasks_per_scan_operator: default_connector_io_tasks_per_scan_operator(),
            io_coalesce_read_enable: default_io_coalesce_read_enable(),
            io_coalesce_read_max_buffer_size: default_io_coalesce_read_max_buffer_size(),
            io_coalesce_read_max_distance_size: default_io_coalesce_read_max_distance_size(),
            io_coalesce_adaptive_lazy_active: default_io_coalesce_adaptive_lazy_active(),
            pipeline_scan_thread_pool_queue_size: default_pipeline_scan_thread_pool_queue_size(),
            pipeline_exec_thread_pool_thread_num: default_pipeline_exec_thread_pool_thread_num(),
            data_runtime_worker_threads: default_data_runtime_worker_threads(),
            data_runtime_max_blocking_threads: default_data_runtime_max_blocking_threads(),
            spill_io_threads: default_spill_io_threads(),
            spill_io_queue_size: default_spill_io_queue_size(),
            scan_submit_fail_max: default_scan_submit_fail_max(),
            scan_submit_fail_timeout_ms: default_scan_submit_fail_timeout_ms(),
            profile_report_interval: default_profile_report_interval(),
            table_schema_service_max_retries: default_table_schema_service_max_retries(),
            table_schema_service_cache_capacity: default_table_schema_service_cache_capacity(),
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
    pub fn effective_be_mem_limit_bytes(&self) -> Result<u64> {
        if self.be_mem_limit_bytes > 0 {
            return Ok(self.be_mem_limit_bytes);
        }

        crate::memory_limit::resolve_starrocks_process_mem_limit_bytes(&self.mem_limit)
            .with_context(|| format!("resolve runtime.mem_limit '{}'", self.mem_limit))
    }

    pub fn effective_be_mem_limit_bytes_for_visible_memory(
        &self,
        visible_memory_bytes: u64,
    ) -> Result<u64> {
        if self.be_mem_limit_bytes > 0 {
            return Ok(self.be_mem_limit_bytes);
        }

        crate::memory_limit::resolve_starrocks_process_mem_limit_bytes_for_visible_memory(
            &self.mem_limit,
            visible_memory_bytes,
        )
        .with_context(|| format!("resolve runtime.mem_limit '{}'", self.mem_limit))
    }

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

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_MEM_LIMIT_SPEC, NovaRocksConfig, RuntimeConfig, StandaloneServerConfig,
        validate_query_control_config,
    };

    #[test]
    fn query_control_config_defaults_are_fixed() {
        let runtime = RuntimeConfig::default();

        assert_eq!(runtime.query_control_heartbeat_interval_ms, 1_000);
        assert_eq!(runtime.query_control_heartbeat_timeout_ms, 5_000);
        assert_eq!(runtime.query_control_init_rpc_timeout_ms, 5_000);
        assert_eq!(runtime.query_control_attach_timeout_ms, 5_000);
        assert_eq!(runtime.query_control_stage_rpc_timeout_ms, 5_000);
        assert_eq!(runtime.query_control_start_rpc_timeout_ms, 2_000);
        assert_eq!(runtime.query_control_pre_start_timeout_ms, 30_000);
        assert_eq!(runtime.query_control_tombstone_retention_ms, 120_000);
        assert_eq!(runtime.query_control_tombstone_capacity, 16_384);
        assert_eq!(runtime.query_control_terminal_drain_timeout_ms, 30_000);
        assert_eq!(runtime.query_control_terminal_ack_timeout_ms, 5_000);
        assert_eq!(
            runtime.query_control_terminal_fallback_rpc_timeout_ms,
            5_000
        );
        assert_eq!(runtime.query_control_terminal_fallback_max_attempts, 5);
        assert_eq!(
            runtime.query_control_terminal_fallback_initial_backoff_ms,
            100
        );
        assert_eq!(
            runtime.query_control_terminal_fallback_max_backoff_ms,
            1_000
        );
        assert_eq!(
            runtime.query_control_terminal_max_encoded_bytes,
            48 * 1024 * 1024
        );
        assert_eq!(
            runtime.query_control_terminal_max_retained_bytes,
            256 * 1024 * 1024
        );
        assert_eq!(runtime.query_control_terminal_retained_capacity, 4_096);
        assert_eq!(runtime.query_control_terminal_retention_ms, 120_000);
        assert_eq!(runtime.query_control_max_active_entries, 4_096);
        assert_eq!(
            runtime.query_control_stage_max_encoded_bytes,
            48 * 1024 * 1024
        );
        assert_eq!(runtime.query_control_stage_max_fragments, 256);
        assert_eq!(runtime.query_control_max_active_staging, 32);
        assert_eq!(
            runtime.query_control_stage_max_inflight_encoded_bytes,
            256 * 1024 * 1024
        );
        assert_eq!(runtime.query_control_stage_max_dormant_workers, 512);
    }

    #[test]
    #[expect(
        clippy::type_complexity,
        reason = "The table-driven validation fixture keeps each field mutator explicit."
    )]
    fn query_control_config_rejects_zero_values() {
        let cases: [(&str, fn(&mut RuntimeConfig)); 8] = [
            ("query_control_heartbeat_interval_ms", |runtime| {
                runtime.query_control_heartbeat_interval_ms = 0;
            }),
            ("query_control_heartbeat_timeout_ms", |runtime| {
                runtime.query_control_heartbeat_timeout_ms = 0;
            }),
            ("query_control_init_rpc_timeout_ms", |runtime| {
                runtime.query_control_init_rpc_timeout_ms = 0;
            }),
            ("query_control_attach_timeout_ms", |runtime| {
                runtime.query_control_attach_timeout_ms = 0;
            }),
            ("query_control_pre_start_timeout_ms", |runtime| {
                runtime.query_control_pre_start_timeout_ms = 0;
            }),
            ("query_control_tombstone_retention_ms", |runtime| {
                runtime.query_control_tombstone_retention_ms = 0;
            }),
            ("query_control_tombstone_capacity", |runtime| {
                runtime.query_control_tombstone_capacity = 0;
            }),
            ("query_control_max_active_entries", |runtime| {
                runtime.query_control_max_active_entries = 0;
            }),
        ];

        for (field, mutate) in cases {
            let mut runtime = RuntimeConfig::default();
            mutate(&mut runtime);
            let error = validate_query_control_config(&runtime)
                .expect_err("zero query-control values must be rejected");
            assert!(
                error.to_string().contains(field),
                "error must identify {field}: {error}"
            );
        }
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture states the two related heartbeat facts progressively."
    )]
    fn query_control_config_rejects_short_heartbeat_timeout() {
        let mut runtime = RuntimeConfig::default();
        runtime.query_control_heartbeat_interval_ms = 1_000;
        runtime.query_control_heartbeat_timeout_ms = 2_999;

        let error = validate_query_control_config(&runtime)
            .expect_err("heartbeat timeout must cover at least three intervals");
        assert!(
            error
                .to_string()
                .contains("query_control_heartbeat_timeout_ms")
        );
    }

    #[test]
    fn query_control_config_load_rejects_invalid_capacity() -> anyhow::Result<()> {
        let temp = tempfile::NamedTempFile::new()?;
        std::fs::write(
            temp.path(),
            r#"
[runtime]
query_control_max_active_entries = 0
"#,
        )?;

        let error = match NovaRocksConfig::load_from_file(temp.path()) {
            Ok(_) => panic!("load must validate query-control capacity"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("query_control_max_active_entries")
        );
        Ok(())
    }

    #[test]
    fn state_store_config_loads_explicit_sqlite_provider() -> anyhow::Result<()> {
        let temp = tempfile::NamedTempFile::new()?;
        std::fs::write(
            temp.path(),
            r#"
[state_store]
provider = "sqlite"
path = "meta/state-store.sqlite"
cluster_id = "cluster-a"
"#,
        )?;

        let cfg = NovaRocksConfig::load_from_file(temp.path())?;

        let state_store = cfg.state_store.expect("state store config");
        assert_eq!(
            state_store.store.path,
            std::path::PathBuf::from("meta/state-store.sqlite")
        );
        Ok(())
    }

    #[test]
    fn state_store_config_requires_provider() -> anyhow::Result<()> {
        let temp = tempfile::NamedTempFile::new()?;
        std::fs::write(
            temp.path(),
            r#"
[state_store]
path = "meta/state-store.sqlite"
cluster_id = "cluster-a"
"#,
        )?;

        let error = match NovaRocksConfig::load_from_file(temp.path()) {
            Ok(_) => panic!("state_store.provider must be explicit"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("deserialize config TOML"));
        Ok(())
    }

    #[test]
    fn state_store_config_rejects_cross_provider_fields() -> anyhow::Result<()> {
        let temp = tempfile::NamedTempFile::new()?;
        std::fs::write(
            temp.path(),
            r#"
[state_store]
provider = "mysql"
path = "meta/state-store.sqlite"
cluster_id = "cluster-a"
database = "remote_state"
"#,
        )?;

        let error = match NovaRocksConfig::load_from_file(temp.path()) {
            Ok(_) => panic!("cross-provider fields must fail closed"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("deserialize config TOML"));
        Ok(())
    }

    #[test]
    fn state_store_config_rejects_relaxed_key_limit() -> anyhow::Result<()> {
        let temp = tempfile::NamedTempFile::new()?;
        std::fs::write(
            temp.path(),
            r#"
[state_store]
provider = "sqlite"
path = "meta/state-store.sqlite"
cluster_id = "cluster-a"

[state_store.limits]
max_key_bytes = 8193
"#,
        )?;

        let error = match NovaRocksConfig::load_from_file(temp.path()) {
            Ok(_) => panic!("provider limits may only tighten the common contract"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("InvalidStateStoreConfig"));
        Ok(())
    }

    #[test]
    fn state_store_config_is_disabled_when_section_is_absent() -> anyhow::Result<()> {
        let temp = tempfile::NamedTempFile::new()?;
        std::fs::write(temp.path(), "log_level = \"info\"\n")?;

        let cfg = NovaRocksConfig::load_from_file(temp.path())?;

        assert!(cfg.state_store.is_none());
        Ok(())
    }

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
    fn test_runtime_mem_limit_defaults_to_starrocks_spec() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
"#,
        )
        .expect("parse config");
        assert_eq!(cfg.runtime.mem_limit, DEFAULT_MEM_LIMIT_SPEC);
        assert_eq!(cfg.runtime.be_mem_limit_bytes, 0);
    }

    #[test]
    fn test_runtime_be_mem_limit_bytes_override_wins() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
mem_limit = "10%"
be_mem_limit_bytes = 34359738368
"#,
        )
        .expect("parse config");
        assert_eq!(
            cfg.runtime
                .effective_be_mem_limit_bytes_for_visible_memory(128 * 1024 * 1024 * 1024)
                .expect("resolve mem limit"),
            32 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn test_runtime_mem_limit_derives_starrocks_soft_limit_from_percentage() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
mem_limit = "90%"
"#,
        )
        .expect("parse config");
        assert_eq!(
            cfg.runtime
                .effective_be_mem_limit_bytes_for_visible_memory(1000)
                .expect("resolve mem limit"),
            810
        );
    }

    #[test]
    fn test_runtime_mem_limit_derives_starrocks_soft_limit_from_units_and_clamps() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
mem_limit = "200G"
"#,
        )
        .expect("parse config");
        assert_eq!(
            cfg.runtime
                .effective_be_mem_limit_bytes_for_visible_memory(100 * 1024 * 1024 * 1024)
                .expect("resolve mem limit"),
            100 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn test_runtime_mem_limit_rejects_zero_effective_limit() {
        let cfg: NovaRocksConfig = toml::from_str(
            r#"
[runtime]
mem_limit = "0"
"#,
        )
        .expect("parse config");
        assert!(
            cfg.runtime
                .effective_be_mem_limit_bytes_for_visible_memory(100 * 1024 * 1024 * 1024)
                .is_err()
        );
    }

    #[test]
    fn test_cluster_builder_default_is_frontend_only() {
        let toml = r#"
[server]
host = "127.0.0.1"
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse default");
        assert_eq!(cfg.cluster.role, super::ClusterRole::Fe);
        assert!(cfg.cluster.frontend_endpoint.is_none());
    }

    #[test]
    fn test_cluster_role_fe_rejects_legacy_backend_seeds() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["127.0.0.1:9070"]
"#;
        assert!(toml::from_str::<NovaRocksConfig>(toml).is_err());
    }

    #[test]
    fn test_cluster_role_be_requires_frontend_endpoint() {
        let toml = r#"
[cluster]
role = "be"
"#;
        let parsed: NovaRocksConfig = toml::from_str(toml).expect("parse be");
        assert!(parsed.cluster.validate().is_err());
    }

    #[test]
    fn test_cluster_role_be_accepts_dns_frontend_endpoint() {
        let toml = r#"
[cluster]
role = "be"
frontend_endpoint = "fe.native.example:9070"
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse be");
        cfg.cluster
            .validate()
            .expect("dns frontend endpoint should pass validation");
    }

    #[test]
    fn test_cluster_role_fe_rejects_frontend_endpoint() {
        let toml = r#"
[cluster]
role = "fe"
frontend_endpoint = "fe.native.example:9070"
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
        assert!(cfg.cluster.validate().is_err());
    }

    #[test]
    fn test_cluster_role_fe_rejects_be_announce_settings() {
        let toml = r#"
[cluster]
role = "fe"
backend_announce_interval_ms = 1000
"#;
        let cfg: NovaRocksConfig = toml::from_str(toml).expect("parse");
        assert!(cfg.cluster.validate().is_err());
    }

    #[test]
    fn test_cluster_role_be_rejects_frontend_lease_settings_and_invalid_backoff() {
        let lease_toml = r#"
[cluster]
role = "be"
frontend_endpoint = "fe.native.example:9070"
backend_announce_lease_ttl_ms = 5000
"#;
        let cfg: NovaRocksConfig = toml::from_str(lease_toml).expect("parse");
        assert!(cfg.cluster.validate().is_err());

        let backoff_toml = r#"
[cluster]
role = "be"
frontend_endpoint = "fe.native.example:9070"
backend_announce_initial_backoff_ms = 2000
backend_announce_max_backoff_ms = 100
"#;
        let cfg: NovaRocksConfig = toml::from_str(backoff_toml).expect("parse");
        assert!(cfg.cluster.validate().is_err());
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
    fn execution_services_defaults_are_sane() {
        let cfg = RuntimeConfig::default();
        assert_eq!(cfg.execution_services.sink_io_max_blocking_threads, 16);
        assert_eq!(cfg.execution_services.async_sink_queue_capacity, 8);
        // 0 means "derive from cores"; resolved value must be >= 1.
        assert!(cfg.execution_services.actual_sink_io_worker_threads() >= 1);
        assert!(cfg.execution_services.actual_sink_io_worker_threads() <= 4);
    }
}
