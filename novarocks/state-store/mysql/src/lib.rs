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

//! MySQL StateStore provider implementation.
//!
//! This crate owns MySQL's physical schema, client runtime, and test hooks. It
//! deliberately contains no application host, registry, or TOML wire model.

mod budget;
mod client;
mod codec;
mod commit;
mod error;
#[cfg(feature = "state-store-test-hooks")]
#[doc(hidden)]
pub mod helper_protocol;
mod identity;
mod metrics;
#[cfg(feature = "state-store-test-hooks")]
mod open_test_hooks;
mod provider;
mod range;
mod runtime;
mod schema;
mod txn;

#[cfg(feature = "state-store-test-hooks")]
mod test_config;
#[cfg(feature = "state-store-test-hooks")]
#[doc(hidden)]
pub mod test_support;

use std::fmt;
use std::fs::File;
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, bail};
use async_trait::async_trait;
use novarocks_secret::SecretValue;
use tokio::time::Instant;

use self::commit::MysqlEvidence;
use self::metrics::StateStoreMetrics;
use self::runtime::MysqlProviderHandle;
use novarocks_state_store_api::{
    AttemptSupervisor, DEFAULT_MAX_OUTSTANDING_ATTEMPTS, InDoubtAdjudicator, ReadTransaction,
    StateStore, StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreProviderId,
    StoreIdentity, WriteAttempt, WriteTransaction,
};

pub const MYSQL_STATE_STORE_PROVIDER_ID: StateStoreProviderId = StateStoreProviderId::new("mysql");
pub const MYSQL_MAX_KEY_BYTES: usize = 3072;
/// Physical width of the commit ledger key.
///
/// An attempt renders as `<instance scope uuid>:<sequence>`, so 36 + 1 + 20
/// bytes is the widest identity this instance can issue.
pub(crate) const MYSQL_MAX_ATTEMPT_ID_BYTES: usize = 64;
/// Physical format generation.
///
/// Bumped from 1 when the change feed and its retention floor were removed and
/// the commit ledger was rekeyed from a caller-minted transaction UUID to an
/// issued attempt identity. The schema digest would have rejected an old
/// database on its own; the version is what says *why*, and neither gate
/// migrates -- an older store is refused, never rewritten.
pub(crate) const MYSQL_SCHEMA_VERSION: u32 = 2;
const MYSQL_MAX_META_VALUE_BYTES: usize = 4096;
const MYSQL_MAX_CONNECT_TIMEOUT_MS: u64 = 60_000;
const MYSQL_MAX_INACTIVE_CONNECTION_TTL_MS: u64 = 86_400_000;

/// Provider-private typed client configuration constructed by Server after it
/// has decoded and validated the deployment wire configuration.
#[derive(Clone, Eq, PartialEq)]
pub struct MySqlClientConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: SecretValue,
    pub tls_mode: MySqlTlsMode,
    pub tls_ca_path: Option<PathBuf>,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub connect_timeout_ms: u64,
    pub pool_min: usize,
    pub pool_max: usize,
    pub inactive_connection_ttl_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MySqlTlsMode {
    Disabled,
    Required,
    VerifyIdentity,
}

/// Provider input after Server has selected MySQL and resolved the shared
/// StateStore limits against MySQL's physical key capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlStateStoreOpenConfig {
    pub cluster_id: String,
    pub database: String,
    pub limits: StateStoreLimits,
}

impl MysqlStateStoreOpenConfig {
    pub fn validate(&self) -> Result<()> {
        if self.cluster_id.is_empty() || self.cluster_id.len() > MYSQL_MAX_META_VALUE_BYTES {
            bail!("InvalidStateStoreConfig: MySQL cluster_id is invalid");
        }
        if self.database.is_empty()
            || self.database.len() > 64
            || !self
                .database
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            bail!("InvalidStateStoreConfig: database must match ASCII [A-Za-z0-9_]{{1,64}}");
        }
        if self.limits.max_key_bytes == 0 || self.limits.max_key_bytes > MYSQL_MAX_KEY_BYTES {
            bail!(
                "InvalidStateStoreConfig: max_key_bytes must be between 1 and {MYSQL_MAX_KEY_BYTES}"
            );
        }
        Ok(())
    }
}

impl MySqlClientConfig {
    pub fn validate(&self) -> Result<()> {
        if self.host.trim().is_empty() {
            bail!("InvalidStateStoreConfig: mysql_client.host must not be empty");
        }
        if self.port == 0 {
            bail!("InvalidStateStoreConfig: mysql_client.port must be non-zero");
        }
        if self.username.trim().is_empty() {
            bail!("InvalidStateStoreConfig: mysql_client.username must not be empty");
        }
        if self.password.is_empty() {
            bail!("InvalidStateStoreConfig: mysql_client.password must not be empty");
        }
        if self.connect_timeout_ms == 0 || self.connect_timeout_ms > MYSQL_MAX_CONNECT_TIMEOUT_MS {
            bail!(
                "InvalidStateStoreConfig: mysql_client.connect_timeout_ms must be between 1 and {MYSQL_MAX_CONNECT_TIMEOUT_MS}"
            );
        }
        if self.pool_min == 0 || self.pool_min > self.pool_max {
            bail!("InvalidStateStoreConfig: mysql_client.pool_min must be between 1 and pool_max");
        }
        if self.inactive_connection_ttl_ms == 0
            || self.inactive_connection_ttl_ms > MYSQL_MAX_INACTIVE_CONNECTION_TTL_MS
        {
            bail!(
                "InvalidStateStoreConfig: mysql_client.inactive_connection_ttl_ms must be between 1 and {MYSQL_MAX_INACTIVE_CONNECTION_TTL_MS}"
            );
        }
        if self.tls_cert_path.is_some() != self.tls_key_path.is_some() {
            let missing = if self.tls_cert_path.is_none() {
                "tls_cert_path"
            } else {
                "tls_key_path"
            };
            bail!(
                "InvalidStateStoreConfig: mysql_client.{missing} is required when the matching client TLS path is configured"
            );
        }
        if self.tls_mode == MySqlTlsMode::VerifyIdentity {
            if self.tls_ca_path.is_none() {
                bail!(
                    "InvalidStateStoreConfig: mysql_client.tls_ca_path is required for verify_identity"
                );
            }
            if mysql_host_is_ip_address(&self.host) {
                bail!(
                    "InvalidStateStoreConfig: mysql_client.host must be a DNS hostname for verify_identity"
                );
            }
        }
        for (name, path) in [
            ("tls_ca_path", self.tls_ca_path.as_deref()),
            ("tls_cert_path", self.tls_cert_path.as_deref()),
            ("tls_key_path", self.tls_key_path.as_deref()),
        ] {
            if let Some(path) = path {
                validate_readable_file(path, &format!("mysql_client.{name}"))?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for MySqlClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MySqlClientConfig")
            .field("host_configured", &!self.host.trim().is_empty())
            .field("port_configured", &(self.port != 0))
            .field("username_configured", &!self.username.trim().is_empty())
            .field("password_configured", &!self.password.is_empty())
            .field("tls_enabled", &(self.tls_mode != MySqlTlsMode::Disabled))
            .field(
                "tls_verify_identity",
                &(self.tls_mode == MySqlTlsMode::VerifyIdentity),
            )
            .field("tls_ca_path_configured", &self.tls_ca_path.is_some())
            .field("tls_cert_path_configured", &self.tls_cert_path.is_some())
            .field("tls_key_path_configured", &self.tls_key_path.is_some())
            .field(
                "connect_timeout_configured",
                &(self.connect_timeout_ms != 0),
            )
            .field(
                "pool_bounds_configured",
                &(self.pool_min != 0 && self.pool_max != 0),
            )
            .field(
                "inactive_connection_ttl_configured",
                &(self.inactive_connection_ttl_ms != 0),
            )
            .finish()
    }
}

pub use provider::MysqlStateStoreProviderFactory;
#[cfg(feature = "state-store-test-hooks")]
#[doc(hidden)]
pub use test_config::{MysqlTestLimitOverrides, MysqlTestProviderConfig, MysqlTestStoreConfig};

struct MysqlStateStore {
    lease: MysqlProviderHandle,
    identity: StoreIdentity,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
    attempts: AttemptSupervisor,
    evidence: Arc<MysqlEvidence>,
}

#[derive(Clone)]
struct MysqlOpenCancellation {
    cancelled: Arc<AtomicBool>,
}

impl MysqlStateStore {
    async fn open(
        lease: MysqlProviderHandle,
        database: String,
        cluster_id: String,
        limits: StateStoreLimits,
        outstanding_attempts: NonZeroUsize,
        deadline: Instant,
        cancellation: MysqlOpenCancellation,
    ) -> Result<Self, StateStoreError> {
        let (identity, _) = schema::validate_store_readiness(
            lease.pool(),
            &database,
            &cluster_id,
            limits.max_key_bytes,
            deadline,
            &cancellation,
        )
        .await?;
        cancellation.check()?;
        tracing::info!(provider = "mysql", client_status = "ready", identity_hash = %codec::redacted_identity_hash(format!("{database}\0{cluster_id}").as_bytes()), "MySQL state store client is ready");
        let evidence = Arc::new(MysqlEvidence::new(
            lease.pool(),
            lease.operations(),
            codec::MysqlCodec::new(limits.max_key_bytes)?,
            limits.transaction_deadline,
        ));
        let attempts = AttemptSupervisor::new(
            outstanding_attempts,
            Arc::clone(&evidence) as Arc<dyn InDoubtAdjudicator>,
        );
        Ok(Self {
            lease,
            identity,
            limits,
            metrics: Arc::new(StateStoreMetrics::new()),
            attempts,
            evidence,
        })
    }
}

pub(crate) fn default_attempt_capacity() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_MAX_OUTSTANDING_ATTEMPTS).expect("default attempt capacity")
}

impl MysqlOpenCancellation {
    fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
    fn check(&self) -> Result<(), StateStoreError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "MySQL state store open waiter was cancelled",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl StateStore for MysqlStateStore {
    fn limits(&self) -> &StateStoreLimits {
        &self.limits
    }
    fn attempts(&self) -> &AttemptSupervisor {
        &self.attempts
    }
    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        let operation = self.lease.acquire_operation()?;
        Ok(Box::new(
            txn::begin_read(
                self.lease.pool(),
                operation,
                self.limits.clone(),
                Arc::clone(&self.metrics),
            )
            .await?,
        ))
    }
    async fn begin_write(
        &self,
        attempt: WriteAttempt,
        _purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        // A capability another instance issued is refused before anything is
        // acquired, so a handle retained across a reopen cannot address this
        // store and is left provably without effect.
        attempt.require_scope(self.attempts.scope())?;
        let operation = self.lease.acquire_operation()?;
        Ok(Box::new(
            txn::begin_write(
                self.lease.pool(),
                operation,
                attempt,
                Arc::clone(&self.evidence),
                self.limits.clone(),
                Arc::clone(&self.metrics),
            )
            .await?,
        ))
    }
    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        Ok(self.identity.clone())
    }
}

fn mysql_host_is_ip_address(host: &str) -> bool {
    let normalized = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    normalized.parse::<IpAddr>().is_ok()
}
fn validate_readable_file(path: &Path, name: &str) -> Result<()> {
    File::open(path).map_err(|error| {
        anyhow::anyhow!("InvalidStateStoreConfig: {name} must be a readable file: {error}")
    })?;
    Ok(())
}
