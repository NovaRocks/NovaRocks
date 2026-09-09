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

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
#[cfg(feature = "state-store-test-hooks")]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use mysql_async::{
    ClientIdentity, Conn, OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts, prelude::Queryable,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, timeout_at};

use novarocks_state_store_api::{StateStoreError, StateStoreErrorKind};

use super::error::MysqlNativeError;
use crate::{MySqlClientConfig, MySqlTlsMode};

#[cfg(feature = "state-store-test-hooks")]
static STATEMENT_COUNT: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "state-store-test-hooks")]
static LAST_EXPLICITLY_DESTROYED_CONNECTION_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) fn record_statement() {
    #[cfg(feature = "state-store-test-hooks")]
    STATEMENT_COUNT.fetch_add(1, Ordering::Relaxed);
}

#[cfg(feature = "state-store-test-hooks")]
pub(crate) fn statement_count_for_test() -> u64 {
    STATEMENT_COUNT.load(Ordering::Relaxed)
}

#[cfg(feature = "state-store-test-hooks")]
pub(crate) fn reset_last_explicit_destroy_for_test() {
    LAST_EXPLICITLY_DESTROYED_CONNECTION_ID.store(0, Ordering::Release);
}

#[cfg(feature = "state-store-test-hooks")]
pub(crate) fn last_explicitly_destroyed_connection_id_for_test() -> u64 {
    LAST_EXPLICITLY_DESTROYED_CONNECTION_ID.load(Ordering::Acquire)
}

pub(crate) trait PoolLifecycle: Send + Sync {
    fn get_conn<'a>(
        &'a self,
        deadline: Instant,
    ) -> BoxFuture<'a, Result<MysqlPoolConnection, MysqlNativeError>>;

    fn disconnect(self: Arc<Self>) -> BoxFuture<'static, Result<(), MysqlNativeError>>;
}

pub(crate) struct MysqlAsyncPoolLifecycle {
    pool: Pool,
    connect_timeout: Duration,
    checkout_slots: Arc<Semaphore>,
}

pub(crate) struct ResolvedMysqlClient {
    config: MySqlClientConfig,
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(crate) struct MysqlClientReadiness {
    pub(crate) server_version: String,
    pub(crate) innodb_page_size: u64,
    pub(crate) innodb_available: bool,
    pub(crate) default_storage_engine: String,
    pub(crate) sql_mode: String,
    pub(crate) time_zone: String,
    pub(crate) character_set: String,
    pub(crate) connection_id: u64,
}

pub(crate) struct MysqlPoolConnection {
    connection: Option<Conn>,
    _checkout_slot: OwnedSemaphorePermit,
}

const READINESS_SQL: &str = "SELECT VERSION(), @@innodb_page_size, \
    EXISTS(SELECT 1 FROM information_schema.ENGINES \
    WHERE ENGINE = 'InnoDB' AND SUPPORT IN ('YES', 'DEFAULT')), \
    @@default_storage_engine, @@SESSION.sql_mode, @@SESSION.time_zone, \
    @@SESSION.character_set_connection, CONNECTION_ID()";

#[cfg(feature = "state-store-test-hooks")]
const DELAYED_READINESS_SQL: &str = "SELECT IF(SLEEP(10) = 0, VERSION(), VERSION()), \
    @@innodb_page_size, EXISTS(SELECT 1 FROM information_schema.ENGINES \
    WHERE ENGINE = 'InnoDB' AND SUPPORT IN ('YES', 'DEFAULT')), \
    @@default_storage_engine, @@SESSION.sql_mode, @@SESSION.time_zone, \
    @@SESSION.character_set_connection, CONNECTION_ID()";

pub(crate) async fn active_readiness(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<MysqlClientReadiness, StateStoreError> {
    active_readiness_with_sql(pool, deadline, READINESS_SQL).await
}

#[cfg(feature = "state-store-test-hooks")]
pub(crate) async fn delayed_active_readiness(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<MysqlClientReadiness, StateStoreError> {
    active_readiness_with_sql(pool, deadline, DELAYED_READINESS_SQL).await
}

async fn active_readiness_with_sql(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
    sql: &'static str,
) -> Result<MysqlClientReadiness, StateStoreError> {
    let connection = checkout_hygienic_connection(pool, deadline).await?;
    let (_connection, row) = execute_with_deadline(connection, deadline, move |connection| {
        Box::pin(connection.query_first(sql))
    })
    .await?;
    type MysqlReadinessRow = (String, u64, u8, String, String, String, String, u64);
    let row: Option<MysqlReadinessRow> = row;
    let (
        server_version,
        innodb_page_size,
        innodb_available,
        default_storage_engine,
        sql_mode,
        time_zone,
        character_set,
        connection_id,
    ) = row.ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::Corruption,
            "MySQL readiness query returned no row",
        )
    })?;
    if server_version != "8.4.10" {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL server version is not the supported 8.4.10 release",
        ));
    }
    if innodb_page_size != 16_384 || innodb_available == 0 {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL InnoDB readiness contract is not satisfied",
        ));
    }
    if !default_storage_engine.eq_ignore_ascii_case("InnoDB") {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL default storage engine is not InnoDB",
        ));
    }
    if !sql_mode
        .split(',')
        .any(|mode| mode.trim().starts_with("STRICT_"))
        || time_zone != "+00:00"
        || character_set != "utf8mb4"
    {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL session readiness contract is not satisfied",
        ));
    }
    Ok(MysqlClientReadiness {
        server_version,
        innodb_page_size,
        innodb_available: true,
        default_storage_engine,
        sql_mode,
        time_zone,
        character_set,
        connection_id,
    })
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(crate) async fn pollute_session(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let connection = checkout_connection(pool, deadline).await?;
    let (_connection, ()) = execute_with_deadline(connection, deadline, |connection| {
        Box::pin(connection.query_drop(
            "SET SESSION time_zone = '+05:00', SESSION sql_mode = '', \
             SESSION character_set_connection = 'latin1'",
        ))
    })
    .await?;
    Ok(())
}

#[cfg(feature = "state-store-test-hooks")]
pub(crate) async fn run_sleep_until_deadline(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let connection = checkout_hygienic_connection(pool, deadline).await?;
    let (_connection, ()) = execute_with_deadline(connection, deadline, |connection| {
        Box::pin(connection.query_drop("SELECT SLEEP(10)"))
    })
    .await?;
    Ok(())
}

pub(crate) async fn checkout_hygienic_connection(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<MysqlPoolConnection, StateStoreError> {
    let connection = checkout_connection(pool, deadline).await?;
    let (connection, ()) = execute_with_deadline(connection, deadline, |connection| {
        Box::pin(apply_session_hygiene(connection))
    })
    .await?;
    Ok(connection)
}

async fn checkout_connection(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<MysqlPoolConnection, StateStoreError> {
    pool.get_conn(deadline)
        .await
        .map_err(MysqlNativeError::into_public)
}

async fn execute_with_deadline<T>(
    mut connection: MysqlPoolConnection,
    deadline: Instant,
    operation: impl for<'a> FnOnce(&'a mut Conn) -> BoxFuture<'a, Result<T, mysql_async::Error>>,
) -> Result<(MysqlPoolConnection, T), StateStoreError> {
    record_statement();
    match timeout_at(deadline, operation(&mut connection)).await {
        Ok(Ok(value)) => Ok((connection, value)),
        Ok(Err(error)) => Err(MysqlNativeError::from(error).into_public()),
        Err(_) => {
            connection.destroy().await;
            Err(mysql_deadline_error())
        }
    }
}

pub(crate) async fn execute_owned_with_deadline<T>(
    mut connection: MysqlPoolConnection,
    deadline: Instant,
    operation: impl for<'a> FnOnce(&'a mut Conn) -> BoxFuture<'a, Result<T, mysql_async::Error>>,
) -> Result<(MysqlPoolConnection, Result<T, MysqlNativeError>), StateStoreError> {
    record_statement();
    match timeout_at(deadline, operation(&mut connection)).await {
        Ok(result) => Ok((connection, result.map_err(MysqlNativeError::from))),
        Err(_) => {
            connection.destroy().await;
            Err(mysql_deadline_error())
        }
    }
}

async fn apply_session_hygiene(connection: &mut Conn) -> Result<(), mysql_async::Error> {
    connection
        .query_drop(
            "SET SESSION time_zone = '+00:00', \
             SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION', \
             SESSION character_set_client = 'utf8mb4', \
             SESSION character_set_connection = 'utf8mb4', \
             SESSION character_set_results = 'utf8mb4'",
        )
        .await
}

fn mysql_deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "MySQL provider operation exceeded its deadline",
    )
}

impl ResolvedMysqlClient {
    pub(crate) fn resolve(config: MySqlClientConfig) -> Result<Self, StateStoreError> {
        config.validate().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL client configuration is invalid",
            )
        })?;
        Ok(Self { config })
    }

    pub(crate) fn build_pool(
        &self,
        database: &str,
    ) -> Result<Arc<dyn PoolLifecycle>, StateStoreError> {
        let constraints = PoolConstraints::new(self.config.pool_min, self.config.pool_max)
            .ok_or_else(|| {
                StateStoreError::new(
                    StateStoreErrorKind::InvalidConfiguration,
                    "MySQL pool constraints are invalid",
                )
            })?;
        let pool_opts = PoolOpts::new()
            .with_constraints(constraints)
            .with_inactive_connection_ttl(Duration::from_millis(
                self.config.inactive_connection_ttl_ms,
            ))
            .with_reset_connection(true);
        let ssl_opts = self.ssl_opts();
        let opts = OptsBuilder::default()
            .ip_or_hostname(self.config.host.clone())
            .tcp_port(self.config.port)
            .user(Some(self.config.username.clone()))
            .pass(Some(self.config.password.expose_secret().to_owned()))
            .db_name(Some(database.to_owned()))
            .prefer_socket(false)
            .pool_opts(pool_opts)
            .ssl_opts(ssl_opts);
        Ok(Arc::new(MysqlAsyncPoolLifecycle {
            pool: Pool::new(opts),
            connect_timeout: Duration::from_millis(self.config.connect_timeout_ms),
            checkout_slots: Arc::new(Semaphore::new(self.config.pool_max)),
        }))
    }

    fn ssl_opts(&self) -> Option<SslOpts> {
        match self.config.tls_mode {
            MySqlTlsMode::Disabled => None,
            MySqlTlsMode::Required => Some(
                SslOpts::default()
                    .with_danger_skip_domain_validation(true)
                    .with_danger_accept_invalid_certs(true),
            ),
            MySqlTlsMode::VerifyIdentity => {
                let mut ssl = SslOpts::default().with_disable_built_in_roots(true);
                if let Some(path) = self.config.tls_ca_path.clone() {
                    ssl = ssl.with_root_certs(vec![path.into()]);
                }
                if let (Some(cert), Some(key)) = (
                    self.config.tls_cert_path.clone(),
                    self.config.tls_key_path.clone(),
                ) {
                    ssl = ssl
                        .with_client_identity(Some(ClientIdentity::new(cert.into(), key.into())));
                }
                Some(ssl)
            }
        }
    }
}

impl PoolLifecycle for MysqlAsyncPoolLifecycle {
    fn get_conn<'a>(
        &'a self,
        deadline: Instant,
    ) -> BoxFuture<'a, Result<MysqlPoolConnection, MysqlNativeError>> {
        Box::pin(async move {
            let checkout_slot =
                timeout_at(deadline, Arc::clone(&self.checkout_slots).acquire_owned())
                    .await
                    .map_err(|_| MysqlNativeError::deadline())?
                    .map_err(|_| MysqlNativeError::provider_unavailable())?;
            let connect_deadline = deadline.min(Instant::now() + self.connect_timeout);
            let connection = timeout_at(connect_deadline, self.pool.get_conn())
                .await
                .map_err(|_| MysqlNativeError::deadline())?
                .map_err(MysqlNativeError::from)?;
            Ok(MysqlPoolConnection {
                connection: Some(connection),
                _checkout_slot: checkout_slot,
            })
        })
    }

    fn disconnect(self: Arc<Self>) -> BoxFuture<'static, Result<(), MysqlNativeError>> {
        Box::pin(async move {
            self.pool
                .clone()
                .disconnect()
                .await
                .map_err(MysqlNativeError::from)
        })
    }
}

impl MysqlPoolConnection {
    pub(crate) async fn destroy(mut self) {
        self.destroy_in_place().await;
    }

    pub(crate) async fn destroy_in_place(&mut self) {
        if let Some(connection) = self.connection.take() {
            #[cfg(feature = "state-store-test-hooks")]
            LAST_EXPLICITLY_DESTROYED_CONNECTION_ID
                .store(u64::from(connection.id()), Ordering::Release);
            let _ = tokio::time::timeout(Duration::from_secs(1), connection.disconnect()).await;
        }
    }
}

impl Deref for MysqlPoolConnection {
    type Target = Conn;

    fn deref(&self) -> &Self::Target {
        self.connection
            .as_ref()
            .expect("connection remains present until disposition")
    }
}

impl DerefMut for MysqlPoolConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection
            .as_mut()
            .expect("connection remains present until disposition")
    }
}

impl fmt::Debug for ResolvedMysqlClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedMysqlClient")
            .field("client_configured", &true)
            .field(
                "tls_enabled",
                &(self.config.tls_mode != MySqlTlsMode::Disabled),
            )
            .field("password_configured", &!self.config.password.is_empty())
            .finish()
    }
}
