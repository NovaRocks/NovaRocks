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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
#[cfg(feature = "state-store-test-hooks")]
use std::time::Duration;
use std::time::Instant as StdInstant;

use novarocks_state_store_api::{
    StateStore, StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreOpenRequest,
};
use tokio::sync::{Notify, oneshot};
use tokio::time::{Instant, timeout_at};

#[cfg(feature = "state-store-test-hooks")]
use super::client::delayed_active_readiness as mysql_delayed_active_readiness;
use super::client::{
    PoolLifecycle, ResolvedMysqlClient, active_readiness as mysql_active_readiness,
};
#[cfg(feature = "state-store-test-hooks")]
use super::client::{checkout_hygienic_connection, pollute_session as mysql_pollute_session};
use super::provider::MysqlProviderOpen;
#[cfg(feature = "state-store-test-hooks")]
use super::test_support::{
    MysqlHeldAdvisoryLock, MysqlHeldConnection, MysqlReadinessSnapshot, MysqlRuntimeOwner,
    MysqlSchemaMutation, MysqlSchemaSnapshot, MysqlStoreReadinessSnapshot, MysqlTestHandle,
};
use super::{MysqlOpenCancellation, MysqlStateStore};
#[cfg(feature = "state-store-test-hooks")]
use crate::MYSQL_MAX_KEY_BYTES;
use crate::MySqlClientConfig;
#[cfg(all(test, feature = "state-store-test-hooks"))]
use crate::MySqlTlsMode;
#[cfg(all(test, feature = "state-store-test-hooks"))]
use novarocks_secret::SecretValue;

const RUNTIME_PID_ERROR: &str = "state store runtime belongs to a different process";

pub(super) struct MysqlRuntime {
    shared: Arc<MysqlRuntimeShared>,
    lifecycle: Mutex<MysqlRuntimeLifecycle>,
}

struct MysqlRuntimeShared {
    owner: MysqlRuntimeOwnerState,
    accepting: AtomicBool,
    in_flight: AtomicUsize,
    provider_handles: AtomicUsize,
    client: ResolvedMysqlClient,
    pools_by_database: Mutex<HashMap<String, Arc<dyn PoolLifecycle>>>,
    drained: Notify,
}

enum MysqlRuntimeLifecycle {
    Running,
    Stopped,
}

#[derive(Clone, Copy)]
struct MysqlRuntimeOwnerState {
    pid: u32,
    tokio_runtime_id: tokio::runtime::Id,
}

impl MysqlRuntime {
    pub(super) fn boot(config: MySqlClientConfig) -> Result<Self, StateStoreError> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL state store runtime requires an active Tokio runtime",
            )
        })?;
        let owner = MysqlRuntimeOwnerState {
            pid: std::process::id(),
            tokio_runtime_id: handle.id(),
        };
        Ok(Self {
            shared: Arc::new(MysqlRuntimeShared {
                owner,
                accepting: AtomicBool::new(true),
                in_flight: AtomicUsize::new(0),
                provider_handles: AtomicUsize::new(0),
                client: ResolvedMysqlClient::resolve(config)?,
                pools_by_database: Mutex::new(HashMap::new()),
                drained: Notify::new(),
            }),
            lifecycle: Mutex::new(MysqlRuntimeLifecycle::Running),
        })
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) fn validate_pid_owner(&self, pid: u32) -> Result<(), StateStoreError> {
        if self.shared.owner.pid != pid {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL state store runtime owner does not match",
            ));
        }
        Ok(())
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) fn owner(&self) -> MysqlRuntimeOwner {
        MysqlRuntimeOwner {
            pid: self.shared.owner.pid,
            tokio_runtime_id: self.shared.owner.tokio_runtime_id,
        }
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) fn is_accepting(&self) -> bool {
        self.shared.accepting.load(Ordering::Acquire)
    }

    pub(super) fn validate_process_and_context(&self) -> Result<(), StateStoreError> {
        if self.shared.owner.pid != std::process::id() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                RUNTIME_PID_ERROR,
            ));
        }
        let current = tokio::runtime::Handle::try_current().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL state store operation requires an active Tokio runtime",
            )
        })?;
        if self.shared.owner.tokio_runtime_id != current.id() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidConfiguration,
                "MySQL state store runtime belongs to a different Tokio runtime",
            ));
        }
        Ok(())
    }

    #[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
    pub(super) async fn open_store(
        &self,
        database: String,
        request: StateStoreOpenRequest,
    ) -> Result<Arc<dyn StateStore>, StateStoreError> {
        let mut opening = self.start_open_store(database, request)?;
        let result = (&mut opening.future).await;
        opening.complete();
        result
    }

    pub(super) fn start_open_store(
        &self,
        database: String,
        request: StateStoreOpenRequest,
    ) -> Result<MysqlProviderOpen, StateStoreError> {
        self.validate_process_and_context()?;
        let opening = self.acquire_operation()?;
        let pool = self.get_or_create_pool(&database)?;
        let deadline = std::cmp::min(
            Instant::from_std(request.deadline),
            Instant::now() + request.limits.transaction_deadline,
        );
        let cancellation = MysqlOpenCancellation::new();
        let shared = Arc::clone(&self.shared);
        let cluster_id = request.cluster_id;
        let limits = request.limits;
        let (sender, receiver) = oneshot::channel();
        let task_cancellation = cancellation.clone();
        tokio::spawn(async move {
            let result = Self::open_store_owned(
                shared,
                pool,
                database,
                cluster_id,
                limits,
                deadline,
                task_cancellation,
                opening,
            )
            .await;
            let _ = sender.send(result);
        });
        Ok(MysqlProviderOpen::new(
            Box::pin(async move {
                receiver.await.map_err(|_| {
                    StateStoreError::new(
                        StateStoreErrorKind::ProviderUnavailable,
                        "MySQL state store open task stopped unexpectedly",
                    )
                })?
            }),
            move || cancellation.cancel(),
        ))
    }

    // The spawned open task must own every resource independently of its caller.
    #[allow(clippy::too_many_arguments)]
    async fn open_store_owned(
        shared: Arc<MysqlRuntimeShared>,
        pool: Arc<dyn PoolLifecycle>,
        database: String,
        cluster_id: String,
        limits: StateStoreLimits,
        deadline: Instant,
        cancellation: MysqlOpenCancellation,
        _opening: MysqlRuntimeGuard,
    ) -> Result<Arc<dyn StateStore>, StateStoreError> {
        cancellation.check()?;
        mysql_active_readiness(Arc::clone(&pool), deadline).await?;
        cancellation.check()?;
        let lease = MysqlProviderHandle::new(shared, pool)?;
        let store =
            MysqlStateStore::open(lease, database, cluster_id, limits, deadline, cancellation)
                .await?;
        Ok(Arc::new(store))
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn prepare_pool(&self, database: &str) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        self.get_or_create_pool(database)?;
        Ok(())
    }

    pub(super) fn get_or_create_pool(
        &self,
        database: &str,
    ) -> Result<Arc<dyn PoolLifecycle>, StateStoreError> {
        let mut pools = self.shared.pools_by_database.lock().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "MySQL pool registry is poisoned",
            )
        })?;
        if !pools.contains_key(database) {
            let pool = self.shared.client.build_pool(database)?;
            pools.insert(database.to_owned(), pool);
        }
        Ok(Arc::clone(
            pools
                .get(database)
                .expect("pool exists immediately after insertion"),
        ))
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) fn pool_count(&self) -> Result<usize, StateStoreError> {
        self.shared
            .pools_by_database
            .lock()
            .map(|pools| pools.len())
            .map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "MySQL pool registry is poisoned",
                )
            })
    }

    pub(super) fn acquire_operation(&self) -> Result<MysqlRuntimeGuard, StateStoreError> {
        self.validate_process_and_context()?;
        MysqlRuntimeGuard::acquire(Arc::clone(&self.shared), MysqlGuardKind::Operation)
    }

    #[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
    pub(super) fn acquire_provider_handle(&self) -> Result<MysqlRuntimeGuard, StateStoreError> {
        self.validate_process_and_context()?;
        MysqlRuntimeGuard::acquire(Arc::clone(&self.shared), MysqlGuardKind::Provider)
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) fn begin_shutdown(&self) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        self.shared.accepting.store(false, Ordering::Release);
        Ok(())
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn active_readiness(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<MysqlReadinessSnapshot, StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let deadline = Instant::now() + total_deadline;
        let snapshot = mysql_active_readiness(pool, deadline).await?;
        Ok(MysqlReadinessSnapshot {
            server_version: snapshot.server_version,
            innodb_page_size: snapshot.innodb_page_size,
            innodb_available: snapshot.innodb_available,
            default_storage_engine: snapshot.default_storage_engine,
            sql_mode: snapshot.sql_mode,
            time_zone: snapshot.time_zone,
            character_set: snapshot.character_set,
            connection_id: snapshot.connection_id,
        })
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn pollute_session(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let deadline = Instant::now() + total_deadline;
        mysql_pollute_session(pool, deadline).await
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn delayed_active_readiness(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<MysqlReadinessSnapshot, StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let deadline = Instant::now() + total_deadline;
        let snapshot = mysql_delayed_active_readiness(pool, deadline).await?;
        Ok(MysqlReadinessSnapshot {
            server_version: snapshot.server_version,
            innodb_page_size: snapshot.innodb_page_size,
            innodb_available: snapshot.innodb_available,
            default_storage_engine: snapshot.default_storage_engine,
            sql_mode: snapshot.sql_mode,
            time_zone: snapshot.time_zone,
            character_set: snapshot.character_set,
            connection_id: snapshot.connection_id,
        })
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn hold_connection(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<MysqlHeldConnection, StateStoreError> {
        self.validate_process_and_context()?;
        let operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let deadline = Instant::now() + total_deadline;
        let connection = checkout_hygienic_connection(pool, deadline).await?;
        Ok(MysqlHeldConnection::new(
            connection,
            MysqlTestHandle::new(move || drop(operation)),
        ))
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn schema_snapshot(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<MysqlSchemaSnapshot, StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::schema::snapshot_for_test(pool, Instant::now() + total_deadline).await
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn apply_schema_mutation(
        &self,
        database: &str,
        mutation: MysqlSchemaMutation,
        total_deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::schema::apply_mutation_for_test(pool, mutation, Instant::now() + total_deadline)
            .await
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn acquire_schema_advisory_lock(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<MysqlHeldAdvisoryLock, StateStoreError> {
        self.validate_process_and_context()?;
        let operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let (connection, lock_name) =
            super::schema::acquire_lock_for_test(pool, database, Instant::now() + total_deadline)
                .await?;
        Ok(MysqlHeldAdvisoryLock::new(
            connection,
            MysqlTestHandle::new(move || drop(operation)),
            lock_name,
        ))
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn is_schema_advisory_lock_free(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<bool, StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::schema::is_lock_free_for_test(pool, database, Instant::now() + total_deadline).await
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn store_readiness_snapshot(
        &self,
        database: &str,
        cluster_id: &str,
        total_deadline: Duration,
    ) -> Result<MysqlStoreReadinessSnapshot, StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let (_, readiness) = super::schema::validate_store_readiness(
            pool,
            database,
            cluster_id,
            MYSQL_MAX_KEY_BYTES,
            Instant::now() + total_deadline,
            &MysqlOpenCancellation::new(),
        )
        .await?;
        Ok(readiness)
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn schema_timeout_connection_is_destroyed(
        &self,
        database: &str,
        timeout_deadline: Duration,
        checkout_deadline: Duration,
    ) -> Result<bool, StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::schema::timeout_connection_is_destroyed_for_test(
            pool,
            Instant::now() + timeout_deadline,
            Instant::now() + checkout_deadline,
        )
        .await
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn insert_malformed_kv_row(
        &self,
        database: &str,
        key: &[u8],
        total_deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::txn::insert_malformed_kv_row_for_test(pool, key, Instant::now() + total_deadline)
            .await
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn deadlock_1213_maps_to_conflict(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let first_operation = self.acquire_operation()?;
        let second_operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::txn::deadlock_1213_maps_to_conflict_for_test(
            pool,
            first_operation,
            second_operation,
            Instant::now() + total_deadline,
        )
        .await
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn lock_timeout_1205_rolls_back_before_conflict(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let first_operation = self.acquire_operation()?;
        let second_operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::txn::lock_timeout_1205_rolls_back_before_conflict_for_test(
            pool,
            first_operation,
            second_operation,
            Instant::now() + total_deadline,
        )
        .await
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn hold_kv_lock(
        &self,
        database: &str,
        key: &[u8],
        total_deadline: Duration,
    ) -> Result<super::txn::MysqlHeldKvLock, StateStoreError> {
        self.validate_process_and_context()?;
        let operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        super::txn::hold_kv_lock_for_test(pool, operation, key, Instant::now() + total_deadline)
            .await
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(super) async fn run_sleep_until_deadline(
        &self,
        database: &str,
        total_deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        let _operation = self.acquire_operation()?;
        let pool = self.get_or_create_pool(database)?;
        let deadline = Instant::now() + total_deadline;
        super::client::run_sleep_until_deadline(pool, deadline).await
    }

    pub(super) async fn shutdown_until(
        &mut self,
        deadline: StdInstant,
    ) -> Result<(), StateStoreError> {
        self.shutdown_with_drain_hook(Instant::from_std(deadline), || {})
            .await
    }

    #[cfg(all(test, feature = "state-store-test-hooks"))]
    async fn shutdown_with_drain_registration_hook(
        &mut self,
        timeout: Duration,
        hook: impl FnMut(),
    ) -> Result<(), StateStoreError> {
        self.shutdown_with_drain_hook(Instant::now() + timeout, hook)
            .await
    }

    async fn shutdown_with_drain_hook(
        &mut self,
        deadline: Instant,
        mut after_registration: impl FnMut(),
    ) -> Result<(), StateStoreError> {
        self.validate_process_and_context()?;
        {
            let lifecycle = self.lifecycle.lock().map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "MySQL runtime lifecycle is poisoned",
                )
            })?;
            match &*lifecycle {
                MysqlRuntimeLifecycle::Stopped => return Ok(()),
                MysqlRuntimeLifecycle::Running => {}
            }
        }
        self.shared.accepting.store(false, Ordering::Release);
        loop {
            let notified = self.shared.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            after_registration();
            if self.shared.is_drained() {
                break;
            }
            if timeout_at(deadline, notified).await.is_err() {
                self.shared.accepting.store(true, Ordering::Release);
                return Err(StateStoreError::new(
                    StateStoreErrorKind::DeadlineExceeded,
                    "MySQL runtime handles did not drain before shutdown deadline",
                ));
            }
        }

        let pools = self
            .shared
            .pools_by_database
            .lock()
            .map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "MySQL pool registry is poisoned",
                )
            })?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for pool in pools {
            if Instant::now() >= deadline {
                self.shared.accepting.store(true, Ordering::Release);
                return Err(shutdown_deadline_error());
            }
            let disconnect = match timeout_at(deadline, pool.disconnect()).await {
                Ok(disconnect) => disconnect,
                Err(_) => {
                    self.shared.accepting.store(true, Ordering::Release);
                    return Err(shutdown_deadline_error());
                }
            };
            if let Err(native) = disconnect {
                return Err(native.into_public());
            }
        }
        self.shared
            .pools_by_database
            .lock()
            .map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "MySQL pool registry is poisoned",
                )
            })?
            .clear();
        let mut lifecycle = self.lifecycle.lock().map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "MySQL runtime lifecycle is poisoned",
            )
        })?;
        *lifecycle = MysqlRuntimeLifecycle::Stopped;
        Ok(())
    }
}

fn shutdown_deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "MySQL runtime handles did not drain before shutdown deadline",
    )
}

#[cfg(feature = "mysql-state-store-provider")]
enum MysqlGuardKind {
    Operation,
    Provider,
}

#[cfg(feature = "mysql-state-store-provider")]
pub(super) struct MysqlProviderHandle {
    _provider: MysqlRuntimeGuard,
    pool: Arc<dyn PoolLifecycle>,
}

#[cfg(feature = "mysql-state-store-provider")]
impl MysqlProviderHandle {
    fn new(
        shared: Arc<MysqlRuntimeShared>,
        pool: Arc<dyn PoolLifecycle>,
    ) -> Result<Self, StateStoreError> {
        Ok(Self {
            _provider: MysqlRuntimeGuard::acquire(shared, MysqlGuardKind::Provider)?,
            pool,
        })
    }

    pub(super) fn pool(&self) -> Arc<dyn PoolLifecycle> {
        Arc::clone(&self.pool)
    }

    pub(super) fn acquire_operation(&self) -> Result<MysqlRuntimeGuard, StateStoreError> {
        MysqlRuntimeGuard::acquire(
            Arc::clone(&self._provider.shared),
            MysqlGuardKind::Operation,
        )
    }
}

#[cfg(feature = "mysql-state-store-provider")]
pub(super) struct MysqlRuntimeGuard {
    shared: Arc<MysqlRuntimeShared>,
    kind: MysqlGuardKind,
}

#[cfg(feature = "mysql-state-store-provider")]
impl MysqlRuntimeGuard {
    fn acquire(
        shared: Arc<MysqlRuntimeShared>,
        kind: MysqlGuardKind,
    ) -> Result<Self, StateStoreError> {
        if !shared.accepting.load(Ordering::Acquire) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "MySQL state store runtime is stopping",
            ));
        }
        let counter = match kind {
            MysqlGuardKind::Operation => &shared.in_flight,
            MysqlGuardKind::Provider => &shared.provider_handles,
        };
        counter.fetch_add(1, Ordering::AcqRel);
        if !shared.accepting.load(Ordering::Acquire) {
            counter.fetch_sub(1, Ordering::AcqRel);
            shared.notify_if_drained();
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "MySQL state store runtime is stopping",
            ));
        }
        Ok(Self { shared, kind })
    }
}

#[cfg(feature = "mysql-state-store-provider")]
impl Drop for MysqlRuntimeGuard {
    fn drop(&mut self) {
        let counter = match self.kind {
            MysqlGuardKind::Operation => &self.shared.in_flight,
            MysqlGuardKind::Provider => &self.shared.provider_handles,
        };
        counter.fetch_sub(1, Ordering::AcqRel);
        self.shared.notify_if_drained();
    }
}

#[cfg(feature = "mysql-state-store-provider")]
impl MysqlRuntimeShared {
    fn is_drained(&self) -> bool {
        self.in_flight.load(Ordering::Acquire) == 0
            && self.provider_handles.load(Ordering::Acquire) == 0
    }

    fn notify_if_drained(&self) {
        if self.is_drained() {
            self.drained.notify_waiters();
        }
    }
}

#[cfg(all(test, feature = "state-store-test-hooks"))]
pub(super) fn test_mysql_runtime_with_pool(pool: Arc<dyn PoolLifecycle>) -> MysqlRuntime {
    let client = ResolvedMysqlClient::resolve(MySqlClientConfig {
        host: "localhost".to_owned(),
        port: 3306,
        username: "runtime-test".to_owned(),
        password: SecretValue::new("test-secret"),
        tls_mode: MySqlTlsMode::Disabled,
        tls_ca_path: None,
        tls_cert_path: None,
        tls_key_path: None,
        connect_timeout_ms: 100,
        pool_min: 1,
        pool_max: 1,
        inactive_connection_ttl_ms: 1_000,
    })
    .expect("test MySQL client");
    let mut pools = HashMap::new();
    pools.insert("test".to_owned(), pool);
    MysqlRuntime {
        shared: Arc::new(MysqlRuntimeShared {
            owner: MysqlRuntimeOwnerState {
                pid: std::process::id(),
                tokio_runtime_id: tokio::runtime::Handle::current().id(),
            },
            accepting: AtomicBool::new(true),
            in_flight: AtomicUsize::new(0),
            provider_handles: AtomicUsize::new(0),
            client,
            pools_by_database: Mutex::new(pools),
            drained: Notify::new(),
        }),
        lifecycle: Mutex::new(MysqlRuntimeLifecycle::Running),
    }
}

#[cfg(all(test, feature = "state-store-test-hooks"))]
mod mysql_tests {
    use super::*;
    use futures::future::BoxFuture;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FailingPool {
        disconnects: Arc<AtomicUsize>,
    }

    struct FailOncePool {
        disconnects: Arc<AtomicUsize>,
    }

    impl PoolLifecycle for FailingPool {
        fn get_conn<'a>(
            &'a self,
            _deadline: Instant,
        ) -> BoxFuture<
            'a,
            Result<
                super::super::client::MysqlPoolConnection,
                super::super::error::MysqlNativeError,
            >,
        > {
            Box::pin(async { Err(super::super::error::MysqlNativeError::provider_unavailable()) })
        }

        fn disconnect(
            self: Arc<Self>,
        ) -> BoxFuture<'static, Result<(), super::super::error::MysqlNativeError>> {
            Box::pin(async move {
                self.disconnects.fetch_add(1, Ordering::AcqRel);
                Err(super::super::error::MysqlNativeError::provider_unavailable())
            })
        }
    }

    impl PoolLifecycle for FailOncePool {
        fn get_conn<'a>(
            &'a self,
            _deadline: Instant,
        ) -> BoxFuture<
            'a,
            Result<
                super::super::client::MysqlPoolConnection,
                super::super::error::MysqlNativeError,
            >,
        > {
            Box::pin(async { Err(super::super::error::MysqlNativeError::provider_unavailable()) })
        }

        fn disconnect(
            self: Arc<Self>,
        ) -> BoxFuture<'static, Result<(), super::super::error::MysqlNativeError>> {
            Box::pin(async move {
                let attempt = self.disconnects.fetch_add(1, Ordering::AcqRel);
                if attempt == 0 {
                    Err(super::super::error::MysqlNativeError::provider_unavailable())
                } else {
                    Ok(())
                }
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mysql_disconnect_failure_retains_pool_and_retries_to_stopped() {
        let disconnects = Arc::new(AtomicUsize::new(0));
        let pool: Arc<dyn PoolLifecycle> = Arc::new(FailOncePool {
            disconnects: Arc::clone(&disconnects),
        });
        let mut runtime = test_mysql_runtime_with_pool(pool);

        let first = runtime
            .shutdown_until(StdInstant::now() + Duration::from_secs(1))
            .await
            .expect_err("disconnect failure must surface");
        assert_eq!(first.kind(), StateStoreErrorKind::ProviderUnavailable);
        assert_eq!(disconnects.load(Ordering::Acquire), 1);
        assert_eq!(runtime.pool_count().expect("pool ownership"), 1);
        assert!(!runtime.is_accepting());
        let blocked = match runtime.acquire_operation() {
            Ok(_) => panic!("draining runtime must not accept new work"),
            Err(error) => error,
        };
        assert_eq!(blocked.kind(), StateStoreErrorKind::ProviderUnavailable);

        runtime
            .shutdown_until(StdInstant::now() + Duration::from_secs(1))
            .await
            .expect("disconnect retry must complete");

        assert_eq!(disconnects.load(Ordering::Acquire), 2);
        assert_eq!(runtime.pool_count().expect("pool ownership"), 0);
        assert!(matches!(
            *runtime.lifecycle.lock().expect("lifecycle"),
            MysqlRuntimeLifecycle::Stopped
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mysql_shutdown_does_not_start_pool_disconnect_after_deadline() {
        let disconnects = Arc::new(AtomicUsize::new(0));
        let pool: Arc<dyn PoolLifecycle> = Arc::new(FailingPool {
            disconnects: Arc::clone(&disconnects),
        });
        let mut runtime = test_mysql_runtime_with_pool(pool);

        let error = runtime
            .shutdown_until(StdInstant::now())
            .await
            .expect_err("an expired deadline must stop shutdown before pool I/O");

        assert_eq!(error.kind(), StateStoreErrorKind::DeadlineExceeded);
        assert_eq!(disconnects.load(Ordering::Acquire), 0);
        assert!(runtime.is_accepting(), "deadline failure must be retryable");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mysql_shutdown_does_not_lose_final_guard_notification() {
        let pool: Arc<dyn PoolLifecycle> = Arc::new(FailingPool {
            disconnects: Arc::new(AtomicUsize::new(0)),
        });
        let mut runtime = test_mysql_runtime_with_pool(pool);
        let guard = runtime
            .acquire_provider_handle()
            .expect("acquire final provider handle");
        let entered = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let shutdown = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                let result = runtime
                    .shutdown_with_drain_registration_hook(Duration::from_millis(250), move || {
                        entered.wait();
                        release.wait();
                    })
                    .await;
                (runtime, result)
            })
        };
        entered.wait();
        drop(guard);
        release.wait();

        let (runtime, result) = shutdown.await.expect("join shutdown");
        let error = result.expect_err("the injected pool disconnect still fails");
        assert_eq!(error.kind(), StateStoreErrorKind::ProviderUnavailable);
        assert!(
            !runtime.shared.accepting.load(Ordering::Acquire),
            "a completed drain must not reopen the runtime"
        );
    }
}
