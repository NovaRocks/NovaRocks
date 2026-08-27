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

#[cfg(feature = "state-store-test-hooks")]
use novarocks_spi::state_store::{
    ChangeCursor, ChangePollRequest, CommitOutcome, CommitReceipt, CommitResolution, Key,
    StateRecord, StoreRevision, TransactionId, Value,
};
use novarocks_spi::state_store::{
    Precondition, StateStore, StateStoreError, StateStoreErrorKind, StateStoreOpenRequest,
};

use super::client::MysqlPoolConnection;
use super::runtime::MysqlRuntime;
use crate::{MySqlClientConfig, MysqlTestStoreConfig};
#[cfg(feature = "state-store-test-hooks")]
use bytes::Bytes;
#[cfg(feature = "state-store-test-hooks")]
use mysql_async::prelude::Queryable;
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(feature = "state-store-test-hooks")]
use uuid::Uuid;

fn repository_root_for_test() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("MySQL provider manifest must be nested under the repository novarocks directory")
        .to_path_buf()
}

#[cfg(feature = "state-store-test-hooks")]
pub use super::open_test_hooks::{MysqlOpenGateControl, MysqlOpenGatePhase, arm_mysql_open_gate};
pub use super::schema::{
    SchemaColumnSnapshot as MysqlSchemaColumnSnapshot, SchemaMutation as MysqlSchemaMutation,
    SchemaSnapshot as MysqlSchemaSnapshot, SchemaTableSnapshot as MysqlSchemaTableSnapshot,
    StoreReadinessSnapshot as MysqlStoreReadinessSnapshot,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlRuntimeOwner {
    pub pid: u32,
    pub tokio_runtime_id: tokio::runtime::Id,
}

pub struct MysqlTestHandle {
    dropper: Option<Box<dyn FnOnce() + Send>>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MysqlReadinessSnapshot {
    pub server_version: String,
    pub innodb_page_size: u64,
    pub innodb_available: bool,
    pub default_storage_engine: String,
    pub sql_mode: String,
    pub time_zone: String,
    pub character_set: String,
    pub connection_id: u64,
}

pub struct MysqlHeldConnection {
    connection: Option<MysqlPoolConnection>,
    operation: Option<MysqlTestHandle>,
}

pub struct MysqlHeldAdvisoryLock {
    connection: Option<MysqlPoolConnection>,
    operation: Option<MysqlTestHandle>,
    lock_name: String,
}

pub struct MysqlHeldKvLock {
    inner: Option<super::txn::MysqlHeldKvLock>,
}
#[cfg(feature = "state-store-test-hooks")]
pub struct MysqlHeldCommitLedgerLock {
    connection: Option<MysqlPoolConnection>,
    operation: Option<MysqlTestHandle>,
}

pub struct MysqlTransactionTestApi;
pub struct MysqlWriteTestApi;
pub struct MysqlOccTestApi;
pub struct MysqlChangeTestApi;
pub struct MysqlCommitTestApi;

pub struct MysqlProviderTestHarness {
    runtime: MysqlRuntime,
}

impl std::fmt::Debug for MysqlProviderTestHarness {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MysqlProviderTestHarness::Mysql")
    }
}

impl MysqlProviderTestHarness {
    pub fn boot(config: MySqlClientConfig) -> Result<Self, StateStoreError> {
        Ok(Self {
            runtime: MysqlRuntime::boot(config)?,
        })
    }

    pub async fn open_store(
        &self,
        config: MysqlTestStoreConfig,
        deadline: Instant,
    ) -> Result<Arc<dyn StateStore>, StateStoreError> {
        let config = config.into_mysql_open()?;
        self.runtime
            .open_store(
                config.database,
                StateStoreOpenRequest {
                    cluster_id: config.cluster_id,
                    limits: config.limits,
                    deadline,
                },
            )
            .await
    }

    pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreError> {
        self.runtime.shutdown_until(deadline).await
    }

    pub(crate) fn mysql_test_owner(&self) -> Result<MysqlRuntimeOwner, StateStoreError> {
        Ok(self.runtime.owner())
    }

    pub(crate) fn mysql_test_validate_owner(&self, pid: u32) -> Result<(), StateStoreError> {
        self.runtime.validate_pid_owner(pid)
    }

    pub(crate) async fn mysql_test_prepare_pool(
        &self,
        database: &str,
    ) -> Result<(), StateStoreError> {
        self.runtime.prepare_pool(database).await
    }

    pub(crate) fn mysql_test_pool(
        &self,
        database: &str,
    ) -> Result<Arc<dyn super::client::PoolLifecycle>, StateStoreError> {
        self.runtime.get_or_create_pool(database)
    }

    pub(crate) fn mysql_test_pool_count(&self) -> Result<usize, StateStoreError> {
        self.runtime.pool_count()
    }

    pub(crate) fn mysql_test_acquire_provider_handle(
        &self,
    ) -> Result<MysqlTestHandle, StateStoreError> {
        let guard = self.runtime.acquire_provider_handle()?;
        Ok(MysqlTestHandle::new(move || drop(guard)))
    }

    pub(crate) fn mysql_test_acquire_operation(&self) -> Result<MysqlTestHandle, StateStoreError> {
        let guard = self.runtime.acquire_operation()?;
        Ok(MysqlTestHandle::new(move || drop(guard)))
    }

    pub(crate) fn mysql_test_is_accepting(&self) -> Result<bool, StateStoreError> {
        Ok(self.runtime.is_accepting())
    }

    pub(crate) fn mysql_test_begin_shutdown(&self) -> Result<(), StateStoreError> {
        self.runtime.begin_shutdown()
    }

    pub(crate) async fn mysql_test_active_readiness(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<MysqlReadinessSnapshot, StateStoreError> {
        self.runtime.active_readiness(database, deadline).await
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(crate) async fn mysql_test_delayed_active_readiness(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<MysqlReadinessSnapshot, StateStoreError> {
        self.runtime
            .delayed_active_readiness(database, deadline)
            .await
    }

    pub(crate) async fn mysql_test_pollute_session(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.runtime.pollute_session(database, deadline).await
    }

    pub(crate) async fn mysql_test_hold_connection(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<MysqlHeldConnection, StateStoreError> {
        self.runtime.hold_connection(database, deadline).await
    }

    pub(crate) async fn mysql_test_schema_snapshot(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<MysqlSchemaSnapshot, StateStoreError> {
        self.runtime.schema_snapshot(database, deadline).await
    }

    pub(crate) async fn mysql_test_apply_schema_mutation(
        &self,
        database: &str,
        mutation: MysqlSchemaMutation,
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.runtime
            .apply_schema_mutation(database, mutation, deadline)
            .await
    }

    pub(crate) async fn mysql_test_acquire_schema_advisory_lock(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<MysqlHeldAdvisoryLock, StateStoreError> {
        self.runtime
            .acquire_schema_advisory_lock(database, deadline)
            .await
    }

    pub(crate) async fn mysql_test_is_schema_advisory_lock_free(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<bool, StateStoreError> {
        self.runtime
            .is_schema_advisory_lock_free(database, deadline)
            .await
    }

    pub(crate) async fn mysql_test_store_readiness_snapshot(
        &self,
        database: &str,
        cluster_id: &str,
        deadline: Duration,
    ) -> Result<MysqlStoreReadinessSnapshot, StateStoreError> {
        self.runtime
            .store_readiness_snapshot(database, cluster_id, deadline)
            .await
    }

    pub(crate) async fn mysql_test_schema_timeout_connection_is_destroyed(
        &self,
        database: &str,
        timeout_deadline: Duration,
        checkout_deadline: Duration,
    ) -> Result<bool, StateStoreError> {
        self.runtime
            .schema_timeout_connection_is_destroyed(database, timeout_deadline, checkout_deadline)
            .await
    }

    pub(crate) async fn mysql_test_insert_malformed_kv_row(
        &self,
        database: &str,
        key: &[u8],
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.runtime
            .insert_malformed_kv_row(database, key, deadline)
            .await
    }

    pub(crate) async fn mysql_test_deadlock_1213_maps_to_conflict(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.runtime
            .deadlock_1213_maps_to_conflict(database, deadline)
            .await
    }

    pub(crate) async fn mysql_test_lock_timeout_1205_rolls_back_before_conflict(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.runtime
            .lock_timeout_1205_rolls_back_before_conflict(database, deadline)
            .await
    }

    pub(crate) async fn mysql_test_hold_kv_lock(
        &self,
        database: &str,
        key: &[u8],
        deadline: Duration,
    ) -> Result<super::txn::MysqlHeldKvLock, StateStoreError> {
        self.runtime.hold_kv_lock(database, key, deadline).await
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub(crate) async fn mysql_test_run_sleep_until_deadline(
        &self,
        database: &str,
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        self.runtime
            .run_sleep_until_deadline(database, deadline)
            .await
    }
}
#[cfg(feature = "state-store-test-hooks")]
pub struct MysqlPollQueryTestControl {
    inner: super::changes::PollQueryHookControl,
}
#[cfg(feature = "state-store-test-hooks")]
pub struct MysqlPostDispatchTestControl {
    inner: super::commit::CommitHookControl,
}
#[cfg(feature = "state-store-test-hooks")]
#[derive(Clone, Copy, Debug)]
pub enum MysqlPrepareRollbackFailure {
    Error,
    Timeout,
}
#[cfg(feature = "state-store-test-hooks")]
pub struct MysqlStatementTestApi;

#[cfg(feature = "state-store-test-hooks")]
impl MysqlStatementTestApi {
    pub fn statement_count() -> u64 {
        super::client::statement_count_for_test()
    }

    pub fn last_write_actor_connection_id() -> u64 {
        super::txn::last_write_actor_connection_id_for_test()
    }

    pub fn reset_last_explicit_destroy() {
        super::client::reset_last_explicit_destroy_for_test();
    }

    pub fn last_explicitly_destroyed_connection_id() -> u64 {
        super::client::last_explicitly_destroyed_connection_id_for_test()
    }
}

impl MysqlWriteTestApi {
    pub fn transaction_envelope_bytes() -> usize {
        super::budget::TRANSACTION_ENVELOPE_BYTES
    }

    pub fn put_accounted_bytes(
        key: &[u8],
        value: &[u8],
        precondition: &Precondition,
    ) -> Result<usize, StateStoreError> {
        super::budget::accounted_put_bytes(key, value, precondition)
    }

    pub fn delete_accounted_bytes(
        key: &[u8],
        precondition: &Precondition,
    ) -> Result<usize, StateStoreError> {
        super::budget::accounted_delete_bytes(key, precondition)
    }
}

impl MysqlOccTestApi {
    #[cfg(feature = "state-store-test-hooks")]
    pub fn last_touched_lock_order() -> Vec<Vec<u8>> {
        super::txn::last_touched_lock_order_for_test()
    }

    pub async fn hold_kv_lock(
        runtime: &MysqlProviderTestHarness,
        database: &str,
        key: &[u8],
        deadline: Duration,
    ) -> Result<MysqlHeldKvLock, StateStoreError> {
        Ok(MysqlHeldKvLock {
            inner: Some(
                runtime
                    .mysql_test_hold_kv_lock(database, key, deadline)
                    .await?,
            ),
        })
    }

    pub async fn deadlock_1213_maps_to_conflict(
        runtime: &MysqlProviderTestHarness,
        database: &str,
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        runtime
            .mysql_test_deadlock_1213_maps_to_conflict(database, deadline)
            .await
    }

    pub async fn lock_timeout_1205_rolls_back_before_conflict(
        runtime: &MysqlProviderTestHarness,
        database: &str,
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        runtime
            .mysql_test_lock_timeout_1205_rolls_back_before_conflict(database, deadline)
            .await
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub async fn statement_deadline_destroys_undrained_connection(
        runtime: &MysqlProviderTestHarness,
        database: &str,
    ) -> Result<(), StateStoreError> {
        let before = active_readiness(runtime, database, Duration::from_secs(4))
            .await?
            .connection_id;
        let error = run_sleep_until_deadline(runtime, database, Duration::from_millis(100))
            .await
            .expect_err("sleep must exceed the statement deadline");
        if error.kind() != StateStoreErrorKind::DeadlineExceeded {
            return Err(error);
        }
        let after = active_readiness(runtime, database, Duration::from_secs(4))
            .await?
            .connection_id;
        if before == after {
            return Err(StateStoreError::new(
                StateStoreErrorKind::Internal,
                "timed out MySQL connection was returned to the pool",
            ));
        }
        Ok(())
    }
}

#[cfg(feature = "state-store-test-hooks")]
impl MysqlChangeTestApi {
    pub fn arm_delayed_poll_query() -> MysqlPollQueryTestControl {
        MysqlPollQueryTestControl {
            inner: super::changes::arm_delayed_poll_query(),
        }
    }

    pub async fn run_scenario(
        runtime: &MysqlProviderTestHarness,
        database: &str,
        store: Arc<dyn StateStore>,
        scenario: &str,
    ) -> Result<(), StateStoreError> {
        match scenario {
            "revision_sequence" | "version_encoding" => {
                let receipt = commit_keys(
                    &store,
                    &[
                        (b"change/z".as_slice(), b"z".as_slice()),
                        (b"change/a".as_slice(), b"a".as_slice()),
                        (b"change/m".as_slice(), b"m".as_slice()),
                    ],
                )
                .await?;
                let revision = u64::from_be_bytes(
                    receipt
                        .revision
                        .as_bytes()
                        .try_into()
                        .map_err(|_| scenario_error())?,
                );
                let expected = [b"change/a".as_slice(), b"change/m", b"change/z"];
                for (sequence, key_bytes) in expected.iter().enumerate() {
                    let record = read_key(&store, key_bytes)
                        .await?
                        .ok_or_else(scenario_error)?;
                    let version: [u8; 12] = record
                        .version
                        .as_bytes()
                        .try_into()
                        .map_err(|_| scenario_error())?;
                    if version[..8] != revision.to_be_bytes()
                        || version[8..]
                            != u32::try_from(sequence)
                                .map_err(|_| scenario_error())?
                                .to_be_bytes()
                    {
                        return Err(scenario_error());
                    }
                }
                let page = store
                    .poll_changes(&ChangePollRequest {
                        after: None,
                        page_size: store.limits().max_page_size,
                    })
                    .await?;
                if page.hints.len() != 3
                    || page
                        .hints
                        .iter()
                        .any(|hint| hint.revision != receipt.revision)
                    || page
                        .hints
                        .iter()
                        .map(|hint| hint.key.as_bytes())
                        .ne(expected)
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "cursor_boundaries" => {
                let identity = store.identity().await?;
                let empty = store
                    .poll_changes(&ChangePollRequest {
                        after: None,
                        page_size: 1,
                    })
                    .await?;
                if !empty.hints.is_empty() || empty.resync_required {
                    return Err(scenario_error());
                }
                let future = ChangeCursor::new(
                    identity.store_id,
                    StoreRevision::try_from(Bytes::copy_from_slice(&1_u64.to_be_bytes()))?,
                    u32::MAX,
                )?;
                if store
                    .poll_changes(&ChangePollRequest {
                        after: Some(future),
                        page_size: 1,
                    })
                    .await
                    .is_ok()
                {
                    return Err(scenario_error());
                }
                let foreign = ChangeCursor::new(Uuid::now_v7(), empty.high_watermark, u32::MAX)?;
                if store
                    .poll_changes(&ChangePollRequest {
                        after: Some(foreign),
                        page_size: 1,
                    })
                    .await
                    .is_ok()
                {
                    return Err(scenario_error());
                }
                let pool = runtime.mysql_test_pool(database)?;
                let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
                let mut connection =
                    super::client::checkout_hygienic_connection(pool, deadline).await?;
                connection
                    .exec_drop(
                        "UPDATE state_store_meta SET meta_value = ? WHERE meta_key = ?",
                        (
                            super::codec::MysqlCodec::new(store.limits().max_key_bytes)?
                                .encode_revision(2)
                                .to_vec(),
                            b"current_revision".to_vec(),
                        ),
                    )
                    .await
                    .map_err(super::error::MysqlNativeError::from)
                    .map_err(super::error::MysqlNativeError::into_public)?;
                let zero_change = ChangeCursor::new(
                    identity.store_id,
                    StoreRevision::try_from(Bytes::copy_from_slice(&2_u64.to_be_bytes()))?,
                    u32::MAX,
                )?;
                let page = store
                    .poll_changes(&ChangePollRequest {
                        after: Some(zero_change),
                        page_size: 1,
                    })
                    .await?;
                let (_, next_sequence) = page.next_cursor.decode(identity.store_id)?;
                if !page.hints.is_empty()
                    || page.high_watermark.as_bytes() != 2_u64.to_be_bytes()
                    || next_sequence != u32::MAX
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "retention_gap" => {
                let receipt = commit_keys(&store, &[(b"gap/key", b"value")]).await?;
                let revision = u64::from_be_bytes(
                    receipt
                        .revision
                        .as_bytes()
                        .try_into()
                        .map_err(|_| scenario_error())?,
                );
                let pool = runtime.mysql_test_pool(database)?;
                let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
                let mut connection =
                    super::client::checkout_hygienic_connection(pool, deadline).await?;
                connection
                    .exec_drop(
                        "UPDATE state_store_meta SET meta_value = ? WHERE meta_key = ?",
                        (
                            super::codec::MysqlCodec::new(store.limits().max_key_bytes)?
                                .encode_cursor(revision, u32::MAX)
                                .to_vec(),
                            b"change_retention_floor".to_vec(),
                        ),
                    )
                    .await
                    .map_err(super::error::MysqlNativeError::from)
                    .map_err(super::error::MysqlNativeError::into_public)?;
                let before: Option<u64> = connection
                    .query_first("SELECT COUNT(*) FROM state_store_changes")
                    .await
                    .map_err(super::error::MysqlNativeError::from)
                    .map_err(super::error::MysqlNativeError::into_public)?;
                let page = store
                    .poll_changes(&ChangePollRequest {
                        after: None,
                        page_size: 1,
                    })
                    .await?;
                let after: Option<u64> = connection
                    .query_first("SELECT COUNT(*) FROM state_store_changes")
                    .await
                    .map_err(super::error::MysqlNativeError::from)
                    .map_err(super::error::MysqlNativeError::into_public)?;
                if !page.resync_required || !page.hints.is_empty() || before != after {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "duplicate_position" => {
                commit_keys(&store, &[(b"duplicate/real", b"value")]).await?;
                super::changes::duplicate_next_poll_row();
                let error = store
                    .poll_changes(&ChangePollRequest {
                        after: None,
                        page_size: store.limits().max_page_size,
                    })
                    .await
                    .expect_err("duplicate production result row must fail closed");
                if error.kind() == StateStoreErrorKind::Corruption {
                    Ok(())
                } else {
                    Err(error)
                }
            }
            "cursor_sequence_gap" => {
                let receipt = commit_keys(&store, &[(b"cursor/real", b"value")]).await?;
                let revision = u64::from_be_bytes(
                    receipt
                        .revision
                        .as_bytes()
                        .try_into()
                        .map_err(|_| scenario_error())?,
                );
                let identity = store.identity().await?;
                let nonexistent = ChangeCursor::new(identity.store_id, receipt.revision, 1)?;
                let error = store
                    .poll_changes(&ChangePollRequest {
                        after: Some(nonexistent),
                        page_size: 1,
                    })
                    .await
                    .expect_err("nonexistent non-MAX cursor must fail closed");
                if error.kind() != StateStoreErrorKind::Corruption {
                    return Err(error);
                }

                let pool = runtime.mysql_test_pool(database)?;
                let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
                let mut connection =
                    super::client::checkout_hygienic_connection(pool, deadline).await?;
                connection
                    .exec_drop(
                        "INSERT INTO state_store_changes (revision, sequence, key_bytes)
                         VALUES (?, ?, ?)",
                        (revision, 2_u32, b"cursor/injected-gap".to_vec()),
                    )
                    .await
                    .map_err(super::error::MysqlNativeError::from)
                    .map_err(super::error::MysqlNativeError::into_public)?;
                let error = store
                    .poll_changes(&ChangePollRequest {
                        after: None,
                        page_size: store.limits().max_page_size,
                    })
                    .await
                    .expect_err("persisted same-revision sequence gap must fail closed");
                if error.kind() == StateStoreErrorKind::Corruption {
                    Ok(())
                } else {
                    Err(error)
                }
            }
            _ => Err(scenario_error()),
        }
    }
}

#[cfg(feature = "state-store-test-hooks")]
impl MysqlPollQueryTestControl {
    pub async fn wait_reached(&self) {
        self.inner.wait_reached().await;
    }
}

#[cfg(feature = "state-store-test-hooks")]
impl MysqlCommitTestApi {
    pub fn fail_next_prepare_after_reservation(rollback: MysqlPrepareRollbackFailure) {
        let mode = match rollback {
            MysqlPrepareRollbackFailure::Error => 1,
            MysqlPrepareRollbackFailure::Timeout => 2,
        };
        super::txn::fail_next_prepare_after_reservation_for_test(mode);
    }

    pub fn last_prepare_failure_connection_id() -> u64 {
        super::txn::last_prepare_failure_connection_id_for_test()
    }

    pub async fn auxiliary_statement_timeout_disposes(
        runtime: &MysqlProviderTestHarness,
        database: &str,
    ) -> Result<u64, StateStoreError> {
        super::commit::auxiliary_statement_timeout_disposes_for_test(
            runtime.mysql_test_pool(database)?,
        )
        .await
    }

    pub async fn auxiliary_native_error_rolls_back(
        runtime: &MysqlProviderTestHarness,
        database: &str,
    ) -> Result<(), StateStoreError> {
        super::commit::auxiliary_native_error_rolls_back_for_test(
            runtime.mysql_test_pool(database)?,
        )
        .await
    }

    pub fn arm_shared_post_dispatch(response_loss: bool) -> MysqlPostDispatchTestControl {
        let mode = if response_loss {
            super::commit::CommitHookMode::SharedResponseLoss
        } else {
            super::commit::CommitHookMode::SharedCancelWaiter
        };
        MysqlPostDispatchTestControl {
            inner: super::commit::arm_commit_hook(mode),
        }
    }

    pub fn arm_terminalization() -> MysqlPostDispatchTestControl {
        MysqlPostDispatchTestControl {
            inner: super::commit::arm_cleanup_hook(),
        }
    }

    pub fn arm_terminalization_query() -> MysqlPostDispatchTestControl {
        MysqlPostDispatchTestControl {
            inner: super::commit::arm_terminalize_query_hook(),
        }
    }

    pub async fn hold_ledger_lock(
        runtime: &MysqlProviderTestHarness,
        database: &str,
        transaction_id: TransactionId,
    ) -> Result<MysqlHeldCommitLedgerLock, StateStoreError> {
        let operation = acquire_operation(runtime)?;
        let connection = super::commit::hold_ledger_lock_for_test(
            runtime.mysql_test_pool(database)?,
            transaction_id,
            tokio::time::Instant::now() + Duration::from_secs(8),
        )
        .await?;
        Ok(MysqlHeldCommitLedgerLock {
            connection: Some(connection),
            operation: Some(operation),
        })
    }

    pub async fn force_committed_ledger(
        runtime: &MysqlProviderTestHarness,
        database: &str,
        transaction_id: TransactionId,
        revision: u64,
    ) -> Result<(), StateStoreError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        let mut connection = super::client::checkout_hygienic_connection(
            runtime.mysql_test_pool(database)?,
            deadline,
        )
        .await?;
        connection
            .exec_drop(
                "UPDATE state_store_commits
                 SET state = ?, reservation_token = NULL, revision = ?, updated_at_ms = ?
                 WHERE transaction_id = ?",
                (
                    2_u8,
                    revision,
                    1_u64,
                    transaction_id.as_uuid().as_bytes().to_vec(),
                ),
            )
            .await
            .map_err(super::error::MysqlNativeError::from)
            .map_err(super::error::MysqlNativeError::into_public)
    }

    pub async fn run_scenario(
        runtime: &mut MysqlProviderTestHarness,
        database: &str,
        store: Arc<dyn StateStore>,
        scenario: &str,
    ) -> Result<(), StateStoreError> {
        let pool = runtime.mysql_test_pool(database)?;
        let codec = super::codec::MysqlCodec::new(store.limits().max_key_bytes)?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        match scenario {
            "reservation_absent" => {
                let transaction_id = TransactionId::from(Uuid::now_v7());
                let token = *Uuid::new_v4().as_bytes();
                if super::commit::reserve_commit(
                    Arc::clone(&pool),
                    &codec,
                    transaction_id,
                    token,
                    deadline,
                )
                .await?
                    != super::commit::ReservationDecision::Reserved
                    || read_ledger_row(Arc::clone(&pool), transaction_id, deadline).await?
                        != Some((1, Some(token.to_vec()), None))
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "reservation_committed" => {
                let transaction_id = TransactionId::from(Uuid::now_v7());
                insert_ledger_row(
                    Arc::clone(&pool),
                    transaction_id,
                    2,
                    None,
                    Some(41),
                    deadline,
                )
                .await?;
                match super::commit::reserve_commit(
                    pool,
                    &codec,
                    transaction_id,
                    *Uuid::new_v4().as_bytes(),
                    deadline,
                )
                .await?
                {
                    super::commit::ReservationDecision::Committed(receipt)
                        if receipt.transaction_id == transaction_id
                            && receipt.revision.as_bytes() == 41_u64.to_be_bytes() =>
                    {
                        Ok(())
                    }
                    _ => Err(scenario_error()),
                }
            }
            "reservation_not_committed" => {
                let transaction_id = TransactionId::from(Uuid::now_v7());
                insert_ledger_row(Arc::clone(&pool), transaction_id, 3, None, None, deadline)
                    .await?;
                if super::commit::reserve_commit(
                    pool,
                    &codec,
                    transaction_id,
                    *Uuid::new_v4().as_bytes(),
                    deadline,
                )
                .await
                .is_ok()
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "reservation_foreign_pending" => {
                let transaction_id = TransactionId::from(Uuid::now_v7());
                let foreign = *Uuid::new_v4().as_bytes();
                insert_ledger_row(
                    Arc::clone(&pool),
                    transaction_id,
                    1,
                    Some(foreign),
                    None,
                    deadline,
                )
                .await?;
                if super::commit::reserve_commit(
                    Arc::clone(&pool),
                    &codec,
                    transaction_id,
                    *Uuid::new_v4().as_bytes(),
                    deadline,
                )
                .await
                .is_ok()
                    || read_ledger_row(pool, transaction_id, deadline).await?
                        != Some((1, Some(foreign.to_vec()), None))
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "reservation_reload" => {
                let transaction_id = TransactionId::from(Uuid::now_v7());
                let ours = *Uuid::new_v4().as_bytes();
                super::commit::lose_next_auxiliary_commit_response();
                if super::commit::reserve_commit(
                    Arc::clone(&pool),
                    &codec,
                    transaction_id,
                    ours,
                    deadline,
                )
                .await?
                    != super::commit::ReservationDecision::Reserved
                    || read_ledger_row(pool, transaction_id, deadline).await?
                        != Some((1, Some(ours.to_vec()), None))
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "ledger_corruption" => {
                let malformed = TransactionId::from(Uuid::now_v7());
                insert_ledger_row(
                    Arc::clone(&pool),
                    malformed,
                    2,
                    Some(*Uuid::new_v4().as_bytes()),
                    Some(1),
                    deadline,
                )
                .await?;
                if store.resolve_commit(&malformed).await.is_ok() {
                    return Err(scenario_error());
                }
                let terminal = TransactionId::from(Uuid::now_v7());
                insert_ledger_row(Arc::clone(&pool), terminal, 2, None, Some(9), deadline).await?;
                super::commit::terminalize_undispatched(
                    Arc::clone(&pool),
                    &codec,
                    terminal,
                    *Uuid::new_v4().as_bytes(),
                    deadline,
                )
                .await?;
                if read_ledger_row(pool, terminal, deadline).await? != Some((2, None, Some(9))) {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "atomic_publication" => {
                let transaction_id = TransactionId::from(Uuid::now_v7());
                let key = Key::try_from(Bytes::from_static(b"atomic/publication"))?;
                let mut writer = store
                    .begin_write(transaction_id, "atomic publication")
                    .await?;
                writer
                    .put(
                        key.clone(),
                        Value::try_from(Bytes::from_static(b"durable"))?,
                        Precondition::Any,
                    )
                    .await?;
                let receipt = committed(writer.commit().await)?;
                let revision = u64::from_be_bytes(
                    receipt
                        .revision
                        .as_bytes()
                        .try_into()
                        .map_err(|_| scenario_error())?,
                );
                let mut connection =
                    super::client::checkout_hygienic_connection(pool, deadline).await?;
                let row: Option<(u64, u64, u64, u8, Option<u64>)> = connection
                    .exec_first(
                        "SELECT
                           (SELECT COUNT(*) FROM state_store_kv WHERE key_bytes = ?),
                           (SELECT COUNT(*) FROM state_store_changes
                            WHERE revision = ? AND key_bytes = ?),
                           (SELECT CAST(CONV(HEX(meta_value), 16, 10) AS UNSIGNED)
                            FROM state_store_meta WHERE meta_key = ?),
                           state, revision
                         FROM state_store_commits WHERE transaction_id = ?",
                        (
                            key.as_bytes().to_vec(),
                            revision,
                            key.as_bytes().to_vec(),
                            b"current_revision".to_vec(),
                            transaction_id.as_uuid().as_bytes().to_vec(),
                        ),
                    )
                    .await
                    .map_err(super::error::MysqlNativeError::from)
                    .map_err(super::error::MysqlNativeError::into_public)?;
                if row != Some((1, 1, revision, 2, Some(revision))) {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "dispatch_error_unknown" => {
                let control =
                    super::commit::arm_commit_hook(super::commit::CommitHookMode::RawDriverError);
                let transaction_id = TransactionId::from(Uuid::now_v7());
                let mut writer = store
                    .begin_write(transaction_id, "raw commit error")
                    .await?;
                writer
                    .put(
                        Key::try_from(Bytes::from_static(b"dispatch/driver-error"))?,
                        Value::try_from(Bytes::from_static(b"must-be-unknown"))?,
                        Precondition::Any,
                    )
                    .await?;
                let waiter = tokio::spawn(async move { writer.commit().await });
                control.wait_reached().await;
                let connection_id = control.connection_id();
                if connection_id == 0 {
                    control.release();
                    return Err(scenario_error());
                }
                let mut killer =
                    super::client::checkout_hygienic_connection(Arc::clone(&pool), deadline)
                        .await?;
                killer
                    .query_drop(format!("KILL CONNECTION {connection_id}"))
                    .await
                    .map_err(super::error::MysqlNativeError::from)
                    .map_err(super::error::MysqlNativeError::into_public)?;
                control.release();
                let outcome = waiter.await.map_err(|_| scenario_error())?;
                if !matches!(outcome, CommitOutcome::CommitUnknown(_))
                    || !control.driver_error_observed()
                    || !matches!(
                        read_ledger_row(pool, transaction_id, deadline).await?,
                        Some((1, Some(_), None))
                    )
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "response_loss" => {
                let control =
                    super::commit::arm_commit_hook(super::commit::CommitHookMode::ResponseLoss);
                let transaction_id = TransactionId::from(Uuid::now_v7());
                let key = Key::try_from(Bytes::copy_from_slice(scenario.as_bytes()))?;
                let mut writer = store.begin_write(transaction_id, "response loss").await?;
                writer
                    .put(
                        key.clone(),
                        Value::try_from(Bytes::from_static(b"committed"))?,
                        Precondition::Any,
                    )
                    .await?;
                if !matches!(writer.commit().await, CommitOutcome::CommitUnknown(_)) {
                    return Err(scenario_error());
                }
                control.wait_reached().await;
                if !matches!(
                    store.resolve_commit(&transaction_id).await?,
                    CommitResolution::Committed(_)
                ) || read_key(&store, key.as_bytes()).await?.is_none()
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "reservation_deadline" => {
                super::commit::delay_next_reservation();
                let transaction_id = TransactionId::from(Uuid::now_v7());
                let mut writer = store
                    .begin_write(transaction_id, "reservation deadline")
                    .await?;
                writer
                    .put(
                        Key::try_from(Bytes::from_static(b"deadline/reservation"))?,
                        Value::try_from(Bytes::from_static(b"must-not-commit"))?,
                        Precondition::Any,
                    )
                    .await?;
                let outcome = writer.commit().await;
                let inspection_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
                if matches!(outcome, CommitOutcome::Committed(_))
                    || read_ledger_row(Arc::clone(&pool), transaction_id, inspection_deadline)
                        .await?
                        != Some((3, None, None))
                    || store.resolve_commit(&transaction_id).await?
                        != CommitResolution::NotCommitted
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "dispatch_deadline" => {
                let control = super::commit::arm_commit_hook(
                    super::commit::CommitHookMode::DeadlineAfterSuccess,
                );
                let transaction_id = TransactionId::from(Uuid::now_v7());
                let mut writer = store
                    .begin_write(transaction_id, "dispatch deadline")
                    .await?;
                writer
                    .put(
                        Key::try_from(Bytes::from_static(b"deadline/dispatch"))?,
                        Value::try_from(Bytes::from_static(b"committed"))?,
                        Precondition::Any,
                    )
                    .await?;
                if !matches!(writer.commit().await, CommitOutcome::CommitUnknown(_)) {
                    return Err(scenario_error());
                }
                control.wait_reached().await;
                if !matches!(
                    store.resolve_commit(&transaction_id).await?,
                    CommitResolution::Committed(_)
                ) {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "resolve_absent" => {
                let transaction_id = TransactionId::from(Uuid::now_v7());
                if store.resolve_commit(&transaction_id).await? != CommitResolution::NotCommitted
                    || read_ledger_row(Arc::clone(&pool), transaction_id, deadline).await?
                        != Some((3, None, None))
                    || store.resolve_commit(&transaction_id).await?
                        != CommitResolution::NotCommitted
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "resolve_reservation_race" => {
                let control = super::commit::arm_resolve_reservation_race();
                let transaction_id = TransactionId::from(Uuid::now_v7());
                let mut writer = store.begin_write(transaction_id, "resolve race").await?;
                writer
                    .put(
                        Key::try_from(Bytes::from_static(b"resolve/race"))?,
                        Value::try_from(Bytes::from_static(b"value"))?,
                        Precondition::Any,
                    )
                    .await?;
                let resolver_store = Arc::clone(&store);
                let resolver =
                    tokio::spawn(
                        async move { resolver_store.resolve_commit(&transaction_id).await },
                    );
                let commit = tokio::spawn(async move { writer.commit().await });
                control.wait_both_observed().await;
                control.release();
                let resolver = resolver.await.map_err(|_| scenario_error())?;
                let commit = commit.await.map_err(|_| scenario_error())?;
                let first = store.resolve_commit(&transaction_id).await?;
                let second = store.resolve_commit(&transaction_id).await?;
                let loser_is_explicit = match (&first, resolver, commit) {
                    (CommitResolution::Committed(_), Err(error), CommitOutcome::Committed(_)) => {
                        error.kind() == StateStoreErrorKind::Conflict
                    }
                    (
                        CommitResolution::NotCommitted,
                        Ok(CommitResolution::NotCommitted),
                        CommitOutcome::Conflict(error),
                    ) => error.kind() == StateStoreErrorKind::Conflict,
                    _ => false,
                };
                if first != second || !loser_is_explicit {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "cleanup_own" => {
                let absent = TransactionId::from(Uuid::now_v7());
                let token = *Uuid::new_v4().as_bytes();
                super::commit::terminalize_undispatched(
                    Arc::clone(&pool),
                    &codec,
                    absent,
                    token,
                    deadline,
                )
                .await?;
                let pending = TransactionId::from(Uuid::now_v7());
                insert_ledger_row(Arc::clone(&pool), pending, 1, Some(token), None, deadline)
                    .await?;
                super::commit::terminalize_undispatched(
                    Arc::clone(&pool),
                    &codec,
                    pending,
                    token,
                    deadline,
                )
                .await?;
                if read_ledger_row(Arc::clone(&pool), absent, deadline).await?
                    != Some((3, None, None))
                    || read_ledger_row(pool, pending, deadline).await? != Some((3, None, None))
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "cleanup_foreign" => {
                let ours = *Uuid::new_v4().as_bytes();
                let foreign = *Uuid::new_v4().as_bytes();
                let pending = TransactionId::from(Uuid::now_v7());
                let committed_id = TransactionId::from(Uuid::now_v7());
                let not_committed = TransactionId::from(Uuid::now_v7());
                insert_ledger_row(Arc::clone(&pool), pending, 1, Some(foreign), None, deadline)
                    .await?;
                insert_ledger_row(Arc::clone(&pool), committed_id, 2, None, Some(7), deadline)
                    .await?;
                insert_ledger_row(Arc::clone(&pool), not_committed, 3, None, None, deadline)
                    .await?;
                for transaction_id in [pending, committed_id, not_committed] {
                    super::commit::terminalize_undispatched(
                        Arc::clone(&pool),
                        &codec,
                        transaction_id,
                        ours,
                        deadline,
                    )
                    .await?;
                }
                if read_ledger_row(Arc::clone(&pool), pending, deadline).await?
                    != Some((1, Some(foreign.to_vec()), None))
                    || read_ledger_row(Arc::clone(&pool), committed_id, deadline).await?
                        != Some((2, None, Some(7)))
                    || read_ledger_row(pool, not_committed, deadline).await?
                        != Some((3, None, None))
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            "cleanup_guard" => {
                let guard = Key::try_from(Bytes::from_static(b"cleanup/guard"))?;
                commit_keys(&store, &[(b"cleanup/guard", b"original")]).await?;
                let stale = read_key(&store, guard.as_bytes())
                    .await?
                    .ok_or_else(scenario_error)?;
                commit_keys(&store, &[(b"cleanup/guard", b"new")]).await?;
                let transaction_id = TransactionId::from(Uuid::now_v7());
                let mut writer = store.begin_write(transaction_id, "cleanup guard").await?;
                writer
                    .put(
                        guard,
                        Value::try_from(Bytes::from_static(b"stale"))?,
                        Precondition::Version(stale.version),
                    )
                    .await?;
                let control = super::commit::arm_cleanup_hook();
                let waiter = tokio::spawn(async move { writer.commit().await });
                control.wait_reached().await;
                waiter.abort();
                if !waiter.await.is_err_and(|error| error.is_cancelled())
                    || read_ledger_row(Arc::clone(&pool), transaction_id, deadline)
                        .await?
                        .is_none()
                {
                    control.release();
                    return Err(scenario_error());
                }
                drop(store);
                let shutdown_error = runtime
                    .shutdown(Instant::now() + Duration::from_millis(100))
                    .await
                    .expect_err("cleanup operation guard must block runtime shutdown");
                if shutdown_error.kind() != StateStoreErrorKind::DeadlineExceeded {
                    control.release();
                    return Err(shutdown_error);
                }
                control.release();
                for _ in 0..100 {
                    if read_ledger_row(Arc::clone(&pool), transaction_id, deadline).await?
                        == Some((3, None, None))
                    {
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(scenario_error())
            }
            "prepare_fallback" => {
                let transaction_id = TransactionId::from(Uuid::now_v7());
                insert_ledger_row(
                    Arc::clone(&pool),
                    transaction_id,
                    2,
                    None,
                    Some(77),
                    deadline,
                )
                .await?;
                super::commit::fail_next_reservation_prepare();
                let mut writer = store
                    .begin_write(transaction_id, "prepare fallback")
                    .await?;
                writer
                    .put(
                        Key::try_from(Bytes::from_static(b"prepare/fallback"))?,
                        Value::try_from(Bytes::from_static(b"ignored"))?,
                        Precondition::Any,
                    )
                    .await?;
                match writer.commit().await {
                    CommitOutcome::Committed(receipt)
                        if receipt.transaction_id == transaction_id
                            && receipt.revision.as_bytes() == 77_u64.to_be_bytes() =>
                    {
                        Ok(())
                    }
                    _ => Err(scenario_error()),
                }
            }
            "resolution_deadline" => {
                let transaction_id = TransactionId::from(Uuid::now_v7());
                super::commit::delay_next_resolution();
                let error = store
                    .resolve_commit(&transaction_id)
                    .await
                    .expect_err("delayed resolution must exceed deadline");
                let inspection_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
                if error.kind() != StateStoreErrorKind::DeadlineExceeded
                    || read_ledger_row(Arc::clone(&pool), transaction_id, inspection_deadline)
                        .await?
                        .is_some()
                    || store.resolve_commit(&transaction_id).await?
                        != CommitResolution::NotCommitted
                {
                    return Err(scenario_error());
                }
                Ok(())
            }
            _ => Err(scenario_error()),
        }
    }
}

#[cfg(feature = "state-store-test-hooks")]
impl MysqlPostDispatchTestControl {
    pub async fn wait_dispatched(&self) {
        self.inner.wait_reached().await;
    }

    pub fn allow_provider_progress(&self) {
        self.inner.release();
    }

    pub fn connection_id(&self) -> u64 {
        self.inner.connection_id()
    }
}

#[cfg(feature = "state-store-test-hooks")]
async fn commit_keys(
    store: &Arc<dyn StateStore>,
    rows: &[(&[u8], &[u8])],
) -> Result<CommitReceipt, StateStoreError> {
    let mut writer = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "MySQL task six change test",
        )
        .await?;
    for (key_bytes, value_bytes) in rows {
        writer
            .put(
                Key::try_from(Bytes::copy_from_slice(key_bytes))?,
                Value::try_from(Bytes::copy_from_slice(value_bytes))?,
                Precondition::Any,
            )
            .await?;
    }
    committed(writer.commit().await)
}

#[cfg(feature = "state-store-test-hooks")]
fn committed(outcome: CommitOutcome) -> Result<CommitReceipt, StateStoreError> {
    match outcome {
        CommitOutcome::Committed(receipt) => Ok(receipt),
        CommitOutcome::Conflict(error)
        | CommitOutcome::TransientBeforeCommit(error)
        | CommitOutcome::DefiniteFailure(error)
        | CommitOutcome::CommitUnknown(error) => Err(error),
    }
}

#[cfg(feature = "state-store-test-hooks")]
async fn read_key(
    store: &Arc<dyn StateStore>,
    key_bytes: &[u8],
) -> Result<Option<StateRecord>, StateStoreError> {
    let mut reader = store.begin_read().await?;
    let record = reader
        .get(&Key::try_from(Bytes::copy_from_slice(key_bytes))?)
        .await?;
    reader.abort().await?;
    Ok(record)
}

#[cfg(feature = "state-store-test-hooks")]
const fn scenario_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Internal,
        "MySQL task six test scenario failed",
    )
}

#[cfg(feature = "state-store-test-hooks")]
async fn insert_ledger_row(
    pool: Arc<dyn super::client::PoolLifecycle>,
    transaction_id: TransactionId,
    state: u8,
    token: Option<[u8; 16]>,
    revision: Option<u64>,
    deadline: tokio::time::Instant,
) -> Result<(), StateStoreError> {
    let mut connection = super::client::checkout_hygienic_connection(pool, deadline).await?;
    connection
        .exec_drop(
            "INSERT INTO state_store_commits
                (transaction_id, state, reservation_token, revision, updated_at_ms)
             VALUES (?, ?, ?, ?, ?)",
            (
                transaction_id.as_uuid().as_bytes().to_vec(),
                state,
                token.map(|token| token.to_vec()),
                revision,
                1_u64,
            ),
        )
        .await
        .map_err(super::error::MysqlNativeError::from)
        .map_err(super::error::MysqlNativeError::into_public)
}

#[cfg(feature = "state-store-test-hooks")]
async fn read_ledger_row(
    pool: Arc<dyn super::client::PoolLifecycle>,
    transaction_id: TransactionId,
    deadline: tokio::time::Instant,
) -> Result<Option<(u8, Option<Vec<u8>>, Option<u64>)>, StateStoreError> {
    let mut connection = super::client::checkout_hygienic_connection(pool, deadline).await?;
    connection
        .exec_first(
            "SELECT state, reservation_token, revision
             FROM state_store_commits WHERE transaction_id = ?",
            (transaction_id.as_uuid().as_bytes().to_vec(),),
        )
        .await
        .map_err(super::error::MysqlNativeError::from)
        .map_err(super::error::MysqlNativeError::into_public)
}

impl MysqlHeldKvLock {
    pub async fn release(mut self) -> Result<(), StateStoreError> {
        self.inner
            .take()
            .ok_or_else(|| {
                StateStoreError::new(
                    StateStoreErrorKind::InvalidRequest,
                    "MySQL key lock is already released",
                )
            })?
            .release()
            .await
    }
}

#[cfg(feature = "state-store-test-hooks")]
impl MysqlHeldCommitLedgerLock {
    pub async fn release(mut self) -> Result<(), StateStoreError> {
        let connection = self.connection.take().ok_or_else(|| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "MySQL held commit ledger connection is missing",
            )
        })?;
        let result = super::commit::release_ledger_lock_for_test(
            connection,
            tokio::time::Instant::now() + Duration::from_secs(4),
        )
        .await;
        drop(self.operation.take());
        result
    }
}

impl MysqlTransactionTestApi {
    pub fn explicit_rollback_count() -> u64 {
        super::txn::explicit_rollback_count_for_test()
    }

    pub async fn insert_malformed_kv_row(
        runtime: &MysqlProviderTestHarness,
        database: &str,
        key: &[u8],
        deadline: Duration,
    ) -> Result<(), StateStoreError> {
        runtime
            .mysql_test_insert_malformed_kv_row(database, key, deadline)
            .await
    }
}

pub fn runtime_owner(
    runtime: &MysqlProviderTestHarness,
) -> Result<MysqlRuntimeOwner, StateStoreError> {
    runtime.mysql_test_owner()
}

pub fn validate_owner(runtime: &MysqlProviderTestHarness, pid: u32) -> Result<(), StateStoreError> {
    runtime.mysql_test_validate_owner(pid)
}

pub async fn prepare_pool(
    runtime: &MysqlProviderTestHarness,
    database: &str,
) -> Result<(), StateStoreError> {
    runtime.mysql_test_prepare_pool(database).await
}

pub fn pool_count(runtime: &MysqlProviderTestHarness) -> Result<usize, StateStoreError> {
    runtime.mysql_test_pool_count()
}

pub fn acquire_provider_handle(
    runtime: &MysqlProviderTestHarness,
) -> Result<MysqlTestHandle, StateStoreError> {
    runtime.mysql_test_acquire_provider_handle()
}

pub fn acquire_operation(
    runtime: &MysqlProviderTestHarness,
) -> Result<MysqlTestHandle, StateStoreError> {
    runtime.mysql_test_acquire_operation()
}

pub fn is_accepting(runtime: &MysqlProviderTestHarness) -> Result<bool, StateStoreError> {
    runtime.mysql_test_is_accepting()
}

pub fn begin_shutdown(runtime: &MysqlProviderTestHarness) -> Result<(), StateStoreError> {
    runtime.mysql_test_begin_shutdown()
}

pub async fn active_readiness(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    deadline: Duration,
) -> Result<MysqlReadinessSnapshot, StateStoreError> {
    runtime
        .mysql_test_active_readiness(database, deadline)
        .await
}

#[cfg(feature = "state-store-test-hooks")]
pub async fn delayed_active_readiness(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    deadline: Duration,
) -> Result<MysqlReadinessSnapshot, StateStoreError> {
    runtime
        .mysql_test_delayed_active_readiness(database, deadline)
        .await
}

pub async fn pollute_session(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    deadline: Duration,
) -> Result<(), StateStoreError> {
    runtime.mysql_test_pollute_session(database, deadline).await
}

pub async fn hold_connection(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    deadline: Duration,
) -> Result<MysqlHeldConnection, StateStoreError> {
    runtime.mysql_test_hold_connection(database, deadline).await
}

pub async fn restart_mysql_fixture() -> Result<(), StateStoreError> {
    let compose_project = std::env::var_os("NOVA_MYSQL_COMPOSE_PROJECT").ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL fixture compose project is missing",
        )
    })?;
    let compose_file = std::env::var_os("NOVA_MYSQL_COMPOSE_FILE").ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL fixture compose file is missing",
        )
    })?;
    let compose_env = std::env::var_os("NOVA_MYSQL_COMPOSE_ENV").ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "MySQL fixture compose environment is missing",
        )
    })?;
    tokio::task::spawn_blocking(move || {
        let restarted = std::process::Command::new("docker")
            .args(["compose", "--env-file"])
            .arg(compose_env)
            .arg("-p")
            .arg(compose_project)
            .arg("-f")
            .arg(compose_file)
            .args(["restart", "mysql"])
            .status()
            .map_err(|_| fixture_control_error())?;
        if !restarted.success() {
            return Err(fixture_control_error());
        }

        let status_script = repository_root_for_test().join("docker/mysql-state-store/status.sh");
        for _ in 0..120 {
            if std::process::Command::new(&status_script)
                .status()
                .is_ok_and(|status| status.success())
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        Err(StateStoreError::new(
            StateStoreErrorKind::DeadlineExceeded,
            "MySQL fixture did not become ready after restart",
        ))
    })
    .await
    .map_err(|_| fixture_control_error())?
}

pub fn advisory_lock_name(database: &str) -> String {
    super::identity::advisory_lock_name(database)
}

pub async fn schema_snapshot(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    deadline: Duration,
) -> Result<MysqlSchemaSnapshot, StateStoreError> {
    runtime.mysql_test_schema_snapshot(database, deadline).await
}

pub async fn apply_schema_mutation(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    mutation: MysqlSchemaMutation,
    deadline: Duration,
) -> Result<(), StateStoreError> {
    runtime
        .mysql_test_apply_schema_mutation(database, mutation, deadline)
        .await
}

pub async fn acquire_schema_advisory_lock(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    deadline: Duration,
) -> Result<MysqlHeldAdvisoryLock, StateStoreError> {
    runtime
        .mysql_test_acquire_schema_advisory_lock(database, deadline)
        .await
}

pub async fn is_schema_advisory_lock_free(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    deadline: Duration,
) -> Result<bool, StateStoreError> {
    runtime
        .mysql_test_is_schema_advisory_lock_free(database, deadline)
        .await
}

pub async fn store_readiness_snapshot(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    cluster_id: &str,
    deadline: Duration,
) -> Result<MysqlStoreReadinessSnapshot, StateStoreError> {
    runtime
        .mysql_test_store_readiness_snapshot(database, cluster_id, deadline)
        .await
}

pub async fn schema_timeout_connection_is_destroyed(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    timeout_deadline: Duration,
    checkout_deadline: Duration,
) -> Result<bool, StateStoreError> {
    runtime
        .mysql_test_schema_timeout_connection_is_destroyed(
            database,
            timeout_deadline,
            checkout_deadline,
        )
        .await
}

#[cfg(feature = "state-store-test-hooks")]
pub async fn run_sleep_until_deadline(
    runtime: &MysqlProviderTestHarness,
    database: &str,
    deadline: Duration,
) -> Result<(), StateStoreError> {
    runtime
        .mysql_test_run_sleep_until_deadline(database, deadline)
        .await
}

fn fixture_control_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "MySQL fixture control command failed",
    )
}

impl MysqlTestHandle {
    pub(crate) fn new(dropper: impl FnOnce() + Send + 'static) -> Self {
        Self {
            dropper: Some(Box::new(dropper)),
        }
    }
}

impl std::fmt::Debug for MysqlTestHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MysqlTestHandle")
    }
}

impl Drop for MysqlTestHandle {
    fn drop(&mut self) {
        if let Some(dropper) = self.dropper.take() {
            dropper();
        }
    }
}

impl std::fmt::Debug for MysqlHeldConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MysqlHeldConnection")
    }
}

impl MysqlHeldConnection {
    pub(crate) fn new(connection: MysqlPoolConnection, operation: MysqlTestHandle) -> Self {
        Self {
            connection: Some(connection),
            operation: Some(operation),
        }
    }

    #[cfg(feature = "state-store-test-hooks")]
    pub async fn connection_id(&mut self, deadline: Duration) -> Result<u64, StateStoreError> {
        let connection = self.connection.take().ok_or_else(|| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "MySQL held connection is missing",
            )
        })?;
        let result = super::client::execute_owned_with_deadline(
            connection,
            tokio::time::Instant::now() + deadline,
            |connection| Box::pin(connection.query_first("SELECT CONNECTION_ID()")),
        )
        .await;
        let (connection, connection_id) = result?;
        self.connection = Some(connection);
        connection_id
            .map_err(super::error::MysqlNativeError::into_public)?
            .ok_or_else(|| {
                StateStoreError::new(
                    StateStoreErrorKind::Corruption,
                    "MySQL held connection ID query returned no row",
                )
            })
    }
}

impl Drop for MysqlHeldConnection {
    fn drop(&mut self) {
        drop(self.connection.take());
        drop(self.operation.take());
    }
}

impl MysqlHeldAdvisoryLock {
    pub(crate) fn new(
        connection: MysqlPoolConnection,
        operation: MysqlTestHandle,
        lock_name: String,
    ) -> Self {
        Self {
            connection: Some(connection),
            operation: Some(operation),
            lock_name,
        }
    }

    pub async fn release(mut self, deadline: Duration) -> Result<(), StateStoreError> {
        let connection = self.connection.take().ok_or_else(|| {
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "MySQL advisory lock connection is missing",
            )
        })?;
        let result = super::schema::release_lock_for_test(
            connection,
            &self.lock_name,
            tokio::time::Instant::now() + deadline,
        )
        .await;
        drop(self.operation.take());
        result
    }
}

impl std::fmt::Debug for MysqlHeldAdvisoryLock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MysqlHeldAdvisoryLock")
    }
}

impl Drop for MysqlHeldAdvisoryLock {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            tokio::spawn(connection.destroy());
        }
        drop(self.operation.take());
    }
}
