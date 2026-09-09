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

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::future::BoxFuture;
use mysql_async::prelude::Queryable;
#[cfg(feature = "state-store-test-hooks")]
use std::sync::{Mutex, OnceLock};
#[cfg(feature = "state-store-test-hooks")]
use tokio::sync::{Notify, Semaphore};
use tokio::time::{Duration, Instant, timeout_at};
use uuid::Uuid;

use super::client::{MysqlPoolConnection, PoolLifecycle, checkout_hygienic_connection};
use super::codec::{DurableCommitState, MysqlCodec};
use novarocks_state_store_api::{
    CommitReceipt, CommitResolution, StateStoreError, StateStoreErrorKind, StoreRevision,
    TransactionId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeCommitPhase {
    BeforeDispatch,
    DispatchStarted,
    Terminal,
}

pub(super) struct NativeCommitResult {
    pub phase: NativeCommitPhase,
    pub connection: Option<MysqlPoolConnection>,
    pub result: Result<(), StateStoreError>,
}

pub(super) trait NativeCommitDispatcher: Send + Sync {
    fn dispatch<'a>(
        &'a self,
        connection: MysqlPoolConnection,
        deadline: Instant,
    ) -> BoxFuture<'a, NativeCommitResult>;
}

pub(super) struct MysqlNativeCommitDispatcher;

#[cfg(feature = "state-store-test-hooks")]
#[derive(Clone, Copy)]
pub(super) enum CommitHookMode {
    RawDriverError,
    ResponseLoss,
    HoldAfterSuccess,
    DeadlineAfterSuccess,
    SharedResponseLoss,
    SharedCancelWaiter,
}

#[cfg(feature = "state-store-test-hooks")]
struct CommitHook {
    mode: CommitHookMode,
    reached: Notify,
    release: Notify,
    connection_id: std::sync::atomic::AtomicU64,
    driver_error_observed: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) struct CommitHookControl {
    hook: Arc<CommitHook>,
}

#[cfg(feature = "state-store-test-hooks")]
static NEXT_COMMIT_HOOK: OnceLock<Mutex<Option<Arc<CommitHook>>>> = OnceLock::new();
#[cfg(feature = "state-store-test-hooks")]
static DELAY_NEXT_RESERVATION: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(feature = "state-store-test-hooks")]
static DELAY_NEXT_RESOLUTION: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(feature = "state-store-test-hooks")]
static FAIL_NEXT_RESERVATION_PREPARE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(feature = "state-store-test-hooks")]
static LOSE_NEXT_AUXILIARY_COMMIT_RESPONSE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(feature = "state-store-test-hooks")]
static NEXT_CLEANUP_HOOK: OnceLock<Mutex<Option<Arc<CommitHook>>>> = OnceLock::new();
#[cfg(feature = "state-store-test-hooks")]
static NEXT_TERMINALIZE_QUERY_HOOK: OnceLock<Mutex<Option<Arc<CommitHook>>>> = OnceLock::new();
#[cfg(feature = "state-store-test-hooks")]
const TERMINALIZE_QUERY_DEADLINE_LAG: Duration = Duration::from_millis(250);
#[cfg(feature = "state-store-test-hooks")]
static NEXT_RESOLVE_RESERVATION_RACE_HOOK: OnceLock<
    Mutex<Option<Arc<ResolveReservationRaceHook>>>,
> = OnceLock::new();

#[cfg(feature = "state-store-test-hooks")]
struct ResolveReservationRaceHook {
    observed: std::sync::atomic::AtomicUsize,
    both_observed: Notify,
    release: Semaphore,
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) struct ResolveReservationRaceControl {
    hook: Arc<ResolveReservationRaceHook>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ReservationDecision {
    Reserved,
    Committed(CommitReceipt),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum TerminalizeDecision {
    NotCommitted,
    Committed(CommitReceipt),
    Unresolved,
}

impl NativeCommitDispatcher for MysqlNativeCommitDispatcher {
    fn dispatch<'a>(
        &'a self,
        mut connection: MysqlPoolConnection,
        deadline: Instant,
    ) -> BoxFuture<'a, NativeCommitResult> {
        Box::pin(async move {
            #[cfg(feature = "state-store-test-hooks")]
            let hook = take_commit_hook();
            #[cfg(feature = "state-store-test-hooks")]
            if let Some(hook) = hook.as_ref()
                && matches!(hook.mode, CommitHookMode::RawDriverError)
            {
                super::client::record_statement();
                let connection_id =
                    timeout_at(deadline, connection.query_first("SELECT CONNECTION_ID()")).await;
                match connection_id {
                    Ok(Ok(Some(connection_id))) => hook
                        .connection_id
                        .store(connection_id, std::sync::atomic::Ordering::Release),
                    Ok(Ok(None)) | Ok(Err(_)) | Err(_) => {
                        return NativeCommitResult {
                            phase: NativeCommitPhase::BeforeDispatch,
                            connection: Some(connection),
                            result: Err(deadline_error()),
                        };
                    }
                }
                hook.reached.notify_one();
                if timeout_at(deadline, hook.release.notified()).await.is_err() {
                    return NativeCommitResult {
                        phase: NativeCommitPhase::BeforeDispatch,
                        connection: Some(connection),
                        result: Err(deadline_error()),
                    };
                }
            }
            #[cfg(feature = "state-store-test-hooks")]
            if let Some(hook) = hook.as_ref()
                && matches!(
                    hook.mode,
                    CommitHookMode::SharedResponseLoss | CommitHookMode::SharedCancelWaiter
                )
            {
                hook.reached.notify_one();
                if timeout_at(deadline, hook.release.notified()).await.is_err() {
                    return NativeCommitResult {
                        phase: NativeCommitPhase::BeforeDispatch,
                        connection: Some(connection),
                        result: Err(deadline_error()),
                    };
                }
            }
            let phase = NativeCommitPhase::DispatchStarted;
            super::client::record_statement();
            let result = timeout_at(deadline, connection.query_drop("COMMIT")).await;
            match result {
                Ok(Ok(())) => {
                    #[cfg(feature = "state-store-test-hooks")]
                    if let Some(hook) = hook {
                        if !matches!(
                            hook.mode,
                            CommitHookMode::RawDriverError
                                | CommitHookMode::SharedResponseLoss
                                | CommitHookMode::SharedCancelWaiter
                        ) {
                            hook.reached.notify_one();
                        }
                        match hook.mode {
                            CommitHookMode::RawDriverError => {
                                return NativeCommitResult {
                                    phase: NativeCommitPhase::Terminal,
                                    connection: Some(connection),
                                    result: Ok(()),
                                };
                            }
                            CommitHookMode::ResponseLoss | CommitHookMode::SharedResponseLoss => {}
                            CommitHookMode::HoldAfterSuccess => hook.release.notified().await,
                            CommitHookMode::DeadlineAfterSuccess => {
                                tokio::time::sleep_until(deadline + Duration::from_millis(10))
                                    .await;
                            }
                            CommitHookMode::SharedCancelWaiter => {
                                return NativeCommitResult {
                                    phase: NativeCommitPhase::Terminal,
                                    connection: Some(connection),
                                    result: Ok(()),
                                };
                            }
                        }
                        connection.destroy().await;
                        return NativeCommitResult {
                            phase,
                            connection: None,
                            result: Err(commit_unknown()),
                        };
                    }
                    NativeCommitResult {
                        phase: NativeCommitPhase::Terminal,
                        connection: Some(connection),
                        result: Ok(()),
                    }
                }
                Ok(Err(_)) => {
                    #[cfg(feature = "state-store-test-hooks")]
                    if let Some(hook) = hook.as_ref()
                        && matches!(hook.mode, CommitHookMode::RawDriverError)
                    {
                        hook.driver_error_observed
                            .store(true, std::sync::atomic::Ordering::Release);
                    }
                    connection.destroy().await;
                    NativeCommitResult {
                        phase,
                        connection: None,
                        result: Err(commit_unknown()),
                    }
                }
                Err(_) => {
                    connection.destroy().await;
                    NativeCommitResult {
                        phase,
                        connection: None,
                        result: Err(commit_unknown()),
                    }
                }
            }
        })
    }
}

pub(super) async fn reserve_commit(
    pool: Arc<dyn PoolLifecycle>,
    codec: &MysqlCodec,
    transaction_id: TransactionId,
    reservation_token: [u8; 16],
    deadline: Instant,
) -> Result<ReservationDecision, StateStoreError> {
    #[cfg(feature = "state-store-test-hooks")]
    if DELAY_NEXT_RESERVATION.swap(false, std::sync::atomic::Ordering::AcqRel) {
        tokio::time::sleep_until(deadline + Duration::from_millis(10)).await;
        return Err(deadline_error());
    }
    let mut connection = begin_serializable(pool.clone(), deadline).await?;
    let transaction_bytes = codec.encode_uuid(*transaction_id.as_uuid()).to_vec();
    let decision = async {
        #[cfg(feature = "state-store-test-hooks")]
        if FAIL_NEXT_RESERVATION_PREPARE.swap(false, std::sync::atomic::Ordering::AcqRel) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "injected MySQL reservation prepare failure",
            ));
        }
        let row =
            read_ledger_for_update(&mut connection, transaction_bytes.clone(), deadline).await?;
        match decode_ledger(codec, row)? {
            None => {
                #[cfg(feature = "state-store-test-hooks")]
                wait_at_resolve_reservation_race().await;
                execute(&mut connection, deadline, move |connection| {
                    Box::pin(connection.exec_drop(
                        "INSERT INTO state_store_commits
                            (transaction_id, state, reservation_token, revision, updated_at_ms)
                         VALUES (?, ?, ?, NULL, ?)",
                        (
                            transaction_bytes,
                            1_u8,
                            reservation_token.to_vec(),
                            now_ms(),
                        ),
                    ))
                })
                .await?;
                Ok(ReservationDecision::Reserved)
            }
            Some(DurableCommitState::Pending(token)) if token == reservation_token => {
                Ok(ReservationDecision::Reserved)
            }
            Some(DurableCommitState::Pending(_)) => Err(StateStoreError::new(
                StateStoreErrorKind::Conflict,
                "MySQL commit transaction identifier is already pending",
            )),
            Some(DurableCommitState::Committed(revision)) => Ok(ReservationDecision::Committed(
                receipt(transaction_id, revision)?,
            )),
            Some(DurableCommitState::NotCommitted) => Err(StateStoreError::new(
                StateStoreErrorKind::Conflict,
                "MySQL commit transaction identifier was already terminalized",
            )),
        }
    }
    .await;
    let decision = match decision {
        Ok(decision) => decision,
        Err(error) => {
            if error.kind() == StateStoreErrorKind::DeadlineExceeded {
                return Err(error);
            }
            return Err(dispose_active_error(connection, deadline, error).await);
        }
    };
    let dispatch = dispatch_auxiliary_commit(connection, deadline).await;
    match dispatch.result {
        Ok(()) => Ok(decision),
        Err(_) => match authoritative_reservation_reload(
            pool,
            codec,
            transaction_id,
            reservation_token,
            deadline,
        )
        .await
        {
            Ok(decision) => Ok(decision),
            Err(_) => Err(commit_unknown()),
        },
    }
}

async fn authoritative_reservation_reload(
    pool: Arc<dyn PoolLifecycle>,
    codec: &MysqlCodec,
    transaction_id: TransactionId,
    reservation_token: [u8; 16],
    deadline: Instant,
) -> Result<ReservationDecision, StateStoreError> {
    match read_ledger(
        pool,
        codec,
        codec.encode_uuid(*transaction_id.as_uuid()).to_vec(),
        deadline,
    )
    .await?
    {
        Some(DurableCommitState::Pending(token)) if token == reservation_token => {
            Ok(ReservationDecision::Reserved)
        }
        Some(DurableCommitState::Committed(revision)) => Ok(ReservationDecision::Committed(
            receipt(transaction_id, revision)?,
        )),
        _ => Err(commit_unknown()),
    }
}

pub(super) async fn terminalize_undispatched(
    pool: Arc<dyn PoolLifecycle>,
    codec: &MysqlCodec,
    transaction_id: TransactionId,
    reservation_token: [u8; 16],
    deadline: Instant,
) -> Result<TerminalizeDecision, StateStoreError> {
    #[cfg(feature = "state-store-test-hooks")]
    if let Some(hook) = take_cleanup_hook() {
        hook.reached.notify_one();
        if timeout_at(deadline, hook.release.notified()).await.is_err() {
            return Err(deadline_error());
        }
    }
    let mut connection = begin_serializable(pool, deadline).await?;
    let transaction_bytes = codec.encode_uuid(*transaction_id.as_uuid()).to_vec();
    let apply: Result<(TerminalizeDecision, bool), StateStoreError> = async {
        let row =
            read_ledger_for_update(&mut connection, transaction_bytes.clone(), deadline).await?;
        let decision = match decode_ledger(codec, row)? {
            None => {
                execute(&mut connection, deadline, move |connection| {
                    Box::pin(connection.exec_drop(
                        "INSERT INTO state_store_commits
                            (transaction_id, state, reservation_token, revision, updated_at_ms)
                         VALUES (?, ?, NULL, NULL, ?)",
                        (transaction_bytes, 3_u8, now_ms()),
                    ))
                })
                .await?;
                (TerminalizeDecision::NotCommitted, true)
            }
            Some(DurableCommitState::Pending(token)) if token == reservation_token => {
                execute(&mut connection, deadline, move |connection| {
                    Box::pin(connection.exec_drop(
                        "UPDATE state_store_commits
                         SET state = ?, reservation_token = NULL, revision = NULL, updated_at_ms = ?
                         WHERE transaction_id = ? AND state = ? AND reservation_token = ?",
                        (
                            3_u8,
                            now_ms(),
                            transaction_bytes,
                            1_u8,
                            reservation_token.to_vec(),
                        ),
                    ))
                })
                .await?;
                (TerminalizeDecision::NotCommitted, true)
            }
            Some(DurableCommitState::Pending(_)) => (TerminalizeDecision::Unresolved, false),
            Some(DurableCommitState::Committed(revision)) => (
                TerminalizeDecision::Committed(receipt(transaction_id, revision)?),
                false,
            ),
            Some(DurableCommitState::NotCommitted) => (TerminalizeDecision::NotCommitted, false),
        };
        Ok(decision)
    }
    .await;
    let (decision, mutated) = match apply {
        Ok(decision) => decision,
        Err(error) => {
            if error.kind() == StateStoreErrorKind::DeadlineExceeded {
                return Err(error);
            }
            return Err(dispose_active_error(connection, deadline, error).await);
        }
    };
    match dispatch_auxiliary_commit(connection, deadline).await.result {
        Ok(()) => Ok(decision),
        Err(_) if !mutated => Ok(decision),
        Err(error) => Err(error),
    }
}

#[cfg(feature = "state-store-test-hooks")]
fn take_cleanup_hook() -> Option<Arc<CommitHook>> {
    NEXT_CLEANUP_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
}

pub(super) async fn resolve_commit(
    pool: Arc<dyn PoolLifecycle>,
    codec: &MysqlCodec,
    transaction_id: &TransactionId,
    deadline: Instant,
) -> Result<CommitResolution, StateStoreError> {
    #[cfg(feature = "state-store-test-hooks")]
    if DELAY_NEXT_RESOLUTION.swap(false, std::sync::atomic::Ordering::AcqRel) {
        tokio::time::sleep_until(deadline + Duration::from_millis(10)).await;
        return Err(deadline_error());
    }
    let transaction_bytes = codec.encode_uuid(*transaction_id.as_uuid()).to_vec();
    if let Some(state) =
        read_ledger(pool.clone(), codec, transaction_bytes.clone(), deadline).await?
    {
        return resolution(*transaction_id, state);
    }

    let mut connection = begin_serializable(pool, deadline).await?;
    let state: Result<DurableCommitState, StateStoreError> = async {
        let row =
            read_ledger_for_update(&mut connection, transaction_bytes.clone(), deadline).await?;
        match decode_ledger(codec, row)? {
            Some(state) => Ok(state),
            None => {
                #[cfg(feature = "state-store-test-hooks")]
                wait_at_resolve_reservation_race().await;
                execute(&mut connection, deadline, move |connection| {
                    Box::pin(connection.exec_drop(
                        "INSERT INTO state_store_commits
                            (transaction_id, state, reservation_token, revision, updated_at_ms)
                         VALUES (?, ?, NULL, NULL, ?)",
                        (transaction_bytes, 3_u8, now_ms()),
                    ))
                })
                .await?;
                Ok(DurableCommitState::NotCommitted)
            }
        }
    }
    .await;
    let state = match state {
        Ok(state) => state,
        Err(error) => {
            if error.kind() == StateStoreErrorKind::DeadlineExceeded {
                return Err(error);
            }
            return Err(dispose_active_error(connection, deadline, error).await);
        }
    };
    dispatch_auxiliary_commit(connection, deadline)
        .await
        .result?;
    resolution(*transaction_id, state)
}

fn decode_ledger(
    codec: &MysqlCodec,
    row: Option<(u8, Option<Vec<u8>>, Option<u64>)>,
) -> Result<Option<DurableCommitState>, StateStoreError> {
    row.map(|(state, token, revision)| codec.decode_commit_state(state, token.as_deref(), revision))
        .transpose()
}

async fn read_ledger(
    pool: Arc<dyn PoolLifecycle>,
    codec: &MysqlCodec,
    transaction_bytes: Vec<u8>,
    deadline: Instant,
) -> Result<Option<DurableCommitState>, StateStoreError> {
    let mut connection = checkout_hygienic_connection(pool, deadline).await?;
    let row = execute(&mut connection, deadline, move |connection| {
        Box::pin(connection.exec_first(
            "SELECT state, reservation_token, revision
             FROM state_store_commits WHERE transaction_id = ?",
            (transaction_bytes,),
        ))
    })
    .await?;
    decode_ledger(codec, row)
}

async fn read_ledger_for_update(
    connection: &mut MysqlPoolConnection,
    transaction_bytes: Vec<u8>,
    deadline: Instant,
) -> Result<Option<(u8, Option<Vec<u8>>, Option<u64>)>, StateStoreError> {
    #[cfg(feature = "state-store-test-hooks")]
    let terminalize_query_hook = take_terminalize_query_hook();
    #[cfg(feature = "state-store-test-hooks")]
    if let Some(hook) = terminalize_query_hook.as_ref() {
        let connection_id: Option<u64> = execute(connection, deadline, |connection| {
            Box::pin(connection.query_first("SELECT CONNECTION_ID()"))
        })
        .await?;
        hook.connection_id.store(
            connection_id.ok_or_else(|| {
                StateStoreError::new(
                    StateStoreErrorKind::Corruption,
                    "MySQL terminalization connection ID query returned no row",
                )
            })?,
            std::sync::atomic::Ordering::Release,
        );
    }
    #[cfg(feature = "state-store-test-hooks")]
    let query_deadline = if terminalize_query_hook.is_some() {
        // Deterministically expose an outer-timeout cancellation racing the statement disposer.
        deadline + TERMINALIZE_QUERY_DEADLINE_LAG
    } else {
        deadline
    };
    #[cfg(not(feature = "state-store-test-hooks"))]
    let query_deadline = deadline;
    execute(connection, query_deadline, move |connection| {
        Box::pin(async move {
            #[cfg(feature = "state-store-test-hooks")]
            if let Some(hook) = terminalize_query_hook {
                hook.reached.notify_one();
            }
            connection
                .exec_first(
                    "SELECT state, reservation_token, revision
                     FROM state_store_commits WHERE transaction_id = ? FOR UPDATE",
                    (transaction_bytes,),
                )
                .await
        })
    })
    .await
}

async fn begin_serializable(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<MysqlPoolConnection, StateStoreError> {
    let mut connection = checkout_hygienic_connection(pool, deadline).await?;
    execute(&mut connection, deadline, |connection| {
        Box::pin(connection.query_drop("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
    })
    .await?;
    execute(&mut connection, deadline, |connection| {
        Box::pin(connection.query_drop("START TRANSACTION"))
    })
    .await?;
    Ok(connection)
}

async fn execute<T>(
    connection: &mut MysqlPoolConnection,
    deadline: Instant,
    operation: impl for<'a> FnOnce(
        &'a mut mysql_async::Conn,
    ) -> BoxFuture<'a, Result<T, mysql_async::Error>>,
) -> Result<T, StateStoreError> {
    super::client::record_statement();
    match timeout_at(deadline, operation(connection)).await {
        Ok(result) => result
            .map_err(super::error::MysqlNativeError::from)
            .map_err(super::error::MysqlNativeError::into_public),
        Err(_) => {
            connection.destroy_in_place().await;
            Err(deadline_error())
        }
    }
}

async fn dispatch_auxiliary_commit(
    mut connection: MysqlPoolConnection,
    deadline: Instant,
) -> NativeCommitResult {
    super::client::record_statement();
    match timeout_at(deadline, connection.query_drop("COMMIT")).await {
        Ok(Ok(())) => {
            #[cfg(feature = "state-store-test-hooks")]
            if LOSE_NEXT_AUXILIARY_COMMIT_RESPONSE.swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                connection.destroy().await;
                return NativeCommitResult {
                    phase: NativeCommitPhase::DispatchStarted,
                    connection: None,
                    result: Err(commit_unknown()),
                };
            }
            NativeCommitResult {
                phase: NativeCommitPhase::Terminal,
                connection: Some(connection),
                result: Ok(()),
            }
        }
        Ok(Err(_)) | Err(_) => {
            connection.destroy().await;
            NativeCommitResult {
                phase: NativeCommitPhase::DispatchStarted,
                connection: None,
                result: Err(commit_unknown()),
            }
        }
    }
}

async fn rollback_connection(
    mut connection: MysqlPoolConnection,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let cleanup_deadline = deadline.max(Instant::now() + Duration::from_secs(1));
    let result = timeout_at(cleanup_deadline, connection.query_drop("ROLLBACK")).await;
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            connection.destroy().await;
            Err(super::error::MysqlNativeError::from(error).into_public())
        }
        Err(_) => {
            connection.destroy().await;
            Err(deadline_error())
        }
    }
}

async fn dispose_active_error(
    connection: MysqlPoolConnection,
    deadline: Instant,
    error: StateStoreError,
) -> StateStoreError {
    match rollback_connection(connection, deadline).await {
        Ok(()) => error,
        Err(rollback_error) => rollback_error,
    }
}

fn resolution(
    transaction_id: TransactionId,
    state: DurableCommitState,
) -> Result<CommitResolution, StateStoreError> {
    match state {
        DurableCommitState::Pending(_) => Ok(CommitResolution::Unresolved),
        DurableCommitState::Committed(revision) => Ok(CommitResolution::Committed(receipt(
            transaction_id,
            revision,
        )?)),
        DurableCommitState::NotCommitted => Ok(CommitResolution::NotCommitted),
    }
}

fn receipt(transaction_id: TransactionId, revision: u64) -> Result<CommitReceipt, StateStoreError> {
    Ok(CommitReceipt {
        transaction_id,
        revision: StoreRevision::try_from(bytes::Bytes::copy_from_slice(&revision.to_be_bytes()))?,
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(super) fn new_reservation_token() -> [u8; 16] {
    *Uuid::new_v4().as_bytes()
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) async fn auxiliary_statement_timeout_disposes_for_test(
    pool: Arc<dyn PoolLifecycle>,
) -> Result<u64, StateStoreError> {
    let setup_deadline = Instant::now() + Duration::from_secs(4);
    let mut connection = begin_serializable(pool, setup_deadline).await?;
    let connection_id = execute(&mut connection, setup_deadline, |connection| {
        Box::pin(connection.query_first("SELECT CONNECTION_ID()"))
    })
    .await?
    .ok_or_else(corruption)?;
    let error = execute(
        &mut connection,
        Instant::now() + Duration::from_millis(100),
        |connection| Box::pin(connection.query_drop("SELECT SLEEP(10)")),
    )
    .await
    .expect_err("auxiliary statement must exceed its deadline");
    if error.kind() != StateStoreErrorKind::DeadlineExceeded {
        return Err(error);
    }
    Ok(connection_id)
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) async fn auxiliary_native_error_rolls_back_for_test(
    pool: Arc<dyn PoolLifecycle>,
) -> Result<(), StateStoreError> {
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut connection = begin_serializable(pool.clone(), deadline).await?;
    let connection_id: u64 = execute(&mut connection, deadline, |connection| {
        Box::pin(connection.query_first("SELECT CONNECTION_ID()"))
    })
    .await?
    .ok_or_else(corruption)?;
    let marker = Uuid::new_v4();
    let revision = u64::from_be_bytes(
        marker.as_bytes()[..8]
            .try_into()
            .map_err(|_| corruption())?,
    );
    let marker_bytes = marker.as_bytes().to_vec();
    execute(&mut connection, deadline, {
        let marker_bytes = marker_bytes.clone();
        move |connection| {
            Box::pin(connection.exec_drop(
                "INSERT INTO state_store_changes (revision, sequence, key_bytes)
                 VALUES (?, ?, ?)",
                (revision, 0_u32, marker_bytes),
            ))
        }
    })
    .await?;
    let error = execute(&mut connection, deadline, |connection| {
        Box::pin(connection.query_drop("THIS IS NOT VALID SQL"))
    })
    .await
    .expect_err("invalid auxiliary statement must fail");
    let error = dispose_active_error(connection, deadline, error).await;
    if error.kind() == StateStoreErrorKind::DeadlineExceeded {
        return Err(error);
    }
    let mut connection = checkout_hygienic_connection(pool, deadline).await?;
    let state: Option<(u64, u64)> = execute(&mut connection, deadline, move |connection| {
        Box::pin(connection.exec_first(
            "SELECT CONNECTION_ID(), COUNT(*)
             FROM state_store_changes WHERE key_bytes = ?",
            (marker_bytes,),
        ))
    })
    .await?;
    if state != Some((connection_id, 0)) {
        return Err(corruption());
    }
    Ok(())
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) fn arm_commit_hook(mode: CommitHookMode) -> CommitHookControl {
    let hook = Arc::new(CommitHook {
        mode,
        reached: Notify::new(),
        release: Notify::new(),
        connection_id: std::sync::atomic::AtomicU64::new(0),
        driver_error_observed: std::sync::atomic::AtomicBool::new(false),
    });
    *NEXT_COMMIT_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&hook));
    CommitHookControl { hook }
}

#[cfg(feature = "state-store-test-hooks")]
fn take_commit_hook() -> Option<Arc<CommitHook>> {
    NEXT_COMMIT_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) fn delay_next_reservation() {
    DELAY_NEXT_RESERVATION.store(true, std::sync::atomic::Ordering::Release);
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) fn delay_next_resolution() {
    DELAY_NEXT_RESOLUTION.store(true, std::sync::atomic::Ordering::Release);
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) fn fail_next_reservation_prepare() {
    FAIL_NEXT_RESERVATION_PREPARE.store(true, std::sync::atomic::Ordering::Release);
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) fn lose_next_auxiliary_commit_response() {
    LOSE_NEXT_AUXILIARY_COMMIT_RESPONSE.store(true, std::sync::atomic::Ordering::Release);
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) fn arm_cleanup_hook() -> CommitHookControl {
    let hook = Arc::new(CommitHook {
        mode: CommitHookMode::HoldAfterSuccess,
        reached: Notify::new(),
        release: Notify::new(),
        connection_id: std::sync::atomic::AtomicU64::new(0),
        driver_error_observed: std::sync::atomic::AtomicBool::new(false),
    });
    *NEXT_CLEANUP_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&hook));
    CommitHookControl { hook }
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) fn arm_terminalize_query_hook() -> CommitHookControl {
    let hook = Arc::new(CommitHook {
        mode: CommitHookMode::HoldAfterSuccess,
        reached: Notify::new(),
        release: Notify::new(),
        connection_id: std::sync::atomic::AtomicU64::new(0),
        driver_error_observed: std::sync::atomic::AtomicBool::new(false),
    });
    *NEXT_TERMINALIZE_QUERY_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&hook));
    CommitHookControl { hook }
}

#[cfg(feature = "state-store-test-hooks")]
fn take_terminalize_query_hook() -> Option<Arc<CommitHook>> {
    NEXT_TERMINALIZE_QUERY_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) async fn hold_ledger_lock_for_test(
    pool: Arc<dyn PoolLifecycle>,
    transaction_id: TransactionId,
    deadline: Instant,
) -> Result<MysqlPoolConnection, StateStoreError> {
    let mut connection = begin_serializable(pool, deadline).await?;
    let transaction_bytes = transaction_id.as_uuid().as_bytes().to_vec();
    let row: Option<u8> = execute(&mut connection, deadline, move |connection| {
        Box::pin(connection.exec_first(
            "SELECT state FROM state_store_commits
             WHERE transaction_id = ? FOR UPDATE",
            (transaction_bytes,),
        ))
    })
    .await?;
    row.ok_or_else(|| {
        StateStoreError::new(
            StateStoreErrorKind::Internal,
            "MySQL test ledger row is missing before lock",
        )
    })?;
    Ok(connection)
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) async fn release_ledger_lock_for_test(
    connection: MysqlPoolConnection,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    rollback_connection(connection, deadline).await
}

#[cfg(feature = "state-store-test-hooks")]
pub(super) fn arm_resolve_reservation_race() -> ResolveReservationRaceControl {
    let hook = Arc::new(ResolveReservationRaceHook {
        observed: std::sync::atomic::AtomicUsize::new(0),
        both_observed: Notify::new(),
        release: Semaphore::new(0),
    });
    *NEXT_RESOLVE_RESERVATION_RACE_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&hook));
    ResolveReservationRaceControl { hook }
}

#[cfg(feature = "state-store-test-hooks")]
async fn wait_at_resolve_reservation_race() {
    let hook = NEXT_RESOLVE_RESERVATION_RACE_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let Some(hook) = hook else {
        return;
    };
    if hook
        .observed
        .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
        == 1
    {
        hook.both_observed.notify_one();
    }
    let permit = hook
        .release
        .acquire()
        .await
        .expect("resolve/reservation race semaphore must stay open");
    permit.forget();
}

#[cfg(feature = "state-store-test-hooks")]
impl ResolveReservationRaceControl {
    pub(super) async fn wait_both_observed(&self) {
        while self
            .hook
            .observed
            .load(std::sync::atomic::Ordering::Acquire)
            < 2
        {
            self.hook.both_observed.notified().await;
        }
    }

    pub(super) fn release(&self) {
        self.hook.release.add_permits(2);
    }
}

#[cfg(feature = "state-store-test-hooks")]
impl Drop for ResolveReservationRaceControl {
    fn drop(&mut self) {
        let mut armed = NEXT_RESOLVE_RESERVATION_RACE_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if armed
            .as_ref()
            .is_some_and(|armed| Arc::ptr_eq(armed, &self.hook))
        {
            *armed = None;
        }
    }
}

#[cfg(feature = "state-store-test-hooks")]
impl CommitHookControl {
    pub(super) async fn wait_reached(&self) {
        self.hook.reached.notified().await;
    }

    pub(super) fn release(&self) {
        self.hook.release.notify_one();
    }

    pub(super) fn connection_id(&self) -> u64 {
        self.hook
            .connection_id
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(super) fn driver_error_observed(&self) -> bool {
        self.hook
            .driver_error_observed
            .load(std::sync::atomic::Ordering::Acquire)
    }
}

#[cfg(feature = "state-store-test-hooks")]
impl Drop for CommitHookControl {
    fn drop(&mut self) {
        for slot in [
            &NEXT_COMMIT_HOOK,
            &NEXT_CLEANUP_HOOK,
            &NEXT_TERMINALIZE_QUERY_HOOK,
        ] {
            let mut armed = slot
                .get_or_init(|| Mutex::new(None))
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if armed
                .as_ref()
                .is_some_and(|armed| Arc::ptr_eq(armed, &self.hook))
            {
                *armed = None;
            }
        }
        self.hook.release.notify_one();
    }
}

const fn commit_unknown() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "MySQL native commit outcome is unknown",
    )
}

const fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "MySQL durable commit operation exceeded its deadline",
    )
}

#[cfg(feature = "state-store-test-hooks")]
const fn corruption() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "MySQL durable commit state is malformed",
    )
}
