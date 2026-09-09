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

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::future::BoxFuture;
use mysql_async::prelude::Queryable;
#[cfg(feature = "state-store-test-hooks")]
use std::sync::OnceLock;
#[cfg(feature = "state-store-test-hooks")]
use tokio::sync::Notify;
use tokio::time::{Duration, Instant, timeout_at};
#[cfg(feature = "state-store-test-hooks")]
use uuid::Uuid;

use super::client::{MysqlPoolConnection, PoolLifecycle, checkout_hygienic_connection};
use super::codec::{
    COMMIT_STATE_NOT_COMMITTED, COMMIT_STATE_PENDING, DurableCommitState, MysqlCodec,
    encode_attempt_id,
};
use super::runtime::MysqlOperationLease;
use novarocks_state_store_api::{
    AttemptId, AttemptOutcome, CommitReceipt, InDoubtAdjudicator, StateStoreError,
    StateStoreErrorKind, StoreRevision,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ReservationDecision {
    Reserved,
    Committed(CommitReceipt),
}

/// What closing an attempt's reservation proved.
///
/// There is deliberately no "unresolved" arm. That arm existed to describe a
/// pending row bearing somebody else's reservation token; under an issued,
/// instance-scoped attempt identity no other writer can reach this row, so a
/// pending row is always this attempt's own and is always closable. A cleanup
/// that could not read stays an `Err`, which is a different thing and is
/// classified as an unknown commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum TerminalizeDecision {
    NotCommitted,
    Committed(CommitReceipt),
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

/// Records that this attempt is about to write, on its own committed
/// connection.
///
/// The ledger row is the attempt's evidence: it exists from here until the
/// attempt's terminal outcome is published and the evidence is released.
/// Because an [`AttemptId`] is minted by one open instance and authorises
/// exactly one transaction body, this row can only ever be written by this
/// attempt -- there is no second writer to fence out, which is why the
/// reservation carries no ownership token beyond the primary key itself.
pub(super) async fn reserve_commit(
    pool: Arc<dyn PoolLifecycle>,
    codec: &MysqlCodec,
    attempt: AttemptId,
    deadline: Instant,
) -> Result<ReservationDecision, StateStoreError> {
    #[cfg(feature = "state-store-test-hooks")]
    if DELAY_NEXT_RESERVATION.swap(false, std::sync::atomic::Ordering::AcqRel) {
        tokio::time::sleep_until(deadline + Duration::from_millis(10)).await;
        return Err(deadline_error());
    }
    let mut connection = begin_serializable(pool.clone(), deadline).await?;
    let attempt_bytes = encode_attempt_id(attempt);
    let decision = async {
        #[cfg(feature = "state-store-test-hooks")]
        if FAIL_NEXT_RESERVATION_PREPARE.swap(false, std::sync::atomic::Ordering::AcqRel) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "injected MySQL reservation prepare failure",
            ));
        }
        let row = read_ledger_for_update(&mut connection, attempt_bytes.clone(), deadline).await?;
        match decode_ledger(codec, row)? {
            None => {
                execute(&mut connection, deadline, move |connection| {
                    Box::pin(connection.exec_drop(
                        "INSERT INTO state_store_commits
                            (attempt_id, state, revision, updated_at_ms)
                         VALUES (?, ?, NULL, ?)",
                        (attempt_bytes, COMMIT_STATE_PENDING, now_ms()),
                    ))
                })
                .await?;
                Ok(ReservationDecision::Reserved)
            }
            // A row already under this attempt can only be one this attempt
            // wrote, so a repeated reservation is the same reservation.
            Some(DurableCommitState::Pending) => Ok(ReservationDecision::Reserved),
            Some(DurableCommitState::Committed(revision)) => {
                Ok(ReservationDecision::Committed(receipt(attempt, revision)?))
            }
            Some(DurableCommitState::NotCommitted) => Err(StateStoreError::new(
                StateStoreErrorKind::Conflict,
                "MySQL commit attempt was already terminalized",
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
        Err(_) => match authoritative_reservation_reload(pool, codec, attempt, deadline).await {
            Ok(decision) => Ok(decision),
            Err(_) => Err(commit_unknown()),
        },
    }
}

async fn authoritative_reservation_reload(
    pool: Arc<dyn PoolLifecycle>,
    codec: &MysqlCodec,
    attempt: AttemptId,
    deadline: Instant,
) -> Result<ReservationDecision, StateStoreError> {
    match read_ledger(pool, codec, encode_attempt_id(attempt), deadline).await? {
        Some(DurableCommitState::Pending) => Ok(ReservationDecision::Reserved),
        Some(DurableCommitState::Committed(revision)) => {
            Ok(ReservationDecision::Committed(receipt(attempt, revision)?))
        }
        _ => Err(commit_unknown()),
    }
}

/// Closes an attempt's reservation once its data transaction provably never
/// dispatched.
///
/// An absent row means the reservation itself never became durable, so the
/// attempt left no trace at all; that is a proven denial and, deliberately,
/// nothing is written for it. The old code inserted a tombstone here so a
/// later caller could ask about an arbitrary identifier; no such caller can
/// exist now, and writing a row only to delete it again would grow the ledger
/// for nobody's benefit.
pub(super) async fn terminalize_undispatched(
    pool: Arc<dyn PoolLifecycle>,
    codec: &MysqlCodec,
    attempt: AttemptId,
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
    let attempt_bytes = encode_attempt_id(attempt);
    let apply: Result<(TerminalizeDecision, bool), StateStoreError> = async {
        let row = read_ledger_for_update(&mut connection, attempt_bytes.clone(), deadline).await?;
        let decision = match decode_ledger(codec, row)? {
            None => (TerminalizeDecision::NotCommitted, false),
            Some(DurableCommitState::Pending) => {
                execute(&mut connection, deadline, move |connection| {
                    Box::pin(connection.exec_drop(
                        "UPDATE state_store_commits
                         SET state = ?, revision = NULL, updated_at_ms = ?
                         WHERE attempt_id = ? AND state = ?",
                        (
                            COMMIT_STATE_NOT_COMMITTED,
                            now_ms(),
                            attempt_bytes,
                            COMMIT_STATE_PENDING,
                        ),
                    ))
                })
                .await?;
                (TerminalizeDecision::NotCommitted, true)
            }
            Some(DurableCommitState::Committed(revision)) => (
                TerminalizeDecision::Committed(receipt(attempt, revision)?),
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

/// Deletes one attempt's evidence.
///
/// This is the only statement that shrinks the ledger. Without it the commit
/// table is append-only for the life of the database, which is exactly what
/// the previous design left behind.
async fn delete_evidence(
    pool: Arc<dyn PoolLifecycle>,
    attempt: AttemptId,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let attempt_bytes = encode_attempt_id(attempt);
    let mut connection = checkout_hygienic_connection(pool, deadline).await?;
    execute(&mut connection, deadline, move |connection| {
        Box::pin(connection.exec_drop(
            "DELETE FROM state_store_commits WHERE attempt_id = ?",
            (attempt_bytes,),
        ))
    })
    .await
}

#[cfg(feature = "state-store-test-hooks")]
fn take_cleanup_hook() -> Option<Arc<CommitHook>> {
    NEXT_CLEANUP_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
}

pub(super) fn decode_ledger(
    codec: &MysqlCodec,
    row: Option<(u8, Option<u64>)>,
) -> Result<Option<DurableCommitState>, StateStoreError> {
    row.map(|(state, revision)| codec.decode_commit_state(state, revision))
        .transpose()
}

async fn read_ledger(
    pool: Arc<dyn PoolLifecycle>,
    codec: &MysqlCodec,
    attempt_bytes: Vec<u8>,
    deadline: Instant,
) -> Result<Option<DurableCommitState>, StateStoreError> {
    let mut connection = checkout_hygienic_connection(pool, deadline).await?;
    let row = execute(&mut connection, deadline, move |connection| {
        Box::pin(connection.exec_first(
            "SELECT state, revision
             FROM state_store_commits WHERE attempt_id = ?",
            (attempt_bytes,),
        ))
    })
    .await?;
    decode_ledger(codec, row)
}

async fn read_ledger_for_update(
    connection: &mut MysqlPoolConnection,
    attempt_bytes: Vec<u8>,
    deadline: Instant,
) -> Result<Option<(u8, Option<u64>)>, StateStoreError> {
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
                    "SELECT state, revision
                     FROM state_store_commits WHERE attempt_id = ? FOR UPDATE",
                    (attempt_bytes,),
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

pub(super) fn receipt(attempt: AttemptId, revision: u64) -> Result<CommitReceipt, StateStoreError> {
    Ok(CommitReceipt {
        attempt,
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
    let marker_bytes = marker.as_bytes().to_vec();
    execute(&mut connection, deadline, {
        let marker_bytes = marker_bytes.clone();
        move |connection| {
            Box::pin(connection.exec_drop(
                "INSERT INTO state_store_kv (key_bytes, value_bytes, version_bytes)
                 VALUES (?, ?, ?)",
                (marker_bytes, b"rollback-probe".to_vec(), vec![0_u8; 12]),
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
             FROM state_store_kv WHERE key_bytes = ?",
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
    attempt_bytes: Vec<u8>,
    deadline: Instant,
) -> Result<MysqlPoolConnection, StateStoreError> {
    let mut connection = begin_serializable(pool, deadline).await?;
    let row: Option<u8> = execute(&mut connection, deadline, move |connection| {
        Box::pin(connection.exec_first(
            "SELECT state FROM state_store_commits
             WHERE attempt_id = ? FOR UPDATE",
            (attempt_bytes,),
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

/// Bound on how many evidence rows may wait for a retried delete before the
/// oldest is abandoned. A dropped entry leaks one row; it never affects a
/// verdict, because a verdict is published before its evidence is released.
const MAX_DEFERRED_EVIDENCE_RELEASES: usize = 256;

/// MySQL's in-doubt callback and evidence owner.
///
/// The ledger row for an attempt *is* the evidence. This type is the only
/// thing that reads it on behalf of the supervisor and the only thing that
/// deletes it.
///
/// # Why absence is never a denial
///
/// A row is missing when the reservation never became durable, when the
/// evidence was already released, or when this instance simply cannot read
/// right now. None of those prove the write did not land, so every one of them
/// answers [`AttemptOutcome::Unresolved`]. Only a durable `Committed` or
/// `NotCommitted` row is proof. The provider used to answer `NotCommitted` for
/// an unknown identifier -- and write a tombstone to make that answer
/// self-fulfilling -- which is precisely the lie the attempt contract exists
/// to prevent.
pub(super) struct MysqlEvidence {
    pool: Arc<dyn PoolLifecycle>,
    operations: MysqlOperationLease,
    codec: MysqlCodec,
    deadline_budget: Duration,
    /// Releases that failed, plus terminals decided by adjudication whose
    /// evidence the supervisor will never ask about again. Drained on the next
    /// evidence operation, which is strictly after the decision that queued
    /// them was published.
    deferred: Mutex<std::collections::VecDeque<AttemptId>>,
}

impl MysqlEvidence {
    pub(super) fn new(
        pool: Arc<dyn PoolLifecycle>,
        operations: MysqlOperationLease,
        codec: MysqlCodec,
        deadline_budget: Duration,
    ) -> Self {
        Self {
            pool,
            operations,
            codec,
            deadline_budget,
            deferred: Mutex::new(std::collections::VecDeque::new()),
        }
    }

    fn deadline(&self) -> Instant {
        Instant::now() + self.deadline_budget
    }

    fn defer(&self, attempt: AttemptId) {
        let mut deferred = self
            .deferred
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if deferred.len() >= MAX_DEFERRED_EVIDENCE_RELEASES {
            let abandoned = deferred.pop_front();
            tracing::warn!(
                provider = "mysql",
                abandoned = abandoned.map(|attempt| attempt.to_string()),
                "MySQL evidence release backlog is full; one commit ledger row is left behind"
            );
        }
        deferred.push_back(attempt);
    }

    fn take_deferred(&self) -> Option<AttemptId> {
        self.deferred
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop_front()
    }

    /// Best-effort retry of releases that could not be completed earlier.
    ///
    /// Stops at the first failure and re-queues, so a store whose database is
    /// unreachable does not spin.
    async fn drain_deferred(&self) {
        while let Some(attempt) = self.take_deferred() {
            if delete_evidence(Arc::clone(&self.pool), attempt, self.deadline())
                .await
                .is_err()
            {
                self.defer(attempt);
                return;
            }
        }
    }
}

#[async_trait::async_trait]
impl InDoubtAdjudicator for MysqlEvidence {
    async fn adjudicate(&self, attempt: AttemptId) -> Result<AttemptOutcome, StateStoreError> {
        let Ok(_operation) = self.operations.acquire() else {
            // A stopping runtime cannot read, and cannot therefore deny.
            return Ok(AttemptOutcome::Unresolved);
        };
        self.drain_deferred().await;
        let state = match read_ledger(
            Arc::clone(&self.pool),
            &self.codec,
            encode_attempt_id(attempt),
            self.deadline(),
        )
        .await
        {
            Ok(state) => state,
            Err(error) => {
                tracing::debug!(
                    provider = "mysql",
                    %error,
                    "MySQL could not read commit evidence; the attempt stays unresolved"
                );
                return Ok(AttemptOutcome::Unresolved);
            }
        };
        match state {
            // Released, or never reserved. Either way this store can prove
            // nothing about the attempt.
            None => Ok(AttemptOutcome::Unresolved),
            // The data transaction may still be in flight on another
            // connection.
            Some(DurableCommitState::Pending) => Ok(AttemptOutcome::Unresolved),
            Some(DurableCommitState::Committed(revision)) => {
                let receipt = receipt(attempt, revision)?;
                self.defer(attempt);
                Ok(AttemptOutcome::Committed(receipt))
            }
            Some(DurableCommitState::NotCommitted) => {
                self.defer(attempt);
                Ok(AttemptOutcome::NotCommitted)
            }
        }
    }

    async fn release_evidence(&self, attempt: AttemptId) -> Result<(), StateStoreError> {
        let _operation = self.operations.acquire()?;
        self.drain_deferred().await;
        delete_evidence(Arc::clone(&self.pool), attempt, self.deadline()).await
    }
}

/// Releases an attempt's evidence from the path that witnessed its terminal.
///
/// The supervisor only drives [`InDoubtAdjudicator::release_evidence`] for
/// attempts that were abandoned mid-flight, so a provider that waits for it
/// grows its ledger without bound. Every witnessed terminal releases here
/// instead, immediately after the outcome is published.
pub(super) async fn release_witnessed_evidence(evidence: &MysqlEvidence, attempt: AttemptId) {
    if let Err(error) = evidence.release_evidence(attempt).await {
        // The verdict is already published, so this is garbage rather than a
        // correctness problem: retry it on the next evidence operation.
        tracing::warn!(
            provider = "mysql",
            %error,
            "MySQL could not release commit evidence; the row is queued for retry"
        );
        evidence.defer(attempt);
    }
}
