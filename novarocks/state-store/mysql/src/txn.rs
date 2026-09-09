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

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
#[cfg(feature = "state-store-test-hooks")]
use std::sync::atomic::AtomicU8;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "state-store-test-hooks")]
use std::sync::{Mutex, OnceLock};
use std::time::Instant as StdInstant;

use async_trait::async_trait;
use futures::future::BoxFuture;
use mysql_async::{Conn, IsolationLevel, Transaction, TxOpts, prelude::Queryable};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, timeout_at};

use super::budget::TransactionBudget;
use super::client::{
    MysqlPoolConnection, PoolLifecycle, checkout_hygienic_connection, execute_owned_with_deadline,
};
use super::codec::{
    COMMIT_STATE_COMMITTED, COMMIT_STATE_PENDING, DurableCommitState, MysqlCodec, encode_attempt_id,
};
use super::commit::{MysqlEvidence, release_witnessed_evidence};
use super::error::{MysqlNativeError, MysqlReadStatementError, MysqlTransactionDisposition};
use super::metrics::{StateStoreMetrics, StateStoreOperation, StateStoreOutcome};
use super::range::{decode_record, read_range_page};
use super::runtime::MysqlRuntimeGuard;
use novarocks_state_store_api::{
    AttemptId, AttemptOutcome, CommitOutcome, CommitReceipt, ContinuationToken, Direction, Key,
    Precondition, RangePage, RangeRequest, ReadTransaction, StateRecord, StateStoreError,
    StateStoreErrorKind, StateStoreLimits, StoreRevision, Value, VersionToken, WriteAttempt,
    WriteTransaction,
};

const PROVISIONAL_VERSION_TAG: &[u8] = b"mysql-provisional-v1\0";
static EXPLICIT_ROLLBACKS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "state-store-test-hooks")]
static LAST_WRITE_ACTOR_CONNECTION_ID: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "state-store-test-hooks")]
static PREPARE_FAILURE_ROLLBACK_MODE: AtomicU8 = AtomicU8::new(0);
#[cfg(feature = "state-store-test-hooks")]
static NEXT_ROLLBACK_FAILURE_MODE: AtomicU8 = AtomicU8::new(0);
#[cfg(feature = "state-store-test-hooks")]
static LAST_PREPARE_FAILURE_CONNECTION_ID: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "state-store-test-hooks")]
static LAST_TOUCHED_LOCK_ORDER: OnceLock<Mutex<Vec<Vec<u8>>>> = OnceLock::new();

pub(super) struct OwnedMysqlTransaction {
    connection: Option<MysqlPoolConnection>,
    operation: Option<MysqlRuntimeGuard>,
    deadline: Instant,
    active: bool,
    statement_in_flight: bool,
}

enum NativeDataCommitOutcome {
    Committed,
    BeforeDispatchFailure(StateStoreError),
    Unknown(StateStoreError),
}

pub(super) struct MysqlReadTransaction {
    commands: Option<mpsc::Sender<ReadCommand>>,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
    issued_continuations: HashSet<ContinuationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PointObservation {
    Absent,
    Present(VersionToken),
}

#[derive(Clone, Debug)]
enum Mutation {
    Put {
        value: Value,
        precondition: Precondition,
        provisional_version: VersionToken,
    },
    Delete {
        precondition: Precondition,
    },
}

impl Mutation {
    fn precondition(&self) -> &Precondition {
        match self {
            Self::Put { precondition, .. } | Self::Delete { precondition } => precondition,
        }
    }
}

pub(super) struct MysqlWriteTransaction {
    commands: Option<mpsc::Sender<WriteCommand>>,
    attempt: AttemptId,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
    issued_continuations: HashSet<ContinuationToken>,
}

struct MysqlWriteActorState {
    pool: Arc<dyn PoolLifecycle>,
    transaction: Option<OwnedMysqlTransaction>,
    codec: MysqlCodec,
    limits: StateStoreLimits,
    /// The one attempt this transaction body was authorised by. The actor owns
    /// it because the actor -- not the caller's future -- is what learns the
    /// commit's outcome, and a terminal has to be published by whoever
    /// witnessed it even if the caller has gone away.
    attempt: WriteAttempt,
    evidence: Arc<MysqlEvidence>,
    base_revision: u64,
    point_observations: BTreeMap<Key, PointObservation>,
    range_observed: bool,
    mutations: Vec<(Key, Mutation)>,
    overlay: BTreeMap<Key, Mutation>,
    budget: TransactionBudget,
    range_frozen: bool,
    issued_continuations: HashSet<ContinuationToken>,
}

enum WriteCommand {
    Get {
        key: Key,
        response: oneshot::Sender<Result<Option<StateRecord>, StateStoreError>>,
    },
    Range {
        request: RangeRequest,
        response: oneshot::Sender<Result<RangePage, StateStoreError>>,
    },
    Put {
        key: Key,
        value: Value,
        precondition: Precondition,
        response: oneshot::Sender<Result<usize, StateStoreError>>,
    },
    Delete {
        key: Key,
        precondition: Precondition,
        response: oneshot::Sender<Result<usize, StateStoreError>>,
    },
    Abort {
        response: oneshot::Sender<Result<(), StateStoreError>>,
    },
    Commit {
        response: oneshot::Sender<CommitOutcome>,
    },
}

enum ReadCommand {
    Get {
        key: Key,
        response: oneshot::Sender<Result<Option<StateRecord>, StateStoreError>>,
    },
    Range {
        request: RangeRequest,
        response: oneshot::Sender<Result<RangePage, StateStoreError>>,
    },
    Abort {
        response: oneshot::Sender<Result<(), StateStoreError>>,
    },
}

#[derive(Clone, Copy)]
enum ReadActorDisposition {
    Reuse,
    Destroy,
}

struct ReadActorExit {
    disposition: ReadActorDisposition,
    pending_error: Option<(ReadCommand, StateStoreError)>,
}

impl ReadActorExit {
    const fn complete(disposition: ReadActorDisposition) -> Self {
        Self {
            disposition,
            pending_error: None,
        }
    }

    const fn error(
        disposition: ReadActorDisposition,
        command: ReadCommand,
        error: StateStoreError,
    ) -> Self {
        Self {
            disposition,
            pending_error: Some((command, error)),
        }
    }
}

pub(super) async fn begin_read(
    pool: Arc<dyn PoolLifecycle>,
    operation: MysqlRuntimeGuard,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
) -> Result<MysqlReadTransaction, StateStoreError> {
    let started = StdInstant::now();
    let deadline = Instant::now() + limits.transaction_deadline;
    let codec = MysqlCodec::new(limits.max_key_bytes)?;
    let (commands, receiver) = mpsc::channel(1);
    let (ready_sender, ready_receiver) = oneshot::channel();
    let actor_limits = limits.clone();
    tokio::spawn(async move {
        read_actor(
            pool,
            operation,
            codec,
            actor_limits,
            deadline,
            receiver,
            ready_sender,
        )
        .await;
    });
    let result = ready_receiver.await.map_err(|_| owner_stopped())?;
    record_result(&metrics, StateStoreOperation::Begin, started, &result);
    result?;
    Ok(MysqlReadTransaction {
        commands: Some(commands),
        limits,
        metrics,
        issued_continuations: HashSet::new(),
    })
}

pub(super) async fn begin_write(
    pool: Arc<dyn PoolLifecycle>,
    operation: MysqlRuntimeGuard,
    attempt: WriteAttempt,
    evidence: Arc<MysqlEvidence>,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
) -> Result<MysqlWriteTransaction, StateStoreError> {
    let started = StdInstant::now();
    let deadline = Instant::now() + limits.transaction_deadline;
    let attempt_id = attempt.id();
    let (commands, receiver) = mpsc::channel(1);
    let (ready_sender, ready_receiver) = oneshot::channel();
    let actor_limits = limits.clone();
    tokio::spawn(async move {
        write_actor(
            pool,
            operation,
            attempt,
            evidence,
            actor_limits,
            deadline,
            receiver,
            ready_sender,
        )
        .await;
    });
    let result = ready_receiver.await.map_err(|_| owner_stopped())?;
    record_result(&metrics, StateStoreOperation::Begin, started, &result);
    result?;
    Ok(MysqlWriteTransaction {
        commands: Some(commands),
        attempt: attempt_id,
        limits,
        metrics,
        issued_continuations: HashSet::new(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn write_actor(
    pool: Arc<dyn PoolLifecycle>,
    operation: MysqlRuntimeGuard,
    attempt: WriteAttempt,
    evidence: Arc<MysqlEvidence>,
    limits: StateStoreLimits,
    deadline: Instant,
    mut commands: mpsc::Receiver<WriteCommand>,
    ready: oneshot::Sender<Result<(), StateStoreError>>,
) {
    let result = initialize_write_actor(pool, operation, attempt, evidence, limits, deadline).await;
    let mut state = match result {
        Ok(state) => {
            if ready.send(Ok(())).is_err() {
                // Nobody is left to drive this transaction. Nothing was
                // dispatched, so the attempt is provably effect-free.
                state.cancel_attempt_before_dispatch();
                return;
            }
            state
        }
        Err((attempt, error)) => {
            let _ = attempt.cancel_before_dispatch();
            let _ = ready.send(Err(error));
            return;
        }
    };
    // Every exit below is reached before `commit_inner` runs, and `commit_inner`
    // owns the only `mark_dispatched` call, so each of them closes the attempt
    // as provably effect-free rather than leaving `Slot::drop` to infer it.
    // Stating it here is what keeps the claim true if the dispatch point ever
    // moves: an already-dispatched attempt makes these calls fail loudly.
    while let Some(command) = commands.recv().await {
        match command {
            WriteCommand::Get { key, response } => {
                let result = state.get_inner(&key).await;
                let terminal = !state.transaction_active();
                let _ = response.send(result);
                if terminal {
                    state.cancel_attempt_before_dispatch();
                    return;
                }
            }
            WriteCommand::Range { request, response } => {
                let result = state.range_inner(&request).await;
                let terminal = !state.transaction_active();
                let _ = response.send(result);
                if terminal {
                    state.cancel_attempt_before_dispatch();
                    return;
                }
            }
            WriteCommand::Put {
                key,
                value,
                precondition,
                response,
            } => {
                let _ = response.send(state.put_inner(key, value, precondition));
            }
            WriteCommand::Delete {
                key,
                precondition,
                response,
            } => {
                let _ = response.send(state.delete_inner(key, precondition));
            }
            WriteCommand::Abort { response } => {
                let result = match state.take_transaction() {
                    Ok(transaction) => transaction.rollback().await,
                    Err(error) => Err(error),
                };
                // An abort never reserves and never writes, so the attempt is
                // closed as having had no effect rather than settled from
                // evidence there is none of.
                state.cancel_attempt_before_dispatch();
                let _ = response.send(result);
                return;
            }
            WriteCommand::Commit { response } => {
                let outcome = state.commit_inner().await;
                // Publish first, release second, and do both before the caller
                // is answered: a caller that goes away mid-commit must not be
                // able to leave a terminal unpublished or its evidence behind.
                state.publish_witnessed_outcome(&outcome).await;
                let _ = response.send(outcome);
                return;
            }
        }
    }
    // The caller dropped the transaction without committing or aborting.
    state.cancel_attempt_before_dispatch();
}

async fn initialize_write_actor(
    pool: Arc<dyn PoolLifecycle>,
    operation: MysqlRuntimeGuard,
    attempt: WriteAttempt,
    evidence: Arc<MysqlEvidence>,
    limits: StateStoreLimits,
    deadline: Instant,
) -> Result<MysqlWriteActorState, (WriteAttempt, StateStoreError)> {
    match initialize_write_actor_inner(pool, operation, limits, deadline).await {
        Ok(parts) => Ok(parts.into_state(attempt, evidence)),
        Err(error) => Err((attempt, error)),
    }
}

struct WriteActorParts {
    pool: Arc<dyn PoolLifecycle>,
    transaction: OwnedMysqlTransaction,
    codec: MysqlCodec,
    limits: StateStoreLimits,
    base_revision: u64,
    budget: TransactionBudget,
}

impl WriteActorParts {
    fn into_state(
        self,
        attempt: WriteAttempt,
        evidence: Arc<MysqlEvidence>,
    ) -> MysqlWriteActorState {
        MysqlWriteActorState {
            pool: self.pool,
            transaction: Some(self.transaction),
            codec: self.codec,
            limits: self.limits,
            attempt,
            evidence,
            base_revision: self.base_revision,
            point_observations: BTreeMap::new(),
            range_observed: false,
            mutations: Vec::new(),
            overlay: BTreeMap::new(),
            budget: self.budget,
            range_frozen: false,
            issued_continuations: HashSet::new(),
        }
    }
}

async fn initialize_write_actor_inner(
    pool: Arc<dyn PoolLifecycle>,
    operation: MysqlRuntimeGuard,
    limits: StateStoreLimits,
    deadline: Instant,
) -> Result<WriteActorParts, StateStoreError> {
    let budget = TransactionBudget::new(limits.clone())?;
    let mut transaction =
        OwnedMysqlTransaction::begin(Arc::clone(&pool), operation, deadline).await?;
    #[cfg(feature = "state-store-test-hooks")]
    {
        let connection_id: Option<u64> = transaction
            .run(|connection| Box::pin(connection.query_first("SELECT CONNECTION_ID()")))
            .await?;
        LAST_WRITE_ACTOR_CONNECTION_ID.store(
            connection_id.ok_or_else(persisted_corruption)?,
            Ordering::Release,
        );
    }
    let revision_bytes: Option<Vec<u8>> = transaction
        .run(|connection| {
            Box::pin(connection.exec_first(
                "SELECT meta_value FROM state_store_meta WHERE meta_key = ?",
                (b"current_revision".to_vec(),),
            ))
        })
        .await?;
    let codec = MysqlCodec::new(limits.max_key_bytes)?;
    let base_revision =
        codec.decode_revision(revision_bytes.as_deref().ok_or_else(persisted_corruption)?)?;
    Ok(WriteActorParts {
        pool,
        transaction,
        codec,
        limits,
        base_revision,
        budget,
    })
}

async fn read_actor(
    pool: Arc<dyn PoolLifecycle>,
    _operation: MysqlRuntimeGuard,
    codec: MysqlCodec,
    limits: StateStoreLimits,
    deadline: Instant,
    mut commands: mpsc::Receiver<ReadCommand>,
    ready: oneshot::Sender<Result<(), StateStoreError>>,
) {
    if ready.send(Ok(())).is_err() {
        return;
    }
    let Some(first_command) = commands.recv().await else {
        return;
    };
    let mut connection = match checkout_hygienic_connection(pool, deadline).await {
        Ok(connection) => connection,
        Err(error) => {
            respond_read_start_error(first_command, error);
            return;
        }
    };
    let disposition = run_read_actor(
        &mut connection,
        &codec,
        &limits,
        deadline,
        first_command,
        &mut commands,
    )
    .await;
    if matches!(disposition, ReadActorDisposition::Destroy) {
        connection.destroy().await;
    }
}

async fn run_read_actor(
    connection: &mut MysqlPoolConnection,
    codec: &MysqlCodec,
    limits: &StateStoreLimits,
    deadline: Instant,
    first_command: ReadCommand,
    commands: &mut mpsc::Receiver<ReadCommand>,
) -> ReadActorDisposition {
    let mut options = TxOpts::default();
    options
        .with_isolation_level(IsolationLevel::RepeatableRead)
        .with_consistent_snapshot(true)
        .with_readonly(true);
    super::client::record_statement();
    let exit = match timeout_at(deadline, connection.start_transaction(options)).await {
        Ok(Ok(transaction)) => {
            run_started_read_actor(
                transaction,
                codec,
                limits,
                deadline,
                first_command,
                commands,
            )
            .await
        }
        Ok(Err(error)) => ReadActorExit::error(
            ReadActorDisposition::Destroy,
            first_command,
            MysqlNativeError::from(error).into_public(),
        ),
        Err(_) => ReadActorExit::error(
            ReadActorDisposition::Destroy,
            first_command,
            deadline_error(),
        ),
    };
    if matches!(exit.disposition, ReadActorDisposition::Destroy) {
        connection.destroy_in_place().await;
    }
    if let Some((command, error)) = exit.pending_error {
        respond_read_start_error(command, error);
    }
    exit.disposition
}

async fn run_started_read_actor(
    mut transaction: Transaction<'_>,
    codec: &MysqlCodec,
    limits: &StateStoreLimits,
    deadline: Instant,
    first_command: ReadCommand,
    commands: &mut mpsc::Receiver<ReadCommand>,
) -> ReadActorExit {
    let mut issued_continuations = HashSet::new();
    let mut next_command = Some(first_command);
    while let Some(command) = match next_command.take() {
        Some(command) => Some(command),
        None => commands.recv().await,
    } {
        match command {
            ReadCommand::Get { key, response } => {
                match actor_get(&mut transaction, codec, limits, &key, deadline).await {
                    Ok(record) => {
                        let _ = response.send(Ok(record));
                    }
                    Err(MysqlReadStatementError::Public(error)) => {
                        let _ = response.send(Err(error));
                    }
                    Err(error) => {
                        let (error, disposition) =
                            dispose_read_statement_error(transaction, error, deadline).await;
                        return ReadActorExit::error(
                            disposition,
                            ReadCommand::Get { key, response },
                            error,
                        );
                    }
                }
            }
            ReadCommand::Range { request, response } => {
                let result = match validate_continuation_ownership(&request, &issued_continuations)
                {
                    Ok(()) => {
                        read_range_page(
                            &mut transaction,
                            codec,
                            &request,
                            limits.max_value_bytes,
                            deadline,
                        )
                        .await
                    }
                    Err(error) => Err(MysqlReadStatementError::Public(error)),
                };
                match result {
                    Ok(page) => {
                        if let Some(continuation) = &page.continuation {
                            issued_continuations.insert(continuation.clone());
                        }
                        let _ = response.send(Ok(page));
                    }
                    Err(MysqlReadStatementError::Public(error)) => {
                        let _ = response.send(Err(error));
                    }
                    Err(error) => {
                        let (error, disposition) =
                            dispose_read_statement_error(transaction, error, deadline).await;
                        return ReadActorExit::error(
                            disposition,
                            ReadCommand::Range { request, response },
                            error,
                        );
                    }
                }
            }
            ReadCommand::Abort { response } => {
                let result = timeout_at(deadline, transaction.rollback()).await;
                match result {
                    Ok(Ok(())) => {
                        EXPLICIT_ROLLBACKS.fetch_add(1, Ordering::Relaxed);
                        let _ = response.send(Ok(()));
                        return ReadActorExit::complete(ReadActorDisposition::Reuse);
                    }
                    Ok(Err(error)) => {
                        return ReadActorExit::error(
                            ReadActorDisposition::Destroy,
                            ReadCommand::Abort { response },
                            MysqlNativeError::from(error).into_public(),
                        );
                    }
                    Err(_) => {
                        return ReadActorExit::error(
                            ReadActorDisposition::Destroy,
                            ReadCommand::Abort { response },
                            deadline_error(),
                        );
                    }
                }
            }
        }
    }

    let rollback = timeout_at(
        Instant::now() + std::time::Duration::from_secs(1),
        transaction.rollback(),
    )
    .await;
    if matches!(rollback, Ok(Ok(()))) {
        ReadActorExit::complete(ReadActorDisposition::Reuse)
    } else {
        ReadActorExit::complete(ReadActorDisposition::Destroy)
    }
}

fn respond_read_start_error(command: ReadCommand, error: StateStoreError) {
    match command {
        ReadCommand::Get { response, .. } => {
            let _ = response.send(Err(error));
        }
        ReadCommand::Range { response, .. } => {
            let _ = response.send(Err(error));
        }
        ReadCommand::Abort { response } => {
            let _ = response.send(Err(error));
        }
    }
}

async fn dispose_read_statement_error(
    transaction: Transaction<'_>,
    error: MysqlReadStatementError,
    deadline: Instant,
) -> (StateStoreError, ReadActorDisposition) {
    match error {
        MysqlReadStatementError::Public(error) => (error, ReadActorDisposition::Reuse),
        MysqlReadStatementError::Deadline(error) => {
            drop(transaction);
            (error, ReadActorDisposition::Destroy)
        }
        MysqlReadStatementError::Native(error) => match error.transaction_disposition() {
            MysqlTransactionDisposition::RollbackRequired => {
                match timeout_at(deadline, transaction.rollback()).await {
                    Ok(Ok(())) => (error.into_public(), ReadActorDisposition::Reuse),
                    Ok(Err(rollback_error)) => (
                        MysqlNativeError::from(rollback_error).into_public(),
                        ReadActorDisposition::Destroy,
                    ),
                    Err(_) => (deadline_error(), ReadActorDisposition::Destroy),
                }
            }
            MysqlTransactionDisposition::TransactionEnded => {
                drop(transaction);
                (error.into_public(), ReadActorDisposition::Reuse)
            }
            MysqlTransactionDisposition::DestroyConnection => {
                drop(transaction);
                (error.into_public(), ReadActorDisposition::Destroy)
            }
        },
    }
}

async fn actor_get(
    transaction: &mut Transaction<'_>,
    codec: &MysqlCodec,
    limits: &StateStoreLimits,
    key: &Key,
    deadline: Instant,
) -> Result<Option<StateRecord>, MysqlReadStatementError> {
    validate_key(key, limits)?;
    let key_bytes = key.as_bytes().to_vec();
    super::client::record_statement();
    let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = match timeout_at(
        deadline,
        transaction.exec_first(
            "SELECT key_bytes, value_bytes, version_bytes
             FROM state_store_kv WHERE key_bytes = ?",
            (key_bytes,),
        ),
    )
    .await
    {
        Ok(Ok(row)) => row,
        Ok(Err(error)) => {
            return Err(MysqlReadStatementError::Native(MysqlNativeError::from(
                error,
            )));
        }
        Err(_) => return Err(MysqlReadStatementError::Deadline(deadline_error())),
    };
    row.map(|(key, value, version)| {
        decode_record(codec, key, value, version, limits.max_value_bytes)
    })
    .transpose()
    .map_err(MysqlReadStatementError::Public)
}

impl OwnedMysqlTransaction {
    async fn begin(
        pool: Arc<dyn PoolLifecycle>,
        operation: MysqlRuntimeGuard,
        deadline: Instant,
    ) -> Result<Self, StateStoreError> {
        Self::begin_with_operation(pool, Some(operation), deadline).await
    }

    async fn begin_without_operation(
        pool: Arc<dyn PoolLifecycle>,
        deadline: Instant,
    ) -> Result<Self, StateStoreError> {
        Self::begin_with_operation(pool, None, deadline).await
    }

    async fn begin_with_operation(
        pool: Arc<dyn PoolLifecycle>,
        operation: Option<MysqlRuntimeGuard>,
        deadline: Instant,
    ) -> Result<Self, StateStoreError> {
        let connection = checkout_hygienic_connection(pool, deadline).await?;
        let mut transaction = Self {
            connection: Some(connection),
            operation,
            deadline,
            active: false,
            statement_in_flight: false,
        };
        transaction
            .run(|connection| {
                Box::pin(connection.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            })
            .await?;
        let lock_wait_timeout_seconds = deadline
            .saturating_duration_since(Instant::now())
            .as_secs()
            .saturating_div(2)
            .max(1);
        transaction
            .run(move |connection| {
                Box::pin(connection.exec_drop(
                    "SET SESSION innodb_lock_wait_timeout = ?",
                    (lock_wait_timeout_seconds,),
                ))
            })
            .await?;
        let started = transaction
            .run(|connection| {
                Box::pin(connection.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT"))
            })
            .await;
        if let Err(error) = started {
            if let Some(connection) = transaction.connection.take() {
                connection.destroy().await;
            }
            return Err(error);
        }
        transaction.active = true;
        Ok(transaction)
    }

    async fn rollback_for_reservation(mut self) -> Result<MysqlRuntimeGuard, StateStoreError> {
        self.rollback_inner().await?;
        self.operation.take().ok_or_else(transaction_finished)
    }

    pub(super) async fn run<T>(
        &mut self,
        operation: impl for<'a> FnOnce(&'a mut Conn) -> BoxFuture<'a, Result<T, mysql_async::Error>>,
    ) -> Result<T, StateStoreError> {
        let connection = self.connection.take().ok_or_else(transaction_finished)?;
        self.statement_in_flight = true;
        let result = execute_owned_with_deadline(connection, self.deadline, operation).await;
        self.statement_in_flight = false;
        match result {
            Ok((connection, Ok(value))) => {
                self.connection = Some(connection);
                Ok(value)
            }
            Ok((connection, Err(error))) => {
                let disposition = error.transaction_disposition();
                match disposition {
                    MysqlTransactionDisposition::RollbackRequired if self.active => {
                        self.rollback_after_statement_error(connection).await?;
                    }
                    MysqlTransactionDisposition::TransactionEnded => {
                        self.active = false;
                        self.connection = Some(connection);
                    }
                    MysqlTransactionDisposition::RollbackRequired
                    | MysqlTransactionDisposition::DestroyConnection => {
                        self.active = false;
                        connection.destroy().await;
                    }
                }
                Err(error.into_public())
            }
            Err(error) => {
                self.active = false;
                Err(error)
            }
        }
    }

    pub(super) async fn rollback(mut self) -> Result<(), StateStoreError> {
        let result = self.rollback_inner().await;
        self.active = false;
        result
    }

    async fn commit_native(mut self) -> NativeDataCommitOutcome {
        if !self.active {
            return NativeDataCommitOutcome::BeforeDispatchFailure(transaction_finished());
        }
        let Some(connection) = self.connection.take() else {
            return NativeDataCommitOutcome::BeforeDispatchFailure(transaction_finished());
        };
        self.statement_in_flight = true;
        let result = super::commit::NativeCommitDispatcher::dispatch(
            &super::commit::MysqlNativeCommitDispatcher,
            connection,
            self.deadline,
        )
        .await;
        self.statement_in_flight = false;
        self.active = false;
        match result.result {
            Ok(()) => {
                let Some(connection) = result.connection else {
                    return NativeDataCommitOutcome::Unknown(transaction_finished());
                };
                self.connection = Some(connection);
                NativeDataCommitOutcome::Committed
            }
            Err(error) if result.phase == super::commit::NativeCommitPhase::BeforeDispatch => {
                let Some(connection) = result.connection else {
                    return NativeDataCommitOutcome::BeforeDispatchFailure(error);
                };
                self.connection = Some(connection);
                self.active = true;
                match self.rollback_inner().await {
                    Ok(()) => NativeDataCommitOutcome::BeforeDispatchFailure(error),
                    Err(rollback_error) => {
                        NativeDataCommitOutcome::BeforeDispatchFailure(rollback_error)
                    }
                }
            }
            Err(error) => NativeDataCommitOutcome::Unknown(error),
        }
    }

    async fn rollback_inner(&mut self) -> Result<(), StateStoreError> {
        if !self.active {
            return Ok(());
        }
        let connection = self.connection.take().ok_or_else(transaction_finished)?;
        self.statement_in_flight = true;
        #[cfg(feature = "state-store-test-hooks")]
        match NEXT_ROLLBACK_FAILURE_MODE.swap(0, Ordering::AcqRel) {
            1 => {
                self.statement_in_flight = false;
                self.active = false;
                connection.destroy().await;
                return Err(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "injected MySQL rollback failure",
                ));
            }
            2 => {
                self.active = false;
                connection.destroy().await;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                self.statement_in_flight = false;
                return Err(StateStoreError::new(
                    StateStoreErrorKind::DeadlineExceeded,
                    "injected MySQL rollback timeout",
                ));
            }
            _ => {}
        }
        let cleanup_deadline = self
            .deadline
            .max(Instant::now() + std::time::Duration::from_secs(1));
        let result = execute_owned_with_deadline(connection, cleanup_deadline, |connection| {
            Box::pin(connection.query_drop("ROLLBACK"))
        })
        .await;
        self.statement_in_flight = false;
        self.active = false;
        match result {
            Ok((connection, Ok(()))) => {
                self.connection = Some(connection);
                Ok(())
            }
            Ok((connection, Err(error))) => {
                connection.destroy().await;
                Err(error.into_public())
            }
            Err(error) => Err(error),
        }
    }

    async fn rollback_after_statement_error(
        &mut self,
        connection: MysqlPoolConnection,
    ) -> Result<(), StateStoreError> {
        let result = execute_owned_with_deadline(connection, self.deadline, |connection| {
            Box::pin(connection.query_drop("ROLLBACK"))
        })
        .await;
        self.active = false;
        match result {
            Ok((connection, Ok(()))) => {
                self.connection = Some(connection);
                Ok(())
            }
            Ok((connection, Err(error))) => {
                connection.destroy().await;
                Err(error.into_public())
            }
            Err(error) => Err(error),
        }
    }
}

impl Drop for OwnedMysqlTransaction {
    fn drop(&mut self) {
        let Some(mut connection) = self.connection.take() else {
            return;
        };
        let operation = self.operation.take();
        if self.statement_in_flight {
            tokio::spawn(async move {
                connection.destroy().await;
                drop(operation);
            });
            return;
        }
        if !self.active {
            drop(connection);
            drop(operation);
            return;
        }
        tokio::spawn(async move {
            let cleanup = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                connection.query_drop("ROLLBACK"),
            )
            .await;
            if !matches!(cleanup, Ok(Ok(()))) {
                connection.destroy().await;
            }
            drop(operation);
        });
    }
}

impl MysqlReadTransaction {
    async fn request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, StateStoreError>>) -> ReadCommand,
    ) -> Result<T, StateStoreError> {
        let commands = self.commands.as_ref().ok_or_else(transaction_finished)?;
        let (response, receiver) = oneshot::channel();
        commands
            .send(build(response))
            .await
            .map_err(|_| owner_stopped())?;
        receiver.await.map_err(|_| owner_stopped())?
    }
}

#[async_trait]
impl ReadTransaction for MysqlReadTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        let started = StdInstant::now();
        let result = validate_key(key, &self.limits).map(|_| key.clone());
        let result = match result {
            Ok(key) => {
                self.request(|response| ReadCommand::Get { key, response })
                    .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Get, started, &result);
        if let Ok(Some(record)) = &result {
            self.metrics.record_bytes_read(
                u64::try_from(record.key.as_bytes().len() + record.value.as_bytes().len())
                    .unwrap_or(u64::MAX),
            );
        }
        result
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        let started = StdInstant::now();
        let result = validate_range(request, &self.limits)
            .and_then(|_| validate_continuation_ownership(request, &self.issued_continuations))
            .map(|()| request.clone());
        let result = match result {
            Ok(request) => {
                self.request(|response| ReadCommand::Range { request, response })
                    .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Range, started, &result);
        if let Ok(page) = &result {
            self.metrics.record_page_records(page.records.len() as u64);
            if let Some(continuation) = &page.continuation {
                self.issued_continuations.insert(continuation.clone());
            }
        }
        result
    }

    async fn abort(mut self: Box<Self>) -> Result<(), StateStoreError> {
        let commands = self.commands.take().ok_or_else(transaction_finished)?;
        let (response, receiver) = oneshot::channel();
        commands
            .send(ReadCommand::Abort { response })
            .await
            .map_err(|_| owner_stopped())?;
        drop(commands);
        receiver.await.map_err(|_| owner_stopped())?
    }
}

impl MysqlWriteActorState {
    fn transaction_active(&self) -> bool {
        self.transaction
            .as_ref()
            .is_some_and(|transaction| transaction.active)
    }

    fn transaction(&mut self) -> Result<&mut OwnedMysqlTransaction, StateStoreError> {
        self.transaction.as_mut().ok_or_else(transaction_finished)
    }

    fn take_transaction(&mut self) -> Result<OwnedMysqlTransaction, StateStoreError> {
        self.transaction.take().ok_or_else(transaction_finished)
    }

    async fn load_base_record(
        &mut self,
        key: &Key,
    ) -> Result<Option<StateRecord>, StateStoreError> {
        let key_bytes = key.as_bytes().to_vec();
        let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = self
            .transaction()?
            .run(move |connection| {
                Box::pin(connection.exec_first(
                    "SELECT key_bytes, value_bytes, version_bytes
                     FROM state_store_kv WHERE key_bytes = ?",
                    (key_bytes,),
                ))
            })
            .await?;
        row.map(|(key, value, version)| {
            decode_record(
                &self.codec,
                key,
                value,
                version,
                self.limits.max_value_bytes,
            )
        })
        .transpose()
    }

    async fn get_inner(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        validate_key(key, &self.limits)?;
        let base = self.load_base_record(key).await?;
        let observation = base.as_ref().map_or(PointObservation::Absent, |record| {
            PointObservation::Present(record.version.clone())
        });
        if let Some(previous) = self.point_observations.get(key) {
            if previous != &observation {
                return Err(persisted_corruption());
            }
        } else {
            self.point_observations.insert(key.clone(), observation);
        }
        Ok(replay_visible_record(
            key,
            base,
            self.mutations
                .iter()
                .filter(|(candidate, _)| candidate == key),
        ))
    }

    async fn range_inner(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        validate_range(request, &self.limits)?;
        validate_continuation_ownership(request, &self.issued_continuations)?;
        self.range_observed = true;
        let resume = request
            .continuation
            .as_ref()
            .map(|token| token.resume_after(request))
            .transpose()?;
        let limit = request
            .page_size
            .checked_add(self.overlay.len())
            .and_then(|value| value.checked_add(1))
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(invalid_range)?;
        let start = request.range.start.as_bytes().to_vec();
        let end = request.range.end.as_bytes().to_vec();
        let direction = request.direction;
        let rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = match (direction, resume.as_ref()) {
            (Direction::Forward, None) => {
                self.transaction()?
                    .run(move |connection| {
                        Box::pin(connection.exec(
                            "SELECT key_bytes, value_bytes, version_bytes
                             FROM state_store_kv
                             WHERE key_bytes >= ? AND key_bytes < ?
                             ORDER BY key_bytes ASC LIMIT ?",
                            (start, end, limit),
                        ))
                    })
                    .await?
            }
            (Direction::Forward, Some(resume)) => {
                let resume = resume.as_bytes().to_vec();
                self.transaction()?
                    .run(move |connection| {
                        Box::pin(connection.exec(
                            "SELECT key_bytes, value_bytes, version_bytes
                             FROM state_store_kv
                             WHERE key_bytes >= ? AND key_bytes < ? AND key_bytes > ?
                             ORDER BY key_bytes ASC LIMIT ?",
                            (start, end, resume, limit),
                        ))
                    })
                    .await?
            }
            (Direction::Reverse, None) => {
                self.transaction()?
                    .run(move |connection| {
                        Box::pin(connection.exec(
                            "SELECT key_bytes, value_bytes, version_bytes
                             FROM state_store_kv
                             WHERE key_bytes >= ? AND key_bytes < ?
                             ORDER BY key_bytes DESC LIMIT ?",
                            (start, end, limit),
                        ))
                    })
                    .await?
            }
            (Direction::Reverse, Some(resume)) => {
                let resume = resume.as_bytes().to_vec();
                self.transaction()?
                    .run(move |connection| {
                        Box::pin(connection.exec(
                            "SELECT key_bytes, value_bytes, version_bytes
                             FROM state_store_kv
                             WHERE key_bytes >= ? AND key_bytes < ? AND key_bytes < ?
                             ORDER BY key_bytes DESC LIMIT ?",
                            (start, end, resume, limit),
                        ))
                    })
                    .await?
            }
        };
        let mut visible = rows
            .into_iter()
            .map(|(key, value, version)| {
                let record = decode_record(
                    &self.codec,
                    key,
                    value,
                    version,
                    self.limits.max_value_bytes,
                )?;
                Ok((record.key.clone(), record))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        for (key, mutation) in self
            .overlay
            .range(request.range.start.clone()..request.range.end.clone())
        {
            match mutation {
                Mutation::Put {
                    value,
                    provisional_version,
                    ..
                } => {
                    visible.insert(
                        key.clone(),
                        StateRecord {
                            key: key.clone(),
                            value: value.clone(),
                            version: provisional_version.clone(),
                        },
                    );
                }
                Mutation::Delete { .. } => {
                    visible.remove(key);
                }
            }
        }
        let mut records = visible.into_values().collect::<Vec<_>>();
        if direction == Direction::Reverse {
            records.reverse();
        }
        if let Some(resume) = resume {
            records.retain(|record| match direction {
                Direction::Forward => record.key > resume,
                Direction::Reverse => record.key < resume,
            });
        }
        let has_more = records.len() > request.page_size;
        records.truncate(request.page_size);
        let continuation = if has_more {
            records
                .last()
                .map(|record| request.continuation_after(&record.key))
                .transpose()?
        } else {
            None
        };
        if continuation.is_some() {
            self.range_frozen = true;
        }
        let page = RangePage {
            records,
            continuation,
        };
        if let Some(continuation) = &page.continuation {
            self.issued_continuations.insert(continuation.clone());
        }
        Ok(page)
    }

    fn put_inner(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<usize, StateStoreError> {
        validate_key_value(&key, Some(&value), &self.limits)?;
        if self.range_frozen {
            return Err(writes_frozen());
        }
        let operation = self
            .budget
            .stage_put(key.as_bytes(), value.as_bytes(), &precondition)?;
        let provisional_version = provisional_version(self.attempt.id(), operation);
        let bytes = key.as_bytes().len().saturating_add(value.as_bytes().len());
        let mutation = Mutation::Put {
            value,
            precondition,
            provisional_version,
        };
        self.overlay.insert(key.clone(), mutation.clone());
        self.mutations.push((key, mutation));
        Ok(bytes)
    }

    fn delete_inner(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<usize, StateStoreError> {
        validate_key(&key, &self.limits)?;
        if self.range_frozen {
            return Err(writes_frozen());
        }
        self.budget.stage_delete(key.as_bytes(), &precondition)?;
        let bytes = key.as_bytes().len();
        let mutation = Mutation::Delete { precondition };
        self.overlay.insert(key.clone(), mutation.clone());
        self.mutations.push((key, mutation));
        Ok(bytes)
    }
}

#[async_trait]
impl ReadTransaction for MysqlWriteTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        let started = StdInstant::now();
        let result = validate_key(key, &self.limits).map(|_| key.clone());
        let result = match result {
            Ok(key) => {
                self.request(|response| WriteCommand::Get { key, response })
                    .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Get, started, &result);
        result
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        let started = StdInstant::now();
        let result = validate_range(request, &self.limits)
            .and_then(|_| validate_continuation_ownership(request, &self.issued_continuations))
            .map(|()| request.clone());
        let result = match result {
            Ok(request) => {
                self.request(|response| WriteCommand::Range { request, response })
                    .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Range, started, &result);
        if let Ok(page) = &result {
            self.metrics.record_page_records(page.records.len() as u64);
            if let Some(continuation) = &page.continuation {
                self.issued_continuations.insert(continuation.clone());
            }
        }
        result
    }

    async fn abort(mut self: Box<Self>) -> Result<(), StateStoreError> {
        let commands = self.commands.take().ok_or_else(transaction_finished)?;
        let (response, receiver) = oneshot::channel();
        commands
            .send(WriteCommand::Abort { response })
            .await
            .map_err(|_| owner_stopped())?;
        drop(commands);
        receiver.await.map_err(|_| owner_stopped())?
    }
}

impl MysqlWriteTransaction {
    async fn request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, StateStoreError>>) -> WriteCommand,
    ) -> Result<T, StateStoreError> {
        let commands = self.commands.as_ref().ok_or_else(transaction_finished)?;
        let (response, receiver) = oneshot::channel();
        commands
            .send(build(response))
            .await
            .map_err(|_| owner_stopped())?;
        receiver.await.map_err(|_| owner_stopped())?
    }
}

#[async_trait]
impl WriteTransaction for MysqlWriteTransaction {
    fn attempt(&self) -> AttemptId {
        self.attempt
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        let started = StdInstant::now();
        let result = validate_key_value(&key, Some(&value), &self.limits);
        let result = match result {
            Ok(()) => {
                self.request(|response| WriteCommand::Put {
                    key,
                    value,
                    precondition,
                    response,
                })
                .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Put, started, &result);
        if let Ok(bytes) = result {
            self.metrics
                .record_bytes_written(u64::try_from(bytes).unwrap_or(u64::MAX));
            Ok(())
        } else {
            result.map(|_| ())
        }
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        let started = StdInstant::now();
        let result = validate_key(&key, &self.limits);
        let result = match result {
            Ok(()) => {
                self.request(|response| WriteCommand::Delete {
                    key,
                    precondition,
                    response,
                })
                .await
            }
            Err(error) => Err(error),
        };
        record_result(&self.metrics, StateStoreOperation::Delete, started, &result);
        if let Ok(bytes) = result {
            self.metrics
                .record_bytes_written(u64::try_from(bytes).unwrap_or(u64::MAX));
            Ok(())
        } else {
            result.map(|_| ())
        }
    }

    async fn commit(mut self: Box<Self>) -> CommitOutcome {
        let started = StdInstant::now();
        let metrics = Arc::clone(&self.metrics);
        let outcome = match self.commands.take() {
            Some(commands) => {
                let (response, receiver) = oneshot::channel();
                if commands
                    .send(WriteCommand::Commit { response })
                    .await
                    .is_err()
                {
                    CommitOutcome::DefiniteFailure(owner_stopped())
                } else {
                    drop(commands);
                    receiver
                        .await
                        .unwrap_or_else(|_| CommitOutcome::DefiniteFailure(owner_stopped()))
                }
            }
            None => CommitOutcome::DefiniteFailure(transaction_finished()),
        };
        record_commit(&metrics, started, &outcome);
        outcome
    }
}

impl MysqlWriteActorState {
    /// Closes the attempt with the terminal this actor actually witnessed,
    /// then drops the evidence that terminal was derived from.
    ///
    /// The order is the contract: the proof is published first, so a later
    /// reader reads a recorded verdict rather than re-deriving one from a row
    /// that is on its way out. [`CommitOutcome::CommitUnknown`] publishes
    /// nothing and releases nothing on purpose -- an ambiguous answer is not a
    /// verdict, and the ledger row is the only thing that can still resolve it.
    async fn publish_witnessed_outcome(&mut self, outcome: &CommitOutcome) {
        let verdict = match outcome {
            CommitOutcome::Committed(receipt) => AttemptOutcome::Committed(receipt.clone()),
            // All three say the write can no longer land, and each of them is
            // reached only after the durable ledger agreed.
            CommitOutcome::Conflict(_)
            | CommitOutcome::TransientBeforeCommit(_)
            | CommitOutcome::DefiniteFailure(_) => AttemptOutcome::NotCommitted,
            CommitOutcome::CommitUnknown(_) => return,
        };
        if let Err(error) = self.attempt.settle(verdict) {
            tracing::warn!(
                provider = "mysql",
                %error,
                "MySQL could not publish a witnessed commit outcome"
            );
            return;
        }
        release_witnessed_evidence(&self.evidence, self.attempt.id()).await;
    }

    /// Records that this attempt never reached storage.
    fn cancel_attempt_before_dispatch(&self) {
        if let Err(error) = self.attempt.cancel_before_dispatch() {
            tracing::warn!(
                provider = "mysql",
                %error,
                "MySQL could not close an undispatched write attempt"
            );
        }
    }

    async fn commit_inner(&mut self) -> CommitOutcome {
        let deadline = match self.transaction() {
            Ok(transaction) => transaction.deadline,
            Err(error) => return CommitOutcome::DefiniteFailure(error),
        };
        let operation = match self.take_transaction() {
            Ok(transaction) => match transaction.rollback_for_reservation().await {
                Ok(operation) => operation,
                Err(error) => return CommitOutcome::DefiniteFailure(error),
            },
            Err(error) => return CommitOutcome::DefiniteFailure(error),
        };
        let _operation = operation;
        // Everything above this line is reads and rollbacks: the reservation
        // below is the first statement that can leave a trace a later reader
        // might see, so the attempt is marked dispatched before it, not before
        // the data commit.
        if let Err(error) = self.attempt.mark_dispatched() {
            return CommitOutcome::DefiniteFailure(error);
        }
        let attempt = self.attempt.id();
        let reservation =
            super::commit::reserve_commit(Arc::clone(&self.pool), &self.codec, attempt, deadline)
                .await;
        match reservation {
            Ok(super::commit::ReservationDecision::Committed(receipt)) => {
                return CommitOutcome::Committed(receipt);
            }
            Ok(super::commit::ReservationDecision::Reserved) => {}
            Err(error) => {
                return classify_terminalized_prepare(
                    error,
                    self.terminalize_undispatched(attempt).await,
                );
            }
        }

        let transaction =
            match OwnedMysqlTransaction::begin_without_operation(Arc::clone(&self.pool), deadline)
                .await
            {
                Ok(transaction) => transaction,
                Err(error) => {
                    return classify_terminalized_prepare(
                        error,
                        self.terminalize_undispatched(attempt).await,
                    );
                }
            };
        self.transaction = Some(transaction);
        let result = self.prepare_and_apply_commit().await;
        match result {
            Ok(revision) => {
                let transaction = match self.take_transaction() {
                    Ok(transaction) => transaction,
                    Err(error) => return CommitOutcome::DefiniteFailure(error),
                };
                match transaction.commit_native().await {
                    NativeDataCommitOutcome::Committed => {
                        CommitOutcome::Committed(CommitReceipt { attempt, revision })
                    }
                    NativeDataCommitOutcome::BeforeDispatchFailure(error) => {
                        classify_terminalized_prepare(
                            error,
                            self.terminalize_undispatched(attempt).await,
                        )
                    }
                    NativeDataCommitOutcome::Unknown(error) => CommitOutcome::CommitUnknown(error),
                }
            }
            Err(error) => self.terminalize_failed_prepare(error).await,
        }
    }

    async fn terminalize_undispatched(
        &self,
        attempt: AttemptId,
    ) -> Result<super::commit::TerminalizeDecision, StateStoreError> {
        let cleanup_deadline = Instant::now() + std::time::Duration::from_secs(2);
        super::commit::terminalize_undispatched(
            Arc::clone(&self.pool),
            &self.codec,
            attempt,
            cleanup_deadline,
        )
        .await
    }

    async fn terminalize_failed_prepare(
        &mut self,
        prepare_error: StateStoreError,
    ) -> CommitOutcome {
        if let Ok(transaction) = self.take_transaction() {
            let _ = transaction.rollback().await;
        }
        let terminalization = self.terminalize_undispatched(self.attempt.id()).await;
        classify_prepare_failure(PrepareFailureEvidence {
            prepare_error,
            terminalization,
        })
    }

    /// Locks the reservation, validates the transaction's observations, and
    /// stages every mutation plus the ledger's flip to committed.
    ///
    /// # Why there is no reservation token any more
    ///
    /// This used to insert a random 16-byte token with the reservation and
    /// re-check it here. The token did two jobs. The first was telling *my*
    /// pending row apart from a pending row some other process had written
    /// under the same caller-minted transaction UUID; that job is gone, because
    /// an [`AttemptId`] carries an instance scope minted at open, never
    /// persisted and impossible to build from bytes, so no second writer can
    /// address this row at all. The second job -- proving the row about to be
    /// flipped is still the live reservation this attempt made -- is *not*
    /// gone, and is kept: the read below takes the row `FOR UPDATE` and refuses
    /// anything but `Pending`, and the flip at the end of this function stays a
    /// compare-and-swap on that same state. Dropping the guard along with the
    /// token would let a row that had already been terminalized be quietly
    /// rewritten to committed, which is the one thing the attempt contract says
    /// can never happen.
    async fn prepare_and_apply_commit(&mut self) -> Result<StoreRevision, StateStoreError> {
        let attempt_bytes = encode_attempt_id(self.attempt.id());
        let ledger: Option<(u8, Option<u64>)> = self
            .transaction()?
            .run({
                let attempt_bytes = attempt_bytes.clone();
                move |connection| {
                    Box::pin(connection.exec_first(
                        "SELECT state, revision
                         FROM state_store_commits WHERE attempt_id = ? FOR UPDATE",
                        (attempt_bytes,),
                    ))
                }
            })
            .await?;
        // The reservation committed on its own connection just before this, so
        // an absent row is not "no reservation" -- it is a row that vanished
        // under a live attempt, which is corruption rather than a conflict.
        let state = ledger
            .map(|(state, revision)| self.codec.decode_commit_state(state, revision))
            .transpose()?
            .ok_or_else(persisted_corruption)?;
        if state != DurableCommitState::Pending {
            return Err(conflict(
                "MySQL commit reservation is no longer pending for this attempt",
            ));
        }
        #[cfg(feature = "state-store-test-hooks")]
        {
            let rollback_mode = PREPARE_FAILURE_ROLLBACK_MODE.swap(0, Ordering::AcqRel);
            if rollback_mode != 0 {
                let connection_id: Option<u64> = self
                    .transaction()?
                    .run(|connection| Box::pin(connection.query_first("SELECT CONNECTION_ID()")))
                    .await?;
                LAST_PREPARE_FAILURE_CONNECTION_ID.store(
                    connection_id.ok_or_else(persisted_corruption)?,
                    Ordering::Release,
                );
                NEXT_ROLLBACK_FAILURE_MODE.store(rollback_mode, Ordering::Release);
                return Err(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "injected MySQL prepare failure after durable reservation",
                ));
            }
        }
        let revision_bytes: Option<Vec<u8>> = self
            .transaction()?
            .run(|connection| {
                Box::pin(connection.exec_first(
                    "SELECT meta_value FROM state_store_meta
                     WHERE meta_key = ? FOR UPDATE",
                    (b"current_revision".to_vec(),),
                ))
            })
            .await?;
        let commit_base_revision = self
            .codec
            .decode_revision(revision_bytes.as_deref().ok_or_else(persisted_corruption)?)?;

        let observations = self
            .point_observations
            .iter()
            .map(|(key, observation)| (key.clone(), observation.clone()))
            .collect::<Vec<_>>();
        for (key, expected) in observations {
            let current = self.load_current_locking(&key).await?;
            let actual = current.as_ref().map_or(PointObservation::Absent, |record| {
                PointObservation::Present(record.version.clone())
            });
            if actual != expected {
                return Err(conflict("MySQL point observation changed before commit"));
            }
        }
        if self.range_observed && commit_base_revision != self.base_revision {
            return Err(conflict(
                "MySQL range observation revision changed before commit",
            ));
        }

        let touched = self
            .mutations
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<BTreeSet<_>>();
        #[cfg(feature = "state-store-test-hooks")]
        {
            touched_lock_order()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clear();
        }
        let mut state = BTreeMap::new();
        for key in &touched {
            #[cfg(feature = "state-store-test-hooks")]
            touched_lock_order()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(key.as_bytes().to_vec());
            state.insert(key.clone(), self.load_current_locking(key).await?);
        }
        let original = state.clone();
        for (key, mutation) in &self.mutations {
            let current = state.get(key).cloned().flatten();
            if !precondition_matches(mutation.precondition(), current.as_ref()) {
                return Err(StateStoreError::new(
                    StateStoreErrorKind::PreconditionFailed,
                    "MySQL transaction precondition failed",
                ));
            }
            let next = match mutation {
                Mutation::Put {
                    value,
                    provisional_version,
                    ..
                } => Some(StateRecord {
                    key: key.clone(),
                    value: value.clone(),
                    version: provisional_version.clone(),
                }),
                Mutation::Delete { .. } => None,
            };
            state.insert(key.clone(), next);
        }
        let changed = touched
            .into_iter()
            .filter(|key| match self.overlay.get(key) {
                Some(Mutation::Put { .. }) => true,
                Some(Mutation::Delete { .. }) => {
                    original.get(key).and_then(Option::as_ref).is_some()
                }
                None => false,
            })
            .collect::<Vec<_>>();
        let next_revision = self.codec.checked_next_revision(commit_base_revision)?;
        for (sequence, key) in changed.iter().enumerate() {
            let sequence = u32::try_from(sequence).map_err(|_| {
                StateStoreError::new(
                    StateStoreErrorKind::LimitExceeded,
                    "MySQL transaction change sequence exceeds the supported range",
                )
            })?;
            match state.get(key).cloned().flatten() {
                Some(record) => {
                    let key_bytes = key.as_bytes().to_vec();
                    let value_bytes = record.value.as_bytes().to_vec();
                    let version = self.codec.encode_version(next_revision, sequence).to_vec();
                    let update_value = value_bytes.clone();
                    let update_version = version.clone();
                    self.transaction()?
                        .run(move |connection| {
                            Box::pin(connection.exec_drop(
                                "INSERT INTO state_store_kv
                                    (key_bytes, value_bytes, version_bytes)
                                 VALUES (?, ?, ?)
                                 ON DUPLICATE KEY UPDATE
                                    value_bytes = ?, version_bytes = ?",
                                (
                                    key_bytes,
                                    value_bytes,
                                    version,
                                    update_value,
                                    update_version,
                                ),
                            ))
                        })
                        .await?;
                }
                None => {
                    let key_bytes = key.as_bytes().to_vec();
                    self.transaction()?
                        .run(move |connection| {
                            Box::pin(connection.exec_drop(
                                "DELETE FROM state_store_kv WHERE key_bytes = ?",
                                (key_bytes,),
                            ))
                        })
                        .await?;
                }
            }
        }
        self.transaction()?
            .run(move |connection| {
                Box::pin(connection.exec_drop(
                    "UPDATE state_store_meta SET meta_value = ?
                     WHERE meta_key = ?",
                    (
                        next_revision.to_be_bytes().to_vec(),
                        b"current_revision".to_vec(),
                    ),
                ))
            })
            .await?;
        // Compare-and-swap, not a blind write: the row must still be the
        // pending reservation this attempt made. Anything else -- released,
        // already terminalized -- affects no row and fails the commit here,
        // before the data transaction is allowed to become durable.
        let ledger_updates = self
            .transaction()?
            .run(move |connection| {
                Box::pin(async move {
                    connection
                        .exec_drop(
                            "UPDATE state_store_commits
                             SET state = ?, revision = ?, updated_at_ms = ?
                             WHERE attempt_id = ? AND state = ?",
                            (
                                COMMIT_STATE_COMMITTED,
                                next_revision,
                                current_time_ms(),
                                attempt_bytes,
                                COMMIT_STATE_PENDING,
                            ),
                        )
                        .await?;
                    Ok(connection.affected_rows())
                })
            })
            .await?;
        if ledger_updates != 1 {
            return Err(persisted_corruption());
        }
        StoreRevision::try_from(bytes::Bytes::copy_from_slice(
            &self.codec.encode_revision(next_revision),
        ))
    }

    async fn load_current_locking(
        &mut self,
        key: &Key,
    ) -> Result<Option<StateRecord>, StateStoreError> {
        let key_bytes = key.as_bytes().to_vec();
        let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = self
            .transaction()?
            .run(move |connection| {
                Box::pin(connection.exec_first(
                    "SELECT key_bytes, value_bytes, version_bytes
                     FROM state_store_kv WHERE key_bytes = ? FOR UPDATE",
                    (key_bytes,),
                ))
            })
            .await?;
        row.map(|(key, value, version)| {
            decode_record(
                &self.codec,
                key,
                value,
                version,
                self.limits.max_value_bytes,
            )
        })
        .transpose()
    }
}

fn replay_visible_record<'a>(
    key: &Key,
    mut state: Option<StateRecord>,
    mutations: impl Iterator<Item = &'a (Key, Mutation)>,
) -> Option<StateRecord> {
    for (_, mutation) in mutations {
        state = match mutation {
            Mutation::Put {
                value,
                provisional_version,
                ..
            } => Some(StateRecord {
                key: key.clone(),
                value: value.clone(),
                version: provisional_version.clone(),
            }),
            Mutation::Delete { .. } => None,
        };
    }
    state
}

fn precondition_matches(precondition: &Precondition, current: Option<&StateRecord>) -> bool {
    match precondition {
        Precondition::Any => true,
        Precondition::Absent => current.is_none(),
        Precondition::Present => current.is_some(),
        Precondition::Version(expected) => {
            current.is_some_and(|record| &record.version == expected)
        }
    }
}

fn provisional_version(attempt: AttemptId, operation: u64) -> VersionToken {
    VersionToken::try_from(bytes::Bytes::from(
        [
            PROVISIONAL_VERSION_TAG,
            attempt.to_string().as_bytes(),
            b"\0",
            &operation.to_be_bytes(),
        ]
        .concat(),
    ))
    .expect("provisional version is non-empty")
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn validate_key(key: &Key, limits: &StateStoreLimits) -> Result<(), StateStoreError> {
    if key.as_bytes().len() > limits.max_key_bytes {
        return Err(StateStoreError::new(
            StateStoreErrorKind::LimitExceeded,
            "key exceeds the configured byte limit",
        ));
    }
    Ok(())
}

fn validate_key_value(
    key: &Key,
    value: Option<&Value>,
    limits: &StateStoreLimits,
) -> Result<(), StateStoreError> {
    validate_key(key, limits)?;
    if value.is_some_and(|value| value.as_bytes().len() > limits.max_value_bytes) {
        return Err(StateStoreError::new(
            StateStoreErrorKind::LimitExceeded,
            "value exceeds the configured byte limit",
        ));
    }
    Ok(())
}

fn validate_range(
    request: &RangeRequest,
    limits: &StateStoreLimits,
) -> Result<(), StateStoreError> {
    request.validate(limits)?;
    validate_key(&request.range.start, limits)?;
    validate_key(&request.range.end, limits)?;
    if let Some(continuation) = &request.continuation {
        validate_key(&continuation.resume_after(request)?, limits)?;
    }
    Ok(())
}

fn validate_continuation_ownership(
    request: &RangeRequest,
    issued_continuations: &HashSet<ContinuationToken>,
) -> Result<(), StateStoreError> {
    if request
        .continuation
        .as_ref()
        .is_some_and(|continuation| !issued_continuations.contains(continuation))
    {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidRequest,
            "MySQL continuation does not belong to this transaction",
        ));
    }
    Ok(())
}

fn record_result<T>(
    metrics: &StateStoreMetrics,
    operation: StateStoreOperation,
    started: StdInstant,
    result: &Result<T, StateStoreError>,
) {
    metrics.record_operation(
        operation,
        if result.is_ok() {
            StateStoreOutcome::Success
        } else {
            StateStoreOutcome::Error
        },
        started.elapsed(),
    );
}

fn record_commit(metrics: &StateStoreMetrics, started: StdInstant, outcome: &CommitOutcome) {
    let metric = match outcome {
        CommitOutcome::Committed(_) => StateStoreOutcome::Success,
        CommitOutcome::Conflict(_) => StateStoreOutcome::Conflict,
        CommitOutcome::TransientBeforeCommit(_) => StateStoreOutcome::TransientBeforeCommit,
        CommitOutcome::DefiniteFailure(_) => StateStoreOutcome::DefiniteFailure,
        CommitOutcome::CommitUnknown(_) => StateStoreOutcome::CommitUnknown,
    };
    metrics.record_operation(StateStoreOperation::Commit, metric, started.elapsed());
}

fn classify_prepare_error(error: StateStoreError) -> CommitOutcome {
    match error.kind() {
        StateStoreErrorKind::Conflict | StateStoreErrorKind::PreconditionFailed => {
            CommitOutcome::Conflict(error)
        }
        StateStoreErrorKind::Transient | StateStoreErrorKind::ProviderUnavailable => {
            CommitOutcome::TransientBeforeCommit(error)
        }
        _ => CommitOutcome::DefiniteFailure(error),
    }
}

fn classify_terminalized_prepare(
    prepare_error: StateStoreError,
    decision: Result<super::commit::TerminalizeDecision, StateStoreError>,
) -> CommitOutcome {
    classify_prepare_failure(PrepareFailureEvidence {
        prepare_error,
        terminalization: decision,
    })
}

struct PrepareFailureEvidence {
    prepare_error: StateStoreError,
    terminalization: Result<super::commit::TerminalizeDecision, StateStoreError>,
}

fn classify_prepare_failure(evidence: PrepareFailureEvidence) -> CommitOutcome {
    let PrepareFailureEvidence {
        prepare_error,
        terminalization,
    } = evidence;
    // Local rollback is connection hygiene only; the durable ledger is the outcome authority.
    match terminalization {
        Ok(super::commit::TerminalizeDecision::Committed(receipt)) => {
            CommitOutcome::Committed(receipt)
        }
        Ok(super::commit::TerminalizeDecision::NotCommitted) => {
            classify_prepare_error(prepare_error)
        }
        Err(cleanup_error) => CommitOutcome::CommitUnknown(cleanup_error),
    }
}

const fn conflict(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::Conflict, message)
}

const fn transaction_finished() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "MySQL state transaction is already finished",
    )
}

const fn owner_stopped() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "MySQL state transaction owner stopped unexpectedly",
    )
}

const fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "MySQL state transaction deadline exceeded",
    )
}

const fn persisted_corruption() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "MySQL state store persisted state is malformed",
    )
}

const fn invalid_range() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "MySQL range request is invalid",
    )
}

const fn writes_frozen() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "writes are frozen after paginated range reads",
    )
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(crate) fn explicit_rollback_count_for_test() -> u64 {
    EXPLICIT_ROLLBACKS.load(Ordering::Relaxed)
}

#[cfg(feature = "state-store-test-hooks")]
pub(crate) fn last_write_actor_connection_id_for_test() -> u64 {
    LAST_WRITE_ACTOR_CONNECTION_ID.load(Ordering::Acquire)
}

#[cfg(feature = "state-store-test-hooks")]
pub(crate) fn fail_next_prepare_after_reservation_for_test(rollback_mode: u8) {
    assert!(matches!(rollback_mode, 1 | 2));
    LAST_PREPARE_FAILURE_CONNECTION_ID.store(0, Ordering::Release);
    PREPARE_FAILURE_ROLLBACK_MODE.store(rollback_mode, Ordering::Release);
}

#[cfg(feature = "state-store-test-hooks")]
pub(crate) fn last_prepare_failure_connection_id_for_test() -> u64 {
    LAST_PREPARE_FAILURE_CONNECTION_ID.load(Ordering::Acquire)
}

#[cfg(feature = "state-store-test-hooks")]
pub(crate) fn last_touched_lock_order_for_test() -> Vec<Vec<u8>> {
    touched_lock_order()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

#[cfg(feature = "state-store-test-hooks")]
fn touched_lock_order() -> &'static Mutex<Vec<Vec<u8>>> {
    LAST_TOUCHED_LOCK_ORDER.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(crate) struct MysqlHeldKvLock {
    transaction: Option<OwnedMysqlTransaction>,
}

impl MysqlHeldKvLock {
    #[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
    pub(crate) async fn release(mut self) -> Result<(), StateStoreError> {
        self.transaction
            .take()
            .ok_or_else(transaction_finished)?
            .rollback()
            .await
    }
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(super) async fn hold_kv_lock_for_test(
    pool: Arc<dyn PoolLifecycle>,
    operation: MysqlRuntimeGuard,
    key: &[u8],
    deadline: Instant,
) -> Result<MysqlHeldKvLock, StateStoreError> {
    let mut transaction = OwnedMysqlTransaction::begin(pool, operation, deadline).await?;
    let key = key.to_vec();
    transaction
        .run(move |connection| {
            Box::pin(connection.exec_drop(
                "INSERT INTO state_store_kv (key_bytes, value_bytes, version_bytes)
                 VALUES (?, ?, ?)
                 ON DUPLICATE KEY UPDATE value_bytes = VALUES(value_bytes)",
                (key, vec![0x7f], vec![0; 12]),
            ))
        })
        .await?;
    Ok(MysqlHeldKvLock {
        transaction: Some(transaction),
    })
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(crate) async fn insert_malformed_kv_row_for_test(
    pool: Arc<dyn PoolLifecycle>,
    key: &[u8],
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let connection = checkout_hygienic_connection(pool, deadline).await?;
    let mut transaction = OwnedMysqlTransaction {
        connection: Some(connection),
        operation: None,
        deadline,
        active: false,
        statement_in_flight: false,
    };
    let key = key.to_vec();
    let oversized_value = vec![0x5a; novarocks_state_store_api::MAX_VALUE_BYTES + 1];
    transaction
        .run(move |connection| {
            Box::pin(connection.exec_drop(
                "INSERT INTO state_store_kv (key_bytes, value_bytes, version_bytes)
                 VALUES (?, ?, ?)",
                (key, oversized_value, vec![0; 12]),
            ))
        })
        .await
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(super) async fn deadlock_1213_maps_to_conflict_for_test(
    pool: Arc<dyn PoolLifecycle>,
    first_operation: MysqlRuntimeGuard,
    second_operation: MysqlRuntimeGuard,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let mut first =
        OwnedMysqlTransaction::begin(Arc::clone(&pool), first_operation, deadline).await?;
    let mut second = OwnedMysqlTransaction::begin(pool, second_operation, deadline).await?;
    let first_key = b"task5-deadlock-a".to_vec();
    let second_key = b"task5-deadlock-b".to_vec();
    first
        .run({
            let first_key = first_key.clone();
            move |connection| {
                Box::pin(connection.exec_drop(
                    "INSERT INTO state_store_kv (key_bytes, value_bytes, version_bytes)
                     VALUES (?, ?, ?)
                     ON DUPLICATE KEY UPDATE value_bytes = VALUES(value_bytes)",
                    (first_key, vec![1], vec![0; 12]),
                ))
            }
        })
        .await?;
    second
        .run({
            let second_key = second_key.clone();
            move |connection| {
                Box::pin(connection.exec_drop(
                    "INSERT INTO state_store_kv (key_bytes, value_bytes, version_bytes)
                     VALUES (?, ?, ?)
                     ON DUPLICATE KEY UPDATE value_bytes = VALUES(value_bytes)",
                    (second_key, vec![2], vec![0; 12]),
                ))
            }
        })
        .await?;

    let first_wait = first.run({
        let second_key = second_key.clone();
        move |connection| {
            Box::pin(connection.exec_drop(
                "UPDATE state_store_kv SET value_bytes = ? WHERE key_bytes = ?",
                (vec![3], second_key),
            ))
        }
    });
    let second_wait = second.run({
        let first_key = first_key.clone();
        move |connection| {
            Box::pin(connection.exec_drop(
                "UPDATE state_store_kv SET value_bytes = ? WHERE key_bytes = ?",
                (vec![4], first_key),
            ))
        }
    });
    let (first_result, second_result) = tokio::join!(first_wait, second_wait);
    let conflicts = [&first_result, &second_result]
        .into_iter()
        .filter(|result| {
            result
                .as_ref()
                .err()
                .is_some_and(|error| error.kind() == StateStoreErrorKind::Conflict)
        })
        .count();
    let successes = [&first_result, &second_result]
        .into_iter()
        .filter(|result| result.is_ok())
        .count();
    let first_rollback = first.rollback().await;
    let second_rollback = second.rollback().await;
    if conflicts != 1 || successes != 1 {
        return Err(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "MySQL deadlock probe did not produce one 1213 conflict and one survivor",
        ));
    }
    first_rollback?;
    second_rollback?;
    Ok(())
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(super) async fn lock_timeout_1205_rolls_back_before_conflict_for_test(
    pool: Arc<dyn PoolLifecycle>,
    first_operation: MysqlRuntimeGuard,
    second_operation: MysqlRuntimeGuard,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let mut holder =
        OwnedMysqlTransaction::begin(Arc::clone(&pool), first_operation, deadline).await?;
    let mut waiter =
        OwnedMysqlTransaction::begin(Arc::clone(&pool), second_operation, deadline).await?;
    waiter
        .run(|connection| {
            Box::pin(connection.query_drop("SET SESSION innodb_lock_wait_timeout = 1"))
        })
        .await?;
    let key = b"task5-lock-timeout".to_vec();
    holder
        .run({
            let key = key.clone();
            move |connection| {
                Box::pin(connection.exec_drop(
                    "INSERT INTO state_store_kv (key_bytes, value_bytes, version_bytes)
                     VALUES (?, ?, ?)
                     ON DUPLICATE KEY UPDATE value_bytes = VALUES(value_bytes)",
                    (key, vec![1], vec![0; 12]),
                ))
            }
        })
        .await?;
    let error = waiter
        .run({
            let key = key.clone();
            move |connection| {
                Box::pin(connection.exec_drop(
                    "INSERT INTO state_store_kv (key_bytes, value_bytes, version_bytes)
                     VALUES (?, ?, ?)
                     ON DUPLICATE KEY UPDATE value_bytes = VALUES(value_bytes)",
                    (key, vec![2], vec![0; 12]),
                ))
            }
        })
        .await
        .expect_err("lock waiter must time out");
    if error.kind() != StateStoreErrorKind::Conflict {
        return Err(error);
    }
    waiter.rollback().await?;
    holder.rollback().await?;

    let connection = checkout_hygienic_connection(pool, deadline).await?;
    let (connection, persisted) = execute_owned_with_deadline(connection, deadline, {
        let key = key.clone();
        move |connection| {
            Box::pin(
                connection.exec_first("SELECT 1 FROM state_store_kv WHERE key_bytes = ?", (key,)),
            )
        }
    })
    .await?;
    let persisted: Option<u8> = persisted.map_err(super::error::MysqlNativeError::into_public)?;
    drop(connection);
    if persisted.is_some() {
        return Err(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "timed out MySQL transaction left a durable row",
        ));
    }
    Ok(())
}
