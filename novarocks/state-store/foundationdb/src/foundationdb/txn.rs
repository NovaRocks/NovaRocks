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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant as StdInstant;

use async_trait::async_trait;
use bytes::Bytes;
use foundationdb::options::{MutationType, TransactionOption};
use foundationdb::{Database, FdbError, Transaction};
use tokio::time::{Instant, timeout_at};

use super::FoundationDbStateStore;
use super::budget::TransactionBudget;
use super::codec::KeyspaceCodec;
use super::commit::{PreparedCommit, supervise_commit};
use super::metrics::{ProviderMetrics, ProviderOperation, ProviderOutcome};
use super::range::range_page;
use super::{classify_native_read_error, record_provider_error_metric};
use crate::runtime::OperationHandle;
use novarocks_state_store_api::{
    AttemptId, CommitOutcome, Key, Precondition, RangePage, RangeRequest, ReadTransaction,
    StateRecord, StateStoreError, StateStoreErrorKind, StateStoreLimits, Value, VersionToken,
    WriteAttempt, WriteTransaction,
};

const PROVISIONAL_VERSION_TAG: &[u8] = b"fdb-provisional-v1\0";

#[derive(Clone, Debug)]
pub(super) enum Mutation {
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

pub(super) struct FoundationDbReadTransaction {
    transaction: Option<Transaction>,
    codec: KeyspaceCodec,
    limits: StateStoreLimits,
    deadline: Instant,
    metrics: Arc<ProviderMetrics>,
    _operation: OperationHandle,
}

pub(super) struct FoundationDbWriteTransaction {
    database: Arc<Database>,
    transaction: Option<Transaction>,
    codec: KeyspaceCodec,
    limits: StateStoreLimits,
    deadline: Instant,
    metrics: Arc<ProviderMetrics>,
    operation: OperationHandle,
    /// The one write this transaction is authorised to make.
    ///
    /// It is consumed by [`PreparedCommit`], which is why a prepared commit is
    /// the only thing that may dispatch it. A transaction dropped or aborted
    /// before that point drops the attempt while it is still merely reserved,
    /// and a reserved attempt is provably free of write effect.
    attempt: WriteAttempt,
    mutations: Vec<(Key, Mutation)>,
    pub(super) overlay: BTreeMap<Key, Mutation>,
    budget: TransactionBudget,
    range_frozen: bool,
}

enum CommitPreparation {
    Ready(PreparedCommit),
    /// Nothing was dispatched, so the attempt is closed as effect-free before
    /// the caller is answered.
    UndispatchedFailure(WriteAttempt, CommitOutcome),
}

impl FoundationDbStateStore {
    pub(super) fn begin_read_transaction(
        &self,
    ) -> Result<FoundationDbReadTransaction, StateStoreError> {
        let started = StdInstant::now();
        let result = (|| {
            let operation = self.lease.acquire_operation()?;
            let deadline = Instant::now() + self.limits.transaction_deadline;
            let database = self.lease.database()?;
            let transaction = create_raw_transaction(database.as_ref(), &self.limits, deadline)?;
            Ok(FoundationDbReadTransaction {
                transaction: Some(transaction),
                codec: self.codec.clone(),
                limits: self.limits.clone(),
                deadline,
                metrics: Arc::clone(&self.metrics),
                _operation: operation,
            })
        })();
        record_result(&self.metrics, ProviderOperation::Begin, started, &result);
        result
    }

    pub(super) fn begin_write_transaction(
        &self,
        attempt: WriteAttempt,
    ) -> Result<FoundationDbWriteTransaction, StateStoreError> {
        let started = StdInstant::now();
        let result = (|| {
            let operation = self.lease.acquire_operation()?;
            let budget = TransactionBudget::new(self.limits.clone(), self.codec.root().len())?;
            let deadline = Instant::now() + self.limits.transaction_deadline;
            let database = self.lease.database()?;
            let transaction = create_raw_transaction(database.as_ref(), &self.limits, deadline)?;
            Ok(FoundationDbWriteTransaction {
                database,
                transaction: Some(transaction),
                codec: self.codec.clone(),
                limits: self.limits.clone(),
                deadline,
                metrics: Arc::clone(&self.metrics),
                operation,
                attempt,
                mutations: Vec::new(),
                overlay: BTreeMap::new(),
                budget,
                range_frozen: false,
            })
        })();
        record_result(&self.metrics, ProviderOperation::Begin, started, &result);
        result
    }
}

pub(super) fn create_raw_transaction(
    database: &Database,
    limits: &StateStoreLimits,
    deadline: Instant,
) -> Result<Transaction, StateStoreError> {
    create_raw_transaction_with_observer(database, limits, deadline, |_| {})
}

pub(super) fn create_raw_transaction_with_observer(
    database: &Database,
    limits: &StateStoreLimits,
    deadline: Instant,
    mut observe_native_error: impl FnMut(FdbError),
) -> Result<Transaction, StateStoreError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(deadline_error());
    }
    let timeout_ms = remaining.as_millis().clamp(1, i32::MAX as u128) as i32;
    let size_limit = i32::try_from(limits.max_transaction_bytes)
        .map_err(|_| limit_error("transaction byte limit exceeds FoundationDB range"))?;
    let transaction = database.create_trx().map_err(|error| {
        observe_native_error(error);
        classify_native_read_error(error)
    })?;
    transaction
        .set_option(TransactionOption::Timeout(timeout_ms))
        .map_err(|error| {
            observe_native_error(error);
            classify_native_read_error(error)
        })?;
    transaction
        .set_option(TransactionOption::RetryLimit(0))
        .map_err(|error| {
            observe_native_error(error);
            classify_native_read_error(error)
        })?;
    transaction
        .set_option(TransactionOption::SizeLimit(size_limit))
        .map_err(|error| {
            observe_native_error(error);
            classify_native_read_error(error)
        })?;
    Ok(transaction)
}

impl FoundationDbReadTransaction {
    fn transaction(&self) -> Result<&Transaction, StateStoreError> {
        self.transaction.as_ref().ok_or_else(transaction_finished)
    }

    async fn get_inner(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        validate_key(key, &self.limits)?;
        load_record(self.transaction()?, &self.codec, key, self.deadline).await
    }

    async fn range_inner(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        validate_range(request, &self.limits)?;
        range_page(
            self.transaction()?,
            &self.codec,
            request,
            &BTreeMap::new(),
            self.deadline,
            false,
        )
        .await
    }
}

impl FoundationDbWriteTransaction {
    fn transaction(&self) -> Result<&Transaction, StateStoreError> {
        self.transaction.as_ref().ok_or_else(transaction_finished)
    }

    async fn get_inner(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        validate_key(key, &self.limits)?;
        self.budget.charge_get_conflict(key.as_bytes().len())?;
        let base = load_record(self.transaction()?, &self.codec, key, self.deadline).await?;
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
        self.budget.charge_range_conflict(
            request.range.start.as_bytes().len(),
            request.range.end.as_bytes().len(),
        )?;
        let page = range_page(
            self.transaction()?,
            &self.codec,
            request,
            &self.overlay,
            self.deadline,
            true,
        )
        .await?;
        if page.continuation.is_some() {
            self.range_frozen = true;
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
        let mutation = Mutation::Put {
            value,
            precondition,
            provisional_version,
        };
        let bytes = mutation_logical_bytes(&key, &mutation);
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
        let mutation = Mutation::Delete { precondition };
        let bytes = mutation_logical_bytes(&key, &mutation);
        self.overlay.insert(key.clone(), mutation.clone());
        self.mutations.push((key, mutation));
        Ok(bytes)
    }

    /// Stages every physical mutation this attempt will commit.
    ///
    /// Everything here is client-side buffering and snapshot reads: nothing
    /// leaves a trace a later reader could see, which is why the attempt is
    /// still merely reserved when this returns and why dispatch is recorded by
    /// the commit supervisor rather than here.
    async fn prepare_commit(mut self) -> CommitPreparation {
        let transaction = match self.transaction.take() {
            Some(transaction) => transaction,
            None => {
                return CommitPreparation::UndispatchedFailure(
                    self.attempt,
                    CommitOutcome::DefiniteFailure(transaction_finished()),
                );
            }
        };
        if Instant::now() >= self.deadline {
            let error = deadline_error();
            record_provider_error_metric(self.metrics.as_ref(), &error);
            return CommitPreparation::UndispatchedFailure(
                self.attempt,
                CommitOutcome::DefiniteFailure(error),
            );
        }

        let mut touched = BTreeSet::new();
        touched.extend(self.mutations.iter().map(|(key, _)| key.clone()));
        let mut base = BTreeMap::new();
        for key in &touched {
            let record = match load_record(&transaction, &self.codec, key, self.deadline).await {
                Ok(record) => record,
                Err(error) => {
                    record_provider_error_metric(self.metrics.as_ref(), &error);
                    let outcome = classify_precommit_error(error);
                    drop(transaction);
                    return CommitPreparation::UndispatchedFailure(self.attempt, outcome);
                }
            };
            base.insert(key.clone(), record);
        }

        if let Err(error) = replay_for_commit(&self.mutations, &base) {
            drop(transaction);
            return CommitPreparation::UndispatchedFailure(
                self.attempt,
                CommitOutcome::Conflict(error),
            );
        }

        let attempt_tag = self.codec.attempt_tag(self.attempt.id());
        for (key, mutation) in &self.overlay {
            let physical_key = self.codec.record_key(key.as_bytes());
            match mutation {
                Mutation::Put { value, .. } => transaction.set(
                    &physical_key,
                    &self.codec.record_value(attempt_tag, value.as_bytes()),
                ),
                Mutation::Delete { .. } => transaction.clear(&physical_key),
            }
        }

        // The one piece of evidence this provider keeps, staged inside the data
        // transaction itself. FoundationDB publishes it atomically with the rows
        // above, so the key's presence *is* the proof that this attempt
        // committed, and nothing outside a real commit can create it.
        transaction.atomic_op(
            &self.codec.commit_state_key(self.attempt.id()),
            &self.codec.committed_value_operand(),
            MutationType::SetVersionstampedValue,
        );
        CommitPreparation::Ready(PreparedCommit {
            database: self.database,
            transaction,
            codec: self.codec,
            limits: self.limits,
            deadline: self.deadline,
            metrics: self.metrics,
            operation: self.operation,
            attempt: self.attempt,
        })
    }
}

async fn load_record(
    transaction: &Transaction,
    codec: &KeyspaceCodec,
    key: &Key,
    deadline: Instant,
) -> Result<Option<StateRecord>, StateStoreError> {
    ensure_active(deadline)?;
    let physical_key = codec.record_key(key.as_bytes());
    let value = timeout_at(deadline, transaction.get(&physical_key, false))
        .await
        .map_err(|_| deadline_error())?
        .map_err(classify_native_read_error)?;
    ensure_active(deadline)?;
    value
        .map(|value| {
            let decoded = codec.decode_record_value(value.as_ref())?;
            Ok(StateRecord {
                key: key.clone(),
                value: Value::try_from(Bytes::from(decoded.payload))?,
                version: VersionToken::try_from(Bytes::copy_from_slice(&decoded.attempt_tag))?,
            })
        })
        .transpose()
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

/// Re-checks every staged precondition against the state its own predecessors
/// would have produced.
///
/// A transaction may touch one key repeatedly, so a precondition has to be
/// judged against the value the earlier mutations in *this* transaction left,
/// not against the base snapshot. There is no longer a change list to derive:
/// the feed that consumed it is gone, so this only answers whether the ordered
/// replay is admissible.
fn replay_for_commit(
    mutations: &[(Key, Mutation)],
    base: &BTreeMap<Key, Option<StateRecord>>,
) -> Result<(), StateStoreError> {
    let mut state = base.clone();
    for (key, mutation) in mutations {
        let current = state.get(key).cloned().flatten();
        if !precondition_matches(mutation.precondition(), current.as_ref()) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::PreconditionFailed,
                "FoundationDB transaction precondition failed",
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
    Ok(())
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

/// Names a value that exists only inside one uncommitted transaction.
///
/// It is tagged so it can never be confused with a persisted version, which is
/// the attempt tag alone; a caller that stored one and presented it later would
/// be presenting a version this keyspace never published.
fn provisional_version(attempt: AttemptId, operation: u64) -> VersionToken {
    let bytes = [
        PROVISIONAL_VERSION_TAG,
        attempt.scope().to_string().as_bytes(),
        b"\0",
        &attempt.sequence().to_be_bytes(),
        &operation.to_be_bytes(),
    ]
    .concat();
    VersionToken::try_from(Bytes::from(bytes)).expect("provisional version is non-empty")
}

fn validate_key(key: &Key, limits: &StateStoreLimits) -> Result<(), StateStoreError> {
    validate_key_value(key, None, limits)
}

fn validate_key_value(
    key: &Key,
    value: Option<&Value>,
    limits: &StateStoreLimits,
) -> Result<(), StateStoreError> {
    if key.as_bytes().len() > limits.max_key_bytes {
        return Err(limit_error("key exceeds the configured byte limit"));
    }
    if value.is_some_and(|value| value.as_bytes().len() > limits.max_value_bytes) {
        return Err(limit_error("value exceeds the configured byte limit"));
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

fn mutation_logical_bytes(key: &Key, mutation: &Mutation) -> usize {
    key.as_bytes().len().saturating_add(match mutation {
        Mutation::Put { value, .. } => value.as_bytes().len(),
        Mutation::Delete { .. } => 0,
    })
}

fn classify_precommit_error(error: StateStoreError) -> CommitOutcome {
    match error.kind() {
        StateStoreErrorKind::Transient | StateStoreErrorKind::ProviderUnavailable => {
            CommitOutcome::TransientBeforeCommit(error)
        }
        _ => CommitOutcome::DefiniteFailure(error),
    }
}

fn ensure_active(deadline: Instant) -> Result<(), StateStoreError> {
    if Instant::now() >= deadline {
        return Err(deadline_error());
    }
    Ok(())
}

fn record_result<T>(
    metrics: &ProviderMetrics,
    operation: ProviderOperation,
    started: StdInstant,
    result: &Result<T, StateStoreError>,
) {
    metrics.record_operation(
        operation,
        if result.is_ok() {
            ProviderOutcome::Success
        } else {
            ProviderOutcome::Error
        },
        started.elapsed(),
    );
}

fn record_commit(metrics: &ProviderMetrics, started: StdInstant, outcome: &CommitOutcome) {
    let metric = match outcome {
        CommitOutcome::Committed(_) => ProviderOutcome::Success,
        CommitOutcome::Conflict(_) => ProviderOutcome::Conflict,
        CommitOutcome::TransientBeforeCommit(_) => ProviderOutcome::TransientBeforeCommit,
        CommitOutcome::DefiniteFailure(_) => ProviderOutcome::DefiniteFailure,
        CommitOutcome::CommitUnknown(_) => ProviderOutcome::CommitUnknown,
    };
    metrics.record_operation(ProviderOperation::Commit, metric, started.elapsed());
}

fn writes_frozen() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "writes are frozen after paginated range reads",
    )
}

fn transaction_finished() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "FoundationDB transaction is already finished",
    )
}

fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "FoundationDB state transaction deadline exceeded",
    )
}

fn limit_error(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::LimitExceeded, message)
}

#[async_trait]
impl ReadTransaction for FoundationDbReadTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        let started = StdInstant::now();
        let result = self.get_inner(key).await;
        record_result(&self.metrics, ProviderOperation::Get, started, &result);
        result
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        let started = StdInstant::now();
        let result = self.range_inner(request).await;
        record_result(&self.metrics, ProviderOperation::Range, started, &result);
        if let Ok(page) = &result {
            self.metrics.record_page_records(page.records.len() as u64);
        }
        result
    }

    async fn abort(mut self: Box<Self>) -> Result<(), StateStoreError> {
        self.transaction.take().ok_or_else(transaction_finished)?;
        Ok(())
    }
}

#[async_trait]
impl ReadTransaction for FoundationDbWriteTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        let started = StdInstant::now();
        let result = self.get_inner(key).await;
        record_result(&self.metrics, ProviderOperation::Get, started, &result);
        result
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        let started = StdInstant::now();
        let result = self.range_inner(request).await;
        record_result(&self.metrics, ProviderOperation::Range, started, &result);
        if let Ok(page) = &result {
            self.metrics.record_page_records(page.records.len() as u64);
        }
        result
    }

    async fn abort(mut self: Box<Self>) -> Result<(), StateStoreError> {
        let finished = self.transaction.take().ok_or_else(transaction_finished);
        // An abort stages nothing and dispatches nothing, so the attempt is
        // closed as provably effect-free rather than left to be decided from
        // evidence there is none of. Simply dropping it would say the same
        // thing, but saying it here is what makes the intent checkable.
        close_undispatched(&self.attempt);
        finished?;
        Ok(())
    }
}

#[async_trait]
impl WriteTransaction for FoundationDbWriteTransaction {
    fn attempt(&self) -> AttemptId {
        self.attempt.id()
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        let started = StdInstant::now();
        let result = self.put_inner(key, value, precondition);
        record_result(&self.metrics, ProviderOperation::Put, started, &result);
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
        let result = self.delete_inner(key, precondition);
        record_result(&self.metrics, ProviderOperation::Delete, started, &result);
        if let Ok(bytes) = result {
            self.metrics
                .record_bytes_written(u64::try_from(bytes).unwrap_or(u64::MAX));
            Ok(())
        } else {
            result.map(|_| ())
        }
    }

    async fn commit(self: Box<Self>) -> CommitOutcome {
        let metrics = Arc::clone(&self.metrics);
        let started = StdInstant::now();
        match (*self).prepare_commit().await {
            CommitPreparation::Ready(prepared) => supervise_commit(prepared, started).await,
            CommitPreparation::UndispatchedFailure(attempt, outcome) => {
                close_undispatched(&attempt);
                record_commit(&metrics, started, &outcome);
                outcome
            }
        }
    }
}

/// Records that an attempt never reached storage.
///
/// The attempt would answer the same way on being dropped while reserved, so a
/// failure here is a diagnostic rather than a correctness problem -- but it does
/// mean the provider's own state machine disagrees with the supervisor's, which
/// is worth a log line.
fn close_undispatched(attempt: &WriteAttempt) {
    if let Err(error) = attempt.cancel_before_dispatch() {
        tracing::warn!(
            provider = "foundationdb",
            attempt = %attempt.id(),
            error_kind = ?error.kind(),
            "FoundationDB could not close an undispatched write attempt"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ATTEMPT_TAG_BYTES;
    use crate::codec::tests_support::attempt_id;

    fn key(value: &'static [u8]) -> Key {
        Key::try_from(Bytes::from_static(value)).expect("key")
    }

    fn value(value: &'static [u8]) -> Value {
        Value::try_from(Bytes::from_static(value)).expect("value")
    }

    fn put(
        value: &'static [u8],
        precondition: Precondition,
        attempt: AttemptId,
        operation: u64,
    ) -> Mutation {
        Mutation::Put {
            value: self::value(value),
            precondition,
            provisional_version: provisional_version(attempt, operation),
        }
    }

    #[test]
    fn provisional_versions_are_operation_specific_and_never_a_persisted_version() {
        let attempt = attempt_id(1);
        let first = provisional_version(attempt, 1);
        let second = provisional_version(attempt, 2);
        assert_ne!(first, second);
        assert!(first.as_bytes().starts_with(PROVISIONAL_VERSION_TAG));
        // A persisted version is the bare attempt tag, so the two encodings can
        // never be mistaken for one another.
        assert_eq!(
            first.as_bytes(),
            [
                PROVISIONAL_VERSION_TAG,
                attempt.scope().to_string().as_bytes(),
                b"\0",
                &attempt.sequence().to_be_bytes(),
                &1_u64.to_be_bytes(),
            ]
            .concat()
        );
        assert_ne!(first.as_bytes().len(), ATTEMPT_TAG_BYTES);
    }

    #[test]
    fn two_attempts_never_share_a_provisional_version() {
        assert_ne!(
            provisional_version(attempt_id(1), 1),
            provisional_version(attempt_id(1), 1),
            "two instances at the same sequence are still two attempts"
        );
    }

    #[test]
    fn ordered_replay_preserves_intermediate_preconditions() {
        let attempt = attempt_id(1);
        let item = key(b"item");
        let first = provisional_version(attempt, 1);
        let mutations = vec![
            (item.clone(), put(b"v1", Precondition::Absent, attempt, 1)),
            (
                item.clone(),
                Mutation::Delete {
                    precondition: Precondition::Version(first),
                },
            ),
            (item.clone(), put(b"v2", Precondition::Absent, attempt, 3)),
        ];
        let base = BTreeMap::from([(item, None)]);
        replay_for_commit(&mutations, &base).expect("ordered replay");
    }

    #[test]
    fn ordered_replay_rejects_a_hidden_stale_precondition() {
        let attempt = attempt_id(1);
        let item = key(b"item");
        let mutations = vec![
            (item.clone(), put(b"v1", Precondition::Any, attempt, 1)),
            (item.clone(), put(b"v2", Precondition::Absent, attempt, 2)),
        ];
        let base = BTreeMap::from([(item, None)]);
        assert_eq!(
            replay_for_commit(&mutations, &base)
                .expect_err("second precondition observes first mutation")
                .kind(),
            StateStoreErrorKind::PreconditionFailed
        );
    }

    #[test]
    fn precommit_errors_retry_only_transient_provider_failures() {
        let cases = [
            (StateStoreErrorKind::Corruption, false),
            (StateStoreErrorKind::InvalidRequest, false),
            (StateStoreErrorKind::Internal, false),
            (StateStoreErrorKind::Transient, true),
            (StateStoreErrorKind::ProviderUnavailable, true),
            (StateStoreErrorKind::DeadlineExceeded, false),
            (StateStoreErrorKind::LimitExceeded, false),
        ];

        for (kind, expect_transient) in cases {
            let outcome = classify_precommit_error(StateStoreError::new(
                kind,
                "classified precommit test error",
            ));
            assert_eq!(
                matches!(outcome, CommitOutcome::TransientBeforeCommit(_)),
                expect_transient,
                "unexpected classification for {kind:?}"
            );
        }
    }

    #[test]
    fn no_preparation_failure_is_ever_classified_as_ambiguous() {
        // Preparation happens strictly before the native commit, so the write
        // provably did not land. Reporting `CommitUnknown` here would put an
        // attempt in doubt that this provider can prove nothing was done for.
        for kind in [
            StateStoreErrorKind::Corruption,
            StateStoreErrorKind::InvalidRequest,
            StateStoreErrorKind::Internal,
            StateStoreErrorKind::Transient,
            StateStoreErrorKind::ProviderUnavailable,
            StateStoreErrorKind::DeadlineExceeded,
            StateStoreErrorKind::LimitExceeded,
        ] {
            let outcome = classify_precommit_error(StateStoreError::new(kind, "prepare error"));
            assert!(
                !matches!(outcome, CommitOutcome::CommitUnknown(_)),
                "an undispatched failure is never ambiguous, but {kind:?} was"
            );
        }
    }
}
