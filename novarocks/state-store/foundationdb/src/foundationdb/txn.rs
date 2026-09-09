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
use super::commit::{
    PreparedCommit, PreparedFailure, supervise_commit, supervise_pre_dispatch_failure,
};
use super::range::range_page;
use super::{classify_native_read_error, record_provider_error_metric};
use crate::runtime::OperationHandle;
use novarocks_state_store_api::StateStoreMetrics;
use novarocks_state_store_api::{
    CommitOutcome, Key, Precondition, RangePage, RangeRequest, ReadTransaction, StateRecord,
    StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreOperation, StateStoreOutcome,
    TransactionId, Value, VersionToken, WriteTransaction,
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
    metrics: Arc<StateStoreMetrics>,
    _operation: OperationHandle,
}

pub(super) struct FoundationDbWriteTransaction {
    database: Arc<Database>,
    transaction: Option<Transaction>,
    codec: KeyspaceCodec,
    limits: StateStoreLimits,
    deadline: Instant,
    metrics: Arc<StateStoreMetrics>,
    operation: OperationHandle,
    transaction_id: TransactionId,
    mutations: Vec<(Key, Mutation)>,
    pub(super) overlay: BTreeMap<Key, Mutation>,
    budget: TransactionBudget,
    range_frozen: bool,
}

enum CommitPreparation {
    Ready(PreparedCommit),
    DurableFailure(PreparedFailure, CommitOutcome),
    Immediate(CommitOutcome),
}

fn decide_prepare_error(error: StateStoreError) -> CommitOutcome {
    classify_precommit_error(error)
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
        record_result(&self.metrics, StateStoreOperation::Begin, started, &result);
        result
    }

    pub(super) fn begin_write_transaction(
        &self,
        transaction_id: TransactionId,
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
                transaction_id,
                mutations: Vec::new(),
                overlay: BTreeMap::new(),
                budget,
                range_frozen: false,
            })
        })();
        record_result(&self.metrics, StateStoreOperation::Begin, started, &result);
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
        let provisional_version = provisional_version(self.transaction_id, operation);
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

    async fn prepare_commit(mut self) -> CommitPreparation {
        let transaction = match self.transaction.take() {
            Some(transaction) => transaction,
            None => {
                return CommitPreparation::Immediate(CommitOutcome::DefiniteFailure(
                    transaction_finished(),
                ));
            }
        };
        if Instant::now() >= self.deadline {
            let error = deadline_error();
            record_provider_error_metric(self.metrics.as_ref(), &error);
            return CommitPreparation::Immediate(CommitOutcome::DefiniteFailure(error));
        }

        let mut touched = BTreeSet::new();
        touched.extend(self.mutations.iter().map(|(key, _)| key.clone()));
        let mut base = BTreeMap::new();
        for key in &touched {
            let record = match load_record(&transaction, &self.codec, key, self.deadline).await {
                Ok(record) => record,
                Err(error) => {
                    record_provider_error_metric(self.metrics.as_ref(), &error);
                    let outcome = decide_prepare_error(error);
                    drop(transaction);
                    return CommitPreparation::DurableFailure(
                        PreparedFailure {
                            database: self.database,
                            codec: self.codec,
                            limits: self.limits,
                            deadline: self.deadline,
                            metrics: self.metrics,
                            _operation: self.operation,
                            transaction_id: self.transaction_id,
                        },
                        outcome,
                    );
                }
            };
            base.insert(key.clone(), record);
        }

        let changed = match replay_for_commit(&self.mutations, &base) {
            Ok(changed) => changed,
            Err(error) => {
                drop(transaction);
                return CommitPreparation::DurableFailure(
                    PreparedFailure {
                        database: self.database,
                        codec: self.codec,
                        limits: self.limits,
                        deadline: self.deadline,
                        metrics: self.metrics,
                        _operation: self.operation,
                        transaction_id: self.transaction_id,
                    },
                    CommitOutcome::Conflict(error),
                );
            }
        };
        for (key, mutation) in &self.overlay {
            let physical_key = self.codec.record_key(key.as_bytes());
            match mutation {
                Mutation::Put { value, .. } => transaction.set(
                    &physical_key,
                    &self
                        .codec
                        .record_value(*self.transaction_id.as_uuid().as_bytes(), value.as_bytes()),
                ),
                Mutation::Delete { .. } => transaction.clear(&physical_key),
            }
        }

        for (sequence, key) in changed.iter().enumerate() {
            let sequence = match u32::try_from(sequence) {
                Ok(sequence) => sequence,
                Err(_) => {
                    return CommitPreparation::Immediate(CommitOutcome::DefiniteFailure(
                        limit_error("transaction change sequence exceeds FoundationDB range"),
                    ));
                }
            };
            transaction.atomic_op(
                &self.codec.change_key_operand(sequence),
                key.as_bytes(),
                MutationType::SetVersionstampedKey,
            );
        }
        transaction.atomic_op(
            &self.codec.high_watermark_key(),
            &self.codec.high_watermark_operand(),
            MutationType::SetVersionstampedValue,
        );
        transaction.atomic_op(
            &self
                .codec
                .commit_state_key(*self.transaction_id.as_uuid().as_bytes()),
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
            _operation: self.operation,
            transaction_id: self.transaction_id,
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
                version: VersionToken::try_from(Bytes::copy_from_slice(&decoded.transaction_id))?,
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

fn replay_for_commit(
    mutations: &[(Key, Mutation)],
    base: &BTreeMap<Key, Option<StateRecord>>,
) -> Result<BTreeSet<Key>, StateStoreError> {
    let mut state = base.clone();
    let mut changed = BTreeSet::new();
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
        if logical_value(&current) != logical_value(&next) {
            changed.insert(key.clone());
        }
        state.insert(key.clone(), next);
    }
    Ok(changed)
}

fn logical_value(record: &Option<StateRecord>) -> Option<&[u8]> {
    record.as_ref().map(|record| record.value.as_bytes())
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

fn provisional_version(transaction_id: TransactionId, operation: u64) -> VersionToken {
    let bytes = [
        PROVISIONAL_VERSION_TAG,
        transaction_id.as_uuid().as_bytes(),
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
        record_result(&self.metrics, StateStoreOperation::Get, started, &result);
        result
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        let started = StdInstant::now();
        let result = self.range_inner(request).await;
        record_result(&self.metrics, StateStoreOperation::Range, started, &result);
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
        record_result(&self.metrics, StateStoreOperation::Get, started, &result);
        result
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        let started = StdInstant::now();
        let result = self.range_inner(request).await;
        record_result(&self.metrics, StateStoreOperation::Range, started, &result);
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
impl WriteTransaction for FoundationDbWriteTransaction {
    fn transaction_id(&self) -> &TransactionId {
        &self.transaction_id
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        let started = StdInstant::now();
        let result = self.put_inner(key, value, precondition);
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
        let result = self.delete_inner(key, precondition);
        record_result(&self.metrics, StateStoreOperation::Delete, started, &result);
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
            CommitPreparation::DurableFailure(prepared, outcome) => {
                supervise_pre_dispatch_failure(prepared, outcome, started).await
            }
            CommitPreparation::Immediate(outcome) => {
                record_commit(&metrics, started, &outcome);
                outcome
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: &'static [u8]) -> Key {
        Key::try_from(Bytes::from_static(value)).expect("key")
    }

    fn value(value: &'static [u8]) -> Value {
        Value::try_from(Bytes::from_static(value)).expect("value")
    }

    fn put(
        value: &'static [u8],
        precondition: Precondition,
        transaction_id: TransactionId,
        operation: u64,
    ) -> Mutation {
        Mutation::Put {
            value: self::value(value),
            precondition,
            provisional_version: provisional_version(transaction_id, operation),
        }
    }

    #[test]
    fn provisional_versions_are_operation_specific_and_wire_exact() {
        let id = TransactionId::from(uuid::Uuid::from_bytes([0x5a; 16]));
        let first = provisional_version(id, 1);
        let second = provisional_version(id, 2);
        assert_ne!(first, second);
        assert_eq!(
            first.as_bytes(),
            [PROVISIONAL_VERSION_TAG, &[0x5a; 16], &1_u64.to_be_bytes()].concat()
        );
    }

    #[test]
    fn ordered_replay_preserves_intermediate_preconditions() {
        let id = TransactionId::from(uuid::Uuid::from_bytes([0x11; 16]));
        let item = key(b"item");
        let first = provisional_version(id, 1);
        let mutations = vec![
            (item.clone(), put(b"v1", Precondition::Absent, id, 1)),
            (
                item.clone(),
                Mutation::Delete {
                    precondition: Precondition::Version(first),
                },
            ),
            (item.clone(), put(b"v2", Precondition::Absent, id, 3)),
        ];
        let base = BTreeMap::from([(item.clone(), None)]);
        assert_eq!(
            replay_for_commit(&mutations, &base).expect("ordered replay"),
            BTreeSet::from([item])
        );
    }

    #[test]
    fn ordered_replay_rejects_a_hidden_stale_precondition() {
        let id = TransactionId::from(uuid::Uuid::from_bytes([0x22; 16]));
        let item = key(b"item");
        let mutations = vec![
            (item.clone(), put(b"v1", Precondition::Any, id, 1)),
            (item.clone(), put(b"v2", Precondition::Absent, id, 2)),
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
    fn prepare_errors_require_an_authoritative_durable_state_fallback() {
        for kind in [
            StateStoreErrorKind::Corruption,
            StateStoreErrorKind::InvalidRequest,
            StateStoreErrorKind::Internal,
            StateStoreErrorKind::Transient,
            StateStoreErrorKind::ProviderUnavailable,
            StateStoreErrorKind::DeadlineExceeded,
            StateStoreErrorKind::LimitExceeded,
        ] {
            let outcome = decide_prepare_error(StateStoreError::new(kind, "prepare error"));
            assert_eq!(
                matches!(outcome, CommitOutcome::TransientBeforeCommit(_)),
                matches!(
                    kind,
                    StateStoreErrorKind::Transient | StateStoreErrorKind::ProviderUnavailable
                ),
                "unexpected local classification for {kind:?}"
            );
        }
    }
}
