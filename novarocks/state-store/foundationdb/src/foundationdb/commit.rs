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

//! Commit supervision and in-doubt evidence.
//!
//! # What the evidence is
//!
//! One key per attempt, written by the data transaction itself through a
//! versionstamped mutation. FoundationDB publishes that mutation atomically
//! with the rows the transaction changed, so the key's presence *is* the proof
//! that the transaction committed and its value *is* that commit's revision.
//! Nothing else writes it: there is no pending marker and no tombstone.
//!
//! That asymmetry is the whole reason `NotCommitted` is a proof obligation
//! here. An absent key means only "no proof of a commit", which a transaction
//! still in flight and a transaction that never ran produce alike, so absence
//! adjudicates to [`AttemptOutcome::Unresolved`]. A denial is published only
//! when FoundationDB itself said the transaction was not committed.
//!
//! # What is not released
//!
//! A witnessed commit releases its own key immediately, which is what keeps
//! the keyspace bounded under ordinary traffic.
//!
//! Two cases do not release, and both are worth naming rather than hiding:
//!
//! * An attempt abandoned while dispatched is released only when a host drives
//!   `AttemptSupervisor::drain_abandoned_attempts`, which the frontend drives
//!   on a cadence and once more at shutdown. Until a sweep runs, such an
//!   attempt keeps both its key and its capacity slot.
//! * An attempt whose caller was told `CommitUnknown` and which
//!   `CommitObservation::outcome` then decided *from* the key has no release
//!   hook at all. The supervisor publishes that decision itself and never
//!   tells the provider the key is spent, so it stays.
//!
//! Those keys all sit under `root || 0x03 || <instance tag>` and belong to one
//! dead open once its process is gone.

use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

use bytes::Bytes;
use foundationdb::{Database, FdbError, Transaction};
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout_at};

use super::codec::{KeyspaceCodec, REVISION_BYTES};
use super::metrics::{ProviderMetrics, ProviderOperation, ProviderOutcome};
use super::txn::create_raw_transaction_with_observer;
use crate::runtime::{OperationHandle, ProviderHandle};
use novarocks_state_store_api::{
    AttemptId, AttemptOutcome, CommitOutcome, CommitReceipt, InDoubtAdjudicator, StateStoreError,
    StateStoreErrorKind, StateStoreLimits, StoreRevision, WriteAttempt,
};

const AUXILIARY_MAX_ATTEMPTS: usize = 5;
const AUXILIARY_DEADLINE: Duration = Duration::from_secs(4);
const NOT_COMMITTED_ERROR_CODE: i32 = 1020;
const DETERMINISTIC_COMMIT_ERROR_CODES: &[i32] = &[
    2000, // client_invalid_operation (including malformed versionstamp operands)
    2002, // commit_read_incomplete
    2004, // key_outside_legal_range
    2006, // invalid_option_value
    2007, // invalid_option
    2018, // invalid_mutation_type
    2020, // transaction_invalid_version
    2023, // transaction_read_only
    2101, // transaction_too_large
    2102, // key_too_large
    2103, // value_too_large
    2108, // unsupported_operation
    2109, // too_many_tags
    2110, // tag_too_long
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeCommitDisposition {
    ConflictNotCommitted,
    RetryableNotCommitted,
    DefiniteNotCommitted,
    Unknown,
}

impl NativeCommitDisposition {
    const fn category(self) -> &'static str {
        match self {
            Self::ConflictNotCommitted => "conflict_not_committed",
            Self::RetryableNotCommitted => "retryable_not_committed",
            Self::DefiniteNotCommitted => "definite_not_committed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitStatePhase {
    DataCommit,
    Versionstamp,
    Adjudication,
    EvidenceRelease,
}

impl CommitStatePhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::DataCommit => "data_commit",
            Self::Versionstamp => "versionstamp",
            Self::Adjudication => "adjudication",
            Self::EvidenceRelease => "evidence_release",
        }
    }
}

struct NativeErrorLogFields {
    attempt: String,
    phase: &'static str,
    native_error_code: i32,
    category: &'static str,
}

fn native_error_log_fields(
    attempt: AttemptId,
    phase: CommitStatePhase,
    error: FdbError,
    disposition: NativeCommitDisposition,
) -> NativeErrorLogFields {
    NativeErrorLogFields {
        attempt: attempt.to_string(),
        phase: phase.as_str(),
        native_error_code: error.code(),
        category: disposition.category(),
    }
}

fn log_native_error(
    attempt: AttemptId,
    phase: CommitStatePhase,
    error: FdbError,
    disposition: NativeCommitDisposition,
) {
    let fields = native_error_log_fields(attempt, phase, error, disposition);
    tracing::warn!(
        attempt = %fields.attempt,
        phase = fields.phase,
        native_error_code = fields.native_error_code,
        category = fields.category,
        "FoundationDB commit-state native error"
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuxiliaryMetricEvent {
    Attempt,
    Deadline,
    BlockingFailure,
}

#[derive(Debug)]
struct AuxiliaryAttemptBudget {
    attempts: usize,
}

impl AuxiliaryAttemptBudget {
    fn new() -> Self {
        Self { attempts: 0 }
    }

    fn try_consume(&mut self) -> bool {
        if self.attempts == AUXILIARY_MAX_ATTEMPTS {
            return false;
        }
        self.attempts += 1;
        true
    }

    fn has_remaining(&self) -> bool {
        self.attempts < AUXILIARY_MAX_ATTEMPTS
    }
}

fn record_auxiliary_metric(metrics: &ProviderMetrics, event: AuxiliaryMetricEvent) {
    match event {
        AuxiliaryMetricEvent::Attempt => metrics.record_retry(),
        AuxiliaryMetricEvent::Deadline => metrics.record_deadline(),
        AuxiliaryMetricEvent::BlockingFailure => metrics.record_blocking_failure(),
    }
}

fn begin_auxiliary_attempt(
    budget: &mut AuxiliaryAttemptBudget,
    deadline: Instant,
    metrics: &ProviderMetrics,
) -> Result<(), StateStoreError> {
    if Instant::now() >= deadline {
        record_auxiliary_metric(metrics, AuxiliaryMetricEvent::Deadline);
        return Err(deadline_error());
    }
    if !budget.try_consume() {
        return Err(auxiliary_attempts_exhausted());
    }
    record_auxiliary_metric(metrics, AuxiliaryMetricEvent::Attempt);
    Ok(())
}

fn record_auxiliary_error(metrics: &ProviderMetrics, error: &StateStoreError) {
    match error.kind() {
        StateStoreErrorKind::DeadlineExceeded => {
            record_auxiliary_metric(metrics, AuxiliaryMetricEvent::Deadline);
        }
        StateStoreErrorKind::Transient | StateStoreErrorKind::ProviderUnavailable => {
            record_auxiliary_metric(metrics, AuxiliaryMetricEvent::BlockingFailure);
        }
        _ => {}
    }
}

fn is_retryable_auxiliary_error(error: &StateStoreError) -> bool {
    matches!(
        error.kind(),
        StateStoreErrorKind::Transient
            | StateStoreErrorKind::ProviderUnavailable
            | StateStoreErrorKind::DeadlineExceeded
    )
}

fn classify_native_commit_error(error: FdbError) -> NativeCommitDisposition {
    if error.code() == NOT_COMMITTED_ERROR_CODE {
        NativeCommitDisposition::ConflictNotCommitted
    } else if error.is_retryable_not_committed() {
        NativeCommitDisposition::RetryableNotCommitted
    } else if DETERMINISTIC_COMMIT_ERROR_CODES.contains(&error.code()) {
        NativeCommitDisposition::DefiniteNotCommitted
    } else {
        NativeCommitDisposition::Unknown
    }
}

fn should_retry_auxiliary_commit(error: FdbError) -> bool {
    error.code() == NOT_COMMITTED_ERROR_CODE
        || error.code() == 1031
        || error.is_retryable_not_committed()
        || error.is_maybe_committed()
}

fn classify_auxiliary_native_error(error: FdbError) -> StateStoreError {
    if error.code() == 1031 {
        deadline_error()
    } else if DETERMINISTIC_COMMIT_ERROR_CODES.contains(&error.code()) {
        deterministic_commit_error(error.code())
    } else if error.is_retryable() || error.is_maybe_committed() {
        provider_error()
    } else {
        StateStoreError::new(
            StateStoreErrorKind::Internal,
            "FoundationDB auxiliary transaction returned an unclassified native error",
        )
    }
}

fn should_record_native_blocking_failure(disposition: NativeCommitDisposition) -> bool {
    matches!(
        disposition,
        NativeCommitDisposition::RetryableNotCommitted | NativeCommitDisposition::Unknown
    )
}

fn native_error_metric_event(
    error: FdbError,
    disposition: NativeCommitDisposition,
) -> Option<AuxiliaryMetricEvent> {
    if error.code() == 1031 {
        Some(AuxiliaryMetricEvent::Deadline)
    } else if should_record_native_blocking_failure(disposition) {
        Some(AuxiliaryMetricEvent::BlockingFailure)
    } else {
        None
    }
}

fn record_native_error(
    metrics: &ProviderMetrics,
    attempt: AttemptId,
    phase: CommitStatePhase,
    error: FdbError,
    disposition: NativeCommitDisposition,
) {
    log_native_error(attempt, phase, error, disposition);
    if let Some(event) = native_error_metric_event(error, disposition) {
        record_auxiliary_metric(metrics, event);
    }
}

/// Maps a witnessed commit outcome onto the verdict it proves, if any.
///
/// The three failure classifications are all statements that FoundationDB
/// refused the transaction, so the write can never land. `CommitUnknown` is not
/// a verdict and must publish nothing: the commit-state key is the only thing
/// that can still decide the attempt, and publishing a denial here would be a
/// guess dressed as proof.
fn witnessed_verdict(outcome: &CommitOutcome) -> Option<AttemptOutcome> {
    match outcome {
        CommitOutcome::Committed(receipt) => Some(AttemptOutcome::Committed(receipt.clone())),
        CommitOutcome::Conflict(_)
        | CommitOutcome::TransientBeforeCommit(_)
        | CommitOutcome::DefiniteFailure(_) => Some(AttemptOutcome::NotCommitted),
        CommitOutcome::CommitUnknown(_) => None,
    }
}

pub(super) struct PreparedCommit {
    pub database: Arc<Database>,
    pub transaction: Transaction,
    pub codec: KeyspaceCodec,
    pub limits: StateStoreLimits,
    pub deadline: Instant,
    pub metrics: Arc<ProviderMetrics>,
    pub operation: OperationHandle,
    pub attempt: WriteAttempt,
}

/// Runs one prepared commit under an owner the caller does not control.
///
/// The owner is a spawned task on purpose: a cancelled caller must not cancel a
/// native commit that is already in flight, and it is the owner -- not the
/// caller -- that witnesses the outcome and publishes the attempt's verdict.
pub(super) async fn supervise_commit(
    prepared: PreparedCommit,
    started: StdInstant,
) -> CommitOutcome {
    // Dispatch is recorded before the native commit can leave any trace a later
    // reader might see. Until this call lands, an unwitnessed attempt answers
    // "not committed" on the strength of "nothing reached storage"; after it,
    // a dropped handle is an abandoned attempt rather than a free slot.
    if let Err(error) = prepared.attempt.mark_dispatched() {
        return CommitOutcome::DefiniteFailure(error);
    }
    let metrics = Arc::clone(&prepared.metrics);
    #[cfg(feature = "state-store-test-hooks")]
    let waiter_drop_guard = super::test_support::arm_commit_waiter_drop_guard();
    let (sender, receiver) = oneshot::channel();
    tokio::spawn(async move {
        let outcome = run_commit_owner(prepared).await;
        record_commit(&metrics, started, &outcome);
        let _ = sender.send(outcome);
    });
    let outcome = receiver.await.unwrap_or_else(|_| {
        CommitOutcome::CommitUnknown(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "FoundationDB commit supervisor stopped before reporting an outcome",
        ))
    });
    #[cfg(feature = "state-store-test-hooks")]
    if let Some(waiter_drop_guard) = waiter_drop_guard {
        waiter_drop_guard.complete();
    }
    outcome
}

async fn run_commit_owner(prepared: PreparedCommit) -> CommitOutcome {
    let PreparedCommit {
        database,
        transaction,
        codec,
        limits,
        deadline,
        metrics,
        operation,
        attempt,
    } = prepared;

    #[cfg(feature = "state-store-test-hooks")]
    let gates = super::test_support::take_commit_gates();
    #[cfg(feature = "state-store-test-hooks")]
    if let Some(gates) = gates.as_ref() {
        gates.before_native_commit().await;
    }

    let versionstamp = transaction.get_versionstamp();
    let native_result = timeout_at(deadline, transaction.commit()).await;
    let outcome = match native_result {
        Ok(Ok(committed_transaction)) => {
            drop(committed_transaction);
            match timeout_at(deadline, versionstamp).await {
                Ok(Ok(versionstamp)) => match revision_from_bytes(versionstamp.as_ref()) {
                    Ok(revision) => CommitOutcome::Committed(CommitReceipt {
                        attempt: attempt.id(),
                        revision,
                    }),
                    // The write landed; only its revision is unreadable here.
                    // The commit-state key carries the same versionstamp, so
                    // the attempt is still decidable from evidence.
                    Err(error) => CommitOutcome::CommitUnknown(error),
                },
                Ok(Err(error)) => {
                    record_native_error(
                        metrics.as_ref(),
                        attempt.id(),
                        CommitStatePhase::Versionstamp,
                        error,
                        classify_native_commit_error(error),
                    );
                    CommitOutcome::CommitUnknown(provider_unknown())
                }
                Err(_) => {
                    record_auxiliary_metric(metrics.as_ref(), AuxiliaryMetricEvent::Deadline);
                    CommitOutcome::CommitUnknown(deadline_unknown())
                }
            }
        }
        Ok(Err(error)) => {
            let error = *error;
            let disposition = classify_native_commit_error(error);
            record_native_error(
                metrics.as_ref(),
                attempt.id(),
                CommitStatePhase::DataCommit,
                error,
                disposition,
            );
            match disposition {
                // FoundationDB stated the transaction was not committed. That
                // is proof, and it needs no durable tombstone: the commit-state
                // key would only have been written by a commit that happened.
                NativeCommitDisposition::ConflictNotCommitted => {
                    CommitOutcome::Conflict(conflict_error())
                }
                NativeCommitDisposition::DefiniteNotCommitted => {
                    CommitOutcome::DefiniteFailure(deterministic_commit_error(error.code()))
                }
                NativeCommitDisposition::RetryableNotCommitted => {
                    CommitOutcome::TransientBeforeCommit(provider_transient())
                }
                // `commit_unknown_result` and its relatives. FoundationDB does
                // not know, so neither do we, and no terminal is published.
                NativeCommitDisposition::Unknown => {
                    CommitOutcome::CommitUnknown(provider_unknown())
                }
            }
        }
        Err(_) => {
            record_auxiliary_metric(metrics.as_ref(), AuxiliaryMetricEvent::Deadline);
            CommitOutcome::CommitUnknown(deadline_unknown())
        }
    };

    #[cfg(feature = "state-store-test-hooks")]
    let outcome = match gates {
        Some(gates) => gates.before_response(outcome).await,
        None => outcome,
    };

    publish_and_release(
        &attempt,
        &outcome,
        database.as_ref(),
        &codec,
        &limits,
        metrics.as_ref(),
    )
    .await;
    drop(operation);
    outcome
}

/// Publishes the terminal this owner witnessed, then drops the evidence that
/// terminal rests on.
///
/// Order is the contract: the proof is recorded first, so a later reader finds
/// a published verdict instead of re-deriving one from evidence on its way out.
/// Releasing here rather than leaving it to
/// `AttemptSupervisor::drain_abandoned_attempts` is what keeps the keyspace
/// bounded -- abandonment is the exception, and a provider that only cleaned up
/// on abandonment would keep one commit-state key per successful write forever.
async fn publish_and_release(
    attempt: &WriteAttempt,
    outcome: &CommitOutcome,
    database: &Database,
    codec: &KeyspaceCodec,
    limits: &StateStoreLimits,
    metrics: &ProviderMetrics,
) {
    let Some(verdict) = witnessed_verdict(outcome) else {
        return;
    };
    let proven_committed = matches!(verdict, AttemptOutcome::Committed(_));
    if let Err(error) = attempt.settle(verdict) {
        tracing::error!(
            attempt = %attempt.id(),
            error_kind = ?error.kind(),
            "FoundationDB commit outcome could not be published on its attempt"
        );
        return;
    }
    if !proven_committed {
        // The commit-state key is staged inside the data transaction, so a
        // transaction FoundationDB proved uncommitted left no key behind.
        return;
    }
    if let Err(error) = clear_commit_evidence(database, codec, limits, attempt.id(), metrics).await
    {
        tracing::warn!(
            attempt = %attempt.id(),
            error_kind = ?error.kind(),
            "FoundationDB commit evidence was not released; one commit-state key is retained"
        );
    }
}

/// The provider's in-doubt callback.
///
/// It is a separate object because [`novarocks_state_store_api::AttemptSupervisor`]
/// needs an adjudicator at construction while the store needs the supervisor,
/// so both share the opened instance's lease rather than one owning the other.
pub(super) struct FoundationDbEvidence {
    lease: Arc<ProviderHandle>,
    codec: KeyspaceCodec,
    limits: StateStoreLimits,
    metrics: Arc<ProviderMetrics>,
}

impl FoundationDbEvidence {
    pub(super) fn new(
        lease: Arc<ProviderHandle>,
        codec: KeyspaceCodec,
        limits: StateStoreLimits,
        metrics: Arc<ProviderMetrics>,
    ) -> Self {
        Self {
            lease,
            codec,
            limits,
            metrics,
        }
    }
}

#[async_trait::async_trait]
impl InDoubtAdjudicator for FoundationDbEvidence {
    async fn adjudicate(&self, attempt: AttemptId) -> Result<AttemptOutcome, StateStoreError> {
        // A read that cannot even be started is not proof that nothing
        // happened, so a draining or unavailable runtime answers "undecided"
        // rather than "denied".
        let Ok(_operation) = self.lease.acquire_operation() else {
            return Ok(AttemptOutcome::Unresolved);
        };
        let Ok(database) = self.lease.database() else {
            return Ok(AttemptOutcome::Unresolved);
        };
        match load_commit_evidence(
            database.as_ref(),
            &self.codec,
            &self.limits,
            attempt,
            self.metrics.as_ref(),
        )
        .await
        {
            Ok(state) => evidence_verdict(attempt, state),
            // A read this provider could not complete says nothing either, but
            // it is not the same event as an absent key and must not vanish
            // from the record just because both answer "undecided".
            Err(error) => {
                tracing::warn!(
                    attempt = %attempt,
                    error_kind = ?error.kind(),
                    "FoundationDB could not read commit evidence; the attempt stays unresolved"
                );
                Ok(AttemptOutcome::Unresolved)
            }
        }
    }

    async fn release_evidence(&self, attempt: AttemptId) -> Result<(), StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        let database = self.lease.database()?;
        clear_commit_evidence(
            database.as_ref(),
            &self.codec,
            &self.limits,
            attempt,
            self.metrics.as_ref(),
        )
        .await
    }
}

/// Turns a successful commit-state read into the verdict it proves.
///
/// This is the whole of the provider's in-doubt rule, and it has exactly two
/// answers because the keyspace only ever holds one kind of fact. A present key
/// was written by the data transaction itself, so it proves that transaction
/// committed. An absent key proves nothing: a commit still in flight, a commit
/// that never started, and a commit whose evidence was already released all
/// read the same way, so the honest answer is that the attempt is undecided.
/// There is deliberately no path to `NotCommitted` here -- a denial comes only
/// from FoundationDB refusing the transaction in front of a witness.
fn evidence_verdict(
    attempt: AttemptId,
    state: Option<[u8; REVISION_BYTES]>,
) -> Result<AttemptOutcome, StateStoreError> {
    match state {
        Some(revision) => Ok(AttemptOutcome::Committed(receipt(attempt, revision)?)),
        None => Ok(AttemptOutcome::Unresolved),
    }
}

async fn load_commit_evidence(
    database: &Database,
    codec: &KeyspaceCodec,
    limits: &StateStoreLimits,
    attempt: AttemptId,
    metrics: &ProviderMetrics,
) -> Result<Option<[u8; REVISION_BYTES]>, StateStoreError> {
    let deadline = Instant::now() + limits.transaction_deadline.min(AUXILIARY_DEADLINE);
    let mut budget = AuxiliaryAttemptBudget::new();
    let state_key = codec.commit_state_key(attempt);
    loop {
        let transaction = create_auxiliary_transaction(
            database,
            limits,
            deadline,
            &mut budget,
            metrics,
            attempt,
            CommitStatePhase::Adjudication,
        )?;
        match read_commit_state(
            &transaction,
            codec,
            &state_key,
            deadline,
            metrics,
            attempt,
            CommitStatePhase::Adjudication,
        )
        .await
        {
            Ok(state) => return Ok(state),
            Err(error) if is_retryable_auxiliary_error(&error) && Instant::now() < deadline => {
                continue;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Removes one attempt's commit-state key.
///
/// Clearing an absent key is a no-op, so this is safe to call for an attempt
/// that never committed and safe to repeat. It is the only thing in this
/// provider that deletes anything outside a caller's own record mutations.
async fn clear_commit_evidence(
    database: &Database,
    codec: &KeyspaceCodec,
    limits: &StateStoreLimits,
    attempt: AttemptId,
    metrics: &ProviderMetrics,
) -> Result<(), StateStoreError> {
    let deadline = Instant::now() + limits.transaction_deadline.min(AUXILIARY_DEADLINE);
    let mut budget = AuxiliaryAttemptBudget::new();
    let state_key = codec.commit_state_key(attempt);
    loop {
        let transaction = create_auxiliary_transaction(
            database,
            limits,
            deadline,
            &mut budget,
            metrics,
            attempt,
            CommitStatePhase::EvidenceRelease,
        )?;
        transaction.clear(&state_key);
        match timeout_at(deadline, transaction.commit()).await {
            Ok(Ok(committed)) => {
                drop(committed);
                return Ok(());
            }
            Ok(Err(error)) => {
                let error = *error;
                let disposition = classify_native_commit_error(error);
                record_native_error(
                    metrics,
                    attempt,
                    CommitStatePhase::EvidenceRelease,
                    error,
                    disposition,
                );
                if should_retry_auxiliary_commit(error)
                    && budget.has_remaining()
                    && Instant::now() < deadline
                {
                    continue;
                }
                return Err(classify_auxiliary_native_error(error));
            }
            Err(_) => {
                record_auxiliary_metric(metrics, AuxiliaryMetricEvent::Deadline);
                return Err(deadline_error());
            }
        }
    }
}

async fn read_commit_state(
    transaction: &Transaction,
    codec: &KeyspaceCodec,
    state_key: &[u8],
    deadline: Instant,
    metrics: &ProviderMetrics,
    attempt: AttemptId,
    phase: CommitStatePhase,
) -> Result<Option<[u8; REVISION_BYTES]>, StateStoreError> {
    let value = match timeout_at(deadline, transaction.get(state_key, false)).await {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            log_native_error(attempt, phase, error, classify_native_commit_error(error));
            let error = classify_auxiliary_native_error(error);
            record_auxiliary_error(metrics, &error);
            return Err(error);
        }
        Err(_) => {
            record_auxiliary_metric(metrics, AuxiliaryMetricEvent::Deadline);
            return Err(deadline_error());
        }
    };
    value
        .map(|value| codec.decode_committed_revision(value.as_ref()))
        .transpose()
}

fn create_auxiliary_transaction(
    database: &Database,
    limits: &StateStoreLimits,
    deadline: Instant,
    budget: &mut AuxiliaryAttemptBudget,
    metrics: &ProviderMetrics,
    attempt: AttemptId,
    phase: CommitStatePhase,
) -> Result<Transaction, StateStoreError> {
    begin_auxiliary_attempt(budget, deadline, metrics)?;
    create_raw_transaction_with_observer(database, limits, deadline, |error| {
        log_native_error(attempt, phase, error, classify_native_commit_error(error));
    })
    .inspect_err(|error| {
        record_auxiliary_error(metrics, error);
    })
}

fn auxiliary_attempts_exhausted() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "FoundationDB auxiliary transaction attempt budget was exhausted",
    )
}

fn receipt(
    attempt: AttemptId,
    revision: [u8; REVISION_BYTES],
) -> Result<CommitReceipt, StateStoreError> {
    Ok(CommitReceipt {
        attempt,
        revision: revision_from_bytes(&revision)?,
    })
}

fn revision_from_bytes(value: &[u8]) -> Result<StoreRevision, StateStoreError> {
    if value.len() != REVISION_BYTES {
        return Err(StateStoreError::new(
            StateStoreErrorKind::Corruption,
            "FoundationDB commit revision is malformed",
        ));
    }
    StoreRevision::try_from(Bytes::copy_from_slice(value))
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

fn conflict_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Conflict,
        "FoundationDB transaction conflicted",
    )
}

fn provider_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "FoundationDB durable commit state operation failed",
    )
}

fn provider_transient() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Transient,
        "FoundationDB commit was not completed",
    )
}

fn deterministic_commit_error(code: i32) -> StateStoreError {
    let kind = match code {
        2101 | 2102 | 2103 | 2109 | 2110 => StateStoreErrorKind::LimitExceeded,
        2006 | 2007 => StateStoreErrorKind::InvalidConfiguration,
        _ => StateStoreErrorKind::InvalidRequest,
    };
    StateStoreError::new(
        kind,
        "FoundationDB rejected the transaction with a deterministic client or limit error",
    )
}

fn provider_unknown() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Transient,
        "FoundationDB transaction commit outcome is unknown",
    )
}

fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "FoundationDB durable commit state deadline exceeded",
    )
}

fn deadline_unknown() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "FoundationDB transaction commit timed out after dispatch",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FOUNDATIONDB_STATE_STORE_PROVIDER_ID;

    fn receipt_for(sequence: u64) -> CommitReceipt {
        // A receipt only needs *an* attempt identity here; the classification
        // under test does not read it.
        CommitReceipt {
            attempt: super::super::codec::tests_support::attempt_id(sequence),
            revision: StoreRevision::try_from(Bytes::from_static(&[0x22; REVISION_BYTES]))
                .expect("revision"),
        }
    }

    #[test]
    fn only_a_witnessed_refusal_publishes_a_denial() {
        assert_eq!(
            witnessed_verdict(&CommitOutcome::Committed(receipt_for(1))),
            Some(AttemptOutcome::Committed(receipt_for(1)))
        );
        for refused in [
            CommitOutcome::Conflict(conflict_error()),
            CommitOutcome::TransientBeforeCommit(provider_transient()),
            CommitOutcome::DefiniteFailure(deterministic_commit_error(2101)),
        ] {
            assert_eq!(
                witnessed_verdict(&refused),
                Some(AttemptOutcome::NotCommitted),
                "FoundationDB stated the transaction was not committed: {refused:?}"
            );
        }
        assert_eq!(
            witnessed_verdict(&CommitOutcome::CommitUnknown(provider_unknown())),
            None,
            "an ambiguous answer is not a verdict and must leave the evidence in place"
        );
    }

    #[test]
    fn commit_unknown_result_is_never_downgraded_to_a_denial() {
        // 1021 is FoundationDB saying it does not know. Treating it as a
        // refusal is exactly the lie the attempt contract forbids.
        assert_eq!(
            classify_native_commit_error(FdbError::from_code(1021)),
            NativeCommitDisposition::Unknown
        );
        assert_eq!(
            classify_native_commit_error(FdbError::from_code(NOT_COMMITTED_ERROR_CODE)),
            NativeCommitDisposition::ConflictNotCommitted
        );
        assert_eq!(
            classify_native_commit_error(FdbError::from_code(2101)),
            NativeCommitDisposition::DefiniteNotCommitted
        );
        assert_eq!(
            classify_native_commit_error(FdbError::from_code(1007)),
            NativeCommitDisposition::RetryableNotCommitted
        );
    }

    #[test]
    fn native_error_logs_name_the_attempt_and_never_the_payload() {
        let attempt = super::super::codec::tests_support::attempt_id(7);
        let fields = native_error_log_fields(
            attempt,
            CommitStatePhase::DataCommit,
            FdbError::from_code(1021),
            NativeCommitDisposition::Unknown,
        );
        assert_eq!(fields.attempt, attempt.to_string());
        assert_eq!(fields.phase, "data_commit");
        assert_eq!(fields.native_error_code, 1021);
        assert_eq!(fields.category, "unknown");
    }

    #[test]
    fn an_absent_commit_state_key_is_undecided_and_never_a_denial() {
        let attempt = super::super::codec::tests_support::attempt_id(3);
        assert_eq!(
            evidence_verdict(attempt, None).expect("absent evidence"),
            AttemptOutcome::Unresolved,
            "absence of evidence is not evidence of absence"
        );
        let AttemptOutcome::Committed(receipt) =
            evidence_verdict(attempt, Some([0x33; REVISION_BYTES])).expect("present evidence")
        else {
            panic!("a present commit-state key proves the transaction committed");
        };
        assert_eq!(receipt.attempt, attempt);
        assert_eq!(receipt.revision.as_bytes(), [0x33; REVISION_BYTES]);
    }

    #[test]
    fn auxiliary_budget_stops_before_it_can_spin() {
        let mut budget = AuxiliaryAttemptBudget::new();
        for _ in 0..AUXILIARY_MAX_ATTEMPTS {
            assert!(budget.try_consume());
        }
        assert!(!budget.has_remaining());
        assert!(!budget.try_consume());
    }

    #[test]
    fn commit_metrics_separate_every_outcome_class() {
        let metrics = ProviderMetrics::new(FOUNDATIONDB_STATE_STORE_PROVIDER_ID);
        for outcome in [
            CommitOutcome::Committed(receipt_for(1)),
            CommitOutcome::Conflict(conflict_error()),
            CommitOutcome::CommitUnknown(provider_unknown()),
        ] {
            record_commit(&metrics, StdInstant::now(), &outcome);
        }
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.commit_count, 3);
        assert_eq!(
            snapshot.operation_duration_observations(ProviderOperation::Commit),
            3
        );
    }
}
