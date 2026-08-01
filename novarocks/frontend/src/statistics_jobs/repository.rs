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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{ExternalMutationEvidence, MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES};
use novarocks_spi::state_store::{
    CommitOutcome, Direction, Key, KeyRange, Precondition, RangeRequest, ReadTransaction,
    StateStore, StateStoreError, StateStoreErrorKind, TransactionId, Value, VersionToken,
    WriteTransaction,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::model::{
    STATISTICS_JOB_SCHEMA_VERSION, StatisticsJob, StatisticsJobCreate, StatisticsJobError,
    StatisticsJobState, StoredStatisticsJobV2,
};

const JOB_PREFIX: &str = "novarocks/frontend/statistics/v2/jobs/";
const MAX_ERROR_MESSAGE_BYTES: usize = 4096;
const MAX_METRIC_NAMES: usize = 128;
const MAX_METRIC_NAME_BYTES: usize = 256;
const MAX_TARGET_COMPONENT_BYTES: usize = 1024;
const MAX_PUBLICATION_EVIDENCE_WIRE_BYTES: usize = MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES + 1024;
// A create has unique job and state-index keys, so a SQLite snapshot conflict
// is never a semantic duplicate. Retry the same durable identity instead of
// exposing storage contention as an ANALYZE failure.
const CREATE_CONFLICT_RETRY_LIMIT: usize = 8;

/// A worker passes this closure to atomically validate its coordination fence
/// before a durable job mutation. The repository deliberately has no lease
/// dependency and never manufactures an in-memory ownership fallback.
pub type FenceValidator = Arc<
    dyn for<'txn> Fn(
            &'txn mut dyn WriteTransaction,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'txn>>
        + Send
        + Sync,
>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsJobRepositoryErrorKind {
    NotFound,
    Conflict,
    InvalidTransition,
    Corruption,
    CommitUnknown,
    Store,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJobRepositoryError {
    kind: StatisticsJobRepositoryErrorKind,
    message: String,
}

impl StatisticsJobRepositoryError {
    pub const fn kind(&self) -> StatisticsJobRepositoryErrorKind {
        self.kind
    }

    fn new(kind: StatisticsJobRepositoryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn corruption(message: impl Into<String>) -> Self {
        Self::new(StatisticsJobRepositoryErrorKind::Corruption, message)
    }
}

impl fmt::Display for StatisticsJobRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StatisticsJobRepositoryError {}

type RepositoryResult<T> = Result<T, StatisticsJobRepositoryError>;

#[derive(Clone)]
pub struct StatisticsJobRepository {
    store: Arc<dyn StateStore>,
}

impl fmt::Debug for StatisticsJobRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatisticsJobRepository")
            .finish_non_exhaustive()
    }
}

impl StatisticsJobRepository {
    pub async fn open(store: Arc<dyn StateStore>) -> RepositoryResult<Self> {
        let repository = Self { store };
        repository.list().await?;
        Ok(repository)
    }

    pub(crate) fn store(&self) -> Arc<dyn StateStore> {
        Arc::clone(&self.store)
    }

    pub async fn create(&self, request: StatisticsJobCreate) -> RepositoryResult<StatisticsJob> {
        self.create_with_fence(request, None).await
    }

    pub async fn create_with_fence(
        &self,
        request: StatisticsJobCreate,
        fence: Option<&FenceValidator>,
    ) -> RepositoryResult<StatisticsJob> {
        validate_create(&request)?;
        let job_id = Uuid::now_v7();
        let operation_id = Uuid::now_v7();
        let stored = StoredStatisticsJobV2 {
            schema_version: STATISTICS_JOB_SCHEMA_VERSION,
            job_id,
            operation_id,
            target: request.target,
            table_pin: request.table_pin,
            metric_names: request.metric_names,
            state: StatisticsJobState::Submitted,
            attempt: 0,
            retry_not_before_ms: None,
            publication_evidence: None,
            cancel_requested: false,
            error: None,
            submitted_at_ms: request.submitted_at_ms,
            updated_at_ms: request.submitted_at_ms,
            completed_at_ms: None,
        };
        for retry in 0..=CREATE_CONFLICT_RETRY_LIMIT {
            let mut transaction = self.begin_write("create frontend statistics job").await?;
            validate_fence(fence, transaction.as_mut()).await?;
            transaction
                .put(
                    job_key(job_id)?,
                    encode_json(&stored, "statistics job")?,
                    Precondition::Absent,
                )
                .await
                .map_err(store_error)?;
            transaction
                .put(
                    state_key(StatisticsJobState::Submitted, job_id)?,
                    index_value(job_id)?,
                    Precondition::Absent,
                )
                .await
                .map_err(store_error)?;
            match self
                .commit_or_recover(transaction, "create frontend statistics job", &stored)
                .await
            {
                Ok(job) => return Ok(job),
                Err(error)
                    if error.kind() == StatisticsJobRepositoryErrorKind::Conflict
                        && retry < CREATE_CONFLICT_RETRY_LIMIT =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("statistics create retry loop always returns")
    }

    pub async fn get(&self, job_id: Uuid) -> RepositoryResult<Option<StatisticsJob>> {
        let mut transaction = self.store.begin_read().await.map_err(store_error)?;
        let result = load_job(transaction.as_mut(), job_id)
            .await?
            .map(|job| StatisticsJob::from(&job.stored));
        transaction.abort().await.map_err(store_error)?;
        Ok(result)
    }

    pub async fn list(&self) -> RepositoryResult<Vec<StatisticsJob>> {
        let prefix = key(JOB_PREFIX)?;
        let range = KeyRange::for_prefix(prefix).map_err(store_error)?;
        let mut transaction = self.store.begin_read().await.map_err(store_error)?;
        let mut request = RangeRequest {
            range,
            direction: Direction::Forward,
            page_size: self.store.limits().max_page_size,
            continuation: None,
        };
        let mut jobs = Vec::new();
        loop {
            let page = transaction.range(&request).await.map_err(store_error)?;
            for record in page.records {
                let stored: StoredStatisticsJobV2 =
                    decode_json(record.value.as_bytes(), "statistics job")?;
                validate_stored(&stored)?;
                jobs.push(StatisticsJob::from(&stored));
            }
            let Some(continuation) = page.continuation else {
                break;
            };
            request.continuation = Some(continuation);
        }
        transaction.abort().await.map_err(store_error)?;
        jobs.sort_by_key(|job| job.job_id);
        Ok(jobs)
    }

    pub async fn list_by_state(
        &self,
        state: StatisticsJobState,
    ) -> RepositoryResult<Vec<StatisticsJob>> {
        let prefix_text = state_prefix(state);
        let prefix = key(prefix_text)?;
        let range = KeyRange::for_prefix(prefix).map_err(store_error)?;
        let mut transaction = self.store.begin_read().await.map_err(store_error)?;
        let mut request = RangeRequest {
            range,
            direction: Direction::Forward,
            page_size: self.store.limits().max_page_size,
            continuation: None,
        };
        let mut jobs = Vec::new();
        loop {
            let page = transaction.range(&request).await.map_err(store_error)?;
            for record in page.records {
                let job_id = decode_index_value(&record.value)?;
                let stored = load_job(transaction.as_mut(), job_id)
                    .await?
                    .ok_or_else(|| {
                        StatisticsJobRepositoryError::corruption(
                            "statistics job state index references a missing job",
                        )
                    })?;
                if stored.stored.state != state {
                    return Err(StatisticsJobRepositoryError::corruption(
                        "statistics job state index does not match its job record",
                    ));
                }
                jobs.push(StatisticsJob::from(&stored.stored));
            }
            let Some(continuation) = page.continuation else {
                break;
            };
            request.continuation = Some(continuation);
        }
        transaction.abort().await.map_err(store_error)?;
        jobs.sort_by_key(|job| job.job_id);
        Ok(jobs)
    }

    /// Claiming begins the worker-owned lifecycle by atomically moving a
    /// submitted job into PREPARING and incrementing its retry attempt.
    pub async fn claim(
        &self,
        job_id: Uuid,
        now_ms: i64,
        fence: &FenceValidator,
    ) -> RepositoryResult<Option<StatisticsJob>> {
        let Some(job) = self.get(job_id).await? else {
            return Ok(None);
        };
        if job.state != StatisticsJobState::Submitted {
            return Ok(None);
        }
        if job.cancel_requested {
            return Ok(None);
        }
        self.transition_with_fence(
            job_id,
            StatisticsJobState::Submitted,
            StatisticsJobState::Preparing,
            now_ms,
            None,
            Some(fence),
            true,
            None,
            None,
        )
        .await
        .map(Some)
    }

    pub async fn transition(
        &self,
        job_id: Uuid,
        expected: StatisticsJobState,
        next: StatisticsJobState,
        now_ms: i64,
        error: Option<StatisticsJobError>,
        fence: &FenceValidator,
    ) -> RepositoryResult<StatisticsJob> {
        if next == StatisticsJobState::Publishing {
            return Err(StatisticsJobRepositoryError::corruption(
                "statistics publication requires operation evidence",
            ));
        }
        self.transition_with_fence(
            job_id,
            expected,
            next,
            now_ms,
            error,
            Some(fence),
            false,
            None,
            None,
        )
        .await
    }

    /// Atomically installs the already-prepared reconciliation evidence and
    /// crosses the irreversible publish boundary under the active lease
    /// fence. A crash after this commit can only reconcile, never recollect or
    /// republish blindly.
    pub async fn begin_publishing(
        &self,
        job_id: Uuid,
        now_ms: i64,
        publication_evidence: Bytes,
        fence: &FenceValidator,
    ) -> RepositoryResult<StatisticsJob> {
        if publication_evidence.is_empty()
            || publication_evidence.len() > MAX_PUBLICATION_EVIDENCE_WIRE_BYTES
        {
            return Err(StatisticsJobRepositoryError::corruption(
                "statistics publication evidence is empty or exceeds the bound",
            ));
        }
        self.transition_with_fence(
            job_id,
            StatisticsJobState::Running,
            StatisticsJobState::Publishing,
            now_ms,
            None,
            Some(fence),
            false,
            None,
            Some(publication_evidence.to_vec()),
        )
        .await
    }

    /// Replays an incomplete attempt after frontend failover. Only preparation
    /// and collection can return to SUBMITTED: PUBLISHING is intentionally
    /// excluded because it must reconcile its operation-specific receipt.
    pub async fn requeue_incomplete(
        &self,
        job_id: Uuid,
        now_ms: i64,
        fence: &FenceValidator,
    ) -> RepositoryResult<Option<StatisticsJob>> {
        let Some(job) = self.get(job_id).await? else {
            return Ok(None);
        };
        if !matches!(
            job.state,
            StatisticsJobState::Preparing | StatisticsJobState::Running
        ) {
            return Ok(None);
        }
        self.transition_with_fence(
            job_id,
            job.state,
            StatisticsJobState::Submitted,
            now_ms,
            None,
            Some(fence),
            false,
            None,
            None,
        )
        .await
        .map(Some)
    }

    /// Records client cancellation intent without changing the worker-owned
    /// lifecycle state.  The active fenced worker consumes this bit in the
    /// same transaction as its `state -> CANCELLED` CAS.
    pub async fn request_cancel(
        &self,
        job_id: Uuid,
        now_ms: i64,
    ) -> RepositoryResult<StatisticsJob> {
        let mut transaction = self
            .begin_write("request frontend statistics job cancellation")
            .await?;
        let versioned = load_job(transaction.as_mut(), job_id)
            .await?
            .ok_or_else(|| not_found(job_id))?;
        if !matches!(
            versioned.stored.state,
            StatisticsJobState::Submitted
                | StatisticsJobState::Preparing
                | StatisticsJobState::Running
        ) {
            return Err(StatisticsJobRepositoryError::new(
                StatisticsJobRepositoryErrorKind::Conflict,
                format!(
                    "cancel statistics job {job_id} conflicts with {:?}",
                    versioned.stored.state
                ),
            ));
        }
        let mut stored = versioned.stored;
        stored.cancel_requested = true;
        stored.updated_at_ms = now_ms;
        transaction
            .put(
                job_key(job_id)?,
                encode_json(&stored, "statistics job")?,
                Precondition::Version(versioned.version),
            )
            .await
            .map_err(store_error)?;
        self.commit_or_recover(
            transaction,
            "request frontend statistics job cancellation",
            &stored,
        )
        .await
    }

    /// Worker-only cancellation transition. Callers must provide the active
    /// lease fence; clients use `request_cancel` instead.
    pub async fn cancel(
        &self,
        job_id: Uuid,
        now_ms: i64,
        fence: &FenceValidator,
    ) -> RepositoryResult<StatisticsJob> {
        let job = self.get(job_id).await?.ok_or_else(|| not_found(job_id))?;
        if job.state == StatisticsJobState::Publishing {
            return Err(StatisticsJobRepositoryError::new(
                StatisticsJobRepositoryErrorKind::Conflict,
                format!("cancel statistics job {job_id} conflicts with PUBLISHING"),
            ));
        }
        self.transition_with_fence(
            job_id,
            job.state,
            StatisticsJobState::Cancelled,
            now_ms,
            None,
            Some(fence),
            false,
            None,
            None,
        )
        .await
    }

    /// Requeues a transient collection failure with a durable retry deadline.
    /// The deadline is atomically written with the CAS and fence validation,
    /// so frontend failover cannot convert backoff into an immediate retry.
    pub async fn retry_running(
        &self,
        job_id: Uuid,
        now_ms: i64,
        retry_not_before_ms: i64,
        fence: &FenceValidator,
    ) -> RepositoryResult<StatisticsJob> {
        if retry_not_before_ms <= now_ms {
            return Err(StatisticsJobRepositoryError::corruption(
                "statistics retry deadline must be after transition time",
            ));
        }
        self.transition_with_fence(
            job_id,
            StatisticsJobState::Running,
            StatisticsJobState::Submitted,
            now_ms,
            None,
            Some(fence),
            false,
            Some(retry_not_before_ms),
            None,
        )
        .await
    }

    async fn transition_with_fence(
        &self,
        job_id: Uuid,
        expected: StatisticsJobState,
        next: StatisticsJobState,
        now_ms: i64,
        error: Option<StatisticsJobError>,
        fence: Option<&FenceValidator>,
        increment_attempt: bool,
        retry_not_before_ms: Option<i64>,
        publication_evidence: Option<Vec<u8>>,
    ) -> RepositoryResult<StatisticsJob> {
        if !expected.can_transition_to(next) {
            return Err(invalid_transition(job_id, expected, next));
        }
        validate_error(error.as_ref())?;
        let mut transaction = self
            .begin_write("transition frontend statistics job")
            .await?;
        validate_fence(fence, transaction.as_mut()).await?;
        let versioned = load_job(transaction.as_mut(), job_id)
            .await?
            .ok_or_else(|| not_found(job_id))?;
        if versioned.stored.state != expected {
            return Err(StatisticsJobRepositoryError::new(
                StatisticsJobRepositoryErrorKind::Conflict,
                format!(
                    "statistics job {job_id} state changed from {expected:?} to {:?}",
                    versioned.stored.state
                ),
            ));
        }
        let old_index = state_key(expected, job_id)?;
        if transaction
            .get(&old_index)
            .await
            .map_err(store_error)?
            .is_none()
        {
            return Err(StatisticsJobRepositoryError::corruption(
                "statistics job record has no matching state index",
            ));
        }
        let mut stored = versioned.stored;
        stored.state = next;
        stored.updated_at_ms = now_ms;
        stored.error = error;
        stored.retry_not_before_ms = retry_not_before_ms;
        if next == StatisticsJobState::Publishing && publication_evidence.is_none() {
            return Err(StatisticsJobRepositoryError::corruption(
                "statistics publication transition is missing operation evidence",
            ));
        }
        if let Some(publication_evidence) = publication_evidence {
            stored.publication_evidence = Some(publication_evidence);
        }
        stored.cancel_requested = false;
        if increment_attempt {
            stored.attempt = stored.attempt.checked_add(1).ok_or_else(|| {
                StatisticsJobRepositoryError::corruption("statistics job attempt overflow")
            })?;
        }
        stored.completed_at_ms = next.is_terminal().then_some(now_ms);
        transaction
            .put(
                job_key(job_id)?,
                encode_json(&stored, "statistics job")?,
                Precondition::Version(versioned.version),
            )
            .await
            .map_err(store_error)?;
        transaction
            .delete(old_index, Precondition::Present)
            .await
            .map_err(store_error)?;
        transaction
            .put(
                state_key(next, job_id)?,
                index_value(job_id)?,
                Precondition::Absent,
            )
            .await
            .map_err(store_error)?;
        self.commit_or_recover(transaction, "transition frontend statistics job", &stored)
            .await
    }

    async fn begin_write(&self, purpose: &str) -> RepositoryResult<Box<dyn WriteTransaction>> {
        self.store
            .begin_write(TransactionId::from(Uuid::now_v7()), purpose)
            .await
            .map_err(store_error)
    }

    /// A commit-unknown is never retried. Resolve the transaction first, then
    /// prove the exact durable successor by its job and operation IDs.
    async fn commit_or_recover(
        &self,
        transaction: Box<dyn WriteTransaction>,
        context: &str,
        expected: &StoredStatisticsJobV2,
    ) -> RepositoryResult<StatisticsJob> {
        let transaction_id = *transaction.transaction_id();
        match transaction.commit().await {
            CommitOutcome::Committed(_) => Ok(StatisticsJob::from(expected)),
            CommitOutcome::Conflict(error) => Err(StatisticsJobRepositoryError::new(
                StatisticsJobRepositoryErrorKind::Conflict,
                format!("{context} conflicted: {error}"),
            )),
            CommitOutcome::CommitUnknown(error) => {
                self.reconcile_commit_unknown(transaction_id, context, expected, error)
                    .await
            }
            CommitOutcome::TransientBeforeCommit(error) | CommitOutcome::DefiniteFailure(error) => {
                Err(store_error(error))
            }
        }
    }

    async fn reconcile_commit_unknown(
        &self,
        transaction_id: TransactionId,
        context: &str,
        expected: &StoredStatisticsJobV2,
        commit_error: StateStoreError,
    ) -> RepositoryResult<StatisticsJob> {
        let resolution = self
            .store
            .resolve_commit(&transaction_id)
            .await
            .map_err(store_error)?;
        let mut transaction = self.store.begin_read().await.map_err(store_error)?;
        let recovered = load_job(transaction.as_mut(), expected.job_id).await?;
        transaction.abort().await.map_err(store_error)?;
        match (resolution, recovered) {
            (_, Some(recovered))
                if recovered.stored == *expected
                    && recovered.stored.operation_id == expected.operation_id =>
            {
                Ok(StatisticsJob::from(&recovered.stored))
            }
            (novarocks_spi::state_store::CommitResolution::NotCommitted, _) => {
                Err(StatisticsJobRepositoryError::new(
                    StatisticsJobRepositoryErrorKind::Store,
                    format!(
                        "{context} transaction {} was not committed: {commit_error}",
                        transaction_id.as_uuid()
                    ),
                ))
            }
            (novarocks_spi::state_store::CommitResolution::Committed(_), _) => {
                Err(StatisticsJobRepositoryError::corruption(format!(
                    "{context} transaction {} is committed but the authoritative job record differs",
                    transaction_id.as_uuid()
                )))
            }
            (novarocks_spi::state_store::CommitResolution::Unresolved, _) => {
                Err(StatisticsJobRepositoryError::new(
                    StatisticsJobRepositoryErrorKind::CommitUnknown,
                    format!(
                        "{context} transaction {} remains unresolved after authoritative reread: {commit_error}",
                        transaction_id.as_uuid()
                    ),
                ))
            }
        }
    }
}

struct VersionedJob {
    stored: StoredStatisticsJobV2,
    version: VersionToken,
}

async fn load_job(
    transaction: &mut dyn ReadTransaction,
    job_id: Uuid,
) -> RepositoryResult<Option<VersionedJob>> {
    let Some(record) = transaction
        .get(&job_key(job_id)?)
        .await
        .map_err(store_error)?
    else {
        return Ok(None);
    };
    let stored: StoredStatisticsJobV2 = decode_json(record.value.as_bytes(), "statistics job")?;
    validate_stored(&stored)?;
    if stored.job_id != job_id {
        return Err(StatisticsJobRepositoryError::corruption(
            "statistics job key does not match record job id",
        ));
    }
    Ok(Some(VersionedJob {
        stored,
        version: record.version,
    }))
}

async fn validate_fence(
    fence: Option<&FenceValidator>,
    transaction: &mut dyn WriteTransaction,
) -> RepositoryResult<()> {
    if let Some(fence) = fence {
        fence(transaction).await.map_err(|message| {
            StatisticsJobRepositoryError::new(StatisticsJobRepositoryErrorKind::Store, message)
        })?;
    }
    Ok(())
}

fn validate_create(request: &StatisticsJobCreate) -> RepositoryResult<()> {
    for component in [
        &request.target.catalog,
        &request.target.namespace,
        &request.target.table,
    ] {
        if component.is_empty() || component.len() > MAX_TARGET_COMPONENT_BYTES {
            return Err(StatisticsJobRepositoryError::corruption(
                "statistics job target components must be non-empty and bounded",
            ));
        }
    }
    request.table_pin.validate().map_err(|error| {
        StatisticsJobRepositoryError::corruption(format!(
            "invalid statistics job table pin: {error}"
        ))
    })?;
    if request.metric_names.is_empty() || request.metric_names.len() > MAX_METRIC_NAMES {
        return Err(StatisticsJobRepositoryError::corruption(
            "statistics job metric names must be non-empty and bounded",
        ));
    }
    if request
        .metric_names
        .iter()
        .any(|metric| metric.is_empty() || metric.len() > MAX_METRIC_NAME_BYTES)
    {
        return Err(StatisticsJobRepositoryError::corruption(
            "statistics job metric name is empty or exceeds the bound",
        ));
    }
    Ok(())
}

fn validate_error(error: Option<&StatisticsJobError>) -> RepositoryResult<()> {
    if error.is_some_and(|error| {
        error.message.is_empty() || error.message.len() > MAX_ERROR_MESSAGE_BYTES
    }) {
        return Err(StatisticsJobRepositoryError::corruption(
            "statistics job error message is empty or exceeds the bound",
        ));
    }
    Ok(())
}

fn validate_stored(stored: &StoredStatisticsJobV2) -> RepositoryResult<()> {
    if stored.schema_version != STATISTICS_JOB_SCHEMA_VERSION {
        return Err(StatisticsJobRepositoryError::corruption(
            "statistics job record has an unsupported schema version",
        ));
    }
    validate_create(&StatisticsJobCreate {
        target: stored.target.clone(),
        table_pin: stored.table_pin.clone(),
        metric_names: stored.metric_names.clone(),
        submitted_at_ms: stored.submitted_at_ms,
    })?;
    validate_error(stored.error.as_ref())?;
    if stored
        .publication_evidence
        .as_ref()
        .is_some_and(|evidence| {
            evidence.is_empty() || evidence.len() > MAX_PUBLICATION_EVIDENCE_WIRE_BYTES
        })
    {
        return Err(StatisticsJobRepositoryError::corruption(
            "statistics job publication evidence is empty or exceeds the bound",
        ));
    }
    if stored.state == StatisticsJobState::Publishing && stored.publication_evidence.is_none() {
        return Err(StatisticsJobRepositoryError::corruption(
            "publishing statistics job is missing operation evidence",
        ));
    }
    if let Some(wire) = &stored.publication_evidence {
        let evidence = ExternalMutationEvidence::try_from_wire_v1(wire).map_err(|error| {
            StatisticsJobRepositoryError::corruption(format!(
                "statistics job publication evidence is invalid: {error}"
            ))
        })?;
        if evidence.operation_id().to_bytes() != *stored.operation_id.as_bytes() {
            return Err(StatisticsJobRepositoryError::corruption(
                "statistics job publication evidence operation ID does not match its job",
            ));
        }
    }
    if stored.state.is_terminal() != stored.completed_at_ms.is_some() {
        return Err(StatisticsJobRepositoryError::corruption(
            "statistics job terminal state and completion timestamp disagree",
        ));
    }
    Ok(())
}

fn key(value: impl AsRef<[u8]>) -> RepositoryResult<Key> {
    Key::try_from(Bytes::copy_from_slice(value.as_ref())).map_err(store_error)
}

fn job_key(job_id: Uuid) -> RepositoryResult<Key> {
    key(format!("{JOB_PREFIX}{job_id}"))
}

fn state_prefix(state: StatisticsJobState) -> &'static str {
    match state {
        StatisticsJobState::Submitted => "novarocks/frontend/statistics/v1/state/SUBMITTED/",
        StatisticsJobState::Preparing => "novarocks/frontend/statistics/v1/state/PREPARING/",
        StatisticsJobState::Running => "novarocks/frontend/statistics/v1/state/RUNNING/",
        StatisticsJobState::Publishing => "novarocks/frontend/statistics/v1/state/PUBLISHING/",
        StatisticsJobState::Succeeded => "novarocks/frontend/statistics/v1/state/SUCCEEDED/",
        StatisticsJobState::Failed => "novarocks/frontend/statistics/v1/state/FAILED/",
        StatisticsJobState::Cancelled => "novarocks/frontend/statistics/v1/state/CANCELLED/",
    }
}

fn state_key(state: StatisticsJobState, job_id: Uuid) -> RepositoryResult<Key> {
    key(format!("{}{job_id}", state_prefix(state)))
}

fn index_value(job_id: Uuid) -> RepositoryResult<Value> {
    Value::try_from(Bytes::from(job_id.to_string())).map_err(store_error)
}

fn decode_index_value(value: &Value) -> RepositoryResult<Uuid> {
    std::str::from_utf8(value.as_bytes())
        .map_err(|_| StatisticsJobRepositoryError::corruption("statistics job index is not UTF-8"))?
        .parse()
        .map_err(|_| StatisticsJobRepositoryError::corruption("statistics job index is not a UUID"))
}

fn encode_json<T: Serialize>(value: &T, context: &str) -> RepositoryResult<Value> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        StatisticsJobRepositoryError::corruption(format!("encode {context} failed: {error}"))
    })?;
    Value::try_from(Bytes::from(bytes)).map_err(store_error)
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8], context: &str) -> RepositoryResult<T> {
    serde_json::from_slice(bytes).map_err(|error| {
        StatisticsJobRepositoryError::corruption(format!("decode {context} failed: {error}"))
    })
}

fn not_found(job_id: Uuid) -> StatisticsJobRepositoryError {
    StatisticsJobRepositoryError::new(
        StatisticsJobRepositoryErrorKind::NotFound,
        format!("statistics job {job_id} does not exist"),
    )
}

fn invalid_transition(
    job_id: Uuid,
    expected: StatisticsJobState,
    next: StatisticsJobState,
) -> StatisticsJobRepositoryError {
    StatisticsJobRepositoryError::new(
        StatisticsJobRepositoryErrorKind::InvalidTransition,
        format!("statistics job {job_id} cannot transition from {expected:?} to {next:?}"),
    )
}

fn store_error(error: StateStoreError) -> StatisticsJobRepositoryError {
    let kind = if error.kind() == StateStoreErrorKind::Corruption {
        StatisticsJobRepositoryErrorKind::Corruption
    } else {
        StatisticsJobRepositoryErrorKind::Store
    };
    StatisticsJobRepositoryError::new(kind, error.to_string())
}
