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

//! Durable, fail-closed GC first-observation accelerator.
//!
//! This module is deliberately independent from table-maintenance jobs and
//! operations. It retains only the time at which one exact provider-proven
//! owned-ref tuple was first observed; it never owns a catalog mutation.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use novarocks_state_store_api::{
    Direction, Key, KeyRange, OriginalWriteTransaction, Precondition, RangeRequest, StateRecord,
    StateStore, StateStoreError, StateStoreExecutionContext, StateStoreExecutionFailure,
    WriteTransaction,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use novarocks_state_store_runtime::{
    DurableRecord, DurableRecordStore, EncodedRecord, PersistentStateFamily, RunFailure,
    StateStoreRunMetrics, StateStoreRunPolicy, run_side_effect_free, run_side_effect_free_in,
};

/// Frozen StateStore identity owned by Table Maintenance.
///
/// The prefix deliberately retains its historic `frontend` namespace because
/// deployed records already use these bytes; it does not imply Frontend owns
/// the record model or its lifecycle.
pub const GC_OWNED_REF_OBSERVATION_STATE_FAMILY: PersistentStateFamily = PersistentStateFamily::new(
    "table-maintenance/gc-owned-ref-observation",
    "novarocks/frontend/table-maintenance/v7/gc-owned-ref-observations/",
    7,
);

pub const GC_OWNED_REF_OBSERVATION_FAMILY: &str = GC_OWNED_REF_OBSERVATION_STATE_FAMILY.owner_id();
pub const GC_OWNED_REF_OBSERVATION_SCHEMA_VERSION: u8 =
    GC_OWNED_REF_OBSERVATION_STATE_FAMILY.record_version();
pub const GC_OWNED_REF_OBSERVATION_MAX_REF_NAME_BYTES: usize = 256;
pub const GC_OWNED_REF_OBSERVATION_RECORD_ENCODED_LIMIT: usize = 4 * 1024;

/// Product policy for this retained, clone-wiped safety accelerator.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcOwnedRefObservationFamilyPolicy;

impl GcOwnedRefObservationFamilyPolicy {
    pub const fn family(self) -> PersistentStateFamily {
        GC_OWNED_REF_OBSERVATION_STATE_FAMILY
    }

    pub const fn family_id(self) -> &'static str {
        GC_OWNED_REF_OBSERVATION_FAMILY
    }

    pub const fn schema_version(self) -> u8 {
        GC_OWNED_REF_OBSERVATION_SCHEMA_VERSION
    }

    pub const fn retain_on_restart(self) -> bool {
        true
    }

    pub const fn wipe_on_clone(self) -> bool {
        true
    }

    pub async fn wipe_for_clone(
        self,
        accelerator: &GcOwnedRefObservationAccelerator,
    ) -> Result<u64, GcOwnedRefObservationError> {
        accelerator.wipe_family().await
    }
}

/// One exact catalog-owned-ref observation made by GC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcOwnedRefObservation {
    pub table_uuid: Uuid,
    pub ref_name: String,
    pub head_snapshot_id: i64,
    pub provenance_version: u16,
    pub provenance_digest: [u8; 32],
    pub first_observed_at_ms: i64,
}

impl GcOwnedRefObservation {
    pub fn try_new(
        table_uuid: Uuid,
        ref_name: String,
        head_snapshot_id: i64,
        provenance_version: u16,
        provenance_digest: [u8; 32],
        first_observed_at_ms: i64,
    ) -> Result<Self, String> {
        let observation = Self {
            table_uuid,
            ref_name,
            head_snapshot_id,
            provenance_version,
            provenance_digest,
            first_observed_at_ms,
        };
        observation.validate()?;
        Ok(observation)
    }

    pub fn validate(&self) -> Result<(), String> {
        Self::validate_key(self.table_uuid, &self.ref_name)?;
        if self.head_snapshot_id <= 0 {
            return Err("GC owned-ref observation head snapshot ID must be positive".to_string());
        }
        if self.provenance_version == 0 {
            return Err("GC owned-ref observation provenance version must be non-zero".to_string());
        }
        if self.first_observed_at_ms <= 0 {
            return Err("GC owned-ref observation timestamp must be positive".to_string());
        }
        Ok(())
    }

    pub fn validate_key(table_uuid: Uuid, ref_name: &str) -> Result<(), String> {
        if table_uuid.is_nil() {
            return Err("GC owned-ref observation table UUID must not be nil".to_string());
        }
        if ref_name.is_empty() || ref_name.len() > GC_OWNED_REF_OBSERVATION_MAX_REF_NAME_BYTES {
            return Err(format!(
                "GC owned-ref observation ref name must contain 1..={} bytes",
                GC_OWNED_REF_OBSERVATION_MAX_REF_NAME_BYTES
            ));
        }
        Ok(())
    }

    pub fn matches_facts(&self, other: &Self) -> bool {
        self.table_uuid == other.table_uuid
            && self.ref_name == other.ref_name
            && self.head_snapshot_id == other.head_snapshot_id
            && self.provenance_version == other.provenance_version
            && self.provenance_digest == other.provenance_digest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GcOwnedRefObservationDecision {
    NotMature { first_observed_at_ms: i64 },
    Mature { first_observed_at_ms: i64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GcOwnedRefObservationErrorKind {
    Corruption,
    CommitUnknown,
    Store,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcOwnedRefObservationError {
    kind: GcOwnedRefObservationErrorKind,
    message: String,
}

impl GcOwnedRefObservationError {
    pub const fn kind(&self) -> GcOwnedRefObservationErrorKind {
        self.kind
    }

    fn corruption(message: impl Into<String>) -> Self {
        Self {
            kind: GcOwnedRefObservationErrorKind::Corruption,
            message: message.into(),
        }
    }

    fn store(message: impl Into<String>) -> Self {
        Self {
            kind: GcOwnedRefObservationErrorKind::Store,
            message: message.into(),
        }
    }
}

impl fmt::Display for GcOwnedRefObservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for GcOwnedRefObservationError {}

/// Original execution preserves the provider's raw failure and the exact
/// unresolved observation; neither is formatted into the legacy String error.
#[derive(Clone, Debug)]
pub enum GcOwnedRefObservationInError {
    Observation(GcOwnedRefObservationError),
    Run(RunFailure<StateStoreExecutionFailure>),
}
impl GcOwnedRefObservationInError {
    pub fn kind(&self) -> GcOwnedRefObservationErrorKind {
        match self {
            Self::Observation(error) => error.kind(),
            Self::Run(RunFailure::CommitUnknown { .. }) => {
                GcOwnedRefObservationErrorKind::CommitUnknown
            }
            Self::Run(_) => GcOwnedRefObservationErrorKind::Store,
        }
    }
    pub fn run_failure(&self) -> Option<&RunFailure<StateStoreExecutionFailure>> {
        match self {
            Self::Run(failure) => Some(failure),
            _ => None,
        }
    }
    pub fn original_failure(&self) -> Option<&StateStoreExecutionFailure> {
        match self {
            Self::Run(
                RunFailure::Begin(error)
                | RunFailure::Operation(error)
                | RunFailure::RetryExhausted(error)
                | RunFailure::DefiniteFailure(error)
                | RunFailure::CommitUnknown { error, .. },
            ) => Some(error),
            _ => None,
        }
    }
}
impl fmt::Display for GcOwnedRefObservationInError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Observation(error) => error.fmt(f),
            Self::Run(failure) => match failure {
                RunFailure::Begin(error) => {
                    write!(f, "GC original observation begin failed: {error}")
                }
                RunFailure::Operation(error) => {
                    write!(f, "GC original observation operation failed: {error}")
                }
                RunFailure::RetryExhausted(error) => {
                    write!(f, "GC original observation retry exhausted: {error}")
                }
                RunFailure::DefiniteFailure(error) => {
                    write!(f, "GC original observation commit failed: {error}")
                }
                RunFailure::CommitUnknown { error, .. } => {
                    write!(f, "GC original observation commit unknown: {error}")
                }
                RunFailure::DeadlineExceeded => {
                    f.write_str("GC original observation deadline exceeded")
                }
            },
        }
    }
}
impl std::error::Error for GcOwnedRefObservationInError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Observation(error) => Some(error),
            _ => self
                .original_failure()
                .map(|error| error as &(dyn std::error::Error + 'static)),
        }
    }
}

/// Product-owned retry facts for GC safety-window persistence.
#[derive(Debug, Default)]
struct GcObservationMetrics {
    retries: AtomicU64,
    saturated_retries: AtomicU64,
    deadlines: AtomicU64,
    unresolved: AtomicU64,
}

impl StateStoreRunMetrics for GcObservationMetrics {
    fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    fn record_saturated_retry(&self) {
        self.saturated_retries.fetch_add(1, Ordering::Relaxed);
    }

    fn record_deadline(&self) {
        self.deadlines.fetch_add(1, Ordering::Relaxed);
    }

    fn record_unresolved(&self) {
        self.unresolved.fetch_add(1, Ordering::Relaxed);
    }
}

/// The sole durable owner for GC first-observation records.
#[derive(Clone)]
pub struct GcOwnedRefObservationAccelerator {
    store: Arc<dyn StateStore>,
    durable: DurableRecordStore,
    metrics: Arc<GcObservationMetrics>,
    policy: StateStoreRunPolicy,
}

impl fmt::Debug for GcOwnedRefObservationAccelerator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GcOwnedRefObservationAccelerator")
            .field("family", &GC_OWNED_REF_OBSERVATION_FAMILY)
            .finish_non_exhaustive()
    }
}

impl GcOwnedRefObservationAccelerator {
    pub async fn open(
        store: Arc<dyn StateStore>,
        policy: StateStoreRunPolicy,
    ) -> Result<Self, GcOwnedRefObservationError> {
        Ok(Self {
            // A business owner, not the storage provider. GC used to label its
            // counters with whatever provider happened to be underneath, which
            // said nothing about which workload was retrying.
            metrics: Arc::new(GcObservationMetrics::default()),
            durable: DurableRecordStore::new(Arc::clone(&store)),
            store,
            policy,
        })
    }

    pub const fn policy(&self) -> GcOwnedRefObservationFamilyPolicy {
        GcOwnedRefObservationFamilyPolicy
    }

    /// Record or compare one exact owned-ref fact tuple. Corrupt or unsupported
    /// values are replaced with a current record and restart the safety clock.
    pub async fn observe(
        &self,
        observation: GcOwnedRefObservation,
        now_ms: i64,
        safe_gc_age_ms: i64,
    ) -> Result<GcOwnedRefObservationDecision, GcOwnedRefObservationError> {
        validate_observation_input(&observation, now_ms, safe_gc_age_ms)?;

        let durable = self.durable.clone();
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            self.policy,
            "record table-maintenance GC owned-ref observation",
            move |transaction| {
                let observation = observation.clone();
                let durable = durable.clone();
                Box::pin(async move {
                    let mut transaction = LegacyObservation(transaction);
                    apply_observation(
                        &mut transaction,
                        &durable,
                        observation,
                        now_ms,
                        safe_gc_age_ms,
                    )
                    .await
                })
            },
        )
        .await;
        match result {
            Ok(success) => success.value,
            Err(failure) => Err(format_run_failure(
                "record table-maintenance GC owned-ref observation",
                failure,
            )),
        }
    }

    /// Same record and clock algorithm on the selected original transaction.
    /// The execution borrower never manufactures another Root or blocking port.
    pub async fn observe_in(
        &self,
        execution: &StateStoreExecutionContext,
        observation: GcOwnedRefObservation,
        now_ms: i64,
        safe_gc_age_ms: i64,
    ) -> Result<GcOwnedRefObservationDecision, GcOwnedRefObservationInError> {
        validate_observation_input(&observation, now_ms, safe_gc_age_ms)
            .map_err(GcOwnedRefObservationInError::Observation)?;
        let durable = self.durable.clone();
        let result = run_side_effect_free_in(
            self.store.as_ref(),
            self.metrics.as_ref(),
            self.policy,
            "record table-maintenance GC owned-ref observation",
            execution,
            move |transaction| {
                let observation = observation.clone();
                let durable = durable.clone();
                Box::pin(async move {
                    let mut transaction = OriginalObservation(transaction);
                    apply_observation(
                        &mut transaction,
                        &durable,
                        observation,
                        now_ms,
                        safe_gc_age_ms,
                    )
                    .await
                })
            },
        )
        .await;
        match result {
            Ok(success) => success
                .value
                .map_err(GcOwnedRefObservationInError::Observation),
            Err(failure) => Err(GcOwnedRefObservationInError::Run(failure)),
        }
    }

    /// Forget an observation once the provider has proven the ref absent or
    /// retired it by its own exact catalog CAS. Deletion does not decode the
    /// old value: an absent ref must never retain a corrupt stale clock.
    pub async fn remove(
        &self,
        table_uuid: Uuid,
        ref_name: String,
    ) -> Result<bool, GcOwnedRefObservationError> {
        let key = observation_key(&table_uuid, &ref_name)?;
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            self.policy,
            "remove table-maintenance GC owned-ref observation",
            move |transaction| {
                let key = key.clone();
                Box::pin(async move {
                    let Some(current) = transaction.get(&key).await? else {
                        return Ok(Ok(false));
                    };
                    transaction
                        .delete(key, Precondition::Version(current.version))
                        .await?;
                    Ok(Ok(true))
                })
            },
        )
        .await;
        match result {
            Ok(success) => success.value,
            Err(failure) => Err(format_run_failure(
                "remove table-maintenance GC owned-ref observation",
                failure,
            )),
        }
    }

    /// Destructively wipe only this accelerator family. This is idempotent and
    /// is the required target-side operation for a deployment clone.
    pub async fn wipe_family(&self) -> Result<u64, GcOwnedRefObservationError> {
        let mut deleted = 0_u64;
        loop {
            let prefix = GC_OWNED_REF_OBSERVATION_STATE_FAMILY
                .key()
                .map_err(|error| {
                    GcOwnedRefObservationError::store(format!(
                        "build GC wipe range failed: {error}"
                    ))
                })?;
            let range = KeyRange::for_prefix(prefix).map_err(|error| {
                GcOwnedRefObservationError::store(format!("build GC wipe range failed: {error}"))
            })?;
            let page_size = self.store.limits().max_page_size;
            let result = run_side_effect_free(
                self.store.as_ref(),
                self.metrics.as_ref(),
                self.policy,
                "wipe table-maintenance GC owned-ref observation family",
                move |transaction| {
                    let range = range.clone();
                    Box::pin(async move {
                        let page = transaction
                            .range(&RangeRequest {
                                range,
                                direction: Direction::Forward,
                                page_size,
                                continuation: None,
                            })
                            .await?;
                        let count = page.records.len() as u64;
                        for record in page.records {
                            transaction
                                .delete(record.key, Precondition::Version(record.version))
                                .await?;
                        }
                        Ok(Ok(count))
                    })
                },
            )
            .await;
            let count = match result {
                Ok(success) => success.value?,
                Err(failure) => {
                    return Err(format_run_failure(
                        "wipe table-maintenance GC owned-ref observation family",
                        failure,
                    ));
                }
            };
            deleted = deleted.checked_add(count).ok_or_else(|| {
                GcOwnedRefObservationError::store("GC observation wipe count overflow")
            })?;
            if count == 0 {
                return Ok(deleted);
            }
        }
    }
}

/// Durable v7 wire value. It intentionally duplicates the key identity so a
/// copied record cannot influence maturity under another table/ref key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredGcOwnedRefObservationV7 {
    schema_version: u8,
    table_uuid: Uuid,
    ref_name: String,
    head_snapshot_id: i64,
    provenance_version: u16,
    provenance_digest: [u8; 32],
    first_observed_at_ms: i64,
}

impl DurableRecord for StoredGcOwnedRefObservationV7 {
    const RECORD_KIND: &'static str = "table-maintenance-gc-owned-ref-observation";
    const SCHEMA_VERSION: u8 = GC_OWNED_REF_OBSERVATION_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = GC_OWNED_REF_OBSERVATION_RECORD_ENCODED_LIMIT;
}

impl From<&GcOwnedRefObservation> for StoredGcOwnedRefObservationV7 {
    fn from(value: &GcOwnedRefObservation) -> Self {
        Self {
            schema_version: GC_OWNED_REF_OBSERVATION_SCHEMA_VERSION,
            table_uuid: value.table_uuid,
            ref_name: value.ref_name.clone(),
            head_snapshot_id: value.head_snapshot_id,
            provenance_version: value.provenance_version,
            provenance_digest: value.provenance_digest,
            first_observed_at_ms: value.first_observed_at_ms,
        }
    }
}

impl TryFrom<StoredGcOwnedRefObservationV7> for GcOwnedRefObservation {
    type Error = String;

    fn try_from(value: StoredGcOwnedRefObservationV7) -> Result<Self, Self::Error> {
        if value.schema_version != GC_OWNED_REF_OBSERVATION_SCHEMA_VERSION {
            return Err("GC owned-ref observation has unsupported schema version".to_string());
        }
        Self::try_new(
            value.table_uuid,
            value.ref_name,
            value.head_snapshot_id,
            value.provenance_version,
            value.provenance_digest,
            value.first_observed_at_ms,
        )
    }
}

type ObservationFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
// Only the physical API differs. The record interpretation and maturity clock
// remain in the one apply_observation/put_not_mature algorithm below.
trait ObservationTransaction: Send {
    type Error: Send;
    fn get<'a>(
        &'a mut self,
        key: &'a Key,
    ) -> ObservationFuture<'a, Result<Option<StateRecord>, Self::Error>>;
    fn put<'a>(
        &'a mut self,
        durable: &'a DurableRecordStore,
        key: Key,
        record: EncodedRecord,
        precondition: Precondition,
    ) -> ObservationFuture<'a, Result<(), Self::Error>>;
}
struct LegacyObservation<'a>(&'a mut dyn WriteTransaction);
impl ObservationTransaction for LegacyObservation<'_> {
    type Error = StateStoreError;
    fn get<'a>(
        &'a mut self,
        key: &'a Key,
    ) -> ObservationFuture<'a, Result<Option<StateRecord>, Self::Error>> {
        Box::pin(self.0.get(key))
    }
    fn put<'a>(
        &'a mut self,
        durable: &'a DurableRecordStore,
        key: Key,
        record: EncodedRecord,
        precondition: Precondition,
    ) -> ObservationFuture<'a, Result<(), Self::Error>> {
        Box::pin(durable.put_record(self.0, key, record, precondition))
    }
}
struct OriginalObservation<'a>(&'a mut dyn OriginalWriteTransaction);
impl ObservationTransaction for OriginalObservation<'_> {
    type Error = StateStoreExecutionFailure;
    fn get<'a>(
        &'a mut self,
        key: &'a Key,
    ) -> ObservationFuture<'a, Result<Option<StateRecord>, Self::Error>> {
        Box::pin(self.0.get_in(key))
    }
    fn put<'a>(
        &'a mut self,
        durable: &'a DurableRecordStore,
        key: Key,
        record: EncodedRecord,
        precondition: Precondition,
    ) -> ObservationFuture<'a, Result<(), Self::Error>> {
        Box::pin(durable.put_encoded_in(self.0, key, record, precondition))
    }
}

async fn apply_observation<T: ObservationTransaction>(
    transaction: &mut T,
    durable: &DurableRecordStore,
    observation: GcOwnedRefObservation,
    now_ms: i64,
    safe_gc_age_ms: i64,
) -> Result<Result<GcOwnedRefObservationDecision, GcOwnedRefObservationError>, T::Error> {
    let key = match observation_key(&observation.table_uuid, &observation.ref_name) {
        Ok(key) => key,
        Err(error) => return Ok(Err(error)),
    };
    let current = transaction.get(&key).await?;
    let Some(current) = current else {
        let first = with_first_observed_at(observation, now_ms);
        return put_not_mature(transaction, durable, key, first, Precondition::Absent).await;
    };

    let prior = decode_observation(&current);
    let Some(prior) = prior.filter(|prior| {
        prior.table_uuid == observation.table_uuid && prior.ref_name == observation.ref_name
    }) else {
        let replacement = with_first_observed_at(observation, now_ms);
        return put_not_mature(
            transaction,
            durable,
            key,
            replacement,
            Precondition::Version(current.version),
        )
        .await;
    };

    if !prior.matches_facts(&observation) {
        let replacement = with_first_observed_at(observation, now_ms);
        return put_not_mature(
            transaction,
            durable,
            key,
            replacement,
            Precondition::Version(current.version),
        )
        .await;
    }

    if now_ms < prior.first_observed_at_ms {
        return Ok(Err(GcOwnedRefObservationError::corruption(
            "GC owned-ref observation wall clock moved backwards",
        )));
    }
    let mature_at_ms = match prior.first_observed_at_ms.checked_add(safe_gc_age_ms) {
        Some(value) => value,
        None => {
            return Ok(Err(GcOwnedRefObservationError::corruption(
                "GC owned-ref observation maturity time overflow",
            )));
        }
    };
    Ok(Ok(if now_ms > mature_at_ms {
        GcOwnedRefObservationDecision::Mature {
            first_observed_at_ms: prior.first_observed_at_ms,
        }
    } else {
        GcOwnedRefObservationDecision::NotMature {
            first_observed_at_ms: prior.first_observed_at_ms,
        }
    }))
}

async fn put_not_mature<T: ObservationTransaction>(
    transaction: &mut T,
    durable: &DurableRecordStore,
    key: Key,
    observation: GcOwnedRefObservation,
    precondition: Precondition,
) -> Result<Result<GcOwnedRefObservationDecision, GcOwnedRefObservationError>, T::Error> {
    let value = match encode_record(durable, &StoredGcOwnedRefObservationV7::from(&observation)) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    transaction.put(durable, key, value, precondition).await?;
    Ok(Ok(GcOwnedRefObservationDecision::NotMature {
        first_observed_at_ms: observation.first_observed_at_ms,
    }))
}

fn with_first_observed_at(
    observation: GcOwnedRefObservation,
    first_observed_at_ms: i64,
) -> GcOwnedRefObservation {
    GcOwnedRefObservation {
        first_observed_at_ms,
        ..observation
    }
}

fn validate_observation_input(
    observation: &GcOwnedRefObservation,
    now_ms: i64,
    safe_gc_age_ms: i64,
) -> Result<(), GcOwnedRefObservationError> {
    validate_observation(observation)?;
    if now_ms <= 0 {
        return Err(GcOwnedRefObservationError::corruption(
            "GC owned-ref observation time must be positive",
        ));
    }
    if safe_gc_age_ms <= 0 {
        return Err(GcOwnedRefObservationError::corruption(
            "GC owned-ref safe age must be positive",
        ));
    }
    Ok(())
}

fn validate_observation(
    observation: &GcOwnedRefObservation,
) -> Result<(), GcOwnedRefObservationError> {
    observation.validate().map_err(|error| {
        GcOwnedRefObservationError::corruption(format!("invalid GC owned-ref observation: {error}"))
    })
}

fn decode_observation(record: &StateRecord) -> Option<GcOwnedRefObservation> {
    let stored: StoredGcOwnedRefObservationV7 =
        serde_json::from_slice(record.value.as_bytes()).ok()?;
    let canonical = serde_json::to_vec(&stored).ok()?;
    if canonical.as_slice() != record.value.as_bytes() {
        return None;
    }
    GcOwnedRefObservation::try_from(stored).ok()
}

fn observation_key(table_uuid: &Uuid, ref_name: &str) -> Result<Key, GcOwnedRefObservationError> {
    GcOwnedRefObservation::validate_key(*table_uuid, ref_name).map_err(|error| {
        GcOwnedRefObservationError::corruption(format!("invalid GC owned-ref key: {error}"))
    })?;
    GC_OWNED_REF_OBSERVATION_STATE_FAMILY
        .key_with_suffix(&format!(
            "{}/{}",
            table_uuid,
            hex::encode(ref_name.as_bytes())
        ))
        .map_err(|error| {
            GcOwnedRefObservationError::store(format!(
                "build GC owned-ref observation key failed: {error}"
            ))
        })
}

fn encode_record<T: DurableRecord>(
    durable: &DurableRecordStore,
    value: &T,
) -> Result<EncodedRecord, GcOwnedRefObservationError> {
    durable.encode(value).map_err(|error| {
        GcOwnedRefObservationError::corruption(format!(
            "encode GC owned-ref observation failed: {error}"
        ))
    })
}

fn format_run_failure(context: &str, failure: RunFailure) -> GcOwnedRefObservationError {
    match failure {
        RunFailure::CommitUnknown { error, .. } => GcOwnedRefObservationError {
            kind: GcOwnedRefObservationErrorKind::CommitUnknown,
            message: format!("{context} failed: commit unknown: {error}"),
        },
        RunFailure::Begin(error) => {
            GcOwnedRefObservationError::store(format!("{context} failed: begin failed: {error}"))
        }
        RunFailure::Operation(error) => GcOwnedRefObservationError::store(format!(
            "{context} failed: operation failed: {error}"
        )),
        RunFailure::RetryExhausted(error) => {
            GcOwnedRefObservationError::store(format!("{context} failed: retry exhausted: {error}"))
        }
        RunFailure::DefiniteFailure(error) => {
            GcOwnedRefObservationError::store(format!("{context} failed: commit failed: {error}"))
        }
        RunFailure::DeadlineExceeded => GcOwnedRefObservationError::store(format!(
            "{context} failed: state store deadline exceeded"
        )),
    }
}

#[cfg(test)]
#[path = "gc_observation/tests_reopen.rs"]
mod reopen_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// These bytes are already in deployed stores.  The literals are repeated
    /// here rather than read from the manifest on purpose: an edit to the
    /// registered prefix has to be made twice, deliberately, or this assertion
    /// catches it before a live store is silently orphaned.
    #[test]
    fn key_bytes_are_stable_under_the_registered_prefix() {
        assert_eq!(
            GC_OWNED_REF_OBSERVATION_STATE_FAMILY
                .key()
                .expect("family prefix")
                .as_bytes(),
            b"novarocks/frontend/table-maintenance/v7/gc-owned-ref-observations/"
        );
        // The same key `tests/application_host.rs` seeds a corrupt record
        // under, so the two literals cross-check each other.
        assert_eq!(
            observation_key(&Uuid::from_u128(0x3c3), "__novarocks__corrupt")
                .expect("observation key")
                .as_bytes(),
            concat!(
                "novarocks/frontend/table-maintenance/v7/gc-owned-ref-observations/",
                "00000000-0000-0000-0000-0000000003c3/",
                "5f5f6e6f7661726f636b735f5f636f7272757074"
            )
            .as_bytes()
        );
    }

    /// The record version comes from the product descriptor and must remain
    /// the `7` already written to deployed stores.
    #[test]
    fn record_version_and_clone_policy_come_from_the_manifest() {
        let policy = GcOwnedRefObservationFamilyPolicy;
        assert_eq!(policy.family(), GC_OWNED_REF_OBSERVATION_STATE_FAMILY);
        assert_eq!(policy.schema_version(), 7);
        assert_eq!(
            policy.family_id(),
            "table-maintenance/gc-owned-ref-observation"
        );
        assert!(policy.retain_on_restart());
        assert!(policy.wipe_on_clone());
        assert_eq!(
            StoredGcOwnedRefObservationV7::SCHEMA_VERSION,
            policy.schema_version()
        );
    }
}

#[cfg(test)]
mod original_observation_tests {
    use super::*;
    use novarocks_state_store_api::{
        StateStoreBlockingOriginal, StateStoreBlockingSlot, StateStoreExecutionPort,
        StateStoreOriginalUnsupported,
    };
    use novarocks_state_store_testkit::testing::InMemoryStateStore;
    struct NoPhysicalPort;
    impl StateStoreExecutionPort for NoPhysicalPort {
        fn bind_original_in(
            &self,
            _: &Arc<dyn StateStoreBlockingOriginal>,
            _: std::time::Instant,
            _: &mut Option<Box<dyn StateStoreBlockingSlot>>,
        ) -> Result<(), StateStoreExecutionFailure> {
            panic!("unsupported provider cannot acquire a physical Source");
        }
        fn request_progress(&self) {
            panic!("no original source was installed");
        }
    }
    #[tokio::test]
    async fn observe_in_keeps_original_unsupported_and_never_writes_through_legacy() {
        let store = Arc::new(InMemoryStateStore::new("gc-original-no-fallback"));
        let accelerator =
            GcOwnedRefObservationAccelerator::open(store.clone(), StateStoreRunPolicy::default())
                .await
                .unwrap();
        let port: Arc<dyn StateStoreExecutionPort> = Arc::new(NoPhysicalPort);
        let execution = StateStoreExecutionContext::new(
            &port,
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        );
        let observation = GcOwnedRefObservation::try_new(
            Uuid::from_u128(31),
            "owned-ref".into(),
            1,
            1,
            [4; 32],
            1,
        )
        .unwrap();
        let failure = accelerator
            .observe_in(&execution, observation.clone(), 1000, 50)
            .await
            .unwrap_err();
        assert_eq!(failure.kind(), GcOwnedRefObservationErrorKind::Store);
        assert!(
            failure
                .original_failure()
                .unwrap()
                .downcast_ref::<StateStoreOriginalUnsupported>()
                .is_some()
        );
        // The first legacy call must still establish its OWN safety window.
        // An implicit legacy fallback above would have made this mature.
        assert_eq!(
            accelerator.observe(observation, 2000, 50).await.unwrap(),
            GcOwnedRefObservationDecision::NotMature {
                first_observed_at_ms: 2000
            }
        );
    }
    #[derive(Debug)]
    struct RawCause;
    impl fmt::Display for RawCause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("original raw physical cause")
        }
    }
    impl std::error::Error for RawCause {}
    impl novarocks_failure_contract::FailureProjection for RawCause {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            novarocks_failure_contract::FailureProjection::failure_fact(&StateStoreError::new(
                novarocks_state_store_api::StateStoreErrorKind::Transient,
                "actual test source failed",
            ))
        }
    }
    #[test]
    fn original_gc_error_retains_raw_arc_and_exact_unresolved_observation() {
        let store = InMemoryStateStore::new("gc-original-raw-carrier");
        let (attempt, observation) = store.attempts().reserve().unwrap();
        let id = observation.id();
        let raw =
            StateStoreExecutionFailure::from_static_detail(RawCause, "actual raw source failed");
        let failure = GcOwnedRefObservationInError::Run(RunFailure::CommitUnknown {
            observation,
            error: raw.clone(),
        });
        assert_eq!(
            failure.kind(),
            GcOwnedRefObservationErrorKind::CommitUnknown
        );
        assert_eq!(failure.original_failure().unwrap(), &raw);
        let captured = std::error::Error::source(&failure)
            .unwrap()
            .downcast_ref::<StateStoreExecutionFailure>()
            .unwrap();
        assert_eq!(captured, &raw);
        assert!(captured.downcast_ref::<RawCause>().is_some());
        let RunFailure::CommitUnknown { observation, .. } = failure.run_failure().unwrap() else {
            unreachable!()
        };
        assert_eq!(observation.id(), id);
        assert_eq!(observation.id(), attempt.id());
        // No commit or actual-exit proof is manufactured by the error carrier.
        assert!(observation.peek().unwrap().is_none());
    }
}
