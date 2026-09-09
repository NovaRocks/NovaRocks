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

use novarocks_state_store_api::{
    Direction, Key, KeyRange, Precondition, RangeRequest, StateRecord, StateStore, StateStoreError,
    WriteTransaction,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::durable::{DurableRecord, DurableRecordStore, EncodedRecord};
use crate::state_family::{ClonePolicy, PersistentKeyPrefix, StateFamily};
use crate::state_store::metrics::{StateStoreConsumer, StateStoreMetrics};
use crate::state_store::{RunFailure, StateStoreRunPolicy, run_side_effect_free};

/// This family's entry in the closed state family manifest.  Every declarative
/// fact below is read from it, so the module holds no second copy of the family
/// id, the record version, the prefix, or the retain/clone policy.
const MANIFEST_ENTRY: StateFamily = StateFamily::GcOwnedRefObservation;

pub const GC_OWNED_REF_OBSERVATION_FAMILY: &str = MANIFEST_ENTRY.family_id();
pub const GC_OWNED_REF_OBSERVATION_SCHEMA_VERSION: u8 = match MANIFEST_ENTRY.record_version() {
    Some(version) => version,
    None => panic!("GC owned-ref observation is a durable accelerator family"),
};
pub const GC_OWNED_REF_OBSERVATION_MAX_REF_NAME_BYTES: usize = 256;
pub const GC_OWNED_REF_OBSERVATION_RECORD_ENCODED_LIMIT: usize = 4 * 1024;

/// The prefix already ends in `/`, so the suffixes below are record paths alone.
const GC_OWNED_REF_OBSERVATION_PREFIX: PersistentKeyPrefix =
    match MANIFEST_ENTRY.persistent_prefix() {
        Some(prefix) => prefix,
        None => panic!("GC owned-ref observation is a durable accelerator family"),
    };

/// Thin accessor over this family's manifest entry.
///
/// It declares nothing of its own: the family id, record version, restart
/// retention and clone policy all resolve to [`MANIFEST_ENTRY`].  What stays
/// here is the *executable* clone wipe, which needs a live accelerator the
/// manifest cannot hold — the manifest declares only that this family wipes on
/// clone, not how to carry the wipe out.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcOwnedRefObservationFamilyPolicy;

impl GcOwnedRefObservationFamilyPolicy {
    /// The manifest entry every fact below is read from.
    pub const fn family(self) -> StateFamily {
        MANIFEST_ENTRY
    }

    pub const fn family_id(self) -> &'static str {
        GC_OWNED_REF_OBSERVATION_FAMILY
    }

    pub const fn schema_version(self) -> u8 {
        GC_OWNED_REF_OBSERVATION_SCHEMA_VERSION
    }

    pub const fn retain_on_restart(self) -> bool {
        MANIFEST_ENTRY.retain_on_restart()
    }

    pub const fn wipe_on_clone(self) -> bool {
        matches!(MANIFEST_ENTRY.clone_policy(), ClonePolicy::WipeAndRebuild)
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
    #[allow(clippy::too_many_arguments)]
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

/// The sole durable owner for GC first-observation records.
#[derive(Clone)]
pub struct GcOwnedRefObservationAccelerator {
    store: Arc<dyn StateStore>,
    durable: DurableRecordStore,
    metrics: Arc<StateStoreMetrics>,
    policy: StateStoreRunPolicy,
}

impl fmt::Debug for GcOwnedRefObservationAccelerator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GcOwnedRefObservationAccelerator")
            .field("family", &GC_OWNED_REF_OBSERVATION_FAMILY)
            .field("consumer", &self.metrics.consumer())
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
            metrics: Arc::new(StateStoreMetrics::new(StateStoreConsumer::GC_OBSERVATION)),
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
        validate_observation(&observation)?;
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

        let durable = self.durable.clone();
        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            self.policy,
            "record frontend GC owned-ref observation",
            move |transaction| {
                let observation = observation.clone();
                let durable = durable.clone();
                Box::pin(async move {
                    apply_observation(transaction, &durable, observation, now_ms, safe_gc_age_ms)
                        .await
                })
            },
        )
        .await;
        match result {
            Ok(success) => success.value,
            Err(failure) => Err(format_run_failure(
                "record frontend GC owned-ref observation",
                failure,
            )),
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
            "remove frontend GC owned-ref observation",
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
                "remove frontend GC owned-ref observation",
                failure,
            )),
        }
    }

    /// Destructively wipe only this accelerator family. This is idempotent and
    /// is the required target-side operation for a deployment clone.
    pub async fn wipe_family(&self) -> Result<u64, GcOwnedRefObservationError> {
        let mut deleted = 0_u64;
        loop {
            let prefix = GC_OWNED_REF_OBSERVATION_PREFIX.key().map_err(|error| {
                GcOwnedRefObservationError::store(format!("build GC wipe range failed: {error}"))
            })?;
            let range = KeyRange::for_prefix(prefix).map_err(|error| {
                GcOwnedRefObservationError::store(format!("build GC wipe range failed: {error}"))
            })?;
            let page_size = self.store.limits().max_page_size;
            let result = run_side_effect_free(
                self.store.as_ref(),
                self.metrics.as_ref(),
                self.policy,
                "wipe frontend GC owned-ref observation family",
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
                        "wipe frontend GC owned-ref observation family",
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

async fn apply_observation(
    transaction: &mut dyn WriteTransaction,
    durable: &DurableRecordStore,
    observation: GcOwnedRefObservation,
    now_ms: i64,
    safe_gc_age_ms: i64,
) -> Result<Result<GcOwnedRefObservationDecision, GcOwnedRefObservationError>, StateStoreError> {
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

async fn put_not_mature(
    transaction: &mut dyn WriteTransaction,
    durable: &DurableRecordStore,
    key: Key,
    observation: GcOwnedRefObservation,
    precondition: Precondition,
) -> Result<Result<GcOwnedRefObservationDecision, GcOwnedRefObservationError>, StateStoreError> {
    let value = match encode_record(durable, &StoredGcOwnedRefObservationV7::from(&observation)) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    durable
        .put_record(transaction, key, value, precondition)
        .await?;
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
    GC_OWNED_REF_OBSERVATION_PREFIX
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
mod tests {
    use super::*;

    /// These bytes are already in deployed stores.  The literals are repeated
    /// here rather than read from the manifest on purpose: an edit to the
    /// registered prefix has to be made twice, deliberately, or this assertion
    /// catches it before a live store is silently orphaned.
    #[test]
    fn key_bytes_are_stable_under_the_registered_prefix() {
        assert_eq!(
            GC_OWNED_REF_OBSERVATION_PREFIX
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

    /// The record version is the manifest's, not a second literal, and it must
    /// still be the `7` already written to deployed stores.
    #[test]
    fn record_version_and_clone_policy_come_from_the_manifest() {
        let policy = GcOwnedRefObservationFamilyPolicy;
        assert_eq!(policy.family(), StateFamily::GcOwnedRefObservation);
        assert_eq!(policy.schema_version(), 7);
        assert_eq!(
            policy.family_id(),
            "frontend/table-maintenance/gc-owned-ref-observation"
        );
        assert!(policy.retain_on_restart());
        assert!(policy.wipe_on_clone());
        assert_eq!(
            StoredGcOwnedRefObservationV7::SCHEMA_VERSION,
            policy.schema_version()
        );
    }
}
