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

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use crate::state_store::metrics::{StateStoreConsumer, StateStoreMetrics};
use crate::state_store::{RunFailure, StateStoreRunPolicy, run_side_effect_free};
use novarocks_spi::connector::{
    CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose,
    CatalogNonSecretProperty, ConnectorInstanceId, ConnectorProviderId, CredentialConsumerRole,
    MAX_CATALOG_NON_SECRET_PROPERTIES, StaticCredentialReference,
    canonicalize_catalog_credential_bindings,
};
use novarocks_state_store_api::{
    AttemptOutcome, Direction, KeyRange, Precondition, RangeRequest, StateRecord, StateStore,
    StateStoreError, StateStoreErrorKind, VersionToken,
};
use uuid::Uuid;

use crate::durable::{DurableRecordError, DurableRecordStore};

use super::codec::{
    CATALOG_ATTACHMENT_SCHEMA_VERSION, StoredCatalogAttachment, StoredCredentialBinding,
    StoredProperty, decode, encode,
};
use super::key::{attachment_key, attachment_prefix};
use super::wakeup::{CatalogAttachmentWakeup, CatalogAttachmentWakeupSignal};

const DEFAULT_ATTACHMENT_SCAN_PAGE_SIZE: usize = 256;

/// How many times one logical create/drop may be replayed after the store
/// proved the previous attempt did not commit.
///
/// A proven `NotCommitted` licenses a replay, and each replay is new work
/// under a new attempt identity. The bound exists only so a store that keeps
/// losing commit responses ends in a reported failure rather than an unbounded
/// loop; it is not a retry policy, which belongs to
/// [`StateStoreRunPolicy`] and is applied inside each attempt.
const MAX_PROVEN_UNCOMMITTED_REPLAYS: usize = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogAttachment {
    pub attachment_id: Uuid,
    pub instance_id: ConnectorInstanceId,
    pub provider_id: ConnectorProviderId,
    pub display_name: String,
    pub durable_properties: Vec<(String, String)>,
    pub credential_bindings: Vec<CatalogCredentialBinding>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogAttachmentVersioned {
    pub attachment: CatalogAttachment,
    pub version: VersionToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogAttachmentErrorKind {
    InvalidRequest,
    NotFound,
    AlreadyExists,
    Conflict,
    Corruption,
    Unavailable,
    CommitUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogAttachmentError {
    kind: CatalogAttachmentErrorKind,
    message: String,
}

impl CatalogAttachmentError {
    pub fn kind(&self) -> CatalogAttachmentErrorKind {
        self.kind
    }

    fn new(kind: CatalogAttachmentErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for CatalogAttachmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CatalogAttachmentError {}

// Design: ADR-0115 (docs/adr/ADR-0115-catalog-desired-state-source-modes.md)
#[derive(Clone)]
pub struct CatalogAttachmentRepository {
    store: Arc<dyn StateStore>,
    durable: DurableRecordStore,
    metrics: Arc<StateStoreMetrics>,
    policy: StateStoreRunPolicy,
    /// Wakeups for this repository instance's own committed writes. Shared by
    /// every clone, so the port that writes and the controller that reads see
    /// one channel.
    wakeup: Arc<CatalogAttachmentWakeup>,
}

impl CatalogAttachmentRepository {
    pub async fn open(
        store: Arc<dyn StateStore>,
        policy: StateStoreRunPolicy,
    ) -> Result<Self, CatalogAttachmentError> {
        let repository = Self {
            durable: DurableRecordStore::new(Arc::clone(&store)),
            // A business owner, not the storage provider. This consumer used to
            // invent a `frontend-catalog` provider identity so it had something
            // to label counters with, which attributed catalog retries to a
            // provider that does not exist.
            metrics: Arc::new(StateStoreMetrics::new(
                StateStoreConsumer::CATALOG_ATTACHMENT,
            )),
            store,
            policy,
            wakeup: Arc::new(CatalogAttachmentWakeup::new()),
        };
        repository.list().await?;
        Ok(repository)
    }

    /// Subscribes to committed-write wakeups from this repository instance.
    ///
    /// Deliberately lossy: it reports only writes made through *this*
    /// instance, so a consumer must own a periodic sweep and must never treat
    /// the absence of a wakeup as the absence of a write.
    pub fn wakeup_signal(&self) -> CatalogAttachmentWakeupSignal {
        self.wakeup.subscribe()
    }

    /// Wakeups this instance has published. Observability and tests only.
    pub fn published_wakeups(&self) -> u64 {
        self.wakeup.published()
    }

    pub async fn get(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Option<CatalogAttachmentVersioned>, CatalogAttachmentError> {
        let key = attachment_key(instance_id).map_err(invalid)?;
        let mut transaction = self.store.begin_read().await.map_err(store)?;
        let record = transaction.get(&key).await.map_err(store)?;
        transaction.abort().await.map_err(store)?;
        record.map(decode_record).transpose()
    }

    pub async fn list(&self) -> Result<Vec<CatalogAttachmentVersioned>, CatalogAttachmentError> {
        self.list_with_page_size(DEFAULT_ATTACHMENT_SCAN_PAGE_SIZE)
            .await
    }

    /// Enumerates the complete attachment prefix, or fails.
    ///
    /// This is the dynamic StateStore mode's snapshot-enumeration entry point,
    /// and the "complete or fail" shape is what its caller depends on: a
    /// catalog desired-state snapshot is total truth, so a reconcile retires
    /// every catalog the enumeration did not return. Returning the pages that
    /// happened to read successfully would therefore delete catalogs nobody
    /// asked to remove. Every failure below — a page read, an undecodable
    /// record, an unsupported record version — aborts the whole enumeration
    /// instead of shortening it.
    pub async fn list_with_page_size(
        &self,
        page_size: usize,
    ) -> Result<Vec<CatalogAttachmentVersioned>, CatalogAttachmentError> {
        if page_size == 0 || page_size > self.store.limits().max_page_size {
            return Err(CatalogAttachmentError::new(
                CatalogAttachmentErrorKind::InvalidRequest,
                "catalog attachment scan page size is outside StateStore limits",
            ));
        }
        let prefix = attachment_prefix().map_err(invalid)?;
        let range = KeyRange::for_prefix(prefix).map_err(store)?;
        let mut transaction = self.store.begin_read().await.map_err(store)?;
        let mut request = RangeRequest {
            range,
            direction: Direction::Forward,
            page_size,
            continuation: None,
        };
        let mut attachments = Vec::new();
        loop {
            let page = transaction.range(&request).await.map_err(store)?;
            attachments.extend(
                page.records
                    .into_iter()
                    .map(decode_record)
                    .collect::<Result<Vec<_>, _>>()?,
            );
            let Some(continuation) = page.continuation else {
                break;
            };
            request.continuation = Some(continuation);
        }
        transaction.abort().await.map_err(store)?;
        attachments.sort_by(|left, right| {
            left.attachment
                .instance_id
                .cmp(&right.attachment.instance_id)
        });
        Ok(attachments)
    }

    /// Creates one attachment under an absent-precondition CAS.
    ///
    /// # Losing sight of the commit
    ///
    /// A lost commit response is answered by asking the attempt that was in
    /// flight, through the commit observation the failure carries. There is
    /// no reconstructed identity and no "replay under the same id": the
    /// attempt is addressed directly, and a replay is new work under a new
    /// attempt, which is the only reading of `NotCommitted` that is honest —
    /// the previous attempt is proven never to commit, so nothing about it is
    /// worth resuming.
    pub async fn create(
        &self,
        attachment: CatalogAttachment,
    ) -> Result<CatalogAttachmentVersioned, CatalogAttachmentError> {
        validate_attachment(&attachment)?;
        let key = attachment_key(&attachment.instance_id).map_err(invalid)?;
        let value = encode(&self.durable, &stored_from(&attachment)).map_err(durable_record)?;
        let mut replays = 0_usize;
        loop {
            let outcome = run_side_effect_free(
                self.store.as_ref(),
                self.metrics.as_ref(),
                self.policy,
                "create catalog attachment",
                |transaction| {
                    let key = key.clone();
                    let value = value.clone();
                    let durable = self.durable.clone();
                    Box::pin(async move {
                        durable
                            .put_record(transaction, key, value, Precondition::Absent)
                            .await?;
                        Ok(())
                    })
                },
            )
            .await;
            let unresolved = match outcome {
                Ok(_) => return self.committed_create(&attachment).await,
                Err(RunFailure::Operation(error) | RunFailure::RetryExhausted(error))
                    if error.kind() == StateStoreErrorKind::PreconditionFailed =>
                {
                    return Err(CatalogAttachmentError::new(
                        CatalogAttachmentErrorKind::AlreadyExists,
                        "catalog attachment already exists",
                    ));
                }
                Err(RunFailure::CommitUnknown { observation, error }) => {
                    match observation.outcome().await.map_err(store)? {
                        AttemptOutcome::Committed(_) => {
                            return self.committed_create(&attachment).await;
                        }
                        AttemptOutcome::NotCommitted => {
                            if replays >= MAX_PROVEN_UNCOMMITTED_REPLAYS {
                                return Err(CatalogAttachmentError::new(
                                    // Proven not to have landed, so this is an
                                    // availability answer, not an unknown one.
                                    CatalogAttachmentErrorKind::Unavailable,
                                    format!(
                                        "create catalog attachment kept losing its commit \
                                         response and is proven not to have committed: {error}"
                                    ),
                                ));
                            }
                            replays += 1;
                            continue;
                        }
                        AttemptOutcome::Unresolved => error,
                    }
                }
                Err(error) => return Err(run_failure("create catalog attachment", error)),
            };
            // Nothing can be proven about the attempt, so the authoritative
            // record is the last word. Our exact identity being there means the
            // write did land: nobody else could have minted it.
            return match self.matching(&attachment).await? {
                Some(found) => {
                    self.wakeup.publish();
                    Ok(found)
                }
                None => Err(CatalogAttachmentError::new(
                    CatalogAttachmentErrorKind::CommitUnknown,
                    format!("create catalog attachment commit outcome is unknown: {unresolved}"),
                )),
            };
        }
    }

    /// Deletes exactly the frozen attachment record, in a transaction that
    /// touches the catalog attachment family and nothing else.
    ///
    /// The version precondition is the whole fence: it fails the delete if the
    /// record changed — including a same-name drop/recreate — since the caller
    /// read it. A lost commit response is not folded into that; it is resolved
    /// through the in-flight attempt's own observation, exactly as `create`
    /// resolves it.
    pub async fn drop_exact(
        &self,
        expected: CatalogAttachmentVersioned,
    ) -> Result<(), CatalogAttachmentError> {
        let key = attachment_key(&expected.attachment.instance_id).map_err(invalid)?;
        let mut replays = 0_usize;
        loop {
            let outcome = run_side_effect_free(
                self.store.as_ref(),
                self.metrics.as_ref(),
                self.policy,
                "drop catalog attachment",
                |transaction| {
                    let key = key.clone();
                    let version = expected.version.clone();
                    Box::pin(async move {
                        transaction
                            .delete(key, Precondition::Version(version))
                            .await?;
                        Ok(())
                    })
                },
            )
            .await;
            let unresolved = match outcome {
                Ok(_) => {
                    self.wakeup.publish();
                    return Ok(());
                }
                Err(RunFailure::Operation(error) | RunFailure::RetryExhausted(error))
                    if matches!(
                        error.kind(),
                        StateStoreErrorKind::PreconditionFailed | StateStoreErrorKind::Conflict
                    ) =>
                {
                    return Err(CatalogAttachmentError::new(
                        CatalogAttachmentErrorKind::Conflict,
                        "catalog attachment changed before drop",
                    ));
                }
                Err(RunFailure::CommitUnknown { observation, error }) => {
                    match observation.outcome().await.map_err(store)? {
                        AttemptOutcome::Committed(_) => {
                            self.wakeup.publish();
                            return Ok(());
                        }
                        AttemptOutcome::NotCommitted => {
                            if replays >= MAX_PROVEN_UNCOMMITTED_REPLAYS {
                                return Err(CatalogAttachmentError::new(
                                    CatalogAttachmentErrorKind::Unavailable,
                                    format!(
                                        "drop catalog attachment kept losing its commit response \
                                         and is proven not to have committed: {error}"
                                    ),
                                ));
                            }
                            replays += 1;
                            continue;
                        }
                        AttemptOutcome::Unresolved => error,
                    }
                }
                Err(error) => return Err(run_failure("drop catalog attachment", error)),
            };
            // Undecidable: fall back on the authoritative record. The exact
            // identity still being present is the only reading under which the
            // delete demonstrably did not happen.
            return match self.get(&expected.attachment.instance_id).await? {
                Some(current)
                    if current.attachment.attachment_id == expected.attachment.attachment_id =>
                {
                    Err(CatalogAttachmentError::new(
                        CatalogAttachmentErrorKind::CommitUnknown,
                        format!("drop catalog attachment commit outcome is unknown: {unresolved}"),
                    ))
                }
                _ => {
                    self.wakeup.publish();
                    Ok(())
                }
            };
        }
    }

    /// Best-effort operational check: refuse the drop while the MV Accelerator
    /// still names this catalog.
    ///
    /// This is the honest replacement for a scan that used to run inside the
    /// attachment delete transaction. Two facts make a transaction the wrong
    /// tool here, and the error text says so rather than letting a caller
    /// inherit a guarantee that is not being provided:
    ///
    /// * The family read here is an `Accelerator`. It is rebuildable and may
    ///   legitimately be wiped in whole, so a wiped, still-rebuilding or
    ///   unreadable Accelerator lets a real reference slip straight past this
    ///   check. An unreadable read is therefore *not* an error: it produced no
    ///   observation, and this check refuses only on an observation.
    /// * The concurrency it appeared to exclude was never excluded. MV DDL on
    ///   another frontend and an external catalog desired-state controller are
    ///   not participants in the attachment transaction, so a reference could
    ///   always appear immediately after the scan.
    ///
    /// What escapes the check is bounded: an MV whose catalog is gone. The MV
    /// side already reports that through its existing unavailable/fail-closed
    /// paths, so the outcome is a refused refresh, never a wrong lake
    /// publication.
    pub(crate) async fn observe_materialized_view_references(
        &self,
        instance_id: &ConnectorInstanceId,
        page_size: usize,
    ) -> Result<(), CatalogAttachmentError> {
        match crate::mv::repository::observe_catalog_references(
            self.store.as_ref(),
            instance_id.as_str(),
            page_size,
        )
        .await
        {
            Ok(None) => Ok(()),
            Ok(Some(reference)) => Err(CatalogAttachmentError::new(
                CatalogAttachmentErrorKind::Conflict,
                format!(
                    "catalog {} still has {} in the materialized view accelerator; \
                     this is a best-effort operational check, not a cross-system \
                     serializability guarantee: drop the referencing materialized \
                     views before dropping the catalog",
                    instance_id.as_str(),
                    reference.describe(),
                ),
            )),
            Err(error) => {
                tracing::warn!(
                    %error,
                    catalog = instance_id.as_str(),
                    "materialized view reference check could not read the accelerator; \
                     the catalog drop proceeds without an observation",
                );
                Ok(())
            }
        }
    }

    /// Confirms a committed create against the authoritative record and wakes
    /// the local reconciler.
    ///
    /// The wakeup is published only here and on a committed drop, so it always
    /// means "a write of ours landed". An attempt whose outcome is unknown
    /// publishes nothing: a wakeup that turned out to mean "maybe" would train
    /// the consumer to reconcile on non-events.
    async fn committed_create(
        &self,
        attachment: &CatalogAttachment,
    ) -> Result<CatalogAttachmentVersioned, CatalogAttachmentError> {
        let created = self.require_matching(attachment).await?;
        self.wakeup.publish();
        Ok(created)
    }

    async fn matching(
        &self,
        expected: &CatalogAttachment,
    ) -> Result<Option<CatalogAttachmentVersioned>, CatalogAttachmentError> {
        Ok(self
            .get(&expected.instance_id)
            .await?
            .filter(|current| current.attachment.attachment_id == expected.attachment_id))
    }

    async fn require_matching(
        &self,
        expected: &CatalogAttachment,
    ) -> Result<CatalogAttachmentVersioned, CatalogAttachmentError> {
        self.matching(expected).await?.ok_or_else(|| {
            CatalogAttachmentError::new(
                CatalogAttachmentErrorKind::CommitUnknown,
                "catalog attachment commit resolved but authoritative record does not match",
            )
        })
    }
}

/// Re-check frozen attachment observations inside a caller-owned StateStore
/// write transaction.  This is deliberately crate-visible rather than a
/// repository cross-call: the caller owns the transaction which couples an MV
/// definition/index write to catalog attachment existence.
pub(crate) async fn assert_attachment_versions(
    transaction: &mut dyn novarocks_state_store_api::WriteTransaction,
    expected: &[CatalogAttachmentVersioned],
) -> Result<(), StateStoreError> {
    for expected in expected {
        let key = attachment_key(&expected.attachment.instance_id).map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "invalid catalog attachment observation key",
            )
        })?;
        let Some(record) = transaction.get(&key).await? else {
            return Err(StateStoreError::new(
                StateStoreErrorKind::Conflict,
                "catalog attachment disappeared before materialized view write",
            ));
        };
        if record.version != expected.version {
            return Err(StateStoreError::new(
                StateStoreErrorKind::Conflict,
                "catalog attachment changed before materialized view write",
            ));
        }
        let current = decode_record(record).map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::Corruption,
                "catalog attachment observation is corrupt",
            )
        })?;
        if current.attachment.attachment_id != expected.attachment.attachment_id {
            return Err(StateStoreError::new(
                StateStoreErrorKind::Conflict,
                "catalog attachment identity changed before materialized view write",
            ));
        }
    }
    Ok(())
}

fn decode_record(
    record: StateRecord,
) -> Result<CatalogAttachmentVersioned, CatalogAttachmentError> {
    let stored = decode(record.value.as_bytes()).map_err(corruption)?;
    let attachment = attachment_from(stored)?;
    let expected_key = attachment_key(&attachment.instance_id).map_err(corruption)?;
    if record.key != expected_key {
        return Err(corruption(
            "catalog attachment key does not match record identity",
        ));
    }
    Ok(CatalogAttachmentVersioned {
        attachment,
        version: record.version,
    })
}

fn stored_from(attachment: &CatalogAttachment) -> StoredCatalogAttachment {
    StoredCatalogAttachment {
        schema_version: CATALOG_ATTACHMENT_SCHEMA_VERSION,
        attachment_id: attachment.attachment_id.to_string(),
        instance_id: attachment.instance_id.as_str().to_string(),
        provider_id: attachment.provider_id.as_str().to_string(),
        display_name: attachment.display_name.clone(),
        durable_properties: attachment
            .durable_properties
            .iter()
            .map(|(key, value)| StoredProperty {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
        credential_bindings: attachment
            .credential_bindings
            .iter()
            .map(stored_binding_from)
            .collect(),
        created_at_ms: attachment.created_at_ms,
    }
}

fn attachment_from(
    stored: StoredCatalogAttachment,
) -> Result<CatalogAttachment, CatalogAttachmentError> {
    let attachment = CatalogAttachment {
        attachment_id: Uuid::parse_str(&stored.attachment_id)
            .map_err(|error| corruption(format!("invalid catalog attachment UUID: {error}")))?,
        instance_id: ConnectorInstanceId::parse(&stored.instance_id)
            .map_err(|error| corruption(error.to_string()))?,
        provider_id: ConnectorProviderId::parse(&stored.provider_id)
            .map_err(|error| corruption(error.to_string()))?,
        display_name: stored.display_name,
        durable_properties: stored
            .durable_properties
            .into_iter()
            .map(|property| (property.key, property.value))
            .collect(),
        credential_bindings: stored
            .credential_bindings
            .into_iter()
            .map(binding_from_stored)
            .collect::<Result<Vec<_>, _>>()?,
        created_at_ms: stored.created_at_ms,
    };
    validate_attachment(&attachment).map_err(|error| {
        CatalogAttachmentError::new(CatalogAttachmentErrorKind::Corruption, error.message)
    })?;
    Ok(attachment)
}

fn validate_attachment(attachment: &CatalogAttachment) -> Result<(), CatalogAttachmentError> {
    if attachment.display_name.trim().is_empty() {
        return Err(invalid("catalog attachment display name must not be empty"));
    }
    if attachment.durable_properties.len() > MAX_CATALOG_NON_SECRET_PROPERTIES {
        return Err(invalid(format!(
            "catalog attachment declares more than {MAX_CATALOG_NON_SECRET_PROPERTIES} non-secret properties"
        )));
    }
    let mut previous = None;
    let mut keys = BTreeSet::new();
    for (key, value) in &attachment.durable_properties {
        CatalogNonSecretProperty::try_new(key, value)
            .map_err(|error| invalid(error.to_string()))?;
        if !keys.insert(key.as_str()) {
            return Err(invalid(format!(
                "duplicate catalog attachment property: {key}"
            )));
        }
        if previous.is_some_and(|last: &str| last >= key.as_str()) {
            return Err(invalid(
                "catalog attachment properties must be sorted by key",
            ));
        }
        previous = Some(key);
    }
    let canonical =
        canonicalize_catalog_credential_bindings(attachment.credential_bindings.clone())
            .map_err(|error| invalid(format!("invalid catalog credential bindings: {error}")))?;
    if canonical != attachment.credential_bindings {
        return Err(invalid(
            "catalog credential bindings must use canonical order",
        ));
    }
    Ok(())
}

fn stored_binding_from(binding: &CatalogCredentialBinding) -> StoredCredentialBinding {
    let (mode, name, generation) = match binding.mode() {
        CatalogCredentialMode::Static(reference) => (
            "static",
            Some(reference.name().to_string()),
            Some(reference.generation().to_string()),
        ),
        CatalogCredentialMode::Vended => ("vended", None, None),
    };
    StoredCredentialBinding {
        purpose: match binding.purpose() {
            CatalogCredentialPurpose::CatalogControl => "catalog-control",
            CatalogCredentialPurpose::ObjectStoreData => "object-store-data",
            CatalogCredentialPurpose::ObjectStoreMetadata => "object-store-metadata",
        }
        .to_string(),
        consumer_role: match binding.consumer_role() {
            CredentialConsumerRole::Frontend => "frontend",
            CredentialConsumerRole::Backend => "backend",
            CredentialConsumerRole::FrontendAndBackend => "frontend-and-backend",
        }
        .to_string(),
        mode: mode.to_string(),
        name,
        generation,
    }
}

fn binding_from_stored(
    stored: StoredCredentialBinding,
) -> Result<CatalogCredentialBinding, CatalogAttachmentError> {
    let purpose = match stored.purpose.as_str() {
        "catalog-control" => CatalogCredentialPurpose::CatalogControl,
        "object-store-data" => CatalogCredentialPurpose::ObjectStoreData,
        "object-store-metadata" => CatalogCredentialPurpose::ObjectStoreMetadata,
        _ => return Err(corruption("unknown catalog credential purpose")),
    };
    let consumer_role = match stored.consumer_role.as_str() {
        "frontend" => CredentialConsumerRole::Frontend,
        "backend" => CredentialConsumerRole::Backend,
        "frontend-and-backend" => CredentialConsumerRole::FrontendAndBackend,
        _ => return Err(corruption("unknown catalog credential consumer role")),
    };
    let mode = match stored.mode.as_str() {
        "static" => {
            let name = stored
                .name
                .as_deref()
                .ok_or_else(|| corruption("static catalog credential binding requires name"))?;
            let generation = stored.generation.as_deref().ok_or_else(|| {
                corruption("static catalog credential binding requires generation")
            })?;
            CatalogCredentialMode::Static(
                StaticCredentialReference::try_new(name, generation)
                    .map_err(|error| corruption(error.to_string()))?,
            )
        }
        "vended" => {
            if stored.name.is_some() || stored.generation.is_some() {
                return Err(corruption(
                    "vended catalog credential binding forbids name and generation",
                ));
            }
            CatalogCredentialMode::Vended
        }
        _ => return Err(corruption("unknown catalog credential mode")),
    };
    CatalogCredentialBinding::try_new(purpose, consumer_role, mode)
        .map_err(|error| corruption(error.to_string()))
}

fn invalid(message: impl Into<String>) -> CatalogAttachmentError {
    CatalogAttachmentError::new(CatalogAttachmentErrorKind::InvalidRequest, message)
}

fn corruption(message: impl Into<String>) -> CatalogAttachmentError {
    CatalogAttachmentError::new(CatalogAttachmentErrorKind::Corruption, message)
}

fn durable_record(error: DurableRecordError) -> CatalogAttachmentError {
    let kind = match error {
        DurableRecordError::BudgetExceeded { .. }
        | DurableRecordError::OpaqueBytesOutOfBounds { .. }
        | DurableRecordError::SmallValueBudgetExceeded { .. } => {
            CatalogAttachmentErrorKind::InvalidRequest
        }
        DurableRecordError::EncodingFailed { .. } => CatalogAttachmentErrorKind::Corruption,
        DurableRecordError::Store(_) => CatalogAttachmentErrorKind::Unavailable,
    };
    CatalogAttachmentError::new(kind, error.to_string())
}

fn store(error: StateStoreError) -> CatalogAttachmentError {
    CatalogAttachmentError::new(CatalogAttachmentErrorKind::Unavailable, error.to_string())
}

fn run_failure(context: &str, failure: RunFailure) -> CatalogAttachmentError {
    let (kind, message) = match failure {
        RunFailure::Operation(error) if error.kind() == StateStoreErrorKind::PreconditionFailed => {
            (CatalogAttachmentErrorKind::Conflict, error.to_string())
        }
        RunFailure::CommitUnknown { error, .. } => {
            (CatalogAttachmentErrorKind::CommitUnknown, error.to_string())
        }
        error => (
            CatalogAttachmentErrorKind::Unavailable,
            format!("{error:?}"),
        ),
    };
    CatalogAttachmentError::new(kind, format!("{context}: {message}"))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::mv::domain::dependency::model::{
        MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
    };
    use crate::state_store::testing::{
        StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
        StateStoreLimitOverrides, StateStoreProviderConfig, TEST_STATE_STORE_PROVIDER_ID,
        builtin_state_store_provider_registry,
    };
    use bytes::Bytes;
    use novarocks_state_store_api::{CommitOutcome, Precondition, StateStore};
    use novarocks_state_store_testkit::conformance::{FaultGate, FaultInjectingStateStore};

    use super::*;

    /// Reserves one write attempt from the store that will run it.
    ///
    /// Seeding used to mint a `TransactionId` from a fresh UUID, which is the
    /// exact shape the attempt contract removed: an identity the store never
    /// issued, that the store could then be asked about. Reserving keeps the
    /// fixture on the same path production uses.
    fn reserve(store: &dyn StateStore) -> novarocks_state_store_api::WriteAttempt {
        store
            .attempts()
            .reserve()
            .expect("reserve a seeding write attempt")
            .0
    }

    fn object_store_binding(generation: &str) -> CatalogCredentialBinding {
        CatalogCredentialBinding::try_new(
            CatalogCredentialPurpose::ObjectStoreData,
            CredentialConsumerRole::Backend,
            CatalogCredentialMode::Static(
                StaticCredentialReference::try_new("warehouse-data", generation)
                    .expect("credential reference"),
            ),
        )
        .expect("credential binding")
    }

    fn metadata_store_binding(generation: &str) -> CatalogCredentialBinding {
        CatalogCredentialBinding::try_new(
            CatalogCredentialPurpose::ObjectStoreMetadata,
            CredentialConsumerRole::Frontend,
            CatalogCredentialMode::Static(
                StaticCredentialReference::try_new("warehouse-metadata", generation)
                    .expect("credential reference"),
            ),
        )
        .expect("credential binding")
    }

    fn attachment(properties: Vec<(String, String)>) -> CatalogAttachment {
        CatalogAttachment {
            attachment_id: Uuid::now_v7(),
            instance_id: ConnectorInstanceId::parse("Warehouse.Main").expect("instance"),
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            display_name: "Warehouse.Main".to_string(),
            durable_properties: properties,
            credential_bindings: vec![object_store_binding("blue")],
            created_at_ms: 1,
        }
    }

    #[test]
    fn metadata_binding_uses_a_stable_persistent_name() {
        let binding = metadata_store_binding("blue");
        let stored = stored_binding_from(&binding);
        assert_eq!(stored.purpose, "object-store-metadata");
        assert_eq!(
            binding_from_stored(stored).expect("stored binding"),
            binding
        );
    }

    #[test]
    fn legacy_shared_data_binding_fails_closed_after_raw_record_decode() {
        let error = binding_from_stored(StoredCredentialBinding {
            purpose: "object-store-data".to_string(),
            consumer_role: "frontend-and-backend".to_string(),
            mode: "static".to_string(),
            name: Some("warehouse-data".to_string()),
            generation: Some("blue".to_string()),
        })
        .expect_err("legacy shared data authority must not become a catalog binding");

        assert_eq!(error.kind(), CatalogAttachmentErrorKind::Corruption);
        assert!(
            error
                .to_string()
                .contains("catalog credential purpose, role, and mode combination")
        );
    }

    #[test]
    fn durable_properties_are_sorted_and_non_sensitive() {
        assert!(validate_attachment(&attachment(vec![("type".into(), "iceberg".into())])).is_ok());
        assert_eq!(
            validate_attachment(&attachment(vec![("password".into(), "x".into())]))
                .expect_err("credential-like property must fail")
                .kind(),
            CatalogAttachmentErrorKind::InvalidRequest
        );
        assert!(
            validate_attachment(&attachment(vec![
                ("z".into(), "1".into()),
                ("a".into(), "2".into()),
            ]))
            .is_err()
        );
    }

    #[tokio::test]
    async fn sqlite_create_is_absent_cas_and_drop_requires_the_frozen_version() {
        let directory = tempfile::tempdir().expect("temporary SQLite StateStore directory");
        let registry =
            builtin_state_store_provider_registry().expect("builtin StateStore registry");
        let mut host = StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "catalog-attachment-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: directory.path().join("state-store.sqlite"),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("open SQLite StateStore");
        assert_eq!(host.provider_id(), TEST_STATE_STORE_PROVIDER_ID);
        let store = host.state_store().expect("ready StateStore");
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");

        let first = repository
            .create(attachment(vec![("type".into(), "iceberg".into())]))
            .await
            .expect("first create");
        assert_eq!(
            repository
                .create(attachment(vec![("type".into(), "iceberg".into())]))
                .await
                .expect_err("second create must conflict")
                .kind(),
            CatalogAttachmentErrorKind::AlreadyExists
        );
        repository
            .drop_exact(first.clone())
            .await
            .expect("exact drop");
        let replacement = repository
            .create(attachment(vec![("type".into(), "iceberg".into())]))
            .await
            .expect("recreate after drop");
        assert_ne!(
            first.attachment.attachment_id,
            replacement.attachment.attachment_id
        );
        assert_eq!(
            repository
                .drop_exact(first)
                .await
                .expect_err("stale exact delete must conflict")
                .kind(),
            CatalogAttachmentErrorKind::Conflict
        );

        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    #[tokio::test]
    async fn restart_reconstructs_the_exact_canonical_binding() {
        let directory = tempfile::tempdir().expect("temporary SQLite StateStore directory");
        let registry =
            builtin_state_store_provider_registry().expect("builtin StateStore registry");
        let mut host = StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "catalog-attachment-binding-restart-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: directory.path().join("state-store.sqlite"),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("open SQLite StateStore");
        let store = host.state_store().expect("ready StateStore");
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let requested = attachment(vec![("type".into(), "iceberg".into())]);
        let created = repository
            .create(requested.clone())
            .await
            .expect("create attachment");
        drop(repository);

        let reopened = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("reopen catalog attachment repository");
        let reconstructed = reopened
            .get(&requested.instance_id)
            .await
            .expect("read attachment")
            .expect("attachment remains");
        assert_eq!(reconstructed.attachment, created.attachment);
        assert_eq!(
            reconstructed.attachment.credential_bindings,
            vec![object_store_binding("blue")]
        );

        drop(reopened);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    #[tokio::test]
    async fn create_rejects_an_over_budget_attachment_without_writing_a_record() {
        let directory = tempfile::tempdir().expect("temporary SQLite StateStore directory");
        let registry =
            builtin_state_store_provider_registry().expect("builtin StateStore registry");
        let mut host = StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "catalog-attachment-budget-test".to_string(),
                        limits: StateStoreLimitOverrides {
                            max_value_bytes: Some(256),
                            ..StateStoreLimitOverrides::default()
                        },
                        provider: StateStoreProviderConfig::Sqlite {
                            path: directory.path().join("state-store.sqlite"),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("open SQLite StateStore");
        let store = host.state_store().expect("ready StateStore");
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");

        let mut requested = attachment(vec![("type".into(), "iceberg".into())]);
        requested.display_name = "opaque-display-name-must-not-leak-".repeat(16);
        let error = repository
            .create(requested)
            .await
            .expect_err("over-budget attachment must fail before beginning a write");
        assert_eq!(error.kind(), CatalogAttachmentErrorKind::InvalidRequest);
        assert!(
            error
                .to_string()
                .contains("catalog-attachment schema version 3")
        );
        assert!(error.to_string().contains("256-byte budget"));
        assert!(
            !error
                .to_string()
                .contains("opaque-display-name-must-not-leak")
        );
        assert!(
            repository
                .list()
                .await
                .expect("list after rejected create")
                .is_empty()
        );

        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// A lost commit response must not cost the caller its answer, and must
    /// not cost the catalog its identity.
    ///
    /// The mechanism this rests on changed — the outcome is read from the
    /// in-flight attempt's own observation rather than derived from a
    /// reconstructed transaction id — but the business property did not, so
    /// this asserts the property and nothing about the mechanism: after the
    /// response is dropped on the floor, CREATE still reports the exact
    /// attachment identity the caller asked for, exactly one durable record
    /// exists, and the matching exact DROP still succeeds. A second create
    /// afterwards must still be refused as a duplicate, which is what would
    /// break if recovery had quietly written a second record.
    #[tokio::test]
    async fn a_lost_commit_response_is_resolved_and_keeps_one_stable_attachment_identity() {
        let directory = tempfile::tempdir().expect("temporary SQLite StateStore directory");
        let registry =
            builtin_state_store_provider_registry().expect("builtin StateStore registry");
        let mut host = StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "catalog-attachment-commit-unknown-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: directory.path().join("state-store.sqlite"),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("open SQLite StateStore");
        let store = host.state_store().expect("ready StateStore");
        let fault = FaultInjectingStateStore::new(Arc::clone(&store));
        let fault_store: Arc<dyn StateStore> = fault.clone();
        let repository = CatalogAttachmentRepository::open(fault_store, host.run_policy())
            .await
            .expect("open catalog attachment repository");

        let requested = attachment(vec![("type".into(), "iceberg".into())]);
        let create_gate = FaultGate::new();
        fault.lose_next_post_dispatch_response(create_gate.clone());
        let create_task = tokio::spawn({
            let repository = repository.clone();
            let requested = requested.clone();
            async move { repository.create(requested).await }
        });
        create_gate.wait_reached().await;
        create_gate.release().await;
        let created = create_task
            .await
            .expect("create task joins")
            .expect("the attempt's own observation recovers the create");
        assert_eq!(
            created.attachment.attachment_id, requested.attachment_id,
            "recovery must report the identity the caller asked for, not a new one"
        );
        assert_eq!(repository.list().await.expect("list attachments").len(), 1);
        // A recovered create wrote once. A second create sees the record it
        // left behind, which a duplicating recovery could not produce.
        assert_eq!(
            repository
                .create(attachment(vec![("type".into(), "iceberg".into())]))
                .await
                .expect_err("the recovered record is a real, unique record")
                .kind(),
            CatalogAttachmentErrorKind::AlreadyExists
        );
        // The commit landed, so the local reconciler was told exactly once.
        assert_eq!(repository.published_wakeups(), 1);

        let drop_gate = FaultGate::new();
        fault.lose_next_post_dispatch_response(drop_gate.clone());
        let drop_task = tokio::spawn({
            let repository = repository.clone();
            async move { repository.drop_exact(created).await }
        });
        drop_gate.wait_reached().await;
        drop_gate.release().await;
        drop_task
            .await
            .expect("drop task joins")
            .expect("the attempt's own observation recovers the exact drop");
        assert!(repository.list().await.expect("list after drop").is_empty());
        assert_eq!(repository.published_wakeups(), 2);

        drop(repository);
        drop(fault);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// The refusal survives; only where it comes from changed. It is now an
    /// observation taken before the delete, so the assertion is on the check
    /// and on the untouched record — not on a transaction that read two
    /// families at once.
    #[tokio::test]
    async fn an_observed_materialized_view_dependency_refuses_the_drop_and_keeps_attachment() {
        let directory = tempfile::tempdir().expect("temporary SQLite StateStore directory");
        let registry =
            builtin_state_store_provider_registry().expect("builtin StateStore registry");
        let mut host = StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "catalog-attachment-mv-fence-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: directory.path().join("state-store.sqlite"),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("open SQLite StateStore");
        let store = host.state_store().expect("ready StateStore");
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let created = repository
            .create(attachment(vec![("type".into(), "iceberg".into())]))
            .await
            .expect("create catalog attachment");

        let upstream = MvDependencyObjectRef {
            catalog: Some(created.attachment.instance_id.as_str().to_string()),
            database_or_namespace: "sales".to_string(),
            name: "orders".to_string(),
            object_type: MvDependencyObjectType::Table,
            storage_engine: MvDependencyStorageEngine::Iceberg,
        };
        let dependency_key = crate::mv::repository::key::dependency_by_upstream_key(&upstream, 1)
            .expect("MV upstream dependency key");
        let mut transaction = store
            .begin_write(
                reserve(store.as_ref()),
                "seed materialized view dependency for catalog drop fence",
            )
            .await
            .expect("begin seed transaction");
        transaction
            .put(
                dependency_key,
                Bytes::from_static(b"dependency index marker")
                    .try_into()
                    .expect("StateStore value"),
                Precondition::Absent,
            )
            .await
            .expect("write dependency index marker");
        assert!(matches!(
            transaction.commit().await,
            CommitOutcome::Committed(_)
        ));

        let refusal = repository
            .observe_materialized_view_references(&created.attachment.instance_id, 256)
            .await
            .expect_err("referenced catalog drop must conflict");
        assert_eq!(refusal.kind(), CatalogAttachmentErrorKind::Conflict);
        assert!(
            refusal
                .to_string()
                .contains("best-effort operational check")
                && refusal
                    .to_string()
                    .contains("not a cross-system serializability guarantee"),
            "the refusal must not advertise a guarantee it no longer provides: {refusal}"
        );
        assert_eq!(
            repository
                .get(&created.attachment.instance_id)
                .await
                .expect("read attachment after rejected drop"),
            Some(created)
        );

        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// The writer-first half of the DROP/MV race closure. A DROP that commits
    /// first must make the MV writer's frozen observation fail, and a same-name
    /// recreate must fail it too so an ABA cannot smuggle a dangling reference
    /// into a durable MV definition.
    #[tokio::test]
    async fn mv_attachment_assertion_rejects_a_dropped_or_recreated_attachment() {
        let directory = tempfile::tempdir().expect("temporary SQLite StateStore directory");
        let registry =
            builtin_state_store_provider_registry().expect("builtin StateStore registry");
        let mut host = StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "catalog-attachment-mv-assert-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: directory.path().join("state-store.sqlite"),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("open SQLite StateStore");
        let store = host.state_store().expect("ready StateStore");
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open catalog attachment repository");
        let observed = repository
            .create(attachment(vec![("type".into(), "iceberg".into())]))
            .await
            .expect("create catalog attachment");

        // A writer that still holds the live observation may proceed.
        let mut transaction = store
            .begin_write(reserve(store.as_ref()), "assert live catalog attachment")
            .await
            .expect("begin live assertion transaction");
        assert_attachment_versions(transaction.as_mut(), std::slice::from_ref(&observed))
            .await
            .expect("a live attachment admits the materialized view write");
        transaction.abort().await.expect("abort live assertion");

        repository
            .drop_exact(observed.clone())
            .await
            .expect("DROP commits first");
        let mut transaction = store
            .begin_write(reserve(store.as_ref()), "assert dropped catalog attachment")
            .await
            .expect("begin dropped assertion transaction");
        assert_eq!(
            assert_attachment_versions(transaction.as_mut(), std::slice::from_ref(&observed))
                .await
                .expect_err("a dropped attachment must reject the write")
                .kind(),
            StateStoreErrorKind::Conflict
        );
        transaction.abort().await.expect("abort dropped assertion");

        // Recreating the same SQL name mints a new lifecycle identity, so the
        // stale observation must not be accepted by version or by name.
        let recreated = repository
            .create(attachment(vec![("type".into(), "iceberg".into())]))
            .await
            .expect("recreate the same catalog name");
        assert_ne!(
            recreated.attachment.attachment_id,
            observed.attachment.attachment_id
        );
        let mut transaction = store
            .begin_write(
                reserve(store.as_ref()),
                "assert recreated catalog attachment",
            )
            .await
            .expect("begin recreated assertion transaction");
        assert_eq!(
            assert_attachment_versions(transaction.as_mut(), std::slice::from_ref(&observed))
                .await
                .expect_err("a recreated attachment must reject the stale observation")
                .kind(),
            StateStoreErrorKind::Conflict
        );
        transaction
            .abort()
            .await
            .expect("abort recreated assertion");

        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }
}
