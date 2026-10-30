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

//! Exact-generation process-local orphan cleanup.
//!
//! Candidate discovery occurs once during planning. Execute and reconcile use
//! only the immutable process-local manifest and never list the table a
//! second time.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use novarocks_spi::connector::{
    BatchReceipt, BatchReceiptSummary, CandidatePage, ConnectorCleanupCandidate,
    ConnectorCleanupCandidatePageRequest, ConnectorCleanupExecuteRequest,
    ConnectorCleanupFinalizeRequest, ConnectorCleanupMaintenance, ConnectorCleanupOwnedRefIdentity,
    ConnectorCleanupOwnedRefSelection, ConnectorCleanupPlan, ConnectorCleanupPlanSummary,
    ConnectorCleanupPlanningRequest, ConnectorCleanupPrepareRequest, ConnectorCleanupScanSafety,
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorProcessLocalPayload,
    ConnectorProviderBindingKey, ConnectorRequestContext, PreparedBatch,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::cleanup_candidates::canonical_object_mtime_ms;
use super::owned_ref_cleanup::{
    OwnedRefCandidate, collect_owned_ref_candidates, matches_owned_ref_candidate,
};
use crate::catalog::CatalogTableName;
use crate::catalog::admission::{CatalogAdmissionRequest, CatalogOperation, connector_unsupported};
use crate::metadata::IcebergTablePayload;
use crate::metadata_context::IcebergMetadataContext;

const ARTIFACT_VERSION: u16 = 2;
const MAX_RECORDS: usize = 262_144;
const MAX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
const MAX_BATCH_OBJECTS: usize = 1024;
const MAX_REASON_CHARS: usize = 1024;
const MANIFEST_DOMAIN: &[u8] = b"novarocks.iceberg.orphan-cleanup.manifest.v1\0";
const BATCH_DOMAIN: &[u8] = b"novarocks.iceberg.orphan-cleanup.batch.v1\0";
const RECEIPT_DOMAIN: &[u8] = b"novarocks.iceberg.orphan-cleanup.receipt.v1\0";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PlanPayload {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogicalManifest {
    version: u16,
    namespace: String,
    table: String,
    table_uuid: String,
    older_than_ms: i64,
    phase: CleanupPhase,
    records: Vec<ManifestRecord>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum CleanupPhase {
    OwnedRefRetire,
    ObjectSweep,
}

struct FrozenCleanupManifest {
    records: Arc<[ManifestRecord]>,
    manifest_digest: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManifestRecord {
    ordinal: u32,
    candidate: ManifestCandidate,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ManifestCandidate {
    Object {
        location: String,
        identity: ObjectIdentity,
    },
    OwnedRef {
        name: String,
        head_snapshot_id: i64,
        provenance_version: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provenance_digest_hex: Option<String>,
        created_at_ms: i64,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ObjectIdentity {
    Version {
        version: String,
        size: u64,
        mtime_ms: i64,
    },
    Etag {
        etag: String,
        size: u64,
        mtime_ms: i64,
    },
    SizeMtime {
        size: u64,
        mtime_ms: i64,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedPayload {
    version: u16,
    batch_ordinal: u32,
    first_ordinal: u32,
    record_count: u32,
    batch_digest_hex: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptPayload {
    version: u16,
    receipt_digest_hex: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReceiptRecord {
    ordinal: u32,
    outcome: ObjectOutcome,
    reason: Option<String>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ObjectOutcome {
    Deleted,
    AlreadyAbsent,
    Failed,
    Unknown,
}

#[derive(Clone)]
pub struct IcebergCleanupMaintenanceAdapter {
    key: ConnectorProviderBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    runtime: Arc<IcebergMetadataContext>,
}

impl IcebergCleanupMaintenanceAdapter {
    pub fn new(
        key: ConnectorProviderBindingKey,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
                instance_id: key.instance_id.clone(),
            },
            key,
            runtime,
        })
    }

    fn ensure_owner(&self, owner: &ConnectorProviderBindingKey) -> Result<(), ConnectorError> {
        if owner != &self.key {
            return Err(invalid(
                "Iceberg cleanup does not match the exact connector generation",
            ));
        }
        Ok(())
    }

    fn plan_payload(&self, plan: &ConnectorCleanupPlan) -> Result<PlanPayload, ConnectorError> {
        let payload: PlanPayload = decode_canonical(plan.provider_payload(), "cleanup plan")?;
        if payload.version != ARTIFACT_VERSION {
            return Err(corrupt("Iceberg cleanup plan payload is invalid"));
        }
        Ok(payload)
    }

    fn manifest(
        &self,
        plan: &ConnectorCleanupPlan,
        payload: &PlanPayload,
        context: &ConnectorRequestContext,
    ) -> Result<Arc<[ManifestRecord]>, ConnectorError> {
        plan.manifest().validate_owner(&self.key)?;
        self.runtime
            .control_state()
            .invalidate_table(&payload.namespace, &payload.table);
        let physical = self
            .runtime
            .observe_table_for_request(&payload.namespace, &payload.table, context)
            .map_err(unavailable)?;
        manifest_loaded(&self.key, plan, payload, &physical.table)
    }

    fn prepared_records(
        &self,
        plan: &ConnectorCleanupPlan,
        prepared: &PreparedBatch,
        context: &ConnectorRequestContext,
    ) -> Result<(PlanPayload, Vec<ManifestRecord>), ConnectorError> {
        let payload = self.plan_payload(plan)?;
        let evidence: PreparedPayload =
            decode_canonical(prepared.evidence_payload(), "cleanup prepared evidence")?;
        if evidence.version != ARTIFACT_VERSION
            || evidence.batch_ordinal != prepared.batch_ordinal()
            || evidence.record_count == 0
            || evidence.record_count as usize > MAX_BATCH_OBJECTS
            || evidence.batch_digest_hex != hex_encode(prepared.batch_digest())
            || prepared.batch_ordinal() >= plan.summary().batch_count()
        {
            return Err(corrupt("Iceberg cleanup prepared evidence is invalid"));
        }
        let records = self.manifest(plan, &payload, context)?;
        let start = evidence.first_ordinal as usize;
        let end = start
            .checked_add(evidence.record_count as usize)
            .ok_or_else(|| corrupt("Iceberg cleanup batch range overflows"))?;
        let batch = records
            .get(start..end)
            .ok_or_else(|| corrupt("Iceberg cleanup batch exceeds its manifest"))?
            .to_vec();
        if batch_digest(&batch) != prepared.batch_digest() {
            return Err(corrupt("Iceberg cleanup batch digest is invalid"));
        }
        Ok((payload, batch))
    }
}

#[path = "cleanup_maintenance/async_original.rs"]
pub(crate) mod async_original;
impl ConnectorCleanupMaintenance for IcebergCleanupMaintenanceAdapter {
    fn metadata_cpu_source_type_id(&self) -> Result<std::any::TypeId, ConnectorError> {
        Ok(std::any::TypeId::of::<
            super::statistics::scoped_cpu::IcebergMetadataCpuOriginal,
        >())
    }

    fn begin_async_in(
        &self,
        start: &mut Option<novarocks_spi::connector::ConnectorCleanupAsyncStart>,
        metadata: Arc<dyn novarocks_spi::connector::ConnectorMetadata>,
        cpu: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
        target: &mut Option<novarocks_spi::connector::ConnectorCleanupAsyncExecution>,
    ) -> Result<(), ConnectorError> {
        async_original::install_in(self, start, metadata, cpu, target)
    }
    fn initial_dependencies(
        &self,
    ) -> Result<Vec<novarocks_spi::connector::SharedDependency>, ConnectorError> {
        self.runtime.metadata_action_initial_dependencies()
    }

    fn scan_gc_safety(&self) -> ConnectorCleanupScanSafety {
        iceberg_scan_gc_safety()
    }

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    fn plan_cleanup(
        &self,
        request: ConnectorCleanupPlanningRequest,
    ) -> Result<ConnectorCleanupPlan, ConnectorError> {
        request.validate()?;
        validate_context(&request.context)?;
        self.ensure_owner(request.owner())?;
        let target: IcebergTablePayload =
            serde_json::from_slice(request.operation().table().payload())
                .map_err(|error| invalid(format!("decode Iceberg cleanup table: {error}")))?;
        if request.operation().table().owner() != &self.key.instance_id
            || target.metadata_table_type.is_some()
        {
            return Err(invalid("Iceberg cleanup requires an owned base table"));
        }
        self.runtime
            .novarocks_catalog()
            .admit(&CatalogAdmissionRequest::new(
                CatalogOperation::RemoveOrphanFiles,
                CatalogTableName::new(target.namespace.clone(), target.table.clone()),
                request.context.initiation(),
            ))
            .map_err(connector_unsupported)?;

        self.runtime
            .control_state()
            .invalidate_table(&target.namespace, &target.table);
        let physical = self
            .runtime
            .load_table_for_request(&target.namespace, &target.table, &request.context)
            .map_err(unavailable)?;
        let table = physical.table;
        plan_loaded(self, &request, target, table)
    }

    fn prepare_batch(
        &self,
        request: ConnectorCleanupPrepareRequest,
    ) -> Result<PreparedBatch, ConnectorError> {
        request.plan.validate()?;
        validate_context(&request.context)?;
        self.ensure_owner(request.plan.owner())?;
        let payload = self.plan_payload(&request.plan)?;
        let records = self.manifest(&request.plan, &payload, &request.context)?;
        prepare_from_records(&self.key, &request, &records)
    }

    fn execute_batch(
        &self,
        request: ConnectorCleanupExecuteRequest,
    ) -> Result<BatchReceipt, ConnectorError> {
        request.plan.validate()?;
        validate_context(&request.context)?;
        self.ensure_owner(request.plan.owner())?;
        let (payload, batch) =
            self.prepared_records(&request.plan, &request.prepared, &request.context)?;
        let outcomes = execute_frozen_batch(
            &self.runtime,
            &self.key,
            request.plan.operation_id(),
            &payload,
            &batch,
            &request.context,
        )?;
        let digest = domain_digest(RECEIPT_DOMAIN, &canonical(&outcomes)?);
        receipt_value(
            &self.descriptor,
            &self.key,
            &request.plan,
            &request.prepared,
            digest,
            &outcomes,
        )
    }

    fn read_candidate_page(
        &self,
        request: ConnectorCleanupCandidatePageRequest,
    ) -> Result<CandidatePage, ConnectorError> {
        request.plan.validate()?;
        validate_context(&request.context)?;
        self.ensure_owner(request.plan.owner())?;
        let payload = self.plan_payload(&request.plan)?;
        let table_uuid = table_uuid_from_payload(&payload)?;
        let records = self.manifest(&request.plan, &payload, &request.context)?;
        page_from_records(&self.key, &request, &records, table_uuid)
    }

    fn finalize_terminal(
        &self,
        request: ConnectorCleanupFinalizeRequest,
    ) -> Result<(), ConnectorError> {
        request.plan.validate()?;
        validate_context(&request.context)?;
        self.ensure_owner(request.plan.owner())
    }
}

/// Ref candidates and object candidates are deliberately never mixed in one
/// cleanup manifest. A successful ref drop changes the table live set, so the
/// following object pass must begin at `plan_cleanup` and reload metadata.
fn selected_owned_ref_manifest_records(
    candidates: Vec<OwnedRefCandidate>,
    selection: Option<&ConnectorCleanupOwnedRefSelection>,
) -> Vec<ManifestRecord> {
    candidates
        .into_iter()
        .filter(|candidate| {
            selection.is_none_or(|selection| {
                selection
                    .identities()
                    .binary_search_by(|identity| owned_ref_identity(candidate).cmp(identity))
                    .is_ok()
            })
        })
        .enumerate()
        .map(|(ordinal, candidate)| ManifestRecord {
            ordinal: ordinal as u32,
            candidate: ManifestCandidate::OwnedRef {
                name: candidate.name,
                head_snapshot_id: candidate.head_snapshot_id,
                provenance_version: candidate.provenance_version,
                provenance_digest_hex: Some(hex_encode(candidate.provenance_digest)),
                created_at_ms: candidate.created_at_ms,
            },
        })
        .collect()
}

fn cleanup_phase_for_owned_refs(
    selection: Option<&ConnectorCleanupOwnedRefSelection>,
    selected_owned_ref_count: usize,
) -> CleanupPhase {
    if selection.is_some() || selected_owned_ref_count > 0 {
        CleanupPhase::OwnedRefRetire
    } else {
        CleanupPhase::ObjectSweep
    }
}

fn owned_ref_identity(candidate: &OwnedRefCandidate) -> ConnectorCleanupOwnedRefIdentity {
    ConnectorCleanupOwnedRefIdentity::try_new(
        Arc::from(candidate.name.as_str()),
        candidate.head_snapshot_id,
        candidate.provenance_version,
        candidate.provenance_digest,
    )
    .expect("owned ref candidate is validated before cleanup planning")
}

fn manifest_candidate_projection(
    record: &ManifestRecord,
    table_uuid: uuid::Uuid,
) -> Result<ConnectorCleanupCandidate, ConnectorError> {
    Ok(match &record.candidate {
        ManifestCandidate::Object { location, .. } => ConnectorCleanupCandidate::Object {
            location: Arc::from(location.as_str()),
        },
        ManifestCandidate::OwnedRef {
            name,
            head_snapshot_id,
            provenance_version,
            provenance_digest_hex,
            created_at_ms,
        } => ConnectorCleanupCandidate::OwnedRef {
            table_uuid,
            name: Arc::from(name.as_str()),
            head_snapshot_id: *head_snapshot_id,
            provenance_version: *provenance_version,
            provenance_digest: decode_owned_ref_provenance_digest(
                provenance_digest_hex.as_deref(),
            )?,
            created_at_ms: *created_at_ms,
        },
    })
}

fn table_uuid_from_payload(payload: &PlanPayload) -> Result<uuid::Uuid, ConnectorError> {
    uuid::Uuid::parse_str(&payload.table_uuid)
        .map_err(|_| invalid("Iceberg cleanup plan has an invalid table UUID"))
}

fn execute_frozen_batch(
    runtime: &IcebergMetadataContext,
    owner: &ConnectorProviderBindingKey,
    operation_id: novarocks_spi::connector::ConnectorCleanupOperationId,
    payload: &PlanPayload,
    batch: &[ManifestRecord],
    context: &ConnectorRequestContext,
) -> Result<Vec<ReceiptRecord>, ConnectorError> {
    // A restored/process-local object manifest cannot bypass current safety.
    // Exact Catalog-ref records do not grant or invoke object deletion.
    if batch
        .iter()
        .any(|record| matches!(&record.candidate, ManifestCandidate::Object { .. }))
    {
        iceberg_scan_gc_safety().require_scanned_object_gc()?;
    }
    let binding = runtime
        .resources()
        .planning_binding()
        .for_request(context.clone());
    batch
        .iter()
        .map(|record| match &record.candidate {
            ManifestCandidate::OwnedRef {
                name,
                head_snapshot_id,
                provenance_version,
                provenance_digest_hex,
                created_at_ms,
                ..
            } => execute_owned_ref(
                runtime,
                owner,
                operation_id,
                payload,
                record.ordinal,
                name,
                *head_snapshot_id,
                *provenance_version,
                provenance_digest_hex.as_deref(),
                *created_at_ms,
                context,
            ),
            ManifestCandidate::Object { location, identity } => {
                if crate::catalog_metadata_root::is_catalog_metadata_root(location) {
                    return receipt(
                        record.ordinal,
                        ObjectOutcome::Failed,
                        Some(
                            "catalog metadata discovery root is protected from object cleanup"
                                .into(),
                        ),
                    );
                }
                let local_mtime = novarocks_fs::FsLocation::parse(location)
                    .map_err(|error| invalid(format!("parse frozen cleanup location: {error}")))?
                    .scheme()
                    == novarocks_fs::FsScheme::Local;
                let access = crate::fs_io::resolve_access_for_location(location, &binding)
                    .map_err(unavailable)?;
                let path = access.single_relative_path().map_err(invalid)?.to_string();
                let operator = access.operator();
                let matches = runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(stat_matches(
                        operator.clone(),
                        path.clone(),
                        identity.clone(),
                        local_mtime,
                    ))
                    .map_err(unavailable)?;
                match matches {
                    Err(error) if error.kind() == crate::opendal::ErrorKind::NotFound => {
                        receipt(record.ordinal, ObjectOutcome::AlreadyAbsent, None)
                    }
                    Err(error) => receipt(
                        record.ordinal,
                        error_outcome(error.kind()),
                        Some(error.to_string()),
                    ),
                    Ok(false) => receipt(
                        record.ordinal,
                        ObjectOutcome::Failed,
                        Some("object identity changed before delete".to_string()),
                    ),
                    Ok(true) => {
                        let deleted = runtime
                            .resources()
                            .catalog_runtime()
                            .block_on(delete_exact(
                                operator,
                                path,
                                location.clone(),
                                identity.clone(),
                            ))
                            .map_err(unavailable)?;
                        match deleted {
                            Ok(()) => receipt(record.ordinal, ObjectOutcome::Deleted, None),
                            Err(error) if error.kind() == crate::opendal::ErrorKind::NotFound => {
                                receipt(record.ordinal, ObjectOutcome::AlreadyAbsent, None)
                            }
                            Err(error) => receipt(
                                record.ordinal,
                                error_outcome(error.kind()),
                                Some(error.to_string()),
                            ),
                        }
                    }
                }
            }
        })
        .collect()
}

#[expect(
    clippy::too_many_arguments,
    reason = "Exact ref retirement keeps every provenance field explicit at the destructive boundary."
)]
fn execute_owned_ref(
    runtime: &IcebergMetadataContext,
    owner: &ConnectorProviderBindingKey,
    operation_id: novarocks_spi::connector::ConnectorCleanupOperationId,
    payload: &PlanPayload,
    ordinal: u32,
    name: &str,
    expected_head_snapshot_id: i64,
    provenance_version: u16,
    provenance_digest_hex: Option<&str>,
    created_at_ms: i64,
    context: &ConnectorRequestContext,
) -> Result<ReceiptRecord, ConnectorError> {
    use novarocks_spi::connector::ExternalMutationOutcome;
    let prepared = runtime.resources().catalog_runtime().observe_preparation(
        || {
            let prepare = || -> Result<_, ConnectorError> {
                let (coordination, physical) = runtime.coordinate_table_for_request(
                    &payload.namespace,
                    &payload.table,
                    context,
                )?;
                let Some(frozen) = freeze_owned_ref_on_table(
                    owner,
                    operation_id,
                    payload,
                    ordinal,
                    name,
                    expected_head_snapshot_id,
                    provenance_version,
                    provenance_digest_hex,
                    created_at_ms,
                    context,
                    &physical.table,
                )?
                else {
                    return Ok(None);
                };
                Ok(Some((coordination, frozen)))
            };
            prepare().map_err(crate::catalog::publication_dispatch::PreparationFailure::from)
        },
        |error: &crate::catalog::publication_dispatch::PreparationFailure| error.0.clone(),
    );
    let (_coordination, frozen) = match prepared {
        Ok(Some(prepared)) => prepared,
        Ok(None) => {
            return receipt(
                ordinal,
                ObjectOutcome::Failed,
                Some("owned ref provenance changed before exact retirement".to_owned()),
            );
        }
        Err(crate::catalog::publication_dispatch::PreparationFailure(failure)) => {
            return receipt(ordinal, ObjectOutcome::Failed, Some(failure.to_string()));
        }
    };
    let journal = ref_journal(
        runtime,
        owner,
        operation_id,
        payload,
        ordinal,
        name,
        expected_head_snapshot_id,
        &frozen,
    )?;
    let catalog = Arc::clone(runtime.novarocks_catalog());
    let policy = catalog.evidence_policy();
    let publication = journal.clone();
    let preparation = context.clone();
    let result = runtime.resources().catalog_runtime().block_on(async move {
        publication
            .publish_once(
                preparation,
                policy,
                move |descriptor, generation, original| async move {
                    catalog
                        .prepare_frozen_publication(
                            frozen,
                            CatalogOperation::DropBranch,
                            None,
                            descriptor,
                            generation,
                            original,
                        )
                        .await
                },
            )
            .await
    });
    let outcome = match result {
        Ok(Ok(entry)) => journal.catalog_entry_outcome(entry),
        Ok(Err(error)) => journal
            .catalog_bridge_failure(format!("Owned ref publication invocation failed: {error}")),
        Err(error) => {
            journal.catalog_bridge_failure(format!("Owned ref publication bridge failed: {error}"))
        }
    };
    match outcome {
        ExternalMutationOutcome::KnownCommitted { receipt, .. } => Ok(receipt),
        ExternalMutationOutcome::KnownUncommitted { failure, .. } => {
            let reason = if failure.kind()
                == novarocks_spi::connector::ConnectorMutationFailureKind::Conflict
            {
                "owned ref changed before exact retirement".to_owned()
            } else {
                failure.to_string()
            };
            receipt(ordinal, ObjectOutcome::Failed, Some(reason))
        }
        ExternalMutationOutcome::CommitUnknown { failure, .. } => {
            receipt(ordinal, ObjectOutcome::Unknown, Some(failure.to_string()))
        }
    }
}

async fn stat_matches(
    operator: crate::opendal::Operator,
    path: String,
    identity: ObjectIdentity,
    local_mtime: bool,
) -> Result<bool, crate::opendal::Error> {
    let metadata = operator.stat(&path).await?;
    let size = metadata.content_length();
    let mtime = metadata
        .last_modified()
        .map(|value| value.into_inner().as_millisecond())
        .map(|value| {
            if local_mtime {
                value
            } else {
                canonical_object_mtime_ms(value)
            }
        });
    Ok(match identity {
        ObjectIdentity::Version {
            version,
            size: expected,
            mtime_ms,
        } => {
            metadata.version() == Some(version.as_str())
                && size == expected
                && mtime == Some(mtime_ms)
        }
        ObjectIdentity::Etag {
            etag,
            size: expected,
            mtime_ms,
        } => metadata.etag() == Some(etag.as_str()) && size == expected && mtime == Some(mtime_ms),
        ObjectIdentity::SizeMtime {
            size: expected,
            mtime_ms,
        } => size == expected && mtime == Some(mtime_ms),
    })
}

async fn delete_exact(
    operator: crate::opendal::Operator,
    path: String,
    location: String,
    identity: ObjectIdentity,
) -> Result<(), crate::opendal::Error> {
    // Operator-relative paths can be only a basename for local bindings.
    // Preserve the complete location at the final destructive boundary.
    if crate::catalog_metadata_root::is_catalog_metadata_root(&location) {
        return Err(crate::opendal::Error::new(
            crate::opendal::ErrorKind::PermissionDenied,
            "catalog metadata discovery root is protected from object cleanup",
        ));
    }
    iceberg_scan_gc_safety()
        .require_scanned_object_gc()
        .map_err(|error| {
            crate::opendal::Error::new(crate::opendal::ErrorKind::Unsupported, error.message())
        })?;
    match identity {
        ObjectIdentity::Version { version, .. } => {
            operator.delete_with(&path).version(&version).await
        }
        _ => operator.delete(&path).await,
    }
}

fn receipt_value(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorProviderBindingKey,
    plan: &ConnectorCleanupPlan,
    prepared: &PreparedBatch,
    digest: [u8; 32],
    records: &[ReceiptRecord],
) -> Result<BatchReceipt, ConnectorError> {
    BatchReceipt::try_new(
        descriptor.clone(),
        key.clone(),
        plan.operation_id(),
        plan.plan_digest(),
        plan.manifest_digest(),
        prepared.batch_ordinal(),
        prepared.batch_digest(),
        receipt_summary(records)?,
        canonical(&ReceiptPayload {
            version: ARTIFACT_VERSION,
            receipt_digest_hex: hex_encode(digest),
        })?,
    )
}

fn receipt_summary(records: &[ReceiptRecord]) -> Result<BatchReceiptSummary, ConnectorError> {
    let mut deleted = 0_u32;
    let mut absent = 0_u32;
    let mut failed = 0_u32;
    let mut unknown = 0_u32;
    for record in records {
        match record.outcome {
            ObjectOutcome::Deleted => deleted = deleted.saturating_add(1),
            ObjectOutcome::AlreadyAbsent => absent = absent.saturating_add(1),
            ObjectOutcome::Failed => failed = failed.saturating_add(1),
            ObjectOutcome::Unknown => unknown = unknown.saturating_add(1),
        }
    }
    let summary = BatchReceiptSummary::new(deleted, absent, failed, unknown);
    if summary.total() != records.len() as u32 {
        return Err(corrupt("Iceberg cleanup receipt summary overflows"));
    }
    Ok(summary)
}

fn receipt(
    ordinal: u32,
    outcome: ObjectOutcome,
    reason: Option<String>,
) -> Result<ReceiptRecord, ConnectorError> {
    Ok(ReceiptRecord {
        ordinal,
        outcome,
        reason: reason.map(|reason| reason.chars().take(MAX_REASON_CHARS).collect()),
    })
}

fn error_outcome(kind: crate::opendal::ErrorKind) -> ObjectOutcome {
    match kind {
        crate::opendal::ErrorKind::PermissionDenied
        | crate::opendal::ErrorKind::ConditionNotMatch => ObjectOutcome::Failed,
        _ => ObjectOutcome::Unknown,
    }
}

fn batch_digest(records: &[ManifestRecord]) -> [u8; 32] {
    domain_digest(
        BATCH_DOMAIN,
        &canonical(records).expect("bounded manifest records are serializable"),
    )
}

fn identity_size(identity: &ObjectIdentity) -> u64 {
    match identity {
        ObjectIdentity::Version { size, .. }
        | ObjectIdentity::Etag { size, .. }
        | ObjectIdentity::SizeMtime { size, .. } => *size,
    }
}

fn manifest_record_size(record: &ManifestRecord) -> u64 {
    match &record.candidate {
        ManifestCandidate::Object { identity, .. } => identity_size(identity),
        // Catalog refs are metadata, not object-store bytes. Keeping this at
        // zero makes the summary an honest object-sweep byte estimate.
        ManifestCandidate::OwnedRef { .. } => 0,
    }
}

fn state_digest(
    uuid: &[u8],
    location: Option<&str>,
    snapshot: Option<i64>,
    schema: i32,
    spec: i32,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.orphan-cleanup.state.v1\0");
    hash.update(uuid);
    hash.update(location.unwrap_or_default().as_bytes());
    hash.update(snapshot.unwrap_or_default().to_be_bytes());
    hash.update(schema.to_be_bytes());
    hash.update(spec.to_be_bytes());
    hash.finalize().into()
}

fn domain_digest(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    hash.finalize().into()
}

fn decode_digest(value: &str) -> Result<[u8; 32], ConnectorError> {
    hex_decode(value)
        .ok_or_else(|| corrupt("Iceberg cleanup digest is not hex"))?
        .try_into()
        .map_err(|_| corrupt("Iceberg cleanup digest has an invalid length"))
}

fn decode_owned_ref_provenance_digest(value: Option<&str>) -> Result<[u8; 32], ConnectorError> {
    let value = value.ok_or_else(|| {
        corrupt(
            "Iceberg cleanup owned ref lacks the frozen provenance proof required for retirement",
        )
    })?;
    decode_digest(value)
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(ALPHABET[(byte >> 4) as usize] as char);
        encoded.push(ALPHABET[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|digits| {
            let high = (digits[0] as char).to_digit(16)? as u8;
            let low = (digits[1] as char).to_digit(16)? as u8;
            Some((high << 4) | low)
        })
        .collect()
}

fn canonical<T: Serialize + ?Sized>(value: &T) -> Result<Bytes, ConnectorError> {
    let value = serde_json::to_value(value)
        .map_err(|error| internal(format!("encode Iceberg cleanup JSON: {error}")))?;
    let mut bytes = Vec::new();
    write_canonical(&value, &mut bytes)?;
    Ok(Bytes::from(bytes))
}

fn decode_canonical<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    label: &str,
) -> Result<T, ConnectorError> {
    let value = serde_json::from_slice(bytes)
        .map_err(|error| corrupt(format!("decode {label}: {error}")))?;
    if canonical(&value)?.as_ref() != bytes {
        return Err(corrupt(format!("{label} is not canonical JSON")));
    }
    serde_json::from_value(value).map_err(|error| corrupt(format!("decode {label}: {error}")))
}

fn write_canonical(value: &Value, bytes: &mut Vec<u8>) -> Result<(), ConnectorError> {
    match value {
        Value::Null => bytes.extend_from_slice(b"null"),
        Value::Bool(value) => bytes.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => bytes.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => bytes.extend_from_slice(
            serde_json::to_string(value)
                .map_err(|error| internal(error.to_string()))?
                .as_bytes(),
        ),
        Value::Array(values) => {
            bytes.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    bytes.push(b',');
                }
                write_canonical(value, bytes)?;
            }
            bytes.push(b']');
        }
        Value::Object(values) => {
            bytes.push(b'{');
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    bytes.push(b',');
                }
                bytes.extend_from_slice(
                    serde_json::to_string(key)
                        .map_err(|error| internal(error.to_string()))?
                        .as_bytes(),
                );
                bytes.push(b':');
                write_canonical(value, bytes)?;
            }
            bytes.push(b'}');
        }
    }
    Ok(())
}

// Actual current catalog capability. No supported variant or test bypass
// invents a persistent/fencing protocol which the Provider does not own.
pub(super) fn iceberg_scan_gc_safety() -> ConnectorCleanupScanSafety {
    ConnectorCleanupScanSafety::Unproven
}

fn validate_context(
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if context.is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message.into())
}

fn plan_loaded(
    adapter: &IcebergCleanupMaintenanceAdapter,
    request: &ConnectorCleanupPlanningRequest,
    target: IcebergTablePayload,
    table: crate::iceberg::table::Table,
) -> Result<ConnectorCleanupPlan, ConnectorError> {
    let older_than_ms = request.operation().older_than_ms();
    // A ref retirement is a separate GC phase. Once a Catalog ref is
    // removed, the live set used by object discovery is stale by
    // definition; a later operation must reload metadata before sweeping.
    let owned_refs = collect_owned_ref_candidates(
        table.metadata(),
        &target.namespace,
        &target.table,
        older_than_ms,
    );
    let owned_ref_records =
        selected_owned_ref_manifest_records(owned_refs, request.owned_ref_selection());
    let (phase, records) =
        if cleanup_phase_for_owned_refs(request.owned_ref_selection(), owned_ref_records.len())
            == CleanupPhase::OwnedRefRetire
        {
            // A second selected-ref plan is never eligible for object
            // discovery. Missing/stale/drifted selections become an empty ref
            // manifest and leak until a later observation, rather than
            // destructively falling through to the object pass.
            (CleanupPhase::OwnedRefRetire, owned_ref_records)
        } else if owned_ref_records.is_empty() {
            adapter
                .scan_gc_safety()
                .require_scanned_object_gc()
                .inspect_err(|_| {
                    tracing::debug!(
                        target: "novarocks_connector_iceberg::maintenance_refusal",
                        site = "cleanup.plan.scanned_gc_safety",
                        refusal = "ScannedGcSafetyUnproven",
                        "Original maintenance capability refused"
                    );
                })?;
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "scanned orphan cleanup has no admitted Provider safety proof",
            ));
        } else {
            (CleanupPhase::OwnedRefRetire, owned_ref_records)
        };
    if records.len() > MAX_RECORDS {
        return Err(exhausted("Iceberg cleanup manifest exceeds 262144 records"));
    }
    let logical = LogicalManifest {
        version: ARTIFACT_VERSION,
        namespace: target.namespace.clone(),
        table: target.table.clone(),
        table_uuid: table.metadata().uuid().to_string(),
        older_than_ms,
        phase,
        records,
    };
    let logical_bytes = canonical(&logical)?;
    if logical_bytes.len() > MAX_ARTIFACT_BYTES {
        return Err(exhausted("Iceberg cleanup manifest exceeds 64 MiB"));
    }
    let manifest_digest = domain_digest(MANIFEST_DOMAIN, &logical_bytes);
    let summary = ConnectorCleanupPlanSummary::try_new(
        logical.records.len() as u64,
        logical.records.iter().map(manifest_record_size).sum(),
        logical.records.len().div_ceil(MAX_BATCH_OBJECTS) as u32,
    )?;
    let state = state_digest(
        table.metadata().uuid().to_string().as_bytes(),
        table.metadata_location(),
        table.metadata().current_snapshot_id(),
        table.metadata().current_schema_id(),
        table.metadata().default_partition_spec_id(),
    );
    let payload = PlanPayload {
        version: ARTIFACT_VERSION,
        namespace: target.namespace,
        table: target.table,
        table_uuid: table.metadata().uuid().to_string(),
    };
    let plan = ConnectorCleanupPlan::try_new(
        request,
        state,
        manifest_digest,
        summary,
        canonical(&payload)?,
        ConnectorProcessLocalPayload::new(
            adapter.key.clone(),
            Arc::new(FrozenCleanupManifest {
                records: logical.records.into(),
                manifest_digest,
            }),
        ),
    )?;
    Ok(plan)
}

fn manifest_loaded(
    key: &ConnectorProviderBindingKey,
    plan: &ConnectorCleanupPlan,
    payload: &PlanPayload,
    table: &crate::iceberg::table::Table,
) -> Result<Arc<[ManifestRecord]>, ConnectorError> {
    if table.metadata().uuid().to_string() != payload.table_uuid {
        return Err(corrupt(
            "Iceberg cleanup table incarnation no longer matches its frozen manifest",
        ));
    }
    let manifest = plan
        .manifest()
        .value::<FrozenCleanupManifest>(key.instance_id())?;
    if manifest.manifest_digest != plan.manifest_digest() {
        return Err(internal(
            "Iceberg cleanup manifest payload digest is invalid",
        ));
    }
    Ok(Arc::clone(&manifest.records))
}

fn prepare_from_records(
    key: &ConnectorProviderBindingKey,
    request: &ConnectorCleanupPrepareRequest,
    records: &[ManifestRecord],
) -> Result<PreparedBatch, ConnectorError> {
    let start = request.batch_ordinal as usize * MAX_BATCH_OBJECTS;
    let end = (start + MAX_BATCH_OBJECTS).min(records.len());
    let batch = records
        .get(start..end)
        .filter(|batch| !batch.is_empty())
        .ok_or_else(|| invalid("Iceberg cleanup batch is outside its manifest"))?;
    let digest = batch_digest(batch);
    PreparedBatch::try_new(
        key.clone(),
        request.plan.operation_id(),
        request.plan.plan_digest(),
        request.plan.manifest_digest(),
        request.batch_ordinal,
        digest,
        canonical(&PreparedPayload {
            version: ARTIFACT_VERSION,
            batch_ordinal: request.batch_ordinal,
            first_ordinal: start as u32,
            record_count: batch.len() as u32,
            batch_digest_hex: hex_encode(digest),
        })?,
    )
}

fn page_from_records(
    key: &ConnectorProviderBindingKey,
    request: &ConnectorCleanupCandidatePageRequest,
    records: &[ManifestRecord],
    table_uuid: uuid::Uuid,
) -> Result<CandidatePage, ConnectorError> {
    let start = request.offset as usize;
    if start > records.len() {
        return Err(invalid("Iceberg cleanup page offset exceeds its manifest"));
    }
    let end = (start + request.limit as usize).min(records.len());
    CandidatePage::try_new(
        key.clone(),
        request.plan.operation_id(),
        request.plan.manifest_digest(),
        request.offset,
        records[start..end]
            .iter()
            .map(|record| manifest_candidate_projection(record, table_uuid))
            .collect::<Result<Vec<_>, _>>()?,
        end == records.len(),
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "Exact ref provenance remains explicit at the original freeze author."
)]
fn freeze_owned_ref_on_table(
    _owner: &ConnectorProviderBindingKey,
    operation_id: novarocks_spi::connector::ConnectorCleanupOperationId,
    payload: &PlanPayload,
    ordinal: u32,
    name: &str,
    expected_head_snapshot_id: i64,
    provenance_version: u16,
    provenance_digest_hex: Option<&str>,
    created_at_ms: i64,
    context: &ConnectorRequestContext,
    table: &crate::iceberg::table::Table,
) -> Result<Option<crate::commit::model::FrozenRequest>, ConnectorError> {
    use crate::commit::model::{AttemptToken, OperationToken, RequestShape};
    use novarocks_spi::connector::ConnectorMutationOperationId;
    let expected = OwnedRefCandidate {
        name: name.to_string(),
        head_snapshot_id: expected_head_snapshot_id,
        provenance_version,
        provenance_digest: decode_owned_ref_provenance_digest(provenance_digest_hex)?,
        created_at_ms,
    };
    if table.metadata().uuid().to_string() != payload.table_uuid
        || !matches_owned_ref_candidate(
            table.metadata(),
            &payload.namespace,
            &payload.table,
            &expected,
        )
    {
        return Ok(None);
    }
    let Some(change) = crate::commit::RefActionPlan::prepare_exact_drop(
        table,
        &payload.table_uuid,
        name,
        expected_head_snapshot_id,
    ) else {
        return Ok(None);
    };
    let token = OperationToken::from_mutation(ConnectorMutationOperationId::from_bytes(
        operation_id.to_bytes(),
    ));
    let frozen = crate::commit::staging::freeze_existing_change(
        table.identifier().clone(),
        "main",
        table.metadata(),
        table.metadata_location().ok_or_else(|| {
            invalid("Owned ref retirement has no authoritative metadata location")
        })?,
        RequestShape::RefRetention,
        AttemptToken::new(token, ordinal),
        change,
        context,
    )
    .map_err(|error| invalid(error.to_string()))?;
    Ok(Some(frozen))
}

fn ref_journal(
    runtime: &IcebergMetadataContext,
    owner: &ConnectorProviderBindingKey,
    operation_id: novarocks_spi::connector::ConnectorCleanupOperationId,
    payload: &PlanPayload,
    ordinal: u32,
    name: &str,
    expected_head_snapshot_id: i64,
    frozen: &crate::commit::model::FrozenRequest,
) -> Result<crate::commit::attempt::observation::PublicationJournal<ReceiptRecord>, ConnectorError>
{
    use novarocks_spi::connector::{ConnectorMutationOperationId, ExternalMutationEvidence};
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
        instance_id: owner.instance_id.clone(),
    };
    let external_evidence = ExternalMutationEvidence::try_new(
        1,
        descriptor,
        owner.incarnation,
        ConnectorMutationOperationId::from_bytes(operation_id.to_bytes()),
        "cleanup-owned-ref-retire",
        canonical(
            &serde_json::json!({"ordinal":ordinal,"namespace":payload.namespace,"table":payload.table,"table_uuid":payload.table_uuid,"ref":name,"head":expected_head_snapshot_id}),
        )?,
    )?;
    let journal = crate::commit::attempt::observation::PublicationJournal::for_runtime(
        frozen.artifacts().attempt().operation(),
        runtime.resources().catalog_runtime(),
    );
    journal
        .record_for_effects(
            external_evidence,
            receipt(ordinal, ObjectOutcome::Deleted, None)?,
            receipt(ordinal, ObjectOutcome::AlreadyAbsent, None)?,
            frozen,
        )
        .map_err(|error| internal(error.to_string()))?;
    Ok(journal)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::{self, File, FileTimes};
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCleanupFinalizeRequest, ConnectorCleanupOperation, ConnectorCleanupOperationId,
        ConnectorCleanupPlanningRequest, ConnectorInstanceId, ConnectorMetadata,
        ConnectorProviderBindingKey, ConnectorProviderId, ConnectorRequestContext,
        ConnectorTableHandle, ConnectorTableIdentity, ConnectorTableRequest,
        ConnectorTableResolution, ProviderBindingEpoch,
    };

    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::iceberg::TableCreation;
    use crate::iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
    use crate::metadata::IcebergMetadata;
    use crate::resources::IcebergMetadataResources;

    pub(super) fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            64 * 1024,
            256 * 1024,
        )
        .expect("context")
    }

    pub(super) fn contract_values() -> (
        ConnectorInstanceDescriptor,
        ConnectorProviderBindingKey,
        ConnectorCleanupPlan,
        PreparedBatch,
    ) {
        let instance_id = ConnectorInstanceId::parse("cleanup-test").expect("instance");
        let key = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([4; 16]),
        };
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: instance_id.clone(),
        };
        let request = ConnectorCleanupPlanningRequest::try_new(
            ConnectorCleanupOperationId::from_bytes([5; 16]),
            key.clone(),
            ConnectorCleanupOperation::remove_unreferenced_objects(
                ConnectorTableHandle::try_new(instance_id, Bytes::from_static(b"table"))
                    .expect("table"),
                1,
            )
            .expect("operation"),
            context(),
        )
        .expect("request");
        let plan = ConnectorCleanupPlan::try_new(
            &request,
            [6; 32],
            [7; 32],
            ConnectorCleanupPlanSummary::try_new(3, 30, 1).expect("summary"),
            Bytes::from_static(b"plan"),
            ConnectorProcessLocalPayload::new(key.clone(), Arc::new(())),
        )
        .expect("plan");
        let prepared = PreparedBatch::try_new(
            key.clone(),
            plan.operation_id(),
            plan.plan_digest(),
            plan.manifest_digest(),
            0,
            [8; 32],
            Bytes::from_static(b"prepared"),
        )
        .expect("prepared");
        (descriptor, key, plan, prepared)
    }

    pub(super) fn local_runtime() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        Arc<IcebergMetadataContext>,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "cleanup-test",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let resources = IcebergMetadataResources::new(binding, executor.handle().clone());
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("control runtime"),
        );
        (executor, warehouse, runtime)
    }

    fn object_record(
        ordinal: u32,
        location: impl Into<String>,
        identity: ObjectIdentity,
    ) -> ManifestRecord {
        ManifestRecord {
            ordinal,
            candidate: ManifestCandidate::Object {
                location: location.into(),
                identity,
            },
        }
    }

    #[test]
    fn unproven_scan_refuses_mature_nested_objects_after_ordinary_commit() {
        let (executor, _warehouse, runtime) = local_runtime();
        let table = executor.block_on(async {
            let schema = Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .expect("schema");
            crate::catalog::admission_test_support::create_static_fixture_table(
                &runtime,
                ProviderBindingEpoch::from_bytes([4; 16]),
                crate::catalog::CatalogNamespaceName::new("orphan_test"),
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .format_version(FormatVersion::V2)
                    .build(),
            )
            .await
        });
        let location =
            novarocks_fs::FsLocation::parse(table.metadata().location()).expect("table location");
        let root = std::path::PathBuf::from(location.path());
        let old_orphan = root.join("data/partition=1/old-orphan.parquet");
        let changed_orphan = root.join("data/partition=1/changed-orphan.parquet");
        let old_metadata_orphan = root.join("metadata/scratch/unlinked.bin");
        let retained_live_data = root.join("data/partition=1/retained.parquet");
        let young_orphan = root.join("data/partition=1/young-orphan.parquet");
        fs::create_dir_all(old_orphan.parent().expect("data parent")).expect("create data path");
        fs::create_dir_all(old_metadata_orphan.parent().expect("metadata parent"))
            .expect("create metadata path");
        fs::write(&young_orphan, b"young orphan").expect("write young orphan");
        let old_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time follows epoch")
            .as_secs()
            - 2 * 60 * 60;
        let old_time = UNIX_EPOCH + Duration::from_secs(old_seconds) + Duration::from_millis(100);
        for orphan in [
            &old_orphan,
            &changed_orphan,
            &old_metadata_orphan,
            &retained_live_data,
        ] {
            fs::write(orphan, b"old orphan").expect("write old orphan");
            File::options()
                .write(true)
                .open(orphan)
                .expect("open old orphan")
                .set_times(FileTimes::new().set_modified(old_time))
                .expect("age old orphan");
        }
        let instance_id = ConnectorInstanceId::parse("cleanup-test").expect("instance");
        let key = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([4; 16]),
        };
        let metadata = IcebergMetadata::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                instance_id: instance_id.clone(),
            },
            key.incarnation,
            Arc::clone(&runtime),
        );
        // Keep one mature physical data file live through an actual current
        // snapshot. Inline application documents have no sidecar to protect.
        executor
            .block_on(crate::commit::run::append_snapshot_for_test(
                metadata.clone(),
                table.clone(),
                vec![crate::commit::WrittenFile {
                    path: format!(
                        "{}/data/partition=1/retained.parquet",
                        table.metadata().location().trim_end_matches('/')
                    ),
                    format: crate::iceberg::spec::DataFileFormat::Parquet,
                    content: crate::iceberg::spec::DataContentType::Data,
                    partition_values: crate::iceberg::spec::Struct::empty(),
                    partition_spec_id: table.metadata().default_partition_spec_id(),
                    record_count: 3,
                    file_size_in_bytes: 10,
                    split_offsets: Vec::new(),
                    column_sizes: HashMap::new(),
                    value_counts: HashMap::new(),
                    null_value_counts: HashMap::new(),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::new(),
                    upper_bounds: HashMap::new(),
                    key_metadata: None,
                    referenced_data_file: None,
                    equality_ids: None,
                    first_row_id: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                    cardinality: None,
                }],
                "main".into(),
                std::collections::BTreeMap::new(),
            ))
            .expect("publish actual live data manifest");
        let cutoff = SystemTime::now() - Duration::from_secs(60 * 60);
        let older_than_ms = i64::try_from(
            cutoff
                .duration_since(UNIX_EPOCH)
                .expect("cutoff follows epoch")
                .as_millis(),
        )
        .expect("cutoff fits i64");
        let loaded = metadata
            .load_table(
                ConnectorTableRequest {
                    table: ConnectorTableIdentity {
                        instance_id,
                        namespace: Arc::from("orphan_test"),
                        table: Arc::from("t"),
                    },
                    resolution: ConnectorTableResolution::StrictBaseTable,
                    context: context(),
                },
                &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
                    novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                    &mut |_| Ok(()),
                    &mut |_| Ok(()),
                ),
            )
            .expect("load real table handle");
        let adapter =
            IcebergCleanupMaintenanceAdapter::new(key.clone(), runtime).expect("cleanup adapter");
        let operation =
            ConnectorCleanupOperation::remove_unreferenced_objects(loaded.table, older_than_ms)
                .expect("cleanup operation");
        let plan = adapter
            .plan_cleanup(
                ConnectorCleanupPlanningRequest::try_new(
                    ConnectorCleanupOperationId::from_bytes([5; 16]),
                    key,
                    operation,
                    context(),
                )
                .expect("planning request"),
            )
            .expect_err("age/current reachability cannot authorize scanned deletion");
        assert_eq!(plan.kind(), ConnectorErrorKind::Unsupported);
        assert!(plan.message().contains("writer fencing"));
        for path in [
            &old_orphan,
            &changed_orphan,
            &old_metadata_orphan,
            &young_orphan,
            &retained_live_data,
        ] {
            assert!(path.exists(), "scan refusal must preserve every object");
        }
        assert!(!root.join("_novarocks").exists());
        assert!(table.metadata_location().is_some_and(|path| {
            novarocks_fs::FsLocation::parse(path)
                .is_ok_and(|location| std::path::Path::new(location.path()).exists())
        }));
    }

    fn cleanup_plan_identity_fixture(
        executor: &tokio::runtime::Runtime,
        runtime: Arc<IcebergMetadataContext>,
        with_candidate: bool,
    ) -> (
        IcebergCleanupMaintenanceAdapter,
        ConnectorCleanupPlan,
        crate::iceberg::table::Table,
        std::path::PathBuf,
    ) {
        let table = executor.block_on(async {
            let schema = Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap();
            crate::catalog::admission_test_support::create_static_fixture_table(
                &runtime,
                ProviderBindingEpoch::from_bytes([4; 16]),
                crate::catalog::CatalogNamespaceName::new("orphan_test"),
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .format_version(FormatVersion::V2)
                    .build(),
            )
            .await
        });
        let location = novarocks_fs::FsLocation::parse(table.metadata().location()).unwrap();
        let root = std::path::PathBuf::from(location.path());
        if with_candidate {
            let candidate = root.join("data/orphan.parquet");
            fs::create_dir_all(candidate.parent().unwrap()).unwrap();
            fs::write(&candidate, b"orphan").unwrap();
            File::options()
                .write(true)
                .open(&candidate)
                .unwrap()
                .set_times(
                    FileTimes::new().set_modified(SystemTime::now() - Duration::from_secs(7200)),
                )
                .unwrap();
        }
        let instance_id = ConnectorInstanceId::parse("cleanup-test").unwrap();
        let key = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([4; 16]),
        };
        let metadata = IcebergMetadata::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: instance_id.clone(),
            },
            key.incarnation,
            Arc::clone(&runtime),
        );
        let loaded = metadata
            .load_table(
                ConnectorTableRequest {
                    table: ConnectorTableIdentity {
                        instance_id,
                        namespace: Arc::from("orphan_test"),
                        table: Arc::from("t"),
                    },
                    resolution: ConnectorTableResolution::StrictBaseTable,
                    context: context(),
                },
                &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
                    novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                    &mut |_| Ok(()),
                    &mut |_| Ok(()),
                ),
            )
            .unwrap();
        let cutoff = (SystemTime::now() - Duration::from_secs(3600))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let adapter = IcebergCleanupMaintenanceAdapter::new(key.clone(), runtime).unwrap();
        let operation =
            ConnectorCleanupOperation::remove_unreferenced_objects(loaded.table, cutoff).unwrap();
        let request = if with_candidate {
            ConnectorCleanupPlanningRequest::try_new(
                ConnectorCleanupOperationId::from_bytes([5; 16]),
                key.clone(),
                operation,
                context(),
            )
            .unwrap()
        } else {
            ConnectorCleanupPlanningRequest::try_new_selected_owned_refs(
                ConnectorCleanupOperationId::from_bytes([5; 16]),
                key.clone(),
                operation,
                ConnectorCleanupOwnedRefSelection::try_new(Vec::new()).unwrap(),
                context(),
            )
            .unwrap()
        };
        let plan = if !with_candidate {
            adapter.plan_cleanup(request).unwrap()
        } else {
            // Data-only legacy artifact fixture, not a supported scan or a
            // safety proof. Real table/owner/UUID checks remain under test.
            let records = vec![object_record(
                0,
                root.join("data/orphan.parquet").to_string_lossy(),
                ObjectIdentity::SizeMtime {
                    size: 6,
                    mtime_ms: 0,
                },
            )];
            let logical = LogicalManifest {
                version: ARTIFACT_VERSION,
                namespace: "orphan_test".into(),
                table: "t".into(),
                table_uuid: table.metadata().uuid().to_string(),
                older_than_ms: cutoff,
                phase: CleanupPhase::ObjectSweep,
                records,
            };
            let digest = domain_digest(MANIFEST_DOMAIN, &canonical(&logical).unwrap());
            ConnectorCleanupPlan::try_new(
                &request,
                [17; 32],
                digest,
                ConnectorCleanupPlanSummary::try_new(1, 6, 1).unwrap(),
                canonical(&PlanPayload {
                    version: ARTIFACT_VERSION,
                    namespace: "orphan_test".into(),
                    table: "t".into(),
                    table_uuid: table.metadata().uuid().to_string(),
                })
                .unwrap(),
                ConnectorProcessLocalPayload::new(
                    key,
                    Arc::new(FrozenCleanupManifest {
                        records: logical.records.into(),
                        manifest_digest: digest,
                    }),
                ),
            )
            .unwrap()
        };
        (adapter, plan, table, root)
    }

    fn with_manifest(
        plan: &ConnectorCleanupPlan,
        manifest: ConnectorProcessLocalPayload,
    ) -> ConnectorCleanupPlan {
        ConnectorCleanupPlan::try_restore(
            plan.owner().clone(),
            plan.operation_id(),
            plan.request_digest(),
            plan.base_state_digest(),
            plan.manifest_digest(),
            plan.summary(),
            plan.provider_payload().clone(),
            plan.plan_digest(),
            manifest,
        )
        .unwrap()
    }

    #[test]
    fn empty_selected_owned_ref_cleanup_preserves_independent_capability() {
        let (executor, _warehouse, runtime) = local_runtime();
        let (adapter, plan, _table, root) =
            cleanup_plan_identity_fixture(&executor, runtime, false);
        assert_eq!(plan.summary().candidate_count(), 0);
        let page = adapter
            .read_candidate_page(
                ConnectorCleanupCandidatePageRequest::try_new(plan.clone(), 0, 10, context())
                    .unwrap(),
            )
            .unwrap();
        assert!(page.candidates().is_empty());
        assert!(page.complete());
        assert!(ConnectorCleanupPrepareRequest::try_new(plan.clone(), 0, context()).is_err());
        assert!(
            adapter
                .prepare_batch(ConnectorCleanupPrepareRequest {
                    plan: plan.clone(),
                    batch_ordinal: 0,
                    context: context()
                })
                .is_err()
        );
        adapter
            .finalize_terminal(ConnectorCleanupFinalizeRequest::try_new(plan, context()).unwrap())
            .unwrap();
        assert!(!root.join("_novarocks").exists());
    }

    #[test]
    fn already_frozen_catalog_roots_are_refused_at_the_final_delete_gate() {
        let (executor, _warehouse, runtime) = local_runtime();
        let (adapter, plan, _table, root) =
            cleanup_plan_identity_fixture(&executor, Arc::clone(&runtime), false);
        let payload = adapter.plan_payload(&plan).unwrap();
        for name in [
            "v1.metadata.json",
            "v7.gz.metadata.json",
            "v8.metadata.json.gz",
            "version-hint.text",
        ] {
            let path = root.join("metadata").join(name);
            fs::write(&path, b"retained root").unwrap();
            let batch = [object_record(
                0,
                path.to_string_lossy(),
                ObjectIdentity::SizeMtime {
                    size: 13,
                    mtime_ms: 0,
                },
            )];
            let result = execute_frozen_batch(
                &runtime,
                plan.owner(),
                plan.operation_id(),
                &payload,
                &batch,
                &context(),
            )
            .err()
            .expect("unproven scanned cleanup is refused");
            assert_eq!(result.kind(), ConnectorErrorKind::Unsupported);
            assert_eq!(fs::read(&path).unwrap(), b"retained root");
        }
        // The low-level exact-delete entry is equally protected, without LIST.
        executor.block_on(async {
            let operator =
                crate::opendal::Operator::new(crate::opendal::services::Memory::default()).unwrap();
            operator
                .write("other/metadata/v1.metadata.json", "retained")
                .await
                .unwrap();
            let error = delete_exact(
                operator.clone(),
                "other/metadata/v1.metadata.json".into(),
                "s3://test/other/metadata/v1.metadata.json".into(),
                ObjectIdentity::SizeMtime {
                    size: 8,
                    mtime_ms: 0,
                },
            )
            .await
            .unwrap_err();
            assert_eq!(error.kind(), crate::opendal::ErrorKind::PermissionDenied);
            assert_eq!(
                operator
                    .read("other/metadata/v1.metadata.json")
                    .await
                    .unwrap()
                    .to_bytes(),
                Bytes::from_static(b"retained")
            );
        });
    }

    #[tokio::test]
    async fn unproven_frozen_object_delete_has_no_actual_delete_call() {
        let base =
            crate::opendal::Operator::new(crate::opendal::services::Memory::default()).unwrap();
        base.write("warehouse/data/old.parquet", b"retained".to_vec())
            .await
            .unwrap();
        let calls = super::super::cleanup_candidates::gc_io_test::GcIoCalls::default();
        let operator = base.clone().layer(calls.clone());
        let error = delete_exact(
            operator,
            "warehouse/data/old.parquet".into(),
            "s3://bucket/warehouse/data/old.parquet".into(),
            ObjectIdentity::SizeMtime {
                size: 8,
                mtime_ms: 0,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), crate::opendal::ErrorKind::Unsupported);
        assert_eq!(calls.counts(), (0, 0));
        assert_eq!(
            base.read("warehouse/data/old.parquet")
                .await
                .unwrap()
                .to_bytes(),
            Bytes::from_static(b"retained")
        );
    }

    #[test]
    fn missing_wrong_owner_type_and_digest_payloads_fail_before_delete() {
        let (executor, _warehouse, runtime) = local_runtime();
        let (adapter, plan, _table, root) = cleanup_plan_identity_fixture(&executor, runtime, true);
        let prepared = adapter
            .prepare_batch(
                ConnectorCleanupPrepareRequest::try_new(plan.clone(), 0, context()).unwrap(),
            )
            .unwrap();
        let content = Arc::clone(
            plan.manifest()
                .value::<FrozenCleanupManifest>("cleanup-test")
                .unwrap(),
        );
        let mut wrong_owner = plan.owner().clone();
        wrong_owner.instance_id = ConnectorInstanceId::parse("another-catalog").unwrap();
        let mut previous_generation = plan.owner().clone();
        previous_generation.incarnation = ProviderBindingEpoch::new();
        let bad_payloads = [
            ConnectorProcessLocalPayload::new(
                plan.owner().clone(),
                Arc::new(None::<Arc<FrozenCleanupManifest>>),
            ),
            ConnectorProcessLocalPayload::new(plan.owner().clone(), Arc::new("unexpected type")),
            ConnectorProcessLocalPayload::new(wrong_owner, content.clone()),
            ConnectorProcessLocalPayload::new(
                plan.owner().clone(),
                Arc::new(FrozenCleanupManifest {
                    records: content.records.clone(),
                    manifest_digest: [0; 32],
                }),
            ),
            ConnectorProcessLocalPayload::new(previous_generation, content.clone()),
        ];
        for (case, payload) in bad_payloads.into_iter().enumerate() {
            let bad_plan = with_manifest(&plan, payload);
            for result in [
                adapter
                    .read_candidate_page(
                        ConnectorCleanupCandidatePageRequest::try_new(
                            bad_plan.clone(),
                            0,
                            10,
                            context(),
                        )
                        .unwrap(),
                    )
                    .map(|_| ()),
                adapter
                    .prepare_batch(
                        ConnectorCleanupPrepareRequest::try_new(bad_plan.clone(), 0, context())
                            .unwrap(),
                    )
                    .map(|_| ()),
                adapter
                    .execute_batch(
                        ConnectorCleanupExecuteRequest::try_new(
                            bad_plan,
                            prepared.clone(),
                            context(),
                        )
                        .unwrap(),
                    )
                    .map(|_| ()),
            ] {
                let error = result.unwrap_err();
                assert_eq!(
                    error.kind(),
                    if case == 2 || case == 4 {
                        ConnectorErrorKind::InvalidRequest
                    } else {
                        ConnectorErrorKind::Internal
                    }
                );
                if case == 4 {
                    assert_eq!(
                        error.message(),
                        "process-local payload belongs to another connector generation"
                    );
                }
            }
            assert!(root.join("data/orphan.parquet").exists());
            assert!(!root.join("_novarocks").exists());
        }
    }

    #[test]
    fn replaced_table_uuid_refuses_page_prepare_and_execute_before_delete() {
        let (executor, _warehouse, runtime) = local_runtime();
        let (adapter, plan, table, root) =
            cleanup_plan_identity_fixture(&executor, runtime.clone(), true);
        let shared_context = context();
        let old = runtime
            .load_table_for_request("orphan_test", "t", &shared_context)
            .unwrap();
        assert_eq!(old.table.metadata().uuid(), table.metadata().uuid());
        let prepared = adapter
            .prepare_batch(
                ConnectorCleanupPrepareRequest::try_new(plan.clone(), 0, shared_context.clone())
                    .unwrap(),
            )
            .unwrap();
        // Replace only the persisted table incarnation, retaining the paths and
        // candidate so every entry point must enforce its own UUID check.
        let metadata_path =
            novarocks_fs::FsLocation::parse(table.metadata_location().unwrap()).unwrap();
        let mut metadata: Value =
            serde_json::from_slice(&fs::read(metadata_path.path()).unwrap()).unwrap();
        metadata["table-uuid"] = Value::String(uuid::Uuid::new_v4().to_string());
        fs::write(metadata_path.path(), serde_json::to_vec(&metadata).unwrap()).unwrap();
        assert_eq!(
            runtime
                .load_table_for_request("orphan_test", "t", &shared_context)
                .unwrap()
                .table
                .metadata()
                .uuid(),
            table.metadata().uuid(),
            "shared planning cache intentionally retains the old UUID"
        );
        for result in [
            adapter
                .read_candidate_page(
                    ConnectorCleanupCandidatePageRequest::try_new(
                        plan.clone(),
                        0,
                        10,
                        shared_context.clone(),
                    )
                    .unwrap(),
                )
                .map(|_| ()),
            adapter
                .prepare_batch(
                    ConnectorCleanupPrepareRequest::try_new(
                        plan.clone(),
                        0,
                        shared_context.clone(),
                    )
                    .unwrap(),
                )
                .map(|_| ()),
            adapter
                .execute_batch(
                    ConnectorCleanupExecuteRequest::try_new(plan, prepared, shared_context.clone())
                        .unwrap(),
                )
                .map(|_| ()),
        ] {
            let error = result.unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
            assert!(error.to_string().contains("table incarnation"));
        }
        assert!(root.join("data/orphan.parquet").exists());
        assert!(!root.join("_novarocks").exists());
    }

    #[test]
    fn manifest_digest_and_receipts_are_canonical_and_bounded() {
        let records = vec![object_record(
            0,
            "file:///tmp/a.parquet",
            ObjectIdentity::SizeMtime {
                size: 10,
                mtime_ms: 20,
            },
        )];
        let bytes = canonical(&records).expect("canonical records");
        let decoded: Vec<ManifestRecord> = decode_canonical(&bytes, "records").unwrap();
        assert_eq!(batch_digest(&decoded), batch_digest(&records));
        assert!(
            decode_canonical::<Vec<ManifestRecord>>(&Bytes::from_static(b"[ ]"), "records")
                .is_err()
        );
        assert_eq!(
            MAX_RECORDS,
            MAX_BATCH_OBJECTS * novarocks_spi::connector::MAX_CONNECTOR_CLEANUP_BATCHES as usize
        );
        let bounded = receipt(
            0,
            ObjectOutcome::Failed,
            Some("r".repeat(MAX_REASON_CHARS + 100)),
        )
        .expect("bounded receipt");
        assert_eq!(
            bounded.reason.as_ref().unwrap().chars().count(),
            MAX_REASON_CHARS
        );
        let outcomes = (0..MAX_BATCH_OBJECTS)
            .map(|ordinal| receipt(ordinal as u32, ObjectOutcome::AlreadyAbsent, None).unwrap())
            .collect::<Vec<_>>();
        let encoded = canonical(&outcomes).unwrap();
        let decoded: Vec<ReceiptRecord> = decode_canonical(&encoded, "receipts").unwrap();
        assert_eq!(
            receipt_summary(&decoded).unwrap().already_absent(),
            MAX_BATCH_OBJECTS as u32
        );
    }

    #[test]
    fn ref_retirement_manifest_never_carries_object_sweep_candidates() {
        let ref_pass = selected_owned_ref_manifest_records(
            vec![OwnedRefCandidate {
                name: "__novarocks_mv_publication_01890f3c-4e70-7cc0-8000-000000000041".to_string(),
                head_snapshot_id: 7,
                provenance_version: 1,
                provenance_digest: [9; 32],
                created_at_ms: 100,
            }],
            None,
        );
        assert_eq!(ref_pass.len(), 1);
        assert!(matches!(
            ref_pass[0].candidate,
            ManifestCandidate::OwnedRef { .. }
        ));
        let frozen = canonical(&ref_pass).expect("encode ref pass");
        let decoded: Vec<ManifestRecord> =
            decode_canonical(&frozen, "ref pass").expect("decode ref pass");
        assert!(matches!(
            &decoded[0].candidate,
            ManifestCandidate::OwnedRef {
                provenance_digest_hex: Some(digest),
                ..
            } if digest == &hex_encode([9; 32])
        ));

        // Model the next invocation after a successful exact CAS. It begins
        // with a new owned-ref discovery result; an empty result is the only
        // condition that permits this later pass to list object candidates.
        let next_pass = selected_owned_ref_manifest_records(Vec::new(), None);
        assert!(next_pass.is_empty());
        let object_pass = object_record(
            0,
            "file:///tmp/fresh-object.parquet",
            ObjectIdentity::SizeMtime {
                size: 1,
                mtime_ms: 1,
            },
        );
        assert!(matches!(
            object_pass.candidate,
            ManifestCandidate::Object { .. }
        ));
    }

    #[test]
    fn selected_owned_ref_plan_is_exact_and_never_promotes_to_object_sweep() {
        let current = vec![
            OwnedRefCandidate {
                name: "__novarocks_mv_publication_01890f3c-4e70-7cc0-8000-00000000000a".to_string(),
                head_snapshot_id: 7,
                provenance_version: 1,
                provenance_digest: [1; 32],
                created_at_ms: 100,
            },
            OwnedRefCandidate {
                name: "__novarocks_mv_publication_01890f3c-4e70-7cc0-8000-00000000000b".to_string(),
                head_snapshot_id: 8,
                provenance_version: 1,
                provenance_digest: [2; 32],
                created_at_ms: 100,
            },
        ];
        let exact = ConnectorCleanupOwnedRefSelection::try_new(vec![
            ConnectorCleanupOwnedRefIdentity::try_new(
                Arc::from("__novarocks_mv_publication_01890f3c-4e70-7cc0-8000-00000000000a"),
                7,
                1,
                [1; 32],
            )
            .expect("exact selected ref"),
        ])
        .expect("canonical selection");
        let selected = selected_owned_ref_manifest_records(current.clone(), Some(&exact));
        assert_eq!(selected.len(), 1);
        assert!(matches!(
            &selected[0].candidate,
            ManifestCandidate::OwnedRef { name, .. }
                if name
                    == "__novarocks_mv_publication_01890f3c-4e70-7cc0-8000-00000000000a"
        ));

        let stale = ConnectorCleanupOwnedRefSelection::try_new(vec![
            ConnectorCleanupOwnedRefIdentity::try_new(
                Arc::from("__novarocks_mv_publication_01890f3c-4e70-7cc0-8000-00000000000a"),
                7,
                1,
                [3; 32],
            )
            .expect("stale selected ref"),
        ])
        .expect("canonical stale selection");
        assert!(selected_owned_ref_manifest_records(current.clone(), Some(&stale)).is_empty());
        assert!(
            selected_owned_ref_manifest_records(
                current,
                Some(
                    &ConnectorCleanupOwnedRefSelection::try_new(Vec::new())
                        .expect("empty selection")
                ),
            )
            .is_empty()
        );
        assert_eq!(
            cleanup_phase_for_owned_refs(Some(&exact), 1),
            CleanupPhase::OwnedRefRetire
        );
        assert_eq!(
            cleanup_phase_for_owned_refs(Some(&stale), 0),
            CleanupPhase::OwnedRefRetire,
            "a stale selected ref must not fall through to an object sweep"
        );
        assert_eq!(
            cleanup_phase_for_owned_refs(None, 0),
            CleanupPhase::ObjectSweep,
            "only a fresh discovery with no owned refs may sweep objects"
        );
    }

    #[test]
    fn receipt_values_and_terminal_finalization_are_idempotent() {
        let (descriptor, key, plan, prepared) = contract_values();
        let records = vec![
            receipt(0, ObjectOutcome::Deleted, None).expect("deleted"),
            receipt(
                1,
                ObjectOutcome::Failed,
                Some("identity changed".to_string()),
            )
            .expect("failed"),
            receipt(
                2,
                ObjectOutcome::Unknown,
                Some("response was lost".to_string()),
            )
            .expect("unknown"),
        ];
        let bytes = canonical(&records).expect("receipt artifact");
        let digest = domain_digest(RECEIPT_DOMAIN, &bytes);
        let first = receipt_value(&descriptor, &key, &plan, &prepared, digest, &records)
            .expect("first receipt");
        let replay = receipt_value(&descriptor, &key, &plan, &prepared, digest, &records)
            .expect("replayed receipt");
        assert_eq!(first, replay);
        assert_eq!(first.summary().deleted(), 1);
        assert_eq!(first.summary().failed(), 1);
        assert_eq!(first.summary().unknown(), 1);
        first.validate().expect("receipt validates");

        let (_executor, _warehouse, runtime) = local_runtime();
        let adapter = IcebergCleanupMaintenanceAdapter::new(key, runtime).expect("adapter");
        adapter
            .finalize_terminal(
                ConnectorCleanupFinalizeRequest::try_new(plan.clone(), context())
                    .expect("first finalization"),
            )
            .expect("first terminal finalization");
        adapter
            .finalize_terminal(
                ConnectorCleanupFinalizeRequest::try_new(plan, context())
                    .expect("replayed finalization"),
            )
            .expect("replayed terminal finalization");
    }

    fn admission_runtime(
        catalog_type: &str,
    ) -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        Arc<IcebergMetadataContext>,
        std::net::TcpListener,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("HMS probe listener");
        listener.set_nonblocking(true).expect("nonblocking probe");
        let mut properties = vec![
            ("iceberg.catalog.type".to_string(), catalog_type.to_string()),
            (
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            ),
        ];
        if catalog_type == "hive" {
            properties.push((
                "hive.metastore.uris".to_string(),
                format!(
                    "thrift://{}",
                    listener.local_addr().expect("listener address")
                ),
            ));
        }
        let configuration = crate::catalog_config::parse_catalog_configuration("ice", &properties)
            .expect("configuration");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                crate::resources::IcebergMetadataResources::new(binding, executor.handle().clone()),
            )
            .expect("control runtime"),
        );
        (executor, warehouse, runtime, listener)
    }

    fn admission_context(catalog_type: &str) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .expect("context")
        .with_initiation(if catalog_type == "hive" {
            novarocks_spi::connector::ConnectorRequestInitiation::Statement
        } else {
            novarocks_spi::connector::ConnectorRequestInitiation::Background
        })
    }

    fn admission_table(
        instance: novarocks_spi::connector::ConnectorInstanceId,
    ) -> ConnectorTableHandle {
        ConnectorTableHandle::try_new(instance, Bytes::from_static(br#"{"namespace":"db","table":"absent","metadata_location":null,"table_info":null,"metadata_columns":[],"metadata_table_type":null,"prepared_files":[],"explicit_files":null}"#)).expect("table handle")
    }

    fn assert_admission_left_no_io(
        warehouse: &tempfile::TempDir,
        listener: &std::net::TcpListener,
    ) {
        assert_eq!(
            std::fs::read_dir(warehouse.path())
                .expect("warehouse inventory")
                .count(),
            0
        );
        let error = listener
            .accept()
            .expect_err("admission must not connect to HMS");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    }

    #[test]
    fn planning_refuses_hms_and_background_hadoop_before_cleanup_artifacts() {
        for catalog_type in ["hive", "hadoop"] {
            let (_executor, warehouse, runtime, listener) = admission_runtime(catalog_type);
            let key = ConnectorProviderBindingKey {
                instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
                incarnation: ProviderBindingEpoch::from_bytes([8; 16]),
            };
            let adapter =
                IcebergCleanupMaintenanceAdapter::new(key.clone(), runtime).expect("adapter");
            let request = ConnectorCleanupPlanningRequest::try_new(
                ConnectorCleanupOperationId::from_bytes([9; 16]),
                key.clone(),
                ConnectorCleanupOperation::remove_unreferenced_objects(
                    admission_table(key.instance_id.clone()),
                    1,
                )
                .expect("cleanup"),
                admission_context(catalog_type),
            )
            .expect("request");
            let error = adapter
                .plan_cleanup(request)
                .expect_err("catalog admission must refuse planning");
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
            assert!(error.to_string().contains(if catalog_type == "hive" {
                "read-only compatibility entry"
            } else {
                "background"
            }));
            assert_admission_left_no_io(&warehouse, &listener);
        }
    }
}
