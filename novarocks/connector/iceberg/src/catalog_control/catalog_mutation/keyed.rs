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

//! Keyed channels share the canonical commit operation and catalog transaction.
use super::*;
use crate::catalog::SnapshotLoading;
use crate::commit::attempt::observation::PublicationJournal;
use crate::commit::attempt::prepared::{
    CatalogPublisher, PinnedExpiryPreparer, PublicationPreparer,
};
use crate::commit::attempt::{self, RetryPolicy};
use crate::commit::model::{
    BaseIdentity, CommitProtocol, Dependency, FileChanges, FrozenRequest, MV_MAIN_ADVANCED,
    MvRefreshDependencies, OperationIntent, OperationIntentParts, OperationToken, RequestShape,
    StartSnapshot, TableTarget,
};
use crate::commit::operation::{IcebergCommitOperation, OperationLimits};
use crate::commit::slot_change::{
    SLOT_STORAGE_EFFECT_SUMMARY_KEY, SlotChangePreparer, document_error,
};
use crate::commit::staging::{PreparedChange, Preparer, StagedView, StagingBase};
use crate::document_storage::{keyed, observation};
use novarocks_spi::connector::{
    ConnectorProviderBindingKey, DocumentIdentity, KeyedDocumentObservationRequest,
    KeyedMarkerWrite, KeyedMutation, KeyedMutationIntent, KeyedRetirement, KeyedSlotChange,
};
use sha2::{Digest, Sha256};

pub(super) fn catalog_operation(documents: &KeyedMutationIntent) -> CatalogOperation {
    if matches!(documents.mutation, KeyedMutation::Retirement(_)) {
        CatalogOperation::PublishDocuments
    } else {
        CatalogOperation::UpdateDocuments
    }
}

struct MarkerPreparer {
    write: KeyedMarkerWrite,
    expected_metadata: Option<novarocks_spi::connector::ConnectorCommittedVersion>,
}
#[async_trait::async_trait]
impl Preparer for MarkerPreparer {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        _intent: &OperationIntent,
    ) -> crate::iceberg::Result<PreparedChange> {
        let updates = keyed::write::marker_properties(view.metadata(), &self.write)
            .map_err(document_error)?;
        if let Some(expected) = &self.expected_metadata {
            let location = observation::metadata_location(expected).map_err(document_error)?;
            let actual =
                observation::committed_version_from_metadata(view.metadata(), Some(&location))
                    .map_err(document_error)?;
            // Preflight also checks the actual catalog pointer in the frozen
            // request. Together these are the complete admitted version.
            if &actual != expected {
                return Err(fenced_marker_base_changed());
            }
        }
        Ok(PreparedChange {
            requirements: Vec::new(),
            updates: vec![TableUpdate::SetProperties { updates }],
        })
    }
}

fn validate_fenced_marker_base(
    frozen: &FrozenRequest,
    documents: &KeyedMutationIntent,
) -> crate::iceberg::Result<()> {
    if !matches!(&documents.mutation, KeyedMutation::FencedMarkers(_)) {
        return Ok(());
    }
    let expected_location =
        observation::metadata_location(&documents.expected_metadata).map_err(document_error)?;
    if !matches!(frozen.base(), BaseIdentity::Existing { metadata_location, parent, .. }
        if metadata_location == &expected_location && *parent == documents.expected_snapshot)
        || !frozen.requirements().iter().any(|requirement| {
            matches!(requirement,
            crate::iceberg::TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id }
            if r#ref == "main" && *snapshot_id == documents.expected_snapshot)
        })
    {
        return Err(fenced_marker_base_changed());
    }
    Ok(())
}

fn fenced_marker_base_changed() -> crate::iceberg::Error {
    crate::iceberg::Error::new(
        crate::iceberg::ErrorKind::CatalogCommitConflicts,
        "Fenced marker metadata base or main changed before dispatch",
    )
}

fn prepare_slot_storage_prefix(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    documents: &KeyedMutationIntent,
    metadata: &crate::iceberg::spec::TableMetadata,
    original_prefix: PreparedChange,
) -> Result<(PreparedChange, Option<[u8; 32]>), ConnectorError> {
    let KeyedMutation::SlotChangeWithTableProperties { properties, .. } = &documents.mutation
    else {
        return Ok((original_prefix, None));
    };
    let target = documents.admission.target();
    provider
        .runtime()
        .novarocks_catalog()
        .admit(&crate::catalog::admission::CatalogAdmissionRequest::new(
            CatalogOperation::AlterProperties,
            CatalogTableName::new(target.namespace.clone(), target.table.clone()),
            request.context.initiation(),
        ))
        .map_err(connector_unsupported)?;
    let prefix = prepare_set_table_properties(metadata, properties, &request.context)?;
    let encoded = serde_json::to_value(&prefix.updates)
        .map(crate::commit::model::canonical_json)
        .and_then(|value| serde_json::to_vec(&value))
        .map_err(|error| internal(format!("encode combined property effect: {error}")))?;
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.slot-storage-effect.v1\0");
    hasher.update(request.operation_id.to_bytes());
    hasher.update(&encoded);
    let digest = hasher.finalize().into();
    request.context.check_active()?;
    Ok((prefix, Some(digest)))
}

fn slot_change_evidence_target(
    frozen: &FrozenRequest,
    change: &KeyedSlotChange,
    target: &ConnectorTableIdentity,
    operation_id: [u8; 16],
    uuid: uuid::Uuid,
    parent: Option<i64>,
    storage: Option<(&[TableUpdate], [u8; 32])>,
) -> crate::iceberg::Result<IcebergMutationEvidenceTarget> {
    let snapshot = frozen
        .ref_snapshot_after("main")
        .ok_or_else(|| document_error(internal("slot mutation has no selected snapshot")))?;
    let slots = change
        .slots
        .keys()
        .map(|slot| (slot.owner.clone(), slot.name.clone()))
        .collect();
    let namespace = target.namespace.to_string();
    let table = target.table.to_string();
    let table_uuid = uuid.to_string();
    if let Some((prefix, effect_digest)) = storage {
        let encoded = DocumentIdentity::from_bytes(effect_digest).to_hex();
        if !frozen.updates().starts_with(prefix)
            || !frozen.updates().iter().any(|update| {
                matches!(update, TableUpdate::AddSnapshot { snapshot: added }
                    if added.snapshot_id() == snapshot
                        && added.summary().additional_properties
                            .get(SLOT_STORAGE_EFFECT_SUMMARY_KEY) == Some(&encoded))
            })
        {
            return Err(document_error(internal(
                "combined slot mutation lost its exact property prefix or snapshot witness",
            )));
        }
        let request_fingerprint = Sha256::digest(frozen.canonical_rest_bytes()?).into();
        Ok(IcebergMutationEvidenceTarget::KeyedSlotStorageChange {
            namespace,
            table,
            table_uuid,
            operation_id,
            asserted_parent: parent,
            preselected_snapshot: snapshot,
            slots,
            effect_digest,
            request_fingerprint,
        })
    } else {
        Ok(IcebergMutationEvidenceTarget::KeyedSlotChange {
            namespace,
            table,
            table_uuid,
            operation_id,
            asserted_parent: parent,
            preselected_snapshot: snapshot,
            slots,
        })
    }
}

fn snapshot_mismatch(
    documents: &KeyedMutationIntent,
    observed: Option<i64>,
) -> Option<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>> {
    if documents.expected_snapshot == observed {
        return None;
    }
    Some(
        if matches!(documents.mutation, KeyedMutation::Retirement(_)) {
            ExternalMutationOutcome::KnownUncommitted {
                failure: crate::commit::commit_failure::validation_failure(
                    crate::commit::model::CommitOperation::RefreshMaterializedView,
                    MV_MAIN_ADVANCED,
                ),
                cleanup: ExternalMutationFinalization::Complete,
            }
        } else {
            known_conflict("keyed document mutation snapshot does not match its observation")
        },
    )
}

fn retirement_evidence_target(
    frozen: &FrozenRequest,
    retirement: &KeyedRetirement,
    target: &ConnectorTableIdentity,
    uuid: uuid::Uuid,
    parent: Option<i64>,
) -> crate::iceberg::Result<IcebergMutationEvidenceTarget> {
    let parent =
        parent.ok_or_else(|| document_error(internal("retirement has no asserted parent")))?;
    let selected = frozen
        .ref_snapshot_after("main")
        .ok_or_else(|| document_error(internal("retirement has no selected snapshot")))?;
    let record = &retirement.record;
    let key = keyed::keys::record_key(&record.owner, &record.name);
    let envelope = keyed::record::encode_record(record).map_err(document_error)?;
    let exact_snapshot = frozen.updates().iter().any(|update| {
        matches!(update, TableUpdate::AddSnapshot { snapshot }
            if snapshot.snapshot_id() == selected
                && snapshot.parent_snapshot_id() == Some(parent)
                && snapshot.summary().operation == crate::iceberg::spec::Operation::Append
                && snapshot.summary().additional_properties.get(&key) == Some(&envelope))
    });
    if !exact_snapshot {
        return Err(document_error(internal(
            "retirement lost the original selected snapshot or record",
        )));
    }
    Ok(IcebergMutationEvidenceTarget::KeyedRetirement {
        namespace: target.namespace.to_string(),
        table: target.table.to_string(),
        table_uuid: uuid.to_string(),
        operation_id: retirement.operation_id.to_bytes(),
        retired_publication: retirement.retired_publication.to_bytes(),
        asserted_parent: parent,
        preselected_snapshot: selected,
        record_owner: record.owner.clone(),
        record_name: record.name.clone(),
        record_envelope: envelope,
    })
}

fn mutation_intent(
    metadata: &crate::iceberg::spec::TableMetadata,
    documents: &KeyedMutationIntent,
    token: OperationToken,
    summary: std::collections::BTreeMap<String, String>,
) -> Result<OperationIntent, ConnectorError> {
    let snapshot_loading = match &documents.mutation {
        KeyedMutation::Retirement(retirement)
            if Some(retirement.baseline_snapshot_id) != documents.expected_snapshot =>
        {
            SnapshotLoading::All
        }
        _ => SnapshotLoading::Refs,
    };
    let (shape, dependencies, protocol) = match &documents.mutation {
        KeyedMutation::Markers(_) => (
            RequestShape::MetadataOnly,
            vec![Dependency::NoRead],
            CommitProtocol::Legacy,
        ),
        KeyedMutation::FencedMarkers(_) => (
            RequestShape::FencedMarkers,
            vec![Dependency::RefUnchanged],
            CommitProtocol::Legacy,
        ),
        KeyedMutation::SlotChange(change)
        | KeyedMutation::SlotChangeWithTableProperties { change, .. } => {
            let observed = keyed::view::parse_properties(metadata.properties())
                .map_err(observation::keyed_read_error)?;
            let slots = change
                .slots
                .keys()
                .map(|name| {
                    let digest = observed
                        .slots
                        .get(name)
                        .map(|slot| slot.value.digest(name, slot.writer));
                    (name.clone(), digest)
                })
                .collect();
            (
                RequestShape::SnapshotProducing,
                Vec::new(),
                CommitProtocol::MvAlterProperties { slots },
            )
        }
        KeyedMutation::Retirement(retirement) => (
            RequestShape::SnapshotProducing,
            Vec::new(),
            CommitProtocol::MvRetirement {
                admitted: MvRefreshDependencies {
                    roots: std::collections::BTreeMap::from([(
                        retirement.retired_record.owner.clone(),
                        Some(retirement.expected_root),
                    )]),
                    slots: Default::default(),
                },
            },
        ),
        KeyedMutation::Expiry(_) => (
            RequestShape::DocumentExpiry,
            vec![Dependency::NoRead],
            CommitProtocol::Legacy,
        ),
    };
    OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: table_ident(documents.admission.target()).map_err(invalid)?,
            uuid: Some(metadata.uuid()),
        },
        target_ref: "main".into(),
        start: metadata.current_snapshot().map(|snapshot| StartSnapshot {
            snapshot_id: snapshot.snapshot_id(),
            sequence_number: snapshot.sequence_number(),
        }),
        changes: FileChanges::default(),
        dependencies,
        protocol,
        shape,
        summary,
        token,
    })
    .map(|intent| intent.with_snapshot_loading(snapshot_loading))
    .map_err(|error| invalid(error.to_string()))
}

pub(super) fn execute(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    documents: &KeyedMutationIntent,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    let owner = ConnectorProviderBindingKey {
        instance_id: provider.descriptor().instance_id.clone(),
        incarnation: provider.incarnation(),
    };
    documents.validate_for(&owner, request.operation_id)?;
    crate::document_storage::validate_management_admission(&documents.admission)?;
    let target = documents.admission.target();
    let object_id = documents
        .admission
        .expected_object_id()
        .ok_or_else(|| invalid("keyed mutation requires its exact table object"))?;
    let observation_request = KeyedDocumentObservationRequest {
        owner,
        catalog_handle: documents.admission.catalog_handle().clone(),
        target: target.clone(),
        object_id: object_id.clone(),
        context: request.context.clone(),
    };
    let (metadata, metadata_location, prefix) = match &documents.mutation {
        KeyedMutation::Expiry(expiry) => {
            let (metadata, change) = keyed::expiry::prepare(&observation_request, expiry)?;
            let location = observation::metadata_location(&documents.expected_metadata)?;
            (metadata, location, change)
        }
        _ => {
            let physical = provider
                .runtime()
                .observe_table_classified_for_request_with_snapshots(
                    &target.namespace,
                    &target.table,
                    &request.context,
                    SnapshotLoading::Refs,
                )
                .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
            let table = physical.into_table();
            observation::validate_expected_object(object_id, table.metadata())?;
            keyed::view::parse_properties(table.metadata().properties())
                .map_err(observation::keyed_read_error)?;
            if !matches!(documents.mutation, KeyedMutation::Retirement(_))
                && (observation::committed_version(&table)? != documents.expected_metadata
                    || table.metadata().current_snapshot_id() != documents.expected_snapshot)
            {
                return Ok(known_conflict(
                    "keyed document mutation observation has changed",
                ));
            }
            let location = table
                .metadata_location()
                .ok_or_else(|| invalid("keyed mutation base has no metadata location"))?
                .to_string();
            (
                table.metadata().clone(),
                location,
                PreparedChange::default(),
            )
        }
    };
    observation::validate_expected_object(object_id, &metadata)?;
    if let Some(conflict) = snapshot_mismatch(documents, metadata.current_snapshot_id()) {
        return Ok(conflict);
    }
    let (prefix, storage_effect) =
        match prepare_slot_storage_prefix(provider, request, documents, &metadata, prefix) {
            Ok(prepared) => prepared,
            Err(error) => return Ok(known_uncommitted(error)),
        };
    let summary = storage_effect
        .into_iter()
        .map(|digest| {
            (
                SLOT_STORAGE_EFFECT_SUMMARY_KEY.to_owned(),
                DocumentIdentity::from_bytes(digest).to_hex(),
            )
        })
        .collect();
    let preflight_property_updates = storage_effect.map(|digest| (prefix.updates.clone(), digest));
    let operation = IcebergCommitOperation::new(
        OperationToken::from_mutation(request.operation_id),
        metadata.location(),
        provider.runtime().resources().planning_binding().clone(),
        request.context.clone(),
        provider.runtime().resources().catalog_runtime().clone(),
        OperationLimits::default(),
    )
    .map_err(|error| invalid(error.to_string()))?;
    let intent = mutation_intent(&metadata, documents, operation.token(), summary)?;
    let shape = intent.shape();
    let policy = RetryPolicy::from_properties(metadata.properties())
        .map_err(|error| invalid(error.to_string()))?;
    let journal = PublicationJournal::for_keyed_operation(&operation, observation_request.clone());
    let preflight_journal = journal.clone();
    let preflight_provider = provider.clone();
    let preflight_documents = documents.clone();
    let preflight_request = request.clone();
    let preflight = Arc::new(
        move |frozen: &FrozenRequest, _operation: &IcebergCommitOperation| {
            let BaseIdentity::Existing { uuid, parent, .. } = frozen.base() else {
                return Err(document_error(internal(
                    "keyed mutation acquired a create base",
                )));
            };
            let target = preflight_documents.admission.target();
            let operation_id = preflight_request.operation_id.to_bytes();
            let target = match &preflight_documents.mutation {
                KeyedMutation::Markers(_) | KeyedMutation::FencedMarkers(_) => {
                    validate_fenced_marker_base(frozen, &preflight_documents)?;
                    let properties = frozen
                        .updates()
                        .iter()
                        .filter_map(|update| match update {
                            TableUpdate::SetProperties { updates } => Some(updates),
                            _ => None,
                        })
                        .flat_map(|updates| updates.iter())
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect();
                    IcebergMutationEvidenceTarget::KeyedMarkers {
                        namespace: target.namespace.to_string(),
                        table: target.table.to_string(),
                        table_uuid: uuid.to_string(),
                        operation_id,
                        properties,
                        fenced: frozen.shape() == RequestShape::FencedMarkers,
                    }
                }
                KeyedMutation::SlotChange(change)
                | KeyedMutation::SlotChangeWithTableProperties { change, .. } => {
                    slot_change_evidence_target(
                        frozen,
                        change,
                        target,
                        operation_id,
                        *uuid,
                        *parent,
                        preflight_property_updates
                            .as_ref()
                            .map(|(updates, digest)| (updates.as_slice(), *digest)),
                    )?
                }
                KeyedMutation::Retirement(retirement) => {
                    retirement_evidence_target(frozen, retirement, target, *uuid, *parent)?
                }
                KeyedMutation::Expiry(_) => IcebergMutationEvidenceTarget::KeyedExpiry {
                    namespace: target.namespace.to_string(),
                    table: target.table.to_string(),
                    table_uuid: uuid.to_string(),
                    operation_id,
                },
            };
            let evidence = evidence(
                &preflight_provider,
                preflight_request.operation_id,
                preflight_request.operation.kind(),
                target,
            )
            .map_err(document_error)?;
            let receipt = receipt(
                &preflight_provider,
                preflight_request.operation_id,
                preflight_request.operation.kind(),
            )
            .map_err(document_error)?;
            preflight_journal.record(evidence, receipt, frozen)?;
            Ok(())
        },
    );
    let owner_publisher: Arc<dyn PublicationPreparer> = Arc::new(CatalogPublisher {
        expected_table_uuid: None,
        catalog_operation: catalog_operation(documents),
        catalog: Arc::clone(provider.runtime().novarocks_catalog()),
        ident: intent.target().ident.clone(),
        target_ref: "main".into(),
        operation: operation.token(),
        recovery_preflight: preflight,
    });
    let inner: Arc<dyn PublicationPreparer> = if shape == RequestShape::DocumentExpiry {
        Arc::new(PinnedExpiryPreparer {
            inner: owner_publisher,
            base: StagingBase::Existing {
                metadata: metadata.clone(),
                metadata_location,
            },
        })
    } else {
        owner_publisher
    };
    let evidence_policy = inner.evidence_policy();
    let publisher = journal.observe_prepared(inner);
    let preparer: Option<Box<dyn Preparer + Send + Sync>> = match &documents.mutation {
        KeyedMutation::Markers(markers) | KeyedMutation::FencedMarkers(markers) => {
            Some(Box::new(MarkerPreparer {
                write: markers.clone(),
                expected_metadata: matches!(&documents.mutation, KeyedMutation::FencedMarkers(_))
                    .then(|| documents.expected_metadata.clone()),
            }))
        }
        KeyedMutation::SlotChange(change)
        | KeyedMutation::SlotChangeWithTableProperties { change, .. } => {
            Some(Box::new(SlotChangePreparer {
                change: change.clone(),
            }))
        }
        KeyedMutation::Retirement(retirement) => {
            Some(Box::new(crate::commit::retirement::RetirementPreparer {
                retirement: retirement.as_ref().clone(),
            }))
        }
        KeyedMutation::Expiry(_) => None,
    };
    let run_operation = operation.clone();
    let result = operation.runtime().block_on(async move {
        let preparers: Vec<&dyn Preparer> = preparer
            .as_ref()
            .map(|preparer| preparer.as_ref() as &dyn Preparer)
            .into_iter()
            .collect();
        attempt::run(
            &run_operation,
            &intent,
            &prefix,
            &preparers,
            &publisher,
            policy,
        )
        .await
    });
    let mut outcome = match result {
        Ok(report) => journal.project(report),
        Err(message) => {
            let recovery_operation = operation.clone();
            let recovery_journal = journal.clone();
            match operation.runtime().block_on(async move {
                recovery_journal
                    .recover_bridge(&recovery_operation, message)
                    .await
            }) {
                Ok(outcome) => outcome,
                Err(message) => journal.bridge_failure(&operation, message),
            }
        }
    };
    if shape != RequestShape::DocumentExpiry
        && let ExternalMutationOutcome::KnownUncommitted {
            failure: rejection, ..
        } = &mut outcome
        && rejection.kind() == ConnectorMutationFailureKind::InvalidRequest
    {
        let error = keyed::write::refusal_with_retained_bytes(
            provider.runtime(),
            &target.namespace,
            &target.table,
            &request.context,
            invalid(rejection.message()),
        );
        *rejection = failure(&error);
    }
    if matches!(outcome, ExternalMutationOutcome::KnownCommitted { .. }) {
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&target.namespace, &target.table);
    }
    if shape != RequestShape::DocumentExpiry
        && let ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization,
            ..
        } = &mut outcome
    {
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&target.namespace, &target.table);
        let refreshed = journal
            .current_entry()
            .ok_or_else(|| internal("committed keyed publication has no journal entry"))
            .and_then(|entry| journal.finalization_context(entry, evidence_policy))
            .and_then(|grant| grant.for_io(&request.context))
            .and_then(|context| {
                let _finalizing = journal
                    .current_entry()
                    .and_then(|entry| journal.finalization_activity(entry));
                let physical = provider
                    .runtime()
                    .observe_table_classified_for_request_with_snapshots(
                        &target.namespace,
                        &target.table,
                        &context,
                        SnapshotLoading::Refs,
                    )
                    .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
                let current = physical.into_table();
                observation::validate_expected_object(object_id, current.metadata())?;
                let entry = journal
                    .current_entry()
                    .expect("committed keyed publication entry");
                let crate::catalog::error::CatalogOutcome::KnownCommitted {
                    receipt: proof, ..
                } = journal.commit_outcome(entry)
                else {
                    return Err(internal("committed keyed publication lost its proof"));
                };
                if proof
                    .metadata_location
                    .as_deref()
                    .is_some_and(|location| current.metadata_location() != Some(location))
                    || proof.snapshot_id.is_some_and(|snapshot| {
                        current.metadata().current_snapshot_id() != Some(snapshot)
                    })
                {
                    return Err(corrupt(
                        "Post-publication keyed metadata no longer belongs to this operation",
                    ));
                }
                observation::committed_version(&current)
            });
        let (version, completion) = match refreshed {
            Ok(version) => (Some(version), finalization.clone()),
            Err(error) => (
                None,
                match finalization.clone() {
                    ExternalMutationFinalization::Complete => {
                        ExternalMutationFinalization::Failed(failure(&error))
                    }
                    ExternalMutationFinalization::Failed(owner) => {
                        ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                            owner.kind(),
                            format!(
                                "{}; keyed metadata projection failed: {error}",
                                owner.message()
                            ),
                        ))
                    }
                },
            ),
        };
        if let Some(entry) = journal.current_entry() {
            journal.record_entry_finalization(entry, completion.clone());
        }
        (*receipt, *finalization) = committed_metadata_receipt(
            provider,
            request.operation_id,
            request.operation.kind(),
            None,
            version,
            completion,
        );
    }
    Ok(outcome)
}

/// Original operation and publication journal retained by the caller's Source.
/// This opaque custody grants no authority to prepare or replay an operation.
struct KeyedMutationOriginalOwner {
    _journal: PublicationJournal<ConnectorCatalogMutationReceipt>,
}

pub(super) async fn execute_async(
    provider: &IcebergMetadata,
    request: &ConnectorCatalogMutationRequest,
    documents: &KeyedMutationIntent,
    receiving: &novarocks_spi::connector::ConnectorCatalogMutationReceiving,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    let owner = ConnectorProviderBindingKey {
        instance_id: provider.descriptor().instance_id.clone(),
        incarnation: provider.incarnation(),
    };
    documents.validate_for(&owner, request.operation_id)?;
    crate::document_storage::validate_management_admission(&documents.admission)?;
    let target = documents.admission.target();
    let object_id = documents
        .admission
        .expected_object_id()
        .ok_or_else(|| invalid("keyed mutation requires its exact table object"))?;
    let observation_request = KeyedDocumentObservationRequest {
        owner,
        catalog_handle: documents.admission.catalog_handle().clone(),
        target: target.clone(),
        object_id: object_id.clone(),
        context: request.context.clone(),
    };
    let (metadata, metadata_location, prefix) = match &documents.mutation {
        KeyedMutation::Expiry(expiry) => {
            let (metadata, change) = keyed::expiry::prepare(&observation_request, expiry)?;
            let location = observation::metadata_location(&documents.expected_metadata)?;
            (metadata, location, change)
        }
        _ => {
            let physical = provider
                .runtime()
                .observe_table_classified_for_request_with_snapshots_async(
                    &target.namespace,
                    &target.table,
                    &request.context,
                    SnapshotLoading::Refs,
                )
                .await
                .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
            let table = physical.into_table();
            observation::validate_expected_object(object_id, table.metadata())?;
            keyed::view::parse_properties(table.metadata().properties())
                .map_err(observation::keyed_read_error)?;
            if !matches!(documents.mutation, KeyedMutation::Retirement(_))
                && (observation::committed_version(&table)? != documents.expected_metadata
                    || table.metadata().current_snapshot_id() != documents.expected_snapshot)
            {
                return Ok(known_conflict(
                    "keyed document mutation observation has changed",
                ));
            }
            let location = table
                .metadata_location()
                .ok_or_else(|| invalid("keyed mutation base has no metadata location"))?
                .to_string();
            (
                table.metadata().clone(),
                location,
                PreparedChange::default(),
            )
        }
    };
    observation::validate_expected_object(object_id, &metadata)?;
    if let Some(conflict) = snapshot_mismatch(documents, metadata.current_snapshot_id()) {
        return Ok(conflict);
    }
    let (prefix, storage_effect) =
        match prepare_slot_storage_prefix(provider, request, documents, &metadata, prefix) {
            Ok(prepared) => prepared,
            Err(error) => return Ok(known_uncommitted(error)),
        };
    let summary = storage_effect
        .into_iter()
        .map(|digest| {
            (
                SLOT_STORAGE_EFFECT_SUMMARY_KEY.to_owned(),
                DocumentIdentity::from_bytes(digest).to_hex(),
            )
        })
        .collect();
    let preflight_property_updates = storage_effect.map(|digest| (prefix.updates.clone(), digest));
    let operation = IcebergCommitOperation::new(
        OperationToken::from_mutation(request.operation_id),
        metadata.location(),
        provider.runtime().resources().planning_binding().clone(),
        request.context.clone(),
        provider.runtime().resources().catalog_runtime().clone(),
        OperationLimits::default(),
    )
    .map_err(|error| invalid(error.to_string()))?;
    let intent = mutation_intent(&metadata, documents, operation.token(), summary)?;
    let shape = intent.shape();
    let policy = RetryPolicy::from_properties(metadata.properties())
        .map_err(|error| invalid(error.to_string()))?;
    let journal = PublicationJournal::for_keyed_operation(&operation, observation_request.clone());
    let preflight_journal = journal.clone();
    let preflight_provider = provider.clone();
    let preflight_documents = documents.clone();
    let preflight_request = request.clone();
    let preflight = Arc::new(
        move |frozen: &FrozenRequest, _operation: &IcebergCommitOperation| {
            let BaseIdentity::Existing { uuid, parent, .. } = frozen.base() else {
                return Err(document_error(internal(
                    "keyed mutation acquired a create base",
                )));
            };
            let target = preflight_documents.admission.target();
            let operation_id = preflight_request.operation_id.to_bytes();
            let target = match &preflight_documents.mutation {
                KeyedMutation::Markers(_) | KeyedMutation::FencedMarkers(_) => {
                    validate_fenced_marker_base(frozen, &preflight_documents)?;
                    let properties = frozen
                        .updates()
                        .iter()
                        .filter_map(|update| match update {
                            TableUpdate::SetProperties { updates } => Some(updates),
                            _ => None,
                        })
                        .flat_map(|updates| updates.iter())
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect();
                    IcebergMutationEvidenceTarget::KeyedMarkers {
                        namespace: target.namespace.to_string(),
                        table: target.table.to_string(),
                        table_uuid: uuid.to_string(),
                        operation_id,
                        properties,
                        fenced: frozen.shape() == RequestShape::FencedMarkers,
                    }
                }
                KeyedMutation::SlotChange(change)
                | KeyedMutation::SlotChangeWithTableProperties { change, .. } => {
                    slot_change_evidence_target(
                        frozen,
                        change,
                        target,
                        operation_id,
                        *uuid,
                        *parent,
                        preflight_property_updates
                            .as_ref()
                            .map(|(updates, digest)| (updates.as_slice(), *digest)),
                    )?
                }
                KeyedMutation::Retirement(retirement) => {
                    retirement_evidence_target(frozen, retirement, target, *uuid, *parent)?
                }
                KeyedMutation::Expiry(_) => IcebergMutationEvidenceTarget::KeyedExpiry {
                    namespace: target.namespace.to_string(),
                    table: target.table.to_string(),
                    table_uuid: uuid.to_string(),
                    operation_id,
                },
            };
            let evidence = evidence(
                &preflight_provider,
                preflight_request.operation_id,
                preflight_request.operation.kind(),
                target,
            )
            .map_err(document_error)?;
            let receipt = receipt(
                &preflight_provider,
                preflight_request.operation_id,
                preflight_request.operation.kind(),
            )
            .map_err(document_error)?;
            preflight_journal.record(evidence, receipt, frozen)?;
            Ok(())
        },
    );
    let owner_publisher: Arc<dyn PublicationPreparer> = Arc::new(CatalogPublisher {
        expected_table_uuid: None,
        catalog_operation: catalog_operation(documents),
        catalog: Arc::clone(provider.runtime().novarocks_catalog()),
        ident: intent.target().ident.clone(),
        target_ref: "main".into(),
        operation: operation.token(),
        recovery_preflight: preflight,
    });
    let inner: Arc<dyn PublicationPreparer> = if shape == RequestShape::DocumentExpiry {
        Arc::new(PinnedExpiryPreparer {
            inner: owner_publisher,
            base: StagingBase::Existing {
                metadata: metadata.clone(),
                metadata_location,
            },
        })
    } else {
        owner_publisher
    };
    let publisher = journal.observe_catalog_prepared(inner, receiving, &request.context);
    let preparer: Option<Box<dyn Preparer + Send + Sync>> = match &documents.mutation {
        KeyedMutation::Markers(markers) | KeyedMutation::FencedMarkers(markers) => {
            Some(Box::new(MarkerPreparer {
                write: markers.clone(),
                expected_metadata: matches!(&documents.mutation, KeyedMutation::FencedMarkers(_))
                    .then(|| documents.expected_metadata.clone()),
            }))
        }
        KeyedMutation::SlotChange(change)
        | KeyedMutation::SlotChangeWithTableProperties { change, .. } => {
            Some(Box::new(SlotChangePreparer {
                change: change.clone(),
            }))
        }
        KeyedMutation::Retirement(retirement) => {
            Some(Box::new(crate::commit::retirement::RetirementPreparer {
                retirement: retirement.as_ref().clone(),
            }))
        }
        KeyedMutation::Expiry(_) => None,
    };
    let preparers: Vec<&dyn Preparer> = preparer
        .as_ref()
        .map(|preparer| preparer.as_ref() as &dyn Preparer)
        .into_iter()
        .collect();
    receiving.retain_original_owner(
        novarocks_spi::connector::ConnectorProcessLocalPayload::new(
            ConnectorProviderBindingKey {
                instance_id: provider.descriptor().instance_id.clone(),
                incarnation: provider.incarnation(),
            },
            Arc::new(KeyedMutationOriginalOwner {
                _journal: journal.clone(),
            }),
        ),
    )?;
    let report = attempt::run_to_committed_frontier(
        &operation, &intent, &prefix, &preparers, &publisher, policy,
    )
    .await;
    let (effect, mut receipt, mut finalization, deferred_cleanup, mut cleanup) = match journal
        .project_retained_committed_frontier(report)
    {
        crate::commit::attempt::observation::RetainedPublicationProjection::Committed {
            effect,
            receipt,
            finalization,
        } => (effect, receipt, finalization, true, None),
        crate::commit::attempt::observation::RetainedPublicationProjection::Settled(
            ExternalMutationOutcome::KnownCommitted {
                effect,
                receipt,
                finalization,
                cleanup,
            },
        ) => (effect, receipt, finalization, false, Some(cleanup)),
        crate::commit::attempt::observation::RetainedPublicationProjection::Settled(outcome) => {
            if shape != RequestShape::DocumentExpiry
                && let ExternalMutationOutcome::KnownUncommitted {
                    failure: rejection, ..
                } = &outcome
                && rejection.kind() == ConnectorMutationFailureKind::InvalidRequest
            {
                let error = keyed::write::refusal_with_retained_bytes_async(
                    provider.runtime(),
                    &target.namespace,
                    &target.table,
                    &request.context,
                    invalid(rejection.message()),
                )
                .await;
                // This observation is diagnostic only. The journal's original
                // rejection, including its source and failure fact, remains primary.
                tracing::warn!("Keyed catalog mutation refusal diagnostic: {error}");
            }
            return Ok(outcome);
        }
    };
    // Actual Applied and its original finite context were received by the
    // concrete publisher before ready-child Drop. Never recreate that grant.
    let finalization_context = if effect == ExternalMutationEffect::Applied {
        receiving
            .observation()
            .finalization_context()
            .cloned()
            .unwrap_or_else(|| {
                Err(internal(
                    "applied keyed publication has no original finalization context",
                ))
            })
    } else {
        // NoOp may still require the same product projection, under its original
        // business control; it does not acquire necessary-effect permission.
        Ok(request.context.clone())
    };
    provider
        .runtime()
        .control_state()
        .invalidate_table_cache(&target.namespace, &target.table);
    if shape != RequestShape::DocumentExpiry {
        let refreshed = async {
            let entry = journal
                .current_entry()
                .ok_or_else(|| internal("committed keyed publication has no journal entry"))?;
            let context = finalization_context.clone()?;
            let _finalizing = journal.finalization_activity(entry);
            let physical = provider
                .runtime()
                .observe_table_classified_for_request_with_snapshots_async(
                    &target.namespace,
                    &target.table,
                    &context,
                    SnapshotLoading::Refs,
                )
                .await
                .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
            let current = physical.into_table();
            observation::validate_expected_object(object_id, current.metadata())?;
            let crate::catalog::error::CatalogOutcome::KnownCommitted { receipt: proof, .. } =
                journal.commit_outcome(entry)
            else {
                return Err(internal("committed keyed publication lost its proof"));
            };
            if proof
                .metadata_location
                .as_deref()
                .is_some_and(|location| current.metadata_location() != Some(location))
                || proof.snapshot_id.is_some_and(|snapshot| {
                    current.metadata().current_snapshot_id() != Some(snapshot)
                })
            {
                return Err(corrupt(
                    "Post-publication keyed metadata no longer belongs to this operation",
                ));
            }
            observation::committed_version(&current)
        }
        .await;
        let (version, completion) = match refreshed {
            Ok(version) => (Some(version), finalization),
            Err(error) => (
                None,
                match finalization {
                    ExternalMutationFinalization::Complete => {
                        ExternalMutationFinalization::Failed(failure(&error))
                    }
                    ExternalMutationFinalization::Failed(owner) => {
                        // The first necessary-finalization cause remains the authority.
                        tracing::warn!(%error, "later keyed metadata projection also failed");
                        ExternalMutationFinalization::Failed(owner)
                    }
                },
            ),
        };
        if let Some(entry) = journal.current_entry() {
            journal.record_entry_finalization(entry, completion.clone());
        }
        (receipt, finalization) = committed_metadata_receipt(
            provider,
            request.operation_id,
            request.operation.kind(),
            None,
            version,
            completion,
        );
    }
    // Necessary business facts and later safe GC are different frontiers of
    // this same original operation. This does not claim cleanup completion.
    receiving.record_needed_committed(effect, &receipt, &finalization)?;
    if deferred_cleanup {
        if let Err(error) = journal
            .settle_committed_cleanup_in(&operation, &request.context)
            .await
        {
            let entry = journal
                .current_entry()
                .ok_or_else(|| internal("committed keyed cleanup has no original journal entry"))?;
            journal
                .record_entry_cleanup(entry, ExternalMutationFinalization::Failed(failure(&error)));
        }
        let entry = journal
            .current_entry()
            .ok_or_else(|| internal("committed keyed cleanup has no original journal entry"))?;
        cleanup = journal.observed_entry_cleanup(entry);
    }
    let cleanup =
        cleanup.ok_or_else(|| internal("committed keyed cleanup has no actual observation"))?;
    Ok(ExternalMutationOutcome::KnownCommitted {
        effect,
        receipt,
        finalization,
        cleanup,
    })
}
