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
    BaseIdentity, Dependency, FileChanges, FrozenRequest, IsolationLevel, OperationIntent,
    OperationIntentParts, OperationToken, RequestShape, StartSnapshot, TableTarget,
};
use crate::commit::operation::{IcebergCommitOperation, OperationLimits};
use crate::commit::slot_change::{SlotChangePreparer, document_error};
use crate::commit::staging::{PreparedChange, Preparer, StagedView, StagingBase};
use crate::document_storage::{keyed, observation};
use novarocks_spi::connector::{
    ConnectorProviderBindingKey, KeyedDocumentObservationRequest, KeyedMarkerWrite, KeyedMutation,
    KeyedMutationIntent,
};

struct MarkerPreparer(KeyedMarkerWrite);
#[async_trait::async_trait]
impl Preparer for MarkerPreparer {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        _intent: &OperationIntent,
    ) -> crate::iceberg::Result<PreparedChange> {
        let updates =
            keyed::write::marker_properties(view.metadata(), &self.0).map_err(document_error)?;
        Ok(PreparedChange {
            requirements: vec![],
            updates: vec![TableUpdate::SetProperties { updates }],
        })
    }
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
            if observation::committed_version(&table)? != documents.expected_metadata
                || table.metadata().current_snapshot_id() != documents.expected_snapshot
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
    if metadata.current_snapshot_id() != documents.expected_snapshot {
        return Ok(known_conflict(
            "keyed document mutation snapshot does not match its observation",
        ));
    }
    let operation = IcebergCommitOperation::new(
        OperationToken::from_mutation(request.operation_id),
        metadata.location(),
        provider.runtime().resources().planning_binding().clone(),
        request.context.clone(),
        provider.runtime().resources().catalog_runtime().clone(),
        OperationLimits::default(),
    )
    .map_err(|error| invalid(error.to_string()))?;
    let shape = match &documents.mutation {
        KeyedMutation::Markers(_) => RequestShape::MetadataOnly,
        KeyedMutation::SlotChange(_) => RequestShape::SnapshotProducing,
        KeyedMutation::Expiry(_) => RequestShape::DocumentExpiry,
    };
    let intent = OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: table_ident(target).map_err(invalid)?,
            uuid: Some(metadata.uuid()),
        },
        target_ref: "main".into(),
        start: metadata.current_snapshot().map(|snapshot| StartSnapshot {
            snapshot_id: snapshot.snapshot_id(),
            sequence_number: snapshot.sequence_number(),
        }),
        changes: FileChanges::default(),
        dependencies: vec![if shape == RequestShape::SnapshotProducing {
            Dependency::RefUnchanged
        } else {
            Dependency::NoRead
        }],
        isolation: IsolationLevel::Snapshot,
        shape,
        summary: BTreeMap::new(),
        token: operation.token(),
    })
    .map_err(|error| invalid(error.to_string()))?
    .with_snapshot_loading(SnapshotLoading::Refs);
    let policy = RetryPolicy::from_properties(metadata.properties())
        .map_err(|error| invalid(error.to_string()))?;
    let journal = PublicationJournal::for_runtime(operation.token(), operation.runtime());
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
                KeyedMutation::Markers(_) => {
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
                    }
                }
                KeyedMutation::SlotChange(change) => {
                    let snapshot = frozen.ref_snapshot_after("main").ok_or_else(|| {
                        document_error(internal("slot mutation has no selected snapshot"))
                    })?;
                    IcebergMutationEvidenceTarget::KeyedSlotChange {
                        namespace: target.namespace.to_string(),
                        table: target.table.to_string(),
                        table_uuid: uuid.to_string(),
                        operation_id,
                        asserted_parent: *parent,
                        preselected_snapshot: snapshot,
                        slots: change
                            .slots
                            .keys()
                            .map(|slot| (slot.owner.clone(), slot.name.clone()))
                            .collect(),
                    }
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
        catalog_operation: CatalogOperation::UpdateDocuments,
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
        KeyedMutation::Markers(markers) => Some(Box::new(MarkerPreparer(markers.clone()))),
        KeyedMutation::SlotChange(change) => Some(Box::new(SlotChangePreparer {
            change: change.clone(),
        })),
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
    _operation: IcebergCommitOperation,
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
            if observation::committed_version(&table)? != documents.expected_metadata
                || table.metadata().current_snapshot_id() != documents.expected_snapshot
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
    if metadata.current_snapshot_id() != documents.expected_snapshot {
        return Ok(known_conflict(
            "keyed document mutation snapshot does not match its observation",
        ));
    }
    let operation = IcebergCommitOperation::new(
        OperationToken::from_mutation(request.operation_id),
        metadata.location(),
        provider.runtime().resources().planning_binding().clone(),
        request.context.clone(),
        provider.runtime().resources().catalog_runtime().clone(),
        OperationLimits::default(),
    )
    .map_err(|error| invalid(error.to_string()))?;
    let shape = match &documents.mutation {
        KeyedMutation::Markers(_) => RequestShape::MetadataOnly,
        KeyedMutation::SlotChange(_) => RequestShape::SnapshotProducing,
        KeyedMutation::Expiry(_) => RequestShape::DocumentExpiry,
    };
    let intent = OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: table_ident(target).map_err(invalid)?,
            uuid: Some(metadata.uuid()),
        },
        target_ref: "main".into(),
        start: metadata.current_snapshot().map(|snapshot| StartSnapshot {
            snapshot_id: snapshot.snapshot_id(),
            sequence_number: snapshot.sequence_number(),
        }),
        changes: FileChanges::default(),
        dependencies: vec![if shape == RequestShape::SnapshotProducing {
            Dependency::RefUnchanged
        } else {
            Dependency::NoRead
        }],
        isolation: IsolationLevel::Snapshot,
        shape,
        summary: BTreeMap::new(),
        token: operation.token(),
    })
    .map_err(|error| invalid(error.to_string()))?
    .with_snapshot_loading(SnapshotLoading::Refs);
    let policy = RetryPolicy::from_properties(metadata.properties())
        .map_err(|error| invalid(error.to_string()))?;
    let journal = PublicationJournal::for_runtime(operation.token(), operation.runtime());
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
                KeyedMutation::Markers(_) => {
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
                    }
                }
                KeyedMutation::SlotChange(change) => {
                    let snapshot = frozen.ref_snapshot_after("main").ok_or_else(|| {
                        document_error(internal("slot mutation has no selected snapshot"))
                    })?;
                    IcebergMutationEvidenceTarget::KeyedSlotChange {
                        namespace: target.namespace.to_string(),
                        table: target.table.to_string(),
                        table_uuid: uuid.to_string(),
                        operation_id,
                        asserted_parent: *parent,
                        preselected_snapshot: snapshot,
                        slots: change
                            .slots
                            .keys()
                            .map(|slot| (slot.owner.clone(), slot.name.clone()))
                            .collect(),
                    }
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
        catalog_operation: CatalogOperation::UpdateDocuments,
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
        KeyedMutation::Markers(markers) => Some(Box::new(MarkerPreparer(markers.clone()))),
        KeyedMutation::SlotChange(change) => Some(Box::new(SlotChangePreparer {
            change: change.clone(),
        })),
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
                _operation: operation.clone(),
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
