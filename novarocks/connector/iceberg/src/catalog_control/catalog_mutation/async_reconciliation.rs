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

//! Read-only asynchronous reconciliation for the original ordinary CREATE.

use super::*;
use novarocks_spi::connector::{
    ConnectorCatalogMutationReceiving, ConnectorProcessLocalPayload, ConnectorProviderBindingKey,
};
use std::{future::Future, pin::Pin};

struct Original {
    provider: IcebergMetadata,
    request: ConnectorCatalogMutationReconcileRequest,
}

pub(super) fn reconcile_async<'a>(
    provider: &'a IcebergMetadata,
    request: ConnectorCatalogMutationReconcileRequest,
    receiving: &'a ConnectorCatalogMutationReceiving,
) -> Pin<Box<dyn Future<Output = Result<ReconciliationOutcome, ConnectorError>> + Send + 'a>> {
    let original = Arc::new(Original {
        provider: provider.clone(),
        request,
    });
    // The request's retained evidence owns the exact journal. No request or
    // generation is reconstructed from its serializable payload.
    let installed = receiving.retain_original_owner(ConnectorProcessLocalPayload::new(
        ConnectorProviderBindingKey {
            instance_id: provider.descriptor().instance_id.clone(),
            incarnation: provider.incarnation(),
        },
        original.clone(),
    ));
    Box::pin(async move {
        installed?;
        let provider = &original.provider;
        let request = &original.request;
        let evidence = &request.evidence;
        let target = validated_reconciliation_target(provider, evidence)?;
        if evidence.operation_kind() == "keyed-documents" {
            match &target {
                IcebergMutationEvidenceTarget::KeyedSlotChange { .. } => {
                    return reconcile_keyed_slot(provider, evidence, &request.context, receiving)
                        .await;
                }
                IcebergMutationEvidenceTarget::KeyedMarkers { fenced: true, .. } => {
                    return reconcile_keyed_markers(
                        provider,
                        evidence,
                        &target,
                        &request.context,
                        receiving,
                    )
                    .await;
                }
                _ => {}
            }
        }
        let namespace = evidence.operation_kind() == "create-namespace"
            && matches!(
                &target,
                IcebergMutationEvidenceTarget::Namespace {
                    should_exist: true,
                    ..
                }
            );
        let table = evidence.operation_kind() == "create-table"
            && matches!(
                &target,
                IcebergMutationEvidenceTarget::Table { .. }
                    | IcebergMutationEvidenceTarget::HadoopCreate { .. }
            );
        if !namespace && !table {
            return Err(unsupported(
                "asynchronous reconciliation requires ordinary CREATE TABLE, CREATE SCHEMA or exact keyed mutation evidence",
            ));
        }
        let (journal, entry, descriptor) = match recover_reconciliation(evidence, &request.context)?
        {
            RecoveredReconciliation::Ready(outcome) => return Ok(outcome),
            RecoveredReconciliation::Pending {
                journal,
                entry,
                descriptor,
            } => (journal, entry, descriptor),
        };
        if matches!(
            &descriptor.attribution,
            crate::commit::publication_descriptor::AttributionPlan::ObserveOnly
        ) {
            // Namespace and ordinary REST table presence cannot attribute
            // this publication. Keep the original journal verdict.
            return Ok(journal.catalog_entry_outcome(entry));
        }
        if namespace {
            return Err(internal(
                "namespace publication has unexpected exact metadata attribution",
            ));
        }
        let observed = async {
            let Some(plan) =
                hadoop_create_observation_plan(&target, &descriptor.attribution, evidence)?
            else {
                return Ok(None);
            };
            let owner = Arc::clone(provider.runtime().novarocks_catalog());
            let binding = provider
                .runtime()
                .resources()
                .planning_binding()
                .for_request(request.context.clone());
            let verdict = owner
                .adjudicate_conditional_create_for_read(plan.clone(), binding)
                .await
                .map_err(|error| unavailable(error.to_string()))?;
            hadoop_create_positive_observation(&plan, verdict)
        }
        .await;
        Ok(finish_reconciliation_observation(
            provider,
            &journal,
            entry,
            &descriptor,
            evidence,
            observed,
        ))
    })
}

async fn reconcile_keyed_slot(
    provider: &IcebergMetadata,
    evidence: &ExternalMutationEvidence,
    context: &ConnectorRequestContext,
    receiving: &ConnectorCatalogMutationReceiving,
) -> Result<ReconciliationOutcome, ConnectorError> {
    use crate::commit::publication_descriptor::PositiveAttributionObservation;
    use novarocks_spi::connector::{
        CommitConclusion, CommitProof, ConnectorDocumentStorageObservation,
    };

    let (journal, entry, descriptor) = match recover_reconciliation(evidence, context)? {
        RecoveredReconciliation::Ready(outcome) => return Ok(outcome),
        RecoveredReconciliation::Pending {
            journal,
            entry,
            descriptor,
        } => (journal, entry, descriptor),
    };
    let mut request = journal
        .keyed_observation_request()
        .cloned()
        .ok_or_else(|| invalid("Keyed slot evidence has no original document observation scope"))?;
    request.context = context.clone().after_external_effect();
    let storage = crate::document_storage::IcebergDocumentStorage::new(
        provider.descriptor().clone(),
        provider.incarnation(),
        Arc::clone(provider.runtime()),
    );
    let decoded = storage.decode_keyed_slot_change_evidence(request.clone(), evidence.clone())?;
    let observed = slot_writer_observation(&request.object_id, &decoded)?;
    let attribution = PositiveAttributionObservation::KeyedSlotWriter(observed.clone());
    if !descriptor.attribution.matches_observation(&attribution) {
        return Err(invalid(
            "SlotWriter evidence differs from the original frozen snapshot and slot",
        ));
    }
    let conclusion = match storage
        .resolve_keyed_commit_async(request.clone(), decoded)
        .await
    {
        Ok(conclusion) => conclusion,
        Err(error) => {
            return Ok(unresolved_reconciliation(
                evidence,
                format!("Exact keyed slot observation remains unresolved: {error}"),
            ));
        }
    };
    let appended = match conclusion {
        CommitConclusion::Committed(_) => {
            let applied = journal.append_positive_proof_with_finalization(
                descriptor.key,
                attribution,
                provider
                    .runtime()
                    .novarocks_catalog()
                    .positive_attribution_finalization(),
            );
            if applied.is_ok() {
                provider
                    .runtime()
                    .control_state()
                    .invalidate_table_cache(&request.target.namespace, &request.target.table);
            }
            applied
        }
        CommitConclusion::NotCommitted(CommitProof::NegativeProof) => {
            journal.append_negative_slot_proof(entry, observed)
        }
        CommitConclusion::NotCommitted(_)
        | CommitConclusion::InvalidatedUndetermined
        | CommitConclusion::StillUnknown => {
            return Ok(journal.catalog_entry_outcome(entry));
        }
    };
    if let Err(error) = appended {
        let original = journal.catalog_entry_outcome(entry);
        if !matches!(original, ExternalMutationOutcome::CommitUnknown { .. }) {
            return Ok(original);
        }
        return Ok(unresolved_reconciliation(
            evidence,
            format!("Keyed slot resolution was refused by its original journal: {error}"),
        ));
    }
    finish_keyed_reconciliation(&journal, entry, context, receiving).await
}

async fn reconcile_keyed_markers(
    provider: &IcebergMetadata,
    evidence: &ExternalMutationEvidence,
    target: &IcebergMutationEvidenceTarget,
    context: &ConnectorRequestContext,
    receiving: &ConnectorCatalogMutationReceiving,
) -> Result<ReconciliationOutcome, ConnectorError> {
    use crate::commit::publication_descriptor::PositiveAttributionObservation;
    use crate::document_storage::keyed;

    let (journal, entry, descriptor) = match recover_reconciliation(evidence, context)? {
        RecoveredReconciliation::Ready(outcome) => return Ok(outcome),
        RecoveredReconciliation::Pending {
            journal,
            entry,
            descriptor,
        } => (journal, entry, descriptor),
    };
    let original_scope = journal
        .keyed_observation_request()
        .ok_or_else(|| invalid("Keyed markers have no original document observation scope"))?;
    let IcebergMutationEvidenceTarget::KeyedMarkers {
        namespace,
        table,
        table_uuid,
        operation_id,
        properties,
        fenced: true,
    } = target
    else {
        return Err(invalid(
            "Keyed marker reconciliation requires marker evidence",
        ));
    };
    if &*original_scope.target.namespace != namespace
        || &*original_scope.target.table != table
        || original_scope.object_id.as_bytes().as_ref() != table_uuid.as_bytes()
        || *operation_id != evidence.operation_id().to_bytes()
    {
        return Err(invalid(
            "Marker evidence differs from its original target or operation",
        ));
    }
    let table_uuid = uuid::Uuid::parse_str(table_uuid)
        .map_err(|_| invalid("Original marker target has an invalid table UUID"))?;
    let expected = PositiveAttributionObservation::KeyedMarkers {
        table_uuid,
        properties: properties.clone(),
    };
    if !descriptor.attribution.matches_observation(&expected) {
        return Err(invalid(
            "Marker evidence differs from its original fenced property update",
        ));
    }

    let freshness = context.clone().after_external_effect();
    let loaded = match provider
        .runtime()
        .observe_table_classified_for_request_with_snapshots_async(
            namespace,
            table,
            &freshness,
            crate::catalog::SnapshotLoading::Refs,
        )
        .await
    {
        Ok(table) => table,
        Err((_, error)) => {
            return Ok(unresolved_reconciliation(
                evidence,
                format!("Exact keyed marker observation remains unresolved: {error}"),
            ));
        }
    };
    let table_metadata = loaded.table.metadata();
    if table_metadata.uuid() != table_uuid {
        return Ok(unresolved_reconciliation(
            evidence,
            "Original marker target was replaced",
        ));
    }
    keyed::view::parse_properties(table_metadata.properties())
        .map_err(crate::document_storage::observation::keyed_read_error)?;
    let Some(observed) = descriptor
        .attribution
        .observe_table_metadata(table_metadata)
    else {
        return Ok(journal.catalog_entry_outcome(entry));
    };
    if !descriptor.attribution.matches_observation(&observed) {
        return Ok(journal.catalog_entry_outcome(entry));
    }
    if let Err(error) = journal.append_positive_proof_with_finalization(
        descriptor.key,
        observed,
        provider
            .runtime()
            .novarocks_catalog()
            .positive_attribution_finalization(),
    ) {
        let original = journal.catalog_entry_outcome(entry);
        if !matches!(original, ExternalMutationOutcome::CommitUnknown { .. }) {
            return Ok(original);
        }
        return Ok(unresolved_reconciliation(
            evidence,
            format!("Keyed marker resolution was refused by its original journal: {error}"),
        ));
    }
    provider
        .runtime()
        .control_state()
        .invalidate_table_cache(namespace, table);
    finish_keyed_reconciliation(&journal, entry, context, receiving).await
}

async fn finish_keyed_reconciliation(
    journal: &crate::commit::attempt::observation::PublicationJournal<
        ConnectorCatalogMutationReceipt,
    >,
    entry: crate::commit::attempt::observation::JournalEntryRef,
    context: &ConnectorRequestContext,
    receiving: &ConnectorCatalogMutationReceiving,
) -> Result<ReconciliationOutcome, ConnectorError> {
    let needed = match journal.catalog_entry_outcome(entry) {
        ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            finalization,
            ..
        } => receiving.record_needed_committed(effect, &receipt, &finalization),
        _ => Ok(()),
    };
    // Cleanup consumes the original selection and artifact ledger after the
    // Provider records the exact publication proof and its verdict.
    if let Err(error) = journal
        .settle_keyed_document_cleanup_in(entry, context)
        .await
    {
        journal.record_entry_cleanup(entry, ExternalMutationFinalization::Failed(failure(&error)));
    }
    needed?;
    Ok(journal.catalog_entry_outcome(entry))
}

fn slot_writer_observation(
    object_id: &novarocks_spi::connector::ConnectorTableObjectId,
    evidence: &novarocks_spi::connector::UnknownCommitEvidence,
) -> Result<crate::commit::publication_descriptor::KeyedSlotWriterObservation, ConnectorError> {
    use crate::commit::publication_descriptor::KeyedSlotWriterObservation;
    use novarocks_spi::connector::UnknownCommitIdentity;

    let UnknownCommitIdentity::SlotWriter { slot } = &evidence.identity else {
        return Err(invalid(
            "Original pure slot change has no exact SlotWriter witness",
        ));
    };
    let table_uuid = std::str::from_utf8(object_id.as_bytes())
        .ok()
        .and_then(|encoded| uuid::Uuid::parse_str(encoded).ok())
        .ok_or_else(|| invalid("Original slot target has an invalid table UUID"))?;
    Ok(KeyedSlotWriterObservation {
        table_uuid,
        snapshot_id: evidence.preselected_snapshot,
        parent: evidence.asserted_parent,
        slot: (slot.owner.clone(), slot.name.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorSlotName, ConnectorTableObjectId, UnknownCommitEvidence, UnknownCommitIdentity,
    };

    #[test]
    fn keyed_slot_d14_uses_iceberg_encoded_object_identity() {
        let table_uuid = uuid::Uuid::now_v7();
        let object =
            ConnectorTableObjectId::try_new(bytes::Bytes::from(table_uuid.to_string())).unwrap();
        let evidence = UnknownCommitEvidence {
            asserted_parent: Some(8),
            preselected_snapshot: 10,
            identity: UnknownCommitIdentity::SlotWriter {
                slot: ConnectorSlotName::try_new("novarocks.mv", "management").unwrap(),
            },
        };
        let observed = slot_writer_observation(&object, &evidence).unwrap();
        assert_eq!(observed.table_uuid, table_uuid);
        assert_eq!(observed.snapshot_id, 10);
        assert_eq!(observed.parent, Some(8));
    }
}
