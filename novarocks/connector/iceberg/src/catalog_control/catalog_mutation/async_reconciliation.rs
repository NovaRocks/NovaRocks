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
        if evidence.operation_kind() == "keyed-documents"
            && matches!(
                &target,
                IcebergMutationEvidenceTarget::KeyedSlotChange { .. }
            )
        {
            return reconcile_keyed_slot(provider, evidence, &request.context, receiving).await;
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
                "asynchronous reconciliation requires ordinary CREATE TABLE, CREATE SCHEMA or exact slot-change evidence",
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
    use crate::commit::publication_descriptor::{
        KeyedSlotWriterObservation, PositiveAttributionObservation,
    };
    use novarocks_spi::connector::{
        CommitConclusion, CommitProof, ConnectorDocumentStorageObservation, UnknownCommitIdentity,
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
    let UnknownCommitIdentity::SlotWriter { slot } = &decoded.identity else {
        return Err(invalid(
            "Original pure slot change has no exact SlotWriter witness",
        ));
    };
    let observed = KeyedSlotWriterObservation {
        table_uuid: uuid::Uuid::from_slice(request.object_id.as_bytes())
            .map_err(|_| invalid("Original slot target has an invalid table UUID"))?,
        snapshot_id: decoded.preselected_snapshot,
        parent: decoded.asserted_parent,
        slot: (slot.owner.clone(), slot.name.clone()),
    };
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
    let needed = match journal.catalog_entry_outcome(entry) {
        ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            finalization,
            ..
        } => receiving.record_needed_committed(effect, &receipt, &finalization),
        _ => Ok(()),
    };
    // Cleanup consumes the original selection and artifact ledger only after
    // the Provider recorded the exact D14 proof and its independent verdict.
    if let Err(error) = journal.settle_keyed_slot_cleanup_in(entry, context).await {
        journal.record_entry_cleanup(entry, ExternalMutationFinalization::Failed(failure(&error)));
    }
    needed?;
    Ok(journal.catalog_entry_outcome(entry))
}
