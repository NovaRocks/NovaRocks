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
        if evidence.operation_kind() != "create-table"
            || !matches!(
                &target,
                IcebergMutationEvidenceTarget::Table { .. }
                    | IcebergMutationEvidenceTarget::HadoopCreate { .. }
            )
        {
            return Err(unsupported(
                "asynchronous reconciliation supports only ordinary CREATE TABLE evidence",
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
            // REST table presence/absence is not attribution for an ordinary
            // CREATE. Preserve the original journal without another read.
            return Ok(journal.catalog_entry_outcome(entry));
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
