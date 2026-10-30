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

//! Direct async leaves for the original property and table-drop publications.

use super::*;
use crate::catalog::publication_dispatch::PreparationFailure;
use crate::commit::attempt::observation::{
    PendingPublicationFinalization, PublicationJournal, PublicationReceipts,
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationReceiving, ConnectorProcessLocalPayload, ConnectorProviderBindingKey,
};
use std::{future::Future, pin::Pin, sync::OnceLock};

type Outcome = ExternalMutationOutcome<ConnectorCatalogMutationReceipt>;
struct Original {
    provider: IcebergMetadata,
    request: ConnectorCatalogMutationRequest,
    journal: PublicationJournal<ConnectorCatalogMutationReceipt>,
    table: OnceLock<OriginalTable>,
}
enum OriginalTable {
    Physical(crate::loaded_table::IcebergPhysicalTable),
    Catalog(crate::loaded_table::IcebergLoadedTable),
}

pub(super) fn execute_async<'a>(
    provider: &'a IcebergMetadata,
    request: ConnectorCatalogMutationRequest,
    receiving: &'a ConnectorCatalogMutationReceiving,
) -> Pin<Box<dyn Future<Output = Result<Outcome, ConnectorError>> + Send + 'a>> {
    let original = Arc::new(Original {
        provider: provider.clone(),
        journal: PublicationJournal::for_runtime(
            crate::commit::model::OperationToken::from_mutation(request.operation_id),
            provider.runtime().resources().catalog_runtime(),
        ),
        request,
        table: OnceLock::new(),
    });
    // The exact operation and journal outlive the borrower and every physical
    // child. This receipt is installed before constructing the opaque future.
    let installed = receiving.retain_original_owner(ConnectorProcessLocalPayload::new(
        ConnectorProviderBindingKey {
            instance_id: provider.descriptor().instance_id.clone(),
            incarnation: provider.incarnation(),
        },
        original.clone(),
    ));
    Box::pin(async move {
        installed?;
        if let Err(error) = ConnectorCatalogMutation::admit(&original.provider, &original.request) {
            return Ok(known_uncommitted(error));
        }
        match &original.request.operation {
            ConnectorCatalogMutationOperation::DropTable { .. } => {
                drop_table(&original, receiving).await
            }
            ConnectorCatalogMutationOperation::AlterProperties { .. } => {
                properties(&original, receiving).await
            }
            _ => unreachable!("only the two closed leaves construct this original"),
        }
    })
}

impl Original {
    fn retain_table(&self, table: OriginalTable) -> &OriginalTable {
        assert!(
            self.table.set(table).is_ok(),
            "one original catalog table receiver"
        );
        self.table
            .get()
            .expect("original table installed before interpretation")
    }
    fn call_error(&self, error: crate::iceberg::Error) -> Outcome {
        // Dispatch certainty comes only from this journal. The existing typed
        // error author supplies the cause, never the effect or replay policy.
        let actual = crate::commit::attempt::before_dispatch_failure(&error);
        let Some(entry) = self.journal.current_entry() else {
            return Outcome::KnownUncommitted {
                failure: actual,
                cleanup: ExternalMutationFinalization::Complete,
            };
        };
        let original_failure = self.journal.catalog_publication_failure().is_some();
        match self.journal.catalog_entry_outcome(entry) {
            Outcome::CommitUnknown { evidence, failure } => Outcome::CommitUnknown {
                failure: if original_failure { failure } else { actual },
                evidence,
            },
            Outcome::KnownUncommitted { failure, cleanup } => Outcome::KnownUncommitted {
                failure: if original_failure { failure } else { actual },
                cleanup,
            },
            outcome @ Outcome::KnownCommitted { .. } => outcome,
        }
    }
}

async fn finish(
    original: &Original,
    receiving: &ConnectorCatalogMutationReceiving,
    pending: PendingPublicationFinalization<ConnectorCatalogMutationReceipt>,
) -> Result<Outcome, ConnectorError> {
    let entry = pending.entry();
    // Receive actual Applied and its same finite context before any ready child
    // Drop. The concrete shared adapter deliberately does nothing for NoOp.
    pending.receive_catalog_applied(receiving, &original.request.context)?;
    let policy = pending.evidence_policy();
    if let Err(error) = pending.finish(policy).await {
        return Ok(original.call_error(error));
    }
    let outcome = original.journal.catalog_entry_outcome(entry);
    if let Outcome::KnownCommitted {
        effect,
        receipt,
        finalization,
        ..
    } = &outcome
    {
        receiving.record_needed_committed(*effect, receipt, finalization)?;
    }
    Ok(outcome)
}

async fn properties(
    original: &Original,
    receiving: &ConnectorCatalogMutationReceiving,
) -> Result<Outcome, ConnectorError> {
    let provider = &original.provider;
    let request = &original.request;
    let ConnectorCatalogMutationOperation::AlterProperties {
        table,
        changes,
        authority,
        expected_committed_partitioning,
    } = &request.operation
    else {
        unreachable!()
    };
    let prepared = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .observe_preparation_async(
            async {
                ensure_owner(provider, &table.instance_id)?;
                let loaded = provider
                    .runtime()
                    .load_table_original_for_request_async(
                        &table.namespace,
                        &table.table,
                        &request.context,
                    )
                    .await?;
                let OriginalTable::Physical(loaded) =
                    original.retain_table(OriginalTable::Physical(loaded))
                else {
                    unreachable!("property preparation retains its original physical table")
                };
                let metadata = loaded.table.metadata();
                let change = prepare_property_change(
                    metadata,
                    changes,
                    *authority,
                    expected_committed_partitioning.as_ref(),
                )?;
                let frozen = crate::commit::staging::freeze_existing_change(
                    loaded.table.identifier().clone(),
                    "main",
                    metadata,
                    loaded.table.metadata_location().ok_or_else(|| {
                        invalid("Table publication requires an authoritative metadata location")
                    })?,
                    crate::commit::model::RequestShape::MetadataOnly,
                    crate::commit::model::AttemptToken::new(
                        crate::commit::model::OperationToken::from_mutation(request.operation_id),
                        0,
                    ),
                    change,
                    &request.context,
                )
                .map_err(|error| invalid(error.to_string()))?;
                let external = evidence(
                    provider,
                    request.operation_id,
                    request.operation.kind(),
                    IcebergMutationEvidenceTarget::TableVersion {
                        namespace: table.namespace.to_string(),
                        table: table.table.to_string(),
                        table_uuid: metadata.uuid().to_string(),
                        before_metadata_location: loaded
                            .table
                            .metadata_location()
                            .map(str::to_owned),
                    },
                )?;
                let seed = receipt(provider, request.operation_id, request.operation.kind())?;
                Ok::<_, PreparationFailure>((frozen, external, seed))
            },
            |error| error.0.clone(),
        )
        .await;
    let (frozen, external, seed) = match prepared {
        Ok(value) => value,
        Err(PreparationFailure(failure)) => {
            return Ok(Outcome::KnownUncommitted {
                failure,
                cleanup: ExternalMutationFinalization::Complete,
            });
        }
    };
    if let Err(error) = original.journal.record(external, seed, &frozen) {
        return Ok(known_uncommitted(invalid(error.to_string())));
    }
    let catalog = Arc::clone(provider.runtime().novarocks_catalog());
    let policy = catalog.evidence_policy();
    let pending = original
        .journal
        .publish_once_to_finalization(
            request.context.clone(),
            policy,
            move |descriptor, generation, context| async move {
                catalog
                    .prepare_frozen_publication(
                        frozen,
                        CatalogOperation::AlterProperties,
                        None,
                        descriptor,
                        generation,
                        context,
                    )
                    .await
            },
        )
        .await;
    let outcome = match pending {
        Ok(pending) => finish(original, receiving, pending).await?,
        Err(error) => original.call_error(error),
    };
    if matches!(outcome, Outcome::KnownCommitted { .. }) {
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&table.namespace, &table.table);
    }
    Ok(outcome)
}

async fn drop_table(
    original: &Original,
    receiving: &ConnectorCatalogMutationReceiving,
) -> Result<Outcome, ConnectorError> {
    use crate::catalog::catalog_request::{CatalogPublicationRequest, CatalogResponseDocument};
    let provider = &original.provider;
    let request = &original.request;
    let ConnectorCatalogMutationOperation::DropTable {
        table,
        policy,
        data_disposition,
    } = &request.operation
    else {
        unreachable!()
    };
    let seed = match receipt(provider, request.operation_id, request.operation.kind()) {
        Ok(seed) => seed,
        Err(error) => return Ok(known_uncommitted(error)),
    };
    let prepared = provider
        .runtime()
        .resources()
        .catalog_runtime()
        .observe_preparation_async(
            async {
                ensure_owner(provider, &table.instance_id)?;
                request.context.check_active()?;
                let namespace = normalize_identifier(&table.namespace).map_err(invalid)?;
                let name = normalize_identifier(&table.table).map_err(invalid)?;
                let target = CatalogTableName::new(namespace.clone(), name.clone());
                let canonical = target.canonical();
                let catalog = Arc::clone(provider.runtime().novarocks_catalog());
                let binding = provider
                    .runtime()
                    .resources()
                    .planning_binding()
                    .for_request(request.context.clone());
                let exists = catalog
                    .table_exists_for_read(target.clone(), binding)
                    .await?;
                request.context.check_active()?;
                if !exists {
                    if *policy == DropPolicy::NoOpIfMissing {
                        return Ok(None);
                    }
                    return Err(not_found("Iceberg table does not exist").into());
                }
                let binding = provider
                    .runtime()
                    .resources()
                    .planning_binding()
                    .for_request(request.context.clone());
                let loaded = catalog.load_table_for_read(target.clone(), binding).await;
                request.context.check_active()?;
                // Preserve the original conservative capture rule. A failed load may
                // leak objects; it never grants collection of an unidentified target.
                let captured = match loaded {
                    Ok(loaded) => {
                        let OriginalTable::Catalog(loaded) =
                            original.retain_table(OriginalTable::Catalog(loaded))
                        else {
                            unreachable!("drop preparation retains its original catalog table")
                        };
                        crate::catalog::CatalogDropTableReceipt {
                            table_uuid: Some(Arc::from(loaded.metadata().uuid().to_string())),
                            table_location: Some(Arc::from(loaded.metadata().location())),
                            metadata_location: loaded.metadata_location().map(Arc::from),
                            last_updated_ms: loaded.metadata().last_updated_ms(),
                        }
                    }
                    Err(_) => crate::catalog::CatalogDropTableReceipt {
                        table_uuid: None,
                        table_location: None,
                        metadata_location: None,
                        last_updated_ms: 0,
                    },
                };
                let external = evidence(
                    provider,
                    request.operation_id,
                    request.operation.kind(),
                    IcebergMutationEvidenceTarget::Table {
                        namespace,
                        table: name,
                        should_exist: false,
                        before_uuid: captured.table_uuid.as_ref().map(ToString::to_string),
                    },
                )?;
                let frozen = CatalogPublicationRequest::DropTable {
                    table: target,
                    no_op_if_missing: *policy == DropPolicy::NoOpIfMissing,
                    purge: *data_disposition == ConnectorDropTableDataDisposition::Purge,
                    capture: Some(captured.clone()),
                }
                .freeze(request.operation_id.to_bytes())?;
                Ok::<_, PreparationFailure>(Some((frozen, external, canonical, captured)))
            },
            |error| error.0.clone(),
        )
        .await;
    let (frozen, external, canonical, captured) = match prepared {
        Err(PreparationFailure(failure)) => {
            return Ok(Outcome::KnownUncommitted {
                failure,
                cleanup: ExternalMutationFinalization::Complete,
            });
        }
        Ok(None) => {
            receiving.record_needed_committed(
                ExternalMutationEffect::NoOp,
                &seed,
                &ExternalMutationFinalization::Complete,
            )?;
            return Ok(Outcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                receipt: seed,
                finalization: ExternalMutationFinalization::Complete,
                cleanup: ExternalMutationFinalization::Complete,
            });
        }
        Ok(Some(prepared)) => prepared,
    };
    let drop_scope = frozen.drop_scope();
    if let Err(error) = original.journal.prepare_catalog_with_receipts(
        external,
        PublicationReceipts::same(seed),
        frozen.targets(),
        frozen.request_bytes().to_vec(),
        frozen.attribution().clone(),
        request.context.clone(),
    ) {
        return Ok(known_uncommitted(invalid(error.to_string())));
    }
    let catalog = Arc::clone(provider.runtime().novarocks_catalog());
    let policy = catalog.evidence_policy();
    let read_binding = provider.runtime().resources().planning_binding().clone();
    let pending = original
        .journal
        .publish_once_to_finalization(
            request.context.clone(),
            policy,
            move |descriptor, generation, context| async move {
                catalog
                    .prepare_catalog_publication(
                        frozen,
                        descriptor,
                        generation,
                        context,
                        CatalogResponseDocument::default(),
                        read_binding,
                    )
                    .await
            },
        )
        .await;
    let outcome = match pending {
        Ok(pending) => finish(original, receiving, pending).await?,
        Err(error) => original.call_error(error),
    };
    if matches!(
        outcome,
        Outcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            ..
        }
    ) {
        provider
            .runtime()
            .control_state()
            .invalidate_table_cache(&table.namespace, &table.table);
        let witness = original.journal.current_entry().and_then(|entry| {
            drop_scope
                .as_ref()
                .and_then(|scope| original.journal.applied_drop_witness(entry, scope))
        });
        if *data_disposition == ConnectorDropTableDataDisposition::Purge
            && let Some(cleanup) = witness.as_ref().and_then(|witness| {
                crate::catalog_control::drop_cleanup::PostCommitCleanupRequest::from_journal_drop(
                    canonical, &captured, witness,
                )
            })
        {
            provider.runtime().drop_cleanup().enqueue(cleanup);
        }
    }
    Ok(outcome)
}
