// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Data mutation owns registered-file facts, never registered-file deletion.

use super::*;
#[cfg(test)]
use crate::catalog::error::{CatalogCommitEvidence, CatalogOutcome};
use crate::catalog::publication::CatalogEvidencePolicy;
use crate::catalog::publication_dispatch::{PreparationResult, PreparedPublication};
#[cfg(test)]
use crate::catalog::transaction::CommitProof;
#[cfg(test)]
use crate::commit::attempt::Publisher;
use crate::commit::attempt::observation::PublicationJournal;
use crate::commit::attempt::prepared::{CatalogPublisher, PublicationPreparer};
use crate::commit::attempt::{self, RetryPolicy};
use crate::commit::model::{
    AddedContent, ArtifactWriter, Dependency, FileChanges, FrozenRequest, IsolationLevel,
    OperationIntent, OperationIntentParts, RequestShape, TableTarget,
};
use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation, OperationLimits};
use crate::commit::publication_descriptor::PublicationDescriptor;
use crate::commit::staging::{PreparedChange, Preparer, StagingBase};
use async_trait::async_trait;
use novarocks_spi::connector::ProviderBindingEpoch;

pub(super) fn format_error(error: ConnectorError) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, error.message())
        .with_source(error)
}

struct MutationPublisher {
    inner: Arc<CatalogPublisher>,
    planned: PlannedIcebergMutation,
    runtime: Arc<IcebergMetadataContext>,
    context: ConnectorRequestContext,
}

#[async_trait]
impl PublicationPreparer for MutationPublisher {
    async fn load_target(
        &self,
        attempt: &IcebergCommitAttempt,
    ) -> crate::iceberg::Result<StagingBase> {
        let base = self.inner.load_target(attempt).await?;
        let StagingBase::Existing {
            metadata,
            metadata_location,
        } = &base
        else {
            return Err(format_error(invalid(
                "Data mutation requires an existing target",
            )));
        };
        let table = crate::iceberg::table::Table::builder()
            .runtime(
                self.runtime
                    .resources()
                    .catalog_runtime()
                    .iceberg_runtime()
                    .clone(),
            )
            .identifier(self.inner.ident.clone())
            .metadata(metadata.clone())
            .metadata_location(metadata_location.clone())
            .file_io(attempt.file_io().clone())
            .build()?;
        match &self.planned {
            PlannedIcebergMutation::Truncate { payload, .. } => {
                validate_frozen_table(&table, payload).map_err(format_error)?
            }
            PlannedIcebergMutation::RegisterExistingFiles {
                payload, manifest, ..
            } => {
                validate_add_files_target_shape(&table, payload).map_err(format_error)?;
                ConnectorOperationControl::check_active(&self.context).map_err(format_error)?;
                let source = payload
                    .source_location
                    .as_deref()
                    .ok_or_else(|| format_error(corrupt("ADD FILES source location is absent")))?;
                // This helper bridges through the injected catalog runtime. The
                // same authoritative M is validated and returned to staging.
                let revalidated = revalidate_manifest_for_table(
                    &table,
                    source,
                    &self
                        .runtime
                        .resources()
                        .planning_binding()
                        .for_request(self.context.clone()),
                    manifest,
                    self.runtime.resources().catalog_runtime(),
                    self.runtime.novarocks_catalog().listing_admission(),
                );
                ConnectorOperationControl::check_active(&self.context).map_err(format_error)?;
                revalidated.map_err(|error| format_error(map_provider_error(error)))?;
            }
        }
        attempt.check_active()?;
        Ok(base)
    }

    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> crate::iceberg::Result<()> {
        self.inner.preflight_recovery(request, operation)
    }

    fn evidence_policy(&self) -> CatalogEvidencePolicy {
        self.inner.evidence_policy()
    }
    async fn prepare_dispatch(
        self: Arc<Self>,
        request: FrozenRequest,
        descriptor: PublicationDescriptor,
        generation: ProviderBindingEpoch,
        preparation: ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication> {
        Arc::clone(&self.inner)
            .prepare_dispatch(request, descriptor, generation, preparation)
            .await
    }
}

fn freeze_intent(
    planned: &PlannedIcebergMutation,
    operation: &IcebergCommitOperation,
    marker: &IcebergDataMutationMarkerV1,
) -> crate::iceberg::Result<OperationIntent> {
    let payload = planned.payload();
    let source = planned.source_metadata();
    let start = source_snapshot(source, &payload.target_ref).map_err(format_error)?;
    if source.uuid().to_string() != payload.table_uuid
        || start.map(|s| s.snapshot_id) != payload.base_snapshot_id
        || start.map(|s| s.sequence_number) != payload.base_sequence_number
        || source.current_schema_id() != payload.schema_id
        || source.default_partition_spec_id() != payload.default_spec_id
    {
        return Err(format_error(corrupt(
            "Data mutation lost its exact admitted source metadata",
        )));
    }
    let (added, dependencies) = match planned {
        PlannedIcebergMutation::RegisterExistingFiles { manifest, .. } => (
            manifest
                .to_data_files()
                .map_err(|error| format_error(map_provider_error(error)))?
                .into_iter()
                .map(|file| AddedContent::new_logical_data(file, payload.default_spec_id))
                .collect::<crate::iceberg::Result<Vec<_>>>()?,
            vec![Dependency::RegisteredFilesNotLive(
                manifest
                    .records
                    .iter()
                    .map(|record| record.location.clone())
                    .collect(),
            )],
        ),
        PlannedIcebergMutation::Truncate { .. } => (vec![], vec![Dependency::RefUnchanged]),
    };
    let marker_value =
        canonical_json(marker, "Iceberg data mutation marker").map_err(format_error)?;
    let summary = BTreeMap::from([(
        MARKER_PROPERTY.into(),
        String::from_utf8(marker_value.to_vec())
            .map_err(|_| format_error(internal("Canonical marker is not UTF-8")))?,
    )]);
    OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: TableIdent::new(
                NamespaceIdent::new(payload.namespace.clone()),
                payload.table.clone(),
            ),
            uuid: Some(source.uuid()),
        },
        target_ref: payload.target_ref.clone(),
        start,
        changes: FileChanges {
            added,
            removed: vec![],
        },
        dependencies,
        isolation: IsolationLevel::Serializable,
        shape: RequestShape::SnapshotProducing,
        summary,
        token: operation.token(),
    })
}

impl RegisteredIcebergDataMutationBackend {
    pub(super) fn execute_publication(
        &self,
        planned: &PlannedIcebergMutation,
        marker: &IcebergDataMutationMarkerV1,
        recovery: &MutationRecoveryTemplate,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        let operation = match IcebergCommitOperation::new(
            OperationToken::from_mutation(recovery.plan.operation_id()),
            planned.source_metadata().location(),
            self.runtime.resources().planning_binding().clone(),
            context.clone(),
            self.runtime.resources().catalog_runtime().clone(),
            OperationLimits::default(),
        ) {
            Ok(operation) => operation,
            Err(error) => {
                return Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: attempt::before_dispatch_failure(&error),
                    cleanup: ExternalMutationFinalization::Complete,
                });
            }
        };
        let journal = PublicationJournal::for_runtime(
            operation.token(),
            self.runtime.resources().catalog_runtime(),
        );
        // External registered parquet is deliberately never adopted into this ledger.
        let setup = self
            .runtime
            .resources()
            .catalog_runtime()
            .observe_preparation(
                || {
                    let table = self.reload_table(
                        &planned.payload().namespace,
                        &planned.payload().table,
                        context,
                    )?;
                    match planned {
                        PlannedIcebergMutation::Truncate { payload, .. } => {
                            validate_frozen_table(&table, payload)?
                        }
                        PlannedIcebergMutation::RegisterExistingFiles { payload, .. } => {
                            validate_add_files_target_shape(&table, payload)?
                        }
                    }
                    anchor_committed_write(&self.runtime, &table)?;
                    let intent = freeze_intent(planned, &operation, marker)
                        .map_err(|error| corrupt(error.to_string()))?;
                    let policy =
                        RetryPolicy::from_properties(planned.source_metadata().properties())
                            .map_err(|error| invalid(error.to_string()))?;
                    Ok::<_, ConnectorError>((intent, policy))
                },
                |error| attempt::before_dispatch_failure(&format_error(error.clone())),
            );
        let (intent, policy) = match setup {
            Ok(setup) => setup,
            Err(error) => {
                let rejected_operation = operation.clone();
                let failure = attempt::before_dispatch_failure(&format_error(error));
                let result = operation
                    .runtime()
                    .block_on(async move { attempt::rejected(&rejected_operation, failure).await });
                return self
                    .project_bridge(&operation, &journal, result)
                    .map(project_external);
            }
        };
        let preflight_journal = journal.clone();
        let template = recovery.clone();
        let preflight = Arc::new(
            move |request: &FrozenRequest, operation: &IcebergCommitOperation| {
                let facts = FrozenPublicationFacts::from_request(request, operation)?;
                let evidence = template
                    .evidence(RecoveryPublication::Dispatched {
                        facts: Box::new(facts),
                    })
                    .map_err(format_error)?;
                let snapshot = request
                    .ref_snapshot_after(&template.payload.target_ref)
                    .ok_or_else(|| {
                        format_error(corrupt(
                            "Snapshot-producing mutation has no published snapshot",
                        ))
                    })?;
                let receipt = template
                    .publication_receipt(snapshot)
                    .map_err(format_error)?;
                preflight_journal.record(evidence, receipt, request)
            },
        );
        let direct = Arc::new(CatalogPublisher {
            expected_table_uuid: None,
            catalog_operation: match planned {
                PlannedIcebergMutation::Truncate { .. } => {
                    crate::catalog::admission::CatalogOperation::Truncate
                }
                PlannedIcebergMutation::RegisterExistingFiles { .. } => {
                    crate::catalog::admission::CatalogOperation::RegisterFiles
                }
            },
            catalog: Arc::clone(self.runtime.novarocks_catalog()),
            ident: intent.target().ident.clone(),
            target_ref: intent.target_ref().into(),
            operation: operation.token(),
            recovery_preflight: preflight,
        });
        let publisher = journal.observe_prepared(Arc::new(MutationPublisher {
            inner: direct,
            planned: planned.clone(),
            runtime: Arc::clone(&self.runtime),
            context: context.clone(),
        }));
        let prefix = PreparedChange {
            updates: vec![],
            requirements: vec![
                crate::iceberg::TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: planned.payload().schema_id,
                },
                crate::iceberg::TableRequirement::DefaultSpecIdMatch {
                    default_spec_id: planned.payload().default_spec_id,
                },
            ],
        };
        let preparer: Box<dyn Preparer> = match planned {
            PlannedIcebergMutation::RegisterExistingFiles { .. } => {
                Box::new(crate::commit::fast_append::FastAppendPreparer)
            }
            PlannedIcebergMutation::Truncate { .. } => {
                Box::new(crate::commit::truncate::TruncatePreparer)
            }
        };
        let running_operation = operation.clone();
        let report = operation.runtime().block_on(async move {
            attempt::run(
                &running_operation,
                &intent,
                &prefix,
                &[preparer.as_ref()],
                &publisher,
                policy,
            )
            .await
        });
        let mut outcome = self.project_bridge(&operation, &journal, report)?;
        if let ExternalMutationOutcome::KnownCommitted { finalization, .. } = &mut outcome {
            let result = journal
                .finalization_context(
                    journal
                        .current_entry()
                        .expect("committed data mutation has a publication entry"),
                    self.runtime.novarocks_catalog().evidence_policy(),
                )
                .and_then(|grant| grant.for_io(context))
                .and_then(|context| {
                    let _finalizing = journal
                        .current_entry()
                        .and_then(|entry| journal.finalization_activity(entry));
                    self.finalize_publication(planned, &context)
                });
            if let Err(error) = result {
                if let Some(entry) = journal.current_entry() {
                    journal.record_entry_finalization(
                        entry,
                        ExternalMutationFinalization::Failed(failure(
                            ConnectorMutationFailureKind::Internal,
                            error.to_string(),
                        )),
                    );
                }
                let message = match finalization {
                    ExternalMutationFinalization::Complete => error.to_string(),
                    ExternalMutationFinalization::Failed(existing) => {
                        format!("{}; {}", existing.message(), error)
                    }
                };
                *finalization = ExternalMutationFinalization::Failed(failure(
                    ConnectorMutationFailureKind::Internal,
                    message,
                ));
            }
        }
        Ok(project_external(outcome))
    }

    fn project_bridge(
        &self,
        operation: &IcebergCommitOperation,
        journal: &PublicationJournal<DataPublication>,
        result: Result<attempt::Report, String>,
    ) -> Result<ExternalMutationOutcome<DataPublication>, ConnectorError> {
        match result {
            Ok(report) => Ok(journal.project(report)),
            Err(error) => {
                let recovery_operation = operation.clone();
                let recovery_journal = journal.clone();
                let recovered = operation.runtime().block_on(async move {
                    recovery_journal
                        .recover_bridge(&recovery_operation, error)
                        .await
                });
                Ok(project_recovery_bridge(operation, journal, recovered))
            }
        }
    }

    fn finalize_publication(
        &self,
        planned: &PlannedIcebergMutation,
        context: &ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        let payload = planned.payload();
        self.finalize_publication_facts(
            &DataFinalizationFacts {
                namespace: payload.namespace.clone(),
                table: payload.table.clone(),
                table_uuid: payload.table_uuid.clone(),
                name_mapping_digest: matches!(
                    planned,
                    PlannedIcebergMutation::RegisterExistingFiles { .. }
                )
                .then(|| payload.name_mapping_digest_hex.clone()),
            },
            context,
        )
    }

    pub(super) fn finalize_publication_facts(
        &self,
        facts: &DataFinalizationFacts,
        context: &ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        context.check_active()?;
        self.runtime
            .control_state()
            .invalidate_table_cache(&facts.namespace, &facts.table);
        let loaded = self
            .runtime
            .load_table_for_request(&facts.namespace, &facts.table, context)
            .map_err(map_provider_error)?;
        context.check_active()?;
        if loaded.table.metadata().uuid().to_string() != facts.table_uuid {
            return Err(corrupt(
                "Committed data mutation target changed identity before finalization",
            ));
        }
        if let Some(expected) = &facts.name_mapping_digest {
            let mapping = loaded
                .table
                .metadata()
                .properties()
                .get(crate::iceberg::spec::TableProperties::PROPERTY_DEFAULT_NAME_MAPPING)
                .map(|value| crate::schema_mapping::canonical_name_mapping(value))
                .transpose()
                .map_err(map_provider_error)?;
            let digest = mapping
                .as_deref()
                .map(|value| hex_encode(Sha256::digest(value.as_bytes())));
            if &digest != expected {
                return Err(corrupt(
                    "schema.name-mapping.default changed after ADD FILES commit",
                ));
            }
        }
        Ok(())
    }
}

pub(super) fn project_external(
    outcome: ExternalMutationOutcome<DataPublication>,
) -> ExternalMutationOutcome<ConnectorDataMutationReceipt> {
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            cleanup,
            effect,
            receipt,
            finalization,
        } => ExternalMutationOutcome::KnownCommitted {
            cleanup,
            effect,
            receipt: receipt.receipt,
            finalization,
        },
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
            ExternalMutationOutcome::KnownUncommitted { failure, cleanup }
        }
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
            ExternalMutationOutcome::CommitUnknown { failure, evidence }
        }
    }
}

fn project_recovery_bridge(
    operation: &IcebergCommitOperation,
    journal: &PublicationJournal<DataPublication>,
    result: Result<ExternalMutationOutcome<DataPublication>, String>,
) -> ExternalMutationOutcome<DataPublication> {
    match result {
        Ok(outcome) => outcome,
        Err(error) => journal.bridge_failure(
            operation,
            format!("Iceberg mutation recovery bridge: {error}"),
        ),
    }
}

#[cfg(test)]
mod tests;
