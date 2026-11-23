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
use crate::catalog_control::add_files::{
    RegistrationPreparation, commit_error, procedure_error, registration_error,
};
use crate::commit::fast_append::CompactFastAppendPreparer;
use novarocks_failure_contract::FailureProjection;

mod registration_checks;
#[cfg(test)]
use crate::catalog::error::CatalogOutcome;
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
    ArtifactWriter, CommitProtocol, Dependency, FileChanges, FrozenRequest, OperationIntent,
    OperationIntentParts, RequestShape, TableTarget,
};
use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation, OperationLimits};
use crate::commit::publication_descriptor::PublicationDescriptor;
use crate::commit::staging::{PreparedChange, StagingBase};
use async_trait::async_trait;
use novarocks_spi::connector::{ProviderBindingEpoch, REGISTER_EXISTING_FILES_KIND};
use registration_checks::RegistrationChecks;

pub(super) fn format_error(error: ConnectorError) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, error.message())
        .with_source(error)
}

struct MutationPublisher {
    inner: Arc<CatalogPublisher>,
    payload: IcebergDataMutationPlanPayloadV2,
    runtime: Arc<IcebergMetadataContext>,
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
        validate_frozen_table(&table, &self.payload).map_err(format_error)?;
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
    let (payload, source) = planned
        .truncate_parts()
        .ok_or_else(|| format_error(invalid("TRUNCATE publication requires a truncate plan")))?;
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
    let main_truncate = planned.is_main_truncate();
    OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: TableIdent::new(
                NamespaceIdent::new(payload.namespace.clone()),
                payload.table.clone(),
            ),
            uuid: Some(source.uuid()),
        },
        target_ref: payload.target_ref.clone(),
        start: if main_truncate { None } else { start },
        changes: FileChanges {
            added: vec![],
            removed: vec![],
        },
        dependencies: if main_truncate {
            vec![]
        } else {
            vec![Dependency::RefUnchanged]
        },
        protocol: if main_truncate {
            CommitProtocol::Truncate
        } else {
            CommitProtocol::Legacy
        },
        shape: RequestShape::SnapshotProducing,
        summary: marker_summary(marker)?,
        token: operation.token(),
    })
}

fn freeze_registration_intent(
    table: &crate::iceberg::table::Table,
    operation: &IcebergCommitOperation,
    start: Option<StartSnapshot>,
    marker: &IcebergDataMutationMarkerV1,
) -> crate::iceberg::Result<OperationIntent> {
    let source = table.metadata();
    OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: table.identifier().clone(),
            uuid: Some(source.uuid()),
        },
        target_ref: "main".into(),
        start,
        changes: FileChanges {
            added: vec![],
            removed: vec![],
        },
        dependencies: vec![],
        protocol: crate::commit::model::CommitProtocol::Legacy,
        shape: RequestShape::SnapshotProducing,
        summary: marker_summary(marker)?,
        token: operation.token(),
    })
    .and_then(|intent| intent.with_compact_stage(1))
}

fn marker_summary(
    marker: &IcebergDataMutationMarkerV1,
) -> crate::iceberg::Result<BTreeMap<String, String>> {
    let marker_value =
        canonical_json(marker, "Iceberg data mutation marker").map_err(format_error)?;
    Ok(BTreeMap::from([(
        MARKER_PROPERTY.into(),
        String::from_utf8(marker_value.to_vec())
            .map_err(|_| format_error(internal("Canonical marker is not UTF-8")))?,
    )]))
}

fn refused_registration(
    error: ConnectorError,
) -> ExternalMutationOutcome<ConnectorDataMutationReceipt> {
    ExternalMutationOutcome::KnownUncommitted {
        failure: ConnectorMutationFailure::from_connector(error),
        cleanup: ExternalMutationFinalization::Complete,
    }
}

impl RegisteredIcebergDataMutationBackend {
    pub(super) fn execute_publication(
        &self,
        planned: &PlannedIcebergMutation,
        marker: &IcebergDataMutationMarkerV1,
        recovery: &MutationRecoveryTemplate,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        let (payload, source) = planned
            .truncate_parts()
            .ok_or_else(|| invalid("TRUNCATE execution requires a truncate plan"))?;
        let operation = match IcebergCommitOperation::new(
            OperationToken::from_mutation(recovery.plan.operation_id()),
            source.location(),
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
                    if !planned.is_main_truncate() {
                        let table =
                            self.reload_table(&payload.namespace, &payload.table, context)?;
                        validate_frozen_table(&table, payload)?;
                        anchor_committed_write(&self.runtime, &table)?;
                    }
                    let intent = freeze_intent(planned, &operation, marker)
                        .map_err(|error| corrupt(error.to_string()))?;
                    let policy = RetryPolicy::from_properties(source.properties())
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
        let direct = self.catalog_publisher(
            &journal,
            recovery,
            &intent,
            &operation,
            CatalogOperation::Truncate,
        );
        let main_truncate = planned.is_main_truncate();
        let (publisher, prefix): (Arc<dyn PublicationPreparer>, PreparedChange) = if main_truncate {
            (direct, PreparedChange::default())
        } else {
            (
                Arc::new(MutationPublisher {
                    inner: direct,
                    payload: payload.clone(),
                    runtime: Arc::clone(&self.runtime),
                }),
                PreparedChange {
                    updates: vec![],
                    requirements: vec![
                        crate::iceberg::TableRequirement::CurrentSchemaIdMatch {
                            current_schema_id: payload.schema_id,
                        },
                        crate::iceberg::TableRequirement::DefaultSpecIdMatch {
                            default_spec_id: payload.default_spec_id,
                        },
                    ],
                },
            )
        };
        let publisher = journal.observe_prepared(publisher);
        let running_operation = operation.clone();
        let report = operation.runtime().block_on(async move {
            let filtered =
                main_truncate.then(crate::commit::filter_delete::FilterDeletePreparer::always_true);
            let legacy = crate::commit::truncate::TruncatePreparer;
            let preparer: &dyn crate::commit::staging::Preparer = match &filtered {
                Some(filtered) => filtered,
                None => &legacy,
            };
            attempt::run(
                &running_operation,
                &intent,
                &prefix,
                &[preparer],
                &publisher,
                policy,
            )
            .await
        });
        let mut outcome = self.project_bridge(&operation, &journal, report)?;
        self.finish_committed(&journal, &recovery.finalization(), context, &mut outcome);
        Ok(project_external(outcome))
    }

    pub(super) fn publish_registration(
        &self,
        planned: &PlannedIcebergMutation,
        request: &ConnectorDataMutationExecuteRequest,
        descriptor: &ConnectorInstanceDescriptor,
        key: &ConnectorProviderBindingKey,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        let PlannedIcebergMutation::RegisterExistingFiles {
            payload,
            location,
            recursion,
        } = planned
        else {
            return Err(invalid("ADD FILES execution requires a registration plan"));
        };
        let context = &request.context;
        let catalog_runtime = self.runtime.resources().catalog_runtime();
        let initialized = catalog_runtime.observe_preparation(
            || {
                context.check_active()?;
                let byte_author = request.registration_byte_author()?;
                // E must reach the catalog even if planning used the same
                // request's frozen read-only observation of this table.
                let table = catalog_runtime
                    .block_on(
                        self.runtime
                            .observe_table_original_for_request_with_snapshots_async(
                                &payload.namespace,
                                &payload.table,
                                context,
                                crate::catalog::SnapshotLoading::All,
                            ),
                    )
                    .map_err(commit_error)??
                    .into_table();
                let source = table.metadata();
                if source.uuid().to_string() != payload.table_uuid {
                    return Err(commit_error("Target table was replaced"));
                }
                let start = source_snapshot(source, "main")
                    .map_err(|error| commit_error(error.message()))?;
                let facts = MutationRecoverySource::registration(payload, start);
                Ok((table, byte_author, facts, start))
            },
            |error: &ConnectorError| ConnectorMutationFailure::from_connector(error.clone()),
        );
        let (table, byte_author, facts, start) = match initialized {
            Ok(initialized) => initialized,
            Err(error) => return Ok(refused_registration(error)),
        };
        let mut recovery = MutationRecoveryTemplate::new(
            descriptor.clone(),
            key.clone(),
            request.plan.clone(),
            facts,
        );
        let marker = recovery.marker();
        let marker_result = find_marker(
            table.metadata(),
            "main",
            &marker.operation_id_hex,
            &marker.identity_digest_hex,
        );
        match marker_result {
            Ok(MarkerLookup::Matching { .. } | MarkerLookup::Conflicting) => {
                return Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: registration_unknown(
                        "An existing data mutation marker has no original local dispatch journal",
                    ),
                    evidence: recovery.evidence(RecoveryPublication::MarkerOnly {})?,
                });
            }
            Ok(MarkerLookup::Missing) => {}
            Err(error) => return Ok(refused_registration(commit_error(error.message()))),
        }
        let binding = self
            .runtime
            .resources()
            .planning_binding()
            .for_request(context.clone());
        let catalog = self.runtime.novarocks_catalog();
        let preparation = RegistrationPreparation {
            table: &table,
            location,
            recursion: *recursion,
            catalog: catalog.as_ref(),
            binding: &binding,
            context,
            byte_author,
        };
        let prepared = catalog_runtime.block_on(
            catalog_runtime.observe_preparation_async(preparation.collect(), |error| {
                ConnectorMutationFailure::from_connector(error.clone())
            }),
        );
        let (files, mapping) = match prepared {
            Ok(Ok(prepared)) => prepared,
            Ok(Err(error)) => return Ok(refused_registration(error)),
            Err(bridge) => return Ok(refused_registration(commit_error(bridge))),
        };
        let counts = files.counts().data;
        let file_count = match u32::try_from(counts.files) {
            Ok(count) => count,
            Err(_) => {
                return Ok(refused_registration(commit_error(
                    "Registered file count exceeds the receipt carrier",
                )));
            }
        };
        recovery.summary =
            ConnectorDataMutationPlanSummary::new(file_count, counts.rows, counts.bytes);
        let source = table.metadata();
        let result = (|| {
            anchor_committed_write(&self.runtime, &table)?;
            let operation = IcebergCommitOperation::new(
                OperationToken::from_mutation(request.plan.operation_id()),
                source.location(),
                self.runtime.resources().planning_binding().clone(),
                context.clone(),
                catalog_runtime.clone(),
                OperationLimits::default(),
            )
            .map_err(registration_error)?;
            let policy =
                RetryPolicy::from_properties(source.properties()).map_err(registration_error)?;
            let intent = freeze_registration_intent(&table, &operation, start, &recovery.marker())
                .map_err(registration_error)?;
            let paths = operation
                .install_registered_new_files(1, files)
                .map_err(registration_error)?;
            Ok::<_, ConnectorError>((operation, policy, intent, paths))
        })();
        let (operation, policy, intent, paths) = match result {
            Ok(prepared) => prepared,
            Err(error) => return Ok(refused_registration(error)),
        };
        let journal = PublicationJournal::for_runtime(operation.token(), catalog_runtime);
        let direct = self.catalog_publisher(
            &journal,
            &recovery,
            &intent,
            &operation,
            CatalogOperation::RegisterFiles,
        );
        let publisher = journal.observe_prepared(direct);
        let report = catalog_runtime.block_on(async {
            let checks = RegistrationChecks {
                table_uuid: source.uuid(),
                partition_spec_id: source.default_partition_spec_id(),
                paths: &paths,
                mapping: mapping.as_ref(),
                last_verified_main: Mutex::new(start.map(|s| s.snapshot_id)),
            };
            let append = CompactFastAppendPreparer(operation.clone());
            attempt::run(
                &operation,
                &intent,
                &PreparedChange::default(),
                &[&checks, &append],
                &publisher,
                policy,
            )
            .await
        });
        drop(paths);
        drop(mapping);
        operation.release_registered_paths();
        let mut outcome = self.project_bridge(&operation, &journal, report)?;
        self.finish_committed(&journal, &recovery.finalization(), context, &mut outcome);
        Ok(project_for_operation(
            outcome,
            request.plan.operation_kind(),
        ))
    }

    fn catalog_publisher(
        &self,
        journal: &PublicationJournal<DataPublication>,
        recovery: &MutationRecoveryTemplate,
        intent: &OperationIntent,
        operation: &IcebergCommitOperation,
        catalog_operation: CatalogOperation,
    ) -> Arc<CatalogPublisher> {
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
                    .ref_snapshot_after(&template.source.target_ref)
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
        Arc::new(CatalogPublisher {
            expected_table_uuid: (catalog_operation == CatalogOperation::RegisterFiles).then(
                || {
                    intent
                        .target()
                        .uuid
                        .expect("registration has an admitted target")
                },
            ),
            catalog_operation,
            catalog: Arc::clone(self.runtime.novarocks_catalog()),
            ident: intent.target().ident.clone(),
            target_ref: intent.target_ref().into(),
            operation: operation.token(),
            recovery_preflight: preflight,
        })
    }

    fn finish_committed(
        &self,
        journal: &PublicationJournal<DataPublication>,
        facts: &DataFinalizationFacts,
        context: &ConnectorRequestContext,
        outcome: &mut ExternalMutationOutcome<DataPublication>,
    ) {
        if let ExternalMutationOutcome::KnownCommitted { finalization, .. } = outcome {
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
                    self.finalize_publication_facts(facts, &context)
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
        Ok(())
    }
}

pub(super) fn project_for_operation(
    outcome: ExternalMutationOutcome<DataPublication>,
    operation_kind: &str,
) -> ExternalMutationOutcome<ConnectorDataMutationReceipt> {
    let mut outcome = project_external(outcome);
    if operation_kind == REGISTER_EXISTING_FILES_KIND {
        match &mut outcome {
            ExternalMutationOutcome::KnownUncommitted { failure, .. } => {
                *failure = registration_refusal(failure);
            }
            ExternalMutationOutcome::CommitUnknown { failure, .. } => {
                *failure = registration_unknown(failure_detail(failure));
            }
            ExternalMutationOutcome::KnownCommitted { .. } => {}
        }
    }
    outcome
}

fn failure_detail(failure: &ConnectorMutationFailure) -> &str {
    failure
        .connector_source()
        .map_or_else(|| failure.message(), ConnectorError::message)
}

fn registration_refusal(failure: &ConnectorMutationFailure) -> ConnectorMutationFailure {
    match failure.kind() {
        ConnectorMutationFailureKind::Cancelled
        | ConnectorMutationFailureKind::DeadlineExceeded
        | ConnectorMutationFailureKind::ResourceExhausted
        | ConnectorMutationFailureKind::Interrupted
        | ConnectorMutationFailureKind::Unsupported => return failure.clone(),
        _ => {}
    }
    let reason = failure.failure_fact().reason();
    if [
        connector_reason::ADD_FILES_DISABLED,
        connector_reason::PROCEDURE_ARGUMENT_INVALID,
        connector_reason::LOCATION_NOT_FOUND,
        connector_reason::SOURCE_TYPE_MISMATCH,
        connector_reason::NOT_NULL_VIOLATION,
        connector_reason::ICEBERG_COMMIT_ERROR,
        connector_reason::COMMIT_TRANSACTION_CONFLICT,
    ]
    .contains(&reason)
    {
        return failure.clone();
    }
    let detail = failure_detail(failure);
    let error = if failure.kind() == ConnectorMutationFailureKind::Conflict {
        procedure_error(
            ConnectorErrorKind::Unavailable,
            FailureClass::Dependency,
            connector_reason::COMMIT_TRANSACTION_CONFLICT,
            format!("Failed to add files: {detail}"),
        )
    } else {
        commit_error(detail)
    };
    ConnectorMutationFailure::from_connector(error)
}

pub(super) fn registration_unknown(detail: &str) -> ConnectorMutationFailure {
    ConnectorMutationFailure::from_connector(procedure_error(
        ConnectorErrorKind::Unavailable,
        FailureClass::Dependency,
        connector_reason::ICEBERG_COMMIT_STATE_UNKNOWN,
        format!("Failed to add files: {detail}"),
    ))
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
