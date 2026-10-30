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

//! Canonical create staging and one-dispatch publication observation.

use super::*;
use crate::catalog::publication::CatalogEvidencePolicy;
use crate::catalog::publication_dispatch::{PreparationResult, PreparedPublication};
use crate::commit::attempt::observation::PublicationJournal;
use crate::commit::attempt::prepared::PublicationPreparer;
use crate::commit::attempt::{self, RetryPolicy};
use crate::commit::model::{
    AddedContent, ArtifactWriter, FileChanges, FrozenRequest, IsolationLevel, ObjectIdentity,
    OperationIntent, OperationIntentParts, OperationToken, RequestShape, StagedCreateIdentity,
    TableTarget,
};
use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation, OperationLimits};
use crate::commit::publication_descriptor::PublicationDescriptor;
use crate::commit::recovery::FrozenPublicationFacts;
use crate::commit::staging::{PreparedChange, Preparer, StagingBase};
use crate::iceberg::{Error, ErrorKind};
use async_trait::async_trait;
use novarocks_spi::connector::ExternalMutationOutcome;
use novarocks_spi::connector::{ConnectorOperationControl, ProviderBindingEpoch};

fn iceberg_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

/// The same live ledger serves publication and every later bounded cleanup.
pub(super) fn ensure_operation(
    adapter: &IcebergStagedCreateAdapter,
    prepared: &mut PreparedOperation,
    operation_id: ConnectorStagedCreateOperationId,
    context: &ConnectorRequestContext,
) -> Result<IcebergCommitOperation, ConnectorError> {
    if let Some(operation) = &prepared.publication {
        return Ok(operation.clone());
    }
    let operation = IcebergCommitOperation::new(
        OperationToken::from_mutation(operation_id),
        prepared.staged.table.metadata().location(),
        adapter.runtime.resources().planning_binding().clone(),
        context.clone(),
        adapter.runtime.resources().catalog_runtime().clone(),
        OperationLimits::default(),
    )
    .map_err(|error| internal(error.to_string()))?;
    // Store ownership before registration: a refused registration must not
    // strand objects already adopted from the proven sealed write.
    prepared.publication = Some(operation.clone());
    if let Some(path) = &prepared.provenance_path {
        operation
            .adopt_operation_reference(
                ObjectIdentity::new(path.clone()).map_err(|error| corrupt(error.to_string()))?,
            )
            .map_err(|error| internal(error.to_string()))?;
    }
    if let Some(write) = &prepared.write {
        let objects: std::collections::BTreeSet<_> =
            write.files.iter().map(|file| file.path.clone()).collect();
        for path in objects {
            operation
                .adopt_session_data(
                    ObjectIdentity::new(path).map_err(|error| corrupt(error.to_string()))?,
                )
                .map_err(|error| internal(error.to_string()))?;
        }
    }
    Ok(operation)
}

pub(super) fn create_intent(
    prepared: &PreparedOperation,
    token: OperationToken,
) -> crate::iceberg::Result<OperationIntent> {
    let files = prepared
        .write
        .as_ref()
        .ok_or_else(|| iceberg_error("Create publication requires a sealed write"))?;
    let added = files
        .files
        .iter()
        .map(|file| {
            AddedContent::new_logical_data(
                crate::commit::frozen_data_file_from_written(file)?,
                file.partition_spec_id,
            )
        })
        .collect::<crate::iceberg::Result<Vec<_>>>()?;
    OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: prepared.staged.table.identifier().clone(),
            uuid: None,
        },
        target_ref: "main".into(),
        start: None,
        changes: FileChanges {
            added,
            removed: vec![],
        },
        dependencies: vec![],
        isolation: IsolationLevel::Serializable,
        shape: RequestShape::Create,
        summary: BTreeMap::new(),
        token,
    })
}

pub(super) fn create_prefix(prepared: &PreparedOperation) -> PreparedChange {
    let mut updates = prepared.staged.publication_updates.clone();
    if let Some(properties) = &prepared.document_properties {
        updates.push(TableUpdate::SetProperties {
            updates: properties.clone(),
        });
    }
    PreparedChange {
        updates,
        requirements: vec![],
    }
}

pub(super) struct CreatePublisher {
    pub(super) adapter: IcebergStagedCreateAdapter,
    pub(super) prepared: Arc<PreparedOperation>,
    pub(super) operation_id: ConnectorStagedCreateOperationId,
    pub(super) journal: PublicationJournal<ConnectorStagedCreateReceipt>,
}

#[async_trait]
impl PublicationPreparer for CreatePublisher {
    async fn load_target(
        &self,
        attempt: &IcebergCommitAttempt,
    ) -> crate::iceberg::Result<StagingBase> {
        attempt.check_active()?;
        Ok(StagingBase::Create {
            staged: StagedCreateIdentity::new(
                OperationToken::from_mutation(self.operation_id),
                Arc::new(self.prepared.staged.table.metadata().clone()),
            ),
            initialization_updates: self.prepared.staged.initialization_updates.clone(),
        })
    }

    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> crate::iceberg::Result<()> {
        let facts = FrozenPublicationFacts::from_request(request, operation)?;
        facts.validate_create_target(
            operation.token(),
            request.identifier(),
            self.prepared.staged.table.metadata().uuid(),
            self.prepared.staged.table.metadata().location(),
            "main",
            request.ref_snapshot_after("main"),
        )?;
        let ident = request.identifier();
        let expected_snapshot_id = request.ref_snapshot_after("main");
        let payload = serde_json::to_vec(&PublishEvidenceV1 {
            version: EVIDENCE_VERSION,
            operation_marker: publication_marker(self.prepared.publication_id),
            table_uuid: self.prepared.staged.table.metadata().uuid().to_string(),
            expected_snapshot_id,
            handle_digest: self.prepared.handle_digest,
            namespace: ident.namespace.to_url_string(),
            table: ident.name.clone(),
            facts,
        })
        .map_err(|error| {
            iceberg_error(format!(
                "Encode complete staged-create recovery envelope: {error}"
            ))
        })?;
        let evidence = self
            .adapter
            .evidence(
                self.operation_id,
                "staged-create-publish",
                Bytes::from(payload),
            )
            .map_err(|error| iceberg_error(error.to_string()))?;
        // UUID and snapshot are already frozen before dispatch, so receipt
        // construction cannot fail after a committed owner result.
        let receipt = publication_receipt(
            &self.adapter,
            self.operation_id,
            &self.prepared.staged.table,
            expected_snapshot_id,
        )
        .map_err(|error| iceberg_error(error.to_string()))?;
        let no_op = self
            .adapter
            .receipt(
                self.operation_id,
                ConnectorStagedCreateReceiptPhase::Published,
                ExternalMutationEffect::NoOp,
                Bytes::new(),
            )
            .map_err(|error| iceberg_error(error.to_string()))?;
        self.journal
            .record_for_effects(evidence, receipt, no_op, request)
    }

    fn evidence_policy(&self) -> CatalogEvidencePolicy {
        self.adapter.runtime.novarocks_catalog().evidence_policy()
    }
    async fn prepare_dispatch(
        self: Arc<Self>,
        request: FrozenRequest,
        descriptor: PublicationDescriptor,
        generation: ProviderBindingEpoch,
        preparation: ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication> {
        preparation.check_active()?;
        let expectation =
            publication_expectation(&self.prepared, request.ref_snapshot_after("main"));
        self.adapter
            .runtime
            .novarocks_catalog()
            .prepare_frozen_publication(
                request,
                crate::catalog::admission::CatalogOperation::CreateTable(
                    crate::catalog::CatalogCreateIntent::CreateTableAsSelect,
                ),
                Some(expectation),
                descriptor,
                generation,
                preparation,
            )
            .await
    }
}

pub(super) fn publish(
    adapter: &IcebergStagedCreateAdapter,
    prepared: &mut PreparedOperation,
    operation_id: ConnectorStagedCreateOperationId,
    context: &ConnectorRequestContext,
) -> Result<ExternalMutationOutcome<ConnectorStagedCreateReceipt>, ConnectorError> {
    if operation_id.to_bytes() != prepared.operation_id().to_bytes() {
        return Err(invalid(
            "Create publication must preserve the original staged operation token",
        ));
    }
    let operation = ensure_operation(adapter, prepared, operation_id, context)?;
    let intent =
        create_intent(prepared, operation.token()).map_err(|error| corrupt(error.to_string()))?;
    let prefix = create_prefix(prepared);
    let policy = RetryPolicy::from_properties(prepared.staged.table.metadata().properties())
        .map_err(|error| invalid(error.to_string()))?;
    let journal = prepared
        .stage_publication
        .journal
        .next_frontier()
        .map_err(|error| internal(error.to_string()))?;
    prepared.final_publication = Some(journal.clone());
    let publisher = journal.observe_prepared(Arc::new(CreatePublisher {
        adapter: adapter.clone(),
        prepared: Arc::new(prepared.clone()),
        operation_id,
        journal: journal.clone(),
    }));
    let run_operation = operation.clone();
    let result = operation.runtime().block_on(async move {
        let append = crate::commit::FastAppendPreparer;
        // Empty CTAS and managed CREATE do not allocate a snapshot.
        let preparers: Vec<&dyn Preparer> = if intent.changes().added.is_empty() {
            vec![]
        } else {
            vec![&append]
        };
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
    Ok(match result {
        Ok(report) => journal.project(report),
        Err(error) => match operation.runtime().block_on({
            let operation = operation.clone();
            let journal = journal.clone();
            async move {
                journal
                    .recover_bridge(
                        &operation,
                        format!("Staged-create publication runtime: {error}"),
                    )
                    .await
            }
        }) {
            Ok(outcome) => outcome,
            Err(error) => journal.bridge_failure(
                &operation,
                format!("Staged-create recovery runtime: {error}"),
            ),
        },
    })
}

pub(super) async fn publish_async(
    adapter: &IcebergStagedCreateAdapter,
    prepared: &mut PreparedOperation,
    operation_id: ConnectorStagedCreateOperationId,
    context: &ConnectorRequestContext,
) -> Result<ExternalMutationOutcome<ConnectorStagedCreateReceipt>, ConnectorError> {
    if operation_id.to_bytes() != prepared.operation_id().to_bytes() {
        return Err(invalid(
            "Create publication must preserve the original staged operation token",
        ));
    }
    let operation = ensure_operation(adapter, prepared, operation_id, context)?;
    let intent =
        create_intent(prepared, operation.token()).map_err(|error| corrupt(error.to_string()))?;
    let prefix = create_prefix(prepared);
    let policy = RetryPolicy::from_properties(prepared.staged.table.metadata().properties())
        .map_err(|error| invalid(error.to_string()))?;
    let journal = prepared
        .stage_publication
        .journal
        .next_frontier()
        .map_err(|error| internal(error.to_string()))?;
    prepared.final_publication = Some(journal.clone());
    let publisher = journal.observe_prepared(Arc::new(CreatePublisher {
        adapter: adapter.clone(),
        prepared: Arc::new(prepared.clone()),
        operation_id,
        journal: journal.clone(),
    }));
    let append = crate::commit::FastAppendPreparer;
    let preparers: Vec<&dyn Preparer> = if intent.changes().added.is_empty() {
        vec![]
    } else {
        vec![&append]
    };
    let report = attempt::run(&operation, &intent, &prefix, &preparers, &publisher, policy).await;
    Ok(journal.project(report))
}

/// Append a fresh exact positive observation to the final request's entry.
/// The stage frontier remains unchanged and cannot receive this proof.
pub(super) fn record_observed_publication(
    prepared: &PreparedOperation,
    table: &crate::iceberg::table::Table,
) -> Result<ExternalMutationOutcome<ConnectorStagedCreateReceipt>, ConnectorError> {
    use crate::commit::publication_descriptor::{AttributionPlan, PositiveAttributionObservation};
    let journal = prepared
        .final_publication
        .as_ref()
        .ok_or_else(|| invalid("Staged publication has no retained final frontier"))?;
    let entry = journal
        .current_entry()
        .ok_or_else(|| invalid("Staged publication has no final request entry"))?;
    if let committed @ ExternalMutationOutcome::KnownCommitted { .. } =
        journal.catalog_entry_outcome(entry)
    {
        return Ok(committed);
    }
    let descriptor = journal
        .entry_descriptor(entry)
        .ok_or_else(|| invalid("Staged publication final request is unavailable"))?;
    let observation = match &descriptor.attribution {
        AttributionPlan::Property { key, .. } => {
            let value =
                table.metadata().properties().get(key).ok_or_else(|| {
                    invalid("Observed staged publication has no operation property")
                })?;
            PositiveAttributionObservation::PropertyMarker {
                table_uuid: table.metadata().uuid(),
                key: key.clone(),
                value: value.clone(),
            }
        }
        AttributionPlan::Snapshot {
            snapshot_id,
            marker_key,
            ..
        } => {
            let snapshot = table
                .metadata()
                .snapshot_by_id(*snapshot_id)
                .ok_or_else(|| invalid("Observed staged publication has no exact snapshot"))?;
            let value = snapshot
                .summary()
                .additional_properties
                .get(marker_key)
                .ok_or_else(|| {
                    invalid("Observed staged publication has no exact snapshot marker")
                })?;
            PositiveAttributionObservation::SnapshotMarker {
                table_uuid: table.metadata().uuid(),
                snapshot_id: *snapshot_id,
                marker_key: marker_key.clone(),
                marker_value: value.clone(),
            }
        }
        _ => {
            return Err(invalid(
                "Staged publication has no supported positive attribution probe",
            ));
        }
    };
    if let Err(error) = journal.append_positive_proof_with_finalization(
        descriptor.key,
        observation,
        ExternalMutationFinalization::Complete,
    ) {
        if let committed @ ExternalMutationOutcome::KnownCommitted { .. } =
            journal.catalog_entry_outcome(entry)
        {
            return Ok(committed);
        }
        return Err(invalid(error.to_string()));
    }
    Ok(journal.catalog_entry_outcome(entry))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::commit::staging::StagingEngine;
    use crate::iceberg::spec::{
        DataContentType, DataFileFormat, FormatVersion, NestedField, PartitionSpec, PrimitiveType,
        Schema, SortOrder, Struct, TableMetadataBuilder, Type,
    };
    use crate::resources::IcebergMetadataResources;
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorMutationOperationId, ConnectorProviderId, ConnectorStopOwner,
        ConnectorWriteReceipt, LakePublicationId, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };
    use std::time::Duration;

    struct Fixture {
        directory: tempfile::TempDir,
        adapter: IcebergStagedCreateAdapter,
        prepared: PreparedOperation,
        context: ConnectorRequestContext,
        _stop: ConnectorStopOwner,
    }
    impl Fixture {
        fn data_path(&self) -> std::path::PathBuf {
            url::Url::parse(self.prepared.staged.table.metadata().location())
                .unwrap()
                .to_file_path()
                .unwrap()
                .join("data.parquet")
        }
        fn provenance_path(&self) -> std::path::PathBuf {
            url::Url::parse(self.prepared.provenance_path.as_ref().unwrap())
                .unwrap()
                .to_file_path()
                .unwrap()
        }
        fn new(with_data: bool) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let publication_id = LakePublicationId::new_v7();
            let location = ctas_staging_location(
                &format!("file://{}", directory.path().display()),
                publication_id,
            )
            .unwrap();
            let table_directory = url::Url::parse(&location).unwrap().to_file_path().unwrap();
            std::fs::create_dir_all(&table_directory).unwrap();
            let handle = tokio::runtime::Handle::current();
            let binding = IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
                Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle.clone())),
            );
            let configuration = crate::catalog_config::parse_catalog_configuration(
                "ice",
                &[(
                    "iceberg.catalog.warehouse".to_string(),
                    directory.path().display().to_string(),
                )],
            )
            .unwrap();
            let runtime = Arc::new(
                IcebergMetadataContext::try_new(
                    IcebergCatalogControlState::new(configuration),
                    IcebergMetadataResources::new(binding.clone(), handle),
                )
                .unwrap(),
            );
            let adapter = IcebergStagedCreateAdapter::try_new(Arc::new(IcebergMetadata::new(
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                    instance_id: ConnectorInstanceId::parse("ice").unwrap(),
                },
                ProviderBindingEpoch::new(),
                runtime.clone(),
            )))
            .unwrap();
            let schema = Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap();
            let metadata = TableMetadataBuilder::new(
                schema,
                PartitionSpec::unpartition_spec(),
                SortOrder::unsorted_order(),
                location.clone(),
                FormatVersion::V3,
                HashMap::from([("user_property".into(), "frozen".into())]),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
            let mut provenance = HashMap::new();
            provenance.insert(
                CTAS_OPERATION_MARKER.into(),
                publication_marker(publication_id),
            );
            provenance.insert(
                CTAS_PROVENANCE_TABLE_UUID.into(),
                metadata.uuid().to_string(),
            );
            let files = if with_data {
                let path = table_directory.join("data.parquet");
                std::fs::write(&path, b"owned output").unwrap();
                vec![WrittenFile {
                    path: format!("file://{}", path.display()),
                    format: DataFileFormat::Parquet,
                    content: DataContentType::Data,
                    partition_values: Struct::empty(),
                    partition_spec_id: metadata.default_partition_spec_id(),
                    record_count: 3,
                    file_size_in_bytes: 12,
                    split_offsets: vec![4],
                    column_sizes: HashMap::from([(1, 24)]),
                    value_counts: HashMap::from([(1, 3)]),
                    null_value_counts: HashMap::new(),
                    nan_value_counts: HashMap::new(),
                    lower_bounds: HashMap::new(),
                    upper_bounds: HashMap::new(),
                    key_metadata: Some(vec![1, 2]),
                    referenced_data_file: None,
                    equality_ids: None,
                    first_row_id: None,
                    content_offset: None,
                    content_size_in_bytes: None,
                    cardinality: None,
                }]
            } else {
                vec![]
            };
            let reports = files
                .iter()
                .map(|file| {
                    crate::commit::report::writer_report_from_written_file(file, &metadata).unwrap()
                })
                .collect::<Vec<_>>();
            let proof = ConnectorStagedWriteProof::try_new(
                ConnectorWriteReceipt::try_new(
                    crate::write_codec::encode_writer_reports(&reports, &metadata).unwrap(),
                )
                .unwrap(),
                if with_data { 3 } else { 0 },
            )
            .unwrap();
            let initialization_updates =
                crate::commit::staging::staged_create_updates(&metadata).unwrap();
            let table = crate::iceberg::table::Table::builder()
                .runtime(
                    runtime
                        .resources()
                        .catalog_runtime()
                        .iceberg_runtime()
                        .clone(),
                )
                .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
                .file_io(crate::fs_io::build_file_io_for_location(&location, binding))
                .metadata(metadata)
                .build()
                .unwrap();
            let provenance_path = unanchored_ctas_provenance_location(&location).unwrap();
            std::fs::write(
                table_directory
                    .parent()
                    .unwrap()
                    .join(CTAS_UNANCHORED_PROVENANCE_FILE),
                b"owned provenance",
            )
            .unwrap();
            let prepared = PreparedOperation {
                stage_publication: super::super::tests::stage_publication_for_test(
                    ConnectorMutationOperationId::from_bytes(publication_id.to_bytes()),
                ),
                final_publication: None,
                publication_id,
                handle_digest: [9; 32],
                staged: RestStagedTableCreate {
                    table,
                    initialization_updates,
                    publication_updates: vec![TableUpdate::SetProperties {
                        updates: provenance,
                    }],
                },
                policy: CreatePolicy::FailIfExists,
                planning: None,
                write: Some(StagedWrite {
                    write: proof,
                    files: files.into(),
                }),
                document_properties: Some(HashMap::from([(
                    "document.manifest".into(),
                    "exact-documents".into(),
                )])),
                publication: None,
                provenance_path: Some(provenance_path),
                publication_closed: false,
            };
            let stop = ConnectorStopOwner::new();
            let context = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(60),
                stop.view(),
                MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .unwrap();
            Self {
                directory,
                adapter,
                prepared,
                context,
                _stop: stop,
            }
        }
    }

    // Only the prepare/bind setup is pure here. Publication uses the actual
    // ICE Source and original inventory; no physical grant is fabricated.
    struct OriginalFixtureCapability {
        adapter: IcebergStagedCreateAdapter,
        handle: ConnectorStagedTableHandle,
        write: ConnectorStagedWriteProof,
        source:
            Mutex<Option<std::sync::Weak<super::super::scoped_publication::PublicationOriginal>>>,
    }
    impl ConnectorStagedCreate for OriginalFixtureCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.adapter.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            self.adapter.incarnation
        }
        fn prepare(
            &self,
            request: ConnectorStagedCreatePrepareRequest,
        ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
            Ok(ConnectorStagedCreatePrepareOutcome::Prepared {
                handle: self.handle.clone(),
                receipt: self.adapter.receipt(
                    request.operation_id,
                    ConnectorStagedCreateReceiptPhase::Prepared,
                    ExternalMutationEffect::Applied,
                    Bytes::new(),
                )?,
                finalization: ExternalMutationFinalization::Complete,
            })
        }
        fn bind_write(
            &self,
            handle: ConnectorStagedTableHandle,
            write: ConnectorStagedWriteProof,
        ) -> Result<(), ConnectorError> {
            if handle != self.handle || write != self.write {
                return Err(invalid(
                    "pure original fixture received a foreign bound write",
                ));
            }
            Ok(())
        }
        fn begin_publish_original(
            &self,
            request: ConnectorStagedCreatePublishRequest,
            receiving: novarocks_spi::connector::ConnectorStagedCreatePublicationRecorder,
        ) -> Result<
            Arc<dyn novarocks_spi::connector::ConnectorStagedCreatePublicationSource>,
            ConnectorError,
        > {
            let source = super::super::scoped_publication::PublicationOriginal::new(
                self.adapter.clone(),
                request,
                receiving,
            )?;
            *self.source.lock().unwrap() = Some(Arc::downgrade(&source));
            Ok(source)
        }
        fn plan_write(
            &self,
            _: ConnectorStagedWritePlanningRequest,
        ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
            unreachable!("the actual sealed fixture write is already bound")
        }
        fn publish(
            &self,
            _: ConnectorStagedCreatePublishRequest,
        ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
            unreachable!("original fixture cannot fall back to synchronous publication")
        }
        fn abort(
            &self,
            _: ConnectorStagedCreateAbortRequest,
        ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
            unreachable!("publication fixture never invokes the separate abort entrance")
        }
        fn adjudicate_publication(
            &self,
            _: ConnectorStagedCreatePublicationAdjudicationRequest,
        ) -> Result<ConnectorStagedCreatePublicationAdjudicationOutcome, ConnectorError> {
            unreachable!("publication fixture never invents an adjudication")
        }
    }
    fn original_publication_fixture() -> (
        tempfile::TempDir,
        std::path::PathBuf,
        Arc<OriginalFixtureCapability>,
        novarocks_spi::connector::ConnectorStagedCreateLease,
        ConnectorStagedCreatePublishRequest,
    ) {
        let mut fixture = Fixture::new(true);
        let operation = fixture.prepared.operation_id();
        let handle = ConnectorStagedTableHandle::try_new(
            fixture.adapter.owner(),
            operation,
            Bytes::from_static(b"original-staged-handle"),
        )
        .unwrap();
        fixture.prepared.handle_digest = handle.digest();
        let data = fixture.data_path();
        let write = fixture.prepared.write.as_ref().unwrap().write.clone();
        let publication = fixture.prepared.publication_id;
        fixture
            .adapter
            .record_terminal(operation, OperationState::Prepared(fixture.prepared));
        let capability = Arc::new(OriginalFixtureCapability {
            adapter: fixture.adapter,
            handle: handle.clone(),
            write: write.clone(),
            source: Mutex::new(None),
        });
        let lease = novarocks_spi::connector::ConnectorStagedCreateLease::new_with_control_runtime(
            capability.adapter.descriptor.clone(),
            novarocks_spi::connector::ConnectorControlRuntimeId::new(),
            capability.adapter.incarnation,
            capability.clone(),
            || {},
        )
        .unwrap();
        lease
            .prepare(ConnectorStagedCreatePrepareRequest {
                owner: lease.owner().clone(),
                publication_id: publication,
                operation_id: operation,
                table: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: lease.owner().instance_id.clone(),
                    namespace: "db".into(),
                    table: "t".into(),
                },
                columns: vec![],
                partitioning: vec![],
                properties: BTreeMap::new(),
                policy: CreatePolicy::FailIfExists,
                mode: ConnectorStagedCreateMode::Ordinary,
                context: fixture.context.clone(),
            })
            .unwrap();
        lease.bind_write(handle.clone(), write.clone()).unwrap();
        let request = ConnectorStagedCreatePublishRequest {
            operation_id: operation,
            handle,
            write,
            payload: ConnectorStagedCreatePublicationPayload::Ordinary,
            context: fixture.context,
        };
        (fixture.directory, data, capability, lease, request)
    }

    #[tokio::test]
    async fn original_publication_factory_keeps_the_actual_prepared_inventory_before_poll() {
        let (_directory, data, capability, lease, request) = original_publication_fixture();
        let operation = request.operation_id;
        let mut wrong = request.clone();
        wrong.operation_id = ConnectorMutationOperationId::new();
        assert!(
            lease
                .begin_publish_original(wrong, &Default::default())
                .is_err()
        );
        assert!(matches!(
            capability
                .adapter
                .operations
                .lock()
                .unwrap()
                .get(&operation),
            Some(OperationState::Prepared(_))
        ));
        let receiving =
            novarocks_spi::connector::ConnectorStagedCreatePublicationReceiving::default();
        let execution = lease
            .begin_publish_original(request.clone(), &receiving)
            .unwrap();
        assert!(
            lease
                .begin_publish_original(request, &Default::default())
                .is_err(),
            "the original operation stays busy while its Source owns Prepared"
        );
        let source = capability
            .source
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        assert_eq!(source.body_for_test(), (true, false));
        assert!(matches!(
            capability
                .adapter
                .operations
                .lock()
                .unwrap()
                .get(&operation),
            Some(OperationState::Publishing { .. })
        ));
        assert!(!receiving.observation().provider_exited());
        assert!(data.exists());
        drop(source);
        drop(execution);
        assert!(
            matches!(
                capability
                    .adapter
                    .operations
                    .lock()
                    .unwrap()
                    .get(&operation),
                Some(OperationState::Publishing { .. })
            ),
            "dropping a borrowed execution does not drop the stable Source"
        );
        drop(receiving);
        assert!(
            matches!(
                capability
                    .adapter
                    .operations
                    .lock()
                    .unwrap()
                    .get(&operation),
                Some(OperationState::Prepared(_))
            ),
            "an unpolled original returns the same prepared inventory"
        );
        assert!(data.exists());
    }

    #[tokio::test]
    async fn original_publication_destructor_panic_keeps_the_same_body_and_is_not_repolled() {
        use std::{
            future::Future,
            pin::Pin,
            sync::atomic::{AtomicUsize, Ordering},
            task::{Context, Poll},
        };
        struct OriginalDropPanic;
        struct ReadyOriginal(Arc<AtomicUsize>);
        impl Future for ReadyOriginal {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                Poll::Ready(())
            }
        }
        impl Drop for ReadyOriginal {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
                std::panic::panic_any(OriginalDropPanic);
            }
        }
        let (_directory, data, capability, lease, request) = original_publication_fixture();
        let receiving =
            novarocks_spi::connector::ConnectorStagedCreatePublicationReceiving::default();
        let mut execution = lease.begin_publish_original(request, &receiving).unwrap();
        let source = capability
            .source
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        source.install_ready_original_for_test(Box::pin(ReadyOriginal(drops.clone())));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let cause = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execution.poll_result(&mut cx)
        }))
        .unwrap_err();
        assert!(cause.is::<OriginalDropPanic>());
        assert!(receiving.observation().foreign_exit_unproven());
        assert!(!receiving.observation().provider_exited());
        assert_eq!(source.body_for_test(), (true, true));
        assert!(execution.poll_closed(&mut cx).is_pending());
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "poison never retries the original destructor"
        );
        assert!(data.exists());
    }

    #[tokio::test]
    async fn positive_observation_appends_only_to_the_retained_final_frontier() {
        use crate::commit::attempt::observation::{DispatchEvidence, MemoryDispatchBarrier};
        use crate::commit::publication_descriptor::AttributionPlan;
        for with_data in [false, true] {
            let mut fixture = Fixture::new(with_data);
            let operation_id = fixture.prepared.operation_id();
            let stage = fixture.prepared.stage_publication.clone();
            let stage_entry = stage
                .journal
                .prepare_catalog(
                    stage.evidence.clone(),
                    stage.receipt.clone(),
                    vec!["db.t".into()],
                    b"initial stage".to_vec(),
                    AttributionPlan::ObserveOnly,
                    fixture.context.clone(),
                )
                .unwrap();
            stage
                .journal
                .claim_dispatch()
                .unwrap()
                .begin_dispatch(&fixture.context, &MemoryDispatchBarrier)
                .await
                .unwrap();
            stage.journal.conclude_entry(
                stage_entry,
                DispatchEvidence::Applied {
                    proof: crate::catalog::transaction::CommitProof::applied(None),
                },
            );
            let journal = stage.journal.next_frontier().unwrap();
            fixture.prepared.final_publication = Some(journal.clone());
            let operation = ensure_operation(
                &fixture.adapter,
                &mut fixture.prepared,
                operation_id,
                &fixture.context,
            )
            .unwrap();
            let intent = create_intent(&fixture.prepared, operation.token()).unwrap();
            let publisher = CreatePublisher {
                adapter: fixture.adapter.clone(),
                prepared: Arc::new(fixture.prepared.clone()),
                operation_id,
                journal: journal.clone(),
            };
            let attempt = operation.begin_attempt().unwrap();
            let mut engine = StagingEngine::begin(
                publisher.load_target(&attempt).await.unwrap(),
                &intent,
                &attempt,
            )
            .unwrap();
            engine
                .stage_change(create_prefix(&fixture.prepared))
                .unwrap();
            if with_data {
                engine
                    .stage(&crate::commit::FastAppendPreparer)
                    .await
                    .unwrap();
            }
            let metadata = engine.metadata().clone();
            let request = engine.freeze(&[]).unwrap();
            publisher.preflight_recovery(&request, &operation).unwrap();
            let entry = journal.current_entry().unwrap();
            journal
                .claim_dispatch()
                .unwrap()
                .begin_dispatch(&fixture.context, &MemoryDispatchBarrier)
                .await
                .unwrap();
            journal.conclude_entry(
                entry,
                DispatchEvidence::Unknown {
                    failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                        novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                        "injected lost final response",
                    ),
                    observation: Default::default(),
                },
            );
            assert!(
                record_observed_publication(&fixture.prepared, &fixture.prepared.staged.table)
                    .is_err()
            );
            assert!(matches!(
                journal.catalog_entry_outcome(entry),
                ExternalMutationOutcome::CommitUnknown { .. }
            ));
            let table = crate::iceberg::table::Table::builder()
                .runtime(
                    fixture
                        .adapter
                        .runtime
                        .resources()
                        .catalog_runtime()
                        .iceberg_runtime()
                        .clone(),
                )
                .identifier(fixture.prepared.staged.table.identifier().clone())
                .file_io(fixture.prepared.staged.table.file_io().clone())
                .metadata(metadata)
                .build()
                .unwrap();
            let outcome = record_observed_publication(&fixture.prepared, &table).unwrap();
            let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
                panic!("exact final marker must append positive proof")
            };
            assert_eq!(
                receipt.phase(),
                ConnectorStagedCreateReceiptPhase::Published
            );
            let ExternalMutationOutcome::KnownCommitted {
                receipt: stage_receipt,
                ..
            } = stage.journal.catalog_entry_outcome(stage_entry)
            else {
                panic!("stage proof remains retained")
            };
            assert_eq!(
                stage_receipt.phase(),
                ConnectorStagedCreateReceiptPhase::Prepared
            );
            assert_eq!(stage.journal.current_entry(), Some(stage_entry));
            assert_eq!(journal.current_entry(), Some(entry));
            assert!(
                matches!(
                    record_observed_publication(&fixture.prepared, &table).unwrap(),
                    ExternalMutationOutcome::KnownCommitted { .. }
                ),
                "repeated observation projects the already-recorded positive proof"
            );
        }
    }

    #[test]
    fn canonical_create_preserves_authority_documents_and_actual_empty_snapshot_shape() {
        let executor = tokio::runtime::Runtime::new().unwrap();
        executor.block_on(async {
            for with_data in [false, true] {
                let mut fixture = Fixture::new(with_data);
                let operation_id = fixture.prepared.operation_id();
                let original = fixture.prepared.staged.table.metadata().clone();
                let operation = ensure_operation(
                    &fixture.adapter,
                    &mut fixture.prepared,
                    operation_id,
                    &fixture.context,
                )
                .unwrap();
                let intent = create_intent(&fixture.prepared, operation.token()).unwrap();
                let journal = PublicationJournal::new(operation.token());
                let publisher = CreatePublisher {
                    adapter: fixture.adapter.clone(),
                    prepared: Arc::new(fixture.prepared.clone()),
                    operation_id,
                    journal: journal.clone(),
                };
                let attempt = operation.begin_attempt().unwrap();
                let mut engine = StagingEngine::begin(
                    publisher.load_target(&attempt).await.unwrap(),
                    &intent,
                    &attempt,
                )
                .unwrap();
                engine
                    .stage_change(create_prefix(&fixture.prepared))
                    .unwrap();
                assert_eq!(
                    engine.metadata().current_schema(),
                    original.current_schema()
                );
                assert_eq!(
                    engine.metadata().default_partition_spec(),
                    original.default_partition_spec()
                );
                assert_eq!(
                    engine.metadata().default_sort_order(),
                    original.default_sort_order()
                );
                assert_eq!(engine.metadata().uuid(), original.uuid());
                assert_eq!(engine.metadata().properties()["user_property"], "frozen");
                assert_eq!(
                    engine.metadata().properties()["document.manifest"],
                    "exact-documents"
                );
                if with_data {
                    engine
                        .stage(&crate::commit::FastAppendPreparer)
                        .await
                        .unwrap();
                }
                let mut refs = operation.publication_references().unwrap();
                refs.extend(
                    fixture
                        .prepared
                        .write
                        .as_ref()
                        .unwrap()
                        .files
                        .iter()
                        .map(|file| ObjectIdentity::new(file.path.clone()).unwrap()),
                );
                let request = engine.freeze(&refs).unwrap();
                assert_eq!(
                    request.requirements(),
                    &[crate::iceberg::TableRequirement::NotExist]
                );
                assert_eq!(request.ref_snapshot_after("main").is_some(), with_data);
                assert_eq!(
                    request
                        .updates()
                        .iter()
                        .any(|update| matches!(update, TableUpdate::AddSnapshot { .. })),
                    with_data
                );
                publisher.preflight_recovery(&request, &operation).unwrap();
                let outcome = journal
                    .bridge_failure(&operation, "injected before-dispatch bridge stop".into());
                assert!(matches!(
                    outcome,
                    ExternalMutationOutcome::KnownUncommitted { .. }
                ));
                let facts = FrozenPublicationFacts::from_request(&request, &operation).unwrap();
                let encoded = serde_json::to_vec(&facts).unwrap();
                let decoded: FrozenPublicationFacts = serde_json::from_slice(&encoded).unwrap();
                assert_eq!(facts, decoded);
                let envelope = PublishEvidenceV1 {
                    version: EVIDENCE_VERSION,
                    operation_marker: publication_marker(fixture.prepared.publication_id),
                    table_uuid: original.uuid().to_string(),
                    expected_snapshot_id: request.ref_snapshot_after("main"),
                    handle_digest: fixture.prepared.handle_digest,
                    namespace: request.identifier().namespace.to_url_string(),
                    table: request.identifier().name.clone(),
                    facts: facts.clone(),
                };
                validate_publish_evidence(&envelope, &fixture.prepared, operation_id).unwrap();
                for field in ["table", "base", "published_snapshot_id"] {
                    let mut tampered = serde_json::to_value(&envelope).unwrap();
                    match field {
                        "table" => tampered["facts"]["table"] = serde_json::json!("another_table"),
                        "base" => {
                            tampered["facts"]["base"]["uuid"] =
                                serde_json::json!(uuid::Uuid::now_v7().to_string())
                        }
                        _ => {
                            tampered["facts"]["published_snapshot_id"] = serde_json::json!(
                                request.ref_snapshot_after("main").unwrap_or(0) + 1
                            )
                        }
                    }
                    let tampered: PublishEvidenceV1 = serde_json::from_value(tampered).unwrap();
                    assert!(
                        validate_publish_evidence(&tampered, &fixture.prepared, operation_id)
                            .is_err(),
                        "nested {field} cannot drift from the exact create identity"
                    );
                }
                decoded
                    .validate(OperationToken::from_mutation(
                        ConnectorMutationOperationId::from_bytes(operation_id.to_bytes()),
                    ))
                    .unwrap();
                let text = String::from_utf8(encoded).unwrap();
                assert!(text.contains(CTAS_UNANCHORED_PROVENANCE_FILE));
                if with_data {
                    assert!(text.contains("data.parquet"));
                    assert_eq!(
                        intent.changes().added[0].file().column_sizes().get(&1),
                        Some(&24)
                    );
                    assert_eq!(
                        intent.changes().added[0].file().key_metadata(),
                        Some(&[1_u8, 2][..])
                    );
                }
                let provenance_object =
                    ObjectIdentity::new(fixture.prepared.provenance_path.clone().unwrap()).unwrap();
                assert!(
                    request
                        .artifacts()
                        .operation_references()
                        .contains(&provenance_object)
                );
                let mut retained = request.artifacts().attempt_owned().to_vec();
                retained.extend_from_slice(request.artifacts().operation_references());
                retained.extend_from_slice(request.artifacts().session_references());
                assert!(matches!(
                    operation.cleanup_after_commit(&retained).await,
                    crate::commit::model::IcebergCleanupReport::Complete { .. }
                ));
                assert!(fixture.provenance_path().exists());
                if with_data {
                    assert!(fixture.data_path().exists());
                }

                // A separate unpublished owner cleans its proven outputs and
                // provenance; no committed owner is reopened for abort.
                let mut aborted = Fixture::new(with_data);
                let abort_id = aborted.prepared.operation_id();
                let abort_operation = ensure_operation(
                    &aborted.adapter,
                    &mut aborted.prepared,
                    abort_id,
                    &aborted.context,
                )
                .unwrap();
                assert!(matches!(
                    abort_operation
                        .cleanup(crate::commit::model::CleanupScope::EntireOperation)
                        .await,
                    crate::commit::model::IcebergCleanupReport::Complete { .. }
                ));
                assert!(!aborted.provenance_path().exists());
                assert!(!aborted.data_path().exists());
            }
        });
    }

    #[test]
    fn create_preflight_bounds_the_complete_ledger_before_dispatch() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let mut fixture = Fixture::new(false);
            let operation_id = fixture.prepared.operation_id();
            let operation = ensure_operation(
                &fixture.adapter,
                &mut fixture.prepared,
                operation_id,
                &fixture.context,
            )
            .unwrap();
            // Unreferenced owned records still belong to recovery; projection
            // must not hide them to squeeze an unknown result into the carrier.
            for ordinal in 0..350 {
                operation
                    .adopt_session_data(
                        ObjectIdentity::new(format!(
                            "file://{}/{}.{}",
                            fixture.directory.path().display(),
                            ordinal,
                            "x".repeat(240)
                        ))
                        .unwrap(),
                    )
                    .unwrap();
            }
            let intent = create_intent(&fixture.prepared, operation.token()).unwrap();
            let journal = PublicationJournal::new(operation.token());
            let publisher = CreatePublisher {
                adapter: fixture.adapter.clone(),
                prepared: Arc::new(fixture.prepared.clone()),
                operation_id,
                journal: journal.clone(),
            };
            let attempt = operation.begin_attempt().unwrap();
            let mut engine = StagingEngine::begin(
                publisher.load_target(&attempt).await.unwrap(),
                &intent,
                &attempt,
            )
            .unwrap();
            engine
                .stage_change(create_prefix(&fixture.prepared))
                .unwrap();
            let request = engine
                .freeze(&operation.publication_references().unwrap())
                .unwrap();
            assert!(publisher.preflight_recovery(&request, &operation).is_err());
            assert!(matches!(
                journal.bridge_failure(&operation, "preflight refused".into()),
                ExternalMutationOutcome::KnownUncommitted { .. }
            ));
            assert_eq!(operation.artifacts().unwrap().len(), 351);
        });
    }
}
