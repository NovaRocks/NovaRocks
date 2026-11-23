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
use crate::commit::fast_append::CompactFastAppendPreparer;
use crate::commit::model::{
    ArtifactWriter, FileChanges, FrozenRequest, ObjectIdentity, OperationIntent,
    OperationIntentParts, OperationToken, RequestShape, StagedCreateIdentity, TableTarget,
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
    if let Some(write) = &prepared.write {
        let inventory = write.seal.adopt_inventory(|inventory| {
            for entry in inventory.files.entries() {
                let addition = entry
                    .addition
                    .as_ref()
                    .ok_or_else(|| corrupt("Iceberg staged-create inventory contains a removal"))?;
                let object = ObjectIdentity::from_shared_path(addition.object_path.clone())
                    .map_err(|error| corrupt(error.to_string()))?;
                operation
                    .adopt_session_data(object)
                    .map_err(|error| internal(error.to_string()))?;
            }
            Ok(())
        })?;
        // The complete path ledger owns cleanup, including if the operation
        // cannot install its manifest input.
        prepared.publication = Some(operation.clone());
        operation
            .install_stage_new_files(
                0,
                crate::commit::PendingNewFiles {
                    received: inventory.files,
                    source: Arc::new(prepared.staged.table.metadata().clone()),
                    codec: inventory.codec,
                },
            )
            .map_err(|error| internal(error.to_string()))?;
    } else {
        prepared.publication = Some(operation.clone());
    }
    Ok(operation)
}

pub(super) fn create_intent(
    prepared: &PreparedOperation,
    token: OperationToken,
) -> crate::iceberg::Result<OperationIntent> {
    use crate::commit::model::CommitProtocol;
    let write = prepared
        .write
        .as_ref()
        .ok_or_else(|| iceberg_error("Create publication requires a sealed write"))?;
    let managed = prepared.document_properties.is_some();
    if managed && write.write.row_count() != 0 {
        return Err(iceberg_error(
            "Document-managed create requires a zero-row sealed write",
        ));
    }
    let intent = OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: prepared.staged.table.identifier().clone(),
            uuid: None,
        },
        target_ref: "main".into(),
        start: None,
        changes: FileChanges::default(),
        dependencies: vec![],
        protocol: if managed {
            CommitProtocol::Legacy
        } else {
            CommitProtocol::CreateTable
        },
        shape: RequestShape::Create,
        summary: BTreeMap::new(),
        token,
    })?;
    if managed {
        Ok(intent)
    } else {
        intent.with_compact_stage(0)
    }
}

pub(super) fn create_prefix(
    prepared: &PreparedOperation,
) -> Result<PreparedChange, ConnectorError> {
    let mut updates = prepared.staged.publication_updates.clone();
    if let Some(properties) = &prepared.document_properties {
        updates.push(TableUpdate::SetProperties {
            updates: properties.clone(),
        });
    }
    Ok(PreparedChange {
        updates,
        requirements: vec![],
    })
}

pub(super) struct CreateStages {
    append: Option<CompactFastAppendPreparer>,
    statistics: crate::commit::write_stack::control::WriteStatisticsPreparer,
}

impl CreateStages {
    pub(super) fn new(prepared: &PreparedOperation, operation: &IcebergCommitOperation) -> Self {
        let append = prepared
            .document_properties
            .is_none()
            .then(|| CompactFastAppendPreparer(operation.clone()));
        let drafts = prepared
            .write
            .as_ref()
            .map_or_else(Vec::new, |write| write.seal.statistics.to_vec());
        Self {
            append,
            statistics: crate::commit::write_stack::control::WriteStatisticsPreparer {
                op_kind: crate::commit::CommitOpKind::FastAppend,
                drafts,
            },
        }
    }

    pub(super) fn preparers(&self) -> Vec<&dyn Preparer> {
        self.append.as_ref().map_or_else(Vec::new, |append| {
            vec![append as &dyn Preparer, &self.statistics]
        })
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
        let metadata_path = self
            .prepared
            .staged
            .final_metadata_path
            .as_ref()
            .map(StagedMetadataPath::as_str);
        let payload = serde_json::to_vec(&PublishEvidenceV2 {
            version: EVIDENCE_VERSION,
            operation_marker: publication_marker(self.prepared.publication_id),
            table_uuid: self.prepared.staged.table.metadata().uuid().to_string(),
            expected_snapshot_id,
            final_metadata_path: metadata_path.map(str::to_owned),
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
    let prefix = create_prefix(prepared)?;
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
    let stages = CreateStages::new(prepared, &operation);
    let run_operation = operation.clone();
    let result = operation.runtime().block_on(async move {
        let preparers = stages.preparers();
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
    let prefix = create_prefix(prepared)?;
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
    let stages = CreateStages::new(prepared, &operation);
    let preparers = stages.preparers();
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
        LakePublicationId, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
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
        fn new(with_data: bool) -> Self {
            Self::build(with_data, None)
        }

        fn managed(metadata_path: Option<StagedMetadataPath>) -> Self {
            let mut fixture = Self::build(false, metadata_path);
            fixture.prepared.document_properties = Some(HashMap::from([(
                "document.manifest".into(),
                "exact-documents".into(),
            )]));
            fixture
        }

        fn build(with_data: bool, metadata_path: Option<StagedMetadataPath>) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let publication_id = LakePublicationId::new_v7();
            let table_directory = directory.path().join("table");
            let location = format!("file://{}", table_directory.display());
            std::fs::create_dir(&table_directory).unwrap();
            let handle = tokio::runtime::Handle::current();
            let binding = IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
                novarocks_fs::TokioRefreshExecutor::shared(handle.clone()),
                Arc::new(novarocks_fs::TokioFileIoSpawner::new(handle.clone())),
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
            let mut properties = HashMap::from([("user_property".into(), "frozen".into())]);
            if let Some(path) = &metadata_path {
                properties.insert(WRITE_METADATA_PATH.into(), path.as_str().into());
            }
            let metadata = TableMetadataBuilder::new(
                schema,
                PartitionSpec::unpartition_spec(),
                SortOrder::unsorted_order(),
                location.clone(),
                FormatVersion::V3,
                properties,
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
            let publication_properties = HashMap::from([(
                CTAS_OPERATION_MARKER.into(),
                publication_marker(publication_id),
            )]);
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
                    key_metadata: None,
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
            let row_count = if with_data { 3 } else { 0 };
            let owner = adapter.owner();
            let receipt = crate::write_codec::seal_staged_files(
                files,
                &metadata,
                publication_id.to_bytes(),
                &owner,
            )
            .unwrap();
            let seal = crate::write_codec::open_compact_staged_files(
                &receipt, &metadata, row_count, &owner,
            )
            .unwrap();
            let proof = ConnectorStagedWriteProof::try_new(receipt, row_count).unwrap();
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
                        updates: publication_properties,
                    }],
                    location_refusal: None,
                    final_metadata_path: metadata_path,
                },
                policy: CreatePolicy::FailIfExists,
                planning: None,
                write: Some(StagedWrite { write: proof, seal }),
                document_properties: None,
                publication: None,
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

    // Observe the actual frozen create request and its retained attribution entry.
    #[tokio::test]
    async fn managed_create_publishes_at_the_staged_location_without_a_snapshot() {
        use crate::commit::attempt::observation::{DispatchEvidence, MemoryDispatchBarrier};

        let metadata_path = "s3://warehouse/base/final-metadata";
        let file_uri = "s3://warehouse/base/final-metadata/00000-unit.metadata.json";
        let mut fixture = Fixture::managed(Some(
            StagedMetadataPath::under_warehouse(
                metadata_path,
                "s3://warehouse/base",
                CTAS_STAGING_NAMESPACE,
            )
            .unwrap(),
        ));
        let staged = fixture.prepared.staged.table.metadata().clone();
        let operation_id = fixture.prepared.operation_id();

        let mut with_rows = fixture.prepared.clone();
        let write = with_rows.write.as_mut().unwrap();
        write.write = ConnectorStagedWriteProof::try_new(write.write.receipt().clone(), 1).unwrap();
        assert!(create_intent(&with_rows, OperationToken::from_mutation(operation_id)).is_err());

        let operation = ensure_operation(
            &fixture.adapter,
            &mut fixture.prepared,
            operation_id,
            &fixture.context,
        )
        .unwrap();
        let intent = create_intent(&fixture.prepared, operation.token()).unwrap();
        let stages = CreateStages::new(&fixture.prepared, &operation);
        assert!(stages.preparers().is_empty());
        let journal = fixture
            .prepared
            .stage_publication
            .journal
            .next_frontier()
            .unwrap();
        let publisher = CreatePublisher {
            adapter: fixture.adapter.clone(),
            prepared: Arc::new(fixture.prepared.clone()),
            operation_id,
            journal: journal.clone(),
        };
        fixture.prepared.final_publication = Some(journal.clone());
        let attempt = operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            publisher.load_target(&attempt).await.unwrap(),
            &intent,
            &attempt,
        )
        .unwrap();
        engine
            .stage_change(create_prefix(&fixture.prepared).unwrap())
            .unwrap();
        let metadata = engine.metadata().clone();
        let request = engine.freeze().unwrap();
        assert_eq!(
            request.requirements(),
            &[crate::iceberg::TableRequirement::NotExist]
        );
        assert_eq!(metadata.location(), staged.location());
        assert_eq!(
            metadata.properties()["document.manifest"],
            "exact-documents"
        );
        assert_eq!(
            metadata
                .properties()
                .get(WRITE_METADATA_PATH)
                .map(String::as_str),
            Some(metadata_path)
        );
        assert_eq!(
            staged.properties().get(WRITE_METADATA_PATH),
            metadata.properties().get(WRITE_METADATA_PATH)
        );
        assert!(request.ref_snapshot_after("main").is_none());
        assert!(request.artifacts().references().is_empty());
        assert_eq!(
            request
                .updates()
                .iter()
                .filter_map(|update| match update {
                    TableUpdate::SetLocation { location } => Some(location.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            [staged.location()],
        );
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
                    "lost custom-location response",
                ),
                observation: Default::default(),
            },
        );
        let fresh_table = |metadata, uri: &str| {
            crate::iceberg::table::Table::builder()
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
                .metadata_location(uri)
                .build()
                .unwrap()
        };
        let wrong_location = fresh_table(
            metadata
                .clone()
                .into_builder(None)
                .set_location("s3://warehouse/base/another-table".into())
                .build()
                .unwrap()
                .metadata,
            file_uri,
        );
        assert!(!publication_matches(
            &wrong_location,
            operation_id,
            &fixture.prepared,
            None
        ));
        let wrong_directory = fresh_table(
            metadata.clone(),
            "s3://warehouse/base/staged/00000-unit.metadata.json",
        );
        assert!(!publication_matches(
            &wrong_directory,
            operation_id,
            &fixture.prepared,
            None
        ));
        let wrong_property = fresh_table(
            metadata
                .clone()
                .into_builder(None)
                .set_properties(HashMap::from([(
                    WRITE_METADATA_PATH.to_string(),
                    "s3://warehouse/base/foreign".to_string(),
                )]))
                .unwrap()
                .build()
                .unwrap()
                .metadata,
            file_uri,
        );
        assert!(!publication_matches(
            &wrong_property,
            operation_id,
            &fixture.prepared,
            None
        ));
        assert!(matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        let exact_location = fresh_table(metadata, file_uri);
        assert!(publication_matches(
            &exact_location,
            operation_id,
            &fixture.prepared,
            None
        ));
        assert!(matches!(
            record_observed_publication(&fixture.prepared, &exact_location).unwrap(),
            ExternalMutationOutcome::KnownCommitted { .. },
        ));
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
        novarocks_spi::connector::ConnectorStagedCreateSession,
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
        let lease =
            novarocks_spi::connector::ConnectorStagedCreateSession::new_with_control_runtime(
                capability.adapter.descriptor.clone(),
                novarocks_spi::connector::ConnectorControlRuntimeId::new(),
                capability.adapter.incarnation,
                capability.clone(),
                || {},
            )
            .unwrap();
        let mut prepare = crate::catalog_control::staged_create::tests::stage_http_request(
            &capability.adapter,
            fixture.context.clone(),
        );
        prepare.operation_id = operation;
        prepare.publication_id = publication;
        lease.prepare(prepare).unwrap();
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
                .stage_change(create_prefix(&fixture.prepared).unwrap())
                .unwrap();
            let stages = CreateStages::new(&fixture.prepared, &operation);
            for preparer in stages.preparers() {
                engine.stage(preparer).await.unwrap();
            }
            let metadata = engine.metadata().clone();
            let request = engine.freeze().unwrap();
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
            if with_data {
                assert!(fixture.data_path().exists());
            }
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
                let seal = Arc::clone(&fixture.prepared.write.as_ref().unwrap().seal);
                seal.with_inventory(|inventory| {
                    assert_eq!(inventory.files.counts.data.files, u64::from(with_data));
                    assert_eq!(
                        inventory.files.counts.data.rows,
                        if with_data { 3 } else { 0 }
                    );
                    Ok(())
                })
                .unwrap();
                let operation = ensure_operation(
                    &fixture.adapter,
                    &mut fixture.prepared,
                    operation_id,
                    &fixture.context,
                )
                .unwrap();
                assert!(seal.with_inventory(|_| Ok(())).is_err());
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
                    .stage_change(create_prefix(&fixture.prepared).unwrap())
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
                let stages = CreateStages::new(&fixture.prepared, &operation);
                for preparer in stages.preparers() {
                    engine.stage(preparer).await.unwrap();
                }
                let snapshot = engine.metadata().current_snapshot().unwrap();
                assert_eq!(
                    snapshot.summary().operation,
                    crate::iceberg::spec::Operation::Append
                );
                assert_eq!(
                    snapshot.summary().additional_properties["added-records"],
                    if with_data { "3" } else { "0" }
                );
                let snapshot_id = snapshot.snapshot_id();
                let request = engine.freeze().unwrap();
                assert_eq!(
                    request.requirements(),
                    &[crate::iceberg::TableRequirement::NotExist]
                );
                assert_eq!(request.ref_snapshot_after("main"), Some(snapshot_id));
                assert_eq!(
                    request
                        .updates()
                        .iter()
                        .filter(|update| matches!(update, TableUpdate::AddSnapshot { .. }))
                        .count(),
                    1
                );
                assert!(
                    !request
                        .updates()
                        .iter()
                        .any(|update| matches!(update, TableUpdate::SetStatistics { .. }))
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
                let envelope = PublishEvidenceV2 {
                    version: EVIDENCE_VERSION,
                    operation_marker: publication_marker(fixture.prepared.publication_id),
                    table_uuid: original.uuid().to_string(),
                    expected_snapshot_id: request.ref_snapshot_after("main"),
                    final_metadata_path: fixture
                        .prepared
                        .staged
                        .final_metadata_path
                        .as_ref()
                        .map(|path| path.as_str().to_owned()),
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
                    let tampered: PublishEvidenceV2 = serde_json::from_value(tampered).unwrap();
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
                if with_data {
                    assert!(!text.contains("data.parquet"));
                    let object =
                        ObjectIdentity::new(format!("file://{}", fixture.data_path().display()))
                            .unwrap();
                    assert!(request.artifacts().references().contains(&object));
                }
                let retained = request.artifacts().references();
                assert!(matches!(
                    operation.cleanup_after_commit(retained.iter()).await,
                    crate::commit::model::IcebergCleanupReport::Complete { .. }
                ));
                if with_data {
                    assert!(fixture.data_path().exists());
                }

                // A separate unpublished owner cleans only the files it adopted.
                let mut aborted = Fixture::new(with_data);
                let sibling = aborted.data_path().with_file_name("sibling.parquet");
                std::fs::write(&sibling, b"outside the operation").unwrap();
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
                assert!(!aborted.data_path().exists());
                assert_eq!(std::fs::read(sibling).unwrap(), b"outside the operation");
            }
        });
    }

    #[test]
    fn create_large_owned_ledger_passes_preflight_and_dispatches_once() {
        struct ControlledCreatePublisher {
            inner: CreatePublisher,
            dispatches: std::sync::atomic::AtomicUsize,
        }
        #[async_trait]
        impl crate::commit::attempt::Publisher for ControlledCreatePublisher {
            async fn load_target(
                &self,
                attempt: &IcebergCommitAttempt,
            ) -> crate::iceberg::Result<StagingBase> {
                self.inner.load_target(attempt).await
            }
            fn preflight_recovery(
                &self,
                request: &FrozenRequest,
                operation: &IcebergCommitOperation,
            ) -> crate::iceberg::Result<()> {
                self.inner.preflight_recovery(request, operation)
            }
            async fn dispatch_once(
                &self,
                request: FrozenRequest,
            ) -> crate::catalog::error::CatalogOutcome<crate::catalog::transaction::CommitProof>
            {
                self.dispatches
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                crate::catalog::error::CatalogOutcome::committed(
                    crate::catalog::transaction::CommitProof::applied(
                        request.ref_snapshot_after("main"),
                    ),
                    ExternalMutationEffect::Applied,
                )
            }
        }
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
            let abandoned = operation.begin_attempt().unwrap();
            for _ in 0..1200 {
                abandoned
                    .allocate(crate::commit::model::ArtifactKind::Manifest)
                    .unwrap();
            }
            let intent = create_intent(&fixture.prepared, operation.token()).unwrap();
            let journal = PublicationJournal::new(operation.token());
            let publisher = Arc::new(ControlledCreatePublisher {
                inner: CreatePublisher {
                    adapter: fixture.adapter.clone(),
                    prepared: Arc::new(fixture.prepared.clone()),
                    operation_id,
                    journal: journal.clone(),
                },
                dispatches: std::sync::atomic::AtomicUsize::new(0),
            });
            let observed = journal.observe(publisher.clone());
            let stages = CreateStages::new(&fixture.prepared, &operation);
            let report = attempt::run(
                &operation,
                &intent,
                &create_prefix(&fixture.prepared).unwrap(),
                &stages.preparers(),
                &observed,
                RetryPolicy::from_properties(fixture.prepared.staged.table.metadata().properties())
                    .unwrap(),
            )
            .await;
            assert!(
                matches!(
                    report.publication,
                    crate::commit::model::PublicationOutcome::Committed(_)
                ),
                "{report:?}"
            );
            assert_eq!(
                publisher
                    .dispatches
                    .load(std::sync::atomic::Ordering::SeqCst),
                1
            );
            assert!(matches!(
                report.cleanup,
                crate::commit::model::IcebergCleanupReport::Complete { .. }
            ));
            assert!(operation.artifacts().unwrap().is_empty());
        });
    }
    #[test]
    fn create_publishes_a_thousand_files_from_the_original_bounded_seal() {
        use crate::iceberg::TableRequirement;
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        let initial = Arc::new(Mutex::new(None::<crate::iceberg::spec::TableMetadata>));
        let committed = Arc::new(Mutex::new(None::<crate::iceberg::spec::TableMetadata>));
        let input = initial.clone();
        let output = committed.clone();
        let server = TestServer::start(Script::new()
            .on("GET", "/v1/config", Reply::config(&[], &[], None))
            .on_fn("POST", "/v1/namespaces/db/tables/t", move |request, index| {
                if index != 0 || output.lock().unwrap().is_some() {
                    return Reply::status(409);
                }
                let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                let requirements: Vec<TableRequirement> = serde_json::from_value(body["requirements"].clone()).unwrap();
                assert_eq!(requirements, [TableRequirement::NotExist]);
                let updates: Vec<TableUpdate> = serde_json::from_value(body["updates"].clone()).unwrap();
                let mut builder = input.lock().unwrap().as_ref().unwrap().clone().into_builder(None);
                for update in updates {
                    builder = update.apply(builder).unwrap();
                }
                let metadata = builder.build().unwrap().metadata;
                let reply = Reply::json(200, &serde_json::json!({
                    "metadata-location": format!("{}/metadata/00001-published.metadata.json", metadata.location()),
                    "metadata": metadata,
                }));
                *output.lock().unwrap() = Some(metadata);
                reply
            }));
        let (executor, adapter) = super::super::tests::stage_http_adapter(&server);
        executor.block_on(async {
            let mut fixture = Fixture::new(true);
            let metadata = fixture.prepared.staged.table.metadata().clone();
            *initial.lock().unwrap() = Some(metadata.clone());
            let initial_write = fixture.prepared.write.as_ref().unwrap();
            let template = crate::write_codec::open_staged_files(
                initial_write.write.receipt(),
                &metadata,
                &fixture.adapter.owner(),
            )
            .unwrap()[0]
                .clone();
            fixture.adapter = adapter;
            let directory = fixture.data_path().parent().unwrap().to_path_buf();
            let mut files = Vec::new();
            for ordinal in 0..1000 {
                let mut file = template.clone();
                let path = if ordinal == 0 {
                    fixture.data_path()
                } else {
                    directory.join(format!("data-{ordinal:04}.parquet"))
                };
                std::fs::write(&path, b"owned output").unwrap();
                file.path = format!("file://{}", path.display());
                files.push(file);
            }
            let paths: std::collections::BTreeSet<_> =
                files.iter().map(|file| file.path.clone()).collect();
            assert_eq!(paths.len(), 1000);
            let reports = files
                .iter()
                .map(|file| {
                    crate::commit::report::writer_report_from_written_file(file, &metadata).unwrap()
                })
                .collect::<Vec<_>>();
            assert!(
                crate::write_codec::encode_writer_reports(&reports, &metadata)
                    .unwrap()
                    .len()
                    > 64 * 1024
            );
            let statistics = novarocks_spi::connector::StatisticsArtifactDraft::try_new(
                vec![1],
                crate::iceberg::puffin::APACHE_DATASKETCHES_THETA_V1,
                Bytes::from_static(include_bytes!(
                    "../../../../../../tests/datasketches-tck/fixtures/theta/rust_quickselect_n1000_ordered_v3.sk"
                )),
                BTreeMap::new(),
            )
            .unwrap();
            let statistics_body = statistics.body().clone();
            let receipt = crate::write_codec::seal_staged_files_with_statistics(
                files,
                vec![statistics],
                &metadata,
                fixture.prepared.publication_id.to_bytes(),
                &fixture.adapter.owner(),
            )
            .unwrap();
            assert!(receipt.payload().len() < 512);
            let seal = crate::write_codec::open_compact_staged_files(
                &receipt,
                &metadata,
                3000,
                &fixture.adapter.owner(),
            )
            .unwrap();
            fixture.prepared.write = Some(StagedWrite {
                write: ConnectorStagedWriteProof::try_new(receipt, 3000).unwrap(),
                seal,
            });
            let operation_id = fixture.prepared.operation_id();
            let outcome = publish_async(
                &fixture.adapter,
                &mut fixture.prepared,
                operation_id,
                &fixture.context,
            )
            .await
            .unwrap();
            assert!(
                matches!(
                    outcome,
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        finalization: ExternalMutationFinalization::Complete,
                        cleanup: ExternalMutationFinalization::Complete,
                        ..
                    }
                ),
                "{outcome:?}"
            );
            assert_eq!(server.count("POST", "/v1/namespaces/db/tables/t"), 1);
            let published = committed.lock().unwrap().as_ref().unwrap().clone();
            let snapshot = published.current_snapshot().unwrap();
            let io = fixture.prepared.staged.table.file_io();
            let statistics = published.statistics_for_snapshot(snapshot.snapshot_id()).unwrap();
            let bodies = crate::stats_loader::StatsLoader::load_theta_bodies_from_file(
                &statistics.statistics_path,
                io,
            )
            .await
            .unwrap();
            assert_eq!(bodies.len(), 1);
            assert_eq!(bodies.get(&1).unwrap().as_slice(), statistics_body.as_ref());
            let list_bytes = io
                .new_input(snapshot.manifest_list())
                .unwrap()
                .read()
                .await
                .unwrap();
            let list = crate::iceberg::spec::ManifestList::parse_with_version(
                &list_bytes,
                FormatVersion::V3,
            )
            .unwrap();
            let mut observed = std::collections::BTreeSet::new();
            let mut rows = 0;
            for manifest in list.entries() {
                let bytes = io
                    .new_input(&manifest.manifest_path)
                    .unwrap()
                    .read()
                    .await
                    .unwrap();
                let decoded = crate::iceberg::spec::Manifest::parse_avro(&bytes).unwrap();
                for entry in decoded.entries() {
                    assert!(observed.insert(entry.data_file().file_path().to_owned()));
                    assert_eq!(entry.data_file().column_sizes().get(&1), Some(&24));
                    rows += entry.data_file().record_count();
                }
            }
            assert_eq!(observed, paths);
            assert_eq!(rows, 3000);
            for path in &paths {
                assert!(std::path::Path::new(path.strip_prefix("file://").unwrap()).exists());
            }
            assert!(
                fixture
                    .prepared
                    .publication
                    .as_ref()
                    .unwrap()
                    .artifacts()
                    .unwrap()
                    .is_empty()
            );
        });
        drop(server); // its owner joins every actual connection actor
    }
}
