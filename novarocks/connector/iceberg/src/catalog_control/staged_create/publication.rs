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
use crate::catalog::error::{CatalogCommitEvidence, CatalogOutcome};
use crate::catalog::transaction::CommitProof;
use crate::commit::attempt::observation::PublicationJournal;
use crate::commit::attempt::{self, Publisher, RetryPolicy};
use crate::commit::model::{
    AddedContent, ArtifactWriter, FileChanges, FrozenRequest, IsolationLevel, ObjectIdentity,
    OperationIntent, OperationIntentParts, OperationToken, RequestShape, StagedCreateIdentity,
    TableTarget,
};
use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation, OperationLimits};
use crate::commit::recovery::FrozenPublicationFacts;
use crate::commit::staging::{PreparedChange, Preparer, StagingBase};
use crate::iceberg::{Error, ErrorKind};
use async_trait::async_trait;
use novarocks_spi::connector::ExternalMutationOutcome;

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

fn create_intent(
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

fn create_prefix(prepared: &PreparedOperation) -> PreparedChange {
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

struct CreatePublisher {
    adapter: IcebergStagedCreateAdapter,
    prepared: PreparedOperation,
    operation_id: ConnectorStagedCreateOperationId,
    journal: PublicationJournal<ConnectorStagedCreateReceipt>,
    file_io: Mutex<Option<crate::iceberg::io::FileIO>>,
}

#[async_trait]
impl Publisher for CreatePublisher {
    async fn load_target(
        &self,
        attempt: &IcebergCommitAttempt,
    ) -> crate::iceberg::Result<StagingBase> {
        attempt.check_active()?;
        *self
            .file_io
            .lock()
            .map_err(|_| iceberg_error("Create request FileIO lock poisoned"))? =
            Some(attempt.file_io().clone());
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
        self.journal.record(evidence, receipt, request)
    }

    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
        let expected_snapshot_id = request.ref_snapshot_after("main");
        let file_io = match self.file_io.lock() {
            Ok(io) => match io.as_ref() {
                Some(io) => io.clone(),
                None => {
                    return CatalogOutcome::uncommitted(
                        ConnectorMutationFailureKind::Internal,
                        "Create dispatch has no admitted FileIO",
                    );
                }
            },
            Err(_) => {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::Internal,
                    "Create request FileIO lock poisoned",
                );
            }
        };
        // This is the original catalog-owner boundary. No frozen update is
        // appended here, and no SDK transaction predicts its metadata.
        let outcome = self
            .adapter
            .runtime
            .novarocks_catalog()
            .commit_staged_table(request.into_table_commit(), file_io)
            .await;
        let evidence = || {
            CatalogCommitEvidence::for_target(format!(
                "{}.{}",
                self.prepared
                    .staged
                    .table
                    .identifier()
                    .namespace
                    .to_url_string(),
                self.prepared.staged.table.identifier().name
            ))
        };
        use crate::catalog::StagedCommitResult;
        match outcome {
            StagedCommitResult::Committed(table)
                if publication_matches(
                    &table,
                    self.prepared.operation_id(),
                    &self.prepared,
                    expected_snapshot_id,
                ) =>
            {
                CatalogOutcome::committed(
                    CommitProof::applied(expected_snapshot_id)
                        .with_table_uuid(table.metadata().uuid().to_string()),
                    ExternalMutationEffect::Applied,
                )
            }
            StagedCommitResult::Committed(_) => CatalogOutcome::unknown(
                "REST response did not prove the exact staged-create publication",
                evidence(),
            ),
            StagedCommitResult::Conflict(message) => {
                CatalogOutcome::uncommitted(ConnectorMutationFailureKind::Conflict, message)
            }
            StagedCommitResult::KnownUncommitted(message) => {
                CatalogOutcome::uncommitted(ConnectorMutationFailureKind::Unavailable, message)
            }
            StagedCommitResult::CommitUnknown(message) => {
                CatalogOutcome::unknown(message, evidence())
            }
            StagedCommitResult::CommittedResponseInvalid(message) => {
                CatalogOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: CommitProof::applied(expected_snapshot_id)
                        .with_table_uuid(self.prepared.staged.table.metadata().uuid().to_string()),
                    finalization: cleanup_failed(format!(
                        "REST staged-create publication committed but response finalization failed: {message}"
                    )),
                }
            }
            StagedCommitResult::Unsupported(reason) => CatalogOutcome::uncommitted(
                ConnectorMutationFailureKind::Unsupported,
                reason.message(),
            ),
        }
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
    let journal = PublicationJournal::new(operation.token());
    let publisher = journal.observe(Arc::new(CreatePublisher {
        adapter: adapter.clone(),
        prepared: prepared.clone(),
        operation_id,
        journal: journal.clone(),
        file_io: Mutex::new(None),
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
                runtime,
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
            let initialization_updates = metadata.staged_create_initialization_updates().unwrap();
            let table = crate::iceberg::table::Table::builder()
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
                    prepared: fixture.prepared.clone(),
                    operation_id,
                    journal: journal.clone(),
                    file_io: Mutex::new(None),
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
                prepared: fixture.prepared.clone(),
                operation_id,
                journal: journal.clone(),
                file_io: Mutex::new(None),
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
