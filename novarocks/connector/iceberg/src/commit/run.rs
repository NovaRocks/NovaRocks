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

//! Permanent publication tests and canonical snapshot seed helpers.

#[cfg(test)]
fn operation_for_test(
    runtime: &crate::metadata_context::IcebergMetadataContext,
    table: &crate::iceberg::table::Table,
) -> crate::iceberg::Result<super::operation::IcebergCommitOperation> {
    let context = novarocks_spi::connector::ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        2 * 1024 * 1024,
        4 * 1024 * 1024,
    )
    .map_err(|error| {
        crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, error.message())
    })?;
    super::operation::IcebergCommitOperation::new(
        super::model::OperationToken::from_write(
            novarocks_spi::connector::ConnectorWriteOperationId::new(),
        ),
        table.metadata().location(),
        runtime.resources().planning_binding().clone(),
        context,
        runtime.resources().catalog_runtime().clone(),
        super::operation::OperationLimits::default(),
    )
}

/// Seed test state through the same frozen request and catalog owner as a write.
#[cfg(test)]
pub(crate) async fn append_snapshot_for_test(
    runtime: std::sync::Arc<crate::metadata_context::IcebergMetadataContext>,
    table: crate::iceberg::table::Table,
    files: Vec<super::WrittenFile>,
    target_ref: String,
    summary: std::collections::BTreeMap<String, String>,
) -> crate::iceberg::Result<i64> {
    use super::attempt::{OwnerPublisher, Publisher, TransitionalPublisher};
    use super::model::{
        AddedContent, Dependency, FileChanges, IsolationLevel, OperationIntent,
        OperationIntentParts, RequestShape, StartSnapshot, TableTarget,
    };
    use super::staging::{StagingBase, StagingEngine};
    let operation = operation_for_test(runtime.as_ref(), &table)?;
    let attempt = operation.begin_attempt()?;
    let source = table.metadata();
    let intent = OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: table.identifier().clone(),
            uuid: Some(source.uuid()),
        },
        target_ref: target_ref.clone(),
        start: source
            .snapshot_for_ref(&target_ref)
            .map(|snapshot| StartSnapshot {
                snapshot_id: snapshot.snapshot_id(),
                sequence_number: snapshot.sequence_number(),
            }),
        changes: FileChanges {
            added: files
                .iter()
                .map(|file| {
                    AddedContent::new_logical_data(
                        super::data_file::from_written_file(file)?,
                        file.partition_spec_id,
                    )
                })
                .collect::<crate::iceberg::Result<Vec<_>>>()?,
            removed: Vec::new(),
        },
        dependencies: vec![Dependency::NoReadDependency],
        isolation: IsolationLevel::Snapshot,
        shape: RequestShape::SnapshotProducing,
        summary: summary.clone(),
        token: operation.token(),
    })?;
    let mut engine = StagingEngine::begin(
        StagingBase::Existing {
            metadata: source.clone(),
            metadata_location: table
                .metadata_location()
                .ok_or_else(|| {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Test seed table has no authoritative metadata location",
                    )
                })?
                .to_string(),
        },
        &intent,
        &attempt,
    )?;
    engine
        .stage(&super::fast_append::FastAppendPreparer)
        .await?;
    let snapshot_id = engine
        .metadata()
        .snapshot_for_ref(&target_ref)
        .ok_or_else(|| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "Test seed staging did not produce its target-ref snapshot",
            )
        })?
        .snapshot_id();
    let request = engine.freeze(&[])?;
    let publisher = OwnerPublisher {
        target: TransitionalPublisher {
            catalog: std::sync::Arc::clone(runtime.novarocks_catalog()),
            ident: table.identifier().clone(),
            target_ref,
            evidence: crate::catalog::error::CatalogCommitEvidence::for_target(
                table.identifier().to_string(),
            ),
            recovery_preflight: std::sync::Arc::new(|_, _| Ok(())),
        },
        operation: operation.token(),
        marker: summary
            .get(super::write_stack::control::ICEBERG_WRITE_SESSION_MARKER_PROPERTY)
            .map(|value| {
                (
                    std::sync::Arc::from(
                        super::write_stack::control::ICEBERG_WRITE_SESSION_MARKER_PROPERTY,
                    ),
                    std::sync::Arc::from(value.as_str()),
                )
            }),
    };
    match publisher.dispatch_once(request).await {
        crate::catalog::error::CatalogOutcome::KnownCommitted {
            effect: novarocks_spi::connector::ExternalMutationEffect::Applied,
            finalization: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            receipt,
        } if receipt.snapshot_id == Some(snapshot_id) => Ok(snapshot_id),
        outcome => Err(crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::Unexpected,
            format!("Canonical test seed was not proven committed: {outcome:?}"),
        )),
    }
}

#[cfg(test)]
async fn live_data_for_test(
    runtime: &crate::metadata_context::IcebergMetadataContext,
    table: &crate::iceberg::table::Table,
) -> crate::iceberg::Result<std::collections::BTreeMap<String, u64>> {
    let operation = operation_for_test(runtime, table)?;
    let attempt = operation.begin_attempt()?;
    let mut inputs = super::dependency::ValidationInputs::new(
        table.metadata(),
        table.metadata().current_snapshot_id(),
        &attempt,
    );
    Ok(inputs
        .live_set()
        .await?
        .values()
        .filter(|entry| entry.file.content_type() == crate::iceberg::spec::DataContentType::Data)
        .map(|entry| (entry.file.file_path().to_owned(), entry.file.record_count()))
        .collect())
}

#[cfg(test)]
mod application_document_publication_trace_tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field};
    use async_trait::async_trait;
    use bytes::Bytes;
    use novarocks_spi::connector::write_stack::session::{
        ConnectorWriteBeginRequest, ConnectorWriteControl, ConnectorWriteFinishPublication,
        ConnectorWriteFinishRequest, ConnectorWriteSessionFlavor,
        ConnectorWriteSessionReconcileRequest,
    };
    use novarocks_spi::connector::write_stack::{
        ConnectorManagedPublicationShape, ConnectorPreparedWriteSet,
    };
    use novarocks_spi::connector::{
        CatalogProperties, ConnectorControlBinding, ConnectorControlPlanningLease,
        ConnectorDocument, ConnectorDocumentAttachment, ConnectorDocumentFormat,
        ConnectorDocumentId, ConnectorDocumentManagementAdmissionRequest,
        ConnectorDocumentManagementOperation, ConnectorDocumentName, ConnectorDocumentOwner,
        ConnectorDocumentPublicationDeclaration, ConnectorDocumentPublicationIntent,
        ConnectorDocumentReference, ConnectorDocumentRevision, ConnectorDocumentSet,
        ConnectorDocumentStorageBinding, ConnectorDocumentStorageManagement,
        ConnectorManagedPartitionField, ConnectorManagedPartitionSpecObservation,
        ConnectorManagedPartitionSpecReplacement, ConnectorManagedPartitionTransform,
        ConnectorManagedPublicationEmptyInputDisposition, ConnectorManagedPublicationTechnique,
        ConnectorMutationOperationId, ConnectorPrepareDocumentsRequest,
        ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorTableIdentity,
        ConnectorTableObjectId, ConnectorWriteAdmissionPurpose, ConnectorWriteBaseVersion,
        ConnectorWriteFieldRequest, ConnectorWriteInputRequest, ConnectorWriteIntent,
        ConnectorWriteOperationId, ConnectorWriteTargetRef, ExternalMutationEffect,
        ExternalMutationFinalization, ExternalMutationOutcome, LakePublicationId,
        MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
    };

    use super::*;
    use crate::catalog::CatalogTableName;
    use crate::catalog::error::{CatalogCommitEvidence, CatalogOutcome};
    use crate::catalog::transaction::{
        CatalogCommitDispatch, CommitProof, TransactionIdentity, TransactionShape,
    };
    use crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_MARKER_PROPERTY;
    use crate::document_storage::envelope::{
        DOCUMENT_ENVELOPE_VERSION, DOCUMENT_MANIFEST_VERSION, IcebergDocumentAttachmentV1,
        IcebergDocumentCarrierV1, IcebergDocumentEnvelopeV1, IcebergDocumentManifestV1,
    };
    use crate::document_storage::publication::PENDING_DOCUMENT_MANIFEST_PROPERTY;
    use crate::iceberg::spec::{
        DataContentType, DataFileFormat, FormatVersion, NestedField, PrimitiveType, Schema, Struct,
        Type,
    };
    use crate::iceberg::table::Table;
    use crate::iceberg::{
        Catalog, Namespace, NamespaceIdent, TableCreation, TableIdent, TableRequirement,
        TableUpdate,
    };
    use uuid::Uuid;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RecordedCommit {
        requirement_kinds: Vec<&'static str>,
        uuid_requirements: Vec<Uuid>,
        ref_requirements: Vec<(String, Option<i64>)>,
        update_kinds: Vec<&'static str>,
        ref_updates: Vec<(String, i64)>,
    }

    #[derive(Debug)]
    struct RecordingCatalog {
        inner: Arc<dyn Catalog>,
        commits: Mutex<Vec<RecordedCommit>>,
    }

    impl RecordingCatalog {
        fn new(inner: Arc<dyn Catalog>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                commits: Mutex::new(Vec::new()),
            })
        }

        fn commits(&self) -> Vec<RecordedCommit> {
            self.commits.lock().expect("recording catalog lock").clone()
        }

        fn clear(&self) {
            self.commits.lock().expect("recording catalog lock").clear();
        }
    }

    #[async_trait]
    impl Catalog for RecordingCatalog {
        async fn list_namespaces(
            &self,
            parent: Option<&NamespaceIdent>,
        ) -> crate::iceberg::Result<Vec<NamespaceIdent>> {
            self.inner.list_namespaces(parent).await
        }

        async fn create_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> crate::iceberg::Result<Namespace> {
            self.inner.create_namespace(namespace, properties).await
        }

        async fn get_namespace(
            &self,
            namespace: &NamespaceIdent,
        ) -> crate::iceberg::Result<Namespace> {
            self.inner.get_namespace(namespace).await
        }

        async fn namespace_exists(
            &self,
            namespace: &NamespaceIdent,
        ) -> crate::iceberg::Result<bool> {
            self.inner.namespace_exists(namespace).await
        }

        async fn update_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> crate::iceberg::Result<()> {
            self.inner.update_namespace(namespace, properties).await
        }

        async fn drop_namespace(&self, namespace: &NamespaceIdent) -> crate::iceberg::Result<()> {
            self.inner.drop_namespace(namespace).await
        }

        async fn list_tables(
            &self,
            namespace: &NamespaceIdent,
        ) -> crate::iceberg::Result<Vec<TableIdent>> {
            self.inner.list_tables(namespace).await
        }

        async fn create_table(
            &self,
            namespace: &NamespaceIdent,
            creation: TableCreation,
        ) -> crate::iceberg::Result<Table> {
            self.inner.create_table(namespace, creation).await
        }

        async fn load_table(&self, table: &TableIdent) -> crate::iceberg::Result<Table> {
            self.inner.load_table(table).await
        }

        async fn drop_table(&self, table: &TableIdent) -> crate::iceberg::Result<()> {
            self.inner.drop_table(table).await
        }

        async fn table_exists(&self, table: &TableIdent) -> crate::iceberg::Result<bool> {
            self.inner.table_exists(table).await
        }

        async fn rename_table(
            &self,
            src: &TableIdent,
            dest: &TableIdent,
        ) -> crate::iceberg::Result<()> {
            self.inner.rename_table(src, dest).await
        }

        async fn register_table(
            &self,
            table: &TableIdent,
            metadata_location: String,
        ) -> crate::iceberg::Result<Table> {
            self.inner.register_table(table, metadata_location).await
        }

        async fn update_table(
            &self,
            mut commit: crate::iceberg::TableCommit,
        ) -> crate::iceberg::Result<Table> {
            let ident = commit.identifier().clone();
            let requirements = commit.take_requirements();
            let updates = commit.take_updates();
            let recorded = RecordedCommit {
                requirement_kinds: requirements.iter().map(requirement_kind).collect(),
                uuid_requirements: requirements
                    .iter()
                    .filter_map(|requirement| match requirement {
                        TableRequirement::UuidMatch { uuid } => Some(*uuid),
                        _ => None,
                    })
                    .collect(),
                ref_requirements: requirements
                    .iter()
                    .filter_map(|requirement| match requirement {
                        TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id } => {
                            Some((r#ref.clone(), *snapshot_id))
                        }
                        _ => None,
                    })
                    .collect(),
                update_kinds: updates.iter().map(update_kind).collect(),
                ref_updates: updates
                    .iter()
                    .filter_map(|update| match update {
                        TableUpdate::SetSnapshotRef {
                            ref_name,
                            reference,
                        } => Some((ref_name.clone(), reference.snapshot_id)),
                        _ => None,
                    })
                    .collect(),
            };
            self.commits
                .lock()
                .expect("recording catalog lock")
                .push(recorded);
            self.inner
                .update_table(
                    crate::iceberg::TableCommit::builder()
                        .ident(ident)
                        .requirements(requirements)
                        .updates(updates)
                        .build(),
                )
                .await
        }

        async fn create_view(
            &self,
            namespace: &NamespaceIdent,
            creation: crate::iceberg::ViewCreation,
        ) -> crate::iceberg::Result<crate::iceberg::spec::ViewMetadata> {
            self.inner.create_view(namespace, creation).await
        }

        async fn load_view(
            &self,
            view: &TableIdent,
        ) -> crate::iceberg::Result<crate::iceberg::spec::ViewMetadata> {
            self.inner.load_view(view).await
        }

        async fn update_view(
            &self,
            commit: crate::iceberg::ViewCommit,
        ) -> crate::iceberg::Result<crate::iceberg::spec::ViewMetadata> {
            self.inner.update_view(commit).await
        }

        async fn drop_view(&self, view: &TableIdent) -> crate::iceberg::Result<()> {
            self.inner.drop_view(view).await
        }

        async fn view_exists(&self, view: &TableIdent) -> crate::iceberg::Result<bool> {
            self.inner.view_exists(view).await
        }

        async fn list_views(
            &self,
            namespace: &NamespaceIdent,
        ) -> crate::iceberg::Result<Vec<TableIdent>> {
            self.inner.list_views(namespace).await
        }
    }

    fn requirement_kind(requirement: &TableRequirement) -> &'static str {
        match requirement {
            TableRequirement::NotExist => "assert-create",
            TableRequirement::UuidMatch { .. } => "assert-table-uuid",
            TableRequirement::RefSnapshotIdMatch { .. } => "assert-ref-snapshot-id",
            TableRequirement::LastAssignedFieldIdMatch { .. } => "assert-last-field-id",
            TableRequirement::CurrentSchemaIdMatch { .. } => "assert-current-schema-id",
            TableRequirement::LastAssignedPartitionIdMatch { .. } => "assert-last-partition-id",
            TableRequirement::DefaultSpecIdMatch { .. } => "assert-default-spec-id",
            TableRequirement::DefaultSortOrderIdMatch { .. } => "assert-default-sort-order-id",
        }
    }

    fn update_kind(update: &TableUpdate) -> &'static str {
        match update {
            TableUpdate::AddSpec { .. } => "add-spec",
            TableUpdate::SetDefaultSpec { .. } => "set-default-spec",
            TableUpdate::AddSnapshot { .. } => "add-snapshot",
            TableUpdate::SetSnapshotRef { .. } => "set-snapshot-ref",
            TableUpdate::SetProperties { .. } => "set-properties",
            TableUpdate::SetStatistics { .. } => "set-statistics",
            _ => "other",
        }
    }

    struct Fixture {
        catalog: Arc<RecordingCatalog>,
        table: Table,
        provider: crate::metadata::IcebergMetadata,
        catalog_handle: novarocks_spi::connector::CatalogHandle,
        _warehouse: tempfile::TempDir,
    }

    /// Hadoop intentionally does not grant document-management admission in
    /// production. This test capability replaces only the admission decision;
    /// the real Iceberg document preparation still produces the carrier that
    /// the real write session binds and commits.
    #[derive(Clone)]
    struct HadoopDocumentTestCapability {
        storage: Arc<crate::document_storage::IcebergDocumentStorage>,
    }

    impl ConnectorDocumentStorageManagement for HadoopDocumentTestCapability {
        fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
            self.storage.descriptor()
        }

        fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
            self.storage.incarnation()
        }

        fn admit_management(
            &self,
            request: ConnectorDocumentManagementAdmissionRequest,
        ) -> Result<Bytes, novarocks_spi::connector::ConnectorError> {
            let operation = match request.operation() {
                ConnectorDocumentManagementOperation::Create => "create",
                ConnectorDocumentManagementOperation::SingleTargetUpdate => "single-target-update",
                ConnectorDocumentManagementOperation::Publication => "publication",
                ConnectorDocumentManagementOperation::Drop => "drop",
            };
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "operation": operation,
                "operation_id": request.operation_id().to_bytes(),
                "namespace": request.target().namespace.as_ref(),
                "table": request.target().table.as_ref(),
                "expected_object_id": request
                    .expected_object_id()
                    .map(|object| object.as_bytes().to_vec()),
            }))
            .map(Bytes::from)
            .map_err(|error| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    format!("encode test document admission: {error}"),
                )
            })
        }

        fn prepare_documents(
            &self,
            request: ConnectorPrepareDocumentsRequest,
        ) -> Result<Bytes, novarocks_spi::connector::ConnectorError> {
            ConnectorDocumentStorageManagement::prepare_documents(self.storage.as_ref(), request)
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            2 * 1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("request context")
    }

    async fn fixture() -> Fixture {
        fixture_with_format(FormatVersion::V2, false).await
    }

    async fn row_lineage_fixture() -> Fixture {
        fixture_with_format(FormatVersion::V3, true).await
    }

    async fn fixture_with_format(format_version: FormatVersion, row_lineage: bool) -> Fixture {
        let warehouse = tempfile::tempdir().expect("warehouse tempdir");
        let warehouse_uri = format!("file://{}", warehouse.path().join("warehouse").display());
        let binding = {
            let runtime = tokio::runtime::Handle::current();
            crate::access_binding::IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
                Arc::new(novarocks_fs::TokioFileTaskSpawner::new(runtime)),
            )
        };
        let concrete = Arc::new(
            crate::hadoop_catalog::HadoopFileSystemCatalog::new_with_binding(
                crate::fs_io::build_file_io_for_location(&warehouse_uri, binding.clone()),
                warehouse_uri.clone(),
                binding.clone(),
            ),
        );
        let inner: Arc<dyn Catalog> = concrete.clone();
        let namespace = NamespaceIdent::new("db".to_string());
        inner
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let table = inner
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .properties(HashMap::from([
                        (
                            crate::document_storage::observation::MANAGED_KIND_PROPERTY.to_string(),
                            "mv".to_string(),
                        ),
                        (
                            crate::document_storage::observation::MANAGED_OWNER_PROPERTY
                                .to_string(),
                            "deployment".to_string(),
                        ),
                        (
                            crate::document_storage::observation::MANAGED_INCARNATION_PROPERTY
                                .to_string(),
                            "writer".to_string(),
                        ),
                        (
                            crate::stats_assembler::COLLECT_ON_WRITE_PROPERTY.to_string(),
                            "false".to_string(),
                        ),
                        ("write.row-lineage".to_string(), row_lineage.to_string()),
                    ]))
                    .format_version(format_version)
                    .build(),
            )
            .await
            .expect("create table");
        let catalog = RecordingCatalog::new(inner);
        let descriptor = novarocks_spi::connector::ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("provider id"),
            instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("ice")
                .expect("instance id"),
        };
        let incarnation = novarocks_spi::connector::ProviderBindingEpoch::from_bytes([6; 16]);
        let catalog_handle = novarocks_spi::connector::CatalogHandle::new(
            descriptor.instance_id.clone(),
            novarocks_spi::connector::CatalogVersion::from_bytes([17; 32]),
        );
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("catalog configuration");
        let resources = crate::resources::IcebergMetadataResources::new(
            binding,
            tokio::runtime::Handle::current(),
        );
        // Eager finish obtains a fresh provider-private transaction. Its
        // dispatch must share this same recording vendored client; wrapping
        // only the legacy `vendored_client()` getter would miss that mutation.
        let owner =
            crate::catalog::factory::NovaRocksCatalogFactory::adopt_recording_hadoop_for_test(
                concrete,
                catalog.clone(),
            );
        // These tests exercise publication protocol outcomes under an explicit
        // admitted owner, independent of Hadoop's product document policy.
        let owner = crate::catalog::admission_test_support::all_admitted(owner);
        let runtime = Arc::new(
            crate::metadata_context::IcebergMetadataContext::with_catalog_for_test(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                resources,
                owner,
            ),
        );
        let provider = crate::metadata::IcebergMetadata::new(descriptor, incarnation, runtime);
        Fixture {
            catalog,
            table,
            provider,
            catalog_handle,
            _warehouse: warehouse,
        }
    }

    fn document_storage_lease(
        fixture: &Fixture,
    ) -> novarocks_spi::connector::ConnectorDocumentStorageLease {
        let descriptor = fixture.provider.descriptor().clone();
        let incarnation = fixture.provider.incarnation();
        let storage = Arc::new(crate::document_storage::IcebergDocumentStorage::new(
            descriptor.clone(),
            incarnation,
            Arc::clone(fixture.provider.runtime()),
        ));
        let document_storage = ConnectorDocumentStorageBinding::try_new(
            descriptor.clone(),
            incarnation,
            Some(storage.clone()),
            Some(Arc::new(HadoopDocumentTestCapability { storage })),
        )
        .expect("document storage binding");
        let capability = Arc::new(fixture.provider.clone());
        let distribution = Arc::new(crate::provider_binding::IcebergInstanceDistribution::new(
            descriptor.clone(),
            incarnation,
        ));
        let binding = ConnectorControlBinding::try_new(
            descriptor.clone(),
            incarnation,
            capability.clone(),
            capability.clone(),
            distribution,
            Some(capability),
        )
        .and_then(|binding| {
            binding.with_catalog_properties(
                CatalogProperties::new(
                    fixture.catalog_handle.clone(),
                    descriptor.provider_id.clone(),
                    1,
                    Vec::new(),
                    Vec::new(),
                )
                .expect("catalog properties"),
            )
        })
        .and_then(|binding| binding.try_with_document_storage(Some(document_storage)))
        .expect("control binding");
        ConnectorControlPlanningLease::new(Arc::new(binding), || {})
            .derive_document_storage_lease()
            .expect("document storage lease")
    }

    struct PreparedPublication {
        declaration: ConnectorDocumentPublicationDeclaration,
        intent: ConnectorDocumentPublicationIntent,
        expected_manifest: Vec<u8>,
        base: ConnectorWriteBaseVersion,
        base_snapshot_id: Option<i64>,
    }

    fn prepare_publication(
        fixture: &Fixture,
        technique: ConnectorManagedPublicationTechnique,
        shape: ConnectorManagedPublicationShape,
        repartition: bool,
        content: Bytes,
        document_count: usize,
        reference_count: usize,
    ) -> PreparedPublication {
        let loaded = fixture
            .provider
            .runtime()
            .load_table("db", "t")
            .expect("load publication target");
        let metadata = loaded.table.metadata();
        let base_snapshot_id = metadata.current_snapshot_id();
        let table_uuid = metadata.uuid().to_string();
        let object_id =
            ConnectorTableObjectId::try_new(Bytes::copy_from_slice(table_uuid.as_bytes()))
                .expect("table object identity");
        let target = ConnectorTableIdentity {
            instance_id: fixture.provider.descriptor().instance_id.clone(),
            namespace: "db".into(),
            table: "t".into(),
        };
        let owner = ConnectorProviderBindingKey {
            instance_id: fixture.provider.descriptor().instance_id.clone(),
            incarnation: fixture.provider.incarnation(),
        };
        let publication_id = LakePublicationId::new_v7();
        let operation_id = ConnectorMutationOperationId::from_bytes(publication_id.to_bytes());
        let (partition_spec_replacement, expected_committed_partitioning) = if repartition {
            let prior = ConnectorManagedPartitionSpecObservation::try_from_fields(
                metadata.default_partition_spec_id(),
                &[],
            )
            .expect("prior unpartitioned observation");
            let replacement = ConnectorManagedPartitionSpecReplacement::try_new(
                ConnectorWriteOperationId::from_bytes(publication_id.to_bytes()),
                prior,
                vec![
                    ConnectorManagedPartitionField::try_new(
                        1,
                        0,
                        ConnectorManagedPartitionTransform::Identity,
                    )
                    .expect("identity partition field"),
                ],
            )
            .expect("partition replacement");
            let expected = crate::commit::write_stack::repartition::preview_managed_repartition(
                metadata,
                &replacement,
            )
            .expect("preview managed repartition")
            .committed()
            .clone();
            (Some(replacement), Some(expected))
        } else {
            (None, None)
        };
        let lease = document_storage_lease(fixture);
        let admission = lease
            .admit_management(
                ConnectorDocumentManagementAdmissionRequest::try_new(
                    owner,
                    fixture.catalog_handle.clone(),
                    operation_id,
                    target,
                    Some(object_id.clone()),
                    ConnectorDocumentManagementOperation::Publication,
                    context(),
                )
                .expect("publication admission request"),
            )
            .expect("publication admission");
        let mut publication_documents = (0..document_count)
            .map(|index| {
                let references = if index == 0 {
                    (0..reference_count)
                        .map(|reference| {
                            ConnectorDocumentReference::try_new(
                                format!("uses-{}-{reference:04}", "r".repeat(96)),
                                ConnectorDocumentId::new(
                                    ConnectorDocumentOwner::parse("novarocks.dependency")
                                        .expect("reference owner"),
                                    ConnectorDocumentName::parse(format!(
                                        "dependency-{}-{reference:04}",
                                        "n".repeat(90)
                                    ))
                                    .expect("reference name"),
                                    ConnectorDocumentRevision::from_bytes([reference as u8; 32]),
                                ),
                            )
                            .expect("document reference")
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                ConnectorDocument::try_new(
                    ConnectorDocumentOwner::parse("novarocks.mv").expect("document owner"),
                    ConnectorDocumentName::parse(format!("publication-{index}"))
                        .expect("document name"),
                    ConnectorDocumentFormat::try_new("novarocks.mv", "publication", 1)
                        .expect("document format"),
                    content.clone(),
                    references,
                    ConnectorDocumentAttachment::CommitOutput,
                )
                .expect("publication document")
            })
            .collect::<Vec<_>>();
        if repartition {
            publication_documents.push(
                ConnectorDocument::try_new(
                    ConnectorDocumentOwner::parse("novarocks.mv").expect("document owner"),
                    ConnectorDocumentName::parse("layout").expect("layout name"),
                    ConnectorDocumentFormat::try_new("novarocks.mv", "layout", 1)
                        .expect("layout format"),
                    Bytes::from_static(b"new-layout"),
                    Vec::new(),
                    ConnectorDocumentAttachment::TableMetadata,
                )
                .expect("layout document"),
            );
        }
        let documents =
            ConnectorDocumentSet::try_new(publication_documents).expect("publication document set");
        let prepared = lease
            .prepare_documents(
                ConnectorPrepareDocumentsRequest::try_new(admission.clone(), documents, context())
                    .expect("document preparation request"),
            )
            .expect("prepare publication documents");
        let expected_manifest = prepared.provider_token().to_vec();
        let base = ConnectorWriteBaseVersion::try_new(Bytes::from(format!(
            "iceberg/write-base/v1/{table_uuid}/main/{}",
            crate::commit::write_shared::snapshot_token(base_snapshot_id)
        )))
        .expect("publication base");
        let declaration = ConnectorDocumentPublicationDeclaration::try_new(
            publication_id,
            admission,
            object_id,
            base.clone(),
            technique,
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            partition_spec_replacement,
            expected_committed_partitioning,
        )
        .expect("publication declaration");
        let intent = ConnectorDocumentPublicationIntent::try_new(&declaration, prepared)
            .expect("publication intent");
        PreparedPublication {
            declaration,
            intent,
            expected_manifest,
            base,
            base_snapshot_id,
        }
    }

    fn data_input() -> ConnectorWriteInputRequest {
        ConnectorWriteInputRequest::Data {
            fields: vec![ConnectorWriteFieldRequest::new(Field::new(
                "id",
                DataType::Int64,
                false,
            ))],
        }
    }

    fn row_mutation_input() -> ConnectorWriteInputRequest {
        ConnectorWriteInputRequest::RowLineage {
            data_fields: vec![ConnectorWriteFieldRequest::new(Field::new(
                "id",
                DataType::Int64,
                false,
            ))],
            row_identity_fields: vec![
                ConnectorWriteFieldRequest::new(Field::new("_file", DataType::Utf8, false)),
                ConnectorWriteFieldRequest::new(Field::new("_pos", DataType::Int64, false)),
            ],
        }
    }

    fn begin_request(
        prepared: &PreparedPublication,
        shape: ConnectorManagedPublicationShape,
    ) -> ConnectorWriteBeginRequest {
        ConnectorWriteBeginRequest {
            table: Arc::from("db.t"),
            target_ref: ConnectorWriteTargetRef::main(),
            intent: match (prepared.declaration.technique(), shape) {
                (ConnectorManagedPublicationTechnique::Full, _) => ConnectorWriteIntent::Overwrite,
                (
                    ConnectorManagedPublicationTechnique::Incremental,
                    ConnectorManagedPublicationShape::RowMutation,
                ) => ConnectorWriteIntent::RowDelta,
                (ConnectorManagedPublicationTechnique::Incremental, _)
                | (ConnectorManagedPublicationTechnique::MetadataOnly, _) => {
                    ConnectorWriteIntent::Append
                }
            },
            purpose: ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            input: match shape {
                ConnectorManagedPublicationShape::RowMutation => row_mutation_input(),
                ConnectorManagedPublicationShape::Data
                | ConnectorManagedPublicationShape::InsertOnlyChangeStream => data_input(),
            },
            base: Some(prepared.base.clone()),
            flavor: ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
                declaration: prepared.declaration.clone(),
                shape,
            },
            context: context(),
        }
    }

    async fn seed_empty_snapshot(fixture: &Fixture) {
        append_snapshot_for_test(
            Arc::clone(fixture.provider.runtime()),
            fixture.table.clone(),
            Vec::new(),
            "main".into(),
            BTreeMap::from([(
                ICEBERG_WRITE_SESSION_MARKER_PROPERTY.to_string(),
                "row-mutation-base".to_string(),
            )]),
        )
        .await
        .expect("seed row-mutation base snapshot");
        fixture.catalog.clear();
        fixture
            .provider
            .runtime()
            .control_state()
            .invalidate_table_cache("db", "t");
    }

    fn prepared_snapshot_properties(label: &str) -> (BTreeMap<String, String>, Vec<u8>) {
        let content = label.as_bytes().to_vec();
        let manifest = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![IcebergDocumentEnvelopeV1 {
                version: DOCUMENT_ENVELOPE_VERSION,
                owner: "novarocks.mv".to_string(),
                name: "publication".to_string(),
                format_owner: "novarocks.mv".to_string(),
                format_name: "publication".to_string(),
                format_version: 1,
                revision: ConnectorDocumentRevision::for_content(&content).to_bytes(),
                encoded_len: content.len() as u64,
                references: Vec::new(),
                attachment: IcebergDocumentAttachmentV1::CommitOutput,
                carrier: IcebergDocumentCarrierV1::Available { content },
            }],
        };
        let encoded = crate::document_storage::codec::encode_document_manifest(&manifest)
            .expect("encode prepared document manifest")
            .to_vec();
        (
            BTreeMap::from([
                (
                    ICEBERG_WRITE_SESSION_MARKER_PROPERTY.to_string(),
                    format!("session-{label}"),
                ),
                (
                    PENDING_DOCUMENT_MANIFEST_PROPERTY.to_string(),
                    String::from_utf8(encoded.clone()).expect("UTF-8 document manifest"),
                ),
            ]),
            encoded,
        )
    }

    fn written_file(table: &Table, label: &str) -> crate::commit::WrittenFile {
        crate::commit::WrittenFile {
            path: format!("{}/data/{label}.parquet", table.metadata().location()),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: table.metadata().default_partition_spec_id(),
            record_count: 3,
            file_size_in_bytes: 128,
            split_offsets: Vec::new(),
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
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
        }
    }

    fn assert_one_snapshot_commit(
        catalog: &RecordingCatalog,
        expected_updates: &[&'static str],
        snapshot_id: i64,
    ) {
        let commits = catalog.commits();
        assert_eq!(commits.len(), 1, "publication must mutate its target once");
        assert_eq!(commits[0].update_kinds, expected_updates);
        assert_eq!(
            commits[0].ref_updates,
            vec![("main".to_string(), snapshot_id)]
        );
        assert!(commits[0].requirement_kinds.contains(&"assert-table-uuid"));
        assert!(
            commits[0]
                .requirement_kinds
                .contains(&"assert-ref-snapshot-id")
        );
    }

    async fn live_data_paths(fixture: &Fixture, table: &Table) -> Vec<String> {
        live_data_for_test(fixture.provider.runtime().as_ref(), table)
            .await
            .expect("observe canonical live data entries")
            .into_keys()
            .collect()
    }

    fn write_control(
        fixture: &Fixture,
    ) -> crate::commit::write_stack::control::IcebergWriteSessionControl {
        crate::commit::write_stack::control::IcebergWriteSessionControl::new(
            fixture.provider.descriptor().clone(),
            fixture.provider.incarnation(),
            fixture.catalog_handle.clone(),
            Arc::clone(fixture.provider.runtime()),
        )
    }

    fn iceberg_handle<'a>(
        fixture: &Fixture,
        plan: &'a novarocks_spi::connector::write_stack::session::ConnectorWriteSessionPlan,
    ) -> &'a crate::commit::write_stack::domain::IcebergCommitHandle {
        crate::commit::write_stack::runtime::build_write_adapter(
            fixture.provider.descriptor().clone(),
            fixture.catalog_handle.clone(),
        )
        .commit_handle(plan.commit_handle())
        .expect("Iceberg commit handle")
    }

    fn assert_exact_target_mutation(
        fixture: &Fixture,
        base_snapshot_id: Option<i64>,
        expected_updates: &[&'static str],
        committed_snapshot_id: i64,
    ) {
        let commits = fixture.catalog.commits();
        assert_eq!(commits.len(), 1, "publication must update_table once");
        assert_eq!(
            commits[0].uuid_requirements,
            vec![fixture.table.metadata().uuid()]
        );
        assert_eq!(
            commits[0].ref_requirements,
            vec![("main".to_string(), base_snapshot_id)]
        );
        assert_eq!(commits[0].update_kinds, expected_updates);
        assert_eq!(
            commits[0].ref_updates,
            vec![("main".to_string(), committed_snapshot_id)]
        );
    }

    async fn assert_exact_manifest(fixture: &Fixture, snapshot_id: i64, expected_manifest: &[u8]) {
        let table = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("reload document publication");
        crate::document_storage::publication::validate_expected_manifest(
            table.metadata(),
            snapshot_id,
            expected_manifest,
        )
        .expect("exact committed document manifest");
    }

    #[tokio::test]
    async fn real_full_overwrite_prepares_begins_and_finishes_one_exact_publication() {
        let fixture = fixture().await;
        // Long, unique references exercise a near-limit manifest without
        // inflating any one application document or relying on a sidecar.
        let prepared = prepare_publication(
            &fixture,
            ConnectorManagedPublicationTechnique::Full,
            ConnectorManagedPublicationShape::Data,
            false,
            Bytes::from_static(b"near-limit-publication"),
            1,
            2_500,
        );
        assert!(
            prepared.expected_manifest.len() > 900 * 1024,
            "prepared manifest was {} bytes",
            prepared.expected_manifest.len()
        );
        assert!(prepared.expected_manifest.len() < 1024 * 1024);
        let control = write_control(&fixture);
        let plan = control
            .begin_write(begin_request(
                &prepared,
                ConnectorManagedPublicationShape::Data,
            ))
            .expect("begin full document publication");
        let outcome = control
            .finish_write(ConnectorWriteFinishRequest {
                commit: plan.commit_handle(),
                prepared: ConnectorPreparedWriteSet::try_new(
                    0,
                    Vec::new(),
                    &plan.expected_targets(),
                )
                .expect("empty prepared write set"),
                statistics: Vec::new(),
                publication: ConnectorWriteFinishPublication::ApplicationDocuments(
                    prepared.intent.clone(),
                ),
                context: context(),
            })
            .expect("finish full document publication");
        let evidence = iceberg_handle(&fixture, &plan)
            .recovery_evidence()
            .expect("checked dispatch recovery evidence");
        assert!(evidence.provider_payload().len() < MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES);
        let evidence_payload =
            crate::commit::write_stack::control::decode_write_recovery_facts_for_test(&evidence)
                .expect("decode evidence payload");
        assert_eq!(
            evidence_payload["document_manifest_digest"]
                .as_array()
                .expect("document manifest digest")
                .len(),
            32
        );
        let ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization,
            ..
        } = outcome
        else {
            panic!("full publication must be known committed");
        };
        assert_eq!(finalization, ExternalMutationFinalization::Complete);
        assert_eq!(receipt.resulting_row_count(), Some(0));
        let snapshot_id = receipt
            .committed_version()
            .and_then(|version| version.snapshot_id())
            .expect("committed snapshot id");
        assert_exact_target_mutation(
            &fixture,
            prepared.base_snapshot_id,
            &["add-snapshot", "set-snapshot-ref"],
            snapshot_id,
        );
        assert_exact_manifest(&fixture, snapshot_id, &prepared.expected_manifest).await;

        let reconciled = control
            .reconcile_write(ConnectorWriteSessionReconcileRequest {
                commit: plan.commit_handle(),
                evidence,
                context: context(),
            })
            .expect("reconcile known full publication");
        let ExternalMutationOutcome::KnownCommitted {
            receipt: reconciled,
            finalization,
            ..
        } = reconciled
        else {
            panic!("known publication must reconcile idempotently");
        };
        assert_eq!(finalization, ExternalMutationFinalization::Complete);
        assert_eq!(reconciled.resulting_row_count(), Some(0));
        assert_eq!(
            reconciled
                .committed_version()
                .and_then(|version| version.snapshot_id()),
            Some(snapshot_id)
        );
        assert_eq!(fixture.catalog.commits().len(), 1);
    }

    #[tokio::test]
    async fn ordinary_zero_row_append_preserves_live_data_and_publishes_one_session_marker() {
        let fixture = fixture().await;
        append_snapshot_for_test(
            Arc::clone(fixture.provider.runtime()),
            fixture.table.clone(),
            vec![written_file(&fixture.table, "ordinary-zero-row-base")],
            "main".into(),
            BTreeMap::new(),
        )
        .await
        .expect("seed populated ordinary append target");
        fixture.catalog.clear();
        fixture
            .provider
            .runtime()
            .control_state()
            .invalidate_table_cache("db", "t");
        let before = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("load populated base");
        let base_snapshot_id = before.metadata().current_snapshot_id();
        let before_live = live_data_for_test(fixture.provider.runtime().as_ref(), &before)
            .await
            .expect("observe base live files");
        let control = write_control(&fixture);
        let plan = control
            .begin_write(ConnectorWriteBeginRequest {
                table: Arc::from("db.t"),
                target_ref: ConnectorWriteTargetRef::main(),
                intent: ConnectorWriteIntent::Append,
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                input: data_input(),
                base: None,
                flavor: ConnectorWriteSessionFlavor::Ordinary,
                context: context(),
            })
            .expect("begin zero-row ordinary append");
        let session_marker = iceberg_handle(&fixture, &plan).session_id().to_string();
        let outcome = control
            .finish_write(ConnectorWriteFinishRequest {
                commit: plan.commit_handle(),
                prepared: ConnectorPreparedWriteSet::try_new(
                    0,
                    Vec::new(),
                    &plan.expected_targets(),
                )
                .expect("complete empty write set"),
                statistics: Vec::new(),
                publication: ConnectorWriteFinishPublication::None,
                context: context(),
            })
            .expect("finish zero-row ordinary append");
        let ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        } = outcome
        else {
            panic!("ordinary append must publish its session marker");
        };
        let snapshot_id = receipt
            .committed_version()
            .and_then(|version| version.snapshot_id())
            .expect("ordinary append receipt snapshot");
        assert_ne!(Some(snapshot_id), base_snapshot_id);
        assert_eq!(receipt.resulting_row_count(), None);
        assert_eq!(before_live.values().sum::<u64>(), 3);
        assert_exact_target_mutation(
            &fixture,
            base_snapshot_id,
            &["add-snapshot", "set-snapshot-ref"],
            snapshot_id,
        );
        let after = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("load zero-row append result");
        let after_live = live_data_for_test(fixture.provider.runtime().as_ref(), &after)
            .await
            .expect("observe resulting live files");
        assert_eq!(after_live, before_live);
        assert_eq!(
            after
                .metadata()
                .current_snapshot()
                .expect("marker snapshot")
                .summary()
                .additional_properties
                .get(ICEBERG_WRITE_SESSION_MARKER_PROPERTY),
            Some(&session_marker),
        );
    }

    #[tokio::test]
    async fn real_incremental_row_mutation_prepares_begins_and_finishes_one_exact_publication() {
        let fixture = row_lineage_fixture().await;
        seed_empty_snapshot(&fixture).await;
        let prepared = prepare_publication(
            &fixture,
            ConnectorManagedPublicationTechnique::Incremental,
            ConnectorManagedPublicationShape::RowMutation,
            false,
            Bytes::from_static(b"incremental-row-mutation"),
            1,
            0,
        );
        // This is the same combined ManagedPublication /
        // ApplicationDocumentPublication match arm locked for the legacy
        // flavor by `a_change_stream_publication_freezes_the_old_deletes_it_supersedes`.
        // The existing old-delete tests additionally prove that the frozen
        // references must be merged exactly before a delete artifact commits.
        assert!(
            crate::commit::write_stack::control::session_freezes_old_deletes(
                &ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
                    declaration: prepared.declaration.clone(),
                    shape: ConnectorManagedPublicationShape::RowMutation,
                },
                &crate::commit::write_stack::test_support::merge_on_read_input_shape(),
            )
        );
        let control = write_control(&fixture);
        let plan = control
            .begin_write(begin_request(
                &prepared,
                ConnectorManagedPublicationShape::RowMutation,
            ))
            .expect("begin incremental row-mutation publication");
        let outcome = control
            .finish_write(ConnectorWriteFinishRequest {
                commit: plan.commit_handle(),
                prepared: ConnectorPreparedWriteSet::try_new(
                    0,
                    Vec::new(),
                    &plan.expected_targets(),
                )
                .expect("empty prepared row-mutation set"),
                statistics: Vec::new(),
                publication: ConnectorWriteFinishPublication::ApplicationDocuments(
                    prepared.intent.clone(),
                ),
                context: context(),
            })
            .expect("finish incremental row-mutation publication");
        let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
            panic!("incremental row-mutation publication must be known committed");
        };
        assert_eq!(receipt.resulting_row_count(), Some(0));
        let snapshot_id = receipt
            .committed_version()
            .and_then(|version| version.snapshot_id())
            .expect("committed snapshot id");
        assert_exact_target_mutation(
            &fixture,
            prepared.base_snapshot_id,
            &["add-snapshot", "set-snapshot-ref"],
            snapshot_id,
        );
        assert_exact_manifest(&fixture, snapshot_id, &prepared.expected_manifest).await;
    }

    #[tokio::test]
    async fn real_eager_repartition_prepares_begins_and_finishes_one_exact_publication() {
        let fixture = fixture().await;
        let prepared = prepare_publication(
            &fixture,
            ConnectorManagedPublicationTechnique::Full,
            ConnectorManagedPublicationShape::Data,
            true,
            Bytes::from_static(b"eager-repartition"),
            1,
            0,
        );
        let expected_partitioning = prepared
            .declaration
            .expected_committed_partitioning()
            .expect("expected committed partitioning")
            .clone();
        let control = write_control(&fixture);
        let plan = control
            .begin_write(begin_request(
                &prepared,
                ConnectorManagedPublicationShape::Data,
            ))
            .expect("begin eager repartition publication");
        let outcome = control
            .finish_write(ConnectorWriteFinishRequest {
                commit: plan.commit_handle(),
                prepared: ConnectorPreparedWriteSet::try_new(
                    0,
                    Vec::new(),
                    &plan.expected_targets(),
                )
                .expect("empty prepared repartition set"),
                statistics: Vec::new(),
                publication: ConnectorWriteFinishPublication::ApplicationDocuments(
                    prepared.intent.clone(),
                ),
                context: context(),
            })
            .expect("finish eager repartition publication");
        let evidence = iceberg_handle(&fixture, &plan)
            .recovery_evidence()
            .expect("checked dispatch recovery evidence");
        let ExternalMutationOutcome::KnownCommitted { receipt, .. } = outcome else {
            panic!("eager repartition publication must be known committed");
        };
        assert_eq!(receipt.resulting_row_count(), Some(0));
        assert_eq!(
            receipt.committed_partitioning(),
            Some(&expected_partitioning)
        );
        let snapshot_id = receipt
            .committed_version()
            .and_then(|version| version.snapshot_id())
            .expect("committed snapshot id");
        assert_exact_target_mutation(
            &fixture,
            prepared.base_snapshot_id,
            &[
                "add-spec",
                "set-default-spec",
                "set-properties",
                "add-snapshot",
                "set-snapshot-ref",
            ],
            snapshot_id,
        );
        assert_exact_manifest(&fixture, snapshot_id, &prepared.expected_manifest).await;

        let committed_table = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("reload repartition documents");
        let projected = crate::document_storage::observation::project_documents(
            committed_table.metadata(),
            novarocks_spi::connector::ConnectorDocumentStorageLimits::spec_default(),
        )
        .expect("project one layout and one publication without duplicate identities");
        assert_eq!(projected.len(), 2);

        let reconciled = control
            .reconcile_write(ConnectorWriteSessionReconcileRequest {
                commit: plan.commit_handle(),
                evidence,
                context: context(),
            })
            .expect("reconcile known repartition publication");
        let ExternalMutationOutcome::KnownCommitted {
            receipt: reconciled,
            finalization,
            ..
        } = reconciled
        else {
            panic!("known repartition must reconcile idempotently");
        };
        assert_eq!(finalization, ExternalMutationFinalization::Complete);
        assert_eq!(reconciled.resulting_row_count(), Some(0));
        assert_eq!(
            reconciled.committed_partitioning(),
            Some(&expected_partitioning)
        );
        assert_eq!(fixture.catalog.commits().len(), 1);
    }

    #[tokio::test]
    async fn normal_document_publication_is_one_exact_main_commit() {
        let fixture = fixture().await;
        let file = written_file(&fixture.table, "normal");
        let (properties, unresolved) = prepared_snapshot_properties("normal");
        let snapshot_id = append_snapshot_for_test(
            Arc::clone(fixture.provider.runtime()),
            fixture.table.clone(),
            vec![file],
            "main".into(),
            properties,
        )
        .await
        .expect("normal document publication");

        assert_one_snapshot_commit(
            &fixture.catalog,
            &["add-snapshot", "set-snapshot-ref"],
            snapshot_id,
        );
        let table = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("reload normal publication");
        crate::document_storage::publication::validate_expected_manifest(
            table.metadata(),
            snapshot_id,
            &unresolved,
        )
        .expect("exact normal document attachment");
    }

    #[derive(Debug)]
    struct RecordingUpdateDispatch {
        catalog: Arc<RecordingCatalog>,
    }

    #[async_trait]
    impl CatalogCommitDispatch for RecordingUpdateDispatch {
        async fn dispatch_once(
            &self,
            staged: Option<&crate::commit::model::FrozenRequest>,
        ) -> crate::iceberg::Result<CommitProof> {
            let staged = staged.expect("eager publication must stage a commit");
            let expected_snapshot = staged.ref_snapshot_after("main");
            let table = self
                .catalog
                .update_table(staged.into_table_commit())
                .await?;
            Ok(CommitProof::applied(expected_snapshot)
                .with_table_uuid(table.metadata().uuid().to_string()))
        }

        async fn adjudicate(
            &self,
        ) -> Result<Option<CommitProof>, novarocks_spi::connector::ConnectorError> {
            Ok(None)
        }

        async fn abort_before_dispatch(
            &self,
        ) -> Result<(), novarocks_spi::connector::ConnectorError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn eager_document_publication_is_one_exact_main_commit() {
        let fixture = fixture().await;
        let file = written_file(&fixture.table, "eager");
        use crate::commit::model::{
            AddedContent, FileChanges, IsolationLevel, OperationIntent, OperationIntentParts,
            OperationToken, RequestShape, TableTarget,
        };
        use crate::commit::operation::{IcebergCommitOperation, OperationLimits};
        use crate::commit::staging::{StagingBase, StagingEngine};
        let (properties, unresolved) = prepared_snapshot_properties("eager");
        let operation = IcebergCommitOperation::new(
            OperationToken::from_mutation(ConnectorMutationOperationId::from_bytes([7; 16])),
            fixture.table.metadata().location(),
            fixture
                .provider
                .runtime()
                .resources()
                .planning_binding()
                .clone(),
            context(),
            fixture
                .provider
                .runtime()
                .resources()
                .catalog_runtime()
                .clone(),
            OperationLimits::default(),
        )
        .unwrap();
        let attempt = operation.begin_attempt().unwrap();
        let intent = OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: fixture.table.identifier().clone(),
                uuid: Some(fixture.table.metadata().uuid()),
            },
            target_ref: "main".into(),
            start: None,
            changes: FileChanges {
                added: vec![
                    AddedContent::new_logical_data(
                        crate::commit::data_file::from_written_file(&file).unwrap(),
                        file.partition_spec_id,
                    )
                    .unwrap(),
                ],
                removed: vec![],
            },
            dependencies: vec![crate::commit::model::Dependency::NoReadDependency],
            isolation: IsolationLevel::Snapshot,
            shape: RequestShape::SnapshotProducing,
            summary: properties,
            token: operation.token(),
        })
        .unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: fixture.table.metadata().clone(),
                metadata_location: fixture.table.metadata_location().unwrap().to_string(),
            },
            &intent,
            &attempt,
        )
        .unwrap();
        engine
            .stage(&crate::commit::fast_append::FastAppendPreparer)
            .await
            .unwrap();
        let snapshot_id = engine
            .metadata()
            .snapshot_for_ref("main")
            .unwrap()
            .snapshot_id();
        let staged = engine.freeze(&[]).unwrap();
        let mut frontier = crate::catalog::transaction::Transaction::new(
            TransactionIdentity::new("document-publication-test", [7; 16]),
            CatalogTableName::new("db", "t"),
            TransactionShape::Existing,
            CatalogCommitEvidence::for_target("db.t"),
            Arc::new(RecordingUpdateDispatch {
                catalog: Arc::clone(&fixture.catalog),
            }),
        );
        frontier
            .stage(staged)
            .expect("stage eager catalog frontier");
        assert!(matches!(
            frontier.commit().await,
            CatalogOutcome::KnownCommitted { .. }
        ));

        assert_one_snapshot_commit(
            &fixture.catalog,
            &["add-snapshot", "set-snapshot-ref"],
            snapshot_id,
        );
        let table = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("reload eager publication");
        crate::document_storage::publication::validate_expected_manifest(
            table.metadata(),
            snapshot_id,
            &unresolved,
        )
        .expect("exact eager document attachment");
    }

    #[tokio::test]
    async fn empty_document_publication_keeps_data_files_and_commits_one_new_snapshot() {
        let fixture = fixture().await;
        let seed_file = written_file(&fixture.table, "seed");
        let seed_properties = BTreeMap::from([(
            ICEBERG_WRITE_SESSION_MARKER_PROPERTY.to_string(),
            "seed".to_string(),
        )]);
        append_snapshot_for_test(
            Arc::clone(fixture.provider.runtime()),
            fixture.table.clone(),
            vec![seed_file],
            "main".into(),
            seed_properties,
        )
        .await
        .expect("seed populated table");
        let populated = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("reload populated table");
        let before_paths = live_data_paths(&fixture, &populated).await;
        fixture
            .catalog
            .commits
            .lock()
            .expect("recording catalog lock")
            .clear();

        let (properties, unresolved) = prepared_snapshot_properties("empty");
        let snapshot_id = append_snapshot_for_test(
            Arc::clone(fixture.provider.runtime()),
            populated.clone(),
            Vec::new(),
            "main".into(),
            properties,
        )
        .await
        .expect("empty document publication");

        assert_one_snapshot_commit(
            &fixture.catalog,
            &["add-snapshot", "set-snapshot-ref"],
            snapshot_id,
        );
        let table = fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .expect("reload empty publication");
        assert_eq!(live_data_paths(&fixture, &table).await, before_paths);
        crate::document_storage::publication::validate_expected_manifest(
            table.metadata(),
            snapshot_id,
            &unresolved,
        )
        .expect("exact empty document attachment");
    }
}
