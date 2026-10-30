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
    provider: crate::metadata::IcebergMetadata,
    table: crate::iceberg::table::Table,
    files: Vec<super::WrittenFile>,
    target_ref: String,
    summary: std::collections::BTreeMap<String, String>,
) -> crate::iceberg::Result<i64> {
    let contents = files
        .iter()
        .map(|file| {
            super::model::AddedContent::new_logical_data(
                super::data_file::from_written_file(file)?,
                file.partition_spec_id,
            )
        })
        .collect::<crate::iceberg::Result<Vec<_>>>()?;
    append_contents_for_test(provider, table, contents, target_ref, summary).await
}

/// Seed exact caller-authored logical data through normal staging and the
/// prepared owner. Each item carries its original explicit partition spec.
#[cfg(test)]
pub(crate) async fn append_contents_for_test(
    provider: crate::metadata::IcebergMetadata,
    table: crate::iceberg::table::Table,
    contents: Vec<super::model::AddedContent>,
    target_ref: String,
    summary: std::collections::BTreeMap<String, String>,
) -> crate::iceberg::Result<i64> {
    use super::model::{
        Dependency, FileChanges, IsolationLevel, OperationIntent, OperationIntentParts,
        RequestShape, StartSnapshot, TableTarget,
    };
    use super::staging::{StagingBase, StagingEngine};
    let runtime = provider.runtime();
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
            added: contents,
            removed: Vec::new(),
        },
        dependencies: vec![Dependency::NoRead],
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
    let outcome = crate::catalog::admission_test_support::publish_frozen(
        std::sync::Arc::clone(runtime.novarocks_catalog()),
        provider.descriptor().clone(),
        provider.incarnation(),
        request,
        crate::catalog::admission::CatalogOperation::Append,
        operation.request_context().clone(),
        None,
    )
    .await;
    match outcome {
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
mod keyed_publication_trace_tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field};
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
        ConnectorDocumentFormat, ConnectorDocumentManagementAdmissionRequest,
        ConnectorDocumentManagementOperation, ConnectorDocumentPublicationDeclaration,
        ConnectorDocumentStorageBinding, ConnectorDocumentStorageManagement,
        ConnectorManagedPartitionField, ConnectorManagedPartitionSpecObservation,
        ConnectorManagedPartitionSpecReplacement, ConnectorManagedPartitionTransform,
        ConnectorManagedPublicationEmptyInputDisposition, ConnectorManagedPublicationTechnique,
        ConnectorMutationOperationId, ConnectorProviderBindingKey, ConnectorRequestContext,
        ConnectorTableIdentity, ConnectorTableObjectId, ConnectorWriteAdmissionPurpose,
        ConnectorWriteBaseVersion, ConnectorWriteFieldRequest, ConnectorWriteInputRequest,
        ConnectorWriteIntent, ConnectorWriteOperationId, ConnectorWriteTargetRef,
        ExternalMutationEffect, ExternalMutationFinalization, ExternalMutationOutcome,
        LakePublicationId,
    };

    use super::*;
    use crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_MARKER_PROPERTY;
    use crate::iceberg::spec::{
        DataContentType, DataFileFormat, FormatVersion, NestedField, PrimitiveType, Schema, Struct,
        Type,
    };
    use crate::iceberg::table::Table;
    use crate::iceberg::{Catalog, TableCreation, TableIdent, TableRequirement, TableUpdate};
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
        raw_updates: Mutex<Vec<Vec<TableUpdate>>>,
        next_error: Arc<Mutex<Option<crate::iceberg::ErrorKind>>>,
    }

    impl RecordingCatalog {
        fn new(inner: Arc<dyn Catalog>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                commits: Mutex::new(Vec::new()),
                raw_updates: Mutex::new(Vec::new()),
                next_error: Arc::new(Mutex::new(None)),
            })
        }

        fn commits(&self) -> Vec<RecordedCommit> {
            self.commits.lock().expect("recording catalog lock").clone()
        }

        fn record_frozen(&self, requirements: &[TableRequirement], updates: &[TableUpdate]) {
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
            self.raw_updates.lock().unwrap().push(updates.to_vec());
        }

        fn clear(&self) {
            self.commits.lock().expect("recording catalog lock").clear();
            self.raw_updates.lock().unwrap().clear();
        }

        fn raw_updates(&self) -> Vec<Vec<TableUpdate>> {
            self.raw_updates.lock().unwrap().clone()
        }

        fn fail_next(&self, kind: crate::iceberg::ErrorKind) {
            assert!(self.next_error.lock().unwrap().replace(kind).is_none());
        }
    }

    impl RecordingCatalog {
        async fn load_table(&self, table: &TableIdent) -> crate::iceberg::Result<Table> {
            self.inner.load_table(table).await
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
        load_modes: Arc<Mutex<Vec<crate::catalog::SnapshotLoading>>>,
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
                crate::resources::IcebergCatalogRuntime::new(tokio::runtime::Handle::current())
                    .iceberg_runtime()
                    .clone(),
            ),
        );
        let inner: Arc<dyn Catalog> = concrete.clone();
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
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
        // Record only a real prepared dispatch after its journal grant.
        // SDK reads and fixture setup never count as publication.
        let owner =
            crate::catalog::factory::NovaRocksCatalogFactory::adopt_hadoop_for_test(concrete);
        // These tests exercise publication protocol outcomes under an explicit
        // admitted owner, independent of Hadoop's product document policy.
        let load_modes = Arc::new(Mutex::new(Vec::new()));
        let recording = catalog.clone();
        let owner = crate::catalog::admission_test_support::all_admitted_recording_with_snapshot_trace_and_dispatch_fault(
            owner,
            Arc::new(move |requirements, updates| recording.record_frozen(requirements, updates)),
            Arc::clone(&load_modes),
            Arc::clone(&catalog.next_error),
        );
        let runtime = Arc::new(
            crate::metadata_context::IcebergMetadataContext::with_catalog_for_test(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                resources,
                owner,
            ),
        );
        let table = crate::catalog::admission_test_support::create_static_fixture_table(
            &runtime,
            incarnation,
            crate::catalog::CatalogNamespaceName::new("db"),
            TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .properties(HashMap::from([
                    (
                        crate::stats_assembler::COLLECT_ON_WRITE_PROPERTY.to_string(),
                        "false".to_string(),
                    ),
                    ("write.row-lineage".to_string(), row_lineage.to_string()),
                ]))
                .format_version(format_version)
                .build(),
        )
        .await;
        let provider = crate::metadata::IcebergMetadata::new(descriptor, incarnation, runtime);
        Fixture {
            load_modes,
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

    fn data_input() -> ConnectorWriteInputRequest {
        ConnectorWriteInputRequest::Data {
            fields: vec![
                ConnectorWriteFieldRequest::try_new_typed(
                    Field::new("id", DataType::Int64, false),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                )
                .expect("publication caller declares required Int64 id"),
            ],
        }
    }

    fn row_mutation_input() -> ConnectorWriteInputRequest {
        ConnectorWriteInputRequest::RowLineage {
            data_fields: vec![
                ConnectorWriteFieldRequest::try_new_typed(
                    Field::new("id", DataType::Int64, false),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                )
                .expect("publication caller declares required Int64 id"),
            ],
            row_identity_fields: vec![
                ConnectorWriteFieldRequest::try_new_typed(
                    Field::new("_file", DataType::Utf8, false),
                    novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
                )
                .expect("publication caller declares required file identity"),
                ConnectorWriteFieldRequest::try_new_typed(
                    Field::new("_pos", DataType::Int64, false),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                )
                .expect("publication caller declares required position identity"),
            ],
        }
    }

    async fn seed_empty_snapshot(fixture: &Fixture) {
        append_snapshot_for_test(
            fixture.provider.clone(),
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
        assert_eq!(
            commits.len(),
            1,
            "publication must dispatch one frozen request"
        );
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

    #[tokio::test]
    async fn ordinary_zero_row_append_preserves_live_data_and_publishes_one_session_marker() {
        let fixture = fixture().await;
        append_snapshot_for_test(
            fixture.provider.clone(),
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
            cleanup: _,
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
        } = outcome
        else {
            panic!("ordinary append must publish its session marker: {outcome:?}");
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

    mod keyed_session_tests {
        use super::*;
        include!("run/keyed_session_tests.rs");
    }
}
