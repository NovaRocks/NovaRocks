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

use super::*;
use crate::commit::model::*;
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::*;
use crate::iceberg::{NamespaceIdent, TableIdent, TableRequirement, TableUpdate};
use novarocks_spi::connector::ConnectorWriteOperationId;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

fn token() -> OperationToken {
    OperationToken::from_write(ConnectorWriteOperationId::from_bytes([7; 16]))
}
fn uuid() -> Uuid {
    Uuid::from_bytes([1; 16])
}
fn fixture(version: FormatVersion) -> TableMetadata {
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
        "s3://bucket/table".to_owned(),
        version,
        HashMap::from([("owner".to_owned(), "test".to_owned())]),
    )
    .unwrap()
    .assign_uuid(uuid())
    .build()
    .unwrap()
    .metadata;
    let mut json = serde_json::to_value(metadata).unwrap();
    json["last-updated-ms"] = 1_700_000_000_000_i64.into();
    serde_json::from_value(json).unwrap()
}
fn intent(shape: RequestShape) -> OperationIntent {
    OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
            uuid: (shape != RequestShape::Create).then_some(uuid()),
        },
        target_ref: "main".into(),
        start: None,
        changes: FileChanges::default(),
        dependencies: vec![Dependency::NoReadDependency],
        isolation: IsolationLevel::Snapshot,
        shape,
        summary: BTreeMap::new(),
        token: token(),
    })
    .unwrap()
}
struct TestWriter {
    io: FileIO,
    attempt: AttemptToken,
    ledger: Mutex<ArtifactLedger>,
    directory: tempfile::TempDir,
}
impl TestWriter {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let runtime = tokio::runtime::Handle::current();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(runtime)),
        );
        let location = format!("file://{}", directory.path().display());
        Self {
            io: crate::fs_io::build_file_io_for_location(&location, binding),
            attempt: AttemptToken::new(token(), 0),
            ledger: Mutex::new(ArtifactLedger::new(token())),
            directory,
        }
    }
}
impl ArtifactWriter for TestWriter {
    fn file_io(&self) -> &FileIO {
        &self.io
    }
    fn allocate(
        &self,
        class: ArtifactClass,
        kind: ArtifactKind,
    ) -> crate::iceberg::Result<ObjectIdentity> {
        let mut ledger = self.ledger.lock().unwrap();
        let path = format!(
            "file://{}/object-{}.{}",
            self.directory.path().display(),
            ledger.records().count(),
            kind.extension()
        );
        let object = ObjectIdentity::new(path)?;
        ledger.register(ArtifactRecord {
            object: object.clone(),
            class,
            attempt: (class == ArtifactClass::Attempt).then_some(self.attempt),
            write_state: ArtifactWriteState::Allocated,
        })?;
        Ok(object)
    }
    fn attempt_token(&self) -> AttemptToken {
        self.attempt
    }
    fn check_active(&self) -> crate::iceberg::Result<()> {
        Ok(())
    }
    fn snapshot_artifacts(
        &self,
        references: &[ObjectIdentity],
    ) -> crate::iceberg::Result<AttemptArtifacts> {
        self.ledger
            .lock()
            .unwrap()
            .snapshot_for_attempt(self.attempt, references.iter().cloned())
    }
}
fn existing(metadata: TableMetadata) -> StagingBase {
    StagingBase::Existing {
        metadata,
        metadata_location:
            "s3://bucket/table/metadata/00000-00000000-0000-0000-0000-000000000001.metadata.json"
                .into(),
    }
}
fn change(updates: Vec<TableUpdate>) -> PreparedChange {
    PreparedChange {
        updates,
        requirements: vec![],
    }
}
fn append(metadata: &TableMetadata, id: i64) -> Vec<TableUpdate> {
    let snapshot = Snapshot::builder()
        .with_snapshot_id(id)
        .with_parent_snapshot_id(metadata.snapshot_for_ref("main").map(|s| s.snapshot_id()))
        .with_sequence_number(metadata.next_sequence_number())
        .with_timestamp_ms(1_700_000_000_000 + id)
        .with_manifest_list(format!("s3://bucket/table/metadata/snap-{id}.avro"))
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .with_schema_id(metadata.current_schema_id())
        .build();
    vec![
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: "main".into(),
            reference: SnapshotReference::new(
                id,
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            ),
        },
    ]
}
fn initialization(base: &TableMetadata) -> Vec<TableUpdate> {
    vec![
        TableUpdate::UpgradeFormatVersion {
            format_version: base.format_version(),
        },
        TableUpdate::AssignUuid { uuid: base.uuid() },
        TableUpdate::AddSchema {
            schema: base.current_schema().as_ref().clone(),
            last_column_id: None,
        },
        TableUpdate::SetCurrentSchema { schema_id: -1 },
        TableUpdate::AddSpec {
            spec: base
                .default_partition_spec()
                .as_ref()
                .clone()
                .into_unbound(),
        },
        TableUpdate::SetDefaultSpec { spec_id: -1 },
        TableUpdate::AddSortOrder {
            sort_order: base.default_sort_order().as_ref().clone(),
        },
        TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
        TableUpdate::SetLocation {
            location: base.location().into(),
        },
        TableUpdate::SetProperties {
            updates: base.properties().clone(),
        },
    ]
}
fn partitioned() -> UnboundPartitionSpec {
    serde_json::from_value(serde_json::json!({"spec-id": 1, "fields": [{"source-id":1,"field-id":1000,"name":"id_part","transform":"identity"}]})).unwrap()
}
fn facts(metadata: &TableMetadata) -> serde_json::Value {
    let mut value = serde_json::to_value(metadata).unwrap();
    for field in ["last-updated-ms", "snapshot-log", "metadata-log"] {
        value.as_object_mut().unwrap().remove(field);
    }
    for field in [
        "schemas",
        "partition-specs",
        "sort-orders",
        "snapshots",
        "statistics",
    ] {
        if let Some(array) = value[field].as_array_mut() {
            array.sort_by_key(|v| v.to_string());
        }
    }
    value
}

#[tokio::test]
async fn every_stage_equals_canonical_prefix_and_whole_request() {
    let base = fixture(FormatVersion::V2);
    let intent = intent(RequestShape::SnapshotProducing);
    let writer = TestWriter::new();
    let mut engine = StagingEngine::begin(existing(base.clone()), &intent, &writer).unwrap();
    engine
        .stage_change(change(vec![
            TableUpdate::AddSpec {
                spec: partitioned(),
            },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
        ]))
        .unwrap();
    assert_eq!(engine.metadata().default_partition_spec_id(), 1);
    assert!(engine.metadata().snapshot_for_ref("main").is_none());
    assert_eq!(
        facts(engine.metadata()),
        facts(&super::normalize::replay(&base, None, engine.updates()).unwrap())
    );
    engine
        .stage(&AppendObserver {
            expected_parent: None,
            expected_spec: 1,
            new_id: 11,
        })
        .await
        .unwrap();
    assert_eq!(
        engine
            .metadata()
            .snapshot_for_ref("main")
            .unwrap()
            .snapshot_id(),
        11
    );
    assert_eq!(
        facts(engine.metadata()),
        facts(&super::normalize::replay(&base, None, engine.updates()).unwrap())
    );
    let statistics = StatisticsFile {
        snapshot_id: 11,
        statistics_path: "s3://bucket/table/stats.puffin".into(),
        file_size_in_bytes: 100,
        file_footer_size_in_bytes: 20,
        key_metadata: None,
        blob_metadata: vec![],
    };
    engine
        .stage(&StatisticsObserver { statistics })
        .await
        .unwrap();
    assert_eq!(
        engine
            .metadata()
            .statistics_for_snapshot(11)
            .unwrap()
            .file_size_in_bytes,
        100
    );
    let staged = facts(engine.metadata());
    let request = engine.freeze(&[]).unwrap();
    assert_eq!(
        staged,
        facts(&super::normalize::replay(&base, None, request.updates()).unwrap())
    );
    assert!(
        request
            .requirements()
            .contains(&TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: base.last_partition_id()
            })
    );
    assert!(
        request
            .requirements()
            .contains(&TableRequirement::DefaultSpecIdMatch { default_spec_id: 0 })
    );
    let table = crate::iceberg::table::Table::builder()
        .identifier(intent.target().ident.clone())
        .metadata(base.clone())
        .file_io(writer.io.clone())
        .metadata_location(
            "s3://bucket/table/metadata/00000-00000000-0000-0000-0000-000000000001.metadata.json",
        )
        .build()
        .unwrap();
    let applied = request.into_table_commit().apply(table).unwrap();
    assert_eq!(staged, facts(applied.metadata()));
}

struct AppendObserver {
    expected_parent: Option<i64>,
    expected_spec: i32,
    new_id: i64,
}
#[async_trait::async_trait]
impl Preparer for AppendObserver {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        _intent: &OperationIntent,
    ) -> crate::iceberg::Result<PreparedChange> {
        assert_eq!(
            view.metadata().default_partition_spec_id(),
            self.expected_spec
        );
        assert_eq!(
            view.metadata()
                .snapshot_for_ref(view.target_ref())
                .map(|s| s.snapshot_id()),
            self.expected_parent
        );
        Ok(change(append(view.metadata(), self.new_id)))
    }
}
struct StatisticsObserver {
    statistics: StatisticsFile,
}
#[async_trait::async_trait]
impl Preparer for StatisticsObserver {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        _intent: &OperationIntent,
    ) -> crate::iceberg::Result<PreparedChange> {
        assert_eq!(view.metadata().default_partition_spec_id(), 1);
        assert_eq!(
            view.metadata()
                .snapshot_for_ref(view.target_ref())
                .unwrap()
                .snapshot_id(),
            self.statistics.snapshot_id
        );
        Ok(change(vec![TableUpdate::SetStatistics {
            statistics: self.statistics.clone(),
        }]))
    }
}
#[tokio::test]
async fn later_preparer_observes_prior_snapshot_and_cas_is_single_base_assertion() {
    let base = fixture(FormatVersion::V2);
    let intent = intent(RequestShape::SnapshotProducing);
    let writer = TestWriter::new();
    let mut engine = StagingEngine::begin(existing(base), &intent, &writer).unwrap();
    engine
        .stage(&AppendObserver {
            expected_parent: None,
            expected_spec: 0,
            new_id: 11,
        })
        .await
        .unwrap();
    engine
        .stage(&AppendObserver {
            expected_parent: Some(11),
            expected_spec: 0,
            new_id: 12,
        })
        .await
        .unwrap();
    let request = engine.freeze(&[]).unwrap();
    assert_eq!(request.ref_snapshot_after("main"), Some(12));
    let cas: Vec<_> = request
        .requirements()
        .iter()
        .filter(|r| matches!(r, TableRequirement::RefSnapshotIdMatch { .. }))
        .collect();
    assert_eq!(
        cas,
        vec![&TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".into(),
            snapshot_id: None
        }]
    );
}
#[tokio::test]
async fn historical_spec_and_equivalent_schema_reuse_emit_explicit_selectors() {
    let base = fixture(FormatVersion::V2);
    let intent = intent(RequestShape::SnapshotProducing);
    let writer = TestWriter::new();
    let mut engine = StagingEngine::begin(existing(base.clone()), &intent, &writer).unwrap();
    engine
        .stage_change(change(vec![
            TableUpdate::AddSpec {
                spec: partitioned(),
            },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
        ]))
        .unwrap();
    engine
        .stage_change(change(vec![
            TableUpdate::AddSpec {
                spec: base
                    .default_partition_spec()
                    .as_ref()
                    .clone()
                    .into_unbound(),
            },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
            TableUpdate::AddSchema {
                schema: {
                    let mut schema = serde_json::to_value(base.current_schema().as_ref()).unwrap();
                    schema["schema-id"] = 99.into();
                    serde_json::from_value(schema).unwrap()
                },
                last_column_id: Some(base.last_column_id()),
            },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
            TableUpdate::AddSortOrder {
                sort_order: SortOrder::unsorted_order(),
            },
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
        ]))
        .unwrap();
    assert_eq!(engine.metadata().default_partition_spec_id(), 0);
    assert_eq!(
        engine
            .updates()
            .iter()
            .filter(|u| matches!(u, TableUpdate::AddSpec { .. }))
            .count(),
        1
    );
    assert!(!engine.updates().iter().any(|u| matches!(
        u,
        TableUpdate::AddSchema { .. } | TableUpdate::AddSortOrder { .. }
    )));
    assert!(
        engine
            .updates()
            .contains(&TableUpdate::SetDefaultSpec { spec_id: 0 })
    );
    assert!(
        engine
            .updates()
            .contains(&TableUpdate::SetCurrentSchema { schema_id: 0 })
    );
}
#[tokio::test]
async fn stage_failure_preserves_the_previous_prefix() {
    let base = fixture(FormatVersion::V2);
    let intent = intent(RequestShape::MetadataOnly);
    let writer = TestWriter::new();
    let mut engine = StagingEngine::begin(existing(base.clone()), &intent, &writer).unwrap();
    assert!(
        engine
            .stage_change(change(vec![
                TableUpdate::SetProperties {
                    updates: HashMap::from([("x".into(), "y".into())])
                },
                TableUpdate::SetDefaultSpec { spec_id: -1 }
            ]))
            .is_err()
    );
    assert!(engine.updates().is_empty());
    assert_eq!(facts(engine.metadata()), facts(&base));
    assert!(engine.stage_change(change(append(&base, 11))).is_err());
    assert!(engine.updates().is_empty());
}
#[tokio::test]
async fn requirements_from_staged_schema_are_folded_to_parent_schema() {
    let base = fixture(FormatVersion::V2);
    let intent = intent(RequestShape::MetadataOnly);
    let writer = TestWriter::new();
    let mut engine = StagingEngine::begin(existing(base.clone()), &intent, &writer).unwrap();
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .unwrap();
    engine
        .stage_change(change(vec![
            TableUpdate::AddSchema {
                schema,
                last_column_id: Some(9),
            },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ]))
        .unwrap();
    assert_eq!(engine.metadata().last_column_id(), 9);
    let current_schema_id = engine.metadata().current_schema_id();
    assert_ne!(current_schema_id, 0);
    engine
        .stage_change(PreparedChange {
            updates: vec![],
            requirements: vec![
                TableRequirement::CurrentSchemaIdMatch { current_schema_id },
                TableRequirement::LastAssignedFieldIdMatch {
                    last_assigned_field_id: 9,
                },
            ],
        })
        .unwrap();
    let request = engine.freeze(&[]).unwrap();
    assert_eq!(
        request
            .requirements()
            .iter()
            .filter(|r| matches!(r, TableRequirement::CurrentSchemaIdMatch { .. }))
            .count(),
        1
    );
    assert!(
        request
            .requirements()
            .contains(&TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: 0
            })
    );
    assert!(
        request
            .requirements()
            .contains(&TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: 1
            })
    );
    assert!(
        !request
            .requirements()
            .iter()
            .any(|r| matches!(r, TableRequirement::RefSnapshotIdMatch { .. }))
    );
}

fn data(path: &str, count: u64, first_row_id: Option<i64>) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.into())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .record_count(count)
        .file_size_in_bytes(100)
        .first_row_id(first_row_id)
        .build()
        .unwrap()
}
#[test]
fn explicit_and_deleted_row_ids_do_not_consume_inheritance_cursor() {
    let mut cursor = FirstRowIdInheritance::new(Some(100));
    assert_eq!(
        cursor
            .resolve(&data("a", 10, None), ManifestStatus::Existing)
            .unwrap(),
        Some(100)
    );
    assert_eq!(
        cursor
            .resolve(&data("b", 30, Some(9)), ManifestStatus::Existing)
            .unwrap(),
        Some(9)
    );
    assert_eq!(
        cursor
            .resolve(&data("c", 40, None), ManifestStatus::Deleted)
            .unwrap(),
        None
    );
    assert_eq!(
        cursor
            .resolve(&data("d", 5, None), ManifestStatus::Existing)
            .unwrap(),
        Some(110)
    );
    assert_eq!(
        FirstRowIdInheritance::new(None)
            .resolve(&data("legacy", 5, None), ManifestStatus::Existing)
            .unwrap(),
        None
    );
}
fn frozen(file: &DataFile, first_row_id: Option<i64>) -> FrozenEntry {
    FrozenEntry::new(
        EntryIdentity::try_from(file).unwrap(),
        EntryFacts {
            added_snapshot_id: Some(1),
            data_sequence: Some(3),
            file_sequence: Some(4),
            partition_spec_id: 0,
            partition: file.partition().clone(),
            record_count: file.record_count(),
            first_row_id,
        },
    )
    .unwrap()
}
#[tokio::test]
async fn manifest_roundtrip_preserves_d12_sequences_and_carried_row_ids() {
    let metadata = fixture(FormatVersion::V3);
    let writer = TestWriter::new();
    let old = data("s3://bucket/old.parquet", 5, None);
    let manifest = write_manifest(
        &writer,
        FormatVersion::V3,
        11,
        metadata.current_schema().clone(),
        metadata.default_partition_spec().as_ref().clone(),
        ManifestContentType::Data,
        vec![
            ManifestEntryWrite::Added(
                AddedContent::new_logical_data(data("s3://bucket/new.parquet", 3, None), 0)
                    .unwrap(),
            ),
            ManifestEntryWrite::Added(
                AddedContent::rewritten_data(
                    data("s3://bucket/rewrite.parquet", 4, Some(80)),
                    0,
                    2,
                )
                .unwrap(),
            ),
            ManifestEntryWrite::Existing {
                file: old.clone(),
                frozen: frozen(&old, Some(100)),
            },
            ManifestEntryWrite::Deleted {
                file: old.clone(),
                frozen: frozen(&old, Some(100)),
            },
        ],
    )
    .await
    .unwrap();
    // Raw manifest has inherited fields; load_manifest would resolve them using the list.
    let bytes = writer
        .file_io()
        .new_input(&manifest.manifest_path)
        .unwrap()
        .read()
        .await
        .unwrap();
    let parsed = Manifest::parse_avro(&bytes).unwrap();
    let entries = parsed.entries();
    assert_eq!(entries[0].sequence_number, None);
    assert_eq!(entries[0].file_sequence_number, None);
    assert_eq!(entries[1].sequence_number, Some(2));
    assert_eq!(entries[1].file_sequence_number, None);
    assert_eq!(entries[2].sequence_number, Some(3));
    assert_eq!(entries[2].file_sequence_number, Some(4));
    assert_eq!(entries[2].data_file.first_row_id(), Some(100));
    assert_eq!(entries[2].snapshot_id, Some(1));
    assert_eq!(entries[3].status, ManifestStatus::Deleted);
    assert_eq!(entries[3].snapshot_id, Some(11));
    assert_eq!(entries[3].data_file.first_row_id(), Some(100));
}
#[tokio::test]
async fn manifest_list_allocates_historical_null_ranges_from_actual_writer_counter() {
    let metadata = fixture(FormatVersion::V3);
    let writer = TestWriter::new();
    let mut assigned = write_manifest(
        &writer,
        FormatVersion::V3,
        1,
        metadata.current_schema().clone(),
        metadata.default_partition_spec().as_ref().clone(),
        ManifestContentType::Data,
        vec![ManifestEntryWrite::Added(
            AddedContent::new_logical_data(data("s3://bucket/assigned", 7, None), 0).unwrap(),
        )],
    )
    .await
    .unwrap();
    assigned.first_row_id = Some(0);
    assigned.sequence_number = 1;
    assigned.min_sequence_number = 1;
    let mut json = serde_json::to_value(&metadata).unwrap();
    json["next-row-id"] = 10.into();
    json["last-sequence-number"] = 2.into();
    let metadata: TableMetadata = serde_json::from_value(json).unwrap();
    let mut historical = write_manifest(
        &writer,
        FormatVersion::V2,
        2,
        metadata.current_schema().clone(),
        metadata.default_partition_spec().as_ref().clone(),
        ManifestContentType::Data,
        vec![ManifestEntryWrite::Added(
            AddedContent::new_logical_data(data("s3://bucket/historical", 5, None), 0).unwrap(),
        )],
    )
    .await
    .unwrap();
    historical.sequence_number = 2;
    historical.min_sequence_number = 2;
    let new = write_manifest(
        &writer,
        FormatVersion::V3,
        11,
        metadata.current_schema().clone(),
        metadata.default_partition_spec().as_ref().clone(),
        ManifestContentType::Data,
        vec![ManifestEntryWrite::Added(
            AddedContent::new_logical_data(data("s3://bucket/new", 3, None), 0).unwrap(),
        )],
    )
    .await
    .unwrap();
    let output = write_manifest_list(
        &writer,
        &metadata,
        11,
        None,
        vec![assigned, historical, new],
    )
    .await
    .unwrap();
    assert_eq!(output.row_range, Some((10, 8)));
    let bytes = writer
        .file_io()
        .new_input(output.object.path())
        .unwrap()
        .read()
        .await
        .unwrap();
    let list = ManifestList::parse_with_version(&bytes, FormatVersion::V3).unwrap();
    assert_eq!(
        list.entries()
            .iter()
            .map(|m| m.first_row_id)
            .collect::<Vec<_>>(),
        vec![Some(0), Some(10), Some(15)]
    );
}
#[tokio::test]
async fn create_initialization_is_complete_and_only_asserts_create() {
    let base = fixture(FormatVersion::V2);
    let intent = intent(RequestShape::Create);
    let writer = TestWriter::new();
    let mut engine = StagingEngine::begin(
        StagingBase::Create {
            staged: StagedCreateIdentity::new(token(), Arc::new(base.clone())),
            initialization_updates: initialization(&base),
        },
        &intent,
        &writer,
    )
    .unwrap();
    engine
        .stage_change(change(append(engine.metadata(), 11)))
        .unwrap();
    let request = engine.freeze(&[]).unwrap();
    assert_eq!(request.requirements(), &[TableRequirement::NotExist]);
    assert_eq!(request.ref_snapshot_after("main"), Some(11));
    assert_eq!(
        request
            .updates()
            .iter()
            .filter(|u| matches!(u, TableUpdate::AddSchema { .. }))
            .count(),
        1
    );
    let mut incomplete = initialization(&base);
    incomplete.retain(|u| !matches!(u, TableUpdate::AddSpec { .. }));
    assert!(
        StagingEngine::begin(
            StagingBase::Create {
                staged: StagedCreateIdentity::new(token(), Arc::new(base)),
                initialization_updates: incomplete
            },
            &intent,
            &writer
        )
        .is_err()
    );
}
#[tokio::test]
async fn frozen_requests_match_three_stable_rest_goldens() {
    for shape in [
        RequestShape::SnapshotProducing,
        RequestShape::MetadataOnly,
        RequestShape::Create,
    ] {
        let base = fixture(FormatVersion::V2);
        let intent = intent(shape);
        let writer = TestWriter::new();
        let staging_base = if shape == RequestShape::Create {
            StagingBase::Create {
                staged: StagedCreateIdentity::new(token(), Arc::new(base.clone())),
                initialization_updates: initialization(&base),
            }
        } else {
            existing(base)
        };
        let mut engine = StagingEngine::begin(staging_base, &intent, &writer).unwrap();
        if shape == RequestShape::SnapshotProducing {
            let mut unbound = serde_json::to_value(partitioned()).unwrap();
            unbound["fields"][0]["field-id"] = serde_json::Value::Null;
            engine
                .stage_change(change(vec![
                    TableUpdate::AddSpec {
                        spec: serde_json::from_value(unbound).unwrap(),
                    },
                    TableUpdate::SetDefaultSpec { spec_id: -1 },
                ]))
                .unwrap();
        }
        if shape == RequestShape::MetadataOnly {
            engine
                .stage_change(change(vec![TableUpdate::SetProperties {
                    updates: HashMap::from([("analyzed".into(), "true".into())]),
                }]))
                .unwrap();
        } else {
            engine
                .stage_change(change(append(engine.metadata(), 11)))
                .unwrap();
        }
        let request = engine.freeze(&[]).unwrap();
        let json = request.to_rest_json().unwrap();
        let (name, golden) = match shape {
            RequestShape::SnapshotProducing => ("snapshot", include_str!("golden/snapshot.json")),
            RequestShape::MetadataOnly => ("metadata", include_str!("golden/metadata.json")),
            RequestShape::Create => ("create", include_str!("golden/create.json")),
        };
        let expected: serde_json::Value = serde_json::from_str(golden).unwrap();
        assert_eq!(
            json,
            expected,
            "REST {name} golden: {}",
            serde_json::to_string_pretty(&json).unwrap()
        );
        let restored_updates: Vec<TableUpdate> =
            serde_json::from_value(json["updates"].clone()).unwrap();
        assert_eq!(restored_updates, request.updates());
        assert_eq!(
            serde_json::to_string(&request.to_rest_json().unwrap()).unwrap(),
            serde_json::to_string(&json).unwrap()
        );
    }
}
#[test]
fn snapshot_ids_are_nonzero_unique_and_never_reuse_existing_snapshot() {
    let base = fixture(FormatVersion::V2);
    let base = super::normalize::replay(&base, None, &append(&base, 11)).unwrap();
    let ids: std::collections::HashSet<_> = (0..256).map(|_| new_snapshot_id(&base)).collect();
    assert_eq!(ids.len(), 256);
    assert!(!ids.contains(&0));
    assert!(!ids.contains(&11));
}

#[tokio::test]
async fn equivalent_schema_identifier_sets_are_independent_of_iteration_order() {
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                2,
                "key",
                Type::Primitive(PrimitiveType::Long),
            )),
        ])
        .with_identifier_field_ids([1, 2])
        .build()
        .unwrap();
    let base = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "s3://bucket/table".into(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .unwrap()
    .assign_uuid(uuid())
    .build()
    .unwrap()
    .metadata;
    let intent = intent(RequestShape::MetadataOnly);
    let writer = TestWriter::new();
    let mut engine = StagingEngine::begin(existing(base.clone()), &intent, &writer).unwrap();
    for _ in 0..16 {
        let mut json = serde_json::to_value(base.current_schema().as_ref()).unwrap();
        json["schema-id"] = 99.into();
        json["identifier-field-ids"] = serde_json::json!([2, 1]);
        let schema = serde_json::from_value(json).unwrap();
        engine
            .stage_change(change(vec![
                TableUpdate::AddSchema {
                    schema,
                    last_column_id: None,
                },
                TableUpdate::SetCurrentSchema { schema_id: -1 },
            ]))
            .unwrap();
    }
    assert_eq!(
        engine.metadata().current_schema_id(),
        base.current_schema_id()
    );
    assert_eq!(engine.metadata().schemas_iter().len(), 1);
    assert!(
        !engine
            .updates()
            .iter()
            .any(|u| matches!(u, TableUpdate::AddSchema { .. }))
    );
}

#[tokio::test]
async fn create_rejects_metadata_initialization_that_contradicts_authority() {
    let base = fixture(FormatVersion::V2);
    let intent = intent(RequestShape::Create);
    let writer = TestWriter::new();
    for corrupted in [
        TableUpdate::SetProperties {
            updates: HashMap::from([("owner".into(), "other".into())]),
        },
        TableUpdate::AddSpec {
            spec: partitioned(),
        },
    ] {
        let mut updates = initialization(&base);
        updates.push(corrupted);
        assert!(
            StagingEngine::begin(
                StagingBase::Create {
                    staged: StagedCreateIdentity::new(token(), Arc::new(base.clone())),
                    initialization_updates: updates
                },
                &intent,
                &writer
            )
            .is_err()
        );
    }
}

#[tokio::test]
async fn new_spec_binds_missing_field_ids_before_rest_freeze() {
    let base = fixture(FormatVersion::V2);
    let intent = intent(RequestShape::MetadataOnly);
    let writer = TestWriter::new();
    let mut engine = StagingEngine::begin(existing(base), &intent, &writer).unwrap();
    let mut unbound = serde_json::to_value(partitioned()).unwrap();
    unbound["fields"][0]["field-id"] = serde_json::Value::Null;
    engine
        .stage_change(change(vec![
            TableUpdate::AddSpec {
                spec: serde_json::from_value(unbound).unwrap(),
            },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
        ]))
        .unwrap();
    let request = engine.freeze(&[]).unwrap();
    let json = request.to_rest_json().unwrap();
    assert_eq!(
        json["updates"][0]["spec"]["fields"][0]["field-id"],
        serde_json::json!(1000)
    );
    assert_eq!(json["updates"][1]["spec-id"], serde_json::json!(1));
}
