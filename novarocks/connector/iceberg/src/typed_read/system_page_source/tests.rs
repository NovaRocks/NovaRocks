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

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use arrow::array::{ListArray, MapArray};
use novarocks_fs::{
    FileCancellation, FileIoRuntime, FileIoSpawner, FsAccessResolver, TokioFileIoRuntime,
    TokioFileIoSpawner,
};
use novarocks_spi::connector::read_stack::SchemaTableName;

use crate::iceberg::spec::{
    DataFileBuilder, DataFileFormat, FormatVersion, ManifestListWriter, ManifestWriterBuilder,
    NestedField, Operation, PartitionSpec, Snapshot, SnapshotReference, SortOrder, Struct,
    Summary, TableMetadataBuilder, Transform,
};
use crate::typed_read::system_table::{
    IcebergSystemTableReferenceParams, SystemManifestSplitParams, TrinoManifestFileParams,
};
use crate::typed_read::test_spawners::{GatedFileIoSpawner, route_reads_through};
use crate::typed_read::test_streams::DrivenStream;

use super::*;

const SNAPSHOT_ID: i64 = 100;
const SNAPSHOT_SEQUENCE_NUMBER: i64 = 3;

fn runtime_and_binding() -> (tokio::runtime::Runtime, IcebergReadBinding, FileReadContext) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let file_runtime: Arc<dyn FileIoRuntime> =
        Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
    let io_spawner: Arc<dyn FileIoSpawner> =
        Arc::new(TokioFileIoSpawner::new(runtime.handle().clone()));
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::clone(&file_runtime),
        novarocks_fs::TokioRefreshExecutor::shared(runtime.handle().clone()),
        Arc::clone(&io_spawner),
    );
    let context = FileReadContext {
        cancellation: FileCancellation::new(),
        deadline: Some(Instant::now() + Duration::from_secs(60)),
        runtime: file_runtime,
        io_spawner,
        range: None,
    };
    (runtime, binding, context)
}

fn provider(
    binding: &IcebergReadBinding,
    context: &FileReadContext,
) -> IcebergSystemTableProvider {
    IcebergSystemTableProvider::new(
        binding.clone(),
        context.clone(),
        NonZeroUsize::new(1024).expect("nonzero"),
    )
}

// -- pure schema fixtures, no I/O -------------------------------------

fn test_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "region",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .expect("schema")
}

fn identity_spec(schema: &Schema, spec_id: i32, column: &str) -> PartitionSpec {
    named_identity_spec(schema, spec_id, column, column)
}

fn named_identity_spec(
    schema: &Schema,
    spec_id: i32,
    column: &str,
    partition_name: &str,
) -> PartitionSpec {
    PartitionSpec::builder(Arc::new(schema.clone()))
        .with_spec_id(spec_id)
        .add_partition_field(column, partition_name, Transform::Identity)
        .expect("partition field")
        .build()
        .expect("partition spec")
}

fn column_names(schema: &SchemaRef) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

#[test]
fn files_frozen_native_schema_round_trip_keeps_canonical_list_child_identity() {
    let schema = Arc::new(ArrowSchema::new(files_fields(None, None)));
    let authored = metadata_relation_row(&schema).unwrap();
    let json = type_to_json(&authored).unwrap();
    let decoded =
        super::super::column_handle::parse_type(&json, "metadata_table_schema_json").unwrap();
    assert_eq!(decoded, authored);
    let Type::Struct(fields) = &authored else {
        panic!("ROW")
    };
    for name in ["split_offsets", "equality_ids"] {
        let field = fields
            .fields()
            .iter()
            .find(|field| field.name == name)
            .unwrap();
        let Type::List(list) = field.field_type.as_ref() else {
            panic!("LIST")
        };
        assert_eq!(list.element_field.name, "element");
        assert!(!list.element_field.required);
        let DataType::List(item) = schema.field_with_name(name).unwrap().data_type() else {
            panic!("Arrow List")
        };
        assert_eq!(item.name(), "item");
    }
}

#[test]
fn files_freezes_the_eighteen_public_columns_in_order() {
    let schema = test_schema();
    let specs = vec![identity_spec(&schema, 7, "region")];
    let relation =
        system_relation_schema(IcebergSystemTableType::Files, &schema, &specs).unwrap();
    assert_eq!(
        column_names(&relation),
        vec![
            "content",
            "file_path",
            "file_format",
            "spec_id",
            "record_count",
            "file_size_in_bytes",
            "column_sizes",
            "value_counts",
            "null_value_counts",
            "nan_value_counts",
            "lower_bounds",
            "upper_bounds",
            "split_offsets",
            "equality_ids",
            "sort_order_id",
            "key_metadata",
            "first_row_id",
            "partition",
        ]
    );
    assert_eq!(relation.fields().len(), 18);
    for (name, ty, nullable) in [
        ("content", DataType::Int32, false),
        ("file_path", DataType::Utf8, false),
        ("record_count", DataType::Int64, false),
        (
            "column_sizes",
            map_type(DataType::Int32, DataType::Int64),
            true,
        ),
        ("key_metadata", DataType::Binary, true),
        ("split_offsets", list_type(DataType::Int64), true),
        ("equality_ids", list_type(DataType::Int32), true),
        ("sort_order_id", DataType::Int32, true),
        ("first_row_id", DataType::Int64, true),
    ] {
        let field = relation.field_with_name(name).unwrap();
        assert_eq!(field.data_type(), &ty, "{name}");
        assert_eq!(field.is_nullable(), nullable, "{name}");
    }
    assert!(relation.field_with_name("readable_metrics").is_err());
    let unpartitioned =
        system_relation_schema(IcebergSystemTableType::Files, &schema, &[]).unwrap();
    assert_eq!(unpartitioned.fields().len(), 17);
    assert!(unpartitioned.field_with_name("partition").is_err());
}

#[test]
fn files_bounds_keep_the_public_field_id_binary_map() {
    let schema = test_schema();
    let specs = vec![identity_spec(&schema, 7, "region")];
    let relation =
        system_relation_schema(IcebergSystemTableType::Files, &schema, &specs).unwrap();
    for name in ["lower_bounds", "upper_bounds"] {
        let field = relation.field_with_name(name).unwrap();
        assert_eq!(
            field.data_type(),
            &map_type(DataType::Int32, DataType::Binary),
            "{name}"
        );
        assert!(field.is_nullable());
    }
    // Historical field IDs retain the canonical bytes even when there is
    // no current schema column for that ID.
    let bounds = BTreeMap::from([(1, Datum::long(7)), (404, Datum::string("historical"))]);
    let array = int_to_binary_map_array(std::iter::once(&bounds)).unwrap();
    let map = array
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .unwrap();
    let row = map.value(0);
    assert_eq!(
        row.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1, 404]
    );
    let values = row
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap();
    assert_eq!(values.value(0), 7_i64.to_le_bytes());
    assert_eq!(values.value(1), b"historical");
}

#[test]
fn entries_freezes_the_public_flat_entry_prefix_and_complete_file_suffix() {
    let schema = test_schema();
    let specs = vec![identity_spec(&schema, 7, "region")];
    let relation =
        system_relation_schema(IcebergSystemTableType::Entries, &schema, &specs).unwrap();
    assert_eq!(
        column_names(&relation),
        vec![
            "status",
            "snapshot_id",
            "sequence_number",
            "file_sequence_number",
            "content",
            "file_path",
            "file_format",
            "spec_id",
            "record_count",
            "file_size_in_bytes",
            "column_sizes",
            "value_counts",
            "null_value_counts",
            "nan_value_counts",
            "lower_bounds",
            "upper_bounds",
            "split_offsets",
            "equality_ids",
            "sort_order_id",
            "key_metadata",
            "first_row_id",
            "partition",
        ]
    );
    assert_eq!(relation.fields().len(), 22);
    assert_eq!(relation.fields()[0].data_type(), &DataType::Int32);
    assert!(!relation.fields()[0].is_nullable());
    for field in &relation.fields()[1..4] {
        assert_eq!(field.data_type(), &DataType::Int64);
        assert!(field.is_nullable());
    }
    let files = system_relation_schema(IcebergSystemTableType::Files, &schema, &specs).unwrap();
    assert_eq!(&relation.fields()[4..], files.fields().as_ref());
    assert!(relation.field_with_name("data_file").is_err());
    assert!(relation.field_with_name("readable_metrics").is_err());
}

#[test]
fn snapshots_history_refs_and_manifests_freeze_their_exact_shapes() {
    let schema = test_schema();
    let specs = vec![identity_spec(&schema, 7, "region")];

    let snapshots = system_relation_schema(IcebergSystemTableType::Snapshots, &schema, &specs)
        .expect("schema");
    assert_eq!(
        column_names(&snapshots),
        vec![
            "committed_at",
            "snapshot_id",
            "parent_id",
            "operation",
            "manifest_list",
            "summary",
        ]
    );
    assert_eq!(
        snapshots
            .field_with_name("committed_at")
            .expect("committed_at")
            .data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some(UTC_TIME_ZONE.into()))
    );
    assert_eq!(
        snapshots
            .field_with_name("summary")
            .expect("summary")
            .data_type(),
        &map_type(DataType::Utf8, DataType::Utf8)
    );

    let history = system_relation_schema(IcebergSystemTableType::History, &schema, &specs)
        .expect("schema");
    assert_eq!(
        column_names(&history),
        vec![
            "made_current_at",
            "snapshot_id",
            "parent_id",
            "is_current_ancestor",
        ]
    );
    assert_eq!(
        history
            .field_with_name("made_current_at")
            .expect("made_current_at")
            .data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some(UTC_TIME_ZONE.into()))
    );
    assert_eq!(
        history
            .field_with_name("is_current_ancestor")
            .expect("is_current_ancestor")
            .data_type(),
        &DataType::Boolean
    );

    let refs =
        system_relation_schema(IcebergSystemTableType::Refs, &schema, &specs).expect("schema");
    assert_eq!(
        refs.fields()
            .iter()
            .map(|field| (field.name().clone(), field.data_type().clone()))
            .collect::<Vec<_>>(),
        vec![
            ("name".to_string(), DataType::Utf8),
            ("type".to_string(), DataType::Utf8),
            ("snapshot_id".to_string(), DataType::Int64),
            ("max_reference_age_in_ms".to_string(), DataType::Int64),
            ("min_snapshots_to_keep".to_string(), DataType::Int32),
            ("max_snapshot_age_in_ms".to_string(), DataType::Int64),
        ]
    );

    let manifests = system_relation_schema(IcebergSystemTableType::Manifests, &schema, &specs)
        .expect("schema");
    assert_eq!(
        manifests
            .fields()
            .iter()
            .map(|field| (field.name().clone(), field.data_type().clone()))
            .collect::<Vec<_>>(),
        vec![
            ("content".to_string(), DataType::Int32),
            ("path".to_string(), DataType::Utf8),
            ("length".to_string(), DataType::Int64),
            ("partition_spec_id".to_string(), DataType::Int32),
            ("added_snapshot_id".to_string(), DataType::Int64),
            ("added_data_files_count".to_string(), DataType::Int32),
            ("existing_data_files_count".to_string(), DataType::Int32),
            ("deleted_data_files_count".to_string(), DataType::Int32),
            ("added_rows_count".to_string(), DataType::Int64),
            ("existing_rows_count".to_string(), DataType::Int64),
            ("deleted_rows_count".to_string(), DataType::Int64),
            ("partition_summaries".to_string(), partition_summary_type()),
        ]
    );
}

#[test]
fn partition_value_slots_preserve_same_name_case_and_generated_name_collisions() {
    let schema = test_schema();
    let specs = [(0,1000,"p"),(1,1001,"P"),(2,1002,"p_1000"),(3,1003,"p_1000_1")]
        .into_iter().map(|(spec_id,field_id,name)| serde_json::from_value::<PartitionSpec>(serde_json::json!({
            "spec-id":spec_id,"fields":[{"field-id":field_id,"source-id":2,"name":name,"transform":"identity"}]
        })).unwrap()).collect::<Vec<_>>();
    let unified = crate::partition_row::UnifiedPartitionRow::of(&schema, &specs).unwrap();
    let mut rows = Vec::new();
    for (slot, spec) in specs.iter().enumerate() {
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("data/{slot}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::from_iter([Some(Literal::string(format!(
                "value-{slot}"
            )))]))
            .partition_spec_id(spec.spec_id())
            .record_count(1)
            .file_size_in_bytes(100)
            .build()
            .unwrap();
        let manifest = Manifest::new(
            crate::iceberg::spec::ManifestMetadata {
                schema: Arc::new(schema.clone()),
                schema_id: schema.schema_id(),
                partition_spec: spec.clone(),
                format_version: FormatVersion::V2,
                content: crate::iceberg::spec::ManifestContentType::Data,
            },
            vec![crate::iceberg::spec::ManifestEntry {
                status: ManifestStatus::Added,
                snapshot_id: Some(SNAPSHOT_ID),
                sequence_number: Some(1),
                file_sequence_number: Some(1),
                data_file: file,
            }],
        );
        let descriptor = TrinoManifestFile::try_new(TrinoManifestFileParams {
            path: format!("metadata/{slot}.avro"),
            length: 100,
            partition_spec_id: spec.spec_id(),
            content: crate::typed_read::TrinoManifestContent::Data,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: SNAPSHOT_ID,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(1),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            first_row_id: None,
            key_metadata: Vec::new(),
        })
        .unwrap();
        rows.extend(
            manifest_rows(
                &descriptor,
                &manifest,
                spec,
                EntryStatusRule::SkipDeleted,
                &unified,
            )
            .unwrap(),
        );
    }
    for kind in [
        IcebergSystemTableType::Files,
        IcebergSystemTableType::Entries,
    ] {
        let output_schema = system_relation_schema(kind, &schema, &specs).unwrap();
        let columns = match kind {
            IcebergSystemTableType::Files => {
                files_columns(&output_schema, &rows, &schema).unwrap()
            }
            IcebergSystemTableType::Entries => {
                entries_columns(&output_schema, &rows, &schema).unwrap()
            }
            _ => unreachable!(),
        };
        let partition = columns[output_schema.index_of("partition").unwrap()]
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            partition.column_names(),
            vec!["p_1000_2", "P", "p_1000", "p_1000_1"]
        );
        for slot in 0..4 {
            let column = partition
                .column(slot)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let expected = (0..4)
                .map(|row| (row == slot).then(|| format!("value-{slot}")))
                .collect::<Vec<_>>();
            assert_eq!(
                column
                    .iter()
                    .map(|value| value.map(str::to_owned))
                    .collect::<Vec<_>>(),
                expected,
                "{kind:?} slot {slot}"
            );
        }
    }
}

#[test]
fn partition_field_ids_keep_renamed_values_in_one_canonical_slot() {
    let schema = test_schema();
    let spec = |id, name: &str| {
        serde_json::from_value::<PartitionSpec>(serde_json::json!({
        "spec-id": id, "fields": [{"field-id":1000,"source-id":2,"name":name,"transform":"identity"}]
    })).unwrap()
    };
    let old = spec(0, "old_region");
    let new = spec(1, "region");
    let unified =
        crate::partition_row::UnifiedPartitionRow::of(&schema, &[new.clone(), old.clone()])
            .unwrap();
    assert_eq!(unified.field_ids(), vec![1000]);
    let row_type = unified.arrow_type().unwrap();
    assert_eq!(
        row_type,
        DataType::Struct(vec![Arc::new(Field::new("region", DataType::Utf8, true))].into())
    );
    let data = |id| {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("data/{id}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::from_iter([Some(Literal::string("east"))]))
            .partition_spec_id(id)
            .record_count(1)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    };
    let old_values = partition_values(&data(0), &old, &unified).unwrap();
    let new_values = partition_values(&data(1), &new, &unified).unwrap();
    let rows = partition_struct_array(&row_type, &[&old_values, &new_values]).unwrap();
    let rows = rows.as_any().downcast_ref::<StructArray>().unwrap();
    let values = rows
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some("east"), Some("east")]
    );
}

// -- real metadata on disk --------------------------------------------

/// A real Iceberg table on disk: manifests, a manifest list, and the exact
/// metadata file a frozen reference points at. The caller owns the tempdir.
struct Warehouse {
    metadata_location: String,
    metadata: TableMetadata,
}

fn data_file(path: &str, region: &str, records: u64, size: u64) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::from_iter([Some(Literal::Primitive(
            PrimitiveLiteral::String(region.to_string()),
        ))]))
        .partition_spec_id(0)
        .record_count(records)
        .file_size_in_bytes(size)
        .lower_bounds(HashMap::from([(1, Datum::long(1_i64))]))
        .upper_bounds(HashMap::from([(1, Datum::long(9_i64))]))
        .null_value_counts(HashMap::from([(1_i32, 0_u64)]))
        .column_sizes(HashMap::from([(1_i32, 64_u64)]))
        .build()
        .expect("data file")
}

/// One snapshot with two manifests: the first holds two live data files and
/// the second holds one file that has already been deleted, so `$files` and
/// `$entries` must disagree about it.
async fn build_warehouse(dir: &Path, binding: IcebergReadBinding) -> Warehouse {
    build_warehouse_with_deletes(dir, binding, false).await
}

async fn build_warehouse_with_deletes(
    dir: &Path,
    binding: IcebergReadBinding,
    with_deletes: bool,
) -> Warehouse {
    let location = format!("file://{}", dir.display());
    std::fs::create_dir_all(dir.join("metadata")).expect("metadata dir");
    let schema = test_schema();
    // `TableMetadataBuilder::new` reassigns the initial spec to id 0, so the
    // manifests must declare the same id or they would name a spec the
    // metadata does not have.
    let spec = identity_spec(&schema, 0, "region");
    let file_io = crate::fs_io::build_file_io_for_location(&location, binding);

    let mut manifests = Vec::new();
    let live_path = format!("{location}/metadata/live.avro");
    let mut writer = ManifestWriterBuilder::new(
        file_io.new_output(&live_path).expect("manifest output"),
        Some(SNAPSHOT_ID),
        Arc::new(schema.clone()),
        spec.clone(),
    )
    .build_v2_data();
    writer
        .add_file(data_file("data/a.parquet", "east", 10, 1_024), 1)
        .expect("add file");
    writer
        .add_file(data_file("data/b.parquet", "west", 20, 2_048), 1)
        .expect("add file");
    if with_deletes {
        writer
            .add_file(data_file("data/d.parquet", "east", 15, 1_536), 1)
            .unwrap();
    }
    let mut manifest = writer.write_manifest_file().await.expect("write manifest");
    manifest.sequence_number = 1;
    manifest.min_sequence_number = 1;
    manifests.push(manifest);

    let removed_path = format!("{location}/metadata/removed.avro");
    let mut writer = ManifestWriterBuilder::new(
        file_io.new_output(&removed_path).expect("manifest output"),
        Some(SNAPSHOT_ID),
        Arc::new(schema.clone()),
        spec.clone(),
    )
    .build_v2_data();
    writer
        .add_delete_file(data_file("data/c.parquet", "east", 5, 512), 1, Some(1))
        .expect("delete file");
    let mut manifest = writer.write_manifest_file().await.expect("write manifest");
    manifest.sequence_number = 1;
    manifest.min_sequence_number = 1;
    manifests.push(manifest);

    if with_deletes {
        let path = format!("{location}/metadata/deletes.avro");
        let mut writer = ManifestWriterBuilder::new(
            file_io.new_output(&path).unwrap(),
            Some(SNAPSHOT_ID),
            Arc::new(schema.clone()),
            spec.clone(),
        )
        .build_v2_deletes();
        let delete =
            |path: &str, content: DataContentType, region: &str, target: Option<&str>| {
                let mut builder = DataFileBuilder::default();
                builder
                    .content(content)
                    .file_path(path.to_owned())
                    .file_format(DataFileFormat::Parquet)
                    .partition(Struct::from_iter([Some(Literal::string(region))]))
                    .partition_spec_id(0)
                    .record_count(1)
                    .file_size_in_bytes(128);
                if content == DataContentType::EqualityDeletes {
                    builder.equality_ids(Some(vec![1]));
                }
                if let Some(target) = target {
                    builder.referenced_data_file(Some(target.to_owned()));
                }
                builder.build().unwrap()
            };
        writer
            .add_file(
                delete(
                    "delete/p.parquet",
                    DataContentType::PositionDeletes,
                    "east",
                    Some("data/a.parquet"),
                ),
                1,
            )
            .unwrap();
        writer
            .add_file(
                delete(
                    "delete/e.parquet",
                    DataContentType::EqualityDeletes,
                    "east",
                    None,
                ),
                2,
            )
            .unwrap();
        // Same-sequence equality does not apply; exact-target position
        // metadata for a different data path does not apply either.
        writer
            .add_file(
                delete(
                    "delete/old.parquet",
                    DataContentType::EqualityDeletes,
                    "west",
                    None,
                ),
                1,
            )
            .unwrap();
        writer
            .add_file(
                delete(
                    "delete/foreign.parquet",
                    DataContentType::PositionDeletes,
                    "east",
                    Some("data/missing.parquet"),
                ),
                2,
            )
            .unwrap();
        let mut manifest = writer.write_manifest_file().await.unwrap();
        manifest.sequence_number = SNAPSHOT_SEQUENCE_NUMBER;
        manifest.min_sequence_number = 1;
        manifests.push(manifest);
    }

    let manifest_list_path = format!("{location}/metadata/snap-{SNAPSHOT_ID}.avro");
    let mut list_writer = ManifestListWriter::v2(
        file_io
            .new_output(&manifest_list_path)
            .expect("manifest list output")
            .writer()
            .await
            .expect("manifest list writer"),
        SNAPSHOT_ID,
        None,
        SNAPSHOT_SEQUENCE_NUMBER,
    );
    list_writer
        .add_manifests(manifests.into_iter())
        .expect("add manifests");
    list_writer.close().await.expect("close manifest list");

    let snapshot = Snapshot::builder()
        .with_snapshot_id(SNAPSHOT_ID)
        .with_sequence_number(SNAPSHOT_SEQUENCE_NUMBER)
        .with_timestamp_ms(1_700_000_000_000)
        .with_manifest_list(manifest_list_path)
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from([(
                "added-data-files".to_string(),
                if with_deletes { "3" } else { "2" }.to_string(),
            )]),
        })
        .with_schema_id(0)
        .build();
    let metadata = TableMetadataBuilder::new(
        schema,
        spec.into_unbound(),
        SortOrder::unsorted_order(),
        location.clone(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .expect("metadata builder")
    .add_snapshot(snapshot)
    .expect("add snapshot")
    .set_ref(
        "main",
        SnapshotReference::new(
            SNAPSHOT_ID,
            SnapshotRetention::Branch {
                min_snapshots_to_keep: Some(2),
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        ),
    )
    .expect("set main")
    .set_ref(
        crate::commit::mv_publication_fence::MV_PUBLICATION_FENCE_REF,
        SnapshotReference::new(
            SNAPSHOT_ID,
            SnapshotRetention::Tag {
                max_ref_age_ms: None,
            },
        ),
    )
    .expect("set fence ref")
    .build()
    .expect("metadata")
    .metadata;

    let metadata_location = format!("{location}/metadata/00001-frozen.metadata.json");
    std::fs::write(
        dir.join("metadata/00001-frozen.metadata.json"),
        serde_json::to_vec(&metadata).expect("metadata json"),
    )
    .expect("write metadata");

    Warehouse {
        metadata_location,
        metadata,
    }
}

fn reference(
    warehouse: &Warehouse,
    relation: IcebergSystemTableType,
    snapshot_id: Option<i64>,
) -> IcebergSystemTableReference {
    IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
        schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
        system_table_type: relation,
        metadata_file_location: warehouse.metadata_location.clone(),
        table_uuid: warehouse.metadata.uuid().hyphenated().to_string(),
        snapshot_id,
    })
    .expect("reference")
}

fn refusal(result: Result<Vec<Vec<ArrayRef>>, ConnectorError>) -> ConnectorError {
    result.expect_err("the reader was expected to fail closed")
}

/// Reads a stream to its end and closes it.
fn drain_driven(mut source: DrivenStream<'_>) -> Result<Vec<Vec<ArrayRef>>, ConnectorError> {
    let mut pages = Vec::new();
    while let Some(page) = source.next_page()? {
        let (_, columns) = page.into_columns().expect("columns");
        pages.push(columns);
    }
    assert!(source.is_finished());
    source.close()?;
    Ok(pages)
}

/// Read singleton work units, or each independent entries manifest unit.
fn single_backend_pages(
    runtime: &tokio::runtime::Runtime,
    provider: &IcebergSystemTableProvider,
    reference: &IcebergSystemTableReference,
    columns: &[IcebergColumnHandle],
) -> Result<Vec<Vec<ArrayRef>>, ConnectorError> {
    let budget = ConnectorPollBudget::new();
    if reference.system_table_type() == IcebergSystemTableType::Entries {
        let mut pages = Vec::new();
        for split in manifest_splits(runtime, provider, reference)? {
            let stream = provider.create_page_stream(
                reference,
                &super::super::IcebergReadSplit::SystemManifest(split),
                columns,
                &budget,
            )?;
            pages.extend(drain_driven(DrivenStream::new(
                runtime,
                stream,
                budget.clone(),
            ))?);
        }
        // Preserve the pre-existing relation-wide value assertions after
        // independently reading every manifest through the unit channel.
        if pages.len() > 1 {
            let columns = (0..pages[0].len())
                .map(|ordinal| {
                    let arrays = pages
                        .iter()
                        .map(|page| page[ordinal].as_ref())
                        .collect::<Vec<_>>();
                    arrow::compute::concat(&arrays).map_err(|error| invalid(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(vec![columns]);
        }
        return Ok(pages);
    }
    let split = super::super::IcebergReadSplit::SystemRelation(SystemRelationSplit::try_new(
        reference.clone(),
    )?);
    let stream = provider.create_page_stream(reference, &split, columns, &budget)?;
    drain_driven(DrivenStream::new(runtime, stream, budget))
}

fn manifest_splits(
    runtime: &tokio::runtime::Runtime,
    provider: &IcebergSystemTableProvider,
    reference: &IcebergSystemTableReference,
) -> Result<Vec<SystemManifestSplit>, ConnectorError> {
    if reference.snapshot_id().is_none() {
        return Ok(Vec::new());
    }
    let mut reader = FrozenMetadataReader::new(&provider.binding, &provider.context);
    let metadata = runtime.block_on(reader.load_metadata(reference))?;
    let manifests = runtime
        .block_on(read_snapshot_manifests(
            &mut reader,
            reference,
            &metadata,
            &unbounded_budget(),
        ))?
        .files;
    let schema = metadata.current_schema();
    let specs = metadata
        .partition_specs_iter()
        .map(|spec| {
            Ok((
                spec.spec_id(),
                serde_json::to_string(spec.as_ref())
                    .map_err(|error| invalid(error.to_string()))?,
            ))
        })
        .collect::<Result<std::collections::BTreeMap<_, _>, ConnectorError>>()?;
    let partition = partition_row_type_with_metadata(&metadata)?
        .as_ref()
        .map(derived_row_type_json)
        .transpose()?;
    manifests
        .into_iter()
        .map(|manifest| {
            SystemManifestSplit::try_new(SystemManifestSplitParams {
                reference: reference.clone(),
                table_has_variant_history: crate::manifest::table_has_variant_history(
                    &metadata,
                ),
                manifest,
                table_schema_json: serde_json::to_string(schema.as_ref())
                    .map_err(|error| invalid(error.to_string()))?,
                metadata_table_schema_json: system_relation_schema_json_with_metadata(
                    reference.system_table_type(),
                    &metadata,
                )?,
                partition_spec_jsons: specs.clone(),
                partition_slot_field_ids: crate::partition_row::UnifiedPartitionRow::of(
                    schema.as_ref(),
                    &metadata
                        .partition_specs_iter()
                        .map(|spec| spec.as_ref().clone())
                        .collect::<Vec<_>>(),
                )?
                .field_ids(),
                partition_column_type_json: partition.clone(),
                bounds_column_type_json: None,
                encryption_key_id: None,
            })
        })
        .collect()
}

/// A `$files` split read to its end through its page stream.
fn files_pages(
    runtime: &tokio::runtime::Runtime,
    provider: &IcebergSystemTableProvider,
    split: &SystemManifestSplit,
    columns: &[IcebergColumnHandle],
) -> Result<Vec<Vec<ArrayRef>>, ConnectorError> {
    let budget = ConnectorPollBudget::new();
    let stream = provider.create_page_stream(
        split.reference(),
        &super::super::IcebergReadSplit::SystemManifest(split.clone()),
        columns,
        &budget,
    )?;
    drain_driven(DrivenStream::new(runtime, stream, budget))
}

/// A budget no read in these tests runs out of.
fn unbounded_budget() -> ConnectorPollBudget {
    let budget = ConnectorPollBudget::new();
    budget.refill(u64::MAX);
    budget
}

fn all_columns(schema: &SchemaRef) -> Vec<IcebergColumnHandle> {
    // Column handles carry an Iceberg field ID and an Iceberg type; a
    // system relation's columns are named metadata columns, so the identity
    // name is the part that selects them and the type is only along for the
    // ride.
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let nested = NestedField::optional(
                i32::try_from(ordinal + 1).expect("ordinal"),
                field.name(),
                Type::Primitive(PrimitiveType::Long),
            );
            IcebergColumnHandle::base_column(&nested).expect("column handle")
        })
        .collect()
}

fn string_column(columns: &[ArrayRef], index: usize) -> Vec<Option<String>> {
    let array = columns[index]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    (0..array.len())
        .map(|row| (!array.is_null(row)).then(|| array.value(row).to_string()))
        .collect()
}

fn long_column(columns: &[ArrayRef], index: usize) -> Vec<Option<i64>> {
    let array = columns[index]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("long column");
    (0..array.len())
        .map(|row| (!array.is_null(row)).then(|| array.value(row)))
        .collect()
}

fn int_column(columns: &[ArrayRef], index: usize) -> Vec<Option<i32>> {
    let array = columns[index]
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int column");
    (0..array.len())
        .map(|row| (!array.is_null(row)).then(|| array.value(row)))
        .collect()
}

#[test]
fn refs_excludes_provider_private_refs() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let provider = provider(&binding, &context);
    let reference = reference(&warehouse, IcebergSystemTableType::Refs, None);
    let schema = system_relation_schema(
        IcebergSystemTableType::Refs,
        warehouse.metadata.current_schema(),
        &[],
    )
    .expect("schema");

    let pages = single_backend_pages(&runtime, &provider, &reference, &all_columns(&schema))
        .expect("pages");
    assert_eq!(pages.len(), 1);
    // `main` survives; the MV publication fence is provider bookkeeping and
    // must not appear as a table ref.
    assert_eq!(string_column(&pages[0], 0), vec![Some("main".to_string())]);
    assert_eq!(
        string_column(&pages[0], 1),
        vec![Some("BRANCH".to_string())]
    );
    assert_eq!(long_column(&pages[0], 2), vec![Some(SNAPSHOT_ID)]);
    assert_eq!(int_column(&pages[0], 4), vec![Some(2)]);
}

#[test]
fn snapshots_reports_a_zoned_commit_time_and_a_summary_map() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let provider = provider(&binding, &context);
    let reference = reference(&warehouse, IcebergSystemTableType::Snapshots, None);
    let schema = system_relation_schema(
        IcebergSystemTableType::Snapshots,
        warehouse.metadata.current_schema(),
        &[],
    )
    .expect("schema");

    let pages = single_backend_pages(&runtime, &provider, &reference, &all_columns(&schema))
        .expect("pages");
    let columns = &pages[0];

    let committed_at = columns[0]
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("zoned timestamp");
    assert_eq!(
        committed_at.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some(UTC_TIME_ZONE.into()))
    );
    assert_eq!(committed_at.value(0), 1_700_000_000_000 * 1_000);
    assert_eq!(long_column(columns, 1), vec![Some(SNAPSHOT_ID)]);
    assert_eq!(long_column(columns, 2), vec![None]);
    assert_eq!(string_column(columns, 3), vec![Some("append".to_string())]);

    let summary = columns[5]
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("summary map");
    let entries = summary.value(0);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("keys");
    assert_eq!(keys.value(0), "added-data-files");
}

#[test]
fn history_marks_the_current_ancestor() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let provider = provider(&binding, &context);
    let reference = reference(&warehouse, IcebergSystemTableType::History, None);
    let schema = system_relation_schema(
        IcebergSystemTableType::History,
        warehouse.metadata.current_schema(),
        &[],
    )
    .expect("schema");

    let pages = single_backend_pages(&runtime, &provider, &reference, &all_columns(&schema))
        .expect("pages");
    let columns = &pages[0];
    assert_eq!(long_column(columns, 1), vec![Some(SNAPSHOT_ID)]);
    let ancestor = columns[3]
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("boolean");
    assert!(ancestor.value(0));
}

#[test]
fn manifests_reports_an_array_of_partition_summary_rows() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let provider = provider(&binding, &context);
    let reference = reference(
        &warehouse,
        IcebergSystemTableType::Manifests,
        Some(SNAPSHOT_ID),
    );
    let schema = system_relation_schema(
        IcebergSystemTableType::Manifests,
        warehouse.metadata.current_schema(),
        &[],
    )
    .expect("schema");

    let pages = single_backend_pages(&runtime, &provider, &reference, &all_columns(&schema))
        .expect("pages");
    let columns = &pages[0];
    assert_eq!(columns[0].len(), 2);
    assert_eq!(int_column(columns, 0), vec![Some(0), Some(0)]);
    assert_eq!(
        int_column(columns, 3),
        vec![Some(0), Some(0)],
        "both manifests use the table's only partition spec"
    );

    let summaries = columns[11]
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("partition summaries list");
    let first = summaries.value(0);
    let first = first
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("summary struct");
    assert_eq!(first.num_columns(), 4);
    let lower = first
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("lower bound");
    // The bound is decoded through the partition field's own type, not
    // printed as raw bytes.
    assert_eq!(lower.value(0), "east");
}

#[test]
fn entries_reads_each_manifest_once_and_keeps_deleted_rows() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().unwrap();
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let provider = provider(&binding, &context);
    let reference = reference(
        &warehouse,
        IcebergSystemTableType::Entries,
        Some(SNAPSHOT_ID),
    );
    let splits = manifest_splits(&runtime, &provider, &reference).unwrap();
    assert_eq!(splits.len(), 2);
    let schema = system_relation_schema_with_metadata(
        IcebergSystemTableType::Entries,
        &warehouse.metadata,
    )
    .unwrap();
    let columns = all_columns(&schema);
    let mut statuses = Vec::new();
    let mut paths = Vec::new();
    for split in splits {
        assert_eq!(split.reference(), &reference);
        let pages = files_pages(&runtime, &provider, &split, &columns).unwrap();
        for page in pages {
            statuses.extend(int_column(&page, 0));
            let values = page[5].as_any().downcast_ref::<StringArray>().unwrap();
            paths.extend((0..values.len()).map(|i| values.value(i).to_owned()));
        }
    }
    assert_eq!(statuses, vec![Some(1), Some(1), Some(2)]);
    assert_eq!(
        paths,
        vec!["data/a.parquet", "data/b.parquet", "data/c.parquet"]
    );
}

#[test]
fn system_work_units_refuse_foreign_scan_references_before_reading() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().unwrap();
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let provider = provider(&binding, &context);
    let original = reference(
        &warehouse,
        IcebergSystemTableType::Snapshots,
        Some(SNAPSHOT_ID),
    );
    let foreign = reference(
        &warehouse,
        IcebergSystemTableType::History,
        Some(SNAPSHOT_ID),
    );
    let split = super::super::IcebergReadSplit::SystemRelation(
        SystemRelationSplit::try_new(original.clone()).unwrap(),
    );
    let error = provider
        .create_page_stream(&foreign, &split, &[], &ConnectorPollBudget::new())
        .err()
        .unwrap();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    let manifest_reference = reference(
        &warehouse,
        IcebergSystemTableType::Entries,
        Some(SNAPSHOT_ID),
    );
    let manifest = manifest_splits(&runtime, &provider, &manifest_reference)
        .unwrap()
        .remove(0);
    let split = super::super::IcebergReadSplit::SystemManifest(manifest);
    let error = provider
        .create_page_stream(&original, &split, &[], &ConnectorPollBudget::new())
        .err()
        .unwrap();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
}

#[test]
fn files_skips_deleted_entries_while_entries_keeps_them() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let provider = provider(&binding, &context);

    let entries_reference = reference(
        &warehouse,
        IcebergSystemTableType::Entries,
        Some(SNAPSHOT_ID),
    );
    let specs = warehouse
        .metadata
        .partition_specs_iter()
        .map(|spec| spec.as_ref().clone())
        .collect::<Vec<_>>();
    let entries_schema = system_relation_schema(
        IcebergSystemTableType::Entries,
        warehouse.metadata.current_schema(),
        &specs,
    )
    .expect("schema");
    let pages = single_backend_pages(
        &runtime,
        &provider,
        &entries_reference,
        &all_columns(&entries_schema),
    )
    .expect("pages");
    let columns = &pages[0];
    // Two live entries plus the tombstone the second manifest carries.
    assert_eq!(columns[0].len(), 3);
    assert_eq!(int_column(columns, 0), vec![Some(1), Some(1), Some(2)]);
    // Sequence numbers are inherited from the manifest list entry.
    assert_eq!(
        long_column(columns, 2),
        vec![Some(1), Some(1), Some(1)],
        "data sequence numbers"
    );

    assert_eq!(columns.len(), 22);
    let paths = columns[5]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("the public flat file_path column");
    assert_eq!(paths.value(2), "data/c.parquet");

    // `$files` over the same snapshot reports only the two live files.
    let files_rows = {
        let mut reader = FrozenMetadataReader::new(&binding, &context);
        let metadata = runtime
            .block_on(reader.load_metadata(&reference(
                &warehouse,
                IcebergSystemTableType::Files,
                Some(SNAPSHOT_ID),
            )))
            .expect("metadata");
        runtime
            .block_on(read_snapshot_file_rows(
                &mut reader,
                &metadata,
                &reference(&warehouse, IcebergSystemTableType::Files, Some(SNAPSHOT_ID)),
                EntryStatusRule::SkipDeleted,
                &unbounded_budget(),
            ))
            .expect("rows")
    };
    assert_eq!(
        files_rows
            .iter()
            .map(|row| row.file_path.as_str())
            .collect::<Vec<_>>(),
        vec!["data/a.parquet", "data/b.parquet"]
    );
}

#[test]
fn files_materializes_one_manifest_split_with_public_binary_bounds() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let provider = provider(&binding, &context);

    let mut reader = FrozenMetadataReader::new(&binding, &context);
    let files_reference =
        reference(&warehouse, IcebergSystemTableType::Files, Some(SNAPSHOT_ID));
    let metadata = runtime
        .block_on(reader.load_metadata(&files_reference))
        .expect("metadata");
    let manifests = runtime
        .block_on(read_snapshot_manifests(
            &mut reader,
            &files_reference,
            &metadata,
            &unbounded_budget(),
        ))
        .expect("manifests")
        .files;

    let schema = warehouse.metadata.current_schema().as_ref().clone();
    let specs: Vec<_> = warehouse
        .metadata
        .partition_specs_iter()
        .map(|spec| spec.as_ref().clone())
        .collect();
    let split = SystemManifestSplit::try_new(SystemManifestSplitParams {
        reference: files_reference.clone(),
        table_has_variant_history: false,
        manifest: manifests[0].clone(),
        table_schema_json: serde_json::to_string(&schema).expect("schema json"),
        metadata_table_schema_json: files_relation_schema_json_with_metadata(
            &warehouse.metadata,
        )
        .expect("frozen files declaration"),
        partition_spec_jsons: specs
            .iter()
            .map(|spec| {
                (
                    spec.spec_id(),
                    serde_json::to_string(spec).expect("spec json"),
                )
            })
            .collect(),
        partition_slot_field_ids: crate::partition_row::UnifiedPartitionRow::of(
            &schema, &specs,
        )
        .unwrap()
        .field_ids(),
        partition_column_type_json: partition_row_type_with_metadata(&warehouse.metadata)
            .expect("partition declaration")
            .as_ref()
            .map(derived_row_type_json)
            .transpose()
            .expect("partition declaration json"),
        bounds_column_type_json: None,
        encryption_key_id: None,
    })
    .expect("split");

    let relation = system_relation_schema_with_metadata(
        IcebergSystemTableType::Files,
        &warehouse.metadata,
    )
    .expect("schema");
    let pages =
        files_pages(&runtime, &provider, &split, &all_columns(&relation)).expect("pages");
    let columns = &pages[0];
    assert_eq!(columns.len(), 18);
    assert_eq!(columns[0].len(), 2);
    assert_eq!(
        string_column(columns, 1),
        vec![
            Some("data/a.parquet".to_string()),
            Some("data/b.parquet".to_string())
        ]
    );
    assert_eq!(
        string_column(columns, 2),
        vec![Some("PARQUET".to_string()); 2]
    );
    assert_eq!(int_column(columns, 3), vec![Some(0), Some(0)]);

    let partition = columns[17]
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("partition row");
    let region = partition
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("region");
    assert_eq!(region.value(0), "east");

    // The public contract preserves field IDs and canonical bound bytes.
    let lower = columns[10]
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .expect("lower bound map");
    let bounds = lower.value(0);
    let ids = bounds
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let values = bounds
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap();
    let ordinal = ids.values().iter().position(|id| *id == 1).unwrap();
    assert_eq!(values.value(ordinal), 1_i64.to_le_bytes());

    // Reader addresses remain exact internal facts, not extra public
    // columns. Inspect the SAME frozen manifest through its real author.
    let manifest = runtime
        .block_on(reader.load_manifest(&manifests[0]))
        .unwrap();
    let mut raw = runtime
        .block_on(reader.load_manifest_list(&metadata, SNAPSHOT_ID))
        .unwrap()
        .remove(0);
    raw.added_files_count = None;
    raw.existing_files_count = None;
    raw.deleted_files_count = None;
    raw.added_rows_count = None;
    raw.existing_rows_count = None;
    raw.deleted_rows_count = None;
    runtime
        .block_on(complete_manifest_counts(
            &mut raw,
            &manifest,
            &unbounded_budget(),
        ))
        .unwrap();
    assert_eq!(
        (
            raw.added_files_count,
            raw.existing_files_count,
            raw.deleted_files_count
        ),
        (Some(2), Some(0), Some(0)),
        "actual manifest status counts"
    );
    assert_eq!(
        (
            raw.added_rows_count,
            raw.existing_rows_count,
            raw.deleted_rows_count
        ),
        (Some(30), Some(0), Some(0)),
        "actual manifest row counts"
    );
    let spec = manifest_spec(&metadata, &manifests[0]).unwrap();
    let rows = manifest_rows(
        &manifests[0],
        &manifest,
        &spec,
        EntryStatusRule::SkipDeleted,
        &crate::partition_row::UnifiedPartitionRow::of(
            metadata.current_schema(),
            &metadata
                .partition_specs_iter()
                .map(|spec| spec.as_ref().clone())
                .collect::<Vec<_>>(),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.pos).collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.manifest_location.as_str())
            .collect::<Vec<_>>(),
        vec![manifests[0].path(); 2]
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.entry.snapshot_id)
            .collect::<Vec<_>>(),
        vec![Some(SNAPSHOT_ID), Some(SNAPSHOT_ID)],
        "added snapshot id is inherited from the manifest list entry"
    );
}

#[test]
fn a_uuid_or_snapshot_mismatch_fails_closed_before_any_row() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let provider = provider(&binding, &context);
    let schema = system_relation_schema(
        IcebergSystemTableType::Snapshots,
        warehouse.metadata.current_schema(),
        &[],
    )
    .expect("schema");
    let columns = all_columns(&schema);

    let wrong_uuid = IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
        schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
        system_table_type: IcebergSystemTableType::Snapshots,
        metadata_file_location: warehouse.metadata_location.clone(),
        table_uuid: "9d1f4c1e-6a1f-4a0b-9c3a-0f2b6d5e7a11".to_string(),
        snapshot_id: None,
    })
    .expect("reference");
    assert_eq!(
        refusal(single_backend_pages(
            &runtime,
            &provider,
            &wrong_uuid,
            &columns
        ))
        .kind(),
        ConnectorErrorKind::CorruptData
    );

    let missing_snapshot =
        IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
            schema_table_name: SchemaTableName::try_new("db", "t").expect("name"),
            system_table_type: IcebergSystemTableType::Manifests,
            metadata_file_location: warehouse.metadata_location.clone(),
            table_uuid: warehouse.metadata.uuid().hyphenated().to_string(),
            snapshot_id: Some(4242),
        })
        .expect("reference");
    assert_eq!(
        refusal(single_backend_pages(
            &runtime,
            &provider,
            &missing_snapshot,
            &columns
        ))
        .kind(),
        ConnectorErrorKind::CorruptData
    );
}

#[test]
fn a_non_empty_manifest_key_metadata_is_rejected() {
    let error = TrinoManifestFile::try_new(TrinoManifestFileParams {
        path: "m0.avro".to_string(),
        length: 128,
        partition_spec_id: 7,
        content: crate::typed_read::system_table::TrinoManifestContent::Data,
        sequence_number: 1,
        min_sequence_number: 1,
        added_snapshot_id: SNAPSHOT_ID,
        added_files_count: Some(1),
        existing_files_count: None,
        deleted_files_count: None,
        added_rows_count: Some(1),
        existing_rows_count: None,
        deleted_rows_count: None,
        first_row_id: None,
        key_metadata: vec![0xAB, 0xCD],
    })
    .expect_err("encrypted manifest");
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
}

#[test]
fn a_distributed_relation_is_refused_by_the_direct_page_source() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let provider = provider(&binding, &context);
    let reference = reference(&warehouse, IcebergSystemTableType::Files, Some(SNAPSHOT_ID));
    let error = refusal(single_backend_pages(&runtime, &provider, &reference, &[]));
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
}

#[test]
fn an_unknown_projected_column_is_refused() {
    let schema = test_schema();
    let relation =
        system_relation_schema(IcebergSystemTableType::Refs, &schema, &[]).expect("schema");
    let nested = NestedField::optional(1, "not_a_column", Type::Primitive(PrimitiveType::Long));
    let handle = IcebergColumnHandle::base_column(&nested).expect("handle");
    assert_eq!(
        project_system_relation_columns(&relation, &[handle])
            .expect_err("unknown column")
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
    // No projected column at all is a count-only scan, not an error.
    assert!(
        project_system_relation_columns(&relation, &[])
            .expect("count-only")
            .is_empty()
    );
}

fn drain_stream(
    runtime: &tokio::runtime::Runtime,
    stream: &mut OwnedConnectorPageStream,
) -> Vec<Vec<ArrayRef>> {
    use futures::StreamExt;
    runtime.block_on(async {
        let mut pages = Vec::new();
        while let Some(page) = stream.next().await {
            let (_, columns) = page.expect("page").into_columns().expect("columns");
            pages.push(columns);
        }
        pages
    })
}

/// The `$files` split of the warehouse's first manifest, and every column
/// of its relation.
fn first_manifest_split(
    runtime: &tokio::runtime::Runtime,
    binding: &IcebergReadBinding,
    context: &FileReadContext,
    warehouse: &Warehouse,
) -> (SystemManifestSplit, Vec<IcebergColumnHandle>) {
    let mut reader = FrozenMetadataReader::new(binding, context);
    let files_reference =
        reference(warehouse, IcebergSystemTableType::Files, Some(SNAPSHOT_ID));
    let metadata = runtime
        .block_on(reader.load_metadata(&files_reference))
        .expect("metadata");
    let manifests = runtime
        .block_on(read_snapshot_manifests(
            &mut reader,
            &files_reference,
            &metadata,
            &unbounded_budget(),
        ))
        .expect("manifests")
        .files;
    let schema = warehouse.metadata.current_schema().as_ref().clone();
    let specs: Vec<_> = warehouse
        .metadata
        .partition_specs_iter()
        .map(|spec| spec.as_ref().clone())
        .collect();
    let split = SystemManifestSplit::try_new(SystemManifestSplitParams {
        reference: files_reference.clone(),
        table_has_variant_history: false,
        manifest: manifests[0].clone(),
        table_schema_json: serde_json::to_string(&schema).expect("schema json"),
        metadata_table_schema_json: files_relation_schema_json_with_metadata(
            &warehouse.metadata,
        )
        .expect("frozen files declaration"),
        partition_spec_jsons: specs
            .iter()
            .map(|spec| {
                (
                    spec.spec_id(),
                    serde_json::to_string(spec).expect("spec json"),
                )
            })
            .collect(),
        partition_slot_field_ids: crate::partition_row::UnifiedPartitionRow::of(
            &schema, &specs,
        )
        .unwrap()
        .field_ids(),
        partition_column_type_json: partition_row_type_with_metadata(&warehouse.metadata)
            .expect("partition declaration")
            .as_ref()
            .map(derived_row_type_json)
            .transpose()
            .expect("partition declaration json"),
        bounds_column_type_json: None,
        encryption_key_id: None,
    })
    .expect("split");
    let relation = system_relation_schema_with_metadata(
        IcebergSystemTableType::Files,
        &warehouse.metadata,
    )
    .expect("schema");
    (split, all_columns(&relation))
}

fn drop_partition_source(warehouse: &mut Warehouse) {
    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields([Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("schema after dropping region");
    warehouse.metadata = warehouse
        .metadata
        .clone()
        .into_builder(None)
        .add_current_schema(schema)
        .expect("current schema")
        .add_partition_spec(PartitionSpec::unpartition_spec().into_unbound())
        .expect("unpartitioned current spec")
        .set_default_partition_spec(-1)
        .expect("default spec")
        .build()
        .expect("metadata with historical partition")
        .metadata;
    assert!(warehouse.metadata.current_schema().field_by_id(2).is_none());
    assert!(
        warehouse
            .metadata
            .schemas_iter()
            .any(|schema| schema.field_by_id(2).is_some())
    );
    std::fs::write(
        warehouse
            .metadata_location
            .strip_prefix("file://")
            .expect("local metadata"),
        serde_json::to_vec(&warehouse.metadata).expect("metadata json"),
    )
    .expect("metadata write");
}

#[test]
fn renamed_partition_has_one_type_across_alias_provider_frozen_split_and_be() {
    use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
    use novarocks_type_contract::ValueTypeStorageLifetime;
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().unwrap();
    let mut warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let mut frozen = serde_json::to_value(&warehouse.metadata).unwrap();
    let old = warehouse
        .metadata
        .partition_specs_iter()
        .find(|spec| !spec.fields().is_empty())
        .unwrap();
    let mut renamed = serde_json::to_value(old.as_ref()).unwrap();
    renamed["spec-id"] = serde_json::json!(1);
    renamed["fields"][0]["name"] = serde_json::json!("renamed_region");
    frozen["partition-specs"]
        .as_array_mut()
        .unwrap()
        .push(renamed);
    frozen["default-spec-id"] = serde_json::json!(1);
    warehouse.metadata = serde_json::from_value(frozen).unwrap();
    std::fs::write(
        warehouse.metadata_location.strip_prefix("file://").unwrap(),
        serde_json::to_vec(&warehouse.metadata).unwrap(),
    )
    .unwrap();
    let expected = DataType::Struct(
        vec![Arc::new(Field::new("renamed_region", DataType::Utf8, true))].into(),
    );
    assert_eq!(
        partition_row_type_with_metadata(&warehouse.metadata).unwrap(),
        Some(expected.clone())
    );
    let mut request = |_| Ok(());
    let mut work = |_| Ok(());
    let mut creation = ConnectorReadSchemaCreation::new(
        ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut work,
    );
    let alias = crate::metadata_batch_reader::metadata_table_output_columns(
        crate::metadata_batch_reader::MetadataTableType::Files,
        &warehouse.metadata,
        &mut creation,
    )
    .unwrap();
    let alias = crate::metadata_batch_reader::metadata_output_schema(&alias).unwrap();
    let provider_schema = system_relation_schema_with_metadata(
        IcebergSystemTableType::Files,
        &warehouse.metadata,
    )
    .unwrap();
    assert_eq!(alias.fields(), provider_schema.fields());
    assert_eq!(
        provider_schema
            .field_with_name("partition")
            .unwrap()
            .data_type(),
        &expected
    );
    let (split, columns) = first_manifest_split(&runtime, &binding, &context, &warehouse);
    assert_eq!(split.partition_slot_field_ids(), &[1000]);
    let frozen_type = super::super::column_handle::parse_type(
        split.partition_column_type_json().unwrap(),
        "partition",
    )
    .unwrap();
    assert_eq!(
        frozen_type,
        metadata_relation_type(&expected, &mut MetadataRelationFieldIds::new()).unwrap()
    );
    let be = FilesSplitRelation::of(&split, &columns).unwrap();
    assert_eq!(be.schema.fields(), provider_schema.fields());
    let pages = files_pages(&runtime, &provider(&binding, &context), &split, &columns).unwrap();
    let partition = pages[0][17].as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        partition
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "east"
    );
}

fn assert_metadata_columns_compile(
    kind: IcebergSystemTableType,
    columns: &[IcebergColumnHandle],
    schema: &SchemaRef,
) {
    use novarocks_type_contract::{CompilePhase, PureCompileControl};
    struct Active;
    impl PureCompileControl for Active {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    let mut checkpoints =
        CompileCheckpoints::try_new(&Active, CompilePhase::LowerProgram).unwrap();
    for (column, field) in columns.iter().zip(schema.fields()) {
        system_column_field_for_compile(
            kind,
            column,
            field,
            ValueLogicalType::Physical,
            &mut checkpoints,
        )
        .unwrap();
    }
    checkpoints.finish().unwrap();
}

fn assert_historical_manifest_summary(
    runtime: &tokio::runtime::Runtime,
    provider: &IcebergSystemTableProvider,
    warehouse: &Warehouse,
    expected: &str,
    count: usize,
) {
    let reference = reference(
        warehouse,
        IcebergSystemTableType::Manifests,
        Some(SNAPSHOT_ID),
    );
    let schema = system_relation_schema_with_metadata(
        IcebergSystemTableType::Manifests,
        &warehouse.metadata,
    )
    .unwrap();
    let pages =
        single_backend_pages(runtime, provider, &reference, &all_columns(&schema)).unwrap();
    let summaries = pages[0][11].as_any().downcast_ref::<ListArray>().unwrap();
    for ordinal in 0..count {
        let value = summaries.value(ordinal);
        let fields = value.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            fields
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            expected
        );
    }
}

#[test]
fn dropped_partition_source_is_absent_from_all_metadata_authors_and_runtime() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().unwrap();
    let mut warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    drop_partition_source(&mut warehouse);
    use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
    use novarocks_type_contract::ValueTypeStorageLifetime;
    let specs = warehouse
        .metadata
        .partition_specs_iter()
        .map(|spec| spec.as_ref().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        partition_row_type_with_metadata(&warehouse.metadata).unwrap(),
        None
    );
    assert_eq!(
        partition_row_type(warehouse.metadata.current_schema(), &specs).unwrap(),
        None
    );
    for (kind, catalog_kind, count) in [
        (
            IcebergSystemTableType::Files,
            crate::metadata_batch_reader::MetadataTableType::Files,
            17,
        ),
        (
            IcebergSystemTableType::Entries,
            crate::metadata_batch_reader::MetadataTableType::LogicalIcebergMetadata,
            21,
        ),
    ] {
        let schema = system_relation_schema_with_metadata(kind, &warehouse.metadata).unwrap();
        assert_eq!(schema.fields().len(), count);
        assert!(schema.field_with_name("partition").is_err());
        let columns = system_relation_columns_with_metadata(kind, &warehouse.metadata).unwrap();
        let mut request = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        let alias = crate::metadata_batch_reader::metadata_table_output_columns(
            catalog_kind,
            &warehouse.metadata,
            &mut creation,
        )
        .unwrap();
        let alias = crate::metadata_batch_reader::metadata_output_schema(&alias).unwrap();
        let borrowed = columns.iter().collect::<Vec<_>>();
        let public = publish_read_schema_with_metadata(
            kind,
            &borrowed,
            &warehouse.metadata,
            &mut creation,
        )
        .unwrap();
        assert_eq!(alias.fields(), schema.fields());
        assert_eq!(public.schema().fields(), schema.fields());
        assert_metadata_columns_compile(kind, &columns, &schema);
    }
    let provider = provider(&binding, &context);
    let entries = reference(
        &warehouse,
        IcebergSystemTableType::Entries,
        Some(SNAPSHOT_ID),
    );
    let schema = system_relation_schema_with_metadata(
        IcebergSystemTableType::Entries,
        &warehouse.metadata,
    )
    .unwrap();
    let pages =
        single_backend_pages(&runtime, &provider, &entries, &all_columns(&schema)).unwrap();
    assert_eq!(pages[0].len(), 21);
    assert_eq!(pages[0][0].len(), 3);
    let (split, columns) = first_manifest_split(&runtime, &binding, &context, &warehouse);
    assert!(split.partition_slot_field_ids().is_empty());
    assert!(split.partition_column_type_json().is_none());
    let pages = files_pages(&runtime, &provider, &split, &columns).unwrap();
    assert_eq!(pages[0].len(), 17);
    assert_historical_manifest_summary(&runtime, &provider, &warehouse, "east", 1);
}

/// Real Avro facts retain the same source through Int -> Long -> removal.
/// Both physical literal widths occur under the same spec in one snapshot.
async fn build_promoted_partition_warehouse(
    dir: &Path,
    binding: IcebergReadBinding,
) -> Warehouse {
    let location = format!("file://{}", dir.display());
    std::fs::create_dir_all(dir.join("metadata")).unwrap();
    let schema = |id, region| {
        let mut fields = vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))];
        if let Some(region) = region {
            fields.push(Arc::new(NestedField::optional(
                2,
                "region",
                Type::Primitive(region),
            )));
        }
        Schema::builder()
            .with_schema_id(id)
            .with_fields(fields)
            .build()
            .unwrap()
    };
    let old = schema(0, Some(PrimitiveType::Int));
    let promoted = schema(1, Some(PrimitiveType::Long));
    let dropped = schema(2, None);
    let spec = identity_spec(&old, 0, "region");
    let file_io = crate::fs_io::build_file_io_for_location(&location, binding);
    let mut manifests = Vec::new();
    for (epoch, physical_schema, values) in [
        (0, old.clone(), vec![Literal::int(-7)]),
        (
            1,
            promoted.clone(),
            vec![Literal::long(-7), Literal::long(3_000_000_000_i64)],
        ),
    ] {
        let path = format!("{location}/metadata/epoch-{epoch}.avro");
        let mut writer = ManifestWriterBuilder::new(
            file_io.new_output(&path).unwrap(),
            Some(SNAPSHOT_ID),
            Arc::new(physical_schema),
            spec.clone(),
        )
        .build_v2_data();
        for (ordinal, value) in values.into_iter().enumerate() {
            let file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("data/{epoch}-{ordinal}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .partition(Struct::from_iter([Some(value)]))
                .partition_spec_id(0)
                .record_count(1)
                .file_size_in_bytes(128)
                .build()
                .unwrap();
            writer.add_file(file, epoch + 1).unwrap();
        }
        let mut manifest = writer.write_manifest_file().await.unwrap();
        manifest.sequence_number = epoch + 1;
        manifest.min_sequence_number = epoch + 1;
        manifests.push(manifest);
    }
    let manifest_list_path = format!("{location}/metadata/snap-{SNAPSHOT_ID}.avro");
    let mut writer = ManifestListWriter::v2(
        file_io
            .new_output(&manifest_list_path)
            .unwrap()
            .writer()
            .await
            .unwrap(),
        SNAPSHOT_ID,
        None,
        SNAPSHOT_SEQUENCE_NUMBER,
    );
    writer.add_manifests(manifests.into_iter()).unwrap();
    writer.close().await.unwrap();
    let snapshot = Snapshot::builder()
        .with_snapshot_id(SNAPSHOT_ID)
        .with_sequence_number(SNAPSHOT_SEQUENCE_NUMBER)
        .with_timestamp_ms(1_700_000_000_000)
        .with_manifest_list(manifest_list_path)
        .with_schema_id(2)
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .build();
    let metadata = TableMetadataBuilder::new(
        old,
        spec.into_unbound(),
        SortOrder::unsorted_order(),
        location,
        FormatVersion::V2,
        HashMap::new(),
    )
    .unwrap()
    .add_current_schema(promoted)
    .unwrap()
    .add_current_schema(dropped)
    .unwrap()
    .add_partition_spec(PartitionSpec::unpartition_spec().into_unbound())
    .unwrap()
    .set_default_partition_spec(-1)
    .unwrap()
    .add_snapshot(snapshot)
    .unwrap()
    .set_ref(
        "main",
        SnapshotReference::new(
            SNAPSHOT_ID,
            SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        ),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let metadata_location = format!("file://{}/metadata/frozen.metadata.json", dir.display());
    std::fs::write(
        dir.join("metadata/frozen.metadata.json"),
        serde_json::to_vec(&metadata).unwrap(),
    )
    .unwrap();
    Warehouse {
        metadata_location,
        metadata,
    }
}

#[test]
fn dropped_promoted_partition_source_has_no_canonical_slot_or_output_column() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().unwrap();
    let warehouse = runtime.block_on(build_promoted_partition_warehouse(
        dir.path(),
        binding.clone(),
    ));
    use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
    use novarocks_type_contract::ValueTypeStorageLifetime;
    let specs = warehouse
        .metadata
        .partition_specs_iter()
        .map(|spec| spec.as_ref().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        partition_row_type_with_metadata(&warehouse.metadata).unwrap(),
        None
    );
    assert_eq!(
        partition_row_type(warehouse.metadata.current_schema(), &specs).unwrap(),
        None
    );
    for (kind, catalog_kind, count) in [
        (
            IcebergSystemTableType::Files,
            crate::metadata_batch_reader::MetadataTableType::Files,
            17,
        ),
        (
            IcebergSystemTableType::Entries,
            crate::metadata_batch_reader::MetadataTableType::LogicalIcebergMetadata,
            21,
        ),
    ] {
        let schema = system_relation_schema_with_metadata(kind, &warehouse.metadata).unwrap();
        assert_eq!(schema.fields().len(), count);
        assert!(schema.field_with_name("partition").is_err());
        let columns = system_relation_columns_with_metadata(kind, &warehouse.metadata).unwrap();
        let mut request = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        let alias = crate::metadata_batch_reader::metadata_table_output_columns(
            catalog_kind,
            &warehouse.metadata,
            &mut creation,
        )
        .unwrap();
        let alias = crate::metadata_batch_reader::metadata_output_schema(&alias).unwrap();
        let borrowed = columns.iter().collect::<Vec<_>>();
        let public = publish_read_schema_with_metadata(
            kind,
            &borrowed,
            &warehouse.metadata,
            &mut creation,
        )
        .unwrap();
        assert_eq!(alias.fields(), schema.fields());
        assert_eq!(public.schema().fields(), schema.fields());
        assert_metadata_columns_compile(kind, &columns, &schema);
    }
    let provider = provider(&binding, &context);
    let entries = reference(
        &warehouse,
        IcebergSystemTableType::Entries,
        Some(SNAPSHOT_ID),
    );
    let schema = system_relation_schema_with_metadata(
        IcebergSystemTableType::Entries,
        &warehouse.metadata,
    )
    .unwrap();
    let pages =
        single_backend_pages(&runtime, &provider, &entries, &all_columns(&schema)).unwrap();
    assert_eq!(pages[0].len(), 21);
    assert_eq!(pages[0][0].len(), 3);
    let (split, columns) = first_manifest_split(&runtime, &binding, &context, &warehouse);
    assert!(split.partition_slot_field_ids().is_empty());
    assert!(split.partition_column_type_json().is_none());
    let pages = files_pages(&runtime, &provider, &split, &columns).unwrap();
    assert_eq!(pages[0].len(), 17);
    assert_historical_manifest_summary(&runtime, &provider, &warehouse, "-7", 2);
}

#[test]
fn historical_partition_normalization_preserves_unrelated_literal_rejection() {
    let field = Arc::new(Field::new("region", DataType::Int64, true));
    let declaration = DataType::Struct(vec![field].into());
    let bad = vec![Some(Literal::string("-7"))];
    assert_eq!(
        partition_struct_array(&declaration, &[&bad])
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::CorruptData
    );
    let declaration =
        DataType::Struct(vec![Arc::new(Field::new("region", DataType::Float64, true))].into());
    let old = vec![Some(Literal::float(1.5_f32))];
    let values = partition_struct_array(&declaration, &[&old]).unwrap();
    let values = values.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        values
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.5
    );
}

#[test]
fn frozen_files_split_rejects_foreign_partition_types_and_slot_field_ids() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let (split, columns) = first_manifest_split(&runtime, &binding, &context, &warehouse);
    for partition in [
        None,
        Some(
            derived_row_type_json(&DataType::Struct(
                vec![Arc::new(Field::new("foreign", DataType::Utf8, true))].into(),
            ))
            .unwrap(),
        ),
    ] {
        let changed = SystemManifestSplit::try_new(SystemManifestSplitParams {
            reference: split.reference().clone(),
            table_has_variant_history: split.table_has_variant_history(),
            manifest: split.manifest().clone(),
            table_schema_json: split.table_schema_json().to_owned(),
            metadata_table_schema_json: split.metadata_table_schema_json().to_owned(),
            partition_spec_jsons: split.partition_spec_jsons().clone(),
            partition_column_type_json: partition,
            partition_slot_field_ids: split.partition_slot_field_ids().to_vec(),
            bounds_column_type_json: split.bounds_column_type_json().map(str::to_owned),
            encryption_key_id: None,
        })
        .unwrap();
        let error = FilesSplitRelation::of(&changed, &columns)
            .err()
            .expect("missing/foreign declaration rejected");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }
    for ids in [Vec::new(), vec![9999]] {
        let mut raw = split.to_proto();
        raw.partition_slot_field_ids = ids;
        let changed = SystemManifestSplit::from_proto(&raw).unwrap();
        assert_eq!(
            FilesSplitRelation::of(&changed, &columns)
                .err()
                .unwrap()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
}

struct ManifestReadFailureSpawner {
    kind: novarocks_fs::FileErrorKind,
    message: &'static str,
    attempts: std::sync::atomic::AtomicUsize,
}

impl FileIoSpawner for ManifestReadFailureSpawner {
    fn spawn(
        &self,
        _task: novarocks_fs::FileIoFuture,
    ) -> novarocks_fs::FileResult<novarocks_fs::FileIoJob> {
        self.attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(novarocks_fs::FileError::new(self.kind, self.message))
    }
}

fn split_with_variant_history(
    split: &SystemManifestSplit,
    history: bool,
) -> SystemManifestSplit {
    SystemManifestSplit::try_new(SystemManifestSplitParams {
        reference: split.reference().clone(),
        table_has_variant_history: history,
        manifest: split.manifest().clone(),
        table_schema_json: split.table_schema_json().to_owned(),
        metadata_table_schema_json: split.metadata_table_schema_json().to_owned(),
        partition_spec_jsons: split.partition_spec_jsons().clone(),
        partition_column_type_json: split.partition_column_type_json().map(str::to_owned),
        partition_slot_field_ids: split.partition_slot_field_ids().to_vec(),
        bounds_column_type_json: split.bounds_column_type_json().map(str::to_owned),
        encryption_key_id: None,
    })
    .expect("same manifest with a frozen history fact")
}

fn assert_manifest_stream_preserves_failure(
    runtime: &tokio::runtime::Runtime,
    binding: &IcebergReadBinding,
    context: &FileReadContext,
    split: &SystemManifestSplit,
    columns: &[IcebergColumnHandle],
    original: &ConnectorError,
) {
    assert!(original.message().contains(split.manifest().path()));
    assert!(!original.message().contains("hint:"));
    for history in [false, true] {
        let split = split_with_variant_history(split, history);
        let budget = ConnectorPollBudget::new();
        let stream = provider(binding, context)
            .create_manifest_page_stream(&split, columns, &budget)
            .expect("manifest page stream");
        let mut stream = DrivenStream::new(runtime, stream, budget);
        let actual = match stream.next_page() {
            Err(error) => error,
            Ok(_) => panic!("manifest failure must precede any page"),
        };
        assert!(stream.is_finished());
        stream.close().expect("manifest read physically exited");

        assert_eq!(actual.kind(), original.kind());
        assert_eq!(
            actual.retryable_before_progress(),
            original.retryable_before_progress()
        );
        assert_eq!(
            actual.table_object_binding_failure(),
            original.table_object_binding_failure()
        );
        // Full equality also checks the private cleanup/binding facts.
        // Only the diagnostic suffix may differ from the actual reader.
        assert_eq!(
            actual
                .clone()
                .map_message(|_| original.message().to_owned()),
            *original
        );
        if history {
            assert!(actual.message().starts_with(original.message()));
            let suffix = &actual.message()[original.message().len()..];
            assert!(suffix.contains("hint: this table has had Variant columns"));
            assert!(suffix.contains("may be related"));
            assert!(suffix.contains("docs/guides/iceberg-v3/variant.md"));
        } else {
            assert_eq!(actual, *original, "no Variant history adds no hint");
        }
    }
}

fn injected_manifest_read_preserves_failure(
    kind: novarocks_fs::FileErrorKind,
    message: &'static str,
) {
    let (runtime, binding, mut context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let (split, columns) = first_manifest_split(&runtime, &binding, &context, &warehouse);
    assert!(
        !files_pages(&runtime, &provider(&binding, &context), &split, &columns)
            .expect("the real manifest is initially readable")
            .is_empty()
    );

    // Install the failing dispatch only after all setup reads. Each
    // remaining read is the exact local manifest named by this split.
    let spawner = Arc::new(ManifestReadFailureSpawner {
        kind,
        message,
        attempts: std::sync::atomic::AtomicUsize::new(0),
    });
    let operations =
        route_reads_through(&mut context, spawner.clone(), runtime.handle().clone());
    let original = runtime
        .block_on(FrozenMetadataReader::new(&binding, &context).load_manifest(split.manifest()))
        .expect_err("manifest read dispatch is injected to fail");
    assert!(original.message().contains(message));
    // The pre-existing metadata reader deliberately maps both storage
    // permission and transient read failures to retryable Unavailable.
    assert_eq!(original.kind(), ConnectorErrorKind::Unavailable);
    assert!(original.retryable_before_progress());
    assert_manifest_stream_preserves_failure(
        &runtime, &binding, &context, &split, &columns, &original,
    );
    assert_eq!(
        spawner.attempts.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "the raw reader and both history branches must hit the injected read"
    );
    assert_eq!(operations.live_operations(), 0);
}

#[test]
fn files_manifest_permission_failure_preserves_facts_and_adds_only_history_hint() {
    injected_manifest_read_preserves_failure(
        novarocks_fs::FileErrorKind::Permission,
        "injected permission denial reading the selected manifest",
    );
}

#[test]
fn files_manifest_network_failure_preserves_facts_and_adds_only_history_hint() {
    injected_manifest_read_preserves_failure(
        novarocks_fs::FileErrorKind::Transient,
        "injected connection reset reading the selected manifest",
    );
}

#[test]
fn files_manifest_corrupt_avro_preserves_facts_and_adds_only_history_hint() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let (split, columns) = first_manifest_split(&runtime, &binding, &context, &warehouse);
    assert!(
        !files_pages(&runtime, &provider(&binding, &context), &split, &columns)
            .expect("the real manifest is initially readable")
            .is_empty()
    );
    let path = split.manifest().path().strip_prefix("file://").unwrap();
    let length = std::fs::metadata(path).expect("manifest metadata").len();
    // Keep the frozen file length valid so this reaches the real Avro
    // parser rather than failing an unrelated short-read contract.
    std::fs::write(path, vec![0; usize::try_from(length).unwrap()])
        .expect("replace the actual manifest with corrupt Avro");
    let original = runtime
        .block_on(FrozenMetadataReader::new(&binding, &context).load_manifest(split.manifest()))
        .expect_err("the real manifest bytes are corrupt");
    assert_eq!(original.kind(), ConnectorErrorKind::CorruptData);
    assert!(!original.retryable_before_progress());
    assert!(original.message().contains("is invalid"));
    assert_manifest_stream_preserves_failure(
        &runtime, &binding, &context, &split, &columns, &original,
    );
}

#[test]
fn every_single_backend_stream_emits_what_its_page_source_emits() {
    let (runtime, binding, context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let provider = provider(&binding, &context);
    let specs = warehouse
        .metadata
        .partition_specs_iter()
        .map(|spec| spec.as_ref().clone())
        .collect::<Vec<_>>();
    for relation in [
        IcebergSystemTableType::Snapshots,
        IcebergSystemTableType::History,
        IcebergSystemTableType::Refs,
        IcebergSystemTableType::Manifests,
    ] {
        let reference = reference(&warehouse, relation, Some(SNAPSHOT_ID));
        let schema =
            system_relation_schema(relation, warehouse.metadata.current_schema(), &specs)
                .expect("schema");
        let columns = all_columns(&schema);
        // One unit for the whole relation, so every later unit of work
        // yields its turn first and the yields count the units spent.
        let budget = ConnectorPollBudget::new();
        budget.refill(1);
        let mut stream = provider
            .create_page_stream(
                &reference,
                &super::super::IcebergReadSplit::SystemRelation(
                    SystemRelationSplit::try_new(reference.clone()).unwrap(),
                ),
                &columns,
                &budget,
            )
            .expect("page stream");
        let streamed = drain_stream(&runtime, &mut stream);
        runtime
            .block_on(stream.close())
            .expect("closing a drained stream");

        assert!(!streamed.is_empty(), "{relation:?} has rows");
        let spent = streamed.len() as u64;
        assert_eq!(budget.exhaustions(), spent - 1, "{relation:?}");
    }
}

#[test]
fn a_files_split_stream_reads_nothing_until_polled_then_reads_its_one_manifest() {
    let (runtime, binding, mut context) = runtime_and_binding();
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = runtime.block_on(build_warehouse(dir.path(), binding.clone()));
    let (split, columns) = first_manifest_split(&runtime, &binding, &context, &warehouse);
    let expected =
        files_pages(&runtime, &provider(&binding, &context), &split, &columns).expect("pages");

    let spawner = GatedFileIoSpawner::closed(runtime.handle().clone());
    let task_operations =
        route_reads_through(&mut context, spawner.clone(), runtime.handle().clone());
    let budget = ConnectorPollBudget::new();
    let mut stream = provider(&binding, &context)
        .create_manifest_page_stream(&split, &columns, &budget)
        .expect("page stream");
    assert_eq!(spawner.started(), 0, "creating the stream reads nothing");
    spawner.release(64);
    budget.refill(1024);
    let streamed = drain_stream(&runtime, &mut stream);
    assert_eq!(spawner.started(), 1, "one read of the split's manifest");
    assert_eq!(streamed, expected);
    runtime
        .block_on(stream.close())
        .expect("closing a drained stream");
    assert_eq!(task_operations.live_operations(), 0);
}
