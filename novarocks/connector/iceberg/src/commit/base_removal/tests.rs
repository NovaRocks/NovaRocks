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

use std::collections::BTreeMap;
use std::error::Error as _;
use std::sync::Arc;

use novarocks_spi::connector::ConnectorMutationFailure;

use super::*;
use crate::commit::model::EntryIdentity;
use crate::iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, FieldSummary, FormatVersion,
    Literal, ManifestFile, ManifestListWriter, ManifestWriterBuilder, NestedField, Operation,
    PartitionSpec, PrimitiveType, Schema, Snapshot, SnapshotReference, SnapshotRetention,
    SortOrder, Struct, Summary, TableMetadata, TableMetadataBuilder, Transform, Type,
};

const E: i64 = 21;
const B: i64 = 22;
const DATA: &str = "memory:///data.parquet";
const OTHER: &str = "memory:///other.parquet";
const EQUALITY: &str = "memory:///equality.parquet";
const DV: &str = "memory:///new.puffin";
const B_DATA_MANIFEST: &str = "memory:///b-data.avro";
const B_DELETE_MANIFEST: &str = "memory:///b-delete.avro";

fn partition(value: i32) -> Struct {
    Struct::from_iter([Some(Literal::int(value))])
}

fn data(path: &str, row_id: Option<i64>) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_owned())
        .file_format(DataFileFormat::Parquet)
        .partition(partition(7))
        .record_count(5)
        .file_size_in_bytes(100)
        .first_row_id(row_id)
        .build()
        .unwrap()
}

fn equality() -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::EqualityDeletes)
        .file_path(EQUALITY.to_owned())
        .file_format(DataFileFormat::Parquet)
        .partition(partition(9))
        .record_count(1)
        .file_size_in_bytes(100)
        .equality_ids(Some(vec![1]))
        .build()
        .unwrap()
}

fn vector() -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(DV.to_owned())
        .file_format(DataFileFormat::Puffin)
        .partition(partition(7))
        .record_count(1)
        .file_size_in_bytes(100)
        .content_offset(Some(4))
        .content_size_in_bytes(Some(12))
        .referenced_data_file(Some(DATA.to_owned()))
        .build()
        .unwrap()
}

fn metadata() -> TableMetadata {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "p",
            Type::Primitive(PrimitiveType::Int),
        ))])
        .build()
        .unwrap();
    let spec = PartitionSpec::builder(Arc::new(schema.clone()))
        .add_partition_field("p", "p_key", Transform::Identity)
        .unwrap()
        .build()
        .unwrap();
    TableMetadataBuilder::new(
        schema,
        spec.into_unbound(),
        SortOrder::unsorted_order(),
        "memory:///table".into(),
        FormatVersion::V3,
        Default::default(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}

async fn snapshot(
    io: &FileIO,
    metadata: TableMetadata,
    snapshot_id: i64,
    sequence: i64,
    manifests: impl IntoIterator<Item = ManifestFile>,
) -> TableMetadata {
    let list_path = format!("memory:///{snapshot_id}-list.avro");
    let first_row_id = metadata.next_row_id();
    let parent = metadata.current_snapshot_id();
    let mut list = ManifestListWriter::v3(
        io.new_output(&list_path).unwrap().writer().await.unwrap(),
        snapshot_id,
        parent,
        sequence,
        Some(first_row_id),
    );
    list.add_manifests(manifests.into_iter()).unwrap();
    let added_rows = list.next_row_id().unwrap() - first_row_id;
    list.close().await.unwrap();
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent)
        .with_sequence_number(sequence)
        .with_timestamp_ms(metadata.last_updated_ms() + 1)
        .with_manifest_list(list_path)
        .with_summary(Summary {
            operation: if parent.is_some() {
                Operation::Replace
            } else {
                Operation::Append
            },
            additional_properties: Default::default(),
        })
        .with_schema_id(metadata.current_schema_id())
        .with_row_range(first_row_id, added_rows)
        .build();
    metadata
        .into_builder(None)
        .add_snapshot(snapshot)
        .unwrap()
        .set_ref(
            "main",
            SnapshotReference::new(
                snapshot_id,
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
        .metadata
}

async fn fixture(
    with_vector: bool,
) -> (
    ManifestReadScope,
    BTreeMap<EntryIdentity, LocatedEntry>,
    FileIO,
) {
    let io = FileIO::new_with_memory();
    let metadata = metadata();
    let writer = |path, id| {
        ManifestWriterBuilder::new(
            io.new_output(path).unwrap(),
            Some(id),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
    };
    let mut original_data = writer("memory:///e-data.avro", E).build_v3_data();
    original_data.add_file(data(DATA, None), 1).unwrap();
    original_data.add_file(data(OTHER, None), 1).unwrap();
    let mut original_deletes = writer("memory:///e-delete.avro", E).build_v3_deletes();
    original_deletes.add_file(equality(), 1).unwrap();
    let source = snapshot(
        &io,
        metadata.clone(),
        E,
        1,
        [
            original_data.write_manifest_file().await.unwrap(),
            original_deletes.write_manifest_file().await.unwrap(),
        ],
    )
    .await;
    let entries =
        SnapshotSources::load(&source, source.current_snapshot().unwrap(), &io, || Ok(()))
            .await
            .unwrap()
            .live_entries(&io, || Ok(()))
            .await
            .unwrap();
    let mut rewritten_data = writer(B_DATA_MANIFEST, B).build_v3_data();
    rewritten_data
        .add_existing_file(data(OTHER, Some(5)), E, 1, Some(1))
        .unwrap();
    rewritten_data
        .add_existing_file(data(DATA, Some(0)), E, 1, Some(1))
        .unwrap();
    let mut data_manifest = rewritten_data.write_manifest_file().await.unwrap();
    // All live entries already have explicit IDs from E.
    data_manifest.first_row_id = Some(0);
    let mut rewritten_deletes = writer(B_DELETE_MANIFEST, B).build_v3_deletes();
    rewritten_deletes
        .add_existing_file(equality(), E, 1, Some(1))
        .unwrap();
    if with_vector {
        rewritten_deletes.add_file(vector(), 2).unwrap();
    }
    let delete_manifest = rewritten_deletes.write_manifest_file().await.unwrap();

    // Successful lookup proves that a disjoint manifest was never opened.
    let bound = Datum::int(99).to_bytes().unwrap();
    let disjoint = ManifestFile {
        manifest_path: "memory:///unreadable-unrelated.avro".to_owned(),
        partitions: Some(vec![FieldSummary {
            contains_null: false,
            contains_nan: Some(false),
            lower_bound: Some(bound.clone()),
            upper_bound: Some(bound),
        }]),
        ..data_manifest.clone()
    };
    let base = snapshot(
        &io,
        source.clone(),
        B,
        2,
        [delete_manifest, disjoint, data_manifest],
    )
    .await;
    let source_scope = ManifestReadScope::new(Arc::new(source), E).unwrap();
    (source_scope.rebind(Arc::new(base), B).unwrap(), entries, io)
}

fn data_identity(path: &str) -> EntryIdentity {
    EntryIdentity::DataFile {
        path: path.to_owned(),
    }
}

#[tokio::test]
async fn locates_rewritten_data_and_equality_under_their_own_partitions() {
    let (mut scope, entries, io) = fixture(false).await;
    let data = &entries[&data_identity(DATA)].frozen;
    let equality = &entries[&EntryIdentity::DeleteFile {
        path: EQUALITY.to_owned(),
    }]
        .frozen;
    let found = locate_on_base(&mut scope, [data, equality], &io, || Ok(()))
        .await
        .unwrap();
    let sources = scope.snapshot_sources().unwrap();
    let paths: Vec<_> = sources
        .sources()
        .map(|source| source.descriptor.manifest_path.as_str())
        .collect();
    assert_eq!(found.len(), 2);
    assert_eq!(&found[0].frozen, data);
    assert_eq!(found[0].ordinal, 1);
    assert_eq!(paths[found[0].source_index], B_DATA_MANIFEST);
    assert_eq!(&found[1].frozen, equality);
    assert_eq!(found[1].content_type, DataContentType::EqualityDeletes);
    assert_eq!(paths[found[1].source_index], B_DELETE_MANIFEST);
}

#[tokio::test]
async fn adds_the_current_vector_when_removing_its_data_file() {
    let (mut scope, entries, io) = fixture(true).await;
    let original = &entries[&data_identity(DATA)].frozen;
    let found = locate_on_base(&mut scope, [original], &io, || Ok(()))
        .await
        .unwrap();
    assert_eq!(found.len(), 2);
    assert_eq!(
        found[1].frozen.identity(),
        &EntryIdentity::DeletionVector {
            path: DV.to_owned(),
            offset: 4,
            length: 12,
            referenced_data_file: DATA.to_owned(),
        }
    );
    assert_eq!(found[1].frozen.facts().added_snapshot_id, Some(B));
    assert_eq!(found[1].frozen.facts().data_sequence, Some(2));
}

#[tokio::test]
async fn missing_or_changed_required_entry_is_a_validation_conflict() {
    let (mut scope, entries, io) = fixture(false).await;
    let original = &entries[&data_identity(DATA)].frozen;
    let mut changed = original.facts().clone();
    changed.record_count += 1;
    let required = [
        FrozenEntry::new(
            data_identity("memory:///absent.parquet"),
            original.facts().clone(),
        )
        .unwrap(),
        FrozenEntry::new(data_identity(DATA), changed).unwrap(),
    ];
    for entry in &required {
        let err = locate_on_base(&mut scope, [entry], &io, || Ok(()))
            .await
            .unwrap_err();
        let cause = err
            .source()
            .unwrap()
            .downcast_ref::<ConnectorMutationFailure>()
            .unwrap();
        assert_eq!(
            cause.message(),
            format!(
                "Missing required files to delete: {}",
                entry.identity().path()
            )
        );
    }
}
