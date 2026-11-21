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

use std::sync::Arc;

use novarocks_failure_contract::FailureProjection;
use novarocks_failure_contract::connector_reason::ICEBERG_COMMIT_VALIDATION_CONFLICT;
use novarocks_spi::connector::ConnectorMutationFailureKind;
use tempfile::TempDir;

use super::*;
use crate::commit::dependency::builtin::tests::{TestIo, append_snapshot};
use crate::commit::dependency::validate;
use crate::commit::model::{
    CommitProtocol, EntryFacts, OperationIntent, OperationIntentParts, RequestShape, TableTarget,
};
use crate::delete_semantics::POSITION_FILE_PATH_FIELD_ID;
use crate::iceberg::spec::{
    DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestListWriter,
    ManifestWriterBuilder, NestedField, PrimitiveType, Schema, Snapshot, SortOrder, Struct,
    Summary, TableMetadata, TableMetadataBuilder, Transform, Type, UnboundPartitionSpec,
};

const DATA: &str = "s3://bucket/data.parquet";
const OTHER: &str = "s3://bucket/other.parquet";
const START: StartSnapshot = StartSnapshot {
    snapshot_id: 11,
    sequence_number: 1,
};

struct Fixture {
    dir: TempDir,
    io: TestIo,
    metadata: TableMetadata,
}

impl Fixture {
    fn new(version: FormatVersion) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let schema = Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                1,
                "region",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(1, "region", Transform::Identity)
            .unwrap()
            .build();
        let metadata = TableMetadataBuilder::new(
            schema,
            spec,
            SortOrder::unsorted_order(),
            dir.path().to_str().unwrap().into(),
            version,
            Default::default(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let metadata = if version == FormatVersion::V1 {
            metadata
        } else {
            append_snapshot(metadata, START.snapshot_id, None, Operation::Append)
        };
        Self {
            dir,
            io: TestIo::new(),
            metadata,
        }
    }

    fn replaced(&self, spec_id: i32, partition: i32) -> FrozenEntry {
        FrozenEntry::new(
            EntryIdentity::DataFile { path: DATA.into() },
            EntryFacts {
                added_snapshot_id: Some(START.snapshot_id),
                data_sequence: Some(START.sequence_number),
                file_sequence: Some(START.sequence_number),
                partition_spec_id: spec_id,
                partition: partition_tuple(partition),
                record_count: 3,
                first_row_id: None,
            },
        )
        .unwrap()
    }

    async fn snapshot(
        &mut self,
        id: i64,
        parent: i64,
        operation: Operation,
        deletes: Vec<DeleteEntry>,
    ) {
        let version = self.metadata.format_version();
        let io = self.io.file_io();
        let seq = self.metadata.next_sequence_number();
        let first_row_id = self.metadata.next_row_id();
        let list_path = self.dir.path().join(format!("list-{id}.avro"));
        let list_path = list_path.to_str().unwrap();
        let list_writer = io.new_output(list_path).unwrap().writer().await.unwrap();
        let mut list = match version {
            FormatVersion::V2 => ManifestListWriter::v2(list_writer, id, Some(parent), seq),
            FormatVersion::V3 => {
                ManifestListWriter::v3(list_writer, id, Some(parent), seq, Some(first_row_id))
            }
            _ => unreachable!("Fixture writes Iceberg v2 or v3 delete manifests"),
        };
        if !deletes.is_empty() {
            let manifest_path = self.dir.path().join(format!("delete-{id}.avro"));
            let writer = ManifestWriterBuilder::new(
                io.new_output(manifest_path.to_str().unwrap()).unwrap(),
                Some(id),
                self.metadata.current_schema().clone(),
                self.metadata.default_partition_spec().as_ref().clone(),
            );
            let mut writer = match version {
                FormatVersion::V2 => writer.build_v2_deletes(),
                FormatVersion::V3 => writer.build_v3_deletes(),
                _ => unreachable!(),
            };
            for entry in deletes {
                match entry.owner {
                    None => writer.add_file(entry.file, entry.sequence),
                    Some(owner) => writer.add_existing_file(
                        entry.file,
                        owner,
                        entry.sequence,
                        Some(entry.sequence),
                    ),
                }
                .unwrap();
            }
            list.add_manifests([writer.write_manifest_file().await.unwrap()].into_iter())
                .unwrap();
        }
        let row_count = list.next_row_id().map(|next| next - first_row_id);
        list.close().await.unwrap();
        let builder = Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(Some(parent))
            .with_sequence_number(seq)
            .with_timestamp_ms(self.metadata.last_updated_ms() + 1)
            .with_manifest_list(list_path)
            .with_summary(Summary {
                operation,
                additional_properties: Default::default(),
            })
            .with_schema_id(self.metadata.current_schema_id());
        let snapshot = match row_count {
            Some(row_count) => builder.with_row_range(first_row_id, row_count).build(),
            None => builder.build(),
        };
        self.metadata = self
            .metadata
            .clone()
            .into_builder(None)
            .add_snapshot(snapshot)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
    }

    fn expire(&mut self, id: i64) {
        self.metadata = self
            .metadata
            .clone()
            .into_builder(None)
            .remove_snapshots(&[id])
            .build()
            .unwrap()
            .metadata;
    }

    async fn check(&self, parent: i64, replaced: FrozenEntry) -> Verdict {
        let intent = OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: crate::iceberg::TableIdent::from_strs(["ns", "test"]).unwrap(),
                uuid: Some(self.metadata.uuid()),
            },
            target_ref: "rewrite-target".into(),
            start: Some(START),
            changes: Default::default(),
            dependencies: Vec::new(),
            protocol: CommitProtocol::RewriteFiles {
                replaced: Arc::from([replaced]),
            },
            shape: RequestShape::SnapshotProducing,
            summary: Default::default(),
            token: self.io.attempt_token().operation(),
        })
        .unwrap();
        validate(&intent, &self.metadata, Some(parent), &self.io)
            .await
            .unwrap()
    }
}

struct DeleteEntry {
    file: DataFile,
    // None denotes a newly written entry with the snapshot's owner.
    owner: Option<i64>,
    sequence: i64,
}

fn added(file: DataFile) -> DeleteEntry {
    DeleteEntry {
        file,
        owner: None,
        sequence: -1,
    }
}

fn partition_tuple(value: i32) -> Struct {
    [Some(Literal::int(value))].into_iter().collect()
}

fn delete_file(path: &str, partition: i32) -> DataFileBuilder {
    let mut builder = DataFileBuilder::default();
    builder
        .file_path(path.into())
        .file_format(DataFileFormat::Parquet)
        .partition(partition_tuple(partition))
        .record_count(3)
        .file_size_in_bytes(256);
    builder
}

fn position(target: Option<&str>, partition: i32) -> DataFile {
    let bounds = || {
        target
            .into_iter()
            .map(|target| (POSITION_FILE_PATH_FIELD_ID, Datum::string(target)))
            .collect()
    };
    delete_file("s3://bucket/position.parquet", partition)
        .content(DataContentType::PositionDeletes)
        .lower_bounds(bounds())
        .upper_bounds(bounds())
        .build()
        .unwrap()
}

fn dv(target: &str) -> DataFile {
    delete_file("s3://bucket/delete.puffin", 1)
        .content(DataContentType::PositionDeletes)
        .file_format(DataFileFormat::Puffin)
        .referenced_data_file(Some(target.into()))
        .content_offset(Some(8))
        .content_size_in_bytes(Some(80))
        .build()
        .unwrap()
}

fn equality() -> DataFile {
    delete_file("s3://bucket/equality.parquet", 1)
        .content(DataContentType::EqualityDeletes)
        .equality_ids(Some(vec![1]))
        .build()
        .unwrap()
}

fn assert_conflict(verdict: Verdict, expected: &str) {
    let Verdict::ValidationConflict(error) = verdict else {
        panic!("Expected validation conflict, found {verdict:?}");
    };
    assert_eq!(error.kind(), ConnectorMutationFailureKind::Conflict);
    assert_eq!(
        error.failure_fact().reason(),
        ICEBERG_COMMIT_VALIDATION_CONFLICT
    );
    assert_eq!(error.message(), expected);
}

fn assert_new_delete(verdict: Verdict) {
    assert_conflict(
        verdict,
        "Cannot commit, found new position delete for replaced data file: s3://bucket/data.parquet",
    );
}

#[tokio::test]
async fn expired_start_with_intact_chain_passes_and_gap_reports_last_known_ancestor() {
    let mut fixture = Fixture::new(FormatVersion::V2);
    fixture.snapshot(12, 11, Operation::Append, vec![]).await;
    fixture.snapshot(13, 12, Operation::Overwrite, vec![]).await;
    let replaced = fixture.replaced(fixture.metadata.default_partition_spec_id(), 1);
    fixture.expire(11);
    assert_eq!(fixture.check(13, replaced.clone()).await, Verdict::Holds);
    fixture.expire(12);
    assert_conflict(
        fixture.check(13, replaced).await,
        "Cannot determine history between starting snapshot 11 and the last known ancestor 13",
    );
}

#[tokio::test]
async fn overwrite_and_delete_snapshots_detect_position_deletes_and_dvs() {
    for operation in [Operation::Overwrite, Operation::Delete] {
        for file in [position(Some(DATA), 1), dv(DATA)] {
            let mut fixture = Fixture::new(FormatVersion::V3);
            fixture
                .snapshot(12, 11, operation.clone(), vec![added(file)])
                .await;
            let replaced = fixture.replaced(fixture.metadata.default_partition_spec_id(), 1);
            assert_new_delete(fixture.check(12, replaced).await);
        }
    }
}

#[tokio::test]
async fn replace_dvs_equality_and_other_target_paths_do_not_conflict() {
    for (operation, file) in [
        (Operation::Replace, dv(DATA)),
        (Operation::Overwrite, equality()),
        (Operation::Overwrite, dv(OTHER)),
        (Operation::Delete, position(Some(OTHER), 1)),
    ] {
        let mut fixture = Fixture::new(FormatVersion::V3);
        fixture.snapshot(12, 11, operation, vec![added(file)]).await;
        let replaced = fixture.replaced(fixture.metadata.default_partition_spec_id(), 1);
        assert_eq!(fixture.check(12, replaced).await, Verdict::Holds);
    }
}

#[tokio::test]
async fn a_carried_replace_dv_does_not_become_an_overwrite_addition() {
    let mut fixture = Fixture::new(FormatVersion::V3);
    fixture
        .snapshot(12, 11, Operation::Replace, vec![added(dv(DATA))])
        .await;
    fixture
        .snapshot(
            13,
            12,
            Operation::Overwrite,
            vec![DeleteEntry {
                file: dv(DATA),
                owner: Some(12),
                sequence: 2,
            }],
        )
        .await;
    let replaced = fixture.replaced(fixture.metadata.default_partition_spec_id(), 1);
    assert_eq!(fixture.check(13, replaced).await, Verdict::Holds);
}

#[tokio::test]
async fn multi_file_v2_positions_match_exact_spec_and_partition() {
    let mut fixture = Fixture::new(FormatVersion::V2);
    let old_spec = fixture.metadata.default_partition_spec_id();
    let same_tuple_other_spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "other_region", Transform::Identity)
        .unwrap()
        .build();
    fixture.metadata = fixture
        .metadata
        .clone()
        .into_builder(None)
        .add_partition_spec(same_tuple_other_spec)
        .unwrap()
        .set_default_partition_spec(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let new_spec = fixture.metadata.default_partition_spec_id();
    assert_ne!(old_spec, new_spec);
    fixture
        .snapshot(12, 11, Operation::Overwrite, vec![added(position(None, 1))])
        .await;
    assert_eq!(
        fixture.check(12, fixture.replaced(new_spec, 2)).await,
        Verdict::Holds
    );
    assert_eq!(
        fixture.check(12, fixture.replaced(old_spec, 1)).await,
        Verdict::Holds
    );
    assert_new_delete(fixture.check(12, fixture.replaced(new_spec, 1)).await);
}

#[tokio::test]
async fn position_delete_sequence_must_be_newer_than_frozen_start_even_after_expiry() {
    let mut fixture = Fixture::new(FormatVersion::V2);
    fixture
        .snapshot(
            12,
            11,
            Operation::Overwrite,
            vec![DeleteEntry {
                file: position(Some(DATA), 1),
                owner: None,
                sequence: START.sequence_number,
            }],
        )
        .await;
    fixture.expire(11);
    let replaced = fixture.replaced(fixture.metadata.default_partition_spec_id(), 1);
    assert_eq!(fixture.check(12, replaced).await, Verdict::Holds);
}

#[tokio::test]
async fn v1_skips_the_rewrite_files_validation_window() {
    let fixture = Fixture::new(FormatVersion::V1);
    let replaced = fixture.replaced(fixture.metadata.default_partition_spec_id(), 1);
    assert_eq!(fixture.check(99, replaced).await, Verdict::Holds);
}
