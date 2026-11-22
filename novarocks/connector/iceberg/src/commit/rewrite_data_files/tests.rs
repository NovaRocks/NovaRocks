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

use std::path::Path;
use std::sync::Arc;

use crate::commit::helpers::now_ms;
use crate::iceberg::TableIdent;
use crate::iceberg::spec::{
    DataFileBuilder, DataFileFormat, Literal, ManifestListWriter, ManifestWriterBuilder,
    NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema, Snapshot,
    SnapshotReference, SnapshotRetention, SortOrder, Struct, Summary, TableMetadataBuilder,
    Transform, Type,
};
use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner};

use super::*;

fn local_test_binding() -> crate::access_binding::IcebergReadBinding {
    let runtime = tokio::runtime::Handle::current();
    crate::access_binding::IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
        Arc::new(TokioFileIoSpawner::new(runtime)),
    )
}

const SNAPSHOT_ID: i64 = 100;
const SNAPSHOT_SEQUENCE_NUMBER: i64 = 12;

/// One synthetic live data manifest: `data_files` files that all share a
/// partition spec id, a partition value and a data sequence number.
struct ManifestPlan {
    partition_spec_id: i32,
    partition_value: i32,
    data_files: usize,
    sequence_number: i64,
}

#[tokio::test]
async fn compaction_stats_count_every_data_file_in_one_partition() {
    let dir = tempfile::tempdir().expect("tempdir");
    let table = table_with_live_data_files(
        dir.path(),
        &[ManifestPlan {
            partition_spec_id: 1,
            partition_value: 1,
            data_files: 3,
            sequence_number: 10,
        }],
    )
    .await;

    assert_eq!(max_compactable_data_files(&table, false).await, 3);
    assert_eq!(max_compactable_data_files(&table, true).await, 3);
}

#[tokio::test]
async fn compaction_stats_report_largest_partition_not_total() {
    let dir = tempfile::tempdir().expect("tempdir");
    let table = table_with_live_data_files(
        dir.path(),
        &[
            ManifestPlan {
                partition_spec_id: 1,
                partition_value: 1,
                data_files: 3,
                sequence_number: 10,
            },
            ManifestPlan {
                partition_spec_id: 1,
                partition_value: 2,
                data_files: 2,
                sequence_number: 10,
            },
        ],
    )
    .await;

    // Five live data files in total, but no rewrite group can span two
    // partitions, so the answer is the largest partition.
    assert_eq!(max_compactable_data_files(&table, false).await, 3);
}

#[tokio::test]
async fn compaction_stats_keep_partition_spec_ids_separate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let table = table_with_live_data_files(
        dir.path(),
        &[
            ManifestPlan {
                partition_spec_id: 1,
                partition_value: 1,
                data_files: 3,
                sequence_number: 10,
            },
            ManifestPlan {
                partition_spec_id: 2,
                partition_value: 1,
                data_files: 4,
                sequence_number: 10,
            },
        ],
    )
    .await;

    // Same partition value under two coexisting specs stays two groups.
    assert_eq!(max_compactable_data_files(&table, false).await, 4);
}

#[tokio::test]
async fn compaction_stats_split_by_sequence_only_when_preserving_row_lineage() {
    let dir = tempfile::tempdir().expect("tempdir");
    let table = table_with_live_data_files(
        dir.path(),
        &[
            ManifestPlan {
                partition_spec_id: 1,
                partition_value: 1,
                data_files: 3,
                sequence_number: 10,
            },
            ManifestPlan {
                partition_spec_id: 1,
                partition_value: 1,
                data_files: 2,
                sequence_number: 11,
            },
            ManifestPlan {
                partition_spec_id: 2,
                partition_value: 1,
                data_files: 4,
                sequence_number: 10,
            },
        ],
    )
    .await;

    // Without row lineage the two sequence numbers fold into one group of 5.
    assert_eq!(max_compactable_data_files(&table, false).await, 5);
    // Preserving row lineage splits them into 3 and 2, so the largest
    // remaining group is the four-file group of the other spec.
    assert_eq!(max_compactable_data_files(&table, true).await, 4);
}

#[tokio::test]
async fn compaction_stats_are_zero_without_a_current_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let table = table_with_live_data_files(dir.path(), &[]).await;

    assert_eq!(max_compactable_data_files(&table, false).await, 0);
    assert_eq!(max_compactable_data_files(&table, true).await, 0);
}

async fn max_compactable_data_files(table: &Table, preserve_row_lineage: bool) -> i64 {
    current_live_data_file_compaction_stats(table, table.file_io(), preserve_row_lineage)
        .await
        .expect("compaction stats")
        .max_compactable_data_files
}

fn test_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "p", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("schema")
}

fn test_partition_spec(schema: &Schema, spec_id: i32) -> PartitionSpec {
    PartitionSpec::builder(schema.clone())
        .with_spec_id(spec_id)
        .add_partition_field("p", "p", Transform::Identity)
        .expect("partition field")
        .build()
        .expect("partition spec")
}

fn test_data_file(plan: &ManifestPlan, ordinal: usize) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!(
            "data/spec-{}-p-{}-seq-{}-{ordinal}.parquet",
            plan.partition_spec_id, plan.partition_value, plan.sequence_number
        ))
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::from_iter([Some(Literal::Primitive(
            PrimitiveLiteral::Int(plan.partition_value),
        ))]))
        .partition_spec_id(plan.partition_spec_id)
        .record_count(1)
        .file_size_in_bytes(1024)
        .build()
        .expect("data file")
}

/// Writes real avro manifests plus a manifest list under `dir` and returns
/// a table whose current snapshot points at them. An empty `plans` list
/// yields a table without any snapshot.
async fn table_with_live_data_files(dir: &Path, plans: &[ManifestPlan]) -> Table {
    let location = format!("file://{}", dir.display());
    std::fs::create_dir_all(dir.join("metadata")).expect("metadata dir");
    let schema = test_schema();
    let file_io = crate::fs_io::build_file_io_for_location(&location, local_test_binding());
    let builder = TableMetadataBuilder::new(
        schema.clone(),
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        location.clone(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .expect("table metadata builder");
    let metadata = if plans.is_empty() {
        builder.build().expect("table metadata").metadata
    } else {
        let mut manifests = Vec::new();
        for (index, plan) in plans.iter().enumerate() {
            let path = format!("{location}/metadata/live-{index}.avro");
            let output = file_io.new_output(&path).expect("manifest output");
            let mut writer = ManifestWriterBuilder::new(
                output,
                Some(SNAPSHOT_ID),
                Arc::new(schema.clone()),
                test_partition_spec(&schema, plan.partition_spec_id),
            )
            .build_v2_data();
            for ordinal in 0..plan.data_files {
                writer
                    .add_file(test_data_file(plan, ordinal), plan.sequence_number)
                    .expect("add data file");
            }
            let mut manifest = writer.write_manifest_file().await.expect("write manifest");
            manifest.sequence_number = plan.sequence_number;
            manifest.min_sequence_number = plan.sequence_number;
            manifests.push(manifest);
        }
        let manifest_list_path = format!("{location}/metadata/snap-{SNAPSHOT_ID}.avro");
        let output = file_io
            .new_output(&manifest_list_path)
            .expect("manifest list output");
        let mut list_writer = ManifestListWriter::v2(
            output.writer().await.unwrap(),
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
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(0)
            .build();
        builder
            .add_snapshot(snapshot)
            .expect("add snapshot")
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
            .expect("set main ref")
            .build()
            .expect("table metadata")
            .metadata
    };
    Table::builder()
        .runtime(crate::iceberg::Runtime::try_current().expect("test runtime"))
        .identifier(TableIdent::from_strs(["db", "t"]).expect("table ident"))
        .file_io(file_io)
        .metadata(metadata)
        .build()
        .expect("table")
}
