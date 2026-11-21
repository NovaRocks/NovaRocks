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

use super::*;
use crate::iceberg::spec::{
    NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Struct, TableMetadataBuilder,
    Type,
};
#[test]
fn carried_entry_requires_actual_snapshot_and_data_sequence_and_preserves_file_sequence_none() {
    let file = DataFileBuilder::default()
        .content(crate::iceberg::spec::DataContentType::Data)
        .file_path("memory://rewrite/carried.parquet".into())
        .file_format(crate::iceberg::spec::DataFileFormat::Parquet)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .record_count(1)
        .file_size_in_bytes(10)
        .build()
        .unwrap();
    let mut entry = ManifestEntry {
        status: ManifestStatus::Existing,
        snapshot_id: Some(77),
        sequence_number: Some(4),
        file_sequence_number: None,
        data_file: file,
    };
    assert_eq!(carried_identity(&entry).unwrap(), (77, 4, None));
    entry.file_sequence_number = Some(2);
    assert_eq!(carried_identity(&entry).unwrap(), (77, 4, Some(2)));
    entry.sequence_number = None;
    assert_eq!(
        carried_identity(&entry).unwrap_err().kind(),
        crate::iceberg::ErrorKind::DataInvalid
    );
    entry.sequence_number = Some(4);
    entry.snapshot_id = None;
    assert_eq!(
        carried_identity(&entry).unwrap_err().kind(),
        crate::iceberg::ErrorKind::DataInvalid
    );
}
#[tokio::test]
async fn explicit_rows_do_not_shift_the_next_inherited_file_during_real_v3_merge() {
    let artifacts = crate::commit::staging::TestWriter::new();
    let io = artifacts.file_io();
    let input = artifacts
        .allocate(crate::commit::model::ArtifactKind::Manifest)
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
        "memory://rewrite/table".into(),
        FormatVersion::V3,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let mut writer = ManifestWriterBuilder::new(
        io.new_output(input.path()).unwrap(),
        Some(11),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().as_ref().clone(),
    )
    .build_v3_data();
    let file = |path: &str, first: Option<i64>, count| {
        DataFileBuilder::default()
            .content(crate::iceberg::spec::DataContentType::Data)
            .file_path(path.to_owned())
            .file_format(crate::iceberg::spec::DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(count)
            .file_size_in_bytes(20)
            .first_row_id(first)
            .build()
            .unwrap()
    };
    writer
        .add_existing_file(
            file("memory://rewrite/explicit.parquet", Some(700), 5),
            11,
            5,
            Some(5),
        )
        .unwrap();
    writer
        .add_file(file("memory://rewrite/inherited.parquet", None, 3), 10)
        .unwrap();
    let mut source = writer.write_manifest_file().await.unwrap();
    source.first_row_id = Some(100);
    source.sequence_number = 10;
    let parent = Snapshot::builder()
        .with_snapshot_id(11)
        .with_sequence_number(10)
        .with_timestamp_ms(1)
        .with_manifest_list("memory://rewrite/parent-list.avro".to_owned())
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .build();
    let merged = merge_manifest_group(&artifacts, &metadata, &parent, &[source], 22)
        .await
        .unwrap();
    let actual = crate::manifest::load_manifest(&merged, io).await.unwrap();
    let actual = actual
        .entries()
        .iter()
        .map(|entry| {
            (
                entry.data_file().file_path().to_owned(),
                entry.data_file().first_row_id(),
            )
        })
        .collect::<HashMap<_, _>>();
    assert_eq!(actual["memory://rewrite/explicit.parquet"], Some(700));
    assert_eq!(actual["memory://rewrite/inherited.parquet"], Some(100));
}
