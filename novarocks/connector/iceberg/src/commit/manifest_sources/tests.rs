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
    DataFileBuilder, FormatVersion, ManifestListWriter, ManifestWriterBuilder, Operation,
    PartitionSpec, Schema, SortOrder, Summary, TableMetadataBuilder,
};

#[tokio::test]
async fn registration_existence_reports_sorted_path_even_with_duplicate_live_entries() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().to_str().unwrap();
    let io = FileIO::new_with_fs();
    let metadata = TableMetadataBuilder::new(
        Schema::builder().build().unwrap(),
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        root.to_owned(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let snapshot_id = 7;
    let manifest_path = format!("{root}/data.avro");
    let mut writer = ManifestWriterBuilder::new(
        io.new_output(&manifest_path).unwrap(),
        Some(snapshot_id),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().as_ref().clone(),
    )
    .build_v2_data();
    for path in [
        "/external/z.parquet",
        "/external/a.parquet",
        "/external/a.parquet",
    ] {
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.into())
            .file_format(DataFileFormat::Parquet)
            .record_count(3)
            .file_size_in_bytes(100)
            .build()
            .unwrap();
        writer.add_file(file, 1).unwrap();
    }
    let data = writer.write_manifest_file().await.unwrap();
    let list_path = format!("{root}/list.avro");
    let mut list = ManifestListWriter::v2(
        io.new_output(&list_path).unwrap().writer().await.unwrap(),
        snapshot_id,
        None,
        1,
    );
    list.add_manifests(std::iter::once(data)).unwrap();
    list.close().await.unwrap();
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(None)
        .with_sequence_number(1)
        .with_timestamp_ms(metadata.last_updated_ms() + 1)
        .with_manifest_list(list_path)
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .with_schema_id(metadata.current_schema_id())
        .build();
    let metadata = metadata
        .into_builder(None)
        .add_snapshot(snapshot)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let active = || Ok(());
    let sources = SnapshotSources::load(
        &metadata,
        metadata.snapshot_by_id(snapshot_id).unwrap(),
        &io,
        active,
    )
    .await
    .unwrap();
    let candidates = ["/external/a.parquet", "/external/z.parquet"];
    assert_eq!(
        sources
            .registered_path_match(|path| candidates.binary_search(&path).ok(), &io, active,)
            .await
            .unwrap(),
        Some(0)
    );
    let exact = BTreeSet::from(candidates);
    let rejected = sources
        .live_data_matches(&exact, &io, active)
        .await
        .unwrap_err();
    assert_eq!(rejected.kind(), ErrorKind::DataInvalid);
    assert!(
        rejected
            .message()
            .contains("Duplicate live Iceberg entry identity")
    );
}
