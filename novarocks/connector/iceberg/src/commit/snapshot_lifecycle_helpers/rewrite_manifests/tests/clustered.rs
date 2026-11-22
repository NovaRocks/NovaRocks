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
use crate::commit::model::ArtifactKind;
use crate::commit::staging::TestWriter;
use crate::iceberg::spec::{
    DataContentType, DataFileFormat, NestedField, Schema, SortOrder, Struct, TableMetadataBuilder,
    Transform, UnboundPartitionSpec,
};

fn avro_source(
    artifacts: &TestWriter,
    metadata: &TableMetadata,
    spec: &PartitionSpec,
    first_row: i64,
) -> ManifestWriterBuilder {
    ManifestWriterBuilder::new(
        artifacts
            .file_io()
            .new_output(artifacts.allocate(ArtifactKind::Manifest).unwrap().path())
            .unwrap(),
        Some(first_row / 10),
        metadata.current_schema().clone(),
        spec.clone(),
    )
}

fn clustering_file(
    path: &str,
    partition: Struct,
    spec: i32,
    first_row_id: Option<i64>,
) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("memory://rewrite/{path}.parquet"))
        .file_format(DataFileFormat::Parquet)
        .partition(partition)
        .partition_spec_id(spec)
        .record_count(3)
        .file_size_in_bytes(100)
        .first_row_id(first_row_id)
        .build()
        .unwrap()
}

#[test]
fn grouping_respects_exact_size_threshold_and_caps_target_group_count() {
    let descriptor = |length| ManifestFile {
        manifest_path: "memory://rewrite/source.avro".to_owned(),
        manifest_length: length,
        partition_spec_id: 0,
        content: ManifestContentType::Data,
        sequence_number: 1,
        min_sequence_number: 1,
        added_snapshot_id: 1,
        added_files_count: Some(1),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(1),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(0),
        partitions: None,
        key_metadata: None,
        first_row_id: None,
    };
    let target = DEFAULT_MANIFEST_TARGET;
    assert_eq!(manifest_groups(&[], target).unwrap(), None);
    assert_eq!(
        manifest_groups(&[descriptor(target as i64 - 1)], target).unwrap(),
        None
    );
    assert_eq!(
        manifest_groups(&[descriptor(target as i64)], target).unwrap(),
        Some(1)
    );
    assert_eq!(
        manifest_groups(&[descriptor(5), descriptor(6)], 10).unwrap(),
        Some(2)
    );
    assert_eq!(
        manifest_groups(&[descriptor(i64::MAX), descriptor(i64::MAX)], 1).unwrap(),
        Some(200)
    );
}

#[test]
fn grouping_uses_trino_rank_with_java_float_and_uuid_order() {
    let groups = PartitionGroups::from_values(
        [-5i128, 2, 9, 99]
            .into_iter()
            .map(ClusterValue::Signed)
            .collect(),
        10,
        || Ok(()),
    )
    .unwrap();
    assert_eq!(groups.group(None), 0);
    assert_eq!(groups.group(Some(ClusterValue::Signed(-5))), 0);
    assert_eq!(groups.group(Some(ClusterValue::Signed(2))), 2);
    assert_eq!(groups.group(Some(ClusterValue::Signed(9))), 5);
    assert_eq!(groups.group(Some(ClusterValue::Signed(99))), 7);
    assert!(java_float_key(-0.0) < java_float_key(0.0));
    assert!(java_float_key(f64::INFINITY) < java_float_key(-f64::NAN));
    assert_eq!(java_float_key(f64::NAN), java_float_key(-f64::NAN));
    assert!(ClusterValue::Uuid(i64::MIN, 0) < ClusterValue::Uuid(0, 0));
    assert!(ClusterValue::Uuid(0, i64::MIN) < ClusterValue::Uuid(0, 0));
}

#[tokio::test]
async fn clustered_v3_rewrite_rolls_historical_lanes_and_keeps_each_live_entry() {
    let artifacts = TestWriter::new();
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(
                1,
                "other",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "p",
                Type::Primitive(PrimitiveType::Long),
            )),
        ])
        .build()
        .unwrap();
    let old = UnboundPartitionSpec::builder()
        .add_partition_field(1, "other_part", Transform::Identity)
        .unwrap()
        .add_partition_field(2, "p_part", Transform::Identity)
        .unwrap()
        .build();
    let current = UnboundPartitionSpec::builder()
        .add_partition_field(2, "p_part", Transform::Identity)
        .unwrap()
        .build();
    let metadata = TableMetadataBuilder::new(
        schema,
        old,
        SortOrder::unsorted_order(),
        "memory://rewrite/table".into(),
        FormatVersion::V3,
        HashMap::new(),
    )
    .unwrap()
    .add_partition_spec(UnboundPartitionSpec::builder().build())
    .unwrap()
    .add_partition_spec(current)
    .unwrap()
    .set_default_partition_spec(-1)
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let old = metadata.partition_spec_by_id(0).unwrap();
    let missing = metadata.partition_spec_by_id(1).unwrap();
    let current = metadata.default_partition_spec();
    assert_eq!(old.fields()[1].field_id, current.fields()[0].field_id);
    let parent = Snapshot::builder()
        .with_snapshot_id(70)
        .with_sequence_number(19)
        .with_timestamp_ms(1)
        .with_manifest_list("memory://rewrite/original-list.avro")
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .build();
    let part =
        |value: Option<i64>| value.map(|value| Literal::Primitive(PrimitiveLiteral::Long(value)));
    let long_path = "oversized".repeat(256);

    let mut old_writer = avro_source(&artifacts, &metadata, old, 1000).build_v3_data();
    for (name, value, first_row) in [
        (long_path.as_str(), Some(-10), None),
        ("after", None, Some(71)),
        ("high", Some(10), None),
    ] {
        old_writer
            .add_file(
                clustering_file(
                    name,
                    Struct::from_iter([part(Some(999)), part(value)]),
                    old.spec_id(),
                    first_row,
                ),
                17,
            )
            .unwrap();
    }
    old_writer
        .add_existing_file(
            clustering_file(
                "same-low-group",
                Struct::from_iter([part(Some(888)), part(Some(-10))]),
                old.spec_id(),
                Some(72),
            ),
            12,
            3,
            Some(5),
        )
        .unwrap();
    old_writer
        .add_delete_file(
            clustering_file(
                "dead",
                Struct::from_iter([part(Some(888)), part(Some(100))]),
                old.spec_id(),
                Some(79),
            ),
            3,
            Some(5),
        )
        .unwrap();
    let mut historical = old_writer.write_manifest_file().await.unwrap();
    historical.first_row_id = Some(1000);
    historical.sequence_number = 19;

    let mut current_writer = avro_source(&artifacts, &metadata, current, 2000).build_v3_data();
    for (name, value) in [("current-high", Some(10)), ("current-null", None)] {
        current_writer
            .add_file(
                clustering_file(
                    name,
                    Struct::from_iter([part(value)]),
                    current.spec_id(),
                    None,
                ),
                18,
            )
            .unwrap();
    }
    let mut present = current_writer.write_manifest_file().await.unwrap();
    present.first_row_id = Some(2000);
    present.sequence_number = 19;

    let mut missing_writer = avro_source(&artifacts, &metadata, missing, 3000).build_v3_data();
    missing_writer
        .add_file(
            clustering_file(
                "previously-unpartitioned",
                Struct::empty(),
                missing.spec_id(),
                None,
            ),
            2,
        )
        .unwrap();
    let mut unpartitioned = missing_writer.write_manifest_file().await.unwrap();
    unpartitioned.first_row_id = Some(3000);
    unpartitioned.sequence_number = 19;

    let sources = vec![historical, present, unpartitioned];
    let groups = manifest_groups(&sources, 512).unwrap().unwrap();
    assert!(groups > 1);
    let rewrite = DataManifestRewrite {
        artifacts: &artifacts,
        metadata: &metadata,
        parent: &parent,
        snapshot_id: 90,
        target: 512,
        backing: RewriteBacking {
            request: None,
            admitted: 0,
            base: 0,
        },
    };
    let rewritten = rewrite
        .write(sources.clone(), groups, vec![])
        .await
        .unwrap();
    assert_eq!(rewritten.sources, sources);
    assert_eq!(rewritten.processed_entries, 7);
    let mut actual = BTreeMap::new();
    let mut by_manifest = Vec::new();
    for descriptor in &rewritten.outputs {
        let loaded = crate::manifest::load_manifest(descriptor, artifacts.file_io())
            .await
            .unwrap();
        let mut paths = BTreeSet::new();
        for entry in loaded.entries() {
            assert_eq!(entry.status, ManifestStatus::Existing);
            let file = entry.data_file();
            let path = file
                .file_path()
                .strip_prefix("memory://rewrite/")
                .unwrap()
                .strip_suffix(".parquet")
                .unwrap()
                .to_owned();
            paths.insert(path.clone());
            assert!(
                actual
                    .insert(
                        path,
                        (
                            descriptor.partition_spec_id,
                            entry.snapshot_id.unwrap(),
                            entry.sequence_number.unwrap(),
                            entry.file_sequence_number.unwrap(),
                            file.first_row_id().unwrap(),
                        )
                    )
                    .is_none()
            );
        }
        by_manifest.push(paths);
    }
    assert_eq!(by_manifest.len(), 6);
    assert!(
        by_manifest.contains(&BTreeSet::from([long_path.clone()])),
        "oversized single entry still writes"
    );
    assert!(
        by_manifest.contains(&BTreeSet::from(["after".into(), "same-low-group".into()])),
        "null follows rank-zero values in the historic lane after rollover"
    );
    assert_eq!(
        actual,
        BTreeMap::from([
            (long_path, (old.spec_id(), 100, 17, 19, 1000)),
            ("after".into(), (old.spec_id(), 100, 17, 19, 71)),
            ("same-low-group".into(), (old.spec_id(), 12, 3, 5, 72)),
            ("high".into(), (old.spec_id(), 100, 17, 19, 1003)),
            (
                "current-high".into(),
                (current.spec_id(), 200, 18, 19, 2000)
            ),
            (
                "current-null".into(),
                (current.spec_id(), 200, 18, 19, 2003)
            ),
            (
                "previously-unpartitioned".into(),
                (missing.spec_id(), 300, 2, 19, 3000)
            ),
        ])
    );
}

#[tokio::test]
async fn execute_keeps_delete_manifest_descriptors_and_reports_kept_count() {
    use crate::commit::model::{
        Dependency, FileChanges, ObjectIdentity, OperationIntent, OperationIntentParts,
        RequestShape, TableTarget,
    };
    use crate::commit::staging::{StagingBase, StagingEngine};
    use crate::iceberg::{NamespaceIdent, TableIdent};

    let artifacts = TestWriter::new();
    let metadata = TableMetadataBuilder::new(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap(),
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "memory://rewrite/table".into(),
        FormatVersion::V2,
        HashMap::from([(MANIFEST_TARGET_SIZE.to_owned(), "512".to_owned())]),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let existing = |metadata| StagingBase::Existing {
        metadata,
        metadata_location: "memory://rewrite/table/metadata/base.metadata.json".into(),
    };
    let spec = metadata.default_partition_spec();
    let mut data = avro_source(&artifacts, &metadata, spec, 1000).build_v2_data();
    data.add_file(clustering_file("data", Struct::empty(), 0, None), -1)
        .unwrap();
    let data = data.write_manifest_file().await.unwrap();

    let mut deletes = avro_source(&artifacts, &metadata, spec, 1000).build_v2_deletes();
    deletes
        .add_file(
            DataFileBuilder::default()
                .content(DataContentType::PositionDeletes)
                .file_path("memory://rewrite/rows-to-delete.parquet".to_owned())
                .file_format(DataFileFormat::Parquet)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .record_count(1)
                .file_size_in_bytes(10)
                .build()
                .unwrap(),
            -1,
        )
        .unwrap();
    let deletes = deletes.write_manifest_file().await.unwrap();

    for manifest in [&data, &deletes] {
        let object = ObjectIdentity::new(manifest.manifest_path.clone()).unwrap();
        artifacts.record_manifest(&object, &[], &[], &[]).unwrap();
    }

    let intent = OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
            uuid: Some(metadata.uuid()),
        },
        target_ref: "main".into(),
        start: None,
        changes: FileChanges::default(),
        dependencies: vec![Dependency::NoRead],
        protocol: crate::commit::model::CommitProtocol::Legacy,
        shape: RequestShape::SnapshotProducing,
        summary: BTreeMap::new(),
        token: artifacts.attempt_token().operation(),
    })
    .unwrap();
    let mut seed = StagingEngine::begin(existing(metadata), &intent, &artifacts).unwrap();
    let change = crate::commit::overwrite::prepare_snapshot_change(
        &seed.view(),
        &intent,
        100,
        Operation::Append,
        vec![data, deletes],
        HashMap::new(),
        false,
    )
    .await
    .unwrap();
    seed.stage_change(change).unwrap();
    let load_list = |metadata: &TableMetadata| {
        let metadata = metadata.clone();
        let io = artifacts.file_io();
        async move {
            crate::manifest::load_manifest_list(metadata.current_snapshot().unwrap(), io, &metadata)
                .await
                .unwrap()
        }
    };
    let before = load_list(seed.metadata()).await;
    let mut engine =
        StagingEngine::begin(existing(seed.metadata().clone()), &intent, &artifacts).unwrap();
    engine
        .stage(&RewriteManifestsPreparer {
            request: None,
            sql_execute: true,
        })
        .await
        .unwrap();
    let after = load_list(engine.metadata()).await;
    assert_eq!(after.entries().len(), before.entries().len());
    assert_eq!(
        after
            .entries()
            .iter()
            .find(|item| item.content == ManifestContentType::Deletes),
        before
            .entries()
            .iter()
            .find(|item| item.content == ManifestContentType::Deletes),
    );
    assert_ne!(
        after
            .entries()
            .iter()
            .find(|item| item.content == ManifestContentType::Data),
        before
            .entries()
            .iter()
            .find(|item| item.content == ManifestContentType::Data),
    );
    let summary = engine.metadata().current_snapshot().unwrap().summary();
    assert_eq!(summary.operation, Operation::Replace);
    assert_eq!(
        execute_metrics(&summary.additional_properties).unwrap(),
        ConnectorManifestRewriteMetrics {
            rewritten: 1,
            added: 1,
            kept: 1,
            processed_entries: 1
        }
    );
}
