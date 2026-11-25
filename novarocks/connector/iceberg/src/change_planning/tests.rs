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

//! White-box tests for window classification, endpoint membership and exact
//! source volume from manifest and logical delete facts.

use std::collections::HashMap;
use std::sync::Arc;

use crate::iceberg::spec::{
    FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
    SortOrder, Summary, TableMetadata, TableMetadataBuilder, Type,
};

use super::*;

fn snapshot(
    snapshot_id: i64,
    parent_snapshot_id: Option<i64>,
    operation: Operation,
    properties: &[(&str, &str)],
    schema_id: i32,
) -> Snapshot {
    snapshot_at(
        snapshot_id,
        parent_snapshot_id,
        operation,
        properties,
        schema_id,
        1_700_000_000_000 + snapshot_id,
    )
}

fn snapshot_at(
    snapshot_id: i64,
    parent_snapshot_id: Option<i64>,
    operation: Operation,
    properties: &[(&str, &str)],
    schema_id: i32,
    timestamp_ms: i64,
) -> Snapshot {
    Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent_snapshot_id)
        .with_sequence_number(snapshot_id)
        .with_timestamp_ms(timestamp_ms)
        .with_manifest_list(format!("file:///tmp/manifest-list-{snapshot_id}.avro"))
        .with_summary(Summary {
            operation,
            additional_properties: properties
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect::<HashMap<_, _>>(),
        })
        .with_schema_id(schema_id)
        .build()
}

fn metadata_with_snapshots(snapshots: Vec<Snapshot>) -> TableMetadata {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("schema");
    let mut builder = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "/tmp/change-window-test".to_string(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .expect("metadata builder");
    for snapshot in snapshots {
        builder = builder.add_snapshot(snapshot).expect("add snapshot");
    }
    builder.build().expect("metadata").metadata
}

#[test]
fn valid_replace_is_metadata_only() {
    let parent = snapshot(1, None, Operation::Append, &[("total-records", "100")], 0);
    let replace = snapshot(
        2,
        Some(1),
        Operation::Replace,
        &[
            ("total-records", "100"),
            ("added-data-files", "3"),
            ("deleted-data-files", "2"),
        ],
        0,
    );
    assert!(matches!(
        prepare_change_window(
            &metadata_with_snapshots(vec![parent, replace]),
            1,
            2,
            &request_context()
        )
        .unwrap(),
        ChangeWindowPlan::Resolved(ConnectorChangeWindowFacts::NetZero(
            ConnectorChangeWindowNetZeroBasis::ReplaceOnly
        ))
    ));
}

#[test]
fn partition_transform_projection_is_typed_and_bounded() {
    assert_eq!(
        connector_transform("bucket(16)"),
        Some(ConnectorChangePartitionTransform::Bucket {
            buckets: NonZeroU32::new(16).expect("nonzero")
        })
    );
    assert_eq!(connector_transform("bucket(0)"), None);
    assert_eq!(connector_transform("void"), None);
}

#[test]
fn file_partition_impact_preserves_opaque_source_and_spec_identities() {
    let values = vec![ChangePartitionFieldValue {
        source_field_id: 17,
        source_column: Some("renamed_region".to_string()),
        field_name: "region_bucket".to_string(),
        transform: "bucket(8)".to_string(),
        value: ChangePartitionValue::Primitive("3".to_string()),
    }];
    let partition = connector_partition(Some(9), &values)
        .expect("provider partition projection")
        .expect("exact partition");
    assert_eq!(
        partition.partition_spec_identity(),
        &crate::storage_inspector::exact_partition_spec_version(9)
    );
    assert_eq!(
        partition.fields()[0].source_field_identity().as_ref(),
        &17_i32.to_be_bytes()
    );
    assert_eq!(partition.fields()[0].source_column(), "renamed_region");
    assert!(
        connector_partition(None, &values)
            .expect("missing spec identity remains unavailable")
            .is_none()
    );
}

#[test]
fn equal_endpoints_are_metadata_only_without_ordering_snapshot_identities() {
    let metadata = metadata_with_snapshots(Vec::new());
    assert!(matches!(
        prepare_change_window(&metadata, 41, 41, &request_context())
            .expect("equal endpoint admission"),
        ChangeWindowPlan::Resolved(ConnectorChangeWindowFacts::NetZero(
            ConnectorChangeWindowNetZeroBasis::SameSnapshot
        ))
    ));
}

#[test]
fn missing_from_snapshot_is_a_typed_lineage_full_rebuild() {
    let current = snapshot(2, None, Operation::Append, &[], 0);
    let metadata = metadata_with_snapshots(vec![current]);
    assert!(matches!(
        prepare_change_window(&metadata, 1, 2, &request_context()).expect("lineage admission"),
        ChangeWindowPlan::Resolved(ConnectorChangeWindowFacts::NotIncremental(
            ConnectorChangeWindowNotIncrementalReason::SnapshotAncestryBroken {
                from_snapshot_id: 1
            }
        ))
    ));
}

#[test]
fn missing_upper_snapshot_remains_a_hard_corrupt_data_error() {
    let metadata = metadata_with_snapshots(Vec::new());
    let error = prepare_change_window(&metadata, 1, 2, &request_context())
        .expect_err("missing upper snapshot");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
}

#[test]
fn field_id_preserving_rename_is_the_only_incremental_schema_change() {
    let previous = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            7,
            "region",
            Type::Primitive(PrimitiveType::String),
        ))])
        .build()
        .expect("previous schema");
    let renamed = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            7,
            "area",
            Type::Primitive(PrimitiveType::String),
        ))])
        .build()
        .expect("renamed schema");
    let widened = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            7,
            "area",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("widened schema");

    assert!(schema_differs_only_by_field_names(&previous, &renamed));
    assert!(!schema_differs_only_by_field_names(&previous, &widened));
}
#[test]
fn rename_only_identifier_sets_ignore_independent_hash_iteration_order() {
    let schema = |name: &str, ids: &[i32]| {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    name,
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "second",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .with_identifier_field_ids(ids.iter().copied())
            .build()
            .unwrap()
    };
    let original = schema("first", &[1, 2]);
    for _ in 0..64 {
        assert!(schema_differs_only_by_field_names(
            &original,
            &schema("renamed", &[2, 1])
        ));
    }
    assert!(!schema_differs_only_by_field_names(
        &original,
        &schema("renamed", &[1])
    ));
}

fn request_context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        256 * 1024,
        1024 * 1024,
    )
    .unwrap()
}

fn endpoint_file(
    metadata: &TableMetadata,
    snapshot: i64,
    delete_sequence: i64,
    prune: bool,
) -> crate::read_model::IcebergReadFile {
    use crate::delete_semantics::*;
    use crate::iceberg::spec::{DataFileFormat, Datum, Struct};
    let schema = metadata.current_schema();
    let spec = PartitionSpec::unpartition_spec();
    let partition = TypedPartition::bind(&spec, schema, &Struct::empty()).unwrap();
    let metrics = |lo, hi| {
        FileMetrics::new(BTreeMap::from([(
            1,
            FieldMetrics {
                resolved_type: PrimitiveType::Long,
                value_count: Some(10),
                null_count: Some(0),
                nan_count: None,
                lower_bound: Some(Datum::long(lo)),
                upper_bound: Some(Datum::long(hi)),
            },
        )]))
    };
    let data = DataFileFact::try_new(
        "data",
        DataSequenceNumber::try_new(1).unwrap(),
        partition.clone(),
        10,
        metrics(8, 9),
    )
    .unwrap();
    let fact = DeleteFact::try_new(DeleteFactParams {
        address: DeleteContentAddress::file("same.parquet").unwrap(),
        kind: DeleteKind::Equality(EqualityFieldGroup::bind(&[1], schema).unwrap()),
        sequence: DataSequenceNumber::try_new(delete_sequence).unwrap(),
        partition,
        read: DeleteReadFacts {
            format: DeleteFormat::Parquet,
            record_count: 10,
            file_size: 100,
            key_metadata: Arc::from([]),
        },
        metrics: metrics(1, 2),
    })
    .unwrap();
    let index = DeleteCandidateIndex::try_new(
        test_read_domain(schema, &[spec], snapshot),
        DeleteObservation::from_normalized_recall([Arc::new(fact)]).unwrap(),
    )
    .unwrap();
    crate::read_model::IcebergReadFile {
        path: "data".into(),
        size: 100,
        record_count: Some(10),
        column_stats: None,
        partition_spec_id: Some(0),
        partition_key: None,
        partition_values: Some(Struct::empty()),
        manifest_path: None,
        first_row_id: None,
        data_sequence_number: Some(1),
        manifest: Arc::new(crate::read_model::IcebergDataFileMetadata {
            file_format: DataFileFormat::Parquet,
            split_offsets: vec![],
            key_metadata: vec![],
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
        }),
        deletes: index.for_data(&data).unwrap().load_view(if prune {
            StatisticsPolicy::MetadataBudget {
                max_candidate_members: 10,
                max_field_comparisons: 10,
            }
        } else {
            StatisticsPolicy::Disabled
        }),
    }
}

#[test]
fn admission_uses_logical_applications_and_ignores_optional_load_pruning() {
    let metadata = metadata_with_snapshots(vec![]);
    let from = endpoint_file(&metadata, 10, 2, false);
    let pruned = endpoint_file(&metadata, 20, 2, true);
    assert_eq!(from.deletes.member_count(), 1);
    assert_eq!(pruned.deletes.member_count(), 0);
    assert!(matches!(
        finish_change_window(
            &metadata,
            std::slice::from_ref(&from),
            &[pruned],
            &request_context()
        )
        .unwrap(),
        ConnectorChangeWindowFacts::NetZero(ConnectorChangeWindowNetZeroBasis::EqualEndpoints)
    ));
    let changed = endpoint_file(&metadata, 20, 3, false);
    assert!(matches!(
        finish_change_window(&metadata, &[from], &[changed], &request_context()).unwrap(),
        ConnectorChangeWindowFacts::Difference {
            negative_weight: ConnectorChangeWindowNegativeWeight::Possible,
            ..
        }
    ));
}

#[test]
fn admission_endpoint_membership_excludes_transient_files_and_checks_data_identity() {
    let metadata = metadata_with_snapshots(vec![]);
    let file = endpoint_file(&metadata, 10, 2, false);
    assert!(matches!(
        finish_change_window(&metadata, &[], &[], &request_context()).unwrap(),
        ConnectorChangeWindowFacts::NetZero(ConnectorChangeWindowNetZeroBasis::EqualEndpoints)
    ));
    assert!(matches!(
        finish_change_window(
            &metadata,
            &[],
            std::slice::from_ref(&file),
            &request_context()
        )
        .unwrap(),
        ConnectorChangeWindowFacts::Difference {
            negative_weight: ConnectorChangeWindowNegativeWeight::Absent,
            ..
        }
    ));
    assert!(matches!(
        finish_change_window(
            &metadata,
            std::slice::from_ref(&file),
            &[],
            &request_context()
        )
        .unwrap(),
        ConnectorChangeWindowFacts::Difference {
            negative_weight: ConnectorChangeWindowNegativeWeight::Possible,
            ..
        }
    ));
    let mut mutated = file.clone();
    mutated.record_count = Some(11);
    assert_eq!(
        finish_change_window(&metadata, &[file], &[mutated], &request_context())
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::CorruptData
    );
}

#[test]
fn shared_lineage_plan_preserves_the_upper_endpoint_schema_for_both_reads() {
    let from = snapshot(1, None, Operation::Append, &[], 0);
    let metadata = metadata_with_snapshots(vec![from]);
    let renamed = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "renamed",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let metadata = metadata
        .into_builder(None)
        .add_current_schema(renamed)
        .unwrap()
        .add_snapshot(snapshot(2, Some(1), Operation::Append, &[], 1))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    assert_eq!(
        metadata
            .snapshot_by_id(1)
            .unwrap()
            .schema(&metadata)
            .unwrap()
            .schema_id(),
        0
    );
    let ChangeWindowPlan::Endpoints { schema } =
        prepare_change_window(&metadata, 1, 2, &request_context()).unwrap()
    else {
        panic!("rename-only lineage must retain incremental endpoint semantics");
    };
    assert_eq!(schema.schema_id(), 1);
    assert_eq!(schema.as_struct().fields()[0].name, "renamed");
}

fn data_schema(id: i32, name: &str, includes_extra: bool) -> Schema {
    let mut fields = vec![Arc::new(NestedField::required(
        1,
        name,
        Type::Primitive(PrimitiveType::Long),
    ))];
    if includes_extra {
        fields.push(Arc::new(NestedField::optional(
            2,
            "extra",
            Type::Primitive(PrimitiveType::Long),
        )));
    }
    Schema::builder()
        .with_schema_id(id)
        .with_fields(fields)
        .build()
        .unwrap()
}

type HistorySnapshot<'a> = (i64, Option<i64>, Operation, i32, &'a [(&'a str, &'a str)]);

fn lineage_fixture(rows: &[HistorySnapshot<'_>]) -> TableMetadata {
    let metadata = metadata_with_snapshots(vec![]);
    let started_at = metadata.last_updated_ms();
    let mut builder = metadata.into_builder(None);
    for schema in [
        data_schema(1, "renamed", false),
        data_schema(2, "renamed", true),
        data_schema(3, "id", true),
    ] {
        builder = builder.add_current_schema(schema).unwrap();
    }
    for (id, parent, operation, schema, summary) in rows {
        builder = builder
            .add_snapshot(snapshot_at(
                *id,
                *parent,
                operation.clone(),
                summary,
                *schema,
                started_at + id,
            ))
            .unwrap();
    }
    builder.build().unwrap().metadata
}

#[test]
fn history_proves_ancestry_before_schema_and_replace_before_evolution() {
    use ConnectorChangeWindowFacts::{NetZero, NotIncremental};
    use ConnectorChangeWindowNetZeroBasis::ReplaceOnly;
    use ConnectorChangeWindowNotIncrementalReason::{SchemaChanged, SnapshotAncestryBroken};

    let original = (
        1,
        None,
        Operation::Append,
        0,
        &[("total-records", "100")][..],
    );
    let rewrite = |schema| {
        (
            2,
            Some(1),
            Operation::Replace,
            schema,
            &[("total-records", "90")][..],
        )
    };
    let overwrite = (2, Some(1), Operation::Overwrite, 0, &[][..]);
    let cases = [
        (
            "only REPLACE with fewer physical rows and missing counts",
            vec![original.clone(), rewrite(0)],
            1,
            Some(NetZero(ReplaceOnly)),
        ),
        (
            "rename then OPTIMIZE",
            vec![original.clone(), rewrite(1)],
            1,
            Some(NetZero(ReplaceOnly)),
        ),
        (
            "add column then OPTIMIZE",
            vec![original.clone(), rewrite(3)],
            1,
            Some(NetZero(ReplaceOnly)),
        ),
        (
            "rename then append",
            vec![original.clone(), (2, Some(1), Operation::Append, 1, &[])],
            1,
            None,
        ),
        (
            "OVERWRITE changes data",
            vec![original.clone(), overwrite],
            1,
            None,
        ),
        (
            "REPLACE changes schema in a mixed window",
            vec![
                original.clone(),
                (2, Some(1), Operation::Delete, 0, &[]),
                (3, Some(2), Operation::Replace, 3, &[]),
            ],
            1,
            Some(NotIncremental(SchemaChanged { snapshot_id: 3 })),
        ),
        (
            "drop column then append",
            vec![
                (1, None, Operation::Append, 3, &[]),
                (2, Some(1), Operation::Append, 0, &[]),
            ],
            1,
            Some(NotIncremental(SchemaChanged { snapshot_id: 2 })),
        ),
        (
            "nearest structural change comes first",
            vec![
                original.clone(),
                (2, Some(1), Operation::Replace, 3, &[]),
                (3, Some(2), Operation::Delete, 1, &[]),
            ],
            1,
            Some(NotIncremental(SchemaChanged { snapshot_id: 3 })),
        ),
        (
            "different branch despite intermediate schema evolution",
            vec![
                original.clone(),
                (2, None, Operation::Append, 0, &[]),
                (3, Some(2), Operation::Append, 3, &[]),
            ],
            1,
            Some(NotIncremental(SnapshotAncestryBroken {
                from_snapshot_id: 1,
            })),
        ),
        (
            "expired From",
            vec![original.clone(), rewrite(0)],
            9,
            Some(NotIncremental(SnapshotAncestryBroken {
                from_snapshot_id: 9,
            })),
        ),
    ];
    for (name, snapshots, from, expected) in cases {
        let to = snapshots.last().unwrap().0;
        let metadata = lineage_fixture(&snapshots);
        match (
            prepare_change_window(&metadata, from, to, &request_context()).unwrap(),
            expected,
        ) {
            (ChangeWindowPlan::Resolved(actual), Some(expected)) => {
                assert_eq!(actual, expected, "{name}")
            }
            (ChangeWindowPlan::Endpoints { schema }, None) => {
                assert_eq!(schema.schema_id(), snapshots.last().unwrap().3, "{name}");
            }
            (actual, expected) => panic!("{name}: {actual:?}, expected {expected:?}"),
        }
    }
}

#[test]
fn change_windows_still_require_the_reachable_from_snapshot_body() {
    let metadata = lineage_fixture(&[
        (1, None, Operation::Append, 0, &[]),
        (2, Some(1), Operation::Replace, 0, &[]),
    ])
    .into_builder(None)
    .remove_snapshots(&[1])
    .build()
    .unwrap()
    .metadata;
    assert!(metadata.snapshot_by_id(1).is_none());
    assert!(matches!(
        prepare_change_window(&metadata, 1, 2, &request_context()).unwrap(),
        ChangeWindowPlan::Resolved(ConnectorChangeWindowFacts::NotIncremental(
            ConnectorChangeWindowNotIncrementalReason::SnapshotAncestryBroken {
                from_snapshot_id: 1,
            },
        )),
    ));
}

fn volume_metadata(version: FormatVersion, partitioned: bool) -> TableMetadata {
    let schema = data_schema(0, "id", false);
    let spec = if partitioned {
        PartitionSpec::builder(Arc::new(schema.clone()))
            .add_partition_field(
                "id",
                "partition_id",
                crate::iceberg::spec::Transform::Identity,
            )
            .unwrap()
            .build()
            .unwrap()
    } else {
        PartitionSpec::unpartition_spec()
    };
    TableMetadataBuilder::new(
        schema,
        spec,
        SortOrder::unsorted_order(),
        "/tmp/window-volume-test".into(),
        version,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}

#[derive(Clone, Copy)]
enum DeleteFixture {
    Clean,
    Dv(u64),
    Position,
    Equality,
}

fn volume_file(
    metadata: &TableMetadata,
    path: &str,
    endpoint: i64,
    rows: u64,
    bytes: i64,
    delete: DeleteFixture,
) -> IcebergReadFile {
    use crate::delete_semantics::*;
    use crate::iceberg::spec::{DataFileFormat, Literal};
    let schema = metadata.current_schema();
    let spec = metadata.default_partition_spec();
    let values = if spec.is_unpartitioned() {
        Struct::empty()
    } else {
        Struct::from_iter([Some(Literal::long(7))])
    };
    let partition = TypedPartition::bind(spec, schema, &values).unwrap();
    let data = DataFileFact::try_new(
        path,
        DataSequenceNumber::try_new(1).unwrap(),
        partition.clone(),
        rows,
        FileMetrics::default(),
    )
    .unwrap();
    let delete_path = format!("{path}-{endpoint}.delete");
    let delete = match delete {
        DeleteFixture::Clean => None,
        DeleteFixture::Dv(count) => Some((
            DeleteContentAddress::puffin(delete_path, 4, 32, 100).unwrap(),
            DeleteKind::DeletionVector {
                exact_target: Arc::from(path),
            },
            DeleteFormat::Puffin,
            count,
        )),
        DeleteFixture::Position => Some((
            DeleteContentAddress::file(delete_path).unwrap(),
            DeleteKind::Position {
                exact_target: Some(Arc::from(path)),
            },
            DeleteFormat::Parquet,
            2,
        )),
        DeleteFixture::Equality => Some((
            DeleteContentAddress::file(delete_path).unwrap(),
            DeleteKind::Equality(EqualityFieldGroup::bind(&[1], schema).unwrap()),
            DeleteFormat::Parquet,
            2,
        )),
    };
    let fact = delete.map(|(address, kind, format, record_count)| {
        Arc::new(
            DeleteFact::try_new(DeleteFactParams {
                address,
                kind,
                sequence: DataSequenceNumber::try_new(endpoint).unwrap(),
                partition,
                read: DeleteReadFacts {
                    format,
                    record_count,
                    file_size: 100,
                    key_metadata: Arc::from([]),
                },
                metrics: FileMetrics::default(),
            })
            .unwrap(),
        )
    });
    let index = DeleteCandidateIndex::try_new(
        test_read_domain(schema, &[spec.as_ref().clone()], endpoint),
        DeleteObservation::from_normalized_recall(fact).unwrap(),
    )
    .unwrap();
    IcebergReadFile {
        path: path.into(),
        size: bytes,
        record_count: Some(i64::try_from(rows).unwrap()),
        column_stats: None,
        partition_spec_id: Some(spec.spec_id()),
        partition_key: None,
        partition_values: Some(values),
        manifest_path: None,
        first_row_id: None,
        data_sequence_number: Some(1),
        manifest: Arc::new(crate::read_model::IcebergDataFileMetadata {
            file_format: DataFileFormat::Parquet,
            split_offsets: vec![],
            key_metadata: vec![],
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
        }),
        deletes: index
            .for_data(&data)
            .unwrap()
            .load_view(StatisticsPolicy::Disabled),
    }
}

#[test]
fn source_volume_counts_dv_merges_visible_endpoints_and_conservative_file_deletes() {
    use ConnectorChangeWindowNegativeWeight::{Absent, Possible};
    use DeleteFixture::{Clean, Dv, Equality, Position};

    let v3 = volume_metadata(FormatVersion::V3, false);
    let before = |path, delete| volume_file(&v3, path, 10, 1_000_000, 100, delete);
    let after = |path, delete| volume_file(&v3, path, 20, 1_000_000, 150, delete);
    let after_same = |delete| volume_file(&v3, "same", 20, 1_000_000, 100, delete);
    let cases = [
        (
            "pure append",
            Vec::new(),
            vec![after("new", Clean)],
            Absent,
            1_000_000,
            150,
        ),
        (
            "new DV on surviving file",
            vec![before("same", Clean)],
            vec![after_same(Dv(10))],
            Possible,
            10,
            100,
        ),
        (
            "DV merge",
            vec![before("same", Dv(400_000))],
            vec![after_same(Dv(400_010))],
            Possible,
            10,
            100,
        ),
        (
            "visible rows in an added file",
            Vec::new(),
            vec![after("new", Dv(400_000))],
            Absent,
            600_000,
            150,
        ),
        (
            "visible rows in a removed file",
            vec![before("old", Dv(400_000))],
            Vec::new(),
            Possible,
            600_000,
            100,
        ),
        (
            "position delete fallback",
            vec![before("same", Position)],
            vec![after_same(Dv(400_010))],
            Possible,
            1_000_000,
            100,
        ),
        (
            "equality delete fallback",
            vec![before("old", Equality)],
            Vec::new(),
            Possible,
            1_000_000,
            100,
        ),
        (
            "delete then optimize with physical cancellation pairs",
            vec![before("old", Dv(400_000))],
            vec![volume_file(&v3, "new", 20, 599_990, 150, Clean)],
            Possible,
            1_199_990,
            250,
        ),
        (
            "delete then append the same logical rows",
            vec![before("old", Clean)],
            vec![after("new", Clean)],
            Possible,
            2_000_000,
            250,
        ),
    ];
    for (name, from, to, negative_weight, rows, bytes) in cases {
        assert_eq!(
            finish_change_window(&v3, &from, &to, &request_context()).unwrap(),
            ConnectorChangeWindowFacts::Difference {
                negative_weight,
                partition_impact: ConnectorChangeWindowPartitionImpact::Unpartitioned,
                volume: ConnectorChangeWindowVolume {
                    source_event_rows_upper_bound: rows,
                    source_data_file_bytes: bytes,
                },
            },
            "{name}",
        );
    }
}

#[test]
fn delete_then_optimize_produces_both_visible_file_contributions() {
    let metadata = lineage_fixture(&[
        (1, None, Operation::Append, 0, &[]),
        (2, Some(1), Operation::Delete, 0, &[]),
        (3, Some(2), Operation::Replace, 0, &[]),
    ]);
    assert!(matches!(
        prepare_change_window(&metadata, 1, 3, &request_context()).unwrap(),
        ChangeWindowPlan::Endpoints { .. }
    ));
    let from = volume_file(&metadata, "old", 10, 100, 100, DeleteFixture::Clean);
    let to = volume_file(&metadata, "new", 20, 99, 128, DeleteFixture::Clean);
    assert_eq!(
        finish_change_window(&metadata, &[from], &[to], &request_context()).unwrap(),
        ConnectorChangeWindowFacts::Difference {
            negative_weight: ConnectorChangeWindowNegativeWeight::Possible,
            partition_impact: ConnectorChangeWindowPartitionImpact::Unpartitioned,
            volume: ConnectorChangeWindowVolume {
                source_event_rows_upper_bound: 199,
                source_data_file_bytes: 228,
            },
        },
    );
}

#[test]
fn a_whole_file_removal_retains_the_exact_removed_partition() {
    let metadata = volume_metadata(FormatVersion::V2, true);
    let file = volume_file(&metadata, "old", 10, 10, 100, DeleteFixture::Clean);
    let facts = finish_change_window(&metadata, &[file], &[], &request_context()).unwrap();
    let ConnectorChangeWindowFacts::Difference {
        negative_weight: ConnectorChangeWindowNegativeWeight::Possible,
        partition_impact: ConnectorChangeWindowPartitionImpact::Exact(impact),
        volume,
    } = facts
    else {
        panic!("expected an exact file removal");
    };
    assert_eq!(volume.source_event_rows_upper_bound, 10);
    assert_eq!(volume.source_data_file_bytes, 100);
    assert!(!impact.has_row_deletes());
    assert!(impact.added().is_empty());
    assert_eq!(impact.removed().len(), 1);
    let partition = &impact.removed()[0];
    assert_eq!(
        partition.partition_spec_identity(),
        &crate::storage_inspector::exact_partition_spec_version(
            metadata.default_partition_spec_id()
        ),
    );
    assert_eq!(partition.fields()[0].source_column(), "id");
    assert_eq!(
        partition.fields()[0].value(),
        &ConnectorChangePartitionValue::String(Arc::from("7"))
    );
}

#[test]
fn invalid_dv_cardinalities_surface_as_corrupt_data() {
    let metadata = volume_metadata(FormatVersion::V3, false);
    let file = |endpoint, deleted| {
        volume_file(
            &metadata,
            "same",
            endpoint,
            100,
            100,
            DeleteFixture::Dv(deleted),
        )
    };
    for (from, to, message) in [
        (
            vec![file(10, 18)],
            vec![file(20, 17)],
            "cardinality decreased",
        ),
        (Vec::new(), vec![file(20, 101)], "cardinality exceeds"),
        (vec![file(10, 101)], Vec::new(), "cardinality exceeds"),
    ] {
        let error = finish_change_window(&metadata, &from, &to, &request_context()).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.message().contains(message), "{error:?}");
    }
}

#[test]
fn partition_deduplication_and_budget_do_not_change_difference_volume_or_validation() {
    let metadata = volume_metadata(FormatVersion::V3, true);
    let from = vec![
        volume_file(
            &metadata,
            "other-removed",
            10,
            10,
            100,
            DeleteFixture::Clean,
        ),
        volume_file(&metadata, "removed", 10, 10, 100, DeleteFixture::Dv(4)),
        volume_file(&metadata, "same", 10, 10, 200, DeleteFixture::Dv(2)),
    ];
    let to = vec![
        volume_file(&metadata, "a-added", 20, 10, 300, DeleteFixture::Clean),
        volume_file(&metadata, "b-added", 20, 10, 400, DeleteFixture::Dv(3)),
        volume_file(&metadata, "same", 20, 10, 200, DeleteFixture::Dv(4)),
    ];
    let ConnectorChangeWindowFacts::Difference {
        negative_weight,
        partition_impact,
        volume,
    } = finish_change_window(&metadata, &from, &to, &request_context()).unwrap()
    else {
        panic!("expected a difference");
    };
    assert_eq!(
        negative_weight,
        ConnectorChangeWindowNegativeWeight::Possible
    );
    assert_eq!(volume.source_event_rows_upper_bound, 35);
    assert_eq!(volume.source_data_file_bytes, 1100);
    let ConnectorChangeWindowPartitionImpact::Exact(impact) = partition_impact else {
        panic!("expected exact partition impact");
    };
    assert!(impact.has_row_deletes());
    assert_eq!(impact.added().len(), 1);
    assert_eq!(impact.removed().len(), 1);
    assert_eq!(impact.added(), impact.removed());
    let budget = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
        16,
        24,
    )
    .unwrap();
    assert_eq!(
        finish_change_window(&metadata, &from, &to, &budget).unwrap(),
        ConnectorChangeWindowFacts::Difference {
            negative_weight,
            partition_impact: ConnectorChangeWindowPartitionImpact::Unavailable,
            volume,
        },
    );

    let mut bad = volume_file(&metadata, "z-bad", 20, 10, 100, DeleteFixture::Clean);
    bad.partition_values = Some(Struct::empty());
    let mut to = to;
    to.push(bad);
    assert_eq!(
        finish_change_window(&metadata, &from, &to, &budget)
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::CorruptData,
    );
}

#[test]
fn a_delete_snapshot_with_equal_endpoints_is_net_zero_in_v2_and_v3() {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let metadata = volume_metadata(version, false);
        let started_at = metadata.last_updated_ms();
        let with_row_ids = |id, parent, operation| {
            if version == FormatVersion::V3 {
                Snapshot::builder()
                    .with_snapshot_id(id)
                    .with_parent_snapshot_id(parent)
                    .with_sequence_number(id)
                    .with_timestamp_ms(started_at + id)
                    .with_manifest_list(format!("file:///tmp/manifest-list-{id}.avro"))
                    .with_summary(Summary {
                        operation,
                        additional_properties: HashMap::new(),
                    })
                    .with_schema_id(0)
                    .with_row_range(0, 0)
                    .build()
            } else {
                snapshot_at(id, parent, operation, &[], 0, started_at + id)
            }
        };
        let metadata = metadata
            .into_builder(None)
            .add_snapshot(with_row_ids(1, None, Operation::Append))
            .unwrap()
            .add_snapshot(with_row_ids(2, Some(1), Operation::Delete))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert!(matches!(
            prepare_change_window(&metadata, 1, 2, &request_context()).unwrap(),
            ChangeWindowPlan::Endpoints { .. }
        ));
        let from = volume_file(&metadata, "same", 10, 10, 100, DeleteFixture::Clean);
        let to = volume_file(&metadata, "same", 20, 10, 100, DeleteFixture::Clean);
        assert_eq!(
            finish_change_window(&metadata, &[from], &[to], &request_context()).unwrap(),
            ConnectorChangeWindowFacts::NetZero(ConnectorChangeWindowNetZeroBasis::EqualEndpoints),
        );
    }
}
