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
use std::error::Error as _;
use std::sync::Mutex;

use novarocks_spi::connector::ConnectorMutationFailure;
use novarocks_spi::connector::read_stack::{
    Bound, ConnectorValue, ConnectorValueType, Domain, Range, TupleDomain, ValueSet,
};

use super::*;
use crate::commit::delete_condition::DeleteCondition;
use crate::commit::model::{AddedContent, ArtifactWriter, CommitOperation, FrozenRequest};
use crate::commit::overwrite::preparer_tests::{Fixture, data, raw_entries};
use crate::commit::staging::{ManifestEntryWrite, StagingBase, StagingEngine, write_manifest};
use crate::iceberg::spec::{
    DataFile, DataFileBuilder, DataFileFormat, Datum, FieldSummary, FormatVersion,
    ManifestContentType, ManifestEntry, ManifestList, ManifestStatus, NestedField, Operation,
    PrimitiveType, Schema, SortOrder, Struct, TableMetadataBuilder, Transform,
    UnboundPartitionSpecBuilder,
};
use crate::typed_read::IcebergColumnHandle;

const A: &str = "file:///a.parquet";
const B: &str = "file:///b.parquet";
const DV: &str = "file:///dv.puffin";
const POSITION: &str = "file:///position.parquet";
const EQUALITY: &str = "file:///equality.parquet";

fn conditional_metadata(primitive: PrimitiveType, transform: Option<Transform>) -> TableMetadata {
    let schema = Schema::builder()
        .with_fields([
            Arc::new(NestedField::optional(
                1,
                "other",
                PrimitiveType::Long.into(),
            )),
            Arc::new(NestedField::optional(2, "part", primitive.into())),
        ])
        .build()
        .unwrap();
    let mut spec = UnboundPartitionSpecBuilder::new();
    if let Some(transform) = transform {
        spec = spec
            .add_partition_field(2, "part_partition", transform)
            .unwrap();
    }
    TableMetadataBuilder::new(
        schema,
        spec.build(),
        SortOrder::unsorted_order(),
        "memory://unit/delete".to_owned(),
        FormatVersion::V3,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}

fn frozen_condition(metadata: &TableMetadata, domain: Domain) -> DeleteCondition {
    let schema = metadata.current_schema();
    let handle = IcebergColumnHandle::base_column(schema.field_by_id(2).unwrap()).unwrap();
    let domains =
        TupleDomain::with_column_domains([(handle, domain)].into_iter().collect()).unwrap();
    DeleteCondition::from_enforced(schema, &domains).unwrap()
}

fn range(low: Bound, high: Bound, value_type: ConnectorValueType) -> Domain {
    let range = Range::try_new(value_type, low, high).unwrap();
    Domain::new(ValueSet::of_ranges(value_type, vec![range]).unwrap(), false)
}

fn judge<'a>(condition: &'a DeleteCondition, metadata: &TableMetadata) -> ConditionJudge<'a> {
    let mut judge = ConditionJudge::bind(condition, metadata, CommitOperation::Delete).unwrap();
    judge
        .prepare_specs(
            metadata,
            metadata.partition_specs_iter().map(|spec| spec.spec_id()),
            &|| Ok(()),
        )
        .unwrap();
    judge
}

fn candidate(
    partition: Struct,
    records: u64,
    bounds: (Option<Datum>, Option<Datum>),
    nulls: Vec<(i32, u64)>,
    nans: Option<u64>,
) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(A.to_owned())
        .file_format(DataFileFormat::Parquet)
        .partition(partition)
        .record_count(records)
        .file_size_in_bytes(100)
        .lower_bounds(bounds.0.map(|datum| (2, datum)).into_iter().collect())
        .upper_bounds(bounds.1.map(|datum| (2, datum)).into_iter().collect())
        .null_value_counts(nulls.into_iter().collect())
        .nan_value_counts(nans.map(|count| (2, count)).into_iter().collect())
        .build()
        .unwrap()
}

fn verdict(
    judge: &ConditionJudge<'_>,
    metadata: &TableMetadata,
    spec_id: i32,
    file: &DataFile,
) -> FileVerdict {
    judge
        .file_verdict(&CandidateFile {
            metadata,
            spec_id,
            file,
        })
        .unwrap()
}

#[test]
fn base_binding_preserves_original_id_through_rename_reuse_and_int_promotion() {
    let source = conditional_metadata(PrimitiveType::Int, Some(Transform::Identity));
    let condition = frozen_condition(
        &source,
        Domain::single_value(ConnectorValue::Integer(10)).unwrap(),
    );
    let evolved = Schema::builder()
        .with_fields([
            Arc::new(NestedField::optional(
                1,
                "other",
                PrimitiveType::Long.into(),
            )),
            Arc::new(NestedField::optional(
                2,
                "renamed",
                PrimitiveType::Long.into(),
            )),
            Arc::new(NestedField::optional(3, "part", PrimitiveType::Int.into())),
        ])
        .build()
        .unwrap();
    let base = source
        .clone()
        .into_builder(None)
        .add_schema(evolved)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let spec = base.default_partition_spec_id();
    let judge = judge(&condition, &base);
    for partition in [Datum::int(10), Datum::long(10)] {
        let file = candidate(
            Struct::from_iter([Some(partition.into())]),
            2,
            (None, None),
            vec![],
            None,
        );
        assert_eq!(verdict(&judge, &base, spec, &file), FileVerdict::All);
    }
    let incompatible = Schema::builder()
        .with_fields([
            Arc::new(NestedField::optional(
                1,
                "other",
                PrimitiveType::Long.into(),
            )),
            Arc::new(NestedField::optional(
                2,
                "renamed",
                PrimitiveType::String.into(),
            )),
            Arc::new(NestedField::optional(3, "part", PrimitiveType::Int.into())),
        ])
        .build()
        .unwrap();
    let removed = Schema::builder()
        .with_fields([
            Arc::new(NestedField::optional(
                1,
                "other",
                PrimitiveType::Long.into(),
            )),
            Arc::new(NestedField::optional(3, "part", PrimitiveType::Int.into())),
        ])
        .build()
        .unwrap();
    let unpartitioned = conditional_metadata(PrimitiveType::Int, None);
    for schema in [incompatible, removed] {
        let base = unpartitioned
            .clone()
            .into_builder(None)
            .add_schema(schema)
            .unwrap()
            .set_current_schema(-1)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let error = ConditionJudge::bind(&condition, &base, CommitOperation::Delete)
            .err()
            .unwrap();
        assert!(error.message().contains("field 2"));
        assert!(
            error
                .source()
                .unwrap()
                .downcast_ref::<ConnectorMutationFailure>()
                .is_some()
        );
    }
}

#[test]
fn bucket_evolution_requires_positive_null_stats_for_strict_range() {
    let source = conditional_metadata(PrimitiveType::Int, Some(Transform::Identity));
    let condition = frozen_condition(
        &source,
        range(
            Bound::Unbounded,
            Bound::Exclusive(ConnectorValue::Integer(20)),
            ConnectorValueType::Integer,
        ),
    );
    let prior_spec = source.default_partition_spec_id();
    let bucket = UnboundPartitionSpecBuilder::new()
        .add_partition_field(2, "part_bucket", Transform::Bucket(16))
        .unwrap();
    let base = source
        .into_builder(None)
        .add_partition_spec(bucket.build())
        .unwrap()
        .set_default_partition_spec(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let judge = judge(&condition, &base);
    let bucket_spec = base.default_partition_spec_id();
    for (nulls, expected) in [
        (vec![], FileVerdict::Partial),
        (vec![(1, 0)], FileVerdict::Partial),
        (vec![(2, 1)], FileVerdict::Partial),
        (vec![(2, 0)], FileVerdict::All),
    ] {
        let file = candidate(
            Struct::from_iter([Some(Datum::int(3).into())]),
            2,
            (None, Some(Datum::int(10))),
            nulls,
            None,
        );
        assert_eq!(verdict(&judge, &base, bucket_spec, &file), expected);
    }
    let identity_file = candidate(
        Struct::from_iter([Some(Datum::int(10).into())]),
        2,
        (None, None),
        vec![],
        None,
    );
    assert_eq!(
        verdict(&judge, &base, prior_spec, &identity_file),
        FileVerdict::All
    );
    let null_partition = candidate(Struct::from_iter([None]), 2, (None, None), vec![], None);
    assert_eq!(
        verdict(&judge, &base, prior_spec, &null_partition),
        FileVerdict::None
    );
    assert_eq!(
        verdict(&judge, &base, bucket_spec, &null_partition),
        FileVerdict::None
    );
    let zero = candidate(Struct::from_iter([None]), 0, (None, None), vec![], None);
    assert_eq!(verdict(&judge, &base, prior_spec, &zero), FileVerdict::All);
}

#[test]
fn date_day_is_exact_and_only_null_matches_the_null_partition() {
    let base = conditional_metadata(PrimitiveType::Date, Some(Transform::Day));
    let spec = base.default_partition_spec_id();
    let points = frozen_condition(
        &base,
        Domain::single_value(ConnectorValue::Date(-1)).unwrap(),
    );
    let null = frozen_condition(&base, Domain::only_null(ConnectorValueType::Date));
    let partition = candidate(
        Struct::from_iter([Some(Datum::date(-1).into())]),
        2,
        (None, None),
        vec![],
        None,
    );
    let null_partition = candidate(Struct::from_iter([None]), 2, (None, None), vec![], None);
    assert_eq!(
        verdict(&judge(&points, &base), &base, spec, &partition),
        FileVerdict::All
    );
    assert_eq!(
        verdict(&judge(&points, &base), &base, spec, &null_partition),
        FileVerdict::None
    );
    assert_eq!(
        verdict(&judge(&null, &base), &base, spec, &null_partition),
        FileVerdict::All
    );
    assert_eq!(
        verdict(&judge(&null, &base), &base, spec, &partition),
        FileVerdict::None
    );
    let malformed = candidate(Struct::empty(), 2, (None, None), vec![], None);
    assert!(
        judge(&null, &base)
            .file_verdict(&CandidateFile {
                metadata: &base,
                spec_id: spec,
                file: &malformed,
            })
            .is_err()
    );
}

#[test]
fn shifted_legacy_time_partition_is_inclusive_but_not_a_strict_proof() {
    for (primitive, value, transform) in [
        (
            PrimitiveType::Timestamptz,
            ConnectorValue::TimestampTzMicros(0),
            Transform::Hour,
        ),
        (
            PrimitiveType::TimestampNs,
            ConnectorValue::TimestampNanos(0),
            Transform::Day,
        ),
    ] {
        let base = conditional_metadata(primitive, Some(transform));
        let spec = base.default_partition_spec_id();
        let value_type = value.value_type();
        let before = frozen_condition(
            &base,
            range(
                Bound::Unbounded,
                Bound::Exclusive(value.clone()),
                value_type,
            ),
        );
        let from = frozen_condition(
            &base,
            range(Bound::Inclusive(value), Bound::Unbounded, value_type),
        );
        let bound: fn(i32) -> Datum = if transform == Transform::Day {
            Datum::date
        } else {
            Datum::int
        };
        for (partition, before_verdict, from_verdict) in [
            (-1, FileVerdict::All, FileVerdict::None),
            (0, FileVerdict::Partial, FileVerdict::Partial),
            (1, FileVerdict::None, FileVerdict::All),
        ] {
            let file = candidate(
                Struct::from_iter([Some(bound(partition).into())]),
                2,
                (None, None),
                vec![],
                None,
            );
            assert_eq!(
                verdict(&judge(&before, &base), &base, spec, &file),
                before_verdict
            );
            assert_eq!(
                verdict(&judge(&from, &base), &base, spec, &file),
                from_verdict
            );
        }
    }
}

#[test]
fn projection_hour_overflow_keeps_file_unproven() {
    let base = conditional_metadata(PrimitiveType::Timestamp, Some(Transform::Hour));
    let spec = base.default_partition_spec_id();
    let condition = frozen_condition(
        &base,
        Domain::single_value(ConnectorValue::TimestampMicros(i64::MAX)).unwrap(),
    );
    let file = candidate(
        Struct::from_iter([Some(Datum::int(0).into())]),
        2,
        (None, None),
        vec![],
        None,
    );
    assert_eq!(
        verdict(&judge(&condition, &base), &base, spec, &file),
        FileVerdict::Partial
    );
}

#[test]
fn absent_nan_count_and_large_in_keep_files_when_metrics_cannot_prove_verdict() {
    let base = conditional_metadata(PrimitiveType::Float, None);
    let spec = base.default_partition_spec_id();
    let condition = frozen_condition(
        &base,
        range(
            Bound::Unbounded,
            Bound::Exclusive(ConnectorValue::Real(20.0)),
            ConnectorValueType::Real,
        ),
    );
    let prepared = judge(&condition, &base);
    for (nans, expected) in [(None, FileVerdict::Partial), (Some(0), FileVerdict::All)] {
        let file = candidate(
            Struct::empty(),
            2,
            (None, Some(Datum::float(10.0_f32))),
            vec![(2, 0)],
            nans,
        );
        assert_eq!(verdict(&prepared, &base, spec, &file), expected);
    }

    let base = conditional_metadata(PrimitiveType::Int, None);
    let spec = base.default_partition_spec_id();
    let in_values = |size| {
        frozen_condition(
            &base,
            Domain::new(
                ValueSet::of_values(
                    ConnectorValueType::Integer,
                    (0..size).map(ConnectorValue::Integer).collect(),
                )
                .unwrap(),
                false,
            ),
        )
    };
    let file = candidate(
        Struct::empty(),
        2,
        (Some(Datum::int(1000)), None),
        vec![],
        None,
    );
    assert_eq!(
        verdict(&judge(&in_values(200), &base), &base, spec, &file),
        FileVerdict::None
    );
    assert_eq!(
        verdict(&judge(&in_values(201), &base), &base, spec, &file),
        FileVerdict::Partial
    );
}

#[test]
fn manifest_absent_summary_keeps_a_candidate() {
    let base = conditional_metadata(PrimitiveType::Int, Some(Transform::Identity));
    let spec = base.default_partition_spec_id();
    let condition = frozen_condition(
        &base,
        Domain::single_value(ConnectorValue::Integer(10)).unwrap(),
    );
    let judge = judge(&condition, &base);
    let mut manifest = ManifestFile {
        manifest_path: "memory://unit/manifest".to_owned(),
        manifest_length: 100,
        partition_spec_id: spec,
        content: ManifestContentType::Data,
        sequence_number: 1,
        min_sequence_number: 1,
        added_snapshot_id: 7,
        added_files_count: Some(1),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(2),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(0),
        partitions: None,
        key_metadata: None,
        first_row_id: None,
    };
    assert!(judge.manifest_may_match(&manifest));
    manifest.partitions = Some(vec![FieldSummary::default()]);
    assert!(judge.manifest_may_match(&manifest));
    manifest.partitions = Some(vec![FieldSummary {
        lower_bound: Some(Datum::int(20).to_bytes().unwrap()),
        ..FieldSummary::default()
    }]);
    assert!(!judge.manifest_may_match(&manifest));
}

fn delete_file(path: &str, content: DataContentType) -> DataFile {
    DataFileBuilder::default()
        .content(content)
        .file_path(path.to_owned())
        .file_format(DataFileFormat::Parquet)
        .record_count(2)
        .file_size_in_bytes(100)
        .partition(Struct::empty())
        .equality_ids((content == DataContentType::EqualityDeletes).then_some(vec![1]))
        .build()
        .unwrap()
}

fn dv() -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(DV.to_owned())
        .file_format(DataFileFormat::Puffin)
        .partition(Struct::empty())
        .record_count(1)
        .file_size_in_bytes(1000)
        .referenced_data_file(Some(A.to_owned()))
        .content_offset(Some(4))
        .content_size_in_bytes(Some(16))
        .build()
        .unwrap()
}

struct Seed(Vec<AddedContent>);

#[async_trait]
impl Preparer for Seed {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        intent: &OperationIntent,
    ) -> Result<PreparedChange> {
        let mut manifests: Vec<_> = view
            .target_sources()
            .await?
            .iter()
            .flat_map(|sources| sources.sources())
            .map(|source| source.descriptor.clone())
            .collect();
        let metadata = view.metadata();
        let id = view.stage_snapshot_id()?;
        for content in [ManifestContentType::Data, ManifestContentType::Deletes] {
            let files: Vec<_> = self
                .0
                .iter()
                .filter(|file| {
                    (file.file().content_type() == DataContentType::Data)
                        == (content == ManifestContentType::Data)
                })
                .cloned()
                .map(ManifestEntryWrite::Added)
                .collect();
            if !files.is_empty() {
                manifests.push(
                    write_manifest(
                        view.artifacts(),
                        metadata.format_version(),
                        id,
                        metadata.current_schema().clone(),
                        metadata.default_partition_spec().as_ref().clone(),
                        content,
                        files,
                    )
                    .await?,
                );
            }
        }
        super::super::overwrite::prepare_snapshot_change(
            view,
            intent,
            id,
            Operation::Append,
            manifests,
            super::super::overwrite::snapshot_file_summary(&self.0, &[])?,
            false,
        )
        .await
    }
}

async fn seed(fixture: &Fixture, metadata: TableMetadata, files: Vec<DataFile>) -> TableMetadata {
    let files: Vec<_> = files
        .into_iter()
        .map(|file| AddedContent::new_logical_data(file, 0).unwrap())
        .collect();
    let intent = fixture.intent(&metadata, "main", files.clone());
    fixture.stage(metadata, &intent, &Seed(files)).await.0
}

async fn execute(
    fixture: &Fixture,
    base: TableMetadata,
    intent: &OperationIntent,
    preparer: &FilterDeletePreparer,
) -> Result<(TableMetadata, FrozenRequest)> {
    let attempt = fixture.operation.begin_attempt()?;
    let mut engine = StagingEngine::begin(
        StagingBase::Existing {
            metadata: base,
            metadata_location: "file:///filter-test-base.metadata.json".into(),
        },
        intent,
        &attempt,
    )?;
    let result = async {
        engine.stage(preparer).await?;
        Ok((engine.metadata().clone(), engine.freeze()?))
    }
    .await;
    if result.is_err() {
        fixture
            .operation
            .retire_attempt_bound_artifacts(attempt.attempt_token())?;
    }
    result
}

async fn list(fixture: &Fixture, metadata: &TableMetadata) -> ManifestList {
    let attempt = fixture.operation.begin_attempt().unwrap();
    crate::manifest::load_manifest_list(
        metadata.current_snapshot().unwrap(),
        attempt.file_io(),
        metadata,
    )
    .await
    .unwrap()
}

fn row(path: &str, count: u64) -> DataFile {
    data(path, count, Struct::empty())
}

fn find_entry<'a>(entries: &'a [ManifestEntry], path: &str) -> &'a ManifestEntry {
    entries
        .iter()
        .find(|entry| entry.data_file().file_path() == path)
        .unwrap()
}

fn assert_summary(metadata: &TableMetadata, expected: &[(&str, &str)], absent: &[&str]) {
    let snapshot = metadata.current_snapshot().unwrap();
    assert_eq!(snapshot.summary().operation, Operation::Delete);
    let summary = &snapshot.summary().additional_properties;
    for &(key, value) in expected {
        assert_eq!(summary.get(key).map(String::as_str), Some(value), "{key}");
    }
    for &key in absent {
        assert!(
            !summary.contains_key(key),
            "{key} should be absent: {summary:?}"
        );
    }
}

#[tokio::test]
async fn always_true_removes_all_live_contents_and_counts_the_dv_blob() {
    let fixture = Fixture::new();
    let base = seed(
        &fixture,
        fixture.metadata(FormatVersion::V3),
        vec![
            row(A, 5),
            dv(),
            delete_file(POSITION, DataContentType::PositionDeletes),
            delete_file(EQUALITY, DataContentType::EqualityDeletes),
        ],
    )
    .await;
    let intent = fixture.intent(&base, "main", vec![]);
    let (after, _) = execute(
        &fixture,
        base,
        &intent,
        &FilterDeletePreparer::always_true(),
    )
    .await
    .unwrap();
    let attempt = fixture.operation.begin_attempt().unwrap();
    let entries = raw_entries(&after, "main", &attempt).await;
    assert_eq!(entries.len(), 4);
    assert!(
        entries
            .iter()
            .all(|entry| entry.status() == ManifestStatus::Deleted)
    );
    assert_summary(
        &after,
        &[
            ("deleted-data-files", "1"),
            ("deleted-records", "5"),
            ("removed-files-size", "266"),
            ("removed-delete-files", "3"),
            ("removed-position-delete-files", "1"),
            ("removed-dvs", "1"),
            ("removed-equality-delete-files", "1"),
            ("removed-position-deletes", "3"),
            ("removed-equality-deletes", "2"),
            ("total-data-files", "0"),
            ("total-delete-files", "0"),
            ("total-records", "0"),
            ("total-files-size", "0"),
            ("total-position-deletes", "0"),
            ("total-equality-deletes", "0"),
        ],
        &[
            "added-data-files",
            "added-dvs",
            "added-delete-files",
            "added-records",
        ],
    );
}

struct Judge {
    all: Option<&'static str>,
    some: Option<&'static str>,
    skip_manifest: Option<String>,
}

impl RowFilterJudge for Judge {
    fn manifest_may_match(
        &self,
        _filter: &Predicate,
        _metadata: &TableMetadata,
        manifest: &ManifestFile,
    ) -> bool {
        self.skip_manifest.as_ref() != Some(&manifest.manifest_path)
    }

    fn file_verdict(&self, _filter: &Predicate, file: &CandidateFile<'_>) -> FileVerdict {
        match file.file.file_path() {
            path if Some(path) == self.all => FileVerdict::All,
            path if Some(path) == self.some => FileVerdict::Partial,
            _ => FileVerdict::None,
        }
    }
}

fn where_paths(
    all: Option<&'static str>,
    some: Option<&'static str>,
    skip_manifest: Option<String>,
) -> FilterDeletePreparer {
    use crate::iceberg::expr::Reference;
    use crate::iceberg::spec::Datum;

    FilterDeletePreparer::new(
        Reference::new("id").equal_to(Datum::long(7)),
        Arc::new(Judge {
            all,
            some,
            skip_manifest,
        }),
    )
}

#[tokio::test]
async fn partial_data_match_is_a_typed_conflict_before_snapshot_writes() {
    let fixture = Fixture::new();
    let base = seed(
        &fixture,
        fixture.metadata(FormatVersion::V2),
        vec![row(A, 5)],
    )
    .await;
    let intent = fixture.intent(&base, "main", vec![]);
    let error = execute(&fixture, base, &intent, &where_paths(None, Some(A), None))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::DataInvalid);
    assert!(
        error
            .message()
            .starts_with("Cannot delete file where some, but not all, rows match filter ")
    );
    assert!(error.message().contains("id"));
    assert!(error.message().ends_with(A));
    assert!(
        error
            .source()
            .unwrap()
            .downcast_ref::<ConnectorMutationFailure>()
            .is_some()
    );
    assert!(fixture.operation.artifacts().unwrap().is_empty());
}

#[tokio::test]
async fn partial_deletes_and_nonmatches_survive_with_original_entry_facts() {
    let fixture = Fixture::new();
    let base = seed(
        &fixture,
        fixture.metadata(FormatVersion::V3),
        vec![
            row(A, 5),
            row(B, 8),
            dv(),
            delete_file(EQUALITY, DataContentType::EqualityDeletes),
        ],
    )
    .await;
    let original = base.current_snapshot().unwrap().snapshot_id();
    let original_sequence = base.current_snapshot().unwrap().sequence_number();
    let intent = fixture.intent(&base, "main", vec![]);
    let (after, _) = execute(
        &fixture,
        base,
        &intent,
        &where_paths(Some(A), Some(EQUALITY), None),
    )
    .await
    .unwrap();
    let after_id = after.current_snapshot().unwrap().snapshot_id();
    let attempt = fixture.operation.begin_attempt().unwrap();
    let entries = raw_entries(&after, "main", &attempt).await;
    for path in [A, DV] {
        let entry = find_entry(&entries, path);
        assert_eq!(entry.status(), ManifestStatus::Deleted);
        assert_eq!(entry.snapshot_id(), Some(after_id));
        assert_eq!(entry.sequence_number(), Some(original_sequence));
        assert_eq!(entry.file_sequence_number, Some(original_sequence));
    }
    for path in [B, EQUALITY] {
        let entry = find_entry(&entries, path);
        assert_eq!(entry.status(), ManifestStatus::Existing);
        assert_eq!(entry.snapshot_id(), Some(original));
        assert_eq!(entry.sequence_number(), Some(original_sequence));
        assert_eq!(entry.file_sequence_number, Some(original_sequence));
    }
    assert_summary(
        &after,
        &[
            ("deleted-data-files", "1"),
            ("deleted-records", "5"),
            ("removed-files-size", "66"),
            ("removed-delete-files", "1"),
            ("removed-dvs", "1"),
            ("total-data-files", "1"),
            ("total-delete-files", "1"),
            ("total-records", "8"),
            ("total-files-size", "180"),
            ("total-position-deletes", "0"),
            ("total-equality-deletes", "2"),
        ],
        &[
            "removed-position-delete-files",
            "removed-equality-delete-files",
            "removed-equality-deletes",
        ],
    );
}

#[tokio::test]
async fn pruned_manifest_is_kept_without_opening_even_with_merge_enabled() {
    let fixture = Fixture::new();
    let base = fixture
        .metadata(FormatVersion::V3)
        .into_builder(None)
        .set_properties(std::collections::HashMap::from([
            (
                "commit.manifest-merge.enabled".to_owned(),
                "true".to_owned(),
            ),
            (
                "commit.manifest.min-count-to-merge".to_owned(),
                "2".to_owned(),
            ),
        ]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let initial = seed(&fixture, base, vec![row(B, 8)]).await;
    let earlier_list = list(&fixture, &initial).await;
    let pruned = earlier_list.entries()[0].clone();
    let base = seed(&fixture, initial, vec![row(A, 5), dv()]).await;
    let intent = fixture.intent(&base, "main", vec![]);
    let preparer = where_paths(Some(A), None, Some(pruned.manifest_path.clone()));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let (after, _) = crate::fs_io::TEST_STORAGE_REQUESTS
        .scope(trace.clone(), execute(&fixture, base, &intent, &preparer))
        .await
        .unwrap();
    assert!(
        !trace
            .lock()
            .unwrap()
            .iter()
            .any(|event| event.path == pruned.manifest_path)
    );
    assert!(list(&fixture, &after).await.entries().contains(&pruned));
    assert_summary(&after, &[("removed-dvs", "1"), ("total-records", "8")], &[]);
}

#[tokio::test]
async fn missing_parent_totals_remain_missing() {
    let fixture = Fixture::new();
    let base = seed(
        &fixture,
        fixture.metadata(FormatVersion::V2),
        vec![row(A, 5)],
    )
    .await;
    let mut json = serde_json::to_value(base).unwrap();
    json["snapshots"][0]["summary"]
        .as_object_mut()
        .unwrap()
        .retain(|key, _| !key.starts_with("total-") || key == "total-data-files");
    let base: TableMetadata = serde_json::from_value(json).unwrap();
    let intent = fixture.intent(&base, "main", vec![]);
    let (after, _) = execute(
        &fixture,
        base,
        &intent,
        &FilterDeletePreparer::always_true(),
    )
    .await
    .unwrap();
    assert_summary(
        &after,
        &[("deleted-records", "5"), ("total-data-files", "0")],
        &[
            "total-records",
            "total-files-size",
            "total-delete-files",
            "total-position-deletes",
            "total-equality-deletes",
        ],
    );
}

#[tokio::test]
async fn retry_reuses_filtered_manifest_on_advanced_base_and_recomputes_totals() {
    let fixture = Fixture::new();
    let base = seed(
        &fixture,
        fixture.metadata(FormatVersion::V2),
        vec![row(A, 5)],
    )
    .await;
    let advanced = seed(&fixture, base.clone(), vec![row(B, 13)]).await;
    let advanced_id = advanced.current_snapshot().unwrap().snapshot_id();
    let intent = fixture.intent(&base, "main", vec![]);
    let preparer = where_paths(Some(A), None, None);
    let attempt = fixture.operation.begin_attempt().unwrap();
    let mut engine = StagingEngine::begin(
        StagingBase::Existing {
            metadata: base,
            metadata_location: "file:///filter-retry-first-base.metadata.json".into(),
        },
        &intent,
        &attempt,
    )
    .unwrap();
    engine.stage(&preparer).await.unwrap();
    let first = engine.metadata().clone();
    engine.freeze().unwrap();
    let first_list = list(&fixture, &first).await;
    let filtered = first_list.entries()[0].manifest_path.clone();
    fixture
        .operation
        .retire_attempt_bound_artifacts(attempt.attempt_token())
        .unwrap();
    let trace = Arc::new(Mutex::new(Vec::new()));
    let (after, _) = crate::fs_io::TEST_STORAGE_REQUESTS
        .scope(
            trace.clone(),
            execute(&fixture, advanced, &intent, &preparer),
        )
        .await
        .unwrap();
    let snapshot = after.current_snapshot().unwrap();
    assert_eq!(snapshot.parent_snapshot_id(), Some(advanced_id));
    assert_ne!(
        snapshot.manifest_list(),
        first.current_snapshot().unwrap().manifest_list()
    );
    assert!(
        list(&fixture, &after)
            .await
            .entries()
            .iter()
            .any(|desc| desc.manifest_path == filtered)
    );
    assert!(
        !trace
            .lock()
            .unwrap()
            .iter()
            .any(|event| { event.operation == "write" && event.path == filtered })
    );
    assert_summary(
        &after,
        &[("deleted-records", "5"), ("total-records", "13")],
        &[],
    );
}

#[tokio::test]
async fn empty_table_still_publishes_an_empty_delete_snapshot() {
    let fixture = Fixture::new();
    let base = fixture.metadata(FormatVersion::V3);
    let intent = fixture.intent(&base, "main", vec![]);
    let (after, _) = execute(
        &fixture,
        base,
        &intent,
        &FilterDeletePreparer::always_true(),
    )
    .await
    .unwrap();
    assert!(list(&fixture, &after).await.entries().is_empty());
    assert_summary(
        &after,
        &[("total-records", "0"), ("total-delete-files", "0")],
        &[
            "deleted-records",
            "removed-dvs",
            "removed-delete-files",
            "removed-files-size",
        ],
    );
}

#[tokio::test]
async fn v1_truncate_writes_a_v1_data_manifest_and_zero_sequence_snapshot() {
    let fixture = Fixture::new();
    let base = seed(
        &fixture,
        fixture.metadata(FormatVersion::V1),
        vec![row(A, 5)],
    )
    .await;
    let parent = base.current_snapshot().unwrap().snapshot_id();
    let intent = fixture.intent(&base, "main", vec![]);
    let (after, _) = execute(
        &fixture,
        base,
        &intent,
        &FilterDeletePreparer::always_true(),
    )
    .await
    .unwrap();

    let snapshot = after.current_snapshot().unwrap();
    assert_eq!(after.format_version(), FormatVersion::V1);
    assert_eq!(snapshot.parent_snapshot_id(), Some(parent));
    assert_eq!(snapshot.sequence_number(), 0);
    assert_eq!(snapshot.row_range(), None);
    assert_summary(
        &after,
        &[
            ("deleted-data-files", "1"),
            ("deleted-records", "5"),
            ("total-records", "0"),
        ],
        &["removed-delete-files", "removed-dvs"],
    );

    let attempt = fixture.operation.begin_attempt().unwrap();
    let sources = crate::commit::manifest_sources::SnapshotSources::load(
        &after,
        snapshot,
        attempt.file_io(),
        || Ok(()),
    )
    .await
    .unwrap();
    let mut sources = sources.sources();
    let source = sources.next().unwrap();
    assert!(sources.next().is_none());
    assert_eq!(source.descriptor.content, ManifestContentType::Data);
    assert_eq!(source.descriptor.sequence_number, 0);
    let raw = source.load(attempt.file_io()).await.unwrap();
    assert_eq!(*raw.metadata().format_version(), FormatVersion::V1);
    let mut entries = source.bind(&raw).unwrap();
    let deleted = entries.next().unwrap().unwrap();
    assert!(entries.next().is_none());
    assert_eq!(deleted.raw.status(), ManifestStatus::Deleted);
    assert_eq!(deleted.raw.file_path(), A);
    assert_eq!(deleted.sequences.snapshot_id, Some(snapshot.snapshot_id()));
    assert_eq!(deleted.sequences.data_sequence, Some(0));
    assert_eq!(deleted.sequences.file_sequence, Some(0));
}
