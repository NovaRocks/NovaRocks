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

use std::cell::Cell;
use std::collections::HashMap;

use futures::channel::oneshot;

use super::packing::{Layout, plan};
use super::*;
use crate::commit::preparation::ManifestMergePolicy;
use crate::commit::staging::{TestWriter, write_manifest, write_manifest_list};
use crate::iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Manifest,
    ManifestContentType, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
    SortOrder, Summary, TableMetadataBuilder, Type, UNASSIGNED_SEQUENCE_NUMBER,
};

fn descriptor(name: &str, bytes: i64, spec_id: i32, content: ManifestContentType) -> ManifestFile {
    ManifestFile {
        manifest_path: format!("s3://bucket/{name}.avro"),
        manifest_length: bytes,
        partition_spec_id: spec_id,
        content,
        sequence_number: 3,
        min_sequence_number: 3,
        added_snapshot_id: 10,
        added_files_count: Some(1),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(1),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(0),
        partitions: None,
        key_metadata: None,
        first_row_id: None,
    }
}

fn candidate(
    name: &str,
    bytes: i64,
    spec_id: i32,
    content: ManifestContentType,
) -> MergeCandidate<'static> {
    MergeCandidate::committed(descriptor(name, bytes, spec_id, content), 10, "old-list")
}

fn inspect(layouts: Vec<Layout<'_>>) -> Vec<(bool, Vec<String>)> {
    layouts
        .into_iter()
        .map(|layout| {
            let (merge, candidates) = match layout {
                Layout::Keep(candidates) => (false, candidates),
                Layout::Merge(candidates) => (true, candidates),
            };
            let names = candidates
                .into_iter()
                .map(|candidate| {
                    candidate
                        .descriptor
                        .manifest_path
                        .trim_start_matches("s3://bucket/")
                        .trim_end_matches(".avro")
                        .to_owned()
                })
                .collect();
            (merge, names)
        })
        .collect()
}

#[test]
fn defaults_and_disabled_layout_are_captured_from_standard_properties() {
    let defaults = ManifestMergePolicy::from_properties(&HashMap::new()).unwrap();
    assert_eq!(defaults.min_count, 100);
    assert_eq!(defaults.target_bytes, 8 * 1024 * 1024);
    assert!(defaults.enabled);
    let policy = ManifestMergePolicy::from_properties(&HashMap::from([
        ("commit.manifest-merge.enabled".into(), "false".into()),
        ("commit.manifest.min-count-to-merge".into(), "2".into()),
        ("commit.manifest.target-size-bytes".into(), "12".into()),
    ]))
    .unwrap();
    assert_eq!(policy.min_count, 2);
    assert_eq!(policy.target_bytes, 12);
    assert!(!policy.enabled);
    let data = ManifestContentType::Data;
    let deletes = ManifestContentType::Deletes;
    let candidates = vec![
        candidate("D0", 3, 0, data),
        candidate("Q0", 3, 7, deletes),
        candidate("D1", 4, 7, data),
    ];
    assert_eq!(
        inspect(plan(candidates, policy).unwrap()),
        [(
            false,
            vec!["D0", "Q0", "D1"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        )]
    );
}

#[test]
fn tail_packing_limits_only_the_first_bin_of_each_content_stream() {
    let data = ManifestContentType::Data;
    let deletes = ManifestContentType::Deletes;
    let candidates = vec![
        candidate("D0", 2, 0, data),
        candidate("D1", 2, 0, data),
        candidate("H0", 3, 7, data),
        candidate("Q0", 4, 0, deletes),
        candidate("D2", 3, 0, data),
        candidate("H1", 3, 7, data),
        candidate("D3", 4, 0, data),
        candidate("D4", 4, 0, data),
        candidate("Q1", 4, 0, deletes),
        candidate("oversized", 80, 7, data),
    ];
    let layouts = plan(
        candidates,
        ManifestMergePolicy {
            enabled: true,
            min_count: 4,
            target_bytes: 8,
        },
    )
    .unwrap();
    let expected = [
        (true, &["H0", "H1"][..]),
        (false, &["oversized"][..]),
        (false, &["D0", "D1", "D2"][..]),
        (true, &["D3", "D4"][..]),
        (false, &["Q0", "Q1"][..]),
    ];
    assert_eq!(
        inspect(layouts),
        expected
            .map(|(merge, names)| (merge, names.iter().map(|name| (*name).to_owned()).collect()))
    );
}

#[tokio::test]
async fn window_drains_started_work_after_the_first_error() {
    let (started_tx, started_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let mut started_tx = Some(started_tx);
    let mut started_rx = Some(started_rx);
    let mut finish_rx = Some(finish_rx);
    let scheduled = Cell::new(0);
    let failed = Cell::new(false);
    let exited = Cell::new(false);
    let mut window = std::pin::pin!(ordered_window(
        2,
        [0, 1, 2],
        || Ok(()),
        |item| {
            scheduled.set(scheduled.get() + 1);
            let started_tx = (item == 1).then(|| started_tx.take().unwrap());
            let started_rx = (item == 0).then(|| started_rx.take().unwrap());
            let finish_rx = (item == 1).then(|| finish_rx.take().unwrap());
            let (failed, exited) = (&failed, &exited);
            async move {
                match item {
                    0 => {
                        started_rx.unwrap().await.unwrap();
                        failed.set(true);
                        Err(invalid("first failure"))
                    }
                    1 => {
                        started_tx.unwrap().send(()).unwrap();
                        finish_rx.unwrap().await.unwrap();
                        exited.set(true);
                        Err(invalid("drained failure"))
                    }
                    _ => panic!("must not replenish"),
                }
            }
        },
    ));
    // Poll until the first error was delivered to the window. The second job
    // still owns its in-flight operation and the window is still pending.
    std::future::poll_fn(|cx| {
        use std::future::Future;
        use std::task::Poll;
        assert!(window.as_mut().poll(cx).is_pending());
        if failed.get() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    assert_eq!(scheduled.get(), 2);
    assert!(!exited.get());
    finish_tx.send(()).unwrap();
    let result: Result<Vec<()>> = window.await;
    assert!(result.unwrap_err().to_string().contains("first failure"));
    assert!(exited.get());
    assert_eq!(scheduled.get(), 2);
}

#[tokio::test]
async fn one_position_finishes_in_order_and_stops_after_cancellation() {
    let active = Cell::new(true);
    let result = ordered_window(
        1,
        [1, 2],
        || {
            active
                .get()
                .then_some(())
                .ok_or_else(|| invalid("operation stopped"))
        },
        |value| {
            let active = &active;
            async move {
                assert_eq!(value, 1);
                active.set(false);
                Ok(value)
            }
        },
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("operation stopped")
    );
    assert_eq!(
        ordered_window(1, [3, 2, 1], || Ok(()), |value| async move { Ok(value) })
            .await
            .unwrap(),
        [3, 2, 1]
    );
}

fn schema(ty: PrimitiveType) -> Schema {
    Schema::builder()
        .with_fields([Arc::new(NestedField::required(
            1,
            "value",
            Type::Primitive(ty),
        ))])
        .build()
        .unwrap()
}

fn table(version: FormatVersion) -> TableMetadata {
    table_with_schema(version, schema(PrimitiveType::Long))
}

fn table_with_schema(version: FormatVersion, schema: Schema) -> TableMetadata {
    TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "s3://bucket/table".into(),
        version,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}

fn data(name: &str, row_id: Option<i64>) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("s3://bucket/{name}.parquet"))
        .file_format(DataFileFormat::Parquet)
        .record_count(2)
        .file_size_in_bytes(10)
        .first_row_id(row_id)
        .build()
        .unwrap()
}

fn added(file: DataFile, data_sequence: Option<i64>) -> ManifestEntryWrite {
    ManifestEntryWrite::Added(
        match data_sequence {
            Some(sequence) => AddedContent::rewritten_data(file, 0, sequence),
            None => AddedContent::new_logical_data(file, 0),
        }
        .unwrap(),
    )
}

fn deleted(file: DataFile) -> ManifestEntryWrite {
    use crate::commit::model::{EntryFacts, EntryIdentity};
    let frozen = FrozenEntry::new(
        EntryIdentity::try_from(&file).unwrap(),
        EntryFacts {
            added_snapshot_id: Some(4),
            data_sequence: Some(2),
            file_sequence: Some(3),
            partition_spec_id: 0,
            partition: file.partition().clone(),
            record_count: file.record_count(),
            first_row_id: file.first_row_id(),
        },
    )
    .unwrap();
    ManifestEntryWrite::Deleted { file, frozen }
}

fn context() -> PreparationContext {
    PreparationContext {
        merge_policy: Ok(ManifestMergePolicy {
            enabled: true,
            min_count: 2,
            target_bytes: 8 * 1024 * 1024,
        }),
        io_window_width: 1,
    }
}

#[tokio::test]
async fn merge_preserves_current_status_and_old_identity_at_capacity_one() {
    let metadata = table(FormatVersion::V3);
    let artifacts = TestWriter::new();
    let spec = metadata.default_partition_spec().as_ref();
    let mut old = write_manifest(
        &artifacts,
        FormatVersion::V3,
        10,
        metadata.current_schema().clone(),
        spec.clone(),
        ManifestContentType::Data,
        [
            added(data("old", Some(800)), None),
            added(data("old-inherited", None), None),
            deleted(data("previous-delete", Some(100))),
        ],
    )
    .await
    .unwrap();
    // Independently supply the old list's assignment; only the row-ID gap
    // after the explicitly assigned file should receive 1000.
    old.first_row_id = Some(1000);
    old.sequence_number = 5;
    let pending = write_manifest(
        &artifacts,
        FormatVersion::V3,
        20,
        metadata.current_schema().clone(),
        spec.clone(),
        ManifestContentType::Data,
        [
            added(data("new", None), None),
            added(data("new-explicit-sequence", Some(601)), Some(2)),
            deleted(data("current-delete", Some(201))),
        ],
    )
    .await
    .unwrap();
    assert_eq!(pending.sequence_number, UNASSIGNED_SEQUENCE_NUMBER);

    let outputs = merge_manifests(
        &artifacts,
        &metadata,
        20,
        context(),
        0,
        vec![
            MergeCandidate::pending(pending),
            MergeCandidate::committed(old, 10, "old-list"),
        ],
    )
    .await
    .unwrap();
    let [manifest] = outputs.as_slice() else {
        panic!("expected the selected bin's output");
    };
    // This merge has both preserved and unassigned live files. The list still
    // owns allocation for the unassigned files.
    assert_eq!(manifest.first_row_id, None);
    let bytes = artifacts
        .file_io()
        .new_input(&manifest.manifest_path)
        .unwrap()
        .read()
        .await
        .unwrap();
    let raw = Manifest::parse_avro(&bytes).unwrap();
    let facts = raw
        .entries()
        .iter()
        .map(|entry| {
            (
                entry.file_path(),
                entry.status,
                entry.snapshot_id,
                entry.sequence_number,
                entry.file_sequence_number,
                entry.data_file().first_row_id(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        facts,
        [
            (
                "s3://bucket/new.parquet",
                ManifestStatus::Added,
                Some(20),
                None,
                None,
                None
            ),
            (
                "s3://bucket/new-explicit-sequence.parquet",
                ManifestStatus::Added,
                Some(20),
                Some(2),
                None,
                Some(601)
            ),
            (
                "s3://bucket/current-delete.parquet",
                ManifestStatus::Deleted,
                Some(20),
                Some(2),
                Some(3),
                Some(201)
            ),
            (
                "s3://bucket/old.parquet",
                ManifestStatus::Existing,
                Some(10),
                Some(5),
                Some(5),
                Some(800)
            ),
            (
                "s3://bucket/old-inherited.parquet",
                ManifestStatus::Existing,
                Some(10),
                Some(5),
                Some(5),
                Some(1000)
            ),
        ]
    );
}

fn bounded_data(name: &str, lower: Datum, upper: Datum) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("s3://bucket/{name}.parquet"))
        .file_format(DataFileFormat::Parquet)
        .record_count(2)
        .file_size_in_bytes(10)
        .lower_bounds(HashMap::from([(1, lower)]))
        .upper_bounds(HashMap::from([(1, upper)]))
        .build()
        .unwrap()
}

async fn merge_date_and_timestamp(
    date_days: (i32, i32),
    timestamp: (Datum, Datum),
) -> Result<Manifest> {
    let original = table_with_schema(FormatVersion::V3, schema(PrimitiveType::Date));
    let old_schema = original.current_schema().clone();
    let spec = original.default_partition_spec().as_ref().clone();
    let metadata = original
        .into_builder(None)
        .add_current_schema(schema(timestamp.0.data_type().clone()))?
        .build()?
        .metadata;
    let artifacts = TestWriter::new();
    let mut old = write_manifest(
        &artifacts,
        FormatVersion::V3,
        10,
        old_schema,
        spec.clone(),
        ManifestContentType::Data,
        [added(
            bounded_data("date", Datum::date(date_days.0), Datum::date(date_days.1)),
            None,
        )],
    )
    .await?;
    let old_path = old.manifest_path.clone();
    let old_bytes = artifacts.file_io().new_input(&old_path)?.read().await?;
    old.sequence_number = 5;
    old.min_sequence_number = 5;
    let pending = write_manifest(
        &artifacts,
        FormatVersion::V3,
        20,
        metadata.current_schema().clone(),
        spec,
        ManifestContentType::Data,
        [added(
            bounded_data("timestamp", timestamp.0, timestamp.1),
            None,
        )],
    )
    .await?;
    let outputs = merge_manifests(
        &artifacts,
        &metadata,
        20,
        context(),
        0,
        vec![
            MergeCandidate::pending(pending),
            MergeCandidate::committed(old, 10, "old-list"),
        ],
    )
    .await;
    assert_eq!(
        artifacts.file_io().new_input(&old_path)?.read().await?,
        old_bytes
    );
    let outputs = outputs?;
    let [output] = outputs.as_slice() else {
        panic!("expected a merged manifest")
    };
    let bytes = artifacts
        .file_io()
        .new_input(&output.manifest_path)?
        .read()
        .await?;
    let manifest = Manifest::parse_avro(&bytes)?;
    assert_eq!(
        manifest.metadata().schema.schema_id(),
        metadata.current_schema_id()
    );
    Ok(manifest)
}

#[tokio::test]
async fn mixed_date_and_timestamp_manifest_has_sdk_readable_bounds() {
    let micros = merge_date_and_timestamp(
        (-2, 3),
        (Datum::timestamp_micros(-15), Datum::timestamp_micros(17)),
    )
    .await
    .unwrap();
    let nanos = merge_date_and_timestamp(
        (-2, 3),
        (Datum::timestamp_nanos(-15), Datum::timestamp_nanos(17)),
    )
    .await
    .unwrap();

    for (manifest, old_expected, new_expected) in [
        (
            micros,
            (
                Datum::timestamp_micros(-172_800_000_000),
                Datum::timestamp_micros(259_200_000_000),
            ),
            (Datum::timestamp_micros(-15), Datum::timestamp_micros(17)),
        ),
        (
            nanos,
            (
                Datum::timestamp_nanos(-172_800_000_000_000),
                Datum::timestamp_nanos(259_200_000_000_000),
            ),
            (Datum::timestamp_nanos(-15), Datum::timestamp_nanos(17)),
        ),
    ] {
        assert_eq!(manifest.entries().len(), 2);
        let timestamp = manifest.entries()[0].data_file();
        let date = manifest.entries()[1].data_file();
        assert_eq!(timestamp.file_path(), "s3://bucket/timestamp.parquet");
        assert_eq!(date.file_path(), "s3://bucket/date.parquet");
        assert_eq!(
            timestamp.lower_bounds(),
            &HashMap::from([(1, new_expected.0)])
        );
        assert_eq!(
            timestamp.upper_bounds(),
            &HashMap::from([(1, new_expected.1)])
        );
        assert_eq!(date.lower_bounds(), &HashMap::from([(1, old_expected.0)]));
        assert_eq!(date.upper_bounds(), &HashMap::from([(1, old_expected.1)]));
    }
}

#[tokio::test]
async fn merge_rejects_date_bounds_outside_the_promoted_range() {
    for (lower, upper) in [
        (Datum::timestamp_micros(-1), Datum::timestamp_micros(1)),
        (Datum::timestamp_nanos(-1), Datum::timestamp_nanos(1)),
    ] {
        let error = merge_date_and_timestamp((0, i32::MAX), (lower, upper))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("DATE manifest bound 2147483647 days overflows")
        );
    }
}

#[tokio::test]
async fn merge_reuses_exact_ordered_inputs_but_rewrites_changed_order_or_encoding() {
    use crate::commit::model::ArtifactWriter;
    use crate::commit::overwrite::preparer_tests::Fixture;
    use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    let fixture = Fixture::new();
    let metadata = fixture.metadata(FormatVersion::V2);
    let artifacts = fixture.operation.begin_attempt().unwrap();
    let snapshot = artifacts.stage_snapshot_id(0, &metadata).unwrap();
    let mut inputs = Vec::new();
    for name in ["one", "two"] {
        inputs.push(
            write_manifest(
                &artifacts,
                metadata.format_version(),
                snapshot,
                metadata.current_schema().clone(),
                metadata.default_partition_spec().as_ref().clone(),
                ManifestContentType::Data,
                [added(data(name, None), None)],
            )
            .await
            .unwrap(),
        );
    }
    fn candidates(inputs: &[ManifestFile]) -> Vec<MergeCandidate<'_>> {
        inputs
            .iter()
            .cloned()
            .map(MergeCandidate::pending)
            .collect()
    }
    fn paths(events: &[crate::fs_io::TestStorageRequest], operation: &str) -> BTreeSet<String> {
        events
            .iter()
            .filter(|event| event.operation == operation)
            .map(|event| event.path.clone())
            .collect()
    }
    fn check_entries(output: &ManifestFile, expected: &[&str]) {
        let bytes = std::fs::read(output.manifest_path.trim_start_matches("file://")).unwrap();
        let manifest = crate::iceberg::spec::Manifest::parse_avro(&bytes).unwrap();
        let actual = manifest
            .entries()
            .iter()
            .map(|entry| {
                assert_eq!(entry.status, ManifestStatus::Added);
                assert_eq!(entry.data_file().record_count(), 2);
                entry.data_file().file_path()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
    async fn merge_observed(
        artifacts: &dyn ArtifactWriter,
        metadata: &TableMetadata,
        snapshot: i64,
        inputs: &[ManifestFile],
    ) -> (ManifestFile, Vec<crate::fs_io::TestStorageRequest>) {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let output = crate::fs_io::TEST_STORAGE_REQUESTS
            .scope(
                trace.clone(),
                merge_manifests(
                    artifacts,
                    metadata,
                    snapshot,
                    context(),
                    0,
                    candidates(inputs),
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            output.len(),
            1,
            "The real min-count=2 packer must merge these two inputs"
        );
        let events = trace.lock().unwrap().clone();
        (output.into_iter().next().unwrap(), events)
    }
    let source_paths = inputs
        .iter()
        .map(|input| input.manifest_path.clone())
        .collect::<BTreeSet<_>>();
    let (first, first_events) = merge_observed(&artifacts, &metadata, snapshot, &inputs).await;
    assert_eq!(paths(&first_events, "read"), source_paths);
    assert_eq!(
        paths(&first_events, "write"),
        BTreeSet::from([first.manifest_path.clone()])
    );
    check_entries(
        &first,
        &["s3://bucket/one.parquet", "s3://bucket/two.parquet"],
    );
    let (same, same_events) = merge_observed(&artifacts, &metadata, snapshot, &inputs).await;
    assert_eq!(same, first);
    assert!(paths(&same_events, "read").is_empty());
    assert!(paths(&same_events, "write").is_empty());
    let reversed = inputs.iter().rev().cloned().collect::<Vec<_>>();
    let (reordered, reordered_events) =
        merge_observed(&artifacts, &metadata, snapshot, &reversed).await;
    assert_ne!(reordered.manifest_path, first.manifest_path);
    assert_eq!(paths(&reordered_events, "read"), source_paths);
    assert_eq!(
        paths(&reordered_events, "write"),
        BTreeSet::from([reordered.manifest_path.clone()])
    );
    check_entries(
        &reordered,
        &["s3://bucket/two.parquet", "s3://bucket/one.parquet"],
    );
    let mut fields = metadata.current_schema().as_struct().fields().to_vec();
    fields.push(Arc::new(NestedField::optional(
        2,
        "note",
        Type::Primitive(PrimitiveType::String),
    )));
    let changed = metadata
        .into_builder(None)
        .add_current_schema(Schema::builder().with_fields(fields).build().unwrap())
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let (reencoded, reencoded_events) =
        merge_observed(&artifacts, &changed, snapshot, &inputs).await;
    assert_ne!(reencoded.manifest_path, first.manifest_path);
    assert_eq!(paths(&reencoded_events, "read"), source_paths);
    assert_eq!(
        paths(&reencoded_events, "write"),
        BTreeSet::from([reencoded.manifest_path.clone()])
    );
    check_entries(
        &reencoded,
        &["s3://bucket/one.parquet", "s3://bucket/two.parquet"],
    );
    for output in [first, reordered, reencoded] {
        assert!(std::path::Path::new(output.manifest_path.trim_start_matches("file://")).exists());
    }
    // The test never dispatches a catalog request; its original owner can
    // reclaim every actually written input/output after all I/O has exited.
    assert_eq!(
        fixture
            .operation
            .cleanup(crate::commit::model::CleanupScope::EntireOperation)
            .await,
        crate::commit::model::IcebergCleanupReport::Complete { deleted: 5 }
    );
}

#[test]
fn rolled_full_manifests_are_kept_alone_and_small_tail_uses_the_existing_merge_policy() {
    let data = ManifestContentType::Data;
    let pending = |name, bytes| MergeCandidate::pending(descriptor(name, bytes, 0, data));
    let policy = ManifestMergePolicy {
        enabled: true,
        min_count: 3,
        target_bytes: 100,
    };
    let retained = || candidate("retained", 25, 0, data);
    let rolled = vec![
        pending("first", 103),
        pending("second", 101),
        pending("tail", 70),
        retained(),
    ];
    assert_eq!(
        inspect(plan(rolled, policy).unwrap()),
        [
            (false, vec!["first".into()]),
            (false, vec!["second".into()]),
            (true, vec!["tail".into(), "retained".into()]),
        ],
    );
    let first_short = vec![pending("tail", 70), retained()];
    assert_eq!(
        inspect(plan(first_short, policy).unwrap()),
        [(false, vec!["tail".into(), "retained".into()])],
    );
}

fn snapshot_with_row_range(
    metadata: &TableMetadata,
    id: i64,
    operation: Operation,
    list: &str,
    (first, rows): (u64, u64),
) -> Snapshot {
    Snapshot::builder()
        .with_snapshot_id(id)
        .with_parent_snapshot_id(metadata.current_snapshot_id())
        .with_sequence_number(metadata.next_sequence_number())
        .with_timestamp_ms(metadata.last_updated_ms() + 1)
        .with_manifest_list(list)
        .with_summary(Summary {
            operation,
            additional_properties: HashMap::new(),
        })
        .with_schema_id(metadata.current_schema_id())
        .with_row_range(first, rows)
        .build()
}

#[tokio::test]
async fn merging_preserved_rewrite_files_does_not_allocate_new_row_ids() {
    use crate::manifest::{load_manifest, load_manifest_list};

    let artifacts = TestWriter::new();
    let metadata = table(FormatVersion::V3);
    let mut seed_manifests = Vec::new();
    for name in ["survivor", "replaced"] {
        seed_manifests.push(
            write_manifest(
                &artifacts,
                FormatVersion::V3,
                10,
                metadata.current_schema().clone(),
                metadata.default_partition_spec().as_ref().clone(),
                ManifestContentType::Data,
                [added(data(name, None), None)],
            )
            .await
            .unwrap(),
        );
    }
    let list = write_manifest_list(&artifacts, &metadata, 10, None, seed_manifests)
        .await
        .unwrap();
    let original = snapshot_with_row_range(
        &metadata,
        10,
        Operation::Append,
        list.object.path(),
        list.row_range.unwrap(),
    );
    let base = metadata
        .into_builder(None)
        .set_branch_snapshot(original.clone(), "main")
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    assert_eq!(base.next_row_id(), 4);
    let original_list = load_manifest_list(&original, artifacts.file_io(), &base)
        .await
        .unwrap();
    let [survivor, replaced] = original_list.entries() else {
        panic!("expected separate seed manifests");
    };
    let expected_first = replaced.first_row_id.unwrap();
    let mut addition = write_manifest(
        &artifacts,
        FormatVersion::V3,
        20,
        base.current_schema().clone(),
        base.default_partition_spec().as_ref().clone(),
        ManifestContentType::Data,
        [added(
            data("rewritten", Some(i64::try_from(expected_first).unwrap())),
            Some(original.sequence_number()),
        )],
    )
    .await
    .unwrap();
    // This is the descriptor authored by a compact stage that certified
    // stored row IDs. Only the survivor is carried from the old list.
    addition.first_row_id = Some(expected_first);
    let merged = merge_manifests(
        &artifacts,
        &base,
        20,
        context(),
        0,
        vec![
            MergeCandidate::pending(addition),
            MergeCandidate::committed(survivor.clone(), 10, original.manifest_list()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(merged.len(), 1, "force the standard merge path");
    let list = write_manifest_list(&artifacts, &base, 20, Some(10), merged)
        .await
        .unwrap();
    assert_eq!(list.row_range, Some((base.next_row_id(), 0)));
    let replace = snapshot_with_row_range(
        &base,
        20,
        Operation::Replace,
        list.object.path(),
        list.row_range.unwrap(),
    );
    assert_eq!(replace.added_rows_count(), Some(0));
    let next = base
        .clone()
        .into_builder(None)
        .set_branch_snapshot(replace.clone(), "main")
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    assert_eq!(next.next_row_id(), base.next_row_id());

    let list = load_manifest_list(&replace, artifacts.file_io(), &next)
        .await
        .unwrap();
    let [descriptor] = list.entries() else {
        panic!("expected one merged manifest in the replacement list");
    };
    assert_eq!(descriptor.first_row_id, survivor.first_row_id);
    let manifest = load_manifest(descriptor, artifacts.file_io())
        .await
        .unwrap();
    let entries = ManifestSource::committed(20, replace.manifest_list(), descriptor)
        .bind(&manifest)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.raw.file_path(),
                entry.first_row_id,
                entry.sequences.data_sequence,
            )
        })
        .collect::<Vec<_>>();
    let e = Some(original.sequence_number());
    assert_eq!(
        entries,
        [
            ("s3://bucket/rewritten.parquet", Some(2), e),
            ("s3://bucket/survivor.parquet", Some(0), e),
        ]
    );
}

#[tokio::test]
async fn merging_tombstones_or_empty_unassigned_data_consumes_no_row_ids() {
    let artifacts = TestWriter::new();
    let metadata = table(FormatVersion::V3);
    for include_empty_file in [false, true] {
        let mut candidates = Vec::new();
        for name in ["a", "b"] {
            let entry = if include_empty_file && name == "b" {
                let empty = DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path("s3://bucket/empty.parquet".to_owned())
                    .file_format(DataFileFormat::Parquet)
                    .record_count(0)
                    .file_size_in_bytes(10)
                    .build()
                    .unwrap();
                added(empty, None)
            } else {
                deleted(data(name, Some(10)))
            };
            candidates.push(MergeCandidate::pending(
                write_manifest(
                    &artifacts,
                    FormatVersion::V3,
                    20,
                    metadata.current_schema().clone(),
                    metadata.default_partition_spec().as_ref().clone(),
                    ManifestContentType::Data,
                    [entry],
                )
                .await
                .unwrap(),
            ));
        }
        let manifests = merge_manifests(&artifacts, &metadata, 20, context(), 0, candidates)
            .await
            .unwrap();
        let [manifest] = manifests.as_slice() else {
            panic!("expected one merged manifest");
        };
        assert_eq!(manifest.first_row_id, None);
        assert_eq!(manifest.added_rows_count, Some(0));
        assert_eq!(manifest.existing_rows_count, Some(0));
        let list = write_manifest_list(&artifacts, &metadata, 20, None, manifests)
            .await
            .unwrap();
        assert_eq!(list.row_range, Some((metadata.next_row_id(), 0)));
    }
}
