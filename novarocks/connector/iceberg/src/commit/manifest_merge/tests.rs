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
use crate::commit::staging::{TestWriter, write_manifest};
use crate::iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Manifest,
    ManifestContentType, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
    TableMetadataBuilder, Type, UNASSIGNED_SEQUENCE_NUMBER,
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
