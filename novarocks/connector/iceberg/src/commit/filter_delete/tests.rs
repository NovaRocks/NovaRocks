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

use std::error::Error as _;
use std::sync::Mutex;

use novarocks_spi::connector::ConnectorMutationFailure;

use super::*;
use crate::commit::model::{AddedContent, ArtifactWriter, FrozenRequest};
use crate::commit::overwrite::preparer_tests::{Fixture, data, raw_entries};
use crate::commit::staging::{ManifestEntryWrite, StagingBase, StagingEngine, write_manifest};
use crate::iceberg::spec::{
    DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestContentType, ManifestEntry,
    ManifestList, ManifestStatus, Operation, Struct,
};

const A: &str = "file:///a.parquet";
const B: &str = "file:///b.parquet";
const DV: &str = "file:///dv.puffin";
const POSITION: &str = "file:///position.parquet";
const EQUALITY: &str = "file:///equality.parquet";

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
            path if Some(path) == self.all => FileVerdict::AllRows,
            path if Some(path) == self.some => FileVerdict::SomeRows,
            _ => FileVerdict::NoRows,
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
