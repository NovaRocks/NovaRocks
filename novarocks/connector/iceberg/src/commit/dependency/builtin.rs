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

//! Current admission policies decide which source scope to observe.

use super::super::model::{ArtifactWriter, Dependency, EntryIdentity, OperationIntent};
use super::{Culprit, ValidationInputs, Verdict};
use crate::iceberg::Result;
use crate::iceberg::spec::TableMetadata;

// Design: ADR-0171 (docs/adr/ADR-0171-commit-operation-model.md)
pub async fn validate(
    intent: &OperationIntent,
    metadata: &TableMetadata,
    parent: Option<i64>,
    io: &dyn ArtifactWriter,
) -> Result<Verdict> {
    let mut inputs = ValidationInputs::new(metadata, parent, io);
    validate_dependencies(
        intent.dependencies(),
        intent.start().map(|start| start.snapshot_id),
        &mut inputs,
    )
    .await
}

/// Reuse the dependency owner for a metadata-only post-rejection diagnosis.
/// Other dependencies must not open manifests or history for this diagnostic.
pub(crate) async fn diagnose_ref_unchanged(
    intent: &OperationIntent,
    metadata: &TableMetadata,
    parent: Option<i64>,
    io: &dyn ArtifactWriter,
) -> Result<Verdict> {
    let mut inputs = ValidationInputs::new(metadata, parent, io);
    validate_dependencies(
        &[Dependency::RefUnchanged],
        intent.start().map(|start| start.snapshot_id),
        &mut inputs,
    )
    .await
}

async fn validate_dependencies(
    dependencies: &[Dependency],
    start: Option<i64>,
    inputs: &mut ValidationInputs<'_>,
) -> Result<Verdict> {
    for dependency in dependencies {
        let culprit = match dependency {
            Dependency::NoRead => None,
            Dependency::RefUnchanged => (inputs.parent != start).then_some(Culprit::Ref {
                expected: start,
                actual: inputs.parent,
            }),
            Dependency::MeasuredSnapshotExists(id) => inputs
                .metadata
                .snapshot_by_id(*id)
                .is_none()
                .then_some(Culprit::Snapshot(*id)),
            Dependency::RegisteredFilesNotLive(paths) => {
                let parent = inputs.parent;
                let requested = paths.iter().map(String::as_str).collect();
                let live = inputs.live_data_matches(&requested).await?;
                paths.iter().find_map(|path| {
                    live.contains(path).then(|| Culprit::Entry {
                        snapshot: parent.expect("live data has a parent"),
                        identity: EntryIdentity::DataFile { path: path.clone() },
                    })
                })
            }
        };
        if let Some(culprit) = culprit {
            return Ok(Verdict::Broken {
                dependency: dependency.clone(),
                culprit,
            });
        }
    }
    Ok(Verdict::Holds)
}

#[cfg(test)]
mod tests {
    use super::super::super::model::{
        ArtifactClass, ArtifactKind, AttemptArtifacts, AttemptToken, ObjectIdentity, OperationToken,
    };
    use super::super::HistoryGap;
    use super::*;
    use crate::iceberg::io::FileIO;
    use crate::iceberg::spec::{
        FormatVersion, Operation, PartitionSpec, Schema, Snapshot, SortOrder, Summary,
        TableMetadataBuilder,
    };
    use std::collections::{BTreeSet, HashMap};

    struct TestIo {
        io: FileIO,
    }
    impl TestIo {
        fn new() -> Self {
            Self {
                io: FileIO::new_with_fs(),
            }
        }
    }
    impl ArtifactWriter for TestIo {
        fn file_io(&self) -> &FileIO {
            &self.io
        }
        fn allocate(&self, _: ArtifactClass, _: ArtifactKind) -> Result<ObjectIdentity> {
            panic!("Validation must not allocate artifacts")
        }
        fn attempt_token(&self) -> AttemptToken {
            AttemptToken::new(
                OperationToken::from_write(
                    novarocks_spi::connector::ConnectorWriteOperationId::from_bytes([1; 16]),
                ),
                0,
            )
        }
        fn check_active(&self) -> Result<()> {
            Ok(())
        }
        fn snapshot_artifacts(&self, _: &[ObjectIdentity]) -> Result<AttemptArtifacts> {
            panic!("Validation must not freeze artifacts")
        }
    }
    fn metadata() -> TableMetadata {
        TableMetadataBuilder::new(
            Schema::builder().build().unwrap(),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "/tmp/iru5-dependency".into(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }
    fn append_snapshot(
        metadata: TableMetadata,
        id: i64,
        parent: Option<i64>,
        operation: Operation,
    ) -> TableMetadata {
        let sequence = metadata.last_sequence_number() + 1;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence)
            .with_timestamp_ms(metadata.last_updated_ms() + 1)
            .with_manifest_list(format!("/absent/history-{id}.avro"))
            .with_summary(Summary {
                operation,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(metadata.current_schema_id())
            .build();
        metadata
            .into_builder(None)
            .add_snapshot(snapshot)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }
    async fn check(
        dependencies: &[Dependency],
        start: Option<i64>,
        metadata: &TableMetadata,
        parent: Option<i64>,
    ) -> Verdict {
        let io = TestIo::new();
        validate_dependencies(
            dependencies,
            start,
            &mut ValidationInputs::new(metadata, parent, &io),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn ref_dependency_uses_exact_target_parent_without_reading_manifests() {
        let metadata = append_snapshot(metadata(), 11, None, Operation::Append);
        assert_eq!(
            check(&[Dependency::RefUnchanged], Some(11), &metadata, Some(11)).await,
            Verdict::Holds
        );
        assert_eq!(
            check(&[Dependency::RefUnchanged], Some(11), &metadata, Some(12)).await,
            Verdict::Broken {
                dependency: Dependency::RefUnchanged,
                culprit: Culprit::Ref {
                    expected: Some(11),
                    actual: Some(12)
                }
            }
        );
        assert_eq!(
            check(&[Dependency::RefUnchanged], None, &metadata, None).await,
            Verdict::Holds
        );
    }

    #[tokio::test]
    async fn no_read_dependency_survives_expired_start_and_ref_rollback() {
        let metadata = append_snapshot(metadata(), 11, None, Operation::Append);
        assert_eq!(
            check(&[Dependency::NoRead], Some(99), &metadata, Some(11)).await,
            Verdict::Holds
        );
        assert_eq!(
            check(&[Dependency::NoRead], Some(11), &metadata, None).await,
            Verdict::Holds
        );
    }

    #[tokio::test]
    async fn measured_snapshot_check_is_metadata_only_and_reports_the_measured_id() {
        let metadata = append_snapshot(metadata(), 11, None, Operation::Append);
        assert_eq!(
            check(
                &[Dependency::MeasuredSnapshotExists(11)],
                None,
                &metadata,
                Some(11)
            )
            .await,
            Verdict::Holds
        );
        assert_eq!(
            check(
                &[Dependency::MeasuredSnapshotExists(10)],
                None,
                &metadata,
                Some(11)
            )
            .await,
            Verdict::Broken {
                dependency: Dependency::MeasuredSnapshotExists(10),
                culprit: Culprit::Snapshot(10)
            }
        );
    }

    #[tokio::test]
    async fn registered_files_on_an_unborn_ref_have_no_live_conflict() {
        assert_eq!(
            check(
                &[Dependency::RegisteredFilesNotLive(vec![
                    "/external/file.parquet".into()
                ])],
                None,
                &metadata(),
                None
            )
            .await,
            Verdict::Holds
        );
    }

    #[tokio::test]
    async fn live_registration_reports_exact_entry_and_history_reads_only_own_manifests() {
        use super::super::history::{HistoryObservationError, physical_added_data};
        use crate::iceberg::spec::{
            DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter,
            ManifestWriterBuilder,
        };
        let dir = tempfile::tempdir().unwrap();
        let io = TestIo::new();
        let mut metadata = metadata();
        let mut inherited_manifests: Vec<crate::iceberg::spec::ManifestFile> = Vec::new();
        for (id, path) in [
            (11, "/external/first.parquet"),
            (12, "/external/second.parquet"),
            (13, "/external/second.parquet"),
            (14, "/external/first.parquet"),
        ] {
            let file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path.into())
                .file_format(DataFileFormat::Parquet)
                .record_count(3)
                .file_size_in_bytes(100)
                .build()
                .unwrap();
            let output = io
                .file_io()
                .new_output(
                    dir.path()
                        .join(format!("manifest-{id}.avro"))
                        .to_str()
                        .unwrap(),
                )
                .unwrap();
            let mut writer = ManifestWriterBuilder::new(
                output,
                Some(id),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().as_ref().clone(),
            )
            .build_v2_data();
            if id == 13 {
                writer.add_existing_file(file, 12, 2, Some(2)).unwrap();
            } else {
                writer.add_file(file, id - 10).unwrap();
            }
            let manifest = writer.write_manifest_file().await.unwrap();
            let list_path = dir
                .path()
                .join(format!("list-{id}.avro"))
                .to_str()
                .unwrap()
                .to_owned();
            let mut list = ManifestListWriter::v2(
                io.file_io()
                    .new_output(&list_path)
                    .unwrap()
                    .writer()
                    .await
                    .unwrap(),
                id,
                (id > 11).then_some(id - 1),
                id - 10,
            );
            let mut manifests = inherited_manifests;
            if id == 13 {
                manifests.retain(|descriptor| descriptor.added_snapshot_id != 12);
            }
            manifests.push(manifest.clone());
            list.add_manifests(manifests.into_iter()).unwrap();
            list.close().await.unwrap();
            let snapshot = Snapshot::builder()
                .with_snapshot_id(id)
                .with_parent_snapshot_id((id > 11).then_some(id - 1))
                .with_sequence_number(id - 10)
                .with_timestamp_ms(metadata.last_updated_ms() + 1)
                .with_manifest_list(list_path)
                .with_summary(Summary {
                    operation: if id == 12 || id == 13 {
                        Operation::Replace
                    } else {
                        Operation::Append
                    },
                    additional_properties: HashMap::new(),
                })
                .with_schema_id(metadata.current_schema_id())
                .build();
            metadata = metadata
                .into_builder(None)
                .add_snapshot(snapshot)
                .unwrap()
                .build()
                .unwrap()
                .metadata;
            inherited_manifests = crate::manifest::load_manifest_list(
                metadata.snapshot_by_id(id).unwrap(),
                io.file_io(),
                &metadata,
            )
            .await
            .unwrap()
            .entries()
            .to_vec();
        }
        let mut inputs = ValidationInputs::new(&metadata, Some(12), &io);
        let live = inputs.compact_live_entries().await.unwrap();
        assert_eq!(live.len(), 2);
        let identity = EntryIdentity::DataFile {
            path: "/external/first.parquet".into(),
        };
        assert_eq!(live[&identity].frozen.facts().added_snapshot_id, Some(11));
        assert_eq!(live[&identity].frozen.facts().record_count, 3);
        assert_eq!(live[&identity].source_index, 0);
        assert_eq!(live[&identity].ordinal, 0);
        assert_eq!(live[&identity].logical_size, 100);
        assert_eq!(
            validate_dependencies(
                &[Dependency::RegisteredFilesNotLive(vec![
                    "/external/first.parquet".into()
                ])],
                Some(999),
                &mut inputs
            )
            .await
            .unwrap(),
            Verdict::Broken {
                dependency: Dependency::RegisteredFilesNotLive(vec![
                    "/external/first.parquet".into()
                ]),
                culprit: Culprit::Entry {
                    snapshot: 12,
                    identity
                }
            }
        );
        assert_eq!(
            validate_dependencies(
                &[Dependency::RegisteredFilesNotLive(vec![
                    "/external/new.parquet".into()
                ])],
                Some(999),
                &mut inputs
            )
            .await
            .unwrap(),
            Verdict::Holds
        );
        let corrupted = ValidationInputs::new(&metadata, Some(14), &io);
        let duplicate_paths = BTreeSet::from(["/external/first.parquet"]);
        assert!(
            corrupted
                .live_data_matches(&duplicate_paths)
                .await
                .unwrap_err()
                .to_string()
                .contains("Duplicate live Iceberg entry identity")
        );
        let unrelated_paths = BTreeSet::from(["/external/second.parquet"]);
        assert_eq!(
            corrupted.live_data_matches(&unrelated_paths).await.unwrap(),
            BTreeSet::from(["/external/second.parquet".to_owned()]),
        );

        let observe =
            |start, end| physical_added_data(&metadata, start, end, io.file_io(), || Ok(()));
        let replaced = observe(Some(11), Some(13)).await.unwrap();
        assert_eq!(replaced.window.endpoints(), (Some(11), Some(13)));
        assert_eq!(
            replaced.paths,
            BTreeSet::from(["/external/second.parquet".to_owned()])
        );
        assert!(observe(Some(12), Some(13)).await.unwrap().paths.is_empty());
        assert!(observe(Some(13), Some(13)).await.unwrap().paths.is_empty());
        assert_eq!(
            observe(None, Some(13)).await.unwrap().paths,
            BTreeSet::from([
                "/external/first.parquet".to_owned(),
                "/external/second.parquet".to_owned()
            ]),
        );
        assert!(matches!(
            observe(Some(13), Some(11)).await,
            Err(HistoryObservationError::Gap(HistoryGap::StartNotAncestor {
                start: 13,
                parent: Some(11)
            }))
        ));
    }

    #[test]
    fn test_history_dependency_requires_continuity_and_filters_operation_labels() {
        let metadata = append_snapshot(metadata(), 11, None, Operation::Append);
        let metadata = append_snapshot(metadata, 12, Some(11), Operation::Replace);
        let metadata = append_snapshot(metadata, 13, Some(12), Operation::Delete);
        let io = TestIo::new();
        let inputs = ValidationInputs::new(&metadata, Some(13), &io);
        let window = inputs.history(Some(11)).unwrap();
        assert_eq!(
            window
                .snapshots()
                .iter()
                .map(|snapshot| snapshot.snapshot_id())
                .collect::<Vec<_>>(),
            vec![13, 12]
        );
        assert_eq!(
            window
                .filtered(&[Operation::Delete])
                .map(|snapshot| snapshot.snapshot_id())
                .collect::<Vec<_>>(),
            vec![13]
        );
        #[derive(Clone, Debug, Eq, PartialEq)]
        struct ContinuousHistoryDependency;
        let test_dependency = ContinuousHistoryDependency;
        let validate_history =
            |start, parent| match super::super::history_window(&metadata, start, parent) {
                Ok(_) => Verdict::Holds,
                Err(reason) => Verdict::Unprovable {
                    dependency: test_dependency.clone(),
                    reason,
                },
            };
        assert_eq!(
            validate_history(Some(99), Some(13)),
            Verdict::Unprovable {
                dependency: test_dependency.clone(),
                reason: HistoryGap::StartExpired(99)
            }
        );
        assert_eq!(
            validate_history(Some(13), Some(11)),
            Verdict::Unprovable {
                dependency: test_dependency,
                reason: HistoryGap::StartNotAncestor {
                    start: 13,
                    parent: Some(11)
                }
            }
        );
        let expired = metadata
            .clone()
            .into_builder(None)
            .remove_snapshots(&[11])
            .build()
            .unwrap()
            .metadata;
        assert_eq!(
            super::super::history_window(&expired, Some(11), Some(13)).unwrap_err(),
            HistoryGap::StartExpired(11)
        );
        let broken_chain = metadata
            .into_builder(None)
            .remove_snapshots(&[12])
            .build()
            .unwrap()
            .metadata;
        assert_eq!(
            super::super::history_window(&broken_chain, Some(11), Some(13)).unwrap_err(),
            HistoryGap::SnapshotMissing(12)
        );
    }
}
