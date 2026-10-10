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

//! Current admission dependencies deliberately do not require a history window.

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

async fn validate_dependencies(
    dependencies: &[Dependency],
    start: Option<i64>,
    inputs: &mut ValidationInputs<'_>,
) -> Result<Verdict> {
    for dependency in dependencies {
        let culprit = match dependency {
            Dependency::NoReadDependency => None,
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
                let live = inputs.live_set().await?;
                paths.iter().find_map(|path| {
                    let identity = EntryIdentity::DataFile { path: path.clone() };
                    live.contains_key(&identity).then(|| Culprit::Entry {
                        snapshot: parent.expect("nonempty live set has a parent"),
                        identity,
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
    use std::collections::HashMap;

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
            check(
                &[Dependency::NoReadDependency],
                Some(99),
                &metadata,
                Some(11)
            )
            .await,
            Verdict::Holds
        );
        assert_eq!(
            check(&[Dependency::NoReadDependency], Some(11), &metadata, None).await,
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
        use crate::iceberg::spec::{
            DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter,
            ManifestWriterBuilder,
        };
        let dir = tempfile::tempdir().unwrap();
        let io = TestIo::new();
        let mut metadata = metadata();
        let mut inherited_manifest: Option<crate::iceberg::spec::ManifestFile> = None;
        for (id, path) in [
            (11, "/external/first.parquet"),
            (12, "/external/second.parquet"),
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
                None,
                metadata.current_schema().clone(),
                metadata.default_partition_spec().as_ref().clone(),
            )
            .build_v2_data();
            writer.add_file(file, id - 10).unwrap();
            let manifest = writer.write_manifest_file().await.unwrap();
            let list_path = dir
                .path()
                .join(format!("list-{id}.avro"))
                .to_str()
                .unwrap()
                .to_owned();
            let mut list = ManifestListWriter::v2(
                io.file_io().new_output(&list_path).unwrap(),
                id,
                (id == 12).then_some(11),
                id - 10,
            );
            let mut manifests = Vec::new();
            if let Some(inherited) = inherited_manifest.as_ref() {
                manifests.push(inherited.clone());
            }
            manifests.push(manifest.clone());
            list.add_manifests(manifests.into_iter()).unwrap();
            list.close().await.unwrap();
            let snapshot = Snapshot::builder()
                .with_snapshot_id(id)
                .with_parent_snapshot_id((id == 12).then_some(11))
                .with_sequence_number(id - 10)
                .with_timestamp_ms(metadata.last_updated_ms() + 1)
                .with_manifest_list(list_path)
                .with_summary(Summary {
                    operation: Operation::Append,
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
            inherited_manifest = Some(
                metadata
                    .snapshot_by_id(id)
                    .unwrap()
                    .load_manifest_list(io.file_io(), &metadata)
                    .await
                    .unwrap()
                    .entries()
                    .last()
                    .unwrap()
                    .clone(),
            );
        }
        let mut inputs = ValidationInputs::new(&metadata, Some(12), &io);
        let live = inputs.live_set().await.unwrap();
        assert_eq!(live.len(), 2);
        let identity = EntryIdentity::DataFile {
            path: "/external/first.parquet".into(),
        };
        assert_eq!(live[&identity].frozen.facts().added_snapshot_id, Some(11));
        assert_eq!(live[&identity].frozen.facts().record_count, 3);
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
        let own = inputs
            .own_snapshot_entries(metadata.snapshot_by_id(12).unwrap())
            .await
            .unwrap();
        assert_eq!(own.len(), 1);
        assert_eq!(own[0].0.path(), "/external/second.parquet");
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
