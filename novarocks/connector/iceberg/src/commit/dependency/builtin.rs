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

use super::super::commit_failure::validation_failure;
use super::super::model::{
    ArtifactWriter, CommitProtocol, Dependency, OperationIntent, WindowValidation,
};
use super::rewrite_files::{RewriteFilesWindowCheck, validate_rewrite_files};
use super::row_delta::validate_row_delta;
use super::{Culprit, RemovedDataFiles, ValidationInputs, Verdict};
use crate::document_storage::keyed::view::parse_properties;
use crate::iceberg::Result;
use crate::iceberg::spec::TableMetadata;

// Design: ADR-0171 (docs/adr/ADR-0171-commit-operation-model.md)
pub async fn validate(
    intent: &OperationIntent,
    metadata: &TableMetadata,
    parent: Option<i64>,
    io: &dyn ArtifactWriter,
) -> Result<Verdict> {
    validate_with_removed_data(intent, metadata, parent, io, None).await
}

pub(crate) async fn validate_with_removed_data(
    intent: &OperationIntent,
    metadata: &TableMetadata,
    parent: Option<i64>,
    io: &dyn ArtifactWriter,
    removed_data: Option<&dyn RemovedDataFiles>,
) -> Result<Verdict> {
    let mut inputs = ValidationInputs::new(metadata, parent, io);
    inputs.stage = intent.compact_stage();
    let start = intent.start().map(|start| start.snapshot_id);
    let dependency = validate_dependency_checks(intent.dependencies(), start, &inputs);
    if dependency != Verdict::Holds {
        return Ok(dependency);
    }
    let protocol = intent.protocol();
    let (roots, slots) = match protocol {
        CommitProtocol::MvRefresh { admitted, .. } => (Some(&admitted.roots), &admitted.slots),
        CommitProtocol::MvAlterProperties { slots } => (None, slots),
        _ => {
            return match protocol.window() {
                WindowValidation::None => Ok(Verdict::Holds),
                WindowValidation::RowDelta(check) => {
                    validate_row_delta(check, &inputs, start, io, removed_data).await
                }
                WindowValidation::RewriteFiles(replaced) => {
                    validate_rewrite_files(
                        RewriteFilesWindowCheck::new(replaced, intent.start()),
                        &inputs,
                        io,
                    )
                    .await
                }
            };
        }
    };
    let reject = |detail: &'static str| {
        Ok(Verdict::ValidationConflict(validation_failure(
            protocol.operation(),
            detail,
        )))
    };
    if let Some(detail) = protocol.fixed_mv_main()
        && parent != start
    {
        return reject(detail);
    }
    let properties = parse_properties(metadata.properties()).map_err(|error| {
        super::super::slot_change::document_error(
            crate::document_storage::observation::keyed_read_error(error),
        )
    })?;
    if roots.is_some_and(|roots| {
        roots.iter().any(|(name, expected)| {
            properties.roots.get(name).map(|root| root.identity) != *expected
        })
    }) {
        return reject("Materialized view root changed since admission");
    }
    if slots.iter().any(|(name, expected)| {
        properties
            .slots
            .get(name)
            .map(|slot| slot.value.digest(name, slot.writer))
            != *expected
    }) {
        return reject("Materialized view slot value or writer changed since admission");
    }
    Ok(Verdict::Holds)
}

/// Reuse the dependency owner for a metadata-only post-rejection diagnosis.
/// Other dependencies must not open manifests or history for this diagnostic.
pub(crate) async fn diagnose_ref_unchanged(
    intent: &OperationIntent,
    metadata: &TableMetadata,
    parent: Option<i64>,
    io: &dyn ArtifactWriter,
) -> Result<Verdict> {
    let inputs = ValidationInputs::new(metadata, parent, io);
    Ok(validate_dependency_checks(
        &[Dependency::RefUnchanged],
        intent.start().map(|start| start.snapshot_id),
        &inputs,
    ))
}

#[cfg(test)]
async fn validate_dependencies(
    dependencies: &[Dependency],
    start: Option<i64>,
    inputs: &mut ValidationInputs<'_>,
) -> Result<Verdict> {
    Ok(validate_dependency_checks(dependencies, start, inputs))
}

fn validate_dependency_checks(
    dependencies: &[Dependency],
    start: Option<i64>,
    inputs: &ValidationInputs<'_>,
) -> Verdict {
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
        };
        if let Some(culprit) = culprit {
            return Verdict::Broken {
                dependency: dependency.clone(),
                culprit,
            };
        }
    }
    Verdict::Holds
}

#[cfg(test)]
pub(super) mod tests {
    use super::super::super::model::{
        ArtifactKind, AttemptToken, EntryIdentity, ObjectIdentity, OperationToken,
    };
    use super::super::HistoryGap;
    use super::*;
    use crate::iceberg::io::FileIO;
    use crate::iceberg::spec::{
        FormatVersion, Operation, PartitionSpec, Schema, Snapshot, SortOrder, Summary,
        TableMetadataBuilder,
    };
    use std::collections::HashMap;

    pub(in crate::commit::dependency) struct TestIo {
        io: FileIO,
    }
    impl TestIo {
        pub(in crate::commit::dependency) fn new() -> Self {
            Self {
                io: FileIO::new_with_fs(),
            }
        }
    }
    impl ArtifactWriter for TestIo {
        fn file_io(&self) -> &FileIO {
            &self.io
        }
        fn allocate(&self, _: ArtifactKind) -> Result<ObjectIdentity> {
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
        fn snapshot_artifacts(
            &self,
            _lists: &[crate::commit::model::ObjectIdentity],
            _statistics: &[crate::commit::model::ObjectIdentity],
        ) -> crate::iceberg::Result<crate::commit::model::AttemptArtifacts> {
            panic!("Validation must not freeze artifacts")
        }

        fn record_manifest(
            &self,
            _object: &crate::commit::model::ObjectIdentity,
            _added_paths: &[String],
            _existing_paths: &[String],
            _replaces: &[String],
        ) -> crate::iceberg::Result<()> {
            panic!("Validation must not record a manifest")
        }

        fn record_manifest_list(
            &self,
            _object: &crate::commit::model::ObjectIdentity,
            _snapshot_id: i64,
            _paths: &[String],
        ) -> crate::iceberg::Result<()> {
            panic!("Validation must not record a manifest list")
        }
    }
    pub(in crate::commit::dependency) fn metadata() -> TableMetadata {
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
    pub(in crate::commit::dependency) fn append_snapshot(
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
            .with_schema_id(metadata.current_schema_id());
        let snapshot = if metadata.format_version() == FormatVersion::V3 {
            snapshot.with_row_range(metadata.next_row_id(), 0).build()
        } else {
            snapshot.build()
        };
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
    async fn compact_live_entries_preserve_identity_position_and_size() {
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
            writer.add_file(file, id - 10).unwrap();
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
                    operation: if id == 12 {
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
        let inputs = ValidationInputs::new(&metadata, Some(12), &io);
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
