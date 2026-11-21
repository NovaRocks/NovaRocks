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

use std::collections::BTreeMap;
use std::fmt;

use novarocks_failure_contract::FailureProjection;
use novarocks_failure_contract::connector_reason::ICEBERG_COMMIT_VALIDATION_CONFLICT;
use novarocks_spi::connector::{ConnectorMutationFailureKind, ConnectorWriteOperationId};
use tempfile::TempDir;

use super::*;
use crate::commit::dependency::validate_with_removed_data;
use crate::commit::model::{
    ArtifactKind, AttemptArtifacts, AttemptToken, ObjectIdentity, OperationIntent,
    OperationIntentParts, OperationToken, RequestShape, StartSnapshot, TableTarget,
};
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataFileBuilder, Datum, FormatVersion, Literal, ManifestFile, ManifestListWriter,
    ManifestWriterBuilder, NestedField, PrimitiveType, Schema, Snapshot, SortOrder, Struct,
    Summary, TableMetadataBuilder, Transform, Type, UnboundPartitionSpec,
};

const MISSING: &str = "Cannot commit, missing data files";
const ADDED: &str = "Found conflicting files that can contain records matching";
const REPLACED: &str = "Cannot commit, found new delete for replaced data file";
const DELETES: &str = "Found new conflicting delete files that can apply";
const REFERENCED: &str = "Cannot delete data files";
const DV: &str = "Found concurrently added DV";

enum Status {
    Added(i64),
    Deleted(i64, i64),
    Existing(i64, i64, i64),
}

struct ManifestEntry {
    file: DataFile,
    status: Status,
}

fn added(file: DataFile) -> ManifestEntry {
    ManifestEntry {
        file,
        status: Status::Added(-1),
    }
}

fn deleted(file: DataFile, sequence: i64) -> ManifestEntry {
    ManifestEntry {
        file,
        status: Status::Deleted(sequence, sequence),
    }
}

fn existing(file: DataFile, snapshot_id: i64, sequence: i64) -> ManifestEntry {
    ManifestEntry {
        file,
        status: Status::Existing(snapshot_id, sequence, sequence),
    }
}

fn file(path: &str, partition: i32) -> DataFileBuilder {
    let mut file = DataFileBuilder::default();
    file.file_path(path.into())
        .partition(
            [Some(Literal::int(partition))]
                .into_iter()
                .collect::<Struct>(),
        )
        .record_count(3)
        .file_size_in_bytes(256);
    file
}

fn data(path: &str, partition: i32) -> DataFile {
    file(path, partition)
        .content(DataContentType::Data)
        .file_format(DataFileFormat::Parquet)
        .build()
        .unwrap()
}

fn position(path: &str, target: Option<&str>, partition: i32) -> DataFile {
    let bounds = || {
        target
            .into_iter()
            .map(|target| (POSITION_FILE_PATH_FIELD_ID, Datum::string(target)))
            .collect()
    };
    file(path, partition)
        .content(DataContentType::PositionDeletes)
        .file_format(DataFileFormat::Parquet)
        .lower_bounds(bounds())
        .upper_bounds(bounds())
        .build()
        .unwrap()
}

fn dv(path: &str, target: &str, partition: i32) -> DataFile {
    file(path, partition)
        .content(DataContentType::PositionDeletes)
        .file_format(DataFileFormat::Puffin)
        .referenced_data_file(Some(target.into()))
        .content_offset(Some(8))
        .content_size_in_bytes(Some(80))
        .build()
        .unwrap()
}

struct Fixture {
    dir: TempDir,
    io: FileIO,
    metadata: TableMetadata,
    descriptors: BTreeMap<i64, Vec<ManifestFile>>,
    removed: BTreeMap<String, DataFile>,
}

impl Fixture {
    fn new(version: FormatVersion) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let io = FileIO::new_with_fs();
        let schema = Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                1,
                "region",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(1, "region", Transform::Identity)
            .unwrap()
            .build();
        let metadata = TableMetadataBuilder::new(
            schema,
            spec,
            SortOrder::unsorted_order(),
            dir.path().to_str().unwrap().into(),
            version,
            Default::default(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        Self {
            dir,
            io,
            metadata,
            descriptors: BTreeMap::new(),
            removed: BTreeMap::new(),
        }
    }

    async fn manifest(
        &self,
        id: i64,
        deletes: bool,
        entries: Vec<ManifestEntry>,
    ) -> Option<ManifestFile> {
        if entries.is_empty() {
            return None;
        }
        let path = self
            .dir
            .path()
            .join(format!("manifest-{id}-{deletes}.avro"));
        let output = self.io.new_output(path.to_str().unwrap()).unwrap();
        let writer = ManifestWriterBuilder::new(
            output,
            Some(id),
            self.metadata.current_schema().clone(),
            self.metadata.default_partition_spec().as_ref().clone(),
        );
        let mut writer = match (self.metadata.format_version(), deletes) {
            (FormatVersion::V2, true) => writer.build_v2_deletes(),
            (FormatVersion::V2, false) => writer.build_v2_data(),
            (FormatVersion::V3, true) => writer.build_v3_deletes(),
            (FormatVersion::V3, false) => writer.build_v3_data(),
            _ => unreachable!("Fixture uses Iceberg v2 or v3"),
        };
        for ManifestEntry { file, status } in entries {
            match status {
                Status::Added(sequence) => writer.add_file(file, sequence),
                Status::Deleted(sequence, file_sequence) => {
                    writer.add_delete_file(file, sequence, Some(file_sequence))
                }
                Status::Existing(owner, sequence, file_sequence) => {
                    writer.add_existing_file(file, owner, sequence, Some(file_sequence))
                }
            }
            .unwrap();
        }
        Some(writer.write_manifest_file().await.unwrap())
    }

    async fn snapshot(
        &mut self,
        id: i64,
        parent: Option<i64>,
        operation: Operation,
        entries: Vec<ManifestEntry>,
    ) {
        let (data, deletes) = entries
            .into_iter()
            .partition::<Vec<_>, _>(|entry| entry.file.content_type() == DataContentType::Data);
        let data = self.manifest(id, false, data).await;
        let deletes = self.manifest(id, true, deletes).await;
        let list_path = self.dir.path().join(format!("list-{id}.avro"));
        let list_path = list_path.to_str().unwrap();
        let seq = self.metadata.next_sequence_number();
        let start_row_id = self.metadata.next_row_id();
        let writer = self
            .io
            .new_output(list_path)
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut list = match self.metadata.format_version() {
            FormatVersion::V2 => ManifestListWriter::v2(writer, id, parent, seq),
            FormatVersion::V3 => {
                ManifestListWriter::v3(writer, id, parent, seq, Some(start_row_id))
            }
            _ => unreachable!("Fixture uses Iceberg v2 or v3"),
        };
        let manifests = parent
            .and_then(|parent| self.descriptors.get(&parent))
            .cloned()
            .unwrap_or_default();
        list.add_manifests(manifests.into_iter().chain(data).chain(deletes))
            .unwrap();
        let added_rows = list.next_row_id().map(|next| next - start_row_id);
        list.close().await.unwrap();
        let snapshot = Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(seq)
            .with_timestamp_ms(self.metadata.last_updated_ms() + 1)
            .with_manifest_list(list_path.to_string())
            .with_summary(Summary {
                operation,
                additional_properties: Default::default(),
            })
            .with_schema_id(self.metadata.current_schema_id());
        let snapshot = match added_rows {
            Some(added_rows) => snapshot.with_row_range(start_row_id, added_rows).build(),
            None => snapshot.build(),
        };
        self.metadata = self
            .metadata
            .clone()
            .into_builder(None)
            .add_snapshot(snapshot)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let sources = self.metadata.snapshot_by_id(id).unwrap();
        self.descriptors.insert(
            id,
            crate::manifest::load_manifest_list(sources, &self.io, &self.metadata)
                .await
                .unwrap()
                .entries()
                .to_vec(),
        );
    }

    /// Omit the old descriptor when building a snapshot that rewrites it.
    /// The already written parent list still represents the original state.
    fn rewrite_manifest(&mut self, owner: i64, content: ManifestContentType) {
        self.descriptors
            .get_mut(&owner)
            .unwrap()
            .retain(|descriptor| {
                descriptor.added_snapshot_id != owner || descriptor.content != content
            });
    }

    async fn check(
        &self,
        start: Option<i64>,
        parent: Option<i64>,
        check: RowDeltaWindowCheck,
    ) -> Verdict {
        let intent = OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: crate::iceberg::TableIdent::from_strs(["ns", "test"]).unwrap(),
                uuid: Some(self.metadata.uuid()),
            },
            target_ref: "main".into(),
            start: start.map(|snapshot_id| StartSnapshot {
                snapshot_id,
                sequence_number: self
                    .metadata
                    .snapshot_by_id(snapshot_id)
                    .map_or(0, |snapshot| snapshot.sequence_number()),
            }),
            changes: Default::default(),
            protocol: crate::commit::model::CommitProtocol::RowDelta {
                window: check,
                writer: crate::commit::model::WriterSchemaSpec {
                    schema_id: self.metadata.current_schema_id(),
                    default_spec_id: self.metadata.default_partition_spec_id(),
                },
            },
            dependencies: Vec::new(),
            shape: RequestShape::SnapshotProducing,
            summary: Default::default(),
            token: self.attempt_token().operation(),
        })
        .unwrap();
        validate_with_removed_data(&intent, &self.metadata, parent, self, Some(self))
            .await
            .unwrap()
    }

    fn message(&self, result: Verdict, _start: Option<i64>, _parent: Option<i64>) -> String {
        let Verdict::ValidationConflict(error) = result else {
            panic!("Expected conflict, found {result:?}");
        };
        assert_eq!(error.kind(), ConnectorMutationFailureKind::Conflict);
        assert_eq!(
            error.failure_fact().reason(),
            ICEBERG_COMMIT_VALIDATION_CONFLICT
        );
        error.message().to_owned()
    }
}

impl RemovedDataFiles for Fixture {
    fn removed_data_file(&self, path: &str) -> Result<CandidateFile<'_>> {
        Ok(CandidateFile {
            metadata: &self.metadata,
            spec_id: self.metadata.default_partition_spec_id(),
            file: &self.removed[path],
        })
    }
}

impl ArtifactWriter for Fixture {
    fn file_io(&self) -> &FileIO {
        &self.io
    }
    fn allocate(&self, _: ArtifactKind) -> Result<ObjectIdentity> {
        panic!("Validation must not allocate artifacts")
    }
    fn attempt_token(&self) -> AttemptToken {
        AttemptToken::new(
            OperationToken::from_write(ConnectorWriteOperationId::from_bytes([1; 16])),
            0,
        )
    }
    fn check_active(&self) -> Result<()> {
        Ok(())
    }
    fn snapshot_artifacts(
        &self,
        _: &[ObjectIdentity],
        _: &[ObjectIdentity],
    ) -> Result<AttemptArtifacts> {
        panic!("Validation must not freeze artifacts")
    }
    fn record_manifest(
        &self,
        _: &ObjectIdentity,
        _: &[String],
        _: &[String],
        _: &[String],
    ) -> Result<()> {
        panic!("Validation must not record artifacts")
    }
    fn record_manifest_list(&self, _: &ObjectIdentity, _: i64, _: &[String]) -> Result<()> {
        panic!("Validation must not record artifacts")
    }
}

struct Partition(Option<i32>);

impl fmt::Display for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(value) => write!(f, "region = {value}"),
            None => f.write_str("true"),
        }
    }
}

impl MayMatch for Partition {
    fn may_match(&self, file: &CandidateFile<'_>) -> std::result::Result<bool, ConnectorError> {
        Ok(self.0.is_none_or(|value| {
            file.file.partition().fields().first() == Some(&Some(Literal::int(value)))
        }))
    }
}

fn dependencies(
    isolation: IsolationLevel,
    partition: Option<i32>,
    facts: RowDeltaValidationFacts,
    writes_data: bool,
    writes_deletes: bool,
) -> RowDeltaWindowCheck {
    RowDeltaWindowCheck::new(
        isolation,
        Arc::new(Partition(partition)),
        Arc::new(facts),
        writes_data,
        writes_deletes,
    )
}

fn facts(referenced: &[&str], removed: &[&str], dv_targets: &[&str]) -> RowDeltaValidationFacts {
    let paths = |values: &[&str]| values.iter().map(|value| Arc::from(*value)).collect();
    RowDeltaValidationFacts {
        referenced: paths(referenced),
        removed: paths(removed),
        dv_targets: paths(dv_targets),
        ..Default::default()
    }
}

#[tokio::test]
async fn validates_exact_ancestor_window() {
    let mut fixture = Fixture::new(FormatVersion::V2);
    fixture
        .snapshot(11, None, Operation::Append, vec![added(data("/root", 0))])
        .await;
    fixture
        .snapshot(12, Some(11), Operation::Replace, vec![])
        .await;
    fixture
        .snapshot(13, Some(12), Operation::Replace, vec![])
        .await;
    fixture
        .snapshot(
            14,
            Some(11),
            Operation::Append,
            vec![added(data("/branch", 0))],
        )
        .await;
    let check = || {
        dependencies(
            IsolationLevel::Serializable,
            None,
            Default::default(),
            false,
            false,
        )
    };
    assert_eq!(
        fixture.check(Some(13), Some(13), check()).await,
        Verdict::Holds
    );
    assert_eq!(
        fixture.check(Some(11), Some(13), check()).await,
        Verdict::Holds
    );
    let entire = fixture.check(None, Some(13), check()).await;
    assert!(fixture.message(entire, None, Some(13)).starts_with(ADDED));
    let branch = fixture.check(Some(14), Some(13), check()).await;
    assert_eq!(
        fixture.message(branch, Some(14), Some(13)),
        "Snapshot 14 is not an ancestor of 13"
    );
    let expired = fixture.check(Some(99), Some(13), check()).await;
    assert_eq!(
        fixture.message(expired, Some(99), Some(13)),
        "Snapshot 99 is not an ancestor of 13"
    );

    fixture.metadata = fixture
        .metadata
        .clone()
        .into_builder(None)
        .remove_snapshots(&[12])
        .build()
        .unwrap()
        .metadata;
    let gap = fixture.check(Some(11), Some(13), check()).await;
    assert_eq!(
        fixture.message(gap, Some(11), Some(13)),
        "Cannot determine history between starting snapshot 11 and the last known ancestor 13"
    );
}

#[tokio::test]
async fn missing_referenced_data_precedes_serializable_append() {
    let mut fixture = Fixture::new(FormatVersion::V2);
    fixture
        .snapshot(1, None, Operation::Append, vec![added(data("/target", 0))])
        .await;
    fixture.rewrite_manifest(1, ManifestContentType::Data);
    fixture
        .snapshot(
            2,
            Some(1),
            Operation::Delete,
            vec![deleted(data("/target", 0), 1)],
        )
        .await;
    fixture
        .snapshot(
            3,
            Some(2),
            Operation::Append,
            vec![added(data("/concurrent", 0))],
        )
        .await;

    let check = |partition, writes_deletes| {
        dependencies(
            IsolationLevel::Serializable,
            partition,
            facts(&["/target"], &[], &[]),
            false,
            writes_deletes,
        )
    };
    for (scope, writes_deletes, prefix) in [(Some(0), true, MISSING), (Some(0), false, ADDED)] {
        let result = fixture
            .check(Some(1), Some(3), check(scope, writes_deletes))
            .await;
        assert!(
            fixture
                .message(result, Some(1), Some(3))
                .starts_with(prefix)
        );
    }
    assert_eq!(
        fixture.check(Some(1), Some(3), check(Some(1), true)).await,
        Verdict::Holds
    );
}

#[tokio::test]
async fn serializable_checks_inclusive_partition_and_excludes_replace() {
    // Iceberg TestRowDelta.testValidateNoConflicts
    let mut fixture = Fixture::new(FormatVersion::V2);
    fixture
        .snapshot(1, None, Operation::Append, vec![added(data("/base", 0))])
        .await;
    fixture
        .snapshot(
            2,
            Some(1),
            Operation::Replace,
            vec![added(data("/physical", 0))],
        )
        .await;
    fixture
        .snapshot(
            3,
            Some(2),
            Operation::Append,
            vec![added(data("/append", 0))],
        )
        .await;
    let check = |isolation, part| dependencies(isolation, part, Default::default(), false, false);
    assert_eq!(
        fixture
            .check(
                Some(1),
                Some(2),
                check(IsolationLevel::Serializable, Some(0))
            )
            .await,
        Verdict::Holds
    );
    assert_eq!(
        fixture
            .check(Some(1), Some(3), check(IsolationLevel::Snapshot, Some(0)))
            .await,
        Verdict::Holds
    );
    assert_eq!(
        fixture
            .check(
                Some(1),
                Some(3),
                check(IsolationLevel::Serializable, Some(1))
            )
            .await,
        Verdict::Holds
    );
    let conflict = fixture
        .check(
            Some(1),
            Some(3),
            check(IsolationLevel::Serializable, Some(0)),
        )
        .await;
    assert_eq!(
        fixture.message(conflict, Some(1), Some(3)),
        "Found conflicting files that can contain records matching region = 0: [/append]"
    );
}

#[tokio::test]
async fn replacement_delete_precedes_conflicting_delete() {
    // Iceberg TestRowDelta.testValidateFileDeleteAndRowDelete
    let mut fixture = Fixture::new(FormatVersion::V2);
    let target = data("/removed", 0);
    fixture
        .removed
        .insert(target.file_path().into(), target.clone());
    fixture
        .snapshot(
            1,
            None,
            Operation::Append,
            vec![added(target), added(data("/other", 0))],
        )
        .await;
    fixture
        .snapshot(
            2,
            Some(1),
            Operation::Delete,
            vec![added(position("/partition_delete", None, 0))],
        )
        .await;
    fixture
        .snapshot(
            3,
            Some(2),
            Operation::Delete,
            vec![added(position("/other_delete", Some("/other"), 0))],
        )
        .await;
    let check = dependencies(
        IsolationLevel::Snapshot,
        None,
        facts(&[], &["/removed"], &[]),
        true,
        true,
    );
    let result = fixture.check(Some(1), Some(3), check.clone()).await;
    assert_eq!(
        fixture.message(result, Some(1), Some(3)),
        format!("{REPLACED}: /removed")
    );
    let result = fixture.check(Some(2), Some(3), check).await;
    assert_eq!(
        fixture.message(result, Some(2), Some(3)),
        format!("{DELETES} to records matching true: [/other_delete]")
    );
}

#[tokio::test]
async fn delete_files_require_a_new_data_sequence_and_expected_operations() {
    let mut fixture = Fixture::new(FormatVersion::V2);
    fixture
        .snapshot(1, None, Operation::Append, vec![added(data("/target", 0))])
        .await;
    fixture
        .snapshot(
            2,
            Some(1),
            Operation::Delete,
            vec![ManifestEntry {
                file: position("/old_age", Some("/target"), 0),
                status: Status::Added(1),
            }],
        )
        .await;
    fixture
        .snapshot(
            3,
            Some(2),
            Operation::Replace,
            vec![added(position("/replace", Some("/target"), 0))],
        )
        .await;
    fixture.rewrite_manifest(3, ManifestContentType::Deletes);
    fixture
        .snapshot(
            4,
            Some(3),
            Operation::Overwrite,
            vec![existing(position("/replace", Some("/target"), 0), 3, 3)],
        )
        .await;
    let check = |writes_data| {
        dependencies(
            IsolationLevel::Snapshot,
            None,
            facts(&["/target"], &[], &[]),
            writes_data,
            true,
        )
    };
    assert_eq!(
        fixture.check(Some(1), Some(3), check(true)).await,
        Verdict::Holds
    );
    // An EXISTING entry rewritten into an eligible new manifest still has
    // its resolved data age in the window. Pure deletes skip this check.
    assert_eq!(
        fixture.check(Some(1), Some(4), check(false)).await,
        Verdict::Holds
    );
    let result = fixture.check(Some(1), Some(4), check(true)).await;
    assert!(
        fixture
            .message(result, Some(1), Some(4))
            .starts_with(DELETES)
    );
}

#[tokio::test]
async fn own_new_delete_cannot_target_a_whole_file_removed_by_the_same_operation() {
    let fixture = Fixture::new(FormatVersion::V2);
    let check = dependencies(
        IsolationLevel::Snapshot,
        None,
        facts(&["/target"], &["/target"], &[]),
        false,
        true,
    );
    let result = fixture.check(None, None, check).await;
    assert_eq!(
        fixture.message(result, None, None),
        format!("{REFERENCED} [/target] that are referenced by new delete files")
    );
}

#[tokio::test]
async fn successive_dvs_conflict_at_the_expected_check() {
    // Iceberg TestRowDelta concurrent successive DVs and partition filters.
    let mut fixture = Fixture::new(FormatVersion::V3);
    fixture
        .snapshot(
            1,
            None,
            Operation::Append,
            vec![added(data("/a", 0)), added(data("/b", 1))],
        )
        .await;
    fixture
        .snapshot(
            2,
            Some(1),
            Operation::Delete,
            vec![added(dv("/first", "/a", 0))],
        )
        .await;
    fixture.rewrite_manifest(2, ManifestContentType::Deletes);
    fixture
        .snapshot(
            3,
            Some(2),
            Operation::Delete,
            vec![
                deleted(dv("/first", "/a", 0), 2),
                added(dv("/second", "/a", 0)),
            ],
        )
        .await;
    let check = |part, writes_data, target| {
        dependencies(
            IsolationLevel::Snapshot,
            part,
            facts(&[target], &[], &[target]),
            writes_data,
            true,
        )
    };
    let data_write = fixture
        .check(Some(1), Some(3), check(None, true, "/b"))
        .await;
    assert_eq!(
        fixture.message(data_write, Some(1), Some(3)),
        format!("{DELETES} to records matching true: [/second]")
    );
    let same_target = fixture
        .check(Some(1), Some(3), check(Some(0), false, "/a"))
        .await;
    assert!(
        fixture
            .message(same_target, Some(1), Some(3))
            .starts_with(&format!("{DV} for /a"))
    );
    assert_eq!(
        fixture
            .check(Some(1), Some(3), check(Some(1), false, "/a"))
            .await,
        Verdict::Holds
    );
    assert_eq!(
        fixture
            .check(Some(1), Some(3), check(Some(0), false, "/b"))
            .await,
        Verdict::Holds
    );
    fixture
        .snapshot(
            4,
            Some(3),
            Operation::Replace,
            vec![added(dv("/replace", "/b", 1))],
        )
        .await;
    let replaced_dv = fixture
        .check(Some(3), Some(4), check(Some(1), false, "/b"))
        .await;
    assert!(
        fixture
            .message(replaced_dv, Some(3), Some(4))
            .starts_with(&format!("{DV} for /b"))
    );
}

#[test]
fn equality_bounds_and_nulls_can_rule_out_replacement_conflict() {
    let data = file("/data", 0)
        .content(DataContentType::Data)
        .file_format(DataFileFormat::Parquet)
        .lower_bounds([(1, Datum::int(1))].into_iter().collect())
        .upper_bounds([(1, Datum::int(5))].into_iter().collect())
        .null_value_counts([(1, 0)].into_iter().collect())
        .build()
        .unwrap();
    let delete = file("/eq", 0)
        .content(DataContentType::EqualityDeletes)
        .file_format(DataFileFormat::Parquet)
        .lower_bounds([(1, Datum::int(11))].into_iter().collect())
        .upper_bounds([(1, Datum::int(15))].into_iter().collect())
        .equality_ids(Some(vec![1]))
        .build()
        .unwrap();
    let fixture = Fixture::new(FormatVersion::V2);
    assert!(!equality_can_match(&delete, &data, &fixture.metadata));
}
