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

//! Frozen whole-table physical rewrite and read-only compaction group facts.
//!
//! The preparer preserves source age and logical identity, while the actual
//! manifest-list writer assigns only historically unassigned row ranges.

use std::collections::{BTreeMap, HashMap};

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, Operation,
};
use crate::iceberg::table::Table;
use async_trait::async_trait;

struct LiveManifestEntry {
    data_file: DataFile,
    partition_spec_id: i32,
    sequence_number: i64,
}

#[derive(Default)]
struct LiveFiles {
    data_files: Vec<LiveManifestEntry>,
    delete_files: Vec<LiveManifestEntry>,
}

/// Provider-private view of how many data files the rewrite action would put
/// into a single group. Only the scalar is ever published; the grouping rule
/// stays inside this module.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LiveDataFileCompactionStats {
    pub max_compactable_data_files: i64,
}

/// Enumerates the table's live manifests and reports the largest group this
/// rewrite action would form. `preserve_row_lineage` decides whether the data
/// sequence number participates in grouping, because preserve-mode rewrites
/// must not fold rows with different sequence numbers into one output file.
pub async fn current_live_data_file_compaction_stats(
    table: &Table,
    file_io: &FileIO,
    preserve_row_lineage: bool,
) -> Result<LiveDataFileCompactionStats, String> {
    let live = enumerate_live_files(table, file_io).await?;
    let mut groups: HashMap<String, i64> = HashMap::new();
    for entry in &live.data_files {
        let sequence = if preserve_row_lineage {
            Some(entry.sequence_number)
        } else {
            None
        };
        let key = format!(
            "spec={};partition={:?};sequence={:?}",
            entry.partition_spec_id,
            entry.data_file.partition(),
            sequence
        );
        let count = groups.entry(key).or_insert(0);
        *count = count
            .checked_add(1)
            .ok_or_else(|| "live data file compaction group count overflow".to_string())?;
    }
    Ok(LiveDataFileCompactionStats {
        max_compactable_data_files: groups.into_values().max().unwrap_or(0),
    })
}

async fn enumerate_live_files(table: &Table, file_io: &FileIO) -> Result<LiveFiles, String> {
    let mut out = LiveFiles::default();
    let m = table.metadata();
    let snapshot = match m.current_snapshot() {
        Some(s) => s,
        None => return Ok(out),
    };
    let list = snapshot
        .load_manifest_list(file_io, m)
        .await
        .map_err(|e| format!("load manifest list failed: {e}"))?;

    for mf in list.entries() {
        let manifest = mf
            .load_manifest(file_io)
            .await
            .map_err(|e| format!("load manifest {} failed: {e}", mf.manifest_path))?;
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let live = LiveManifestEntry {
                data_file: entry.data_file().clone(),
                partition_spec_id: mf.partition_spec_id,
                sequence_number: entry.sequence_number().unwrap_or(mf.sequence_number),
            };
            match mf.content {
                ManifestContentType::Data => out.data_files.push(live),
                ManifestContentType::Deletes => out.delete_files.push(live),
            }
        }
    }
    Ok(out)
}

/// Whole-table physical replacement under the operation's frozen dependency.
/// It never expands the removed set to include a concurrent writer's files.
pub(crate) struct RewriteDataFilesPreparer;

#[async_trait]
impl super::staging::Preparer for RewriteDataFilesPreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        let parent = view
            .metadata()
            .snapshot_for_ref(view.target_ref())
            .map(|s| s.snapshot_id());
        let mut inputs =
            super::dependency::ValidationInputs::new(view.metadata(), parent, view.artifacts());
        let live = inputs.live_set().await?;
        let removed = intent
            .changes()
            .removed
            .iter()
            .map(|f| f.identity().clone())
            .collect::<std::collections::BTreeSet<_>>();
        if removed != live.keys().cloned().collect() {
            return Err(rewrite_invalid(
                "Whole-data rewrite frozen set does not equal the target-ref live set",
            ));
        }
        prepare_physical_rewrite(view, intent, live, &removed, true).await
    }
}

/// Shared by whole-data and exact selected-delete replacement. All inputs are
/// frozen before this attempt; live entries retain their actual source facts.
pub(crate) async fn prepare_physical_rewrite(
    view: &super::staging::StagedView<'_>,
    intent: &super::model::OperationIntent,
    live: &super::dependency::LiveSet,
    removed: &std::collections::BTreeSet<super::model::EntryIdentity>,
    data_rewrite: bool,
) -> crate::iceberg::Result<super::staging::PreparedChange> {
    use super::model::{EntryIdentity, SeqField};
    use super::staging::{ManifestEntryWrite, write_manifest};
    let metadata = view.metadata();
    if metadata.format_version() == FormatVersion::V1 {
        return Err(rewrite_invalid(
            "Physical rewrite does not support V1 tables",
        ));
    }
    if live.is_empty() && intent.changes().added.is_empty() {
        return Ok(super::staging::PreparedChange::default());
    }
    let source = intent
        .start()
        .ok_or_else(|| rewrite_invalid("Physical rewrite has no frozen starting snapshot"))?;
    for frozen in &intent.changes().removed {
        let current = live
            .get(frozen.identity())
            .ok_or_else(|| rewrite_invalid("Frozen rewrite entry is no longer live"))?;
        if current.frozen != *frozen {
            return Err(rewrite_invalid(
                "Frozen rewrite entry facts differ from the live entry",
            ));
        }
    }
    let mut added_groups: BTreeMap<(i32, bool), Vec<super::model::AddedContent>> = BTreeMap::new();
    for added in &intent.changes().added {
        if added.data_sequence() != SeqField::Explicit(source.sequence_number) {
            return Err(rewrite_invalid(
                "Physical rewrite output must carry the frozen starting data sequence",
            ));
        }
        let spec_id = if data_rewrite {
            if added.file().content_type() != DataContentType::Data {
                return Err(rewrite_invalid(
                    "Whole-data rewrite output must contain only data files",
                ));
            }
            added.partition_spec_id()
        } else {
            let EntryIdentity::DeletionVector {
                referenced_data_file,
                ..
            } = EntryIdentity::try_from(added.file())?
            else {
                return Err(rewrite_invalid(
                    "Position-delete rewrite output must contain deletion vectors",
                ));
            };
            let data = live
                .get(&EntryIdentity::DataFile {
                    path: referenced_data_file,
                })
                .ok_or_else(|| {
                    rewrite_invalid(
                        "Replacement DV references a data file absent from the target ref",
                    )
                })?;
            if data.file.partition() != added.file().partition() {
                return Err(rewrite_invalid(
                    "Replacement DV partition differs from its exact referenced data file",
                ));
            }
            if added.partition_spec_id() != data.frozen.facts().partition_spec_id {
                return Err(rewrite_invalid(
                    "Replacement DV spec differs from its exact referenced data file",
                ));
            }
            added.partition_spec_id()
        };
        added_groups
            .entry((
                spec_id,
                data_rewrite && added.file().first_row_id().is_some(),
            ))
            .or_default()
            .push(added.clone());
    }
    let snapshot_id = super::staging::new_snapshot_id(metadata);
    let entries = live
        .iter()
        .map(|(identity, entry)| (entry.clone(), removed.contains(identity)))
        .collect();
    let mut manifests =
        super::overwrite::write_live_entry_groups(view, snapshot_id, entries).await?;
    for ((spec_id, assigned), added) in added_groups {
        let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
            rewrite_invalid(format!("Rewrite partition spec {spec_id} is absent"))
        })?;
        let assigned_min =
            (metadata.format_version() == FormatVersion::V3 && data_rewrite && assigned)
                .then(|| {
                    added
                        .iter()
                        .filter_map(|entry| entry.file().first_row_id())
                        .min()
                })
                .flatten();
        let mut manifest = write_manifest(
            view.artifacts(),
            metadata.format_version(),
            snapshot_id,
            metadata.current_schema().clone(),
            spec.as_ref().clone(),
            if data_rewrite {
                ManifestContentType::Data
            } else {
                ManifestContentType::Deletes
            },
            added.into_iter().map(ManifestEntryWrite::Added),
        )
        .await?;
        if let Some(first) = assigned_min {
            manifest.first_row_id = Some(
                u64::try_from(first)
                    .map_err(|_| rewrite_invalid("Assigned rewrite row ID must be nonnegative"))?,
            );
        }
        manifests.push(manifest);
    }
    let deleted = live
        .iter()
        .filter(|(identity, _)| removed.contains(*identity))
        .map(|(_, entry)| entry.clone())
        .collect::<Vec<_>>();
    let mut summary = super::overwrite::snapshot_file_summary(&intent.changes().added, &deleted)?;
    if data_rewrite {
        summary.insert("total-records".into(), summary["added-records"].clone());
    }
    if !data_rewrite {
        summary.insert("rewritten-delete-files".into(), removed.len().to_string());
    }
    super::overwrite::prepare_snapshot_change(
        view,
        intent,
        snapshot_id,
        Operation::Replace,
        manifests,
        summary,
        false,
    )
    .await
}

fn rewrite_invalid(message: impl Into<String>) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, message)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use crate::commit::helpers::now_ms;
    use crate::iceberg::TableIdent;
    use crate::iceberg::spec::{
        DataFileBuilder, DataFileFormat, Literal, ManifestListWriter, ManifestWriterBuilder,
        NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Struct, Summary, TableMetadataBuilder,
        Transform, Type,
    };
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    use super::*;

    fn local_test_binding() -> crate::access_binding::IcebergReadBinding {
        let runtime = tokio::runtime::Handle::current();
        crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        )
    }

    const SNAPSHOT_ID: i64 = 100;
    const SNAPSHOT_SEQUENCE_NUMBER: i64 = 12;

    /// One synthetic live data manifest: `data_files` files that all share a
    /// partition spec id, a partition value and a data sequence number.
    struct ManifestPlan {
        partition_spec_id: i32,
        partition_value: i32,
        data_files: usize,
        sequence_number: i64,
    }

    #[tokio::test]
    async fn compaction_stats_count_every_data_file_in_one_partition() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table = table_with_live_data_files(
            dir.path(),
            &[ManifestPlan {
                partition_spec_id: 1,
                partition_value: 1,
                data_files: 3,
                sequence_number: 10,
            }],
        )
        .await;

        assert_eq!(max_compactable_data_files(&table, false).await, 3);
        assert_eq!(max_compactable_data_files(&table, true).await, 3);
    }

    #[tokio::test]
    async fn compaction_stats_report_largest_partition_not_total() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table = table_with_live_data_files(
            dir.path(),
            &[
                ManifestPlan {
                    partition_spec_id: 1,
                    partition_value: 1,
                    data_files: 3,
                    sequence_number: 10,
                },
                ManifestPlan {
                    partition_spec_id: 1,
                    partition_value: 2,
                    data_files: 2,
                    sequence_number: 10,
                },
            ],
        )
        .await;

        // Five live data files in total, but no rewrite group can span two
        // partitions, so the answer is the largest partition.
        assert_eq!(max_compactable_data_files(&table, false).await, 3);
    }

    #[tokio::test]
    async fn compaction_stats_keep_partition_spec_ids_separate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table = table_with_live_data_files(
            dir.path(),
            &[
                ManifestPlan {
                    partition_spec_id: 1,
                    partition_value: 1,
                    data_files: 3,
                    sequence_number: 10,
                },
                ManifestPlan {
                    partition_spec_id: 2,
                    partition_value: 1,
                    data_files: 4,
                    sequence_number: 10,
                },
            ],
        )
        .await;

        // Same partition value under two coexisting specs stays two groups.
        assert_eq!(max_compactable_data_files(&table, false).await, 4);
    }

    #[tokio::test]
    async fn compaction_stats_split_by_sequence_only_when_preserving_row_lineage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table = table_with_live_data_files(
            dir.path(),
            &[
                ManifestPlan {
                    partition_spec_id: 1,
                    partition_value: 1,
                    data_files: 3,
                    sequence_number: 10,
                },
                ManifestPlan {
                    partition_spec_id: 1,
                    partition_value: 1,
                    data_files: 2,
                    sequence_number: 11,
                },
                ManifestPlan {
                    partition_spec_id: 2,
                    partition_value: 1,
                    data_files: 4,
                    sequence_number: 10,
                },
            ],
        )
        .await;

        // Without row lineage the two sequence numbers fold into one group of 5.
        assert_eq!(max_compactable_data_files(&table, false).await, 5);
        // Preserving row lineage splits them into 3 and 2, so the largest
        // remaining group is the four-file group of the other spec.
        assert_eq!(max_compactable_data_files(&table, true).await, 4);
    }

    #[tokio::test]
    async fn compaction_stats_are_zero_without_a_current_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table = table_with_live_data_files(dir.path(), &[]).await;

        assert_eq!(max_compactable_data_files(&table, false).await, 0);
        assert_eq!(max_compactable_data_files(&table, true).await, 0);
    }

    async fn max_compactable_data_files(table: &Table, preserve_row_lineage: bool) -> i64 {
        current_live_data_file_compaction_stats(table, table.file_io(), preserve_row_lineage)
            .await
            .expect("compaction stats")
            .max_compactable_data_files
    }

    fn test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "p", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("schema")
    }

    fn test_partition_spec(schema: &Schema, spec_id: i32) -> PartitionSpec {
        PartitionSpec::builder(schema.clone())
            .with_spec_id(spec_id)
            .add_partition_field("p", "p", Transform::Identity)
            .expect("partition field")
            .build()
            .expect("partition spec")
    }

    fn test_data_file(plan: &ManifestPlan, ordinal: usize) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!(
                "data/spec-{}-p-{}-seq-{}-{ordinal}.parquet",
                plan.partition_spec_id, plan.partition_value, plan.sequence_number
            ))
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::from_iter([Some(Literal::Primitive(
                PrimitiveLiteral::Int(plan.partition_value),
            ))]))
            .partition_spec_id(plan.partition_spec_id)
            .record_count(1)
            .file_size_in_bytes(1024)
            .build()
            .expect("data file")
    }

    /// Writes real avro manifests plus a manifest list under `dir` and returns
    /// a table whose current snapshot points at them. An empty `plans` list
    /// yields a table without any snapshot.
    async fn table_with_live_data_files(dir: &Path, plans: &[ManifestPlan]) -> Table {
        let location = format!("file://{}", dir.display());
        std::fs::create_dir_all(dir.join("metadata")).expect("metadata dir");
        let schema = test_schema();
        let file_io = crate::fs_io::build_file_io_for_location(&location, local_test_binding());
        let builder = TableMetadataBuilder::new(
            schema.clone(),
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            location.clone(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("table metadata builder");
        let metadata = if plans.is_empty() {
            builder.build().expect("table metadata").metadata
        } else {
            let mut manifests = Vec::new();
            for (index, plan) in plans.iter().enumerate() {
                let path = format!("{location}/metadata/live-{index}.avro");
                let output = file_io.new_output(&path).expect("manifest output");
                let mut writer = ManifestWriterBuilder::new(
                    output,
                    Some(SNAPSHOT_ID),
                    None,
                    Arc::new(schema.clone()),
                    test_partition_spec(&schema, plan.partition_spec_id),
                )
                .build_v2_data();
                for ordinal in 0..plan.data_files {
                    writer
                        .add_file(test_data_file(plan, ordinal), plan.sequence_number)
                        .expect("add data file");
                }
                let mut manifest = writer.write_manifest_file().await.expect("write manifest");
                manifest.sequence_number = plan.sequence_number;
                manifest.min_sequence_number = plan.sequence_number;
                manifests.push(manifest);
            }
            let manifest_list_path = format!("{location}/metadata/snap-{SNAPSHOT_ID}.avro");
            let output = file_io
                .new_output(&manifest_list_path)
                .expect("manifest list output");
            let mut list_writer =
                ManifestListWriter::v2(output, SNAPSHOT_ID, None, SNAPSHOT_SEQUENCE_NUMBER);
            list_writer
                .add_manifests(manifests.into_iter())
                .expect("add manifests");
            list_writer.close().await.expect("close manifest list");
            let snapshot = Snapshot::builder()
                .with_snapshot_id(SNAPSHOT_ID)
                .with_sequence_number(SNAPSHOT_SEQUENCE_NUMBER)
                .with_timestamp_ms(now_ms())
                .with_manifest_list(manifest_list_path)
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .with_schema_id(0)
                .build();
            builder
                .add_snapshot(snapshot)
                .expect("add snapshot")
                .set_ref(
                    "main",
                    SnapshotReference::new(
                        SNAPSHOT_ID,
                        SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    ),
                )
                .expect("set main ref")
                .build()
                .expect("table metadata")
                .metadata
        };
        Table::builder()
            .identifier(TableIdent::from_strs(["db", "t"]).expect("table ident"))
            .file_io(file_io)
            .metadata(metadata)
            .build()
            .expect("table")
    }
}
