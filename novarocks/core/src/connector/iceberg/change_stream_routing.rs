// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Generic routing for Iceberg change-stream writer reports.
//!
//! The distributed writer wire format stays identical to ordinary Iceberg
//! sinks. This module interprets the planned writer-fragment-to-branch mapping
//! and injects converted writer reports into the commit collector channels that
//! `RowDeltaDvFromFiles` already understands.

use std::collections::BTreeMap;

use iceberg::spec::TableMetadata;

use crate::connector::iceberg::commit::{IcebergCommitCollector, WrittenFile};
use crate::connector::iceberg::report::IcebergWriterReport;
use crate::sql::common::ChangeStreamBranchKind;
use crate::sql::planner::distributed::FragmentId;
use crate::sql::planner::distributed::write::change_stream::IcebergChangeStreamWriteTopology;

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriterCommitPlan {
    branch_by_writer_fragment: BTreeMap<i32, ChangeStreamBranchKind>,
}

impl ChangeStreamWriterCommitPlan {
    pub(crate) fn new(branch_by_writer_fragment: BTreeMap<i32, ChangeStreamBranchKind>) -> Self {
        Self {
            branch_by_writer_fragment,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.branch_by_writer_fragment.is_empty()
    }

    pub(crate) fn from_topology(
        topology: &IcebergChangeStreamWriteTopology,
    ) -> Result<Self, String> {
        let mut branch_by_writer_fragment = BTreeMap::new();
        for branch in &topology.writer_branches {
            insert_writer_fragment_branch(
                &mut branch_by_writer_fragment,
                branch.writer_fragment_id,
                branch.branch_id,
                branch.branch_kind,
            )?;
        }
        Ok(Self::new(branch_by_writer_fragment))
    }

    #[cfg(test)]
    pub(crate) fn writer_fragment_ids_for_topology(
        &self,
        topology: &IcebergChangeStreamWriteTopology,
    ) -> Vec<Option<FragmentId>> {
        let by_kind = self
            .branch_by_writer_fragment
            .iter()
            .filter_map(|(fragment_id, branch_kind)| {
                u32::try_from(*fragment_id)
                    .ok()
                    .map(|fragment_id| (*branch_kind, fragment_id))
            })
            .collect::<BTreeMap<_, _>>();
        topology
            .writer_branches
            .iter()
            .map(|branch| by_kind.get(&branch.branch_kind).copied())
            .collect()
    }

    fn branch_for_fragment_id(
        &self,
        fragment_id: FragmentId,
    ) -> Result<ChangeStreamBranchKind, String> {
        let fragment_id = i32::try_from(fragment_id)
            .map_err(|_| format!("writer fragment {fragment_id} does not fit in commit plan"))?;
        self.branch_by_writer_fragment
            .get(&fragment_id)
            .copied()
            .ok_or_else(|| {
                format!(
                    "writer fragment {fragment_id} is not declared in change-stream commit plan"
                )
            })
    }
}

fn insert_writer_fragment_branch(
    branch_by_writer_fragment: &mut BTreeMap<i32, ChangeStreamBranchKind>,
    writer_fragment_id: FragmentId,
    branch_id: i32,
    branch_kind: ChangeStreamBranchKind,
) -> Result<(), String> {
    let writer_fragment_id = i32::try_from(writer_fragment_id).map_err(|_| {
        format!(
            "writer fragment id {writer_fragment_id} for change-stream branch {branch_id} \
             does not fit in commit-plan fragment id field"
        )
    })?;
    if branch_by_writer_fragment
        .insert(writer_fragment_id, branch_kind)
        .is_some()
    {
        return Err(format!(
            "duplicate writer fragment id {writer_fragment_id} in change-stream commit plan"
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ChangeStreamRoutedWriterFiles {
    pub(crate) reuse_or_dv: Vec<WrittenFile>,
    pub(crate) fresh: Vec<WrittenFile>,
}

/// One logical generic writer's provider-private staged reports.  The
/// fragment ID comes from the immutable writer identity, rather than from a
/// legacy query-level commit carrier.
#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriterReports {
    pub(crate) fragment_id: i32,
    pub(crate) reports: Vec<IcebergWriterReport>,
}

impl ChangeStreamRoutedWriterFiles {
    fn converted_files(&self) -> Vec<WrittenFile> {
        self.reuse_or_dv
            .iter()
            .chain(self.fresh.iter())
            .cloned()
            .collect()
    }

    fn converted_files_with(&self, current: &[WrittenFile]) -> Vec<WrittenFile> {
        self.reuse_or_dv
            .iter()
            .chain(self.fresh.iter())
            .chain(current.iter())
            .cloned()
            .collect()
    }

    pub(crate) fn inject(self, collector: &IcebergCommitCollector) {
        if matches!(
            collector.op_kind,
            crate::connector::iceberg::commit::CommitOpKind::FastAppend
                | crate::connector::iceberg::commit::CommitOpKind::Overwrite
        ) {
            let mut files = self.reuse_or_dv;
            files.extend(self.fresh);
            collector.inject_written_files(files);
        } else {
            collector.inject_written_files(self.reuse_or_dv);
            collector.inject_appended_files(self.fresh);
        }
    }
}

#[derive(Debug)]
pub(crate) struct ChangeStreamWriterRoutingError {
    message: String,
    converted_files: Vec<WrittenFile>,
}

impl ChangeStreamWriterRoutingError {
    fn new(message: String, converted_files: Vec<WrittenFile>) -> Self {
        Self {
            message,
            converted_files,
        }
    }

    pub(crate) fn into_parts(self) -> (String, Vec<WrittenFile>) {
        (self.message, self.converted_files)
    }
}

/// Route generic provider-private reports according to the writer identity
/// frozen in the change-stream manifest.  This is the carrier-neutral path
/// used by the connector control adapter; no native Iceberg commit DTO is
/// decoded here.
pub(crate) fn route_change_stream_staged_reports(
    collector: &IcebergCommitCollector,
    reports: impl IntoIterator<Item = ChangeStreamWriterReports>,
    plan: &ChangeStreamWriterCommitPlan,
) -> Result<ChangeStreamRoutedWriterFiles, ChangeStreamWriterRoutingError> {
    let mut routed = ChangeStreamRoutedWriterFiles::default();

    for writer in reports {
        let mut writer_files = Vec::with_capacity(writer.reports.len());
        for report in writer.reports {
            let file = collector.convert_writer_report(report).map_err(|message| {
                ChangeStreamWriterRoutingError::new(
                    message,
                    routed.converted_files_with(&writer_files),
                )
            })?;
            writer_files.push(file);
        }
        let branch = plan
            .branch_for_fragment_id(u32::try_from(writer.fragment_id).map_err(|_| {
                ChangeStreamWriterRoutingError::new(
                    format!(
                        "generic change-stream writer fragment {} is negative",
                        writer.fragment_id
                    ),
                    routed.converted_files_with(&writer_files),
                )
            })?)
            .map_err(|message| {
                ChangeStreamWriterRoutingError::new(
                    message,
                    routed.converted_files_with(&writer_files),
                )
            })?;
        match branch {
            ChangeStreamBranchKind::FreshData => routed.fresh.extend(writer_files),
            ChangeStreamBranchKind::DeleteDv | ChangeStreamBranchKind::ReuseData => {
                routed.reuse_or_dv.extend(writer_files);
            }
        }
    }

    Ok(routed)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use iceberg::TableCreation;
    use iceberg::spec::{
        DataContentType, FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, Struct,
        TableMetadataBuilder, Type,
    };
    use iceberg::{NamespaceIdent, TableIdent};

    use crate::common::types::UniqueId;
    use crate::connector::iceberg::commit::CommitOpKind;
    use crate::connector::iceberg::delete_file::IcebergFileContent;
    use crate::connector::iceberg::report::{
        IcebergPartitionReport, IcebergWriterReport, IcebergWrittenFileReport,
    };

    fn test_unpartitioned_metadata() -> TableMetadata {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        let creation = TableCreation::builder()
            .name("t".to_string())
            .location("file:///warehouse/db/t".to_string())
            .schema(schema)
            .partition_spec(PartitionSpec::unpartition_spec())
            .format_version(FormatVersion::V3)
            .build();
        TableMetadataBuilder::from_table_creation(creation)
            .expect("table metadata builder")
            .build()
            .expect("table metadata")
            .metadata
    }

    fn test_collector(metadata: &TableMetadata, op_kind: CommitOpKind) -> IcebergCommitCollector {
        IcebergCommitCollector::new(
            op_kind,
            TableIdent::new(NamespaceIdent::new("db".to_string()), "t".to_string()),
            metadata.current_snapshot().map(|s| s.snapshot_id()),
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            "file:///warehouse/db/t/staging".to_string(),
            UniqueId::new(0, 0),
        )
        .with_table_metadata(metadata.clone())
    }

    fn writer_report_for_data_file(metadata: &TableMetadata, path: &str) -> IcebergWriterReport {
        IcebergWriterReport {
            file: IcebergWrittenFileReport {
                path: path.to_string(),
                format: "parquet".to_string(),
                content: IcebergFileContent::Data,
                record_count: 2,
                file_size_in_bytes: 128,
                partition: IcebergPartitionReport {
                    partition_path: String::new(),
                    null_fingerprint: String::new(),
                    partition_spec_id: metadata.default_partition_spec_id(),
                    partition_values: Struct::empty(),
                },
                split_offsets: Some(vec![4]),
                column_stats: None,
                referenced_data_file: None,
                first_row_id: None,
                equality_ids: None,
                key_metadata: None,
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            },
            is_overwrite: None,
            is_rewrite: None,
        }
    }

    #[test]
    fn change_stream_commit_routes_provider_reports_without_legacy_carrier() {
        let metadata = test_unpartitioned_metadata();
        let collector = test_collector(&metadata, CommitOpKind::RowDeltaDvFromFiles);
        let plan = ChangeStreamWriterCommitPlan::new(BTreeMap::from([
            (10, ChangeStreamBranchKind::ReuseData),
            (11, ChangeStreamBranchKind::FreshData),
        ]));
        route_change_stream_staged_reports(
            &collector,
            vec![
                ChangeStreamWriterReports {
                    fragment_id: 10,
                    reports: vec![writer_report_for_data_file(&metadata, "reuse.parquet")],
                },
                ChangeStreamWriterReports {
                    fragment_id: 11,
                    reports: vec![writer_report_for_data_file(&metadata, "fresh.parquet")],
                },
            ],
            &plan,
        )
        .expect("route provider reports")
        .inject(&collector);

        assert_eq!(
            collector
                .take_written_files()
                .expect("written channel")
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec!["reuse.parquet"]
        );
        assert_eq!(
            collector
                .take_appended_files()
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec!["fresh.parquet"]
        );
    }

    #[test]
    fn change_stream_commit_plan_uses_topology_writer_fragments() {
        let topology = topology_for_test(vec![(0, ChangeStreamBranchKind::ReuseData, 7)]);

        let plan = ChangeStreamWriterCommitPlan::from_topology(&topology)
            .expect("topology writer mapping");

        assert_eq!(
            plan.writer_fragment_ids_for_topology(&topology),
            vec![Some(7)]
        );
    }

    #[test]
    fn change_stream_commit_plan_rejects_duplicate_writer_fragment() {
        let topology = topology_for_test(vec![
            (0, ChangeStreamBranchKind::DeleteDv, 7),
            (1, ChangeStreamBranchKind::ReuseData, 7),
        ]);

        let err = ChangeStreamWriterCommitPlan::from_topology(&topology)
            .expect_err("duplicate writer fragment");

        assert!(err.contains("duplicate writer fragment id 7"), "{err}");
    }

    fn topology_for_test(
        branches: Vec<(i32, ChangeStreamBranchKind, FragmentId)>,
    ) -> IcebergChangeStreamWriteTopology {
        IcebergChangeStreamWriteTopology {
            writer_branches: branches
                .into_iter()
                .map(|(branch_id, branch_kind, writer_fragment_id)| {
                    crate::sql::planner::distributed::write::change_stream::IcebergChangeStreamWriterBranch {
                        branch_id,
                        branch_kind,
                        writer_fragment_id,
                        sink_spec: crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec(),
                    }
                })
                .collect(),
        }
    }
}
