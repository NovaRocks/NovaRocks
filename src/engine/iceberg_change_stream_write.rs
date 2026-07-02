// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Engine-owned commit routing for planned Iceberg change-stream writes.
//!
//! The distributed writer wire format stays identical to ordinary Iceberg
//! sinks. This module interprets the planned writer-fragment-to-branch mapping
//! and injects converted writer reports into the commit collector channels that
//! `RowDeltaDvFromFiles` already understands.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use iceberg::spec::TableMetadata;

use crate::connector::iceberg::commit::{
    CleanupAttempt, CommitOutcome, CommitServiceError, IcebergCommitCollector, WrittenFile,
};
use crate::engine::StandaloneState;
use crate::engine::query_options::StandaloneQueryOptions;
use crate::engine::write_transaction::{
    IcebergWriteCommitExecutor, IcebergWriteTransactionExecutor, IcebergWriteTransactionSpec,
};
use crate::runtime::coordinator::CoordinatedQueryResult;
use crate::runtime::write_coordinator::WriteCommitInput;
use crate::sql::codegen::FragmentId;
use crate::sql::common::ChangeStreamBranchKind;
use crate::sql::optimizer::OptimizerPhysicalNode;
use crate::sql::planner::{ChangeStreamWriteDagSpec, IcebergChangeStreamWriteTopology};

pub(crate) fn writer_fragment_id_from_finst_lo(finst_lo: i64) -> i32 {
    (finst_lo >> 16) as i32
}

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

    fn branch_for_finst_lo(&self, finst_lo: i64) -> Result<ChangeStreamBranchKind, String> {
        let fragment_id = writer_fragment_id_from_finst_lo(finst_lo);
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

pub(crate) fn route_change_stream_writer_reports(
    collector: &IcebergCommitCollector,
    table_metadata: &TableMetadata,
    write_commit: &WriteCommitInput,
    plan: &ChangeStreamWriterCommitPlan,
) -> Result<ChangeStreamRoutedWriterFiles, ChangeStreamWriterRoutingError> {
    let mut routed = ChangeStreamRoutedWriterFiles::default();

    for writer in &write_commit.writers {
        let reports = crate::runtime::sink_commit_wire::sink_commit_infos_to_writer_reports(
            writer.sink_commit_infos.clone(),
            table_metadata,
        )
        .map_err(|message| {
            ChangeStreamWriterRoutingError::new(message, routed.converted_files())
        })?;
        let mut writer_files = Vec::with_capacity(reports.len());
        for report in reports {
            let file = collector.convert_writer_report(report).map_err(|message| {
                ChangeStreamWriterRoutingError::new(
                    message,
                    routed.converted_files_with(&writer_files),
                )
            })?;
            writer_files.push(file);
        }
        let branch = plan
            .branch_for_finst_lo(writer.writer_key.fragment_instance_id.lo)
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

pub(crate) struct ChangeStreamPhysicalBuildInput {
    pub(crate) state: Arc<StandaloneState>,
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) physical_plan: OptimizerPhysicalNode,
    pub(crate) dag: ChangeStreamWriteDagSpec,
    pub(crate) query_opts: Option<StandaloneQueryOptions>,
    pub(crate) mv_refresh_ctx:
        Option<Arc<crate::engine::mv::refresh_context::IcebergMvRefreshContext>>,
}

pub(crate) struct ChangeStreamWriteTransactionExecutor {
    build_input: Mutex<Option<ChangeStreamPhysicalBuildInput>>,
    commit_executor: IcebergWriteCommitExecutor,
    commit_plan: Mutex<Option<ChangeStreamWriterCommitPlan>>,
}

impl ChangeStreamWriteTransactionExecutor {
    pub(crate) fn new(
        build_input: ChangeStreamPhysicalBuildInput,
        commit_executor: IcebergWriteCommitExecutor,
    ) -> Self {
        Self {
            build_input: Mutex::new(Some(build_input)),
            commit_executor,
            commit_plan: Mutex::new(None),
        }
    }
}

impl IcebergWriteTransactionExecutor for ChangeStreamWriteTransactionExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        let mut build_input = self
            .build_input
            .lock()
            .expect("change-stream build input lock poisoned")
            .take()
            .ok_or_else(|| "change-stream write build input was already consumed".to_string())?;
        let planned = crate::engine::build_physical_plan_as_iceberg_change_stream_write(
            &build_input.state,
            build_input.current_catalog.as_deref(),
            &build_input.current_database,
            &build_input.physical_plan,
            &mut build_input.dag,
            build_input.mv_refresh_ctx.as_deref(),
        )?;
        let crate::engine::PlannedIcebergChangeStreamWrite {
            build_result,
            commit_plan,
            ..
        } = planned;
        *self
            .commit_plan
            .lock()
            .expect("change-stream commit plan lock poisoned") = Some(commit_plan);
        crate::engine::execute_planned_iceberg_change_stream_write(
            build_result,
            build_input.query_opts.clone(),
        )
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let guard = self
            .commit_plan
            .lock()
            .expect("change-stream commit plan lock poisoned");
        let plan = guard.as_ref().ok_or_else(|| {
            CommitServiceError::known_uncommitted(
                "change-stream commit plan is missing; coordinated write did not complete"
                    .to_string(),
                CleanupAttempt::not_attempted(),
            )
        })?;
        self.commit_executor
            .commit_change_stream_write_input(write_commit, plan)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
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
    use crate::runtime::sink_commit_wire::writer_report_to_sink_commit_info;
    use crate::runtime::write_coordinator::{WriteCommitInput, WriterCommitInput, WriterKey};
    use crate::thrift::types;

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

    fn test_collector(metadata: &TableMetadata) -> IcebergCommitCollector {
        IcebergCommitCollector::new(
            CommitOpKind::RowDeltaDvFromFiles,
            TableIdent::new(NamespaceIdent::new("db".to_string()), "t".to_string()),
            metadata.current_snapshot().map(|s| s.snapshot_id()),
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            "file:///warehouse/db/t/staging".to_string(),
            UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone())
    }

    fn sink_commit_info_for_data_file(
        metadata: &TableMetadata,
        path: &str,
    ) -> types::TSinkCommitInfo {
        let report = IcebergWriterReport {
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
        };
        writer_report_to_sink_commit_info(report, metadata).expect("wire encode")
    }

    fn writer_key(
        query_id: &types::TUniqueId,
        writer_fragment_id: i32,
        backend_num: i32,
    ) -> WriterKey {
        WriterKey {
            query_id: query_id.clone(),
            fragment_instance_id: types::TUniqueId::new(
                101,
                ((writer_fragment_id as i64) << 16) | backend_num as i64,
            ),
            backend_num,
        }
    }

    fn write_commit_for_fragments(
        metadata: &TableMetadata,
        fragments: Vec<(i32, &str)>,
    ) -> WriteCommitInput {
        let query_id = types::TUniqueId::new(10, 20);
        let writers = fragments
            .into_iter()
            .enumerate()
            .map(|(idx, (writer_fragment_id, path))| {
                let backend_num = idx as i32;
                WriterCommitInput {
                    writer_id: idx,
                    writer_key: writer_key(&query_id, writer_fragment_id, backend_num),
                    sink_commit_infos: vec![sink_commit_info_for_data_file(metadata, path)],
                    tablet_commit_infos: Vec::new(),
                    tablet_fail_infos: Vec::new(),
                    load_counters: BTreeMap::new(),
                    loaded_rows: 2,
                    loaded_bytes: 128,
                    filtered_rows: 0,
                }
            })
            .collect();
        WriteCommitInput {
            write_id: query_id,
            writers,
        }
    }

    #[test]
    fn finst_id_decodes_writer_fragment_id() {
        let finst = types::TUniqueId::new(7, (42_i64 << 16) | 3);
        assert_eq!(writer_fragment_id_from_finst_lo(finst.lo), 42);
    }

    #[test]
    fn change_stream_commit_routes_fresh_files_to_appended_channel() {
        let metadata = test_unpartitioned_metadata();
        let collector = test_collector(&metadata);
        let write_commit = write_commit_for_fragments(
            &metadata,
            vec![(10, "reuse.parquet"), (11, "fresh.parquet")],
        );
        let plan = ChangeStreamWriterCommitPlan::new(BTreeMap::from([
            (10, ChangeStreamBranchKind::ReuseData),
            (11, ChangeStreamBranchKind::FreshData),
        ]));

        route_change_stream_writer_reports(&collector, &metadata, &write_commit, &plan)
            .expect("route writer reports")
            .inject(&collector);

        let written = collector.take_written_files().expect("written channel");
        assert_eq!(
            written.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
            vec!["reuse.parquet"]
        );
        let appended = collector.take_appended_files();
        assert_eq!(
            appended.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
            vec!["fresh.parquet"]
        );
        assert!(
            written
                .iter()
                .chain(appended.iter())
                .all(|f| f.content == DataContentType::Data)
        );
    }

    #[test]
    fn change_stream_commit_rejects_unknown_writer_fragment() {
        let metadata = test_unpartitioned_metadata();
        let collector = test_collector(&metadata);
        let write_commit = write_commit_for_fragments(&metadata, vec![(12, "unknown.parquet")]);
        let plan = ChangeStreamWriterCommitPlan::new(BTreeMap::from([(
            10,
            ChangeStreamBranchKind::ReuseData,
        )]));

        let err = route_change_stream_writer_reports(&collector, &metadata, &write_commit, &plan)
            .expect_err("unknown writer fragment");
        let (message, converted) = err.into_parts();

        assert!(message.contains("writer fragment 12 is not declared"));
        assert_eq!(
            converted
                .iter()
                .map(|f| f.path.as_str())
                .collect::<Vec<_>>(),
            vec!["unknown.parquet"]
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
    ) -> crate::sql::planner::IcebergChangeStreamWriteTopology {
        crate::sql::planner::IcebergChangeStreamWriteTopology {
            writer_branches: branches
                .into_iter()
                .map(|(branch_id, branch_kind, writer_fragment_id)| {
                    crate::sql::planner::IcebergChangeStreamWriterBranch {
                        branch_id,
                        branch_kind,
                        writer_fragment_id,
                        sink_spec: crate::sql::planner::write_sink::test_support::simple_sink_spec(
                        ),
                    }
                })
                .collect(),
        }
    }
}
