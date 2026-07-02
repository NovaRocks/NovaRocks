use std::collections::BTreeSet;

use crate::sql::common::ChangeStreamBranchKind;
use crate::sql::planner::write_sink::IcebergWriteSinkSpec;

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteBranchSpec {
    pub(crate) branch_id: i32,
    pub(crate) branch_kind: ChangeStreamBranchKind,
    pub(crate) stream_output_ordinals: Vec<usize>,
    pub(crate) output_partition_ordinals: Vec<usize>,
    pub(crate) sink_spec: IcebergWriteSinkSpec,
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeStreamWriteDagSpec {
    pub(crate) change_op_output_ordinal: Option<usize>,
    pub(crate) data_route_output_ordinal: Option<usize>,
    pub(crate) branches: Vec<ChangeStreamWriteBranchSpec>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergChangeStreamRouterSink {
    pub(crate) group_id: i32,
    pub(crate) change_op_output_ordinal: usize,
    pub(crate) data_route_output_ordinal: Option<usize>,
    pub(crate) branches: Vec<IcebergChangeStreamBranchRoute>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergChangeStreamBranchRoute {
    pub(crate) branch_id: i32,
    pub(crate) branch_kind: ChangeStreamBranchKind,
    pub(crate) target_fragment_id: crate::sql::codegen::FragmentId,
    pub(crate) target_exchange_node_id: i32,
    pub(crate) output_ordinals: Vec<usize>,
    pub(crate) output_partition_ordinals: Vec<usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergChangeStreamWriteTopology {
    pub(crate) writer_branches: Vec<IcebergChangeStreamWriterBranch>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergChangeStreamWriterBranch {
    pub(crate) branch_id: i32,
    pub(crate) branch_kind: ChangeStreamBranchKind,
    pub(crate) writer_fragment_id: crate::sql::codegen::FragmentId,
    pub(crate) sink_spec: IcebergWriteSinkSpec,
}

#[derive(Clone, Debug)]
pub(crate) struct PlannedIcebergChangeStreamDistributedPlan {
    pub(crate) distributed_plan: crate::sql::planner::DistributedPlan,
    pub(crate) topology: IcebergChangeStreamWriteTopology,
}

impl ChangeStreamWriteBranchSpec {
    #[cfg(test)]
    pub(crate) fn for_test(
        branch_id: i32,
        branch_kind: ChangeStreamBranchKind,
        stream_output_ordinals: Vec<usize>,
    ) -> Self {
        let mut sink_spec = crate::sql::planner::write_sink::test_support::simple_sink_spec();
        sink_spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::write_sink::test_support::single_bucket_partition_metadata_json(),
        );
        sink_spec.mode = match branch_kind {
            ChangeStreamBranchKind::DeleteDv => {
                crate::sql::planner::write_sink::IcebergWriteSinkMode::DeletionVectors
            }
            ChangeStreamBranchKind::ReuseData => {
                crate::sql::planner::write_sink::IcebergWriteSinkMode::RowLineageData
            }
            ChangeStreamBranchKind::FreshData => {
                crate::sql::planner::write_sink::IcebergWriteSinkMode::Data
            }
        };
        Self {
            branch_id,
            branch_kind,
            stream_output_ordinals,
            output_partition_ordinals: Vec::new(),
            sink_spec,
        }
    }

    #[cfg(test)]
    pub(crate) fn delete_dv_for_test(stream_output_ordinals: Vec<usize>) -> Self {
        Self::for_test(0, ChangeStreamBranchKind::DeleteDv, stream_output_ordinals)
    }

    #[cfg(test)]
    pub(crate) fn reuse_data_for_test(stream_output_ordinals: Vec<usize>) -> Self {
        Self::for_test(1, ChangeStreamBranchKind::ReuseData, stream_output_ordinals)
    }

    #[cfg(test)]
    pub(crate) fn fresh_data_for_test(stream_output_ordinals: Vec<usize>) -> Self {
        Self::for_test(2, ChangeStreamBranchKind::FreshData, stream_output_ordinals)
    }
}

impl ChangeStreamWriteDagSpec {
    pub(crate) fn validate(&self) -> Result<(), String> {
        validate_branch_set(&self.branches)?;
        let has_data_branch = self.branches.iter().any(|b| {
            matches!(
                b.branch_kind,
                ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData
            )
        });
        if has_data_branch && self.data_route_output_ordinal.is_none() {
            return Err(
                "data_route_output_ordinal is required when data branches are declared".to_string(),
            );
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        change_op_output_ordinal: Option<usize>,
        data_route_output_ordinal: Option<usize>,
        branches: Vec<ChangeStreamWriteBranchSpec>,
    ) -> Self {
        Self {
            change_op_output_ordinal,
            data_route_output_ordinal,
            branches,
        }
    }
}

pub(crate) fn validate_branch_set(branches: &[ChangeStreamWriteBranchSpec]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for branch in branches {
        if !seen.insert(branch.branch_kind) {
            return Err(format!(
                "duplicate change-stream branch kind {:?}",
                branch.branch_kind
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_duplicate_branch_kind() {
        let branches = vec![
            ChangeStreamWriteBranchSpec::for_test(0, ChangeStreamBranchKind::DeleteDv, Vec::new()),
            ChangeStreamWriteBranchSpec::for_test(1, ChangeStreamBranchKind::DeleteDv, Vec::new()),
        ];
        let err = validate_branch_set(&branches).expect_err("duplicate branch kind");
        assert!(err.contains("duplicate change-stream branch kind DeleteDv"));
    }

    #[test]
    fn validate_requires_data_route_when_data_branch_exists() {
        let spec = ChangeStreamWriteDagSpec::for_test(
            Some(0),
            None,
            vec![ChangeStreamWriteBranchSpec::for_test(
                0,
                ChangeStreamBranchKind::ReuseData,
                Vec::new(),
            )],
        );
        let err = spec.validate().expect_err("missing data_route");
        assert!(
            err.contains("data_route_output_ordinal is required when data branches are declared")
        );
    }

    #[test]
    fn validate_allows_delete_only_without_data_route() {
        let spec = ChangeStreamWriteDagSpec::for_test(
            Some(0),
            None,
            vec![ChangeStreamWriteBranchSpec::for_test(
                0,
                ChangeStreamBranchKind::DeleteDv,
                Vec::new(),
            )],
        );
        spec.validate()
            .expect("delete-only does not require data route");
    }

    #[test]
    fn validate_allows_delete_and_one_data_branch_with_data_route() {
        let spec = ChangeStreamWriteDagSpec::for_test(
            Some(0),
            Some(1),
            vec![
                ChangeStreamWriteBranchSpec::for_test(
                    0,
                    ChangeStreamBranchKind::DeleteDv,
                    Vec::new(),
                ),
                ChangeStreamWriteBranchSpec::for_test(
                    1,
                    ChangeStreamBranchKind::FreshData,
                    Vec::new(),
                ),
            ],
        );
        spec.validate()
            .expect("change_op alone distinguishes delete from one data branch");
    }
}
