//! Test-only IR equivalence helpers.

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use iceberg::spec::{
        DataContentType, DataFileFormat, FormatVersion, NestedField, PrimitiveType, Schema, Struct,
        Type,
    };

    use crate::connector::iceberg::IcebergMetadataTableType;
    use crate::connector::{ConnectorRegistry, iceberg::IcebergConnectorScanPlanner};
    use crate::meta::repository::mv::StoredMvDefinition;
    use crate::meta::repository::mv_contract::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, MvSchemaContract, OutputColumnLineage,
        OutputContract, TargetContract, TargetVisibleColumn,
    };
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, SortItem, TypedExpr,
    };
    use crate::sql::catalog::{
        CatalogProvider, ColumnDef, IcebergDataFileBinding, IcebergDataFileInfo,
        IcebergMvTargetStatePartitionConstraint, IcebergMvTargetStateRowFilter,
        IcebergMvTargetStateScan, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::codegen::fragment_builder::PlanFragmentBuilder;
    use crate::sql::codegen::{
        AggregateStateTargetPositionLocator, DirectExecPlan, FragmentBuildResult, FragmentEdge,
        FragmentEdgeKind, MultiFragmentBuildResult, OutputColumn as CodegenOutputColumn,
        PlanBuildResult, RuntimeFilterPlanResult,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        AggMode, AggregateStateMergeOp, JoinDistribution, Operator, PhysicalAssertOneRowOp,
        PhysicalCTEAnchorOp, PhysicalCTEConsumeOp, PhysicalCTEProduceOp, PhysicalDecodeOp,
        PhysicalDistributionOp, PhysicalExceptOp, PhysicalFilterOp, PhysicalGenerateSeriesOp,
        PhysicalHashAggregateOp, PhysicalHashJoinEqCondition, PhysicalHashJoinOp,
        PhysicalIntersectOp, PhysicalLimitOp, PhysicalNestLoopJoinOp, PhysicalProjectOp,
        PhysicalRepeatOp, PhysicalScanOp, PhysicalSortOp, PhysicalTableFunctionOp, PhysicalTopNOp,
        PhysicalUnionOp, PhysicalValuesOp, PhysicalWindowOp, ScanDictionaryColumn, TopNPhase,
    };
    use crate::sql::optimizer::physical_plan::{PhysicalPlanNode, PlanExecutionProps};
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::runtime_filter_pass::{RuntimeFilterDesc, RuntimeFilterProbe};
    use crate::sql::optimizer::statistics::Statistics;
    use crate::sql::planner::plan::{AggregateCall, DecodeMapping, WindowExpr};

    #[test]
    fn scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("scan", scan_plan());
    }

    #[test]
    fn scan_filter_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("scan_filter", filter_plan(scan_plan()));
    }

    #[test]
    fn scan_project_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("scan_project", project_plan(scan_plan()));
    }

    #[test]
    fn scan_filter_project_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "scan_filter_project",
            project_plan(filter_plan(scan_plan())),
        );
    }

    #[test]
    fn root_gather_scan_filter_project_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "root_gather_scan_filter_project",
            root_gather_plan(project_plan(filter_plan(scan_plan()))),
        );
    }

    #[test]
    fn sort_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("sort_over_scan", sort_plan(scan_plan()));
    }

    #[test]
    fn limit_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "limit_over_scan",
            limit_plan(scan_plan(), Some(5), None),
        );
    }

    #[test]
    fn limit_over_sort_with_offset_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "limit_over_sort_with_offset",
            limit_plan(sort_plan(scan_plan()), Some(5), Some(2)),
        );
    }

    #[test]
    fn limit_over_top_n_overrides_top_n_limit_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "limit_over_top_n_overrides_top_n_limit",
            limit_plan(
                top_n_plan(scan_plan(), TopNPhase::Final, false, Some(10), Some(0)),
                Some(5),
                Some(2),
            ),
        );
    }

    #[test]
    fn limit_over_aggregate_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "limit_over_aggregate",
            limit_plan(aggregate_count_plan(scan_plan()), Some(3), None),
        );
    }

    #[test]
    fn limit_over_hash_join_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "limit_over_hash_join",
            limit_plan(inner_hash_join_two_scans_plan(), Some(4), None),
        );
    }

    #[test]
    fn top_n_final_single_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "top_n_final_single_over_scan",
            top_n_plan(scan_plan(), TopNPhase::Final, false, Some(5), Some(1)),
        );
    }

    #[test]
    fn top_n_partial_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "top_n_partial_over_scan",
            top_n_plan(scan_plan(), TopNPhase::Partial, false, Some(7), Some(0)),
        );
    }

    #[test]
    fn top_n_split_matches_direct_fragment_builder() {
        let partial = top_n_plan(scan_plan(), TopNPhase::Partial, false, Some(5), Some(0));
        assert_distributed_plan_equivalent(
            "top_n_split",
            top_n_plan(partial, TopNPhase::Final, true, Some(5), Some(0)),
        );
    }

    #[test]
    fn limit_offset_exchange_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "limit_offset_exchange",
            limit_plan(scan_plan(), Some(5), Some(1)),
        );
    }

    #[test]
    fn gather_over_limit_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "gather_over_limit",
            sort_plan(distribution_plan(
                limit_plan(scan_plan(), Some(5), None),
                DistributionSpec::Gather,
            )),
        );
    }

    #[test]
    fn aggregate_single_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "aggregate_single_over_scan",
            aggregate_group_by_plan(scan_plan()),
        );
    }

    #[test]
    fn aggregate_with_count_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "aggregate_with_count",
            aggregate_count_plan(scan_plan()),
        );
    }

    #[test]
    fn shuffle_agg_exchange_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "shuffle_agg_exchange",
            aggregate_group_by_plan(distribution_plan(
                scan_plan(),
                DistributionSpec::shuffle_agg([ColumnId::new_for_test(1)]),
            )),
        );
    }

    #[test]
    fn nested_gather_exchange_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "nested_gather_exchange",
            sort_plan(distribution_plan(scan_plan(), DistributionSpec::Gather)),
        );
    }

    #[test]
    fn cte_produce_consume_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("cte_produce_consume", cte_produce_consume_plan());
    }

    #[test]
    fn sort_over_project_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "sort_over_project_over_scan",
            sort_plan(project_plan(scan_plan())),
        );
    }

    #[test]
    fn inner_hash_join_two_scans_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "inner_hash_join_two_scans",
            inner_hash_join_two_scans_plan(),
        );
    }

    #[test]
    fn broadcast_join_exchange_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "broadcast_join_exchange",
            broadcast_join_exchange_plan(),
        );
    }

    #[test]
    fn two_sided_shuffle_join_exchange_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "two_sided_shuffle_join_exchange",
            two_sided_shuffle_join_exchange_plan(),
        );
    }

    #[test]
    fn gather_root_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "gather_root",
            root_gather_plan(project_plan(filter_plan(scan_plan()))),
        );
    }

    #[test]
    fn left_outer_hash_join_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("left_outer_hash_join", left_outer_hash_join_plan());
    }

    #[test]
    fn hash_join_other_condition_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "hash_join_other_condition",
            hash_join_other_condition_plan(),
        );
    }

    #[test]
    fn left_semi_hash_join_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "left_semi_hash_join",
            hash_join_surviving_side_plan(JoinKind::LeftSemi),
        );
    }

    #[test]
    fn right_anti_hash_join_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "right_anti_hash_join",
            hash_join_surviving_side_plan(JoinKind::RightAnti),
        );
    }

    #[test]
    fn null_aware_left_anti_hash_join_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "null_aware_left_anti_hash_join",
            hash_join_surviving_side_plan(JoinKind::NullAwareLeftAnti),
        );
    }

    #[test]
    fn nest_loop_cross_join_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("nest_loop_cross_join", nest_loop_cross_join_plan());
    }

    #[test]
    fn nest_loop_inner_condition_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "nest_loop_inner_condition",
            nest_loop_condition_plan(JoinKind::Inner),
        );
    }

    #[test]
    fn nest_loop_left_outer_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "nest_loop_left_outer",
            nest_loop_condition_plan(JoinKind::LeftOuter),
        );
    }

    #[test]
    fn nest_loop_left_anti_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "nest_loop_left_anti",
            nest_loop_surviving_side_plan(JoinKind::LeftAnti),
        );
    }

    #[test]
    fn nest_loop_null_aware_left_anti_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "nest_loop_null_aware_left_anti",
            nest_loop_surviving_side_plan(JoinKind::NullAwareLeftAnti),
        );
    }

    #[test]
    fn union_all_two_scans_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "union_all_two_scans",
            union_plan(
                true,
                aliased_scan_plan("l", 1, 2),
                aliased_scan_plan("r", 3, 4),
            ),
        );
    }

    #[test]
    fn union_distinct_two_scans_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent_with_large_stack(
            "union_distinct_two_scans",
            union_plan(
                false,
                aliased_scan_plan("l", 1, 2),
                aliased_scan_plan("r", 3, 4),
            ),
        );
    }

    #[test]
    fn intersect_two_scans_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "intersect_two_scans",
            intersect_plan(aliased_scan_plan("l", 1, 2), aliased_scan_plan("r", 3, 4)),
        );
    }

    #[test]
    fn except_two_scans_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "except_two_scans",
            except_plan(aliased_scan_plan("l", 1, 2), aliased_scan_plan("r", 3, 4)),
        );
    }

    #[test]
    fn values_rows_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("values_rows", values_rows_plan());
    }

    #[test]
    fn assert_one_row_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "assert_one_row_over_scan",
            assert_one_row_plan(scan_plan()),
        );
    }

    #[test]
    fn decode_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("decode_over_scan", decode_over_scan_plan());
    }

    #[test]
    fn repeat_grouping_sets_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("repeat_grouping_sets", repeat_grouping_sets_plan());
    }

    #[test]
    fn window_row_number_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "window_row_number_over_scan",
            window_row_number_over_scan_plan(),
        );
    }

    #[test]
    fn generate_series_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent("generate_series", generate_series_plan());
    }

    #[test]
    fn unnest_table_function_over_scan_matches_direct_fragment_builder() {
        assert_distributed_plan_equivalent(
            "unnest_table_function_over_scan",
            unnest_table_function_over_scan_plan(),
        );
    }

    #[test]
    fn decode_output_expr_uses_materialized_string_slot() {
        let build = build_distributed_plan_only("decode_output_expr_slot", decode_over_scan_plan());
        let root = fragment_by_id("decode_output_expr_slot", &build, build.root_fragment_id);
        let decode = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == crate::plan_nodes::TPlanNodeType::DECODE_NODE)
            .expect("decode node");
        let decode_payload = decode.decode_node.as_ref().expect("decode payload");
        let mapping = decode_payload
            .dict_id_to_string_ids
            .as_ref()
            .expect("decode mapping");
        let string_slot_id = *mapping.values().next().expect("one decode mapping");
        let output_exprs = root.output_exprs.as_ref().expect("root output exprs");
        let slot_ref = output_exprs[0].nodes[0]
            .slot_ref
            .as_ref()
            .expect("decode output slot ref");
        assert_eq!(
            slot_ref.slot_id, string_slot_id,
            "decode result sink must read the materialized string slot"
        );
    }

    #[test]
    fn set_op_uses_declared_child_output_order() {
        let build =
            build_distributed_plan_only("set_op_child_output_order", reordered_union_values_plan());
        let root = fragment_by_id("set_op_child_output_order", &build, build.root_fragment_id);
        let union = root.plan.nodes.first().expect("set op root");
        assert_eq!(
            union.node_type,
            crate::plan_nodes::TPlanNodeType::UNION_NODE
        );
        assert_eq!(union.num_children, 2);
        let first_expr = &union
            .union_node
            .as_ref()
            .expect("union payload")
            .result_expr_lists[0][0];
        assert_eq!(
            first_expr.nodes[0].node_type,
            crate::exprs::TExprNodeType::SLOT_REF,
            "first set-op expression should read the declared string child column directly"
        );
    }

    #[test]
    fn set_op_child_arity_mismatch_fails_fast() {
        assert_distributed_plan_error_contains(
            "set_op_child_arity_mismatch",
            union_plan(
                true,
                aliased_scan_plan("l", 1, 2),
                single_column_scan_plan(output_col(901, "k", DataType::Int64, false)),
            ),
            "set operation child 1 column count mismatch",
        );
    }

    #[test]
    fn values_row_length_mismatch_fails_fast() {
        assert_distributed_plan_error_contains(
            "values_row_length_mismatch",
            bad_values_row_length_plan(),
            "VALUES row column count mismatch",
        );
    }

    #[test]
    fn iceberg_data_file_scan_ranges_match_direct_fragment_builder() {
        let mut connectors = ConnectorRegistry::new();
        connectors.register_scan_planner(Arc::new(IcebergConnectorScanPlanner::new()));
        let (direct, distributed) = build_both_paths(
            "iceberg_data_file_scan_ranges",
            iceberg_data_file_scan_plan(),
            &connectors,
        );

        assert_non_empty_scan_ranges("iceberg_data_file_scan_ranges direct", &direct);
        assert_non_empty_scan_ranges(
            "iceberg_data_file_scan_ranges DistributedPlan",
            &distributed,
        );
        assert_multi_fragment_equivalent("iceberg_data_file_scan_ranges", &direct, &distributed);
    }

    #[test]
    fn aggregate_state_merge_direct_exec_matches_ir_builder() {
        let ctx = aggregate_refresh_context_for_test();
        let plan = aggregate_state_merge_plan_for_context(&ctx);
        let (direct, distributed) = build_both_paths_with_mv_refresh_ctx(
            "aggregate_state_merge_direct_exec",
            plan,
            &ConnectorRegistry::new(),
            Some(&ctx),
        );

        assert_multi_fragment_equivalent(
            "aggregate_state_merge_direct_exec",
            &direct,
            &distributed,
        );
    }

    #[test]
    fn mv_target_state_scan_matches_ir_builder() {
        let ctx = scan_refresh_context_for_test();
        let plan = target_state_scan_plan_for_context(&ctx);
        let mut connectors = ConnectorRegistry::new();
        connectors.register_scan_planner(Arc::new(IcebergConnectorScanPlanner::new()));
        let (direct, distributed) = build_both_paths_with_mv_refresh_ctx(
            "mv_target_state_scan",
            plan,
            &connectors,
            Some(&ctx),
        );

        let root = fragment_by_id(
            "mv_target_state_scan direct",
            &direct,
            direct.root_fragment_id,
        );
        assert!(
            root.direct_exec.is_none(),
            "target-state scan equivalence must exercise regular fragment build"
        );
        assert_multi_fragment_equivalent("mv_target_state_scan", &direct, &distributed);
    }

    #[test]
    fn mv_version_scan_matches_ir_builder() {
        let fixture = local_version_scan_fixture();
        let mut ctx = scan_refresh_context_for_test();
        ctx.base_catalog_entries = [("ice".to_string(), fixture.catalog_entry.clone())]
            .into_iter()
            .collect();
        let plan = version_scan_plan_for_fixture(&fixture);
        let mut connectors = ConnectorRegistry::new();
        connectors.register_scan_planner(Arc::new(IcebergConnectorScanPlanner::new()));
        let (direct, distributed) =
            build_both_paths_with_mv_refresh_ctx("mv_version_scan", plan, &connectors, Some(&ctx));

        let root = fragment_by_id("mv_version_scan direct", &direct, direct.root_fragment_id);
        assert!(
            root.direct_exec.is_none(),
            "version scan equivalence must exercise regular fragment build"
        );
        assert_multi_fragment_equivalent("mv_version_scan", &direct, &distributed);
    }

    #[test]
    fn branch_union_aggregate_direct_exec_matches_ir_builder() {
        let ctx = aggregate_refresh_context_for_test();
        let state_names = aggregate_state_names_for_context(&ctx);
        let branch0 = branch_union_project_for_test(
            aggregate_merge_plan_for_test(
                values_plan_for_columns(aggregate_physical_columns_for_test()),
                values_plan_for_columns(aggregate_delta_state_columns_for_test()),
                state_names.clone(),
            ),
            0,
            1000,
        );
        let branch1 = branch_union_project_for_test(
            aggregate_merge_plan_for_test(
                values_plan_for_columns(aggregate_physical_columns_for_test()),
                values_plan_for_columns(aggregate_delta_state_columns_for_test()),
                state_names,
            ),
            1,
            1100,
        );
        let output_columns = branch0.output_columns.clone();
        let plan = physical_node(
            Operator::PhysicalUnion(PhysicalUnionOp {
                all: true,
                output_columns: output_columns.clone(),
                child_output_columns: vec![
                    branch0.output_columns.clone(),
                    branch1.output_columns.clone(),
                ],
            }),
            vec![branch0, branch1],
            output_columns,
        );
        let (direct, distributed) = build_both_paths_with_mv_refresh_ctx(
            "branch_union_aggregate_direct_exec",
            plan,
            &ConnectorRegistry::new(),
            Some(&ctx),
        );

        assert_multi_fragment_equivalent(
            "branch_union_aggregate_direct_exec",
            &direct,
            &distributed,
        );
    }

    #[test]
    fn iceberg_sink_matches_ir_builder() {
        let plan = values_plan_for_columns(vec![output_col(1, "id", DataType::Int32, false)]);
        let connectors = ConnectorRegistry::new();
        let mut sink_spec =
            crate::sql::codegen::iceberg_write_sink::test_support::simple_sink_spec();
        sink_spec.iceberg.serialized_metadata = Some(
            crate::sql::codegen::iceberg_write_sink::test_support::single_bucket_partition_metadata_json(),
        );
        let catalog = DummyCatalog;
        let direct = PlanFragmentBuilder::build_with_iceberg_sink(
            &plan,
            &catalog,
            &connectors,
            "test_db",
            None,
            &sink_spec,
        )
        .expect("direct iceberg sink build");
        let distributed = PlanFragmentBuilder::build_via_distributed_plan_with_iceberg_sink(
            &plan,
            &catalog,
            &connectors,
            "test_db",
            None,
            &sink_spec,
        )
        .expect("DistributedPlan iceberg sink build");

        assert_multi_fragment_equivalent("iceberg_sink", &direct, &distributed);
    }

    fn assert_distributed_plan_equivalent(case_name: &str, plan: PhysicalPlanNode) {
        let connectors = ConnectorRegistry::new();
        let (direct, distributed) = build_both_paths(case_name, plan, &connectors);
        assert_multi_fragment_equivalent(case_name, &direct, &distributed);
    }

    fn assert_distributed_plan_equivalent_with_large_stack(
        case_name: &'static str,
        plan: PhysicalPlanNode,
    ) {
        std::thread::Builder::new()
            .name(format!("{case_name}_equiv"))
            .stack_size(64 * 1024 * 1024)
            .spawn(move || assert_distributed_plan_equivalent(case_name, plan))
            .expect("spawn equivalence test thread")
            .join()
            .expect("equivalence test thread panicked");
    }

    fn build_both_paths(
        case_name: &str,
        plan: PhysicalPlanNode,
        connectors: &ConnectorRegistry,
    ) -> (MultiFragmentBuildResult, MultiFragmentBuildResult) {
        let catalog = DummyCatalog;
        let direct = PlanFragmentBuilder::build(&plan, &catalog, &connectors, "test_db")
            .unwrap_or_else(|err| panic!("{case_name}: direct build failed: {err}"));
        let distributed = PlanFragmentBuilder::build_via_distributed_plan(
            &plan,
            &catalog,
            &connectors,
            "test_db",
        )
        .unwrap_or_else(|err| panic!("{case_name}: DistributedPlan build failed: {err}"));

        (direct, distributed)
    }

    fn build_both_paths_with_mv_refresh_ctx(
        case_name: &str,
        plan: PhysicalPlanNode,
        connectors: &ConnectorRegistry,
        mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    ) -> (MultiFragmentBuildResult, MultiFragmentBuildResult) {
        let catalog = DummyCatalog;
        let direct = PlanFragmentBuilder::build_with_mv_refresh_ctx(
            &plan,
            &catalog,
            &connectors,
            "test_db",
            mv_refresh_ctx,
        )
        .unwrap_or_else(|err| panic!("{case_name}: direct build failed: {err}"));
        let distributed = PlanFragmentBuilder::build_via_distributed_plan_with_mv_refresh_ctx(
            &plan,
            &catalog,
            &connectors,
            "test_db",
            mv_refresh_ctx,
        )
        .unwrap_or_else(|err| panic!("{case_name}: DistributedPlan build failed: {err}"));

        (direct, distributed)
    }

    fn build_distributed_plan_only(
        case_name: &str,
        plan: PhysicalPlanNode,
    ) -> MultiFragmentBuildResult {
        let catalog = DummyCatalog;
        let connectors = ConnectorRegistry::new();
        PlanFragmentBuilder::build_via_distributed_plan(&plan, &catalog, &connectors, "test_db")
            .unwrap_or_else(|err| panic!("{case_name}: DistributedPlan build failed: {err}"))
    }

    fn assert_distributed_plan_error_contains(
        case_name: &str,
        plan: PhysicalPlanNode,
        expected: &str,
    ) {
        let catalog = DummyCatalog;
        let connectors = ConnectorRegistry::new();
        let err = match PlanFragmentBuilder::build_via_distributed_plan(
            &plan,
            &catalog,
            &connectors,
            "test_db",
        ) {
            Ok(_) => panic!("{case_name}: DistributedPlan build unexpectedly succeeded"),
            Err(err) => err,
        };
        assert!(
            err.contains(expected),
            "{case_name}: expected error to contain `{expected}`, got `{err}`"
        );
    }

    fn assert_multi_fragment_equivalent(
        case_name: &str,
        direct: &MultiFragmentBuildResult,
        distributed: &MultiFragmentBuildResult,
    ) {
        assert_eq!(
            direct.root_fragment_id, distributed.root_fragment_id,
            "{case_name}: root_fragment_id"
        );
        assert_eq!(
            direct.fragment_results.len(),
            distributed.fragment_results.len(),
            "{case_name}: fragment count"
        );
        assert_edges_eq(case_name, &direct.edges, &distributed.edges);
        assert_eq!(
            direct.boundary_schemas, distributed.boundary_schemas,
            "{case_name}: multi-fragment boundary schemas"
        );
        assert_runtime_filter_plan_eq(case_name, &direct.rf_plan, &distributed.rf_plan);

        for direct_fragment in &direct.fragment_results {
            let distributed_fragment =
                fragment_by_id(case_name, distributed, direct_fragment.fragment_id);
            assert_fragment_equivalent(case_name, direct_fragment, distributed_fragment);
        }
    }

    fn fragment_by_id<'a>(
        case_name: &str,
        result: &'a MultiFragmentBuildResult,
        fragment_id: crate::sql::codegen::FragmentId,
    ) -> &'a FragmentBuildResult {
        result
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == fragment_id)
            .unwrap_or_else(|| panic!("{case_name}: fragment {fragment_id} not found"))
    }

    fn assert_fragment_equivalent(
        case_name: &str,
        direct: &FragmentBuildResult,
        distributed: &FragmentBuildResult,
    ) {
        assert_eq!(
            direct.fragment_id, distributed.fragment_id,
            "{case_name}: fragment_id"
        );
        assert_eq!(direct.plan, distributed.plan, "{case_name}: fragment plan");
        assert_eq!(
            direct.desc_tbl, distributed.desc_tbl,
            "{case_name}: descriptor table"
        );
        assert_eq!(
            direct.exec_params, distributed.exec_params,
            "{case_name}: exec params"
        );
        assert_eq!(
            direct.output_sink, distributed.output_sink,
            "{case_name}: output sink"
        );
        assert_eq!(
            direct.output_exprs, distributed.output_exprs,
            "{case_name}: output exprs"
        );
        assert_output_columns_eq(
            case_name,
            &direct.output_columns,
            &distributed.output_columns,
        );
        assert_direct_exec_eq(case_name, &direct.direct_exec, &distributed.direct_exec);
        assert_eq!(
            direct.boundary_schemas, distributed.boundary_schemas,
            "{case_name}: fragment boundary schemas"
        );
        assert_eq!(direct.cte_id, distributed.cte_id, "{case_name}: cte_id");
        assert_eq!(
            direct.cte_exchange_nodes, distributed.cte_exchange_nodes,
            "{case_name}: cte exchange nodes"
        );
        assert_eq!(
            direct.query_global_dicts, distributed.query_global_dicts,
            "{case_name}: query global dicts"
        );
        assert_eq!(
            direct.query_global_dict_exprs, distributed.query_global_dict_exprs,
            "{case_name}: query global dict exprs"
        );
    }

    fn assert_direct_exec_eq(
        case_name: &str,
        direct: &Option<Box<DirectExecPlan>>,
        distributed: &Option<Box<DirectExecPlan>>,
    ) {
        match (direct.as_deref(), distributed.as_deref()) {
            (None, None) => {}
            (Some(direct), Some(distributed)) => {
                assert_direct_exec_plan_eq(case_name, direct, distributed)
            }
            _ => panic!("{case_name}: direct_exec presence mismatch"),
        }
    }

    fn assert_direct_exec_plan_eq(
        case_name: &str,
        direct: &DirectExecPlan,
        distributed: &DirectExecPlan,
    ) {
        match (direct, distributed) {
            (
                DirectExecPlan::AggregateStateMerge {
                    old_input: direct_old,
                    delta_input: direct_delta,
                    layout: direct_layout,
                    branch_id: direct_branch_id,
                    pruning_limits: direct_pruning_limits,
                    target_position_locator: direct_locator,
                },
                DirectExecPlan::AggregateStateMerge {
                    old_input: distributed_old,
                    delta_input: distributed_delta,
                    layout: distributed_layout,
                    branch_id: distributed_branch_id,
                    pruning_limits: distributed_pruning_limits,
                    target_position_locator: distributed_locator,
                },
            ) => {
                assert_plan_build_result_eq(
                    &format!("{case_name}: aggregate merge old input"),
                    direct_old,
                    distributed_old,
                );
                assert_plan_build_result_eq(
                    &format!("{case_name}: aggregate merge delta input"),
                    direct_delta,
                    distributed_delta,
                );
                assert_eq!(
                    direct_layout, distributed_layout,
                    "{case_name}: aggregate merge layout"
                );
                assert_eq!(
                    direct_branch_id, distributed_branch_id,
                    "{case_name}: aggregate merge branch_id"
                );
                assert_eq!(
                    direct_pruning_limits, distributed_pruning_limits,
                    "{case_name}: aggregate merge pruning limits"
                );
                assert_target_position_locator_eq(case_name, direct_locator, distributed_locator);
            }
            (
                DirectExecPlan::AggregateStatePhysicalize {
                    input: direct_input,
                    layout: direct_layout,
                },
                DirectExecPlan::AggregateStatePhysicalize {
                    input: distributed_input,
                    layout: distributed_layout,
                },
            ) => {
                assert_plan_build_result_eq(
                    &format!("{case_name}: aggregate physicalize input"),
                    direct_input,
                    distributed_input,
                );
                assert_eq!(
                    direct_layout, distributed_layout,
                    "{case_name}: aggregate physicalize layout"
                );
            }
            (
                DirectExecPlan::UnionAll {
                    inputs: direct_inputs,
                },
                DirectExecPlan::UnionAll {
                    inputs: distributed_inputs,
                },
            ) => {
                assert_eq!(
                    direct_inputs.len(),
                    distributed_inputs.len(),
                    "{case_name}: direct union input count"
                );
                for (idx, (direct_input, distributed_input)) in direct_inputs
                    .iter()
                    .zip(distributed_inputs.iter())
                    .enumerate()
                {
                    assert_plan_build_result_eq(
                        &format!("{case_name}: direct union input {idx}"),
                        direct_input,
                        distributed_input,
                    );
                }
            }
            _ => panic!("{case_name}: direct_exec variant mismatch"),
        }
    }

    fn assert_plan_build_result_eq(
        case_name: &str,
        direct: &PlanBuildResult,
        distributed: &PlanBuildResult,
    ) {
        assert_eq!(direct.plan, distributed.plan, "{case_name}: plan");
        assert_eq!(
            direct.desc_tbl, distributed.desc_tbl,
            "{case_name}: descriptor table"
        );
        assert_eq!(
            direct.exec_params, distributed.exec_params,
            "{case_name}: exec params"
        );
        assert_output_columns_eq(
            case_name,
            &direct.output_columns,
            &distributed.output_columns,
        );
        assert_direct_exec_eq(case_name, &direct.direct_exec, &distributed.direct_exec);
        assert_eq!(
            direct.boundary_schemas, distributed.boundary_schemas,
            "{case_name}: boundary schemas"
        );
        assert_eq!(
            direct.query_global_dicts, distributed.query_global_dicts,
            "{case_name}: query global dicts"
        );
        assert_eq!(
            direct.query_global_dict_exprs, distributed.query_global_dict_exprs,
            "{case_name}: query global dict exprs"
        );
    }

    fn assert_target_position_locator_eq(
        case_name: &str,
        direct: &Option<AggregateStateTargetPositionLocator>,
        distributed: &Option<AggregateStateTargetPositionLocator>,
    ) {
        match (direct, distributed) {
            (None, None) => {}
            (Some(direct), Some(distributed)) => {
                assert_eq!(
                    direct.target_entry.kind, distributed.target_entry.kind,
                    "{case_name}: target locator catalog kind"
                );
                assert_eq!(
                    direct.target_entry.warehouse_uri, distributed.target_entry.warehouse_uri,
                    "{case_name}: target locator warehouse uri"
                );
                assert_eq!(
                    direct.target_entry.rest_uri, distributed.target_entry.rest_uri,
                    "{case_name}: target locator REST uri"
                );
                assert_eq!(
                    direct.target_entry.hms_uris, distributed.target_entry.hms_uris,
                    "{case_name}: target locator HMS uris"
                );
                assert_eq!(
                    direct.target_entry.properties, distributed.target_entry.properties,
                    "{case_name}: target locator catalog properties"
                );
                assert_eq!(
                    direct.target_entry.warehouse_path, distributed.target_entry.warehouse_path,
                    "{case_name}: target locator warehouse path"
                );
                assert_eq!(
                    direct.target_table.identifier().to_string(),
                    distributed.target_table.identifier().to_string(),
                    "{case_name}: target locator table identifier"
                );
                assert_eq!(
                    direct.target_table.metadata().uuid(),
                    distributed.target_table.metadata().uuid(),
                    "{case_name}: target locator table uuid"
                );
                assert_eq!(
                    direct.target_table.metadata().location(),
                    distributed.target_table.metadata().location(),
                    "{case_name}: target locator table location"
                );
                assert_eq!(
                    direct.target_table.metadata().current_snapshot_id(),
                    distributed.target_table.metadata().current_snapshot_id(),
                    "{case_name}: target locator table snapshot"
                );
                assert_eq!(
                    direct.target_table.metadata().current_schema(),
                    distributed.target_table.metadata().current_schema(),
                    "{case_name}: target locator table schema"
                );
                assert_eq!(
                    direct.partition_filter, distributed.partition_filter,
                    "{case_name}: target locator partition filter"
                );
                assert_eq!(
                    direct.apply_key_column, distributed.apply_key_column,
                    "{case_name}: target locator apply key column"
                );
            }
            _ => panic!("{case_name}: target_position_locator presence mismatch"),
        }
    }

    fn assert_output_columns_eq(
        case_name: &str,
        direct: &[CodegenOutputColumn],
        distributed: &[CodegenOutputColumn],
    ) {
        assert_eq!(
            direct.len(),
            distributed.len(),
            "{case_name}: output column count"
        );
        for (idx, (direct, distributed)) in direct.iter().zip(distributed.iter()).enumerate() {
            assert_eq!(
                direct.name, distributed.name,
                "{case_name}: output column {idx} name"
            );
            assert_eq!(
                direct.data_type, distributed.data_type,
                "{case_name}: output column {idx} type"
            );
            assert_eq!(
                direct.nullable, distributed.nullable,
                "{case_name}: output column {idx} nullability"
            );
        }
    }

    fn assert_edges_eq(case_name: &str, direct: &[FragmentEdge], distributed: &[FragmentEdge]) {
        assert_eq!(direct.len(), distributed.len(), "{case_name}: edge count");
        for (idx, (direct, distributed)) in direct.iter().zip(distributed.iter()).enumerate() {
            assert_eq!(
                direct.source_fragment_id, distributed.source_fragment_id,
                "{case_name}: edge {idx} source fragment"
            );
            assert_eq!(
                direct.target_fragment_id, distributed.target_fragment_id,
                "{case_name}: edge {idx} target fragment"
            );
            assert_eq!(
                direct.target_exchange_node_id, distributed.target_exchange_node_id,
                "{case_name}: edge {idx} target exchange node"
            );
            assert_eq!(
                direct.output_partition, distributed.output_partition,
                "{case_name}: edge {idx} output partition"
            );
            assert_eq!(
                direct.stream_kind, distributed.stream_kind,
                "{case_name}: edge {idx} stream kind"
            );
            assert_fragment_edge_kind_eq(case_name, idx, &direct.edge_kind, &distributed.edge_kind);
        }
    }

    fn assert_fragment_edge_kind_eq(
        case_name: &str,
        idx: usize,
        direct: &FragmentEdgeKind,
        distributed: &FragmentEdgeKind,
    ) {
        match (direct, distributed) {
            (FragmentEdgeKind::Stream, FragmentEdgeKind::Stream) => {}
            (
                FragmentEdgeKind::CteMulticast { cte_id: direct_id },
                FragmentEdgeKind::CteMulticast {
                    cte_id: distributed_id,
                },
            ) => assert_eq!(
                direct_id, distributed_id,
                "{case_name}: edge {idx} CTE multicast id"
            ),
            _ => panic!("{case_name}: edge {idx} kind mismatch: direct and DistributedPlan differ"),
        }
    }

    fn assert_runtime_filter_plan_eq(
        case_name: &str,
        direct: &Option<RuntimeFilterPlanResult>,
        distributed: &Option<RuntimeFilterPlanResult>,
    ) {
        match (direct, distributed) {
            (None, None) => {}
            (Some(direct), Some(distributed)) => {
                assert_eq!(
                    direct.all_filters, distributed.all_filters,
                    "{case_name}: runtime filter descriptors"
                );
                assert_eq!(
                    direct.build_side_filters, distributed.build_side_filters,
                    "{case_name}: runtime filter build-side map"
                );
                assert_eq!(
                    direct.probe_side_filters, distributed.probe_side_filters,
                    "{case_name}: runtime filter probe-side map"
                );
            }
            _ => panic!("{case_name}: runtime filter plan presence mismatch"),
        }
    }

    fn assert_non_empty_scan_ranges(case_name: &str, result: &MultiFragmentBuildResult) {
        let root = fragment_by_id(case_name, result, result.root_fragment_id);
        let ranges = &root.exec_params.per_node_scan_ranges;
        assert!(
            !ranges.is_empty() && ranges.values().any(|node_ranges| !node_ranges.is_empty()),
            "{case_name}: expected non-empty scan ranges"
        );
    }

    struct DummyCatalog;

    impl CatalogProvider for DummyCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("equivalence tests use fully resolved metadata-table scans".to_string())
        }
    }

    fn scan_plan() -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "test_db".to_string(),
                table: metadata_table_def(),
                alias: Some("t".to_string()),
                columns: vec![k.clone(), v.clone()],
                predicates: vec![cmp_expr(
                    column_ref_expr(1, "k", DataType::Int64, false),
                    BinOp::Eq,
                    int_lit(7),
                )],
                required_columns: Some(vec!["k".to_string(), "v".to_string()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k, v],
        )
    }

    fn iceberg_data_file_scan_plan() -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "test_db".to_string(),
                table: iceberg_data_table_def(),
                alias: Some("t".to_string()),
                columns: vec![k.clone(), v.clone()],
                predicates: vec![cmp_expr(
                    column_ref_expr(1, "k", DataType::Int64, false),
                    BinOp::Eq,
                    int_lit(7),
                )],
                required_columns: Some(vec!["k".to_string(), "v".to_string()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k, v],
        )
    }

    fn values_plan_for_columns(columns: Vec<OutputColumn>) -> PhysicalPlanNode {
        physical_node(
            Operator::PhysicalValues(PhysicalValuesOp {
                rows: Vec::new(),
                columns: columns.clone(),
            }),
            Vec::new(),
            columns,
        )
    }

    fn aggregate_state_names_for_context(
        ctx: &crate::engine::mv::refresh_context::IcebergMvRefreshContext,
    ) -> Vec<String> {
        ctx.rewrite
            .aggregate_shape_and_layout_for_execution()
            .expect("aggregate layout")
            .1
            .state_columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }

    fn aggregate_state_merge_plan_for_context(
        ctx: &crate::engine::mv::refresh_context::IcebergMvRefreshContext,
    ) -> PhysicalPlanNode {
        aggregate_merge_plan_for_test(
            values_plan_for_columns(aggregate_physical_columns_for_test()),
            values_plan_for_columns(aggregate_delta_state_columns_for_test()),
            aggregate_state_names_for_context(ctx),
        )
    }

    fn target_state_scan_plan_for_context(
        ctx: &crate::engine::mv::refresh_context::IcebergMvRefreshContext,
    ) -> PhysicalPlanNode {
        let row_id = output_col(110, "__row_id__", DataType::Utf8, false);
        let region = output_col(111, "region", DataType::Utf8, true);
        let scan = target_state_scan_for_context(ctx);
        physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "ns".to_string(),
                table: target_state_table_def(scan),
                alias: Some("mv".to_string()),
                columns: vec![row_id.clone(), region.clone()],
                predicates: vec![],
                required_columns: Some(vec!["__row_id__".to_string(), "region".to_string()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![row_id, region],
        )
    }

    fn target_state_scan_for_context(
        ctx: &crate::engine::mv::refresh_context::IcebergMvRefreshContext,
    ) -> IcebergMvTargetStateScan {
        let (_, layout) = ctx
            .rewrite
            .aggregate_shape_and_layout_for_execution()
            .expect("aggregate target-state layout");
        IcebergMvTargetStateScan {
            catalog: ctx.rewrite.target.catalog.clone(),
            database: ctx.rewrite.target.namespace.clone(),
            table: ctx.rewrite.target.table.clone(),
            target_table_uuid: ctx.rewrite.target_table_uuid.clone(),
            target_snapshot_id: ctx.rewrite.target_snapshot_id,
            aggregate_state_layout_version: ctx
                .rewrite
                .schema_contract
                .aggregate
                .as_ref()
                .expect("aggregate contract")
                .state_layout_version,
            columns: vec![
                column_def("__row_id__", DataType::Utf8, false),
                column_def("region", DataType::Utf8, true),
            ],
            group_key_names: vec!["region".to_string()],
            aggregate_state_names: layout
                .state_columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            physical_column_names: layout
                .physical_columns
                .iter()
                .map(|column| column.column.name.clone())
                .collect(),
            row_id_column_name: "__row_id__".to_string(),
            row_filter: IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: "__row_id__".to_string(),
                branch_scope: None,
            },
            partition_constraint: IcebergMvTargetStatePartitionConstraint::Unpartitioned,
        }
    }

    fn target_state_table_def(scan: IcebergMvTargetStateScan) -> TableDef {
        TableDef {
            name: scan.table.clone(),
            columns: aggregate_physical_columns_for_test()
                .into_iter()
                .map(|column| column_def(&column.name, column.data_type, column.nullable))
                .collect(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergMvTargetState(scan),
        }
    }

    struct LocalVersionScanFixture {
        _tmpdir: tempfile::TempDir,
        catalog_entry: crate::connector::iceberg::catalog::registry::IcebergCatalogEntry,
        table_info: IcebergTableInfo,
        snapshot_id: i64,
    }

    fn local_version_scan_fixture() -> LocalVersionScanFixture {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        runtime.block_on(async {
            use crate::connector::iceberg::commit::{
                CommitCtx, CommitOpKind, FastAppendCommit, IcebergCommitAction,
                IcebergCommitCollector, WrittenFile,
            };
            use iceberg::{NamespaceIdent, TableCreation, TableIdent};

            let tmpdir = tempfile::tempdir().expect("tempdir");
            let warehouse_uri = format!("file://{}", tmpdir.path().display());
            let file_io = iceberg::io::FileIO::new_with_fs();
            let catalog: Arc<dyn iceberg::Catalog> = Arc::new(
                crate::connector::iceberg::catalog::hadoop_catalog::HadoopFileSystemCatalog::new(
                    file_io,
                    warehouse_uri.clone(),
                ),
            );
            let namespace = NamespaceIdent::new("ns".to_string());
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .expect("create namespace");
            let schema = Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .expect("base schema");
            let table_ident = TableIdent::new(namespace.clone(), "version_base".to_string());
            let mut table = catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("version_base".to_string())
                        .schema(schema)
                        .format_version(FormatVersion::V3)
                        .build(),
                )
                .await
                .expect("create base table");

            let table_location = table.metadata().location().to_string();
            let metadata = table.metadata();
            let collector = Arc::new(
                IcebergCommitCollector::new(
                    CommitOpKind::FastAppend,
                    table_ident.clone(),
                    metadata
                        .current_snapshot()
                        .map(|snapshot| snapshot.snapshot_id()),
                    metadata.last_sequence_number(),
                    metadata.current_schema().clone(),
                    metadata.default_partition_spec().clone(),
                    format!("{table_location}/staging"),
                    crate::common::types::UniqueId { hi: 0, lo: 0 },
                )
                .with_table_metadata(metadata.clone()),
            );
            collector.inject_written_file(WrittenFile {
                path: format!("{table_location}/data/file-0.parquet"),
                format: DataFileFormat::Parquet,
                content: DataContentType::Data,
                partition_values: Struct::empty(),
                partition_spec_id: 0,
                record_count: 10,
                file_size_in_bytes: 1024,
                split_offsets: vec![],
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                key_metadata: None,
                referenced_data_file: None,
                equality_ids: None,
                first_row_id: None,
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            });
            let file_io = table.file_io().clone();
            let abort_handle = collector.abort_log.clone();
            let snapshot_properties = BTreeMap::new();
            let ctx = CommitCtx {
                collector: &collector,
                table: &table,
                catalog: catalog.as_ref(),
                file_io: &file_io,
                commit_uuid: uuid::Uuid::new_v4(),
                abort_handle,
                target_ref: "main",
                snapshot_properties: &snapshot_properties,
            };
            FastAppendCommit
                .commit(ctx)
                .await
                .expect("append synthetic data file");
            table = catalog
                .load_table(&table_ident)
                .await
                .expect("reload base table");
            let snapshot_id = table
                .metadata()
                .current_snapshot()
                .expect("current snapshot")
                .snapshot_id();
            let catalog_entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "ice",
                &[
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    (
                        "iceberg.catalog.warehouse".to_string(),
                        warehouse_uri.clone(),
                    ),
                ],
            )
            .expect("base catalog entry");
            let table_info = IcebergTableInfo {
                catalog: "ice".to_string(),
                namespace: "ns".to_string(),
                table: "version_base".to_string(),
                table_uuid: Some(table.metadata().uuid().to_string()),
                current_snapshot_id: Some(snapshot_id),
                schema_id: table.metadata().current_schema_id(),
                location: table.metadata().location().to_string(),
                schema: IcebergSchemaDef { fields: vec![] },
                serialized_metadata: None,
                serialized_metadata_rows: None,
            };

            LocalVersionScanFixture {
                _tmpdir: tmpdir,
                catalog_entry,
                table_info,
                snapshot_id,
            }
        })
    }

    fn version_scan_plan_for_fixture(fixture: &LocalVersionScanFixture) -> PhysicalPlanNode {
        let id = output_col(120, "id", DataType::Int64, false);
        physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "ns".to_string(),
                table: TableDef {
                    name: fixture.table_info.table.clone(),
                    columns: vec![column_def("id", DataType::Int64, false)],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergVersionTable {
                        table: fixture.table_info.clone(),
                        snapshot_id: fixture.snapshot_id,
                    },
                },
                alias: Some("base".to_string()),
                columns: vec![id.clone()],
                predicates: vec![],
                required_columns: Some(vec!["id".to_string()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![id],
        )
    }

    fn aggregate_merge_plan_for_test(
        old_child: PhysicalPlanNode,
        delta_child: PhysicalPlanNode,
        aggregate_state_names: Vec<String>,
    ) -> PhysicalPlanNode {
        let output_columns = vec![
            output_col(1, "region", DataType::Utf8, true),
            output_col(2, "c", DataType::Int64, false),
            output_col(3, "s", DataType::Int64, true),
            output_col(4, "__change_op", DataType::Int8, false),
        ];
        physical_node(
            Operator::PhysicalAggregateStateMerge(AggregateStateMergeOp {
                group_key_names: vec!["region".to_string()],
                aggregate_state_names,
                change_op_column: crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
                output_columns: output_columns.clone(),
            }),
            vec![old_child, delta_child],
            output_columns,
        )
    }

    fn aggregate_physical_columns_for_test() -> Vec<OutputColumn> {
        vec![
            output_col(10, "__row_id__", DataType::Utf8, false),
            output_col(11, "region", DataType::Utf8, true),
            output_col(12, "c", DataType::Int64, false),
            output_col(13, "s", DataType::Int64, true),
            output_col(14, "__agg_state_c", DataType::Binary, false),
            output_col(15, "__agg_state_s", DataType::Binary, false),
        ]
    }

    fn aggregate_delta_state_columns_for_test() -> Vec<OutputColumn> {
        vec![
            output_col(21, "region", DataType::Utf8, true),
            output_col(22, "__agg_state_c", DataType::Binary, false),
            output_col(23, "__agg_state_s", DataType::Binary, false),
        ]
    }

    fn branch_union_project_for_test(
        merge: PhysicalPlanNode,
        branch_id: i32,
        output_id_base: u32,
    ) -> PhysicalPlanNode {
        let mut items = merge
            .output_columns
            .iter()
            .enumerate()
            .map(|(idx, column)| ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: column.column_id,
                        qualifier: None,
                        column: column.name.clone(),
                    },
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                },
                output_name: column.name.clone(),
                output_column_id: ColumnId::new_for_test(output_id_base + idx as u32),
            })
            .collect::<Vec<_>>();
        let branch_column_id = ColumnId::new_for_test(output_id_base + items.len() as u32);
        items.push(ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(branch_id as i64)),
                data_type: DataType::Int32,
                nullable: false,
            },
            output_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN
                .to_string(),
            output_column_id: branch_column_id,
        });
        let output_columns = items
            .iter()
            .map(|item| OutputColumn {
                column_id: item.output_column_id,
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
                is_internal: false,
            })
            .collect::<Vec<_>>();
        physical_node(
            Operator::PhysicalProject(PhysicalProjectOp {
                items,
                output_qualifier: None,
            }),
            vec![merge],
            output_columns,
        )
    }

    fn aggregate_refresh_context_for_test()
    -> crate::engine::mv::refresh_context::IcebergMvRefreshContext {
        mv_refresh_context_for_test(Some(99))
    }

    fn scan_refresh_context_for_test() -> crate::engine::mv::refresh_context::IcebergMvRefreshContext
    {
        mv_refresh_context_for_test(None)
    }

    fn mv_refresh_context_for_test(
        target_snapshot_id: Option<i64>,
    ) -> crate::engine::mv::refresh_context::IcebergMvRefreshContext {
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use iceberg::{CatalogBuilder, NamespaceIdent, TableIdent};

        let target_schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        100,
                        "region",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::required(
                        101,
                        "c",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        102,
                        "s",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        999,
                        "__row_id__",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::required(
                        200,
                        "__agg_state_c",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                    Arc::new(NestedField::required(
                        201,
                        "__agg_state_s",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                ])
                .build()
                .expect("aggregate target schema"),
        );
        let contract = MvSchemaContract {
            contract_version: 3,
            base: BaseContract {
                table_fqn: "ice.ns.orders".to_string(),
                table_uuid: "uuid-orders".to_string(),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![
                        BaseFieldRecord {
                            field_id: 1,
                            name_at_create: "region".to_string(),
                            type_signature: "string".to_string(),
                            required: false,
                        },
                        BaseFieldRecord {
                            field_id: 2,
                            name_at_create: "amount".to_string(),
                            type_signature: "long".to_string(),
                            required: false,
                        },
                    ],
                },
            },
            bases: Vec::new(),
            output: OutputContract {
                columns: vec![
                    OutputColumnLineage {
                        expression: ExpressionLineage {
                            kind: ExpressionKind::Column,
                            referenced_base_field_ids: vec![1],
                            referenced_base_fields: Vec::new(),
                        },
                    },
                    OutputColumnLineage {
                        expression: ExpressionLineage {
                            kind: ExpressionKind::Func,
                            referenced_base_field_ids: Vec::new(),
                            referenced_base_fields: Vec::new(),
                        },
                    },
                    OutputColumnLineage {
                        expression: ExpressionLineage {
                            kind: ExpressionKind::Func,
                            referenced_base_field_ids: vec![2],
                            referenced_base_fields: Vec::new(),
                        },
                    },
                ],
                filter: None,
            },
            join: None,
            aggregate: Some(AggregateStateContract {
                state_layout_version: 1,
                row_id_column_name: "__row_id__".to_string(),
                state_columns: vec![
                    AggregateStateColumnContract {
                        column_name: "__agg_state_c".to_string(),
                        target_field_id: 200,
                        type_signature: "binary".to_string(),
                        nullable: false,
                        role: AggregateStateRoleContract::Single,
                    },
                    AggregateStateColumnContract {
                        column_name: "__agg_state_s".to_string(),
                        target_field_id: 201,
                        type_signature: "binary".to_string(),
                        nullable: false,
                        role: AggregateStateRoleContract::Single,
                    },
                ],
            }),
            branch: None,
            target: TargetContract {
                table_fqn: "tgt.ns.orders_mv".to_string(),
                table_uuid: "uuid-target".to_string(),
                schema_id_at_create: 7,
                visible_columns: vec![
                    TargetVisibleColumn {
                        output_name: "region".to_string(),
                        target_field_id: 100,
                        type_signature: "string".to_string(),
                        nullable: true,
                    },
                    TargetVisibleColumn {
                        output_name: "c".to_string(),
                        target_field_id: 101,
                        type_signature: "long".to_string(),
                        nullable: false,
                    },
                    TargetVisibleColumn {
                        output_name: "s".to_string(),
                        target_field_id: 102,
                        type_signature: "long".to_string(),
                        nullable: true,
                    },
                ],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__row_id__".to_string(),
                    target_field_id: 999,
                    source: ApplyKeySource::GroupRowId,
                },
                partition: None,
            },
        };
        let mv_definition = StoredMvDefinition {
            mv_id: 42,
            select_sql:
                "select region, count(*) as c, sum(amount) as s from ice.ns.orders group by region"
                    .to_string(),
            base_table_refs: vec!["ice.ns.orders".to_string()],
            primary_key_columns: vec!["__row_id__".to_string()],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("tgt".to_string()),
            target_namespace: Some("ns".to_string()),
            target_table: Some("orders_mv".to_string()),
            schema_contract: Some(contract.clone()),
            partition_spec: None,
            partition_state_complete: false,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: [("ice.ns.orders".to_string(), 11i64)].into_iter().collect(),
            last_refresh_table_uuids: [("ice.ns.orders".to_string(), "uuid-orders".to_string())]
                .into_iter()
                .collect(),
            last_refreshed_iceberg_snapshot_id: target_snapshot_id,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: Default::default(),
            refresh_policy: Default::default(),
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 0,
        };
        let query = Arc::new(
            crate::engine::mv::refresh_context::tests_support::parse_query(
                "select region, count(*) as c, sum(amount) as s from ice.ns.orders group by region",
            ),
        );
        let base_refs: Arc<[crate::connector::starrocks::table::model::IcebergTableRef]> =
            Arc::from(vec![
                crate::engine::mv::refresh_context::tests_support::make_ref("ice", "ns", "orders"),
            ]);
        let pin = Arc::new(crate::engine::mv::refresh_context::tests_support::make_pin(
            &[("ice.ns.orders", 22, "uuid-orders")],
        ));
        let rewrite = Arc::new(
            crate::engine::mv::refresh_context::IcebergMvRewriteContext::from_parts(
                crate::engine::mv::iceberg_refresh::IcebergMvTarget {
                    catalog: "tgt".to_string(),
                    namespace: "ns".to_string(),
                    table: "orders_mv".to_string(),
                },
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                Arc::new(mv_definition),
                query,
                base_refs,
                pin,
                target_snapshot_id,
                "uuid-target".to_string(),
                target_schema.clone(),
                Some(Arc::new(contract)),
            )
            .expect("aggregate rewrite context"),
        );
        let warehouse = format!("memory://equiv-{}", uuid::Uuid::new_v4());
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let iceberg_catalog: Arc<dyn iceberg::Catalog> = Arc::new(
            runtime
                .block_on(MemoryCatalogBuilder::default().load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
                ))
                .expect("memory catalog"),
        );
        let target_entry = Arc::new(
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "tgt",
                &[
                    ("iceberg.catalog.type".to_string(), "memory".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse),
                ],
            )
            .expect("target entry"),
        );
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            target_schema.as_ref().clone(),
            iceberg::spec::PartitionSpec::unpartition_spec().into_unbound(),
            iceberg::spec::SortOrder::unsorted_order(),
            "memory://target/orders_mv".to_string(),
            iceberg::spec::FormatVersion::V3,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let target_table = iceberg::table::Table::builder()
            .file_io(iceberg::io::FileIO::new_with_memory())
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("ns".to_string()),
                "orders_mv".to_string(),
            ))
            .build()
            .expect("target table");

        crate::engine::mv::refresh_context::IcebergMvRefreshContext {
            rewrite,
            target_entry,
            base_catalog_entries: BTreeMap::new(),
            iceberg_catalog,
            target_table,
            affected_partitions:
                crate::engine::mv::partition::AffectedTargetPartitions::not_derived("test context"),
            pruning_limits: crate::engine::mv::refresh_context::MvRefreshPruningLimits::default(),
        }
    }

    fn filter_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalFilter(PhysicalFilterOp {
                predicate: and_expr(
                    cmp_expr(
                        column_ref_expr(1, "k", DataType::Int64, false),
                        BinOp::Gt,
                        int_lit(10),
                    ),
                    cmp_expr(
                        column_ref_expr(2, "v", DataType::Int64, true),
                        BinOp::Lt,
                        int_lit(20),
                    ),
                ),
            }),
            vec![child],
            output_columns,
        )
    }

    fn project_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = vec![output_col(101, "k_plus_one", DataType::Int64, false)];
        physical_node(
            Operator::PhysicalProject(PhysicalProjectOp {
                items: vec![ProjectItem {
                    expr: add_expr(column_ref_expr(1, "k", DataType::Int64, false), int_lit(1)),
                    output_name: "k_plus_one".to_string(),
                    output_column_id: ColumnId::new_for_test(101),
                }],
                output_qualifier: None,
            }),
            vec![child],
            output_columns,
        )
    }

    fn sort_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let sort_col = child.output_columns[0].clone();
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalSort(PhysicalSortOp {
                items: vec![SortItem {
                    expr: column_ref_expr(
                        sort_col.column_id.0,
                        &sort_col.name,
                        sort_col.data_type.clone(),
                        sort_col.nullable,
                    ),
                    asc: true,
                    nulls_first: false,
                }],
                analytic_partition_exprs: vec![],
                partition_limit: None,
                topn_type: None,
            }),
            vec![child],
            output_columns,
        )
    }

    fn limit_plan(
        child: PhysicalPlanNode,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalLimit(PhysicalLimitOp { limit, offset }),
            vec![child],
            output_columns,
        )
    }

    fn top_n_plan(
        child: PhysicalPlanNode,
        phase: TopNPhase,
        is_split: bool,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> PhysicalPlanNode {
        let sort_col = child.output_columns[0].clone();
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalTopN(PhysicalTopNOp {
                items: vec![SortItem {
                    expr: column_ref_expr(
                        sort_col.column_id.0,
                        &sort_col.name,
                        sort_col.data_type.clone(),
                        sort_col.nullable,
                    ),
                    asc: true,
                    nulls_first: false,
                }],
                limit,
                offset,
                phase,
                is_split,
            }),
            vec![child],
            output_columns,
        )
    }

    fn aggregate_group_by_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        physical_node(
            Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: vec![column_ref_expr(1, "k", DataType::Int64, false)],
                aggregates: vec![],
                output_columns: vec![k.clone()],
                is_merge: vec![],
            }),
            vec![child],
            vec![k],
        )
    }

    fn aggregate_count_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        let count = output_col(201, "count(*)", DataType::Int64, true);
        physical_node(
            Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: vec![column_ref_expr(1, "k", DataType::Int64, false)],
                aggregates: vec![AggregateCall {
                    name: "count".to_string(),
                    args: vec![],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: ColumnId::new_for_test(201),
                }],
                output_columns: vec![k.clone(), count.clone()],
                is_merge: vec![false],
            }),
            vec![child],
            vec![k, count],
        )
    }

    fn root_gather_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Gather,
            }),
            vec![child],
            output_columns,
        )
    }

    fn distribution_plan(child: PhysicalPlanNode, spec: DistributionSpec) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalDistribution(PhysicalDistributionOp { spec }),
            vec![child],
            output_columns,
        )
    }

    fn cte_produce_consume_plan() -> PhysicalPlanNode {
        let cte_id = 7;
        let produce = single_column_scan_plan(output_col(1101, "k", DataType::Int64, false));
        let produce_output_columns = produce.output_columns.clone();
        let consume_output_columns = vec![output_col(1102, "k", DataType::Int64, false)];
        let produce = physical_node(
            Operator::PhysicalCTEProduce(PhysicalCTEProduceOp {
                cte_id,
                output_columns: produce_output_columns.clone(),
            }),
            vec![produce],
            produce_output_columns,
        );
        let consume = physical_node(
            Operator::PhysicalCTEConsume(PhysicalCTEConsumeOp {
                cte_id,
                alias: "cte".to_string(),
                output_columns: consume_output_columns.clone(),
            }),
            vec![],
            consume_output_columns.clone(),
        );
        physical_node(
            Operator::PhysicalCTEAnchor(PhysicalCTEAnchorOp { cte_id }),
            vec![produce, consume],
            consume_output_columns,
        )
    }

    fn inner_hash_join_two_scans_plan() -> PhysicalPlanNode {
        let (mut join, left_key, right_key) = hash_join_plan(JoinKind::Inner);
        join.children[0].probe_runtime_filters = vec![RuntimeFilterProbe {
            filter_id: 7,
            probe_expr: left_key.clone(),
        }];
        join.build_runtime_filters = vec![RuntimeFilterDesc {
            filter_id: 7,
            build_expr: right_key,
            probe_expr: left_key,
            expr_order: 0,
            distribution: JoinDistribution::Broadcast,
        }];
        join
    }

    fn broadcast_join_exchange_plan() -> PhysicalPlanNode {
        let left = aliased_scan_plan("l", 1, 2);
        let right = distribution_plan(aliased_scan_plan("r", 3, 4), DistributionSpec::Broadcast);
        let left_key = column_ref_expr_with_qualifier(1, "l", "k", DataType::Int64, false);
        let right_key = column_ref_expr_with_qualifier(3, "r", "k", DataType::Int64, false);
        let output_columns = join_output_columns(&left, &right, JoinOutput::Both);
        physical_node(
            Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn two_sided_shuffle_join_exchange_plan() -> PhysicalPlanNode {
        let left = distribution_plan(
            aliased_scan_plan("l", 1, 2),
            DistributionSpec::shuffle_agg([ColumnId::new_for_test(1)]),
        );
        let right = distribution_plan(
            aliased_scan_plan("r", 3, 4),
            DistributionSpec::shuffle_agg([ColumnId::new_for_test(3)]),
        );
        let left_key = column_ref_expr_with_qualifier(1, "l", "k", DataType::Int64, false);
        let right_key = column_ref_expr_with_qualifier(3, "r", "k", DataType::Int64, false);
        let output_columns = join_output_columns(&left, &right, JoinOutput::Both);
        physical_node(
            Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Shuffle,
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn left_outer_hash_join_plan() -> PhysicalPlanNode {
        let (join, _, _) = hash_join_plan(JoinKind::LeftOuter);
        join
    }

    fn hash_join_plan(join_type: JoinKind) -> (PhysicalPlanNode, TypedExpr, TypedExpr) {
        hash_join_plan_with_options(join_type, None, JoinOutput::Both)
    }

    fn hash_join_other_condition_plan() -> PhysicalPlanNode {
        let left_value = column_ref_expr_with_qualifier(2, "l", "v", DataType::Int64, true);
        let right_value = column_ref_expr_with_qualifier(4, "r", "v", DataType::Int64, true);
        let other_condition = cmp_expr(left_value, BinOp::Gt, right_value);
        let (join, _, _) =
            hash_join_plan_with_options(JoinKind::Inner, Some(other_condition), JoinOutput::Both);
        join
    }

    fn hash_join_surviving_side_plan(join_type: JoinKind) -> PhysicalPlanNode {
        let output = match join_type {
            JoinKind::RightSemi | JoinKind::RightAnti => JoinOutput::RightOnly,
            _ => JoinOutput::LeftOnly,
        };
        let (join, _, _) = hash_join_plan_with_options(join_type, None, output);
        join
    }

    #[derive(Clone, Copy)]
    enum JoinOutput {
        Both,
        LeftOnly,
        RightOnly,
    }

    fn hash_join_plan_with_options(
        join_type: JoinKind,
        other_condition: Option<TypedExpr>,
        output: JoinOutput,
    ) -> (PhysicalPlanNode, TypedExpr, TypedExpr) {
        let left = aliased_scan_plan("l", 1, 2);
        let right = aliased_scan_plan("r", 3, 4);
        let left_key = column_ref_expr_with_qualifier(1, "l", "k", DataType::Int64, false);
        let right_key = column_ref_expr_with_qualifier(3, "r", "k", DataType::Int64, false);
        let output_columns = join_output_columns(&left, &right, output);
        let node = physical_node(
            Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: left_key.clone(),
                    right: right_key.clone(),
                    null_safe: false,
                }],
                other_condition,
                distribution: JoinDistribution::Broadcast,
            }),
            vec![left, right],
            output_columns,
        );
        (node, left_key, right_key)
    }

    fn join_output_columns(
        left: &PhysicalPlanNode,
        right: &PhysicalPlanNode,
        output: JoinOutput,
    ) -> Vec<OutputColumn> {
        match output {
            JoinOutput::Both => {
                let mut output_columns = left.output_columns.clone();
                output_columns.extend(right.output_columns.clone());
                output_columns
            }
            JoinOutput::LeftOnly => left.output_columns.clone(),
            JoinOutput::RightOnly => right.output_columns.clone(),
        }
    }

    fn nest_loop_cross_join_plan() -> PhysicalPlanNode {
        nest_loop_plan(JoinKind::Cross, None, JoinOutput::Both)
    }

    fn nest_loop_condition_plan(join_type: JoinKind) -> PhysicalPlanNode {
        let left_value = column_ref_expr_with_qualifier(2, "l", "v", DataType::Int64, true);
        let right_value = column_ref_expr_with_qualifier(4, "r", "v", DataType::Int64, true);
        nest_loop_plan(
            join_type,
            Some(cmp_expr(left_value, BinOp::Gt, right_value)),
            JoinOutput::Both,
        )
    }

    fn nest_loop_surviving_side_plan(join_type: JoinKind) -> PhysicalPlanNode {
        let output = match join_type {
            JoinKind::RightSemi | JoinKind::RightAnti => JoinOutput::RightOnly,
            _ => JoinOutput::LeftOnly,
        };
        let left_value = column_ref_expr_with_qualifier(2, "l", "v", DataType::Int64, true);
        let right_value = column_ref_expr_with_qualifier(4, "r", "v", DataType::Int64, true);
        nest_loop_plan(
            join_type,
            Some(cmp_expr(left_value, BinOp::Gt, right_value)),
            output,
        )
    }

    fn nest_loop_plan(
        join_type: JoinKind,
        condition: Option<TypedExpr>,
        output: JoinOutput,
    ) -> PhysicalPlanNode {
        let left = aliased_scan_plan("l", 1, 2);
        let right = aliased_scan_plan("r", 3, 4);
        let output_columns = join_output_columns(&left, &right, output);
        physical_node(
            Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
                join_type,
                condition,
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn union_plan(all: bool, left: PhysicalPlanNode, right: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = set_op_output_columns();
        physical_node(
            Operator::PhysicalUnion(PhysicalUnionOp {
                all,
                output_columns: output_columns.clone(),
                child_output_columns: vec![
                    left.output_columns.clone(),
                    right.output_columns.clone(),
                ],
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn intersect_plan(left: PhysicalPlanNode, right: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = set_op_output_columns();
        physical_node(
            Operator::PhysicalIntersect(PhysicalIntersectOp {
                output_columns: output_columns.clone(),
                child_output_columns: vec![
                    left.output_columns.clone(),
                    right.output_columns.clone(),
                ],
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn except_plan(left: PhysicalPlanNode, right: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = set_op_output_columns();
        physical_node(
            Operator::PhysicalExcept(PhysicalExceptOp {
                output_columns: output_columns.clone(),
                child_output_columns: vec![
                    left.output_columns.clone(),
                    right.output_columns.clone(),
                ],
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn set_op_output_columns() -> Vec<OutputColumn> {
        vec![
            output_col(501, "k", DataType::Int64, false),
            output_col(502, "v", DataType::Int64, true),
        ]
    }

    fn values_rows_plan() -> PhysicalPlanNode {
        let k = output_col(601, "k", DataType::Int64, false);
        let v = output_col(602, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalValues(PhysicalValuesOp {
                rows: vec![
                    vec![int_lit(1), int_lit(10)],
                    vec![int_lit(2), null_int_lit()],
                ],
                columns: vec![k.clone(), v.clone()],
            }),
            vec![],
            vec![k, v],
        )
    }

    fn bad_values_row_length_plan() -> PhysicalPlanNode {
        let k = output_col(611, "k", DataType::Int64, false);
        let v = output_col(612, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalValues(PhysicalValuesOp {
                rows: vec![vec![int_lit(1)]],
                columns: vec![k.clone(), v.clone()],
            }),
            vec![],
            vec![k, v],
        )
    }

    fn reordered_union_values_plan() -> PhysicalPlanNode {
        let left = two_column_values_plan(621, 622);
        let right = two_column_values_plan(623, 624);
        let output_columns = vec![
            output_col(625, "s", DataType::Utf8, true),
            output_col(626, "k", DataType::Int64, false),
        ];
        let child_output_columns = vec![
            vec![
                left.output_columns[1].clone(),
                left.output_columns[0].clone(),
            ],
            vec![
                right.output_columns[1].clone(),
                right.output_columns[0].clone(),
            ],
        ];
        physical_node(
            Operator::PhysicalUnion(PhysicalUnionOp {
                all: true,
                output_columns: output_columns.clone(),
                child_output_columns,
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn two_column_values_plan(k_id: u32, s_id: u32) -> PhysicalPlanNode {
        let k = output_col(k_id, "k", DataType::Int64, false);
        let s = output_col(s_id, "s", DataType::Utf8, true);
        physical_node(
            Operator::PhysicalValues(PhysicalValuesOp {
                rows: vec![],
                columns: vec![k.clone(), s.clone()],
            }),
            vec![],
            vec![k, s],
        )
    }

    fn assert_one_row_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalAssertOneRow(PhysicalAssertOneRowOp {
                subquery_text: "select k from t".to_string(),
            }),
            vec![child],
            output_columns,
        )
    }

    fn decode_over_scan_plan() -> PhysicalPlanNode {
        let source_id = ColumnId::new_for_test(701);
        let output_id = ColumnId::new_for_test(702);
        let scan = physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "test_db".to_string(),
                table: dict_metadata_table_def(),
                alias: Some("t".to_string()),
                columns: vec![OutputColumn {
                    column_id: source_id,
                    name: "s".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    is_internal: false,
                }],
                predicates: vec![],
                required_columns: Some(vec!["s_dict".to_string()]),
                dict_columns: vec![ScanDictionaryColumn {
                    source_column: "s".to_string(),
                    dict_column: "s_dict".to_string(),
                    dictionary: dict_snapshot_a_b(),
                }],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![OutputColumn {
                column_id: source_id,
                name: "s_dict".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
        );
        let decoded = output_col(output_id.0, "s", DataType::Utf8, false);
        physical_node(
            Operator::PhysicalDecode(PhysicalDecodeOp {
                mappings: vec![DecodeMapping {
                    source_column_id: source_id,
                    output_column_id: output_id,
                    dict_column: "s_dict".to_string(),
                    string_column: "s".to_string(),
                }],
                output_columns: vec![decoded.clone()],
            }),
            vec![scan],
            vec![decoded],
        )
    }

    fn repeat_grouping_sets_plan() -> PhysicalPlanNode {
        let k = output_col(801, "k", DataType::Int64, false);
        let grouping_col = output_col(802, "__grouping_fn_0", DataType::Int64, false);
        physical_node(
            Operator::PhysicalRepeat(PhysicalRepeatOp {
                repeat_column_ref_list: vec![vec!["k".to_string()], vec![]],
                repeat_column_ref_ids: vec![vec![k.column_id], vec![]],
                grouping_ids: vec![0, 1],
                all_rollup_columns: vec!["k".to_string()],
                all_rollup_column_ids: vec![k.column_id],
                grouping_key_aliases: vec![],
                grouping_fn_args: vec![("__grouping_fn_0".to_string(), vec!["k".to_string()])],
                grouping_fn_arg_ids: vec![vec![k.column_id]],
                grouping_fn_ids: vec![("__grouping_fn_0".to_string(), grouping_col.column_id)],
            }),
            vec![single_column_scan_plan(k.clone())],
            vec![k, grouping_col],
        )
    }

    fn window_row_number_over_scan_plan() -> PhysicalPlanNode {
        let k = output_col(901, "k", DataType::Int64, false);
        let row_number = output_col(902, "rn", DataType::Int64, false);
        physical_node(
            Operator::PhysicalWindow(PhysicalWindowOp {
                window_exprs: vec![WindowExpr {
                    name: "row_number".to_string(),
                    args: vec![],
                    distinct: false,
                    partition_by: vec![],
                    order_by: vec![],
                    window_frame: None,
                    result_type: DataType::Int64,
                    output_name: row_number.name.clone(),
                    output_column_id: row_number.column_id,
                    ignore_nulls: false,
                }],
                output_columns: vec![k.clone(), row_number.clone()],
            }),
            vec![single_column_scan_plan(k.clone())],
            vec![k, row_number],
        )
    }

    fn generate_series_plan() -> PhysicalPlanNode {
        let value = output_col(911, "value", DataType::Int64, false);
        physical_node(
            Operator::PhysicalGenerateSeries(PhysicalGenerateSeriesOp {
                start: 1,
                end: 3,
                step: 1,
                column_name: value.name.clone(),
                alias: Some("gs".to_string()),
                output_column_id: value.column_id,
            }),
            vec![],
            vec![value],
        )
    }

    fn unnest_table_function_over_scan_plan() -> PhysicalPlanNode {
        let array_type = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int64,
            true,
        )));
        let arr = output_col(921, "arr", array_type.clone(), true);
        let item = output_col(922, "item", DataType::Int64, true);
        physical_node(
            Operator::PhysicalTableFunction(PhysicalTableFunctionOp {
                function_name: "unnest".to_string(),
                args: vec![column_ref_expr(921, "arr", array_type, true)],
                output_columns: vec![item.clone()],
                alias: Some("u".to_string()),
                is_left_join: false,
            }),
            vec![single_column_scan_plan(arr.clone())],
            vec![arr, item],
        )
    }

    fn aliased_scan_plan(alias: &str, key_id: u32, value_id: u32) -> PhysicalPlanNode {
        let k = output_col(key_id, "k", DataType::Int64, false);
        let v = output_col(value_id, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "test_db".to_string(),
                table: metadata_table_def(),
                alias: Some(alias.to_string()),
                columns: vec![k.clone(), v.clone()],
                predicates: vec![],
                required_columns: Some(vec!["k".to_string(), "v".to_string()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k, v],
        )
    }

    fn single_column_scan_plan(column: OutputColumn) -> PhysicalPlanNode {
        physical_node(
            Operator::PhysicalScan(PhysicalScanOp {
                database: "test_db".to_string(),
                table: single_column_metadata_table_def(&column),
                alias: Some("t".to_string()),
                columns: vec![column.clone()],
                predicates: vec![],
                required_columns: Some(vec![column.name.clone()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![column],
        )
    }

    fn physical_node(
        op: Operator,
        children: Vec<PhysicalPlanNode>,
        output_columns: Vec<OutputColumn>,
    ) -> PhysicalPlanNode {
        PhysicalPlanNode {
            op,
            children,
            stats: Statistics::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    fn metadata_table_def() -> TableDef {
        TableDef {
            name: "t$snapshots".to_string(),
            columns: vec![
                column_def("k", DataType::Int64, false),
                column_def("v", DataType::Int64, true),
                column_def("unused", DataType::Int64, true),
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergMetadataTable {
                table: iceberg_table_info(),
                metadata_table_type: IcebergMetadataTableType::Snapshots,
                serialized_table: "{}".to_string(),
                cloud_properties: Default::default(),
                metadata_payload: None,
            },
        }
    }

    fn single_column_metadata_table_def(column: &OutputColumn) -> TableDef {
        TableDef {
            name: "t$snapshots".to_string(),
            columns: vec![column_def(
                &column.name,
                column.data_type.clone(),
                column.nullable,
            )],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergMetadataTable {
                table: iceberg_table_info(),
                metadata_table_type: IcebergMetadataTableType::Snapshots,
                serialized_table: "{}".to_string(),
                cloud_properties: Default::default(),
                metadata_payload: None,
            },
        }
    }

    fn dict_metadata_table_def() -> TableDef {
        TableDef {
            name: "dict_t$snapshots".to_string(),
            columns: vec![column_def("s", DataType::Utf8, false)],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergMetadataTable {
                table: iceberg_table_info(),
                metadata_table_type: IcebergMetadataTableType::Snapshots,
                serialized_table: "{}".to_string(),
                cloud_properties: Default::default(),
                metadata_payload: None,
            },
        }
    }

    fn iceberg_data_table_def() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![
                column_def("k", DataType::Int64, false),
                column_def("v", DataType::Int64, true),
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg_table_info(),
                files: vec![iceberg_data_file("s3://bucket/t/data-1.parquet")],
                cloud_properties: Default::default(),
                binding: IcebergDataFileBinding::ExplicitFiles,
            },
        }
    }

    fn iceberg_data_file(path: &str) -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: path.to_string(),
            size: 128,
            row_count: Some(10),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        }
    }

    fn iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "t".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///warehouse/t".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn column_def(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn output_col(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn column_ref_expr(id: u32, column: &str, data_type: DataType, nullable: bool) -> TypedExpr {
        column_ref_expr_with_qualifier(id, "t", column, data_type, nullable)
    }

    fn column_ref_expr_with_qualifier(
        id: u32,
        qualifier: &str,
        column: &str,
        data_type: DataType,
        nullable: bool,
    ) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(qualifier.to_string()),
                column: column.to_string(),
            },
            data_type,
            nullable,
        }
    }

    fn int_lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn null_int_lit() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Null),
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn dict_snapshot_a_b() -> Arc<crate::engine::dictionary::model::DictionarySnapshot> {
        use crate::engine::dictionary::model::{
            DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue,
            DictionaryWatermark,
        };

        Arc::new(DictionarySnapshot {
            dictionary_id: 1,
            owner: DictionaryOwner::IcebergTable {
                catalog: "test_catalog".to_string(),
                namespace: "test_db".to_string(),
                table: "dict_t".to_string(),
                table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            },
            column_id: None,
            column_name: "s".to_string(),
            data_type: DataType::Int32,
            version: 1,
            watermark: DictionaryWatermark::Iceberg {
                snapshot_id: Some(7),
                schema_id: 1,
            },
            values: vec![
                DictionaryValue {
                    id: 1,
                    bytes: b"a".to_vec(),
                },
                DictionaryValue {
                    id: 2,
                    bytes: b"b".to_vec(),
                },
            ],
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving: true,
        })
    }

    fn cmp_expr(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn add_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Add,
                right: Box::new(right),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn and_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }
}
