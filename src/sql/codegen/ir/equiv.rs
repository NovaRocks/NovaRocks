//! Test-only IR structural coverage helpers.

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
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
        FragmentBuildRequest, FragmentBuildResult, FragmentEdgeKind, FragmentId,
        MultiFragmentBuildResult,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        AggMode, AggregateOutputLayout, AssertOneRowOp, CTEAnchorOp, CTEConsumeOp, CTEProduceOp,
        DecodeOp, ExceptOp, FilterOp, GenerateSeriesOp, IntersectOp, JoinDistribution, LimitOp,
        Operator, PhysicalDistributionOp, PhysicalHashAggregateOp, PhysicalHashJoinEqCondition,
        PhysicalHashJoinOp, PhysicalNestLoopJoinOp, ProjectOp, RepeatOp, ScanDictionaryColumn,
        ScanOp, SortOp, TableFunctionOp, TopNOp, TopNPhase, UnionOp, ValuesOp, WindowOp,
    };
    use crate::sql::optimizer::physical_tree::{
        JoinExecutionDistribution, OptimizerPhysicalNode, PlanExecutionProps, attach_scalar_arena,
    };
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::runtime_filter_pass::{RuntimeFilterDesc, RuntimeFilterProbe};
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::optimizer::statistics::Statistics;
    use crate::sql::planner::optimizer_bridge::scalar::{
        intern_aggregate_calls, intern_exprs, intern_project_items, intern_sort_items,
        intern_window_exprs,
    };
    use crate::sql::planner::plan::{AggregateCall, DecodeMapping, WindowExpr};

    #[test]
    fn scan_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure("scan", scan_plan(), 1);
    }

    #[test]
    fn scan_filter_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure("scan_filter", filter_plan(scan_plan()), 1);
    }

    #[test]
    fn scan_project_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure("scan_project", project_plan(scan_plan()), 1);
    }

    #[test]
    fn scan_filter_project_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "scan_filter_project",
            project_plan(filter_plan(scan_plan())),
            1,
        );
    }

    #[test]
    fn root_gather_scan_filter_project_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "root_gather_scan_filter_project",
            root_gather_plan(project_plan(filter_plan(scan_plan()))),
            1,
        );
    }

    #[test]
    fn sort_over_scan_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure("sort_over_scan", sort_plan(scan_plan()), 1);
    }

    #[test]
    fn limit_over_scan_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "limit_over_scan",
            limit_plan(scan_plan(), Some(5), None),
            1,
        );
    }

    #[test]
    fn limit_over_sort_with_offset_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "limit_over_sort_with_offset",
            limit_plan(sort_plan(scan_plan()), Some(5), Some(2)),
            1,
        );
    }

    #[test]
    fn limit_over_top_n_overrides_top_n_limit_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "limit_over_top_n_overrides_top_n_limit",
            limit_plan(
                top_n_plan(scan_plan(), TopNPhase::Final, false, Some(10), Some(0)),
                Some(5),
                Some(2),
            ),
            1,
        );
    }

    #[test]
    fn limit_over_aggregate_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "limit_over_aggregate",
            limit_plan(aggregate_count_plan(scan_plan()), Some(3), None),
            1,
        );
    }

    #[test]
    fn limit_over_hash_join_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "limit_over_hash_join",
            limit_plan(inner_hash_join_two_scans_plan(), Some(4), None),
            1,
        );
    }

    #[test]
    fn top_n_final_single_over_scan_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "top_n_final_single_over_scan",
            top_n_plan(scan_plan(), TopNPhase::Final, false, Some(5), Some(1)),
            1,
        );
    }

    #[test]
    fn top_n_partial_over_scan_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "top_n_partial_over_scan",
            top_n_plan(scan_plan(), TopNPhase::Partial, false, Some(7), Some(0)),
            1,
        );
    }

    #[test]
    fn top_n_split_builds_ir_fragment_structure() {
        let partial = top_n_plan(scan_plan(), TopNPhase::Partial, false, Some(5), Some(0));
        assert_distributed_plan_ir_structure(
            "top_n_split",
            top_n_plan(partial, TopNPhase::Final, true, Some(5), Some(0)),
            2,
        );
    }

    #[test]
    fn limit_offset_exchange_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "limit_offset_exchange",
            limit_plan(scan_plan(), Some(5), Some(1)),
            2,
        );
    }

    #[test]
    fn gather_over_limit_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "gather_over_limit",
            sort_plan(distribution_plan(
                limit_plan(scan_plan(), Some(5), None),
                DistributionSpec::Gather,
            )),
            2,
        );
    }

    #[test]
    fn aggregate_single_over_scan_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "aggregate_single_over_scan",
            aggregate_group_by_plan(scan_plan()),
            1,
        );
    }

    #[test]
    fn aggregate_with_count_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "aggregate_with_count",
            aggregate_count_plan(scan_plan()),
            1,
        );
    }

    #[test]
    fn shuffle_agg_exchange_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "shuffle_agg_exchange",
            aggregate_group_by_plan(distribution_plan(
                scan_plan(),
                DistributionSpec::shuffle_agg([ColumnId::new_for_test(1)]),
            )),
            2,
        );
    }

    #[test]
    fn nested_gather_exchange_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "nested_gather_exchange",
            sort_plan(distribution_plan(scan_plan(), DistributionSpec::Gather)),
            2,
        );
    }

    #[test]
    fn cte_produce_consume_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure("cte_produce_consume", cte_produce_consume_plan(), 2);
    }

    #[test]
    fn sort_over_project_over_scan_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "sort_over_project_over_scan",
            sort_plan(project_plan(scan_plan())),
            1,
        );
    }

    #[test]
    fn inner_hash_join_two_scans_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "inner_hash_join_two_scans",
            inner_hash_join_two_scans_plan(),
            1,
        );
    }

    #[test]
    fn broadcast_join_exchange_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "broadcast_join_exchange",
            broadcast_join_exchange_plan(),
            2,
        );
    }

    #[test]
    fn two_sided_shuffle_join_exchange_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "two_sided_shuffle_join_exchange",
            two_sided_shuffle_join_exchange_plan(),
            3,
        );
    }

    #[test]
    fn gather_root_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "gather_root",
            root_gather_plan(project_plan(filter_plan(scan_plan()))),
            1,
        );
    }

    #[test]
    fn left_outer_hash_join_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "left_outer_hash_join",
            left_outer_hash_join_plan(),
            1,
        );
    }

    #[test]
    fn hash_join_other_condition_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "hash_join_other_condition",
            hash_join_other_condition_plan(),
            1,
        );
    }

    #[test]
    fn left_semi_hash_join_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "left_semi_hash_join",
            hash_join_surviving_side_plan(JoinKind::LeftSemi),
            1,
        );
    }

    #[test]
    fn right_anti_hash_join_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "right_anti_hash_join",
            hash_join_surviving_side_plan(JoinKind::RightAnti),
            1,
        );
    }

    #[test]
    fn null_aware_left_anti_hash_join_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "null_aware_left_anti_hash_join",
            hash_join_surviving_side_plan(JoinKind::NullAwareLeftAnti),
            1,
        );
    }

    #[test]
    fn nest_loop_cross_join_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "nest_loop_cross_join",
            nest_loop_cross_join_plan(),
            1,
        );
    }

    #[test]
    fn nest_loop_inner_condition_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "nest_loop_inner_condition",
            nest_loop_condition_plan(JoinKind::Inner),
            1,
        );
    }

    #[test]
    fn nest_loop_left_outer_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "nest_loop_left_outer",
            nest_loop_condition_plan(JoinKind::LeftOuter),
            1,
        );
    }

    #[test]
    fn nest_loop_left_anti_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "nest_loop_left_anti",
            nest_loop_surviving_side_plan(JoinKind::LeftAnti),
            1,
        );
    }

    #[test]
    fn nest_loop_null_aware_left_anti_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "nest_loop_null_aware_left_anti",
            nest_loop_surviving_side_plan(JoinKind::NullAwareLeftAnti),
            1,
        );
    }

    #[test]
    fn union_all_two_scans_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "union_all_two_scans",
            union_plan(
                true,
                aliased_scan_plan("l", 1, 2),
                aliased_scan_plan("r", 3, 4),
            ),
            1,
        );
    }

    #[test]
    fn union_distinct_two_scans_rejects_residual_distinct_before_ir_build() {
        assert_distributed_plan_error_contains(
            "union_distinct_two_scans",
            union_plan(
                false,
                aliased_scan_plan("l", 1, 2),
                aliased_scan_plan("r", 3, 4),
            ),
            "UNION DISTINCT must be rewritten by UnionDistinctToAggregate before distributed build",
        );
    }

    #[test]
    fn intersect_two_scans_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "intersect_two_scans",
            intersect_plan(aliased_scan_plan("l", 1, 2), aliased_scan_plan("r", 3, 4)),
            1,
        );
    }

    #[test]
    fn except_two_scans_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "except_two_scans",
            except_plan(aliased_scan_plan("l", 1, 2), aliased_scan_plan("r", 3, 4)),
            1,
        );
    }

    #[test]
    fn values_rows_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure("values_rows", values_rows_plan(), 1);
    }

    #[test]
    fn assert_one_row_over_scan_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "assert_one_row_over_scan",
            assert_one_row_plan(scan_plan()),
            1,
        );
    }

    #[test]
    fn decode_over_scan_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure("decode_over_scan", decode_over_scan_plan(), 1);
    }

    #[test]
    fn repeat_grouping_sets_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "repeat_grouping_sets",
            repeat_grouping_sets_plan(),
            1,
        );
    }

    #[test]
    fn window_row_number_over_scan_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "window_row_number_over_scan",
            window_row_number_over_scan_plan(),
            1,
        );
    }

    #[test]
    fn generate_series_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure("generate_series", generate_series_plan(), 1);
    }

    #[test]
    fn unnest_table_function_over_scan_builds_ir_fragment_structure() {
        assert_distributed_plan_ir_structure(
            "unnest_table_function_over_scan",
            unnest_table_function_over_scan_plan(),
            1,
        );
    }

    #[test]
    fn decode_output_expr_uses_materialized_string_slot() {
        let build = build_distributed_plan_only("decode_output_expr_slot", decode_over_scan_plan());
        assert_multi_fragment_ir_structure("decode_output_expr_slot", &build, 1);
        let root = fragment_by_id("decode_output_expr_slot", &build, build.root_fragment_id);
        let decode = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == crate::thrift::plan_nodes::TPlanNodeType::DECODE_NODE)
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
        assert_multi_fragment_ir_structure("set_op_child_output_order", &build, 1);
        let root = fragment_by_id("set_op_child_output_order", &build, build.root_fragment_id);
        let union = root.plan.nodes.first().expect("set op root");
        assert_eq!(
            union.node_type,
            crate::thrift::plan_nodes::TPlanNodeType::UNION_NODE
        );
        assert_eq!(union.num_children, 2);
        let first_expr = &union
            .union_node
            .as_ref()
            .expect("union payload")
            .result_expr_lists[0][0];
        assert_eq!(
            first_expr.nodes[0].node_type,
            crate::thrift::exprs::TExprNodeType::SLOT_REF,
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
    fn iceberg_data_file_scan_ranges_builds_ir_fragment_structure() {
        let mut connectors = ConnectorRegistry::new();
        connectors.register_scan_planner(Arc::new(IcebergConnectorScanPlanner::new()));
        let distributed = build_distributed_plan(
            "iceberg_data_file_scan_ranges",
            iceberg_data_file_scan_plan(),
            &connectors,
        );

        assert_non_empty_scan_ranges("iceberg_data_file_scan_ranges", &distributed);
        assert_multi_fragment_ir_structure("iceberg_data_file_scan_ranges", &distributed, 1);
    }

    #[test]
    fn mv_target_state_scan_builds_ir_fragment_structure() {
        let ctx = scan_refresh_context_for_test();
        let plan = target_state_scan_plan_for_context(&ctx);
        let mut connectors = ConnectorRegistry::new();
        connectors.register_scan_planner(Arc::new(IcebergConnectorScanPlanner::new()));
        let distributed = build_distributed_plan_with_mv_refresh_ctx(
            "mv_target_state_scan",
            plan,
            &connectors,
            Some(&ctx),
        );

        assert_multi_fragment_ir_structure("mv_target_state_scan", &distributed, 1);
    }

    #[test]
    fn mv_version_scan_builds_ir_fragment_structure() {
        let fixture = local_version_scan_fixture();
        let mut ctx = scan_refresh_context_for_test();
        ctx.base_catalog_entries = [("ice".to_string(), fixture.catalog_entry.clone())]
            .into_iter()
            .collect();
        let plan = version_scan_plan_for_fixture(&fixture);
        let mut connectors = ConnectorRegistry::new();
        connectors.register_scan_planner(Arc::new(IcebergConnectorScanPlanner::new()));
        let distributed = build_distributed_plan_with_mv_refresh_ctx(
            "mv_version_scan",
            plan,
            &connectors,
            Some(&ctx),
        );

        assert_multi_fragment_ir_structure("mv_version_scan", &distributed, 1);
    }

    #[test]
    fn iceberg_sink_builds_ir_fragment_structure() {
        let mut plan = values_plan_for_columns(vec![output_col(1, "id", DataType::Int32, false)]);
        let connectors = ConnectorRegistry::new();
        let mut sink_spec = crate::sql::planner::write_sink::test_support::simple_sink_spec();
        sink_spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::write_sink::test_support::single_bucket_partition_metadata_json(),
        );
        let catalog = DummyCatalog;
        prepare_bridge2_test_props(&mut plan);
        let dp = crate::sql::planner::optimizer_bridge::distributed::optimizer_physical_to_distributed_plan(
            &plan,
        )
        .expect("build DistributedPlan");
        let dp = crate::sql::planner::with_iceberg_write_sink(
            dp,
            crate::sql::planner::IcebergWriteFragmentSink {
                descriptor_database: "test_db".to_string(),
                spec: sink_spec,
                input: crate::sql::planner::IcebergWriteInputBinding::RootOutputByOrdinal,
            },
        )
        .expect("plan iceberg write sink");
        let distributed = PlanFragmentBuilder::build(FragmentBuildRequest::result(
            &dp,
            &catalog,
            &connectors,
            None,
        ))
        .expect("DistributedPlan iceberg sink build");

        let root = fragment_by_id("iceberg_sink", &distributed, distributed.root_fragment_id);
        assert_eq!(
            root.output_sink.type_,
            crate::thrift::data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
        );
        assert!(
            root.output_sink.iceberg_table_sink.is_some(),
            "iceberg_sink: root output sink must carry iceberg payload"
        );
        assert!(
            root.output_exprs
                .as_ref()
                .is_some_and(|exprs| !exprs.is_empty()),
            "iceberg_sink: root output exprs must be present"
        );
        assert_multi_fragment_ir_structure("iceberg_sink", &distributed, 1);
    }

    fn assert_distributed_plan_ir_structure(
        case_name: &str,
        plan: OptimizerPhysicalNode,
        expected_fragment_count: usize,
    ) {
        let connectors = ConnectorRegistry::new();
        let distributed = build_distributed_plan(case_name, plan, &connectors);
        assert_multi_fragment_ir_structure(case_name, &distributed, expected_fragment_count);
    }

    fn build_distributed_plan(
        case_name: &str,
        mut plan: OptimizerPhysicalNode,
        connectors: &ConnectorRegistry,
    ) -> MultiFragmentBuildResult {
        prepare_bridge2_test_props(&mut plan);
        let catalog = DummyCatalog;
        let dp =
            crate::sql::planner::optimizer_bridge::distributed::optimizer_physical_to_distributed_plan(
                &plan,
            )
            .unwrap_or_else(|err| panic!("{case_name}: DistributedPlan build failed: {err}"));
        PlanFragmentBuilder::build(FragmentBuildRequest::result(
            &dp, &catalog, connectors, None,
        ))
        .unwrap_or_else(|err| panic!("{case_name}: DistributedPlan build failed: {err}"))
    }

    fn build_distributed_plan_with_mv_refresh_ctx(
        case_name: &str,
        mut plan: OptimizerPhysicalNode,
        connectors: &ConnectorRegistry,
        mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    ) -> MultiFragmentBuildResult {
        prepare_bridge2_test_props(&mut plan);
        let catalog = DummyCatalog;
        let dp =
            crate::sql::planner::optimizer_bridge::distributed::optimizer_physical_to_distributed_plan(
                &plan,
            )
            .unwrap_or_else(|err| panic!("{case_name}: DistributedPlan build failed: {err}"));
        PlanFragmentBuilder::build(FragmentBuildRequest::result(
            &dp,
            &catalog,
            connectors,
            mv_refresh_ctx,
        ))
        .unwrap_or_else(|err| panic!("{case_name}: DistributedPlan build failed: {err}"))
    }

    fn build_distributed_plan_only(
        case_name: &str,
        mut plan: OptimizerPhysicalNode,
    ) -> MultiFragmentBuildResult {
        prepare_bridge2_test_props(&mut plan);
        let catalog = DummyCatalog;
        let connectors = ConnectorRegistry::new();
        let dp =
            crate::sql::planner::optimizer_bridge::distributed::optimizer_physical_to_distributed_plan(
                &plan,
            )
            .unwrap_or_else(|err| panic!("{case_name}: DistributedPlan build failed: {err}"));
        PlanFragmentBuilder::build(FragmentBuildRequest::result(
            &dp,
            &catalog,
            &connectors,
            None,
        ))
        .unwrap_or_else(|err| panic!("{case_name}: DistributedPlan build failed: {err}"))
    }

    fn assert_distributed_plan_error_contains(
        case_name: &str,
        mut plan: OptimizerPhysicalNode,
        expected: &str,
    ) {
        prepare_bridge2_test_props(&mut plan);
        let catalog = DummyCatalog;
        let connectors = ConnectorRegistry::new();
        let err = match crate::sql::planner::optimizer_bridge::distributed::optimizer_physical_to_distributed_plan(
            &plan,
        ) {
            Ok(dp) => match PlanFragmentBuilder::build(FragmentBuildRequest::result(
                &dp,
                &catalog,
                &connectors,
                None,
            )) {
                Ok(_) => panic!("{case_name}: DistributedPlan build unexpectedly succeeded"),
                Err(err) => err,
            },
            Err(err) => err,
        };
        assert!(
            err.contains(expected),
            "{case_name}: expected error to contain `{expected}`, got `{err}`"
        );
    }

    fn prepare_bridge2_test_props(node: &mut OptimizerPhysicalNode) {
        for child in &mut node.children {
            prepare_bridge2_test_props(child);
        }
        if let Operator::PhysicalHashJoin(join) = &node.op {
            node.execution_props.join_distribution =
                join_execution_distribution_for_test(join.distribution.clone());
        }
    }

    fn join_execution_distribution_for_test(
        distribution: JoinDistribution,
    ) -> Option<JoinExecutionDistribution> {
        match distribution {
            JoinDistribution::Broadcast => Some(JoinExecutionDistribution::Broadcast),
            JoinDistribution::Shuffle => Some(JoinExecutionDistribution::Partitioned),
            JoinDistribution::Colocate => Some(JoinExecutionDistribution::Colocate),
            JoinDistribution::Unknown => None,
        }
    }

    fn assert_multi_fragment_ir_structure(
        case_name: &str,
        result: &MultiFragmentBuildResult,
        expected_fragment_count: usize,
    ) {
        assert!(
            !result.fragment_results.is_empty(),
            "{case_name}: expected at least one fragment"
        );
        assert_eq!(
            result.fragment_results.len(),
            expected_fragment_count,
            "{case_name}: fragment count"
        );
        assert_eq!(
            result
                .fragment_results
                .iter()
                .filter(|fragment| fragment.fragment_id == result.root_fragment_id)
                .count(),
            1,
            "{case_name}: root fragment id must exist exactly once"
        );

        let fragment_ids = assert_unique_fragment_ids(case_name, result);
        let node_ids_by_fragment = result
            .fragment_results
            .iter()
            .map(|fragment| {
                (
                    fragment.fragment_id,
                    assert_fragment_ir_structure(case_name, fragment),
                )
            })
            .collect::<BTreeMap<_, _>>();

        assert_edges_well_formed(case_name, result, &fragment_ids, &node_ids_by_fragment);
        assert_boundary_schemas_well_formed(
            case_name,
            result,
            &fragment_ids,
            &node_ids_by_fragment,
        );
        assert_runtime_filters_well_formed(case_name, result, &fragment_ids, &node_ids_by_fragment);
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

    fn assert_unique_fragment_ids(
        case_name: &str,
        result: &MultiFragmentBuildResult,
    ) -> BTreeSet<FragmentId> {
        let mut ids = BTreeSet::new();
        for fragment in &result.fragment_results {
            assert!(
                ids.insert(fragment.fragment_id),
                "{case_name}: duplicate fragment id {}",
                fragment.fragment_id
            );
        }
        ids
    }

    fn assert_fragment_ir_structure(
        case_name: &str,
        fragment: &FragmentBuildResult,
    ) -> BTreeSet<i32> {
        let node_ids = assert_plan_node_ids_unique(
            &format!("{case_name}: fragment {}", fragment.fragment_id),
            &fragment.plan,
        );
        for (cte_id, exchange_node_id, _receive_producer_column_ids) in &fragment.cte_exchange_nodes
        {
            assert!(
                node_ids.contains(exchange_node_id),
                "{case_name}: fragment {} cte_id {} references missing exchange node {}",
                fragment.fragment_id,
                cte_id,
                exchange_node_id
            );
            assert_exchange_node(case_name, fragment, *exchange_node_id, "cte exchange node");
        }
        node_ids
    }

    fn assert_plan_node_ids_unique(
        case_name: &str,
        plan: &crate::thrift::plan_nodes::TPlan,
    ) -> BTreeSet<i32> {
        assert!(
            !plan.nodes.is_empty(),
            "{case_name}: normal fragment plan must contain nodes"
        );
        assert_plan_node_ids_follow_preorder(case_name, plan);
        let mut node_ids = BTreeSet::new();
        for node in &plan.nodes {
            assert!(
                node_ids.insert(node.node_id),
                "{case_name}: duplicate node id {}",
                node.node_id
            );
        }
        node_ids
    }

    fn assert_plan_node_ids_follow_preorder(
        case_name: &str,
        plan: &crate::thrift::plan_nodes::TPlan,
    ) {
        if plan.nodes.is_empty() {
            return;
        }

        let consumed = assert_plan_subtree_node_ids_follow_preorder(case_name, &plan.nodes, 0);
        assert_eq!(
            consumed,
            plan.nodes.len(),
            "{case_name}: TPlan nodes must contain exactly one pre-order tree"
        );
    }

    fn assert_plan_subtree_node_ids_follow_preorder(
        case_name: &str,
        nodes: &[crate::thrift::plan_nodes::TPlanNode],
        root_idx: usize,
    ) -> usize {
        let root = nodes
            .get(root_idx)
            .unwrap_or_else(|| panic!("{case_name}: missing TPlan subtree root at {root_idx}"));
        let mut next_idx = root_idx + 1;
        let mut previous_child_root_id = None;
        for child_ordinal in 0..root.num_children {
            let child = nodes.get(next_idx).unwrap_or_else(|| {
                panic!(
                    "{case_name}: node {} declares missing child {}",
                    root.node_id, child_ordinal
                )
            });
            assert!(
                root.node_id > child.node_id,
                "{case_name}: parent node id {} must be greater than child root id {} in TPlan pre-order",
                root.node_id,
                child.node_id
            );
            if let Some(previous_child_root_id) = previous_child_root_id {
                assert!(
                    previous_child_root_id < child.node_id,
                    "{case_name}: sibling child root node ids must increase, got {} before {} under parent {}",
                    previous_child_root_id,
                    child.node_id,
                    root.node_id
                );
            }
            previous_child_root_id = Some(child.node_id);
            next_idx = assert_plan_subtree_node_ids_follow_preorder(case_name, nodes, next_idx);
        }
        next_idx
    }

    fn assert_edges_well_formed(
        case_name: &str,
        result: &MultiFragmentBuildResult,
        fragment_ids: &BTreeSet<FragmentId>,
        node_ids_by_fragment: &BTreeMap<FragmentId, BTreeSet<i32>>,
    ) {
        let cte_id_by_fragment = result
            .fragment_results
            .iter()
            .filter_map(|fragment| fragment.cte_id.map(|cte_id| (fragment.fragment_id, cte_id)))
            .collect::<BTreeMap<_, _>>();
        for (idx, edge) in result.edges.iter().enumerate() {
            assert!(
                fragment_ids.contains(&edge.source_fragment_id),
                "{case_name}: edge {idx} references missing source fragment {}",
                edge.source_fragment_id
            );
            assert!(
                fragment_ids.contains(&edge.target_fragment_id),
                "{case_name}: edge {idx} references missing target fragment {}",
                edge.target_fragment_id
            );
            assert!(
                node_ids_by_fragment
                    .get(&edge.target_fragment_id)
                    .is_some_and(|node_ids| node_ids.contains(&edge.target_exchange_node_id)),
                "{case_name}: edge {idx} references missing target exchange node {} in fragment {}",
                edge.target_exchange_node_id,
                edge.target_fragment_id
            );
            let target = fragment_by_id(case_name, result, edge.target_fragment_id);
            assert_exchange_node(
                case_name,
                target,
                edge.target_exchange_node_id,
                "edge target",
            );
            match &edge.edge_kind {
                FragmentEdgeKind::Stream => assert_ne!(
                    edge.source_fragment_id, edge.target_fragment_id,
                    "{case_name}: stream edge {idx} must cross fragments"
                ),
                FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids,
                } => {
                    assert_eq!(
                        cte_id_by_fragment.get(&edge.source_fragment_id),
                        Some(cte_id),
                        "{case_name}: CTE edge {idx} source fragment must declare matching cte_id"
                    );
                    assert!(
                        target.cte_exchange_nodes.iter().any(
                            |(
                                target_cte_id,
                                exchange_node_id,
                                target_receive_producer_column_ids,
                            )| {
                                target_cte_id == cte_id
                                    && *exchange_node_id == edge.target_exchange_node_id
                                    && target_receive_producer_column_ids
                                        == receive_producer_column_ids
                            }
                        ),
                        "{case_name}: CTE edge {idx} target fragment must record exchange node and receive columns"
                    );
                }
                FragmentEdgeKind::IcebergChangeStreamRouter { .. } => {
                    // Task 2 only models the edge; execution wiring must reject it elsewhere.
                    assert_ne!(
                        edge.source_fragment_id, edge.target_fragment_id,
                        "{case_name}: Iceberg change-stream router edge {idx} must cross fragments"
                    );
                }
            }
        }
    }

    fn assert_exchange_node(
        case_name: &str,
        fragment: &FragmentBuildResult,
        node_id: i32,
        label: &str,
    ) {
        let node = fragment
            .plan
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .unwrap_or_else(|| {
                panic!(
                    "{case_name}: fragment {} {label} node {} not found",
                    fragment.fragment_id, node_id
                )
            });
        assert_eq!(
            node.node_type,
            crate::thrift::plan_nodes::TPlanNodeType::EXCHANGE_NODE,
            "{case_name}: fragment {} {label} node {} must be EXCHANGE_NODE",
            fragment.fragment_id,
            node_id
        );
    }

    fn assert_boundary_schemas_well_formed(
        case_name: &str,
        result: &MultiFragmentBuildResult,
        fragment_ids: &BTreeSet<FragmentId>,
        node_ids_by_fragment: &BTreeMap<FragmentId, BTreeSet<i32>>,
    ) {
        for (idx, boundary) in result.boundary_schemas.iter().enumerate() {
            if let Some(fragment_id) = boundary.fragment_id {
                let fragment_id = FragmentId::try_from(fragment_id)
                    .unwrap_or_else(|_| panic!("{case_name}: boundary {idx} negative fragment id"));
                assert!(
                    fragment_ids.contains(&fragment_id),
                    "{case_name}: boundary {idx} references missing fragment {fragment_id}"
                );
                if boundary.node_id >= 0 {
                    let node_in_fragment = node_ids_by_fragment
                        .get(&fragment_id)
                        .is_some_and(|node_ids| node_ids.contains(&boundary.node_id));
                    let sender_edge_exists = matches!(
                        boundary.boundary_kind,
                        crate::sql::codegen::boundary_schema::BoundaryKind::ExchangeSender
                    ) && result.edges.iter().any(|edge| {
                        edge.source_fragment_id == fragment_id
                            && edge.target_exchange_node_id == boundary.node_id
                    });
                    assert!(
                        node_in_fragment || sender_edge_exists,
                        "{case_name}: boundary {idx} references missing node {} in fragment {}",
                        boundary.node_id,
                        fragment_id
                    );
                }
            }
            assert!(
                !boundary.columns.is_empty(),
                "{case_name}: boundary {idx} should describe at least one column"
            );
        }
    }

    fn assert_runtime_filters_well_formed(
        case_name: &str,
        result: &MultiFragmentBuildResult,
        fragment_ids: &BTreeSet<FragmentId>,
        node_ids_by_fragment: &BTreeMap<FragmentId, BTreeSet<i32>>,
    ) {
        let Some(rf_plan) = result.rf_plan.as_ref() else {
            return;
        };
        for (fragment_id, filter_ids) in &rf_plan.build_side_filters {
            assert!(
                fragment_ids.contains(fragment_id),
                "{case_name}: runtime filter build side references missing fragment {fragment_id}"
            );
            for filter_id in filter_ids {
                assert!(
                    rf_plan.all_filters.contains_key(filter_id),
                    "{case_name}: build-side runtime filter {filter_id} missing descriptor"
                );
            }
        }
        for (fragment_id, probes) in &rf_plan.probe_side_filters {
            assert!(
                fragment_ids.contains(fragment_id),
                "{case_name}: runtime filter probe side references missing fragment {fragment_id}"
            );
            for (filter_id, probe_node_id) in probes {
                assert!(
                    rf_plan.all_filters.contains_key(filter_id),
                    "{case_name}: probe-side runtime filter {filter_id} missing descriptor"
                );
                assert!(
                    node_ids_by_fragment
                        .get(fragment_id)
                        .is_some_and(|node_ids| node_ids.contains(probe_node_id)),
                    "{case_name}: runtime filter {filter_id} references missing probe node {} in fragment {}",
                    probe_node_id,
                    fragment_id
                );
            }
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
            Err("IR structural tests use fully resolved metadata-table scans".to_string())
        }
    }

    fn scan_plan() -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let k = output_col(1, "k", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        physical_node_with_scalars(
            Operator::PhysicalScan(ScanOp {
                database: "test_db".to_string(),
                table: metadata_table_def(),
                alias: Some("t".to_string()),
                stats_ref: None,
                columns: vec![k.clone(), v.clone()],
                predicates: intern_exprs(
                    &mut scalars,
                    &[cmp_expr(
                        column_ref_expr(1, "k", DataType::Int64, false),
                        BinOp::Eq,
                        int_lit(7),
                    )],
                ),
                required_columns: Some(vec!["k".to_string(), "v".to_string()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k, v],
            scalars,
        )
    }

    fn iceberg_data_file_scan_plan() -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let k = output_col(1, "k", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        physical_node_with_scalars(
            Operator::PhysicalScan(ScanOp {
                database: "test_db".to_string(),
                table: iceberg_data_table_def(),
                alias: Some("t".to_string()),
                stats_ref: None,
                columns: vec![k.clone(), v.clone()],
                predicates: intern_exprs(
                    &mut scalars,
                    &[cmp_expr(
                        column_ref_expr(1, "k", DataType::Int64, false),
                        BinOp::Eq,
                        int_lit(7),
                    )],
                ),
                required_columns: Some(vec!["k".to_string(), "v".to_string()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k, v],
            scalars,
        )
    }

    fn values_plan_for_columns(columns: Vec<OutputColumn>) -> OptimizerPhysicalNode {
        physical_node(
            Operator::PhysicalValues(ValuesOp {
                rows: Vec::new(),
                columns: columns.clone(),
            }),
            Vec::new(),
            columns,
        )
    }

    fn target_state_scan_plan_for_context(
        ctx: &crate::engine::mv::refresh_context::IcebergMvRefreshContext,
    ) -> OptimizerPhysicalNode {
        let row_id = output_col(110, "__row_id__", DataType::Utf8, false);
        let region = output_col(111, "region", DataType::Utf8, true);
        let scan = target_state_scan_for_context(ctx);
        physical_node(
            Operator::PhysicalScan(ScanOp {
                database: "ns".to_string(),
                table: target_state_table_def(scan),
                alias: Some("mv".to_string()),
                stats_ref: None,
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
                nan_value_counts: HashMap::new(),
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

    fn version_scan_plan_for_fixture(fixture: &LocalVersionScanFixture) -> OptimizerPhysicalNode {
        let id = output_col(120, "id", DataType::Int64, false);
        physical_node(
            Operator::PhysicalScan(ScanOp {
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
                stats_ref: None,
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

    fn scan_refresh_context_for_test() -> crate::engine::mv::refresh_context::IcebergMvRefreshContext
    {
        mv_refresh_context_for_test(None)
    }

    fn mv_refresh_context_for_test(
        target_snapshot_id: Option<i64>,
    ) -> crate::engine::mv::refresh_context::IcebergMvRefreshContext {
        use iceberg::{NamespaceIdent, TableIdent};

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
        let warehouse_dir = tempfile::TempDir::new()
            .expect("target warehouse tempdir")
            .keep();
        let warehouse = format!("file://{}", warehouse_dir.join("warehouse").display());
        let target_entry = Arc::new(
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "tgt",
                &[
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse.clone()),
                ],
            )
            .expect("target entry"),
        );
        let iceberg_catalog: Arc<dyn iceberg::Catalog> = Arc::new(
            crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&target_entry)
                .expect("build hadoop catalog"),
        );
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            target_schema.as_ref().clone(),
            iceberg::spec::PartitionSpec::unpartition_spec().into_unbound(),
            iceberg::spec::SortOrder::unsorted_order(),
            format!("{warehouse}/target/orders_mv"),
            iceberg::spec::FormatVersion::V3,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let target_table = iceberg::table::Table::builder()
            .file_io(iceberg::io::FileIO::new_with_fs())
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

    fn filter_plan(child: OptimizerPhysicalNode) -> OptimizerPhysicalNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let output_columns = child.output_columns.clone();
        physical_node_with_scalars(
            Operator::PhysicalFilter(FilterOp {
                predicate: intern_exprs(
                    &mut scalars,
                    &[and_expr(
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
                    )],
                )[0],
            }),
            vec![child],
            output_columns,
            scalars,
        )
    }

    fn project_plan(child: OptimizerPhysicalNode) -> OptimizerPhysicalNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let output_columns = vec![output_col(101, "k_plus_one", DataType::Int64, false)];
        let items = vec![ProjectItem {
            expr: add_expr(column_ref_expr(1, "k", DataType::Int64, false), int_lit(1)),
            output_name: "k_plus_one".to_string(),
            output_column_id: ColumnId::new_for_test(101),
        }];
        physical_node_with_scalars(
            Operator::PhysicalProject(ProjectOp {
                items: intern_project_items(&mut scalars, &items),
                output_qualifier: None,
            }),
            vec![child],
            output_columns,
            scalars,
        )
    }

    fn sort_plan(child: OptimizerPhysicalNode) -> OptimizerPhysicalNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let sort_col = child.output_columns[0].clone();
        let output_columns = child.output_columns.clone();
        physical_node_with_scalars(
            Operator::PhysicalSort(SortOp {
                items: intern_sort_items(
                    &mut scalars,
                    &[SortItem {
                        expr: column_ref_expr(
                            sort_col.column_id.0,
                            &sort_col.name,
                            sort_col.data_type.clone(),
                            sort_col.nullable,
                        ),
                        asc: true,
                        nulls_first: false,
                    }],
                ),
                analytic_partition_exprs: vec![],
                partition_limit: None,
                topn_type: None,
            }),
            vec![child],
            output_columns,
            scalars,
        )
    }

    fn limit_plan(
        child: OptimizerPhysicalNode,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> OptimizerPhysicalNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalLimit(LimitOp { limit, offset }),
            vec![child],
            output_columns,
        )
    }

    fn top_n_plan(
        child: OptimizerPhysicalNode,
        phase: TopNPhase,
        is_split: bool,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> OptimizerPhysicalNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let sort_col = child.output_columns[0].clone();
        let output_columns = child.output_columns.clone();
        physical_node_with_scalars(
            Operator::PhysicalTopN(TopNOp {
                items: intern_sort_items(
                    &mut scalars,
                    &[SortItem {
                        expr: column_ref_expr(
                            sort_col.column_id.0,
                            &sort_col.name,
                            sort_col.data_type.clone(),
                            sort_col.nullable,
                        ),
                        asc: true,
                        nulls_first: false,
                    }],
                ),
                limit,
                offset,
                phase,
                is_split,
            }),
            vec![child],
            output_columns,
            scalars,
        )
    }

    fn aggregate_group_by_plan(child: OptimizerPhysicalNode) -> OptimizerPhysicalNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let k = output_col(1, "k", DataType::Int64, false);
        physical_node_with_scalars(
            Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: intern_exprs(
                    &mut scalars,
                    &[column_ref_expr(1, "k", DataType::Int64, false)],
                ),
                aggregates: vec![],
                output_layout: AggregateOutputLayout::new(vec![k.clone()], vec![]),
                output_columns: vec![k.clone()],
                is_merge: vec![],
            }),
            vec![child],
            vec![k],
            scalars,
        )
    }

    fn aggregate_count_plan(child: OptimizerPhysicalNode) -> OptimizerPhysicalNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let k = output_col(1, "k", DataType::Int64, false);
        let count = output_col(201, "count(*)", DataType::Int64, true);
        let aggregate_calls = vec![AggregateCall {
            name: "count".to_string(),
            args: vec![],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: ColumnId::new_for_test(201),
        }];
        physical_node_with_scalars(
            Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: intern_exprs(
                    &mut scalars,
                    &[column_ref_expr(1, "k", DataType::Int64, false)],
                ),
                aggregates: intern_aggregate_calls(&mut scalars, &aggregate_calls),
                output_layout: AggregateOutputLayout::new(vec![k.clone()], vec![count.clone()]),
                output_columns: vec![k.clone(), count.clone()],
                is_merge: vec![false],
            }),
            vec![child],
            vec![k, count],
            scalars,
        )
    }

    fn root_gather_plan(child: OptimizerPhysicalNode) -> OptimizerPhysicalNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Gather,
            }),
            vec![child],
            output_columns,
        )
    }

    fn distribution_plan(
        child: OptimizerPhysicalNode,
        spec: DistributionSpec,
    ) -> OptimizerPhysicalNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalDistribution(PhysicalDistributionOp { spec }),
            vec![child],
            output_columns,
        )
    }

    fn cte_produce_consume_plan() -> OptimizerPhysicalNode {
        let cte_id = 7;
        let produce = single_column_scan_plan(output_col(1101, "k", DataType::Int64, false));
        let produce_output_columns = produce.output_columns.clone();
        let consume_output_columns = vec![output_col(1102, "k", DataType::Int64, false)];
        let produce = physical_node(
            Operator::PhysicalCTEProduce(CTEProduceOp {
                cte_id,
                output_columns: produce_output_columns.clone(),
            }),
            vec![produce],
            produce_output_columns,
        );
        let consume = physical_node(
            Operator::PhysicalCTEConsume(CTEConsumeOp {
                cte_id,
                alias: "cte".to_string(),
                output_columns: consume_output_columns.clone(),
                producer_column_ids: vec![ColumnId::new_for_test(1101)],
            }),
            vec![],
            consume_output_columns.clone(),
        );
        physical_node(
            Operator::PhysicalCTEAnchor(CTEAnchorOp { cte_id }),
            vec![produce, consume],
            consume_output_columns,
        )
    }

    fn inner_hash_join_two_scans_plan() -> OptimizerPhysicalNode {
        let (mut join, _, _) = hash_join_plan(JoinKind::Inner);
        let Operator::PhysicalHashJoin(hash_join) = &join.op else {
            panic!("expected hash join");
        };
        let left_key = hash_join.eq_conditions[0].left;
        let right_key = hash_join.eq_conditions[0].right;
        join.children[0].probe_runtime_filters = vec![RuntimeFilterProbe {
            filter_id: 7,
            probe_expr: left_key,
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

    fn broadcast_join_exchange_plan() -> OptimizerPhysicalNode {
        let left = aliased_scan_plan("l", 1, 2);
        let right = distribution_plan(aliased_scan_plan("r", 3, 4), DistributionSpec::Broadcast);
        let left_key = column_ref_expr_with_qualifier(1, "l", "k", DataType::Int64, false);
        let right_key = column_ref_expr_with_qualifier(3, "r", "k", DataType::Int64, false);
        let output_columns = join_output_columns(&left, &right, JoinOutput::Both);
        let mut scalars = scalars_from_children(&[left.clone(), right.clone()]);
        let scalar_left_key = intern_exprs(&mut scalars, std::slice::from_ref(&left_key))[0];
        let scalar_right_key = intern_exprs(&mut scalars, std::slice::from_ref(&right_key))[0];
        physical_node_with_scalars(
            Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: scalar_left_key,
                    right: scalar_right_key,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            vec![left, right],
            output_columns,
            scalars,
        )
    }

    fn two_sided_shuffle_join_exchange_plan() -> OptimizerPhysicalNode {
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
        let mut scalars = scalars_from_children(&[left.clone(), right.clone()]);
        let scalar_left_key = intern_exprs(&mut scalars, std::slice::from_ref(&left_key))[0];
        let scalar_right_key = intern_exprs(&mut scalars, std::slice::from_ref(&right_key))[0];
        physical_node_with_scalars(
            Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: scalar_left_key,
                    right: scalar_right_key,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Shuffle,
            }),
            vec![left, right],
            output_columns,
            scalars,
        )
    }

    fn left_outer_hash_join_plan() -> OptimizerPhysicalNode {
        let (join, _, _) = hash_join_plan(JoinKind::LeftOuter);
        join
    }

    fn hash_join_plan(join_type: JoinKind) -> (OptimizerPhysicalNode, TypedExpr, TypedExpr) {
        hash_join_plan_with_options(join_type, None, JoinOutput::Both)
    }

    fn hash_join_other_condition_plan() -> OptimizerPhysicalNode {
        let left_value = column_ref_expr_with_qualifier(2, "l", "v", DataType::Int64, true);
        let right_value = column_ref_expr_with_qualifier(4, "r", "v", DataType::Int64, true);
        let other_condition = cmp_expr(left_value, BinOp::Gt, right_value);
        let (join, _, _) =
            hash_join_plan_with_options(JoinKind::Inner, Some(other_condition), JoinOutput::Both);
        join
    }

    fn hash_join_surviving_side_plan(join_type: JoinKind) -> OptimizerPhysicalNode {
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
    ) -> (OptimizerPhysicalNode, TypedExpr, TypedExpr) {
        let left = aliased_scan_plan("l", 1, 2);
        let right = aliased_scan_plan("r", 3, 4);
        let left_key = column_ref_expr_with_qualifier(1, "l", "k", DataType::Int64, false);
        let right_key = column_ref_expr_with_qualifier(3, "r", "k", DataType::Int64, false);
        let output_columns = join_output_columns(&left, &right, output);
        let mut scalars = scalars_from_children(&[left.clone(), right.clone()]);
        let scalar_left_key = intern_exprs(&mut scalars, std::slice::from_ref(&left_key))[0];
        let scalar_right_key = intern_exprs(&mut scalars, std::slice::from_ref(&right_key))[0];
        let other_condition_id = other_condition
            .as_ref()
            .map(|expr| intern_exprs(&mut scalars, std::slice::from_ref(expr))[0]);
        let node = physical_node_with_scalars(
            Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: scalar_left_key,
                    right: scalar_right_key,
                    null_safe: false,
                }],
                other_condition: other_condition_id,
                distribution: JoinDistribution::Broadcast,
            }),
            vec![left, right],
            output_columns,
            scalars,
        );
        (node, left_key, right_key)
    }

    fn join_output_columns(
        left: &OptimizerPhysicalNode,
        right: &OptimizerPhysicalNode,
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

    fn nest_loop_cross_join_plan() -> OptimizerPhysicalNode {
        nest_loop_plan(JoinKind::Cross, None, JoinOutput::Both)
    }

    fn nest_loop_condition_plan(join_type: JoinKind) -> OptimizerPhysicalNode {
        let left_value = column_ref_expr_with_qualifier(2, "l", "v", DataType::Int64, true);
        let right_value = column_ref_expr_with_qualifier(4, "r", "v", DataType::Int64, true);
        nest_loop_plan(
            join_type,
            Some(cmp_expr(left_value, BinOp::Gt, right_value)),
            JoinOutput::Both,
        )
    }

    fn nest_loop_surviving_side_plan(join_type: JoinKind) -> OptimizerPhysicalNode {
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
    ) -> OptimizerPhysicalNode {
        let left = aliased_scan_plan("l", 1, 2);
        let right = aliased_scan_plan("r", 3, 4);
        let output_columns = join_output_columns(&left, &right, output);
        let mut scalars = scalars_from_children(&[left.clone(), right.clone()]);
        let condition = condition
            .as_ref()
            .map(|expr| intern_exprs(&mut scalars, std::slice::from_ref(expr))[0]);
        physical_node_with_scalars(
            Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
                join_type,
                condition,
            }),
            vec![left, right],
            output_columns,
            scalars,
        )
    }

    fn union_plan(
        all: bool,
        left: OptimizerPhysicalNode,
        right: OptimizerPhysicalNode,
    ) -> OptimizerPhysicalNode {
        let output_columns = set_op_output_columns();
        physical_node(
            Operator::PhysicalUnion(UnionOp {
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

    fn intersect_plan(
        left: OptimizerPhysicalNode,
        right: OptimizerPhysicalNode,
    ) -> OptimizerPhysicalNode {
        let output_columns = set_op_output_columns();
        physical_node(
            Operator::PhysicalIntersect(IntersectOp {
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

    fn except_plan(
        left: OptimizerPhysicalNode,
        right: OptimizerPhysicalNode,
    ) -> OptimizerPhysicalNode {
        let output_columns = set_op_output_columns();
        physical_node(
            Operator::PhysicalExcept(ExceptOp {
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

    fn values_rows_plan() -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let k = output_col(601, "k", DataType::Int64, false);
        let v = output_col(602, "v", DataType::Int64, true);
        let rows = [
            vec![int_lit(1), int_lit(10)],
            vec![int_lit(2), null_int_lit()],
        ];
        physical_node_with_scalars(
            Operator::PhysicalValues(ValuesOp {
                rows: rows
                    .iter()
                    .map(|row| intern_exprs(&mut scalars, row))
                    .collect(),
                columns: vec![k.clone(), v.clone()],
            }),
            vec![],
            vec![k, v],
            scalars,
        )
    }

    fn bad_values_row_length_plan() -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let k = output_col(611, "k", DataType::Int64, false);
        let v = output_col(612, "v", DataType::Int64, true);
        let row = vec![int_lit(1)];
        physical_node_with_scalars(
            Operator::PhysicalValues(ValuesOp {
                rows: vec![intern_exprs(&mut scalars, &row)],
                columns: vec![k.clone(), v.clone()],
            }),
            vec![],
            vec![k, v],
            scalars,
        )
    }

    fn reordered_union_values_plan() -> OptimizerPhysicalNode {
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
            Operator::PhysicalUnion(UnionOp {
                all: true,
                output_columns: output_columns.clone(),
                child_output_columns,
            }),
            vec![left, right],
            output_columns,
        )
    }

    fn two_column_values_plan(k_id: u32, s_id: u32) -> OptimizerPhysicalNode {
        let k = output_col(k_id, "k", DataType::Int64, false);
        let s = output_col(s_id, "s", DataType::Utf8, true);
        physical_node(
            Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![k.clone(), s.clone()],
            }),
            vec![],
            vec![k, s],
        )
    }

    fn assert_one_row_plan(child: OptimizerPhysicalNode) -> OptimizerPhysicalNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalAssertOneRow(AssertOneRowOp {
                subquery_text: "select k from t".to_string(),
            }),
            vec![child],
            output_columns,
        )
    }

    fn decode_over_scan_plan() -> OptimizerPhysicalNode {
        let source_id = ColumnId::new_for_test(701);
        let output_id = ColumnId::new_for_test(702);
        let scan = physical_node(
            Operator::PhysicalScan(ScanOp {
                database: "test_db".to_string(),
                table: dict_metadata_table_def(),
                alias: Some("t".to_string()),
                stats_ref: None,
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
            Operator::PhysicalDecode(DecodeOp {
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

    fn repeat_grouping_sets_plan() -> OptimizerPhysicalNode {
        let k = output_col(801, "k", DataType::Int64, false);
        let grouping_col = output_col(802, "__grouping_fn_0", DataType::Int64, false);
        physical_node(
            Operator::PhysicalRepeat(RepeatOp {
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

    fn window_row_number_over_scan_plan() -> OptimizerPhysicalNode {
        let k = output_col(901, "k", DataType::Int64, false);
        let child = single_column_scan_plan(k.clone());
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let k = output_col(901, "k", DataType::Int64, false);
        let row_number = output_col(902, "rn", DataType::Int64, false);
        let window_exprs = vec![WindowExpr {
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
        }];
        physical_node_with_scalars(
            Operator::PhysicalWindow(WindowOp {
                window_exprs: intern_window_exprs(&mut scalars, &window_exprs),
                output_columns: vec![k.clone(), row_number.clone()],
            }),
            vec![child],
            vec![k, row_number],
            scalars,
        )
    }

    fn generate_series_plan() -> OptimizerPhysicalNode {
        let value = output_col(911, "value", DataType::Int64, false);
        physical_node(
            Operator::PhysicalGenerateSeries(GenerateSeriesOp {
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

    fn unnest_table_function_over_scan_plan() -> OptimizerPhysicalNode {
        let array_type = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int64,
            true,
        )));
        let arr = output_col(921, "arr", array_type.clone(), true);
        let item = output_col(922, "item", DataType::Int64, true);
        let child = single_column_scan_plan(arr.clone());
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        physical_node_with_scalars(
            Operator::PhysicalTableFunction(TableFunctionOp {
                function_name: "unnest".to_string(),
                args: intern_exprs(
                    &mut scalars,
                    &[column_ref_expr(921, "arr", array_type, true)],
                ),
                output_columns: vec![item.clone()],
                alias: Some("u".to_string()),
                is_left_join: false,
            }),
            vec![child],
            vec![arr, item],
            scalars,
        )
    }

    fn aliased_scan_plan(alias: &str, key_id: u32, value_id: u32) -> OptimizerPhysicalNode {
        let k = output_col(key_id, "k", DataType::Int64, false);
        let v = output_col(value_id, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalScan(ScanOp {
                database: "test_db".to_string(),
                table: metadata_table_def(),
                alias: Some(alias.to_string()),
                stats_ref: None,
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

    fn single_column_scan_plan(column: OutputColumn) -> OptimizerPhysicalNode {
        physical_node(
            Operator::PhysicalScan(ScanOp {
                database: "test_db".to_string(),
                table: single_column_metadata_table_def(&column),
                alias: Some("t".to_string()),
                stats_ref: None,
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
        children: Vec<OptimizerPhysicalNode>,
        output_columns: Vec<OutputColumn>,
    ) -> OptimizerPhysicalNode {
        let scalars = scalars_from_children(&children);
        physical_node_with_scalars(op, children, output_columns, scalars)
    }

    fn physical_node_with_scalars(
        op: Operator,
        children: Vec<OptimizerPhysicalNode>,
        output_columns: Vec<OutputColumn>,
        scalars: ScalarArena,
    ) -> OptimizerPhysicalNode {
        let mut plan = OptimizerPhysicalNode {
            op,
            children,
            stats: Statistics::default(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn scalars_from_children(children: &[OptimizerPhysicalNode]) -> ScalarArena {
        children
            .iter()
            .find_map(|child| child.execution_props.scalar_arena.as_deref().cloned())
            .unwrap_or_else(ScalarArena::new)
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
