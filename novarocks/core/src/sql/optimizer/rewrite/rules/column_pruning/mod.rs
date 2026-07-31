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

//! Phase 2 per-operator column pruning rules. Each reads its node's
//! `required_output_columns` (set by the Phase-1 TagRequiredColumns pass)
//! and prunes that node's own output metadata. None ⇒ no-op (keep all).

pub(crate) mod prune_aggregate;
pub(crate) mod prune_cte_anchor;
pub(crate) mod prune_cte_consume;
pub(crate) mod prune_cte_produce;
pub(crate) mod prune_except;
pub(crate) mod prune_filter;
pub(crate) mod prune_intersect;
pub(crate) mod prune_join;
pub(crate) mod prune_limit;
pub(crate) mod prune_project;
pub(crate) mod prune_repeat;
pub(crate) mod prune_scan;
pub(crate) mod prune_sort;
pub(crate) mod prune_table_function;
pub(crate) mod prune_union;
pub(crate) mod prune_window;

use std::collections::HashSet;

use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

/// Returns all 16 per-operator Phase-2 column pruning rules.
///
/// These rules consume the `required_output_columns` tags written by the
/// Phase-1 `TagRequiredColumns` pass and prune each operator's output columns
/// accordingly.
pub(crate) fn all_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![
        Box::new(prune_scan::PruneScanColumns),
        Box::new(prune_project::PruneProjectColumns),
        Box::new(prune_filter::PruneFilterColumns),
        Box::new(prune_aggregate::PruneAggregateColumns),
        Box::new(prune_join::PruneJoinColumns),
        Box::new(prune_sort::PruneSortColumns),
        Box::new(prune_limit::PruneLimitColumns),
        Box::new(prune_window::PruneWindowColumns),
        Box::new(prune_union::PruneUnionColumns),
        Box::new(prune_intersect::PruneIntersectColumns),
        Box::new(prune_except::PruneExceptColumns),
        Box::new(prune_cte_anchor::PruneCTEAnchorColumns),
        Box::new(prune_cte_consume::PruneCTEConsumeColumns),
        Box::new(prune_cte_produce::PruneCTEProduceColumns),
        Box::new(prune_repeat::PruneRepeatColumns),
        Box::new(prune_table_function::PruneTableFunctionColumns),
    ]
}

/// When pruning would leave a Project with zero items, mint a placeholder
/// constant column so the operator still has a valid output. Mirrors
/// StarRocks' `Utils.findSmallestColumnRef` / `ConstantOperator.createTinyInt`
/// auto-fill behavior.
///
/// Returns `None` when no factory is available in context (rules that do
/// not have a factory set will fall back to "keep first original column"
/// instead of minting).
pub(crate) fn auto_fill_column_id(ctx: &mut RewriteContext) -> Option<ColumnId> {
    let factory = ctx.column_ref_factory()?;
    // We need a mutable borrow of the factory to call create().
    // Use RefCell::try_borrow_mut which won't panic in the common case.
    let id = factory
        .try_borrow_mut()
        .ok()
        .map(|mut f: std::cell::RefMut<ColumnRefFactory>| {
            f.create(
                None,
                "auto_fill".to_string(),
                arrow::datatypes::DataType::Int8,
                false,
            )
        })?;
    Some(id)
}

/// Keep at least one column from `output_columns` by returning the
/// first column's id when the filtered set would be empty.
///
/// For nodes that use `output_columns: Vec<OutputColumn>` (not Project items),
/// "keep first original" is simpler and safer than minting a fresh id.
pub(crate) fn keep_at_least_one(
    filtered: HashSet<ColumnId>,
    fallback_id: ColumnId,
) -> HashSet<ColumnId> {
    if filtered.is_empty() {
        let mut s = HashSet::new();
        s.insert(fallback_id);
        s
    } else {
        filtered
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::logical::*;
    use crate::sql::planner::payload::*;
    // Pipeline-level integration tests that verify the Phase-1 (TagRequiredColumns)
    // + Phase-2 (v2 Prune* rules) combination sets `Scan.required_columns` correctly.
    //
    // These tests mirror the behavioral coverage of the four tests that lived in
    // the old `column_pruning.rs` before the v1→v2 switch.

    use std::collections::HashMap;

    use arrow::datatypes::DataType;

    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::logical::*;
    use crate::sql::planner::optimizer_bridge::logical::{to_logical_plan, to_optimizer_expr};
    use crate::sql::planner::payload::*;

    // -----------------------------------------------------------------------
    // Helper builders
    // -----------------------------------------------------------------------

    fn col_def(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn output_col(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_ref(id: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn make_scan(cols: &[(ColumnId, &str)]) -> LogicalPlanNode {
        let table = TableDef {
            name: "t1".to_string(),
            columns: cols.iter().map(|(_, name)| col_def(name)).collect(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        };
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "default".to_string(),
                table: table,
                alias: None,
                columns: cols
                    .iter()
                    .map(|(id, name)| output_col(*id, name))
                    .collect(),
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn run_pipeline(plan: LogicalPlanNode) -> LogicalPlanNode {
        let pipeline = query_rewrite_pipeline();
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_query_stats_input(
            crate::sql::optimizer::stats_input::OptimizerStatsInput::from_test_table_statistics(
                &HashMap::new(),
            ),
        );
        let mut scalars = ScalarArena::new();
        let opt_plan = to_optimizer_expr(&plan, &mut scalars);
        let arena_rc = Rc::new(RefCell::new(scalars));
        ctx.set_scalar_arena(arena_rc.clone());
        let opt_result = pipeline.rewrite(opt_plan, &mut ctx).unwrap();
        let arena = arena_rc.borrow();
        to_logical_plan(opt_result, &arena)
    }

    fn extract_scan(plan: &LogicalPlanNode) -> &PlanScanNode {
        // Walk down through Project/Filter/Aggregate to reach the Scan leaf.
        match &plan.kind {
            LogicalPlanKind::Scan(s) => s,
            LogicalPlanKind::Project(_)
            | LogicalPlanKind::Filter(_)
            | LogicalPlanKind::Aggregate(_) => extract_scan(plan.unary_input()),
            _ => panic!("unexpected plan node, expected to find Scan"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 1 (ported from old column_pruning.rs):
    //   A bare Scan at root (no parent) keeps all columns — required_columns is None.
    // -----------------------------------------------------------------------

    #[test]
    fn root_scan_without_parent_keeps_all_columns() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let plan = make_scan(&[(id_a, "a"), (id_b, "b"), (id_c, "c")]);
        let result = run_pipeline(plan);

        let LogicalPlanKind::Scan(s) = &result.kind else {
            panic!("expected Scan at root");
        };
        // Phase-1 tags all columns when there is no parent restriction; Phase-2
        // PruneScanColumns then writes required_columns with all three names —
        // keeping every column is the correct behavior.
        let req = s
            .required_columns
            .as_ref()
            .expect("required_columns must be set after pipeline");
        let req_set: std::collections::HashSet<&str> = req.iter().map(|s| s.as_str()).collect();
        assert!(req_set.contains("a"), "a must be kept");
        assert!(req_set.contains("b"), "b must be kept");
        assert!(req_set.contains("c"), "c must be kept");
    }

    // -----------------------------------------------------------------------
    // Test 2 (ported from old column_pruning.rs):
    //   Project[a] → Scan[a,b,c] — after pipeline, Scan.required_columns = ["a"]
    // -----------------------------------------------------------------------

    #[test]
    fn project_selecting_one_col_prunes_scan_required_columns() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);
        // output of the Project expression
        let out_a = ColumnId::new_for_test(101);

        let scan = make_scan(&[(id_a, "a"), (id_b, "b"), (id_c, "c")]);
        let project = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: col_ref(id_a, "a"),
                    output_name: "a".to_string(),
                    output_column_id: out_a,
                }],
                output_qualifier: None,
            }),
            vec![scan],
            None,
        );

        let result = run_pipeline(project);
        let scan_node = extract_scan(&result);
        let req = scan_node
            .required_columns
            .as_ref()
            .expect("required_columns must be set");
        assert_eq!(req.len(), 1, "only 'a' should survive pruning");
        assert_eq!(req[0], "a");
    }

    // -----------------------------------------------------------------------
    // Test 3 (ported from old column_pruning.rs):
    //   Project[a] → Filter[b = 'x'] → Scan[a,b,c]
    //   After pipeline: Scan.required_columns contains both "a" and "b"
    // -----------------------------------------------------------------------

    #[test]
    fn filter_predicate_columns_are_preserved_in_scan_required() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);
        let out_a = ColumnId::new_for_test(101);

        let scan = make_scan(&[(id_a, "a"), (id_b, "b"), (id_c, "c")]);
        let filter = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col_ref(id_b, "b")),
                        op: BinOp::Eq,
                        right: Box::new(TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::String("x".to_string())),
                            data_type: DataType::Utf8,
                            nullable: false,
                        }),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![scan],
            None,
        );
        let project = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: col_ref(id_a, "a"),
                    output_name: "a".to_string(),
                    output_column_id: out_a,
                }],
                output_qualifier: None,
            }),
            vec![filter],
            None,
        );

        let result = run_pipeline(project);
        let scan_node = extract_scan(&result);
        let req = scan_node
            .required_columns
            .as_ref()
            .expect("required_columns must be set");
        let req_set: std::collections::HashSet<&str> = req.iter().map(|s| s.as_str()).collect();
        assert!(req_set.contains("a"), "a must be kept (projected)");
        assert!(
            req_set.contains("b"),
            "b must be kept (predicate reference)"
        );
        assert!(!req_set.contains("c"), "c must be pruned");
    }

    // -----------------------------------------------------------------------
    // Test 4 (ported from old column_pruning.rs):
    //   Aggregate[group_by=[b], sum(c)] → Scan[a,b,c]
    //   After pipeline:
    //   - When Aggregate is the root (parent_needed=None), tag_aggregate
    //     propagates None to the child, so Scan keeps all columns
    //     (required_columns=None) — pruning is not possible without a parent constraint.
    //   - When a Project wraps the Aggregate and selects only some outputs,
    //     the Project provides a non-None parent_needed which tag_aggregate
    //     receives as Some(_).  In that case Task 3 requires only group-by
    //     inputs and selected aggregate inputs to remain needed, so selecting
    //     only the group key prunes the aggregate arg from Scan.
    // -----------------------------------------------------------------------

    #[test]
    fn aggregate_group_key_prunes_unselected_agg_arg_from_scan() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);
        // Aggregate output columns: group_by result b@out_b, agg result sum_c@out_sum
        let out_b = ColumnId::new_for_test(201);
        let out_sum = ColumnId::new_for_test(202);

        let scan = make_scan(&[(id_a, "a"), (id_b, "b"), (id_c, "c")]);
        let agg = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(id_b, "b")],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_ref(id_c, "c")],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: out_sum,
                }],
                output_columns: vec![
                    OutputColumn {
                        column_id: out_b,
                        name: "b".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: out_sum,
                        name: "sum_c".to_string(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                ],
                already_pushed: false,
            }),
            vec![scan],
            None,
        );

        // Wrap in a Project that selects only out_b (b) so tag_project provides
        // a non-None parent_needed to tag_aggregate. tag_aggregate then passes
        // only the selected group-key dependency {b@2} to the Scan.
        let proj = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    output_column_id: ColumnId::new_for_test(901),
                    output_name: "b".to_string(),
                    expr: col_ref(out_b, "b"),
                }],
                output_qualifier: None,
            }),
            vec![agg],
            None,
        );

        let result = run_pipeline(proj);
        let scan_node = extract_scan(&result);
        let req = scan_node
            .required_columns
            .as_ref()
            .expect("required_columns must be set");
        let req_set: std::collections::HashSet<&str> = req.iter().map(|s| s.as_str()).collect();
        assert!(req_set.contains("b"), "b must be kept (group_by)");
        assert!(
            !req_set.contains("c"),
            "c must be pruned (unselected aggregate arg)"
        );
        assert!(
            !req_set.contains("a"),
            "a must be pruned (not referenced by selected group key)"
        );
    }
}
