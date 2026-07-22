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

//! Pushes the root `ImvDelta` marker down through unary Project/Filter nodes
//! so it directly wraps the leaf Scan, where `BindIcebergScanRule` can bind it.
//!
//! Delta commutes with projection and filtering (a row's insert/delete action
//! is preserved through column projection and row filtering), so
//! `Delta(Project(x)) == Project(Delta(x))` and `Delta(Filter(x)) == Filter(Delta(x))`.
//! Delta does NOT commute with Aggregate/Union; unsupported shapes fail-fast
//! here unless an earlier rewrite consumed them. Join is handled by
//! `RewriteJoinDeltaRule` in the same stage's fixpoint.

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result};
use crate::sql::planner::logical::{LogicalImvDeltaNode, LogicalPlanKind, LogicalPlanNode};

pub(crate) struct PushDeltaThroughUnaryRule;

impl LogicalRewriteRule for PushDeltaThroughUnaryRule {
    fn name(&self) -> &'static str {
        "PushDeltaThroughUnary"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        if !matches!(&expr.op, Operator::LogicalImvDelta(_)) {
            return false;
        }
        if expr.children.is_empty() {
            return false;
        }
        matches!(
            &expr.children[0].op,
            Operator::LogicalProject(_)
                | Operator::LogicalFilter(_)
                | Operator::LogicalAggregate(_)
                | Operator::LogicalJoin(_)
                | Operator::LogicalUnion(_)
        )
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        bridge_apply_result(expr, ctx, |plan, _ctx| apply_plan(plan))
    }
}

fn apply_plan(plan: LogicalPlanNode) -> Result<PlanRewriteResult, String> {
    let LogicalPlanNode {
        kind,
        mut children,
        required_output_columns: _,
    } = plan;
    let LogicalPlanKind::ImvDelta(delta) = &kind else {
        return Ok(PlanRewriteResult::Unchanged);
    };
    if children.len() != 1 {
        return Ok(PlanRewriteResult::Unchanged);
    }
    let child = children.remove(0);
    // Decide based on the child kind WITHOUT consuming `delta` yet. This
    // two-phase structure avoids both (a) moving the child before we
    // know how to handle the child and (b) rebuilding an identical marker
    // for an unhandled child, which would loop forever under fixpoint.
    // Fail-fast on unsupported shapes here (structural stage) is the first of
    // three layers; PropagateActionColumnRule and ActionColumnValidationRule
    // re-assert the same boundary later with richer diagnostics (base FQN).
    match &child.kind {
        LogicalPlanKind::Project(_) | LogicalPlanKind::Filter(_) => { /* fall through to push */ }
        LogicalPlanKind::Aggregate(_) => {
            return Err("Iceberg IMV rewrite does not support this aggregate shape".to_string());
        }
        LogicalPlanKind::Join(_) => {
            // Left for RewriteJoinDeltaRule in the same stage's fixpoint.
            return Ok(PlanRewriteResult::Unchanged);
        }
        LogicalPlanKind::Union(_) => {
            return Err("Iceberg IMV rewrite does not support this union shape".to_string());
        }
        // Scan or any other shape: the marker already directly wraps a leaf
        // (or a node we do not push through). Leave it for BindIcebergScan.
        _ => return Ok(PlanRewriteResult::Unchanged),
    }

    // The relocated marker is no longer at the structural plan root, so it is
    // is_root: false. WrapRootInImvDelta (the only is_root reader) has already
    // run in the earlier imv-delta-marker stage and never re-runs.
    let action_column = delta.action_column;
    let LogicalPlanNode {
        kind: child_kind,
        children: mut child_children,
        required_output_columns,
    } = child;
    if child_children.len() != 1 {
        return Ok(PlanRewriteResult::Unchanged);
    }
    let original_input = child_children.remove(0);
    match child_kind {
        LogicalPlanKind::Project(p) => {
            // Commutation Delta(Project(x)) == Project(Delta(x)) holds because
            // Project items are row-local and the delta only marks each row's
            // change action (carried through by action-column propagation).
            // Window calls cannot appear here because the planner extracts
            // them into a dedicated PlanWindowNode.
            let inner = LogicalPlanNode::new(
                LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                    is_root: false,
                    action_column,
                    branch_scope: None,
                }),
                vec![original_input],
                None,
            );
            Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                LogicalPlanKind::Project(p),
                vec![inner],
                required_output_columns,
            )))
        }
        LogicalPlanKind::Filter(f) => {
            let inner = LogicalPlanNode::new(
                LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                    is_root: false,
                    action_column,
                    branch_scope: None,
                }),
                vec![original_input],
                None,
            );
            Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                LogicalPlanKind::Filter(f),
                vec![inner],
                required_output_columns,
            )))
        }
        // The decision match above guarantees the child is Project or
        // Filter at this point; every other shape returned early.
        _ => unreachable!("child kind already filtered to Project/Filter"),
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::logical::*;
    use crate::sql::planner::payload::*;
    use std::collections::BTreeMap;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::sql::analysis::{
        ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::logical::{
        LogicalAggregateNode, LogicalJoinNode, LogicalPlanKind, LogicalUnionNode,
    };
    use crate::sql::planner::optimizer_bridge::logical::{to_logical_plan, to_optimizer_expr};
    use crate::sql::planner::payload::{PlanFilterNode, PlanProjectNode, PlanScanNode};

    fn ctx_with_arena() -> (RewriteContext, Rc<RefCell<ScalarArena>>) {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let arena = Rc::new(RefCell::new(ScalarArena::new()));
        ctx.set_scalar_arena(Rc::clone(&arena));
        (ctx, arena)
    }

    /// A leaf scan. Pushdown does not care about the scan source; an Iceberg
    /// data-files source mirrors the realistic pre-binding shape.
    fn leaf_scan() -> LogicalPlanNode {
        let column = ColumnDef {
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: "b".to_string(),
                    columns: vec![column],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::IcebergDataFiles {
                        table: IcebergTableInfo {
                            catalog: "ice".to_string(),
                            namespace: "db".to_string(),
                            table: "b".to_string(),
                            table_uuid: Some("uuid-b".to_string()),
                            current_snapshot_id: Some(22),
                            schema_id: 7,
                            location: "file:///tmp/ice/db/b".to_string(),
                            schema: IcebergSchemaDef { fields: Vec::new() },
                            serialized_metadata: None,
                            serialized_metadata_rows: None,
                        },
                        files: Vec::new(),
                        cloud_properties: BTreeMap::new(),
                        binding:
                            crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id: ColumnId(1),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
                predicates: Vec::new(),
                required_columns: None,
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn delta(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: true,
                action_column: None,
                branch_scope: None,
            }),
            vec![input],
            None,
        )
    }

    fn project_over(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId(1),
                            qualifier: None,
                            column: "k".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: "k".to_string(),
                    output_column_id: ColumnId(1),
                }],
                output_qualifier: None,
            }),
            vec![input],
            None,
        )
    }

    fn filter_over(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Bool(true)),
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![input],
            None,
        )
    }

    fn aggregate_over(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: Vec::new(),
                aggregates: Vec::new(),
                output_columns: Vec::new(),
                already_pushed: false,
            }),
            vec![input],
            None,
        )
    }

    fn join_over(left: LogicalPlanNode, right: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![left, right],
            None,
        )
    }

    fn union_over(inputs: Vec<LogicalPlanNode>) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: Vec::new(),
            }),
            inputs,
            None,
        )
    }

    #[test]
    fn pushes_delta_through_project() {
        let rule = PushDeltaThroughUnaryRule;
        let (mut ctx, arena) = ctx_with_arena();
        let plan = delta(project_over(leaf_scan()));
        let expr = to_optimizer_expr(&plan, &mut arena.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let result = rule.apply(expr, &mut ctx).expect("apply must succeed");
        let RewriteResult::Changed(rewritten_expr) = result else {
            panic!("expected Changed(Project)");
        };
        let rewritten = to_logical_plan(rewritten_expr, &arena.borrow());
        let LogicalPlanKind::Project(_) = &rewritten.kind else {
            panic!("expected Changed(Project), got {rewritten:?}");
        };
        let delta_plan = rewritten.unary_input();
        let LogicalPlanKind::ImvDelta(delta) = &delta_plan.kind else {
            panic!("expected ImvDelta under Project");
        };
        assert!(!delta.is_root, "relocated marker is no longer the root");
        assert!(matches!(
            &delta_plan.unary_input().kind,
            LogicalPlanKind::Scan(_)
        ));
    }

    #[test]
    fn pushes_delta_through_filter() {
        let rule = PushDeltaThroughUnaryRule;
        let (mut ctx, arena) = ctx_with_arena();
        let plan = delta(filter_over(leaf_scan()));
        let expr = to_optimizer_expr(&plan, &mut arena.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let result = rule.apply(expr, &mut ctx).expect("apply must succeed");
        let RewriteResult::Changed(rewritten_expr) = result else {
            panic!("expected Changed(Filter)");
        };
        let rewritten = to_logical_plan(rewritten_expr, &arena.borrow());
        let LogicalPlanKind::Filter(_) = &rewritten.kind else {
            panic!("expected Changed(Filter), got {rewritten:?}");
        };
        let delta_plan = rewritten.unary_input();
        let LogicalPlanKind::ImvDelta(delta) = &delta_plan.kind else {
            panic!("expected ImvDelta under Filter");
        };
        assert!(!delta.is_root, "relocated marker is no longer the root");
        assert!(matches!(
            &delta_plan.unary_input().kind,
            LogicalPlanKind::Scan(_)
        ));
    }

    #[test]
    fn leaves_delta_on_scan() {
        let rule = PushDeltaThroughUnaryRule;
        let (mut ctx, arena) = ctx_with_arena();
        let plan = delta(leaf_scan());
        // matches() is false because the direct child is a Scan, not a
        // pushable unary node.
        let expr = to_optimizer_expr(&plan, &mut arena.borrow_mut());
        assert!(!rule.matches(&expr, &ctx));
        // apply() is also a no-op defensively.
        let result = rule.apply(expr, &mut ctx).expect("apply must succeed");
        assert!(matches!(result, RewriteResult::Unchanged));
    }

    #[test]
    fn rejects_delta_over_aggregate() {
        let rule = PushDeltaThroughUnaryRule;
        let (mut ctx, arena) = ctx_with_arena();
        let plan = delta(aggregate_over(leaf_scan()));
        let expr = to_optimizer_expr(&plan, &mut arena.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let err = rule.apply(expr, &mut ctx).expect_err("Aggregate must fail");
        assert!(
            err.contains("Iceberg IMV rewrite does not support this aggregate shape"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn leaves_delta_over_join_for_join_delta_rule() {
        let rule = PushDeltaThroughUnaryRule;
        let (mut ctx, arena) = ctx_with_arena();
        let plan = delta(join_over(leaf_scan(), leaf_scan()));
        let expr = to_optimizer_expr(&plan, &mut arena.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let result = rule
            .apply(expr, &mut ctx)
            .expect("join must be a no-op, not fail");
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "delta over join is left for RewriteJoinDeltaRule"
        );
    }

    #[test]
    fn rejects_delta_over_union() {
        let rule = PushDeltaThroughUnaryRule;
        let (mut ctx, arena) = ctx_with_arena();
        let plan = delta(union_over(vec![leaf_scan()]));
        let expr = to_optimizer_expr(&plan, &mut arena.borrow_mut());
        assert!(rule.matches(&expr, &ctx));
        let err = rule.apply(expr, &mut ctx).expect_err("Union must fail");
        assert!(
            err.contains("Iceberg IMV rewrite does not support this union shape"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn pushes_delta_through_project_then_filter_in_two_steps() {
        // Delta(Project(Filter(Scan))): one apply pushes through Project,
        // a second pushes through Filter, reaching Delta(Scan) at the leaf.
        // This proves the marker fully descends across multiple unary levels
        // when the rule is driven to fixpoint, one level per apply.
        let rule = PushDeltaThroughUnaryRule;
        let (mut ctx, arena) = ctx_with_arena();
        let plan = delta(project_over(filter_over(leaf_scan())));

        // First apply: push through Project.
        let expr = to_optimizer_expr(&plan, &mut arena.borrow_mut());
        let RewriteResult::Changed(after1_expr) =
            rule.apply(expr, &mut ctx).expect("apply must succeed")
        else {
            panic!("expected Changed after first apply");
        };
        let after1 = to_logical_plan(after1_expr, &arena.borrow());
        let LogicalPlanKind::Project(_) = &after1.kind else {
            panic!("expected Project at root");
        };
        // The child is now Delta(Filter(Scan)).
        let nested_delta = after1.unary_input().clone();
        let LogicalPlanKind::ImvDelta(_) = &nested_delta.kind else {
            panic!("expected Delta under Project");
        };

        // Second apply on the nested Delta(Filter(Scan)): push through Filter.
        let nested_expr = to_optimizer_expr(&nested_delta, &mut arena.borrow_mut());
        let RewriteResult::Changed(after2_expr) = rule
            .apply(nested_expr, &mut ctx)
            .expect("apply must succeed")
        else {
            panic!("expected Changed after second apply");
        };
        let after2 = to_logical_plan(after2_expr, &arena.borrow());
        let LogicalPlanKind::Filter(_) = &after2.kind else {
            panic!("expected Filter");
        };
        let delta_plan = after2.unary_input();
        let LogicalPlanKind::ImvDelta(d) = &delta_plan.kind else {
            panic!("expected Delta under Filter");
        };
        // Leaf marker reached a Scan, and is_root is false (relocated).
        assert!(matches!(
            &delta_plan.unary_input().kind,
            LogicalPlanKind::Scan(_)
        ));
        assert!(!d.is_root, "relocated marker is no longer the root");
    }

    #[test]
    fn pushes_through_project_then_fails_fast_at_aggregate() {
        // Delta(Project(Aggregate(Scan))): one apply pushes through the Project,
        // yielding Project(Delta(Aggregate(Scan))); a second apply on the nested
        // Delta(Aggregate(Scan)) must fail-fast at the aggregate boundary.
        let rule = PushDeltaThroughUnaryRule;
        let (mut ctx, arena) = ctx_with_arena();
        let plan = delta(project_over(aggregate_over(leaf_scan())));

        let expr = to_optimizer_expr(&plan, &mut arena.borrow_mut());
        let RewriteResult::Changed(after1_expr) =
            rule.apply(expr, &mut ctx).expect("apply must succeed")
        else {
            panic!("expected Changed after first apply");
        };
        let after1 = to_logical_plan(after1_expr, &arena.borrow());
        let LogicalPlanKind::Project(_) = &after1.kind else {
            panic!("expected Project at root");
        };
        // Second apply on Delta(Aggregate(Scan)) must fail-fast.
        let nested_delta = after1.unary_input().clone();
        let nested_expr = to_optimizer_expr(&nested_delta, &mut arena.borrow_mut());
        let err = rule
            .apply(nested_expr, &mut ctx)
            .expect_err("aggregate must fail");
        assert!(
            err.contains("Iceberg IMV rewrite does not support this aggregate shape"),
            "got: {err}"
        );
    }
}
