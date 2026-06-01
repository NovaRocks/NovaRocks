//! Pushes the root `ImvDelta` marker down through unary Project/Filter nodes
//! so it directly wraps the leaf Scan, where `BindIcebergScanRule` can bind it.
//!
//! Delta commutes with projection and filtering (a row's insert/delete action
//! is preserved through column projection and row filtering), so
//! `Delta(Project(x)) == Project(Delta(x))` and `Delta(Filter(x)) == Filter(Delta(x))`.
//! Delta does NOT commute with Aggregate/Join/Union; those are unsupported in
//! Phase 2 and fail-fast here (Phase 4/5/6).

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::LogicalPlan;

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

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(
            plan,
            LogicalPlan::ImvDelta(node)
                if matches!(
                    node.input.as_ref(),
                    LogicalPlan::Project(_)
                        | LogicalPlan::Filter(_)
                        | LogicalPlan::Aggregate(_)
                        | LogicalPlan::Join(_)
                        | LogicalPlan::Union(_)
                )
        )
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::ImvDelta(delta) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        // Decide based on the child kind WITHOUT consuming `delta` yet. This
        // two-phase structure avoids both (a) moving `delta.input` before we
        // know how to handle the child and (b) rebuilding an identical marker
        // for an unhandled child, which would loop forever under fixpoint.
        // Fail-fast on unsupported shapes here (structural stage) is the first of
        // three layers; PropagateActionColumnRule and ActionColumnValidationRule
        // re-assert the same boundary later with richer diagnostics (base FQN).
        match delta.input.as_ref() {
            LogicalPlan::Project(_) | LogicalPlan::Filter(_) => { /* fall through to push */ }
            LogicalPlan::Aggregate(_) => {
                return Err(
                    "Iceberg IMV aggregate rewrite did not consume Delta(Aggregate); \
                     verify RewriteAggregateStateRule ran before PushDeltaThroughUnary"
                        .to_string(),
                );
            }
            LogicalPlan::Join(_) => {
                return Err(
                    "Iceberg IMV join delta rewrite did not consume Delta(Join); \
                     verify RewriteJoinAggregateDeltaRule ran before PushDeltaThroughUnary"
                        .to_string(),
                );
            }
            LogicalPlan::Union(_) => {
                return Err(
                    "IMV delta pushdown does not support Union above delta-bound scans \
                     in Phase 2; union delta rewrite is scheduled for Phase 6"
                        .to_string(),
                );
            }
            // Scan or any other shape: the marker already directly wraps a leaf
            // (or a node we do not push through). Leave it for BindIcebergScan.
            _ => return Ok(RewriteResult::Unchanged),
        }

        // The relocated marker is no longer at the structural plan root, so it is
        // is_root: false. WrapRootInImvDelta (the only is_root reader) has already
        // run in the earlier imv-delta-marker stage and never re-runs.
        let action_column = delta.action_column;
        match *delta.input {
            LogicalPlan::Project(mut p) => {
                // Commutation Delta(Project(x)) == Project(Delta(x)) holds because
                // Project items are row-local and the delta only marks each row's
                // change action (carried through by action-column propagation).
                // Volatile/non-deterministic projection expressions are out of
                // scope for Phase 2; window calls cannot appear here (the planner
                // extracts them into a dedicated WindowNode).
                let inner = LogicalPlan::ImvDelta(ImvDeltaNode {
                    input: p.input,
                    is_root: false,
                    action_column,
                });
                p.input = Box::new(inner);
                Ok(RewriteResult::Changed(LogicalPlan::Project(p)))
            }
            LogicalPlan::Filter(mut f) => {
                let inner = LogicalPlan::ImvDelta(ImvDeltaNode {
                    input: f.input,
                    is_root: false,
                    action_column,
                });
                f.input = Box::new(inner);
                Ok(RewriteResult::Changed(LogicalPlan::Filter(f)))
            }
            // The decision match above guarantees the child is Project or
            // Filter at this point; every other shape returned early.
            _ => unreachable!("child kind already filtered to Project/Filter"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{
        ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::planner::plan::{
        AggregateNode, FilterNode, JoinNode, ProjectNode, ScanNode, UnionNode,
    };

    fn ctx() -> RewriteContext {
        RewriteContext::for_mv_refresh(Vec::<String>::new())
    }

    /// A leaf scan. Pushdown does not care about the scan source; an Iceberg
    /// data-files source mirrors the realistic pre-binding shape.
    fn leaf_scan() -> LogicalPlan {
        let column = ColumnDef {
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        LogicalPlan::Scan(ScanNode {
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
                    },
                    files: Vec::new(),
                    cloud_properties: BTreeMap::new(),
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
            dict_columns: Vec::new(),
            required_output_columns: None,
        })
    }

    fn delta(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(input),
            is_root: true,
            action_column: None,
        })
    }

    fn project_over(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
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
            required_output_columns: None,
        })
    }

    fn filter_over(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Filter(FilterNode {
            input: Box::new(input),
            predicate: TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Bool(true)),
                data_type: DataType::Boolean,
                nullable: false,
            },
            required_output_columns: None,
        })
    }

    fn aggregate_over(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(input),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            output_columns: Vec::new(),
            already_pushed: false,
            required_output_columns: None,
        })
    }

    fn join_over(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Inner,
            condition: None,
            required_output_columns: None,
        })
    }

    fn union_over(inputs: Vec<LogicalPlan>) -> LogicalPlan {
        LogicalPlan::Union(UnionNode {
            inputs,
            all: true,
            output_columns: Vec::new(),
            required_output_columns: None,
        })
    }

    #[test]
    fn pushes_delta_through_project() {
        let rule = PushDeltaThroughUnaryRule;
        let mut ctx = ctx();
        let plan = delta(project_over(leaf_scan()));
        assert!(rule.matches(&plan, &ctx));
        let result = rule.apply(plan, &mut ctx).expect("apply must succeed");
        let RewriteResult::Changed(LogicalPlan::Project(project)) = result else {
            panic!("expected Changed(Project)");
        };
        let LogicalPlan::ImvDelta(delta) = *project.input else {
            panic!("expected ImvDelta under Project");
        };
        assert!(!delta.is_root, "relocated marker is no longer the root");
        assert!(matches!(*delta.input, LogicalPlan::Scan(_)));
    }

    #[test]
    fn pushes_delta_through_filter() {
        let rule = PushDeltaThroughUnaryRule;
        let mut ctx = ctx();
        let plan = delta(filter_over(leaf_scan()));
        assert!(rule.matches(&plan, &ctx));
        let result = rule.apply(plan, &mut ctx).expect("apply must succeed");
        let RewriteResult::Changed(LogicalPlan::Filter(filter)) = result else {
            panic!("expected Changed(Filter)");
        };
        let LogicalPlan::ImvDelta(delta) = *filter.input else {
            panic!("expected ImvDelta under Filter");
        };
        assert!(!delta.is_root, "relocated marker is no longer the root");
        assert!(matches!(*delta.input, LogicalPlan::Scan(_)));
    }

    #[test]
    fn leaves_delta_on_scan() {
        let rule = PushDeltaThroughUnaryRule;
        let mut ctx = ctx();
        let plan = delta(leaf_scan());
        // matches() is false because the direct child is a Scan, not a
        // pushable unary node.
        assert!(!rule.matches(&plan, &ctx));
        // apply() is also a no-op defensively.
        let result = rule.apply(plan, &mut ctx).expect("apply must succeed");
        assert!(matches!(result, RewriteResult::Unchanged));
    }

    #[test]
    fn rejects_delta_over_aggregate() {
        let rule = PushDeltaThroughUnaryRule;
        let mut ctx = ctx();
        let plan = delta(aggregate_over(leaf_scan()));
        assert!(rule.matches(&plan, &ctx));
        let err = rule.apply(plan, &mut ctx).expect_err("Aggregate must fail");
        assert!(
            err.contains("RewriteAggregateStateRule"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_delta_over_join() {
        let rule = PushDeltaThroughUnaryRule;
        let mut ctx = ctx();
        let plan = delta(join_over(leaf_scan(), leaf_scan()));
        assert!(rule.matches(&plan, &ctx));
        let err = rule.apply(plan, &mut ctx).expect_err("Join must fail");
        assert!(
            err.contains("RewriteJoinAggregateDeltaRule"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_delta_over_union() {
        let rule = PushDeltaThroughUnaryRule;
        let mut ctx = ctx();
        let plan = delta(union_over(vec![leaf_scan()]));
        assert!(rule.matches(&plan, &ctx));
        let err = rule.apply(plan, &mut ctx).expect_err("Union must fail");
        assert!(err.contains("Phase 6"), "unexpected error: {err}");
    }

    #[test]
    fn pushes_delta_through_project_then_filter_in_two_steps() {
        // Delta(Project(Filter(Scan))): one apply pushes through Project,
        // a second pushes through Filter, reaching Delta(Scan) at the leaf.
        // This proves the marker fully descends across multiple unary levels
        // when the rule is driven to fixpoint, one level per apply.
        let rule = PushDeltaThroughUnaryRule;
        let mut ctx = ctx();
        let plan = delta(project_over(filter_over(leaf_scan())));

        // First apply: push through Project.
        let RewriteResult::Changed(after1) =
            rule.apply(plan, &mut ctx).expect("apply must succeed")
        else {
            panic!("expected Changed after first apply");
        };
        let LogicalPlan::Project(p) = after1 else {
            panic!("expected Project at root");
        };
        // The child is now Delta(Filter(Scan)).
        let LogicalPlan::ImvDelta(_) = p.input.as_ref() else {
            panic!("expected Delta under Project");
        };

        // Second apply on the nested Delta(Filter(Scan)): push through Filter.
        let RewriteResult::Changed(after2) =
            rule.apply(*p.input, &mut ctx).expect("apply must succeed")
        else {
            panic!("expected Changed after second apply");
        };
        let LogicalPlan::Filter(f) = after2 else {
            panic!("expected Filter");
        };
        let LogicalPlan::ImvDelta(d) = f.input.as_ref() else {
            panic!("expected Delta under Filter");
        };
        // Leaf marker reached a Scan, and is_root is false (relocated).
        assert!(matches!(d.input.as_ref(), LogicalPlan::Scan(_)));
        assert!(!d.is_root, "relocated marker is no longer the root");
    }

    #[test]
    fn pushes_through_project_then_fails_fast_at_aggregate() {
        // Delta(Project(Aggregate(Scan))): one apply pushes through the Project,
        // yielding Project(Delta(Aggregate(Scan))); a second apply on the nested
        // Delta(Aggregate(Scan)) must fail-fast with the Phase 4 boundary.
        let rule = PushDeltaThroughUnaryRule;
        let mut ctx = ctx();
        let plan = delta(project_over(aggregate_over(leaf_scan())));

        let RewriteResult::Changed(after1) =
            rule.apply(plan, &mut ctx).expect("apply must succeed")
        else {
            panic!("expected Changed after first apply");
        };
        let LogicalPlan::Project(p) = after1 else {
            panic!("expected Project at root");
        };
        // Second apply on Delta(Aggregate(Scan)) must fail-fast.
        let err = rule
            .apply(*p.input, &mut ctx)
            .expect_err("aggregate must fail");
        assert!(err.contains("RewriteAggregateStateRule"), "got: {err}");
    }
}
