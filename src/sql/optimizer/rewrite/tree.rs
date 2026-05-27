use std::time::Instant;

use crate::sql::optimizer::rewrite::context::{RewriteContext, RewriteFailurePolicy};
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{
    AggregateNode, CTEAnchorNode, CTEProduceNode, ExceptNode, FilterNode, IntersectNode, JoinNode,
    LimitNode, LogicalPlan, ProjectNode, RepeatPlanNode, SortNode, SubqueryAliasNode,
    TableFunctionNode, UnionNode, WindowNode,
};

pub(crate) fn rewrite_with_rule(
    plan: LogicalPlan,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlan, bool), String> {
    match rule.traversal() {
        RewriteTraversal::TopDown => rewrite_top_down(plan, rule, ctx),
        RewriteTraversal::BottomUp => rewrite_bottom_up(plan, rule, ctx),
    }
}

fn rewrite_top_down(
    plan: LogicalPlan,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlan, bool), String> {
    let (plan, node_changed) = apply_rule_to_node(plan, rule, ctx)?;
    let (plan, child_changed) = rewrite_children(plan, rule, ctx)?;
    Ok((plan, node_changed || child_changed))
}

fn rewrite_bottom_up(
    plan: LogicalPlan,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlan, bool), String> {
    let (plan, child_changed) = rewrite_children(plan, rule, ctx)?;
    let (plan, node_changed) = apply_rule_to_node(plan, rule, ctx)?;
    Ok((plan, child_changed || node_changed))
}

fn apply_rule_to_node(
    plan: LogicalPlan,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlan, bool), String> {
    if !rule.matches(&plan, ctx) {
        return Ok((plan, false));
    }

    let original = plan.clone();
    let phase = rule.phase();
    let rule_name = rule.name();
    ctx.trace_mut().rule_matched(phase, rule_name);

    let start = Instant::now();
    match rule.apply(plan, ctx) {
        Ok(RewriteResult::Unchanged) => Ok((original, false)),
        Ok(RewriteResult::Changed(next)) => {
            ctx.trace_mut()
                .rule_changed(phase, rule_name, start.elapsed().as_micros());
            Ok((next, true))
        }
        Ok(RewriteResult::Rejected(diagnostic)) => {
            let message = diagnostic.message;
            ctx.trace_mut()
                .rule_rejected(phase, rule_name, message.clone());
            match ctx.policy().failure_policy {
                RewriteFailurePolicy::CollectDiagnostics => Ok((original, false)),
                RewriteFailurePolicy::FailFast => Err(message),
            }
        }
        Err(message) => {
            ctx.trace_mut()
                .rule_failed(phase, rule_name, message.clone());
            Err(message)
        }
    }
}

fn rewrite_children(
    plan: LogicalPlan,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(LogicalPlan, bool), String> {
    match plan {
        leaf @ (LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_)) => Ok((leaf, false)),
        LogicalPlan::Filter(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::Filter(FilterNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::Project(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::Project(ProjectNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::Aggregate(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::Aggregate(AggregateNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::Sort(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::Sort(SortNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::Limit(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::Limit(LimitNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::Window(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::Window(WindowNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::TableFunction(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::TableFunction(TableFunctionNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::SubqueryAlias(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::Repeat(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::Repeat(RepeatPlanNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::CTEProduce(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::CTEProduce(CTEProduceNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::Join(node) => {
            let (left, left_changed) = rewrite_with_rule(*node.left, rule, ctx)?;
            let (right, right_changed) = rewrite_with_rule(*node.right, rule, ctx)?;
            Ok((
                LogicalPlan::Join(JoinNode {
                    left: Box::new(left),
                    right: Box::new(right),
                    ..node
                }),
                left_changed || right_changed,
            ))
        }
        LogicalPlan::CTEAnchor(node) => {
            let (produce, produce_changed) = rewrite_with_rule(*node.produce, rule, ctx)?;
            let (consumer, consumer_changed) = rewrite_with_rule(*node.consumer, rule, ctx)?;
            Ok((
                LogicalPlan::CTEAnchor(CTEAnchorNode {
                    produce: Box::new(produce),
                    consumer: Box::new(consumer),
                    ..node
                }),
                produce_changed || consumer_changed,
            ))
        }
        LogicalPlan::Union(node) => {
            let all = node.all;
            let (inputs, changed) = rewrite_plan_list(node.inputs, rule, ctx)?;
            Ok((LogicalPlan::Union(UnionNode { inputs, all }), changed))
        }
        LogicalPlan::Intersect(node) => {
            let (inputs, changed) = rewrite_plan_list(node.inputs, rule, ctx)?;
            Ok((LogicalPlan::Intersect(IntersectNode { inputs }), changed))
        }
        LogicalPlan::Except(node) => {
            let (inputs, changed) = rewrite_plan_list(node.inputs, rule, ctx)?;
            Ok((LogicalPlan::Except(ExceptNode { inputs }), changed))
        }
    }
}

fn rewrite_plan_list(
    inputs: Vec<LogicalPlan>,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(Vec<LogicalPlan>, bool), String> {
    let mut changed = false;
    let mut rewritten = Vec::with_capacity(inputs.len());
    for input in inputs {
        let (input, input_changed) = rewrite_with_rule(input, rule, ctx)?;
        changed |= input_changed;
        rewritten.push(input);
    }
    Ok((rewritten, changed))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::rewrite_with_rule;
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
    use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
    use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;
    use crate::sql::planner::plan::{LogicalPlan, ProjectNode, ScanNode};

    struct RenameScanRule;

    impl LogicalRewriteRule for RenameScanRule {
        fn name(&self) -> &'static str {
            "RenameScanRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::StructuralRewrite
        }

        fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            matches!(plan, LogicalPlan::Scan(node) if node.table.name == "before")
        }

        fn apply(
            &self,
            plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            let LogicalPlan::Scan(mut node) = plan else {
                return Ok(RewriteResult::Unchanged);
            };
            node.table.name = "after".to_string();
            Ok(RewriteResult::Changed(LogicalPlan::Scan(node)))
        }
    }

    struct RejectProjectRule;

    impl LogicalRewriteRule for RejectProjectRule {
        fn name(&self) -> &'static str {
            "RejectProjectRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::StructuralRewrite
        }

        fn traversal(&self) -> RewriteTraversal {
            RewriteTraversal::TopDown
        }

        fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            matches!(plan, LogicalPlan::Project(_))
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
                self.name(),
                "project rejected",
            )))
        }
    }

    #[test]
    fn bottom_up_rewrite_rebuilds_project_child() {
        let plan = project_over_scan("before");
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        let (rewritten, changed) = rewrite_with_rule(plan, &RenameScanRule, &mut ctx).unwrap();

        assert!(changed);
        let LogicalPlan::Project(project) = rewritten else {
            panic!("expected project root");
        };
        let LogicalPlan::Scan(scan) = *project.input else {
            panic!("expected rewritten scan child");
        };
        assert_eq!(scan.table.name, "after");
    }

    #[test]
    fn rejected_rule_collects_diagnostic_without_changing_plan() {
        let plan = project_over_scan("before");
        let before = format!("{plan:?}");
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        let (rewritten, changed) = rewrite_with_rule(plan, &RejectProjectRule, &mut ctx).unwrap();

        assert!(!changed);
        assert_eq!(format!("{rewritten:?}"), before);
        assert!(ctx.trace().events().iter().any(|event| {
            matches!(
                event,
                RewriteTraceEvent::RuleRejected {
                    phase: RewritePhase::StructuralRewrite,
                    rule: "RejectProjectRule",
                    message
                } if message == "project rejected"
            )
        }));
    }

    fn project_over_scan(table_name: &str) -> LogicalPlan {
        let output = output_column("c1");
        LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Scan(ScanNode {
                database: "db".to_string(),
                table: table_def(table_name),
                alias: None,
                columns: vec![output.clone()],
                predicates: vec![],
                required_columns: None,
            })),
            items: vec![ProjectItem {
                expr: column_ref(output.column_id, "c1"),
                output_name: "c1".to_string(),
            }],
        })
    }

    fn table_def(name: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: "c1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            iceberg_table: None,
            source: ScanSource::StarRocks,
        }
    }

    fn output_column(name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(1),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn column_ref(column_id: ColumnId, column: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: column.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    #[test]
    fn rewrite_visits_all_logical_plan_variants() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::result::RewriteResult;
        use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
        use crate::sql::planner::plan::*;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountVisitsRule {
            count: Arc<AtomicUsize>,
        }

        impl LogicalRewriteRule for CountVisitsRule {
            fn name(&self) -> &'static str {
                "CountVisitsRule"
            }
            fn phase(&self) -> RewritePhase {
                RewritePhase::LogicalNormalize
            }
            fn traversal(&self) -> RewriteTraversal {
                RewriteTraversal::TopDown
            }
            fn matches(&self, _plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
                self.count.fetch_add(1, Ordering::SeqCst);
                false
            }
            fn apply(
                &self,
                _plan: LogicalPlan,
                _ctx: &mut RewriteContext,
            ) -> Result<RewriteResult, String> {
                Ok(RewriteResult::Unchanged)
            }
        }

        let leaf = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        });

        // Exhaustive match on &LogicalPlan. This is the intentional trip-wire:
        // if a new variant lands in LogicalPlan, this test fails to compile.
        fn assert_variant_handled(variant: &LogicalPlan) {
            match variant {
                LogicalPlan::Scan(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::Project(_)
                | LogicalPlan::Aggregate(_)
                | LogicalPlan::Join(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Limit(_)
                | LogicalPlan::Union(_)
                | LogicalPlan::Intersect(_)
                | LogicalPlan::Except(_)
                | LogicalPlan::Values(_)
                | LogicalPlan::GenerateSeries(_)
                | LogicalPlan::TableFunction(_)
                | LogicalPlan::Window(_)
                | LogicalPlan::SubqueryAlias(_)
                | LogicalPlan::Repeat(_)
                | LogicalPlan::CTEAnchor(_)
                | LogicalPlan::CTEProduce(_)
                | LogicalPlan::CTEConsume(_) => {}
            }
        }
        assert_variant_handled(&leaf);

        let count = Arc::new(AtomicUsize::new(0));
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let (_, _) = super::rewrite_with_rule(
            leaf,
            &CountVisitsRule {
                count: Arc::clone(&count),
            },
            &mut ctx,
        )
        .unwrap();

        assert!(count.load(Ordering::SeqCst) >= 1);
    }
}
