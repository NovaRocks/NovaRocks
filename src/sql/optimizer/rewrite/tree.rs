use std::time::Instant;

use crate::sql::optimizer::rewrite::context::{RewriteContext, RewriteFailurePolicy};
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{
    AggregateNode, AggregateStateMergeNode, ApplyNode, AssertOneRowNode, CTEAnchorNode,
    CTEProduceNode, DecodeNode, ExceptNode, FilterNode, IntersectNode, JoinNode, LimitNode,
    LogicalPlan, ProjectNode, RepeatPlanNode, SortNode, TableFunctionNode, UnionNode, WindowNode,
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
            let output_columns = node.output_columns;
            let required_output_columns = node.required_output_columns;
            let (inputs, changed) = rewrite_plan_list(node.inputs, rule, ctx)?;
            Ok((
                LogicalPlan::Union(UnionNode {
                    inputs,
                    all,
                    output_columns,
                    required_output_columns,
                }),
                changed,
            ))
        }
        LogicalPlan::Intersect(node) => {
            let output_columns = node.output_columns;
            let required_output_columns = node.required_output_columns;
            let (inputs, changed) = rewrite_plan_list(node.inputs, rule, ctx)?;
            Ok((
                LogicalPlan::Intersect(IntersectNode {
                    inputs,
                    output_columns,
                    required_output_columns,
                }),
                changed,
            ))
        }
        LogicalPlan::Except(node) => {
            let output_columns = node.output_columns;
            let required_output_columns = node.required_output_columns;
            let (inputs, changed) = rewrite_plan_list(node.inputs, rule, ctx)?;
            Ok((
                LogicalPlan::Except(ExceptNode {
                    inputs,
                    output_columns,
                    required_output_columns,
                }),
                changed,
            ))
        }
        LogicalPlan::Decode(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::Decode(DecodeNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::AggregateStateMerge(node) => {
            let (old_input, old_changed) = rewrite_with_rule(*node.old_input, rule, ctx)?;
            let (delta_input, delta_changed) = rewrite_with_rule(*node.delta_input, rule, ctx)?;
            Ok((
                LogicalPlan::AggregateStateMerge(AggregateStateMergeNode {
                    old_input: Box::new(old_input),
                    delta_input: Box::new(delta_input),
                    ..node
                }),
                old_changed || delta_changed,
            ))
        }
        LogicalPlan::Apply(node) => {
            let (left, left_changed) = rewrite_with_rule(*node.left, rule, ctx)?;
            let (right, right_changed) = rewrite_with_rule(*node.right, rule, ctx)?;
            Ok((
                LogicalPlan::Apply(ApplyNode {
                    left: Box::new(left),
                    right: Box::new(right),
                    ..node
                }),
                left_changed || right_changed,
            ))
        }
        LogicalPlan::AssertOneRow(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::AssertOneRow(AssertOneRowNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::ImvDelta(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::ImvDelta(crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::ImvVersion(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::ImvVersion(
                    crate::sql::optimizer::rewrite::imv::marker::ImvVersionNode {
                        input: Box::new(input),
                        ..node
                    },
                ),
                changed,
            ))
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
                dict_columns: vec![],
                variant_columns: vec![],
                required_output_columns: None,
            })),
            items: vec![ProjectItem {
                expr: column_ref(output.column_id, "c1"),
                output_name: "c1".to_string(),
                output_column_id: output.column_id,
            }],
            output_qualifier: None,
            required_output_columns: None,
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
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn output_column(name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(1),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
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
    fn rewrite_traverses_into_imv_delta_child() {
        use crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode;
        use crate::sql::planner::plan::{LogicalPlan, ScanNode};

        let inner = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: table_def("before"),
            alias: None,
            columns: vec![output_column("c1")],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            required_output_columns: None,
        });

        let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(inner),
            is_root: true,
            action_column: None,
            branch_scope: None,
        });

        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let (rewritten, changed) = rewrite_with_rule(plan, &RenameScanRule, &mut ctx).unwrap();

        assert!(changed, "RenameScanRule should rewrite the wrapped Scan");
        let LogicalPlan::ImvDelta(delta) = rewritten else {
            panic!("expected ImvDelta to remain at root after child rewrite");
        };
        let LogicalPlan::Scan(scan) = *delta.input else {
            panic!("expected Scan inside ImvDelta");
        };
        assert_eq!(scan.table.name, "after");
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
            required_output_columns: None,
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
                | LogicalPlan::Repeat(_)
                | LogicalPlan::CTEAnchor(_)
                | LogicalPlan::CTEProduce(_)
                | LogicalPlan::CTEConsume(_)
                | LogicalPlan::Decode(_)
                | LogicalPlan::AggregateStateMerge(_)
                | LogicalPlan::Apply(_)
                | LogicalPlan::AssertOneRow(_)
                | LogicalPlan::ImvDelta(_)
                | LogicalPlan::ImvVersion(_) => {}
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

    #[test]
    fn bottom_up_rewrite_rebuilds_apply_children() {
        use std::collections::HashSet;

        use crate::sql::planner::plan::{ApplyKind, ApplyNode};

        let outer = project_over_scan("outer");
        let LogicalPlan::Project(outer_project) = outer else {
            panic!("helper returns project");
        };
        let inner = project_over_scan("before");
        let LogicalPlan::Project(inner_project) = inner else {
            panic!("helper returns project");
        };

        let plan = LogicalPlan::Apply(ApplyNode {
            left: outer_project.input,
            right: inner_project.input,
            kind: ApplyKind::Scalar,
            subquery_expr: column_ref(ColumnId(7), "sq"),
            output_column: output_column("sq"),
            inner_output_column_id: ColumnId(7),
            correlation_column_ids: vec![],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let (rewritten, changed) = rewrite_with_rule(plan, &RenameScanRule, &mut ctx).unwrap();

        assert!(changed);
        let LogicalPlan::Apply(apply) = rewritten else {
            panic!("expected apply root");
        };
        let LogicalPlan::Scan(right_scan) = *apply.right else {
            panic!("expected scan on apply right side");
        };
        assert_eq!(right_scan.table.name, "after");
    }
}
