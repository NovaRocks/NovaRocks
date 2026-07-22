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

use std::time::Instant;

use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::{RewriteContext, RewriteFailurePolicy};
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};

pub(crate) fn rewrite_with_rule(
    plan: OptExpr,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(OptExpr, bool), String> {
    match rule.traversal() {
        RewriteTraversal::TopDown => rewrite_top_down(plan, rule, ctx),
        RewriteTraversal::BottomUp => rewrite_bottom_up(plan, rule, ctx),
    }
}

fn rewrite_top_down(
    plan: OptExpr,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(OptExpr, bool), String> {
    let (plan, node_changed) = apply_rule_to_node(plan, rule, ctx)?;
    let (plan, child_changed) = rewrite_children(plan, rule, ctx)?;
    Ok((plan, node_changed || child_changed))
}

fn rewrite_bottom_up(
    plan: OptExpr,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(OptExpr, bool), String> {
    let (plan, child_changed) = rewrite_children(plan, rule, ctx)?;
    let (plan, node_changed) = apply_rule_to_node(plan, rule, ctx)?;
    Ok((plan, child_changed || node_changed))
}

fn apply_rule_to_node(
    plan: OptExpr,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(OptExpr, bool), String> {
    if super::tree_binder::bind_tree(&rule.pattern(), &plan).is_none() {
        return Ok((plan, false));
    }

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
    mut plan: OptExpr,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(OptExpr, bool), String> {
    let (children, changed) = rewrite_plan_list(std::mem::take(&mut plan.children), rule, ctx)?;
    plan.children = children;
    Ok((plan, changed))
}

fn rewrite_plan_list(
    inputs: Vec<OptExpr>,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(Vec<OptExpr>, bool), String> {
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
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        Operator, ProjectOp, ScalarProjectItem, ScanOp, ValuesOp,
    };
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::pattern::{OpKind, Pattern};
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
    use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
    use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

    struct RenameScanRule;

    impl LogicalRewriteRule for RenameScanRule {
        fn name(&self) -> &'static str {
            "RenameScanRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::StructuralRewrite
        }

        fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
            matches!(&expr.op, Operator::LogicalScan(op) if op.table.name == "before")
        }

        fn apply(
            &self,
            mut expr: OptExpr,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            let Operator::LogicalScan(ref mut op) = expr.op else {
                return Ok(RewriteResult::Unchanged);
            };
            op.table.name = "after".to_string();
            Ok(RewriteResult::Changed(expr))
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

        fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
            matches!(&expr.op, Operator::LogicalProject(_))
        }

        fn apply(
            &self,
            _expr: OptExpr,
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
        let Operator::LogicalProject(_) = &rewritten.op else {
            panic!("expected project root");
        };
        let Operator::LogicalScan(scan) = &rewritten.unary_input().op else {
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

    #[test]
    fn pattern_pre_gate_skips_matches_on_structural_miss() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct FilterPatternRule {
            matches_count: Arc<AtomicUsize>,
        }

        impl LogicalRewriteRule for FilterPatternRule {
            fn name(&self) -> &'static str {
                "FilterPatternRule"
            }

            fn phase(&self) -> RewritePhase {
                RewritePhase::StructuralRewrite
            }

            fn pattern(&self) -> Pattern {
                Pattern::Op {
                    kind: OpKind::Filter,
                    children: vec![Pattern::MultiLeaf],
                }
            }

            fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
                self.matches_count.fetch_add(1, Ordering::SeqCst);
                true
            }

            fn apply(
                &self,
                _expr: OptExpr,
                _ctx: &mut RewriteContext,
            ) -> Result<RewriteResult, String> {
                Ok(RewriteResult::Unchanged)
            }
        }

        let plan = OptExpr::new(
            Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
        );
        let before = format!("{plan:?}");
        let matches_count = Arc::new(AtomicUsize::new(0));
        let rule = FilterPatternRule {
            matches_count: Arc::clone(&matches_count),
        };
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        let (rewritten, changed) = rewrite_with_rule(plan, &rule, &mut ctx).unwrap();

        assert!(!changed);
        assert_eq!(format!("{rewritten:?}"), before);
        assert_eq!(matches_count.load(Ordering::SeqCst), 0);
    }

    fn project_over_scan(table_name: &str) -> OptExpr {
        use crate::sql::analysis::{ExprKind, TypedExpr};
        use crate::sql::optimizer::scalar::ScalarArena;

        use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
        let output = output_column("c1");
        let mut arena = ScalarArena::new();
        let col_expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: output.column_id,
                qualifier: None,
                column: "c1".to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        };
        let expr_id = intern_typed(&mut arena, &col_expr);
        OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![ScalarProjectItem {
                    expr: expr_id,
                    output_name: "c1".to_string(),
                    output_column_id: output.column_id,
                    expr_display: None,
                }],
                output_qualifier: None,
            }),
            vec![OptExpr::new(
                Operator::LogicalScan(ScanOp {
                    database: "db".to_string(),
                    table: table_def(table_name),
                    alias: None,
                    stats_ref: None,
                    columns: vec![output.clone()],
                    predicates: vec![],
                    required_columns: None,
                    variant_columns: vec![],
                    mv_rewritten_from: None,
                }),
                vec![],
            )],
        )
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

    #[test]
    fn rewrite_traverses_into_join_child() {
        // Replaces the former rewrite_traverses_into_imv_delta_child test.
        // LogicalJoin is available in Operator and exercises the same traversal
        // path: a parent node that is not matched, with a matched child below it.
        use crate::sql::analysis::JoinKind;
        use crate::sql::optimizer::operator::LogicalJoinOp;

        let inner = OptExpr::new(
            Operator::LogicalScan(ScanOp {
                database: "db".to_string(),
                table: table_def("before"),
                alias: None,
                stats_ref: None,
                columns: vec![output_column("c1")],
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
        );
        let dummy = OptExpr::new(
            Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
        );

        let plan = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![inner, dummy],
        );

        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let (rewritten, changed) = rewrite_with_rule(plan, &RenameScanRule, &mut ctx).unwrap();

        assert!(changed, "RenameScanRule should rewrite the wrapped Scan");
        let Operator::LogicalJoin(_) = &rewritten.op else {
            panic!("expected LogicalJoin to remain at root after child rewrite");
        };
        let Operator::LogicalScan(scan) = &rewritten.children[0].op else {
            panic!("expected Scan inside join left child");
        };
        assert_eq!(scan.table.name, "after");
    }

    #[test]
    fn rewrite_visits_all_logical_operator_variants() {
        use crate::sql::optimizer::operator::Operator;
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::result::RewriteResult;
        use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
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
            fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
                self.count.fetch_add(1, Ordering::SeqCst);
                false
            }
            fn apply(
                &self,
                _expr: OptExpr,
                _ctx: &mut RewriteContext,
            ) -> Result<RewriteResult, String> {
                Ok(RewriteResult::Unchanged)
            }
        }

        let leaf = OptExpr::new(
            Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
        );

        // Exhaustive match on &Operator logical variants. This is the intentional
        // trip-wire: if a new logical variant lands in Operator, this test fails
        // to compile.
        fn assert_variant_handled(op: &Operator) {
            match op {
                Operator::LogicalScan(_)
                | Operator::LogicalFilter(_)
                | Operator::LogicalProject(_)
                | Operator::LogicalAggregate(_)
                | Operator::LogicalJoin(_)
                | Operator::LogicalSort(_)
                | Operator::LogicalLimit(_)
                | Operator::LogicalTopN(_)
                | Operator::LogicalWindow(_)
                | Operator::LogicalUnion(_)
                | Operator::LogicalIntersect(_)
                | Operator::LogicalExcept(_)
                | Operator::LogicalValues(_)
                | Operator::LogicalGenerateSeries(_)
                | Operator::LogicalTableFunction(_)
                | Operator::LogicalRepeat(_)
                | Operator::LogicalChangeEventExpand(_)
                | Operator::LogicalCTEAnchor(_)
                | Operator::LogicalCTEProduce(_)
                | Operator::LogicalCTEConsume(_)
                | Operator::LogicalAssertOneRow(_)
                // Pre-memo logical-only variants (eliminated before memo entry).
                | Operator::LogicalApply(_)
                | Operator::LogicalImvDelta(_)
                | Operator::LogicalImvVersion(_)
                // Physical variants — also exhaustively listed so the match
                // is complete without a wildcard.
                | Operator::PhysicalScan(_)
                | Operator::PhysicalFilter(_)
                | Operator::PhysicalProject(_)
                | Operator::PhysicalHashJoin(_)
                | Operator::PhysicalNestLoopJoin(_)
                | Operator::PhysicalHashAggregate(_)
                | Operator::PhysicalSort(_)
                | Operator::PhysicalLimit(_)
                | Operator::PhysicalTopN(_)
                | Operator::PhysicalWindow(_)
                | Operator::PhysicalDistribution(_)
                | Operator::PhysicalCTEAnchor(_)
                | Operator::PhysicalCTEProduce(_)
                | Operator::PhysicalCTEConsume(_)
                | Operator::PhysicalRepeat(_)
                | Operator::PhysicalChangeEventExpand(_)
                | Operator::PhysicalUnion(_)
                | Operator::PhysicalIntersect(_)
                | Operator::PhysicalExcept(_)
                | Operator::PhysicalValues(_)
                | Operator::PhysicalGenerateSeries(_)
                | Operator::PhysicalTableFunction(_)
                | Operator::PhysicalAssertOneRow(_) => {}
            }
        }
        assert_variant_handled(&leaf.op);

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
    fn bottom_up_rewrite_rebuilds_join_children() {
        // Replaces the former bottom_up_rewrite_rebuilds_apply_children test.
        // LogicalJoin has two children like Apply, exercising the same
        // left/right child traversal. No Apply or ImvDelta in Operator.
        use crate::sql::analysis::JoinKind;
        use crate::sql::optimizer::operator::LogicalJoinOp;

        let left = project_over_scan("outer");
        let Operator::LogicalProject(_) = &left.op else {
            panic!("helper returns project");
        };
        let right = project_over_scan("before");
        let Operator::LogicalProject(_) = &right.op else {
            panic!("helper returns project");
        };

        let plan = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![left, right],
        );

        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let (rewritten, changed) = rewrite_with_rule(plan, &RenameScanRule, &mut ctx).unwrap();

        assert!(changed);
        let Operator::LogicalJoin(_) = &rewritten.op else {
            panic!("expected join root");
        };
        let Operator::LogicalScan(right_scan) = &rewritten.right().unary_input().op else {
            panic!("expected scan on join right side (under project)");
        };
        assert_eq!(right_scan.table.name, "after");
    }
}
