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

use crate::compiler::SqlCompileError;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};

use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::{RewriteContext, RewriteFailurePolicy};
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};

pub(crate) fn rewrite_with_rule(
    plan: OptExpr,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
) -> Result<(OptExpr, bool), SqlCompileError> {
    let control = ctx.control_view();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate)
        .map_err(crate::compiler::SqlCompileError::from)?;
    let result = rewrite_with_rule_inner(plan, rule, ctx, &mut work)?;
    work.finish()
        .map_err(crate::compiler::SqlCompileError::from)?;
    Ok(result)
}

fn rewrite_with_rule_inner(
    plan: OptExpr,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(OptExpr, bool), SqlCompileError> {
    work.step()
        .map_err(crate::compiler::SqlCompileError::from)?;
    match rule.traversal() {
        RewriteTraversal::TopDown => rewrite_top_down(plan, rule, ctx, work),
        RewriteTraversal::BottomUp => rewrite_bottom_up(plan, rule, ctx, work),
    }
}

fn rewrite_top_down(
    plan: OptExpr,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(OptExpr, bool), SqlCompileError> {
    let (plan, node_changed) = apply_rule_to_node(plan, rule, ctx, work)?;
    let (plan, child_changed) = rewrite_children(plan, rule, ctx, work)?;
    Ok((plan, node_changed || child_changed))
}

fn rewrite_bottom_up(
    plan: OptExpr,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(OptExpr, bool), SqlCompileError> {
    let (plan, child_changed) = rewrite_children(plan, rule, ctx, work)?;
    let (plan, node_changed) = apply_rule_to_node(plan, rule, ctx, work)?;
    Ok((plan, child_changed || node_changed))
}

fn apply_rule_to_node(
    plan: OptExpr,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(OptExpr, bool), SqlCompileError> {
    if super::tree_binder::bind_tree_observed(&rule.pattern(), &plan, work)?.is_none() {
        return Ok((plan, false));
    }

    if !rule.matches(&plan, ctx) {
        return Ok((plan, false));
    }

    ctx.check_deadline(rule.name())?;
    // OptExpr's derived clone remains opaque: these observations surround the
    // clone but do not claim cooperative work inside its recursive payload.
    let original = plan.clone();
    ctx.check_deadline(rule.name())?;
    let phase = rule.phase();
    let rule_name = rule.name();
    ctx.trace_mut().rule_matched(phase, rule_name);

    let start = Instant::now();
    let applied = rule.apply(plan, ctx);
    // A request stop is never a rule diagnostic or a rejected candidate.
    if applied.is_ok() {
        ctx.check_deadline(rule_name)?;
    }
    match applied {
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
                RewriteFailurePolicy::FailFast => Err(SqlCompileError::Compilation(message)),
            }
        }
        Err(error) => {
            if let SqlCompileError::Compilation(message) = &error {
                ctx.trace_mut()
                    .rule_failed(phase, rule_name, message.clone());
            }
            Err(error)
        }
    }
}

fn rewrite_children(
    mut plan: OptExpr,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(OptExpr, bool), SqlCompileError> {
    let (children, changed) =
        rewrite_plan_list(std::mem::take(&mut plan.children), rule, ctx, work)?;
    plan.children = children;
    Ok((plan, changed))
}

fn rewrite_plan_list(
    inputs: Vec<OptExpr>,
    rule: &dyn LogicalRewriteRule,
    ctx: &mut RewriteContext,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Vec<OptExpr>, bool), SqlCompileError> {
    let mut changed = false;
    let mut rewritten = Vec::with_capacity(inputs.len());
    for input in inputs {
        let (input, input_changed) = rewrite_with_rule_inner(input, rule, ctx, work)?;
        changed |= input_changed;
        rewritten.push(input);
    }
    Ok((rewritten, changed))
}

#[cfg(test)]
mod tests {
    use crate::compiler::SqlCompileError;
    use arrow::datatypes::DataType;

    use super::rewrite_with_rule;
    use crate::analysis::OutputColumn;
    use crate::column_id::ColumnId;
    use crate::optimizer::operator::{Operator, ProjectOp, ScalarProjectItem, ScanOp, ValuesOp};
    use crate::optimizer::opt_expr::OptExpr;
    use crate::optimizer::pattern::{OpKind, Pattern};
    use crate::optimizer::rewrite::context::RewriteContext;
    use crate::optimizer::rewrite::phase::RewritePhase;
    use crate::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
    use crate::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
    use crate::optimizer::rewrite::trace::RewriteTraceEvent;
    use crate::planner::table::TableDef;
    use novarocks_types::schema::ColumnDef;

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
        ) -> Result<RewriteResult, SqlCompileError> {
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
        ) -> Result<RewriteResult, SqlCompileError> {
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
            ) -> Result<RewriteResult, SqlCompileError> {
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
        use crate::analysis::{ExprKind, TypedExpr};
        use crate::optimizer::scalar::ScalarArena;

        use crate::planner::optimizer_bridge::scalar::intern_typed;
        let output = output_column("c1");
        let mut arena = ScalarArena::new();
        let col_expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: output.column_id,
                qualifier: None,
                column: "c1".to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        };
        let expr_id = intern_typed(
            &mut arena,
            &col_expr,
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();
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
            source: crate::compiler::mv_rewrite::test_scan_source(
                crate::planner::table::SqlScanKind::ConnectorRead,
            ),
        }
    }

    fn output_column(name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(1),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

            is_internal: false,
        }
    }

    #[test]
    fn rewrite_traverses_into_join_child() {
        // Replaces the former rewrite_traverses_into_imv_delta_child test.
        // LogicalJoin is available in Operator and exercises the same traversal
        // path: a parent node that is not matched, with a matched child below it.
        use crate::analysis::JoinKind;
        use crate::optimizer::operator::LogicalJoinOp;

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
                naa_author: None,
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
        use crate::optimizer::operator::Operator;
        use crate::optimizer::rewrite::context::RewriteContext;
        use crate::optimizer::rewrite::phase::RewritePhase;
        use crate::optimizer::rewrite::result::RewriteResult;
        use crate::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
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
            ) -> Result<RewriteResult, SqlCompileError> {
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
        use crate::analysis::JoinKind;
        use crate::optimizer::operator::LogicalJoinOp;

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
                naa_author: None,
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

    #[derive(Clone, Copy)]
    enum StopPoint {
        Entry,
        Batch,
        Finish,
    }
    struct ObservedControl {
        units: std::sync::Mutex<Vec<u32>>,
        stop: Option<(StopPoint, novarocks_type_contract::CompileControlError)>,
    }
    impl novarocks_type_contract::PureCompileControl for ObservedControl {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            work_units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            self.units.lock().unwrap().push(work_units);
            if let Some((point, error)) = self.stop.clone() {
                let stop = match point {
                    StopPoint::Entry => work_units == 0,
                    StopPoint::Batch => work_units == 256,
                    StopPoint::Finish => work_units > 0 && work_units < 256,
                };
                if stop {
                    return Err(error);
                }
            }
            Ok(())
        }
    }
    struct NeverMatches {
        visits: std::sync::atomic::AtomicUsize,
    }

    #[test]
    fn one_shot_rule_control_failure_survives_successful_followup_checkpoint() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        struct OneShotControl {
            failure: CompileControlError,
            fired: AtomicBool,
            successful_after_failure: AtomicUsize,
        }
        impl PureCompileControl for OneShotControl {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                if units == 17 && !self.fired.swap(true, Ordering::SeqCst) {
                    return Err(self.failure.clone());
                }
                if self.fired.load(Ordering::SeqCst) {
                    self.successful_after_failure.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            }
        }
        struct ControlledRule;
        impl LogicalRewriteRule for ControlledRule {
            fn name(&self) -> &'static str {
                "ControlledRule"
            }
            fn phase(&self) -> RewritePhase {
                RewritePhase::LogicalNormalize
            }
            fn matches(&self, _: &OptExpr, _: &RewriteContext) -> bool {
                true
            }
            fn apply(
                &self,
                _: OptExpr,
                ctx: &mut RewriteContext,
            ) -> Result<RewriteResult, SqlCompileError> {
                ctx.control_view().checkpoint(CompilePhase::Validate, 17)?;
                panic!("one-shot control must stop the actual rule")
            }
        }

        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = OneShotControl {
                failure: failure.clone(),
                fired: AtomicBool::new(false),
                successful_after_failure: AtomicUsize::new(0),
            };
            let mut ctx = RewriteContext::for_query_with_settings(
                Default::default(),
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                &control,
            );
            let input = OptExpr::leaf(Operator::LogicalValues(ValuesOp {
                rows: vec![vec![]],
                columns: vec![],
            }));
            let error = rewrite_with_rule(input, &ControlledRule, &mut ctx).unwrap_err();
            assert_eq!(error, SqlCompileError::from(failure));
            assert!(control.fired.load(Ordering::SeqCst));
            control.checkpoint(CompilePhase::Validate, 0).unwrap();
            assert_eq!(control.successful_after_failure.load(Ordering::SeqCst), 1);
            assert!(!ctx.trace().events().iter().any(|event| matches!(
                event,
                RewriteTraceEvent::RuleFailed { .. } | RewriteTraceEvent::RuleRejected { .. }
            )));
        }
    }
    impl LogicalRewriteRule for NeverMatches {
        fn name(&self) -> &'static str {
            "NeverMatches"
        }
        fn phase(&self) -> RewritePhase {
            RewritePhase::StructuralRewrite
        }
        fn matches(&self, _: &OptExpr, _: &RewriteContext) -> bool {
            self.visits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            false
        }
        fn apply(
            &self,
            _: OptExpr,
            _: &mut RewriteContext,
        ) -> Result<RewriteResult, SqlCompileError> {
            panic!("structurally unmatched rule must never run")
        }
    }
    fn wide_union() -> OptExpr {
        OptExpr::new(
            Operator::LogicalUnion(crate::optimizer::operator::UnionOp {
                all: true,
                output_columns: vec![],
                child_output_columns: vec![vec![]; 320],
            }),
            (0..320)
                .map(|_| {
                    OptExpr::leaf(Operator::LogicalValues(ValuesOp {
                        rows: vec![vec![]],
                        columns: vec![],
                    }))
                })
                .collect(),
        )
    }
    #[test]
    fn rewrite_accounts_actual_wide_nodes_and_finishes_pending_work() {
        let control = ObservedControl {
            units: Default::default(),
            stop: None,
        };
        let mut ctx = RewriteContext::for_query_with_settings(
            Default::default(),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            &control,
        );
        let rule = NeverMatches {
            visits: Default::default(),
        };
        let (output, changed) = rewrite_with_rule(wide_union(), &rule, &mut ctx).unwrap();
        assert!(!changed);
        assert_eq!(output.children.len(), 320);
        assert_eq!(rule.visits.load(std::sync::atomic::Ordering::SeqCst), 321);
        assert_eq!(*control.units.lock().unwrap(), vec![0, 256, 256, 130]);
    }
    #[test]
    fn rewrite_keeps_all_control_categories_at_entry_batch_and_finish() {
        use novarocks_type_contract::CompileControlError as Error;
        for error in [
            Error::Cancelled,
            Error::DeadlineExceeded,
            Error::ResourceExhausted,
        ] {
            for point in [StopPoint::Entry, StopPoint::Batch, StopPoint::Finish] {
                let control = ObservedControl {
                    units: Default::default(),
                    stop: Some((point, error.clone())),
                };
                let mut ctx = RewriteContext::for_query_with_settings(
                    Default::default(),
                    novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    &control,
                );
                let rule = NeverMatches {
                    visits: Default::default(),
                };
                let failure = rewrite_with_rule(wide_union(), &rule, &mut ctx).unwrap_err();
                assert_eq!(
                    failure,
                    crate::compiler::SqlCompileError::from(error.clone())
                );
                assert!(
                    ctx.trace().events().is_empty(),
                    "control must not become a rule diagnostic"
                );
                let units = control.units.lock().unwrap();
                match point {
                    StopPoint::Entry => {
                        assert_eq!(*units, vec![0]);
                        assert_eq!(rule.visits.load(std::sync::atomic::Ordering::SeqCst), 0);
                    }
                    StopPoint::Batch => {
                        assert_eq!(*units, vec![0, 256]);
                        assert!(rule.visits.load(std::sync::atomic::Ordering::SeqCst) < 320);
                    }
                    StopPoint::Finish => assert_eq!(*units, vec![0, 256, 256, 130]),
                }
            }
        }
    }
    #[test]
    fn successful_rewrite_output_does_not_retain_control() {
        let owner = std::sync::Arc::new(ObservedControl {
            units: Default::default(),
            stop: None,
        });
        let weak = std::sync::Arc::downgrade(&owner);
        let mut ctx = RewriteContext::for_query_with_settings(
            Default::default(),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            owner.as_ref(),
        );
        let cloned = ctx.clone();
        let rule = NeverMatches {
            visits: Default::default(),
        };
        let (output, _) = rewrite_with_rule(wide_union(), &rule, &mut ctx).unwrap();
        drop(cloned);
        drop(ctx);
        drop(owner);
        assert!(weak.upgrade().is_none());
        assert_eq!(output.children.len(), 320);
    }

    #[test]
    fn actual_wide_structural_gate_stops_before_field_matching_or_rule_apply() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        struct StructuralRule {
            matches: AtomicUsize,
            applies: AtomicUsize,
        }
        impl LogicalRewriteRule for StructuralRule {
            fn name(&self) -> &'static str {
                "WideStructuralRule"
            }
            fn phase(&self) -> RewritePhase {
                RewritePhase::StructuralRewrite
            }
            fn traversal(&self) -> RewriteTraversal {
                RewriteTraversal::TopDown
            }
            fn pattern(&self) -> Pattern {
                Pattern::Op {
                    kind: OpKind::Union,
                    children: (0..320)
                        .map(|_| Pattern::Op {
                            kind: OpKind::Values,
                            children: vec![],
                        })
                        .collect(),
                }
            }
            fn matches(&self, _: &OptExpr, _: &RewriteContext) -> bool {
                self.matches.fetch_add(1, Ordering::SeqCst);
                true
            }
            fn apply(
                &self,
                _: OptExpr,
                _: &mut RewriteContext,
            ) -> Result<RewriteResult, SqlCompileError> {
                self.applies.fetch_add(1, Ordering::SeqCst);
                Ok(RewriteResult::Unchanged)
            }
        }
        struct OneShotBatch {
            error: CompileControlError,
            fired: AtomicBool,
            units: std::sync::Mutex<Vec<u32>>,
        }
        impl PureCompileControl for OneShotBatch {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                self.units.lock().unwrap().push(units);
                if units == 256 && !self.fired.swap(true, Ordering::SeqCst) {
                    return Err(self.error.clone());
                }
                Ok(())
            }
        }
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = OneShotBatch {
                error: error.clone(),
                fired: AtomicBool::new(false),
                units: Default::default(),
            };
            let mut ctx = RewriteContext::for_query_with_settings(
                Default::default(),
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                &control,
            );
            let rule = StructuralRule {
                matches: AtomicUsize::new(0),
                applies: AtomicUsize::new(0),
            };
            assert_eq!(
                rewrite_with_rule(wide_union(), &rule, &mut ctx).unwrap_err(),
                SqlCompileError::from(error)
            );
            assert_eq!(rule.matches.load(Ordering::SeqCst), 0);
            assert_eq!(rule.applies.load(Ordering::SeqCst), 0);
            assert!(ctx.trace().events().is_empty());
            assert_eq!(*control.units.lock().unwrap(), vec![0, 256]);
            control.checkpoint(CompilePhase::Validate, 0).unwrap();
        }
    }
}
