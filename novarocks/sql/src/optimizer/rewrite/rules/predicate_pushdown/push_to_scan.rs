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

//! PushDownPredicateScan — `Filter(Scan)` rewrite.
//!
//! Pushes filter conjuncts into `ScanOp.predicates` when every column
//! the conjunct references is present in the scan's output. Unpushable
//! conjuncts are wrapped back as a residual `Filter` above the scan.
//!
//! Migrated to `OptExpr` / `LogicalRewriteRule`.

use crate::compiler::SqlCompileError;
use std::collections::{HashMap, HashSet};

use crate::optimizer::operator::Operator;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::pattern::{OpKind, Pattern};
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::optimizer::rewrite::rules::utils::wrap_remaining_filter_opt_scalar;
use crate::optimizer::scalar_expr;

pub(crate) struct PushDownPredicateScan;

impl LogicalRewriteRule for PushDownPredicateScan {
    fn name(&self) -> &'static str {
        "PushDownPredicateScan"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Filter,
            children: vec![Pattern::Op {
                kind: OpKind::Scan,
                children: vec![Pattern::MultiLeaf],
            }],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(
        &self,
        expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError> {
        let OptExpr {
            op,
            mut children,
            required_output_columns: _,
        } = expr;
        let Operator::LogicalFilter(filter) = op else {
            return Ok(RewriteResult::Unchanged);
        };
        if children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let scan_expr = children.remove(0);
        let OptExpr {
            op: scan_op,
            required_output_columns,
            ..
        } = scan_expr;
        let Operator::LogicalScan(mut scan) = scan_op else {
            return Ok(RewriteResult::Unchanged);
        };

        let arena_rc = ctx.scalar_arena();
        let mut pushed = Vec::new();
        let mut remaining = Vec::new();
        {
            let arena = arena_rc.borrow();
            // A connector may execute a pushed atom before every scan residual.
            // If it leaves the atom to SQL, residual evaluation preserves the
            // supplied order. Both placements must be allowed to remove rows.
            if scan.predicates.iter().any(|&predicate| {
                !arena.effects(predicate).permits_filter_row_elimination
                    || scalar_expr::is_internal_assertion(&arena, predicate)
            }) {
                return Ok(RewriteResult::Unchanged);
            }
            let columns: HashMap<_, _> = scan
                .columns
                .iter()
                .map(|column| (column.column_id, &column.value_type))
                .collect();
            if columns.len() != scan.columns.len() {
                return Ok(RewriteResult::Unchanged);
            }
            let mut conjuncts = Vec::new();
            scalar_expr::split_conjuncts(&arena, filter.predicate, &mut conjuncts);
            let mut seen = HashSet::new();
            let mut can_move = true;
            for conjunct in conjuncts {
                let effects = arena.effects(conjunct);
                let may_eliminate_rows = effects.permits_filter_row_elimination
                    && !scalar_expr::is_internal_assertion(&arena, conjunct);
                if can_move
                    && may_eliminate_rows
                    && effects.expression.permits_earlier_evaluation()
                    && scalar_expr::all_column_refs_match(&arena, conjunct, |id, ty| {
                        columns
                            .get(&id)
                            .is_some_and(|&column_type| column_type == ty)
                    })
                {
                    // Interner equality is exact typed definition equality. A
                    // total pure conjunct needs no second occurrence on surviving rows.
                    if seen.insert(conjunct) {
                        pushed.push(conjunct);
                    }
                } else {
                    remaining.push(conjunct);
                    can_move &= may_eliminate_rows;
                }
            }
            if pushed.is_empty() {
                return Ok(RewriteResult::Unchanged);
            }
            scan.predicates
                .retain(|predicate| !seen.contains(predicate));
        }

        // Keep these before surviving scan residuals even when the connector
        // does not consume them. Existing residuals stay in their original order.
        pushed.append(&mut scan.predicates);
        scan.predicates = pushed;

        let new_scan = OptExpr {
            op: Operator::LogicalScan(scan),
            children: vec![],
            required_output_columns,
        };

        let result = wrap_remaining_filter_opt_scalar(
            new_scan,
            remaining,
            &mut arena_rc.borrow_mut(),
            &ctx.control_view(),
        )?;
        Ok(RewriteResult::Changed(result))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::operator::{FilterOp, Operator, ScanOp};
    use crate::optimizer::opt_expr::OptExpr;
    use crate::optimizer::rewrite::context::RewriteContext;
    use crate::optimizer::rewrite::tree_binder::bind_tree;
    use crate::optimizer::scalar::ScalarArena;
    use crate::planner::table::TableDef;
    use arrow::datatypes::DataType;
    use novarocks_types::schema::ColumnDef;

    fn test_col_id(name: &str) -> ColumnId {
        match name {
            "a" => ColumnId::new_for_test(1),
            "b" => ColumnId::new_for_test(2),
            "zz" => ColumnId::new_for_test(99),
            _ => ColumnId::new_for_test(100),
        }
    }

    fn col_with_id(name: &str, column_id: ColumnId) -> TypedExpr {
        TypedExpr {
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),

            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: name.into(),
            },
        }
    }

    fn col(name: &str) -> TypedExpr {
        col_with_id(name, test_col_id(name))
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }

    fn eq(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),

            kind: ExprKind::BinaryOp {
                left: novarocks_parser::ast::AstBox::new(a),
                op: BinOp::Eq,
                right: novarocks_parser::ast::AstBox::new(b),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
        }
    }

    fn and(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),

            kind: ExprKind::BinaryOp {
                left: novarocks_parser::ast::AstBox::new(a),
                op: BinOp::And,
                right: novarocks_parser::ast::AstBox::new(b),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
        }
    }

    fn is_not_null(e: TypedExpr) -> TypedExpr {
        TypedExpr {
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),

            kind: ExprKind::IsNull {
                expr: novarocks_parser::ast::AstBox::new(e),
                negated: true,
            },
        }
    }

    fn make_table_def(cols: &[&str]) -> TableDef {
        TableDef {
            name: "t".into(),
            columns: cols
                .iter()
                .map(|n| ColumnDef {
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                })
                .collect(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::compiler::mv_rewrite::test_scan_source(
                crate::planner::table::SqlScanKind::ConnectorRead,
            ),
        }
    }

    fn scan_opt(_arena: &mut ScalarArena, cols: &[&str]) -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table: make_table_def(cols),
            alias: None,
            stats_ref: None,
            columns: cols
                .iter()
                .map(|n| OutputColumn {
                    column_id: test_col_id(n),
                    name: (*n).into(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        true,
                    ),

                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn filter_opt(arena: &mut ScalarArena, predicate: TypedExpr, child: OptExpr) -> OptExpr {
        let pred_id = crate::planner::optimizer_bridge::scalar::intern_typed(
            arena,
            &predicate,
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();
        OptExpr::new(
            Operator::LogicalFilter(FilterOp { predicate: pred_id }),
            vec![child],
        )
    }

    fn make_ctx(arena: ScalarArena) -> RewriteContext<'static> {
        let mut ctx = RewriteContext::for_query(std::iter::empty::<String>());
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));
        ctx
    }

    #[test]
    fn pushes_single_scan_column_predicate() {
        let mut arena = ScalarArena::new();
        let scan = scan_opt(&mut arena, &["a", "b"]);
        let filter = filter_opt(&mut arena, eq(col("a"), int_lit(1)), scan);
        let rule = PushDownPredicateScan;
        let mut ctx = make_ctx(arena);
        assert!(
            bind_tree(
                &rule.pattern(),
                &filter,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
            .is_some()
        );
        let result = rule.apply(filter, &mut ctx).unwrap();
        match result {
            RewriteResult::Changed(out) => match &out.op {
                Operator::LogicalScan(s) => {
                    assert_eq!(s.predicates.len(), 1);
                }
                other => panic!("expected bare Scan after full pushdown, got {:?}", other),
            },
            other => panic!("expected Changed, got {:?}", other),
        }
    }

    #[test]
    fn leaves_unmatched_shape_alone() {
        let mut arena = ScalarArena::new();
        let scan = scan_opt(&mut arena, &["a"]);
        let rule = PushDownPredicateScan;
        assert!(
            bind_tree(
                &rule.pattern(),
                &scan,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn pattern_rejects_filter_over_project() {
        let mut arena = ScalarArena::new();
        let scan = scan_opt(&mut arena, &["a"]);
        let project = OptExpr::new(
            Operator::LogicalProject(crate::optimizer::operator::ProjectOp {
                items: vec![],
                output_qualifier: None,
            }),
            vec![scan],
        );
        let filter = filter_opt(&mut arena, eq(col("a"), int_lit(1)), project);
        let rule = PushDownPredicateScan;
        assert!(
            bind_tree(
                &rule.pattern(),
                &filter,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn returns_unchanged_when_nothing_pushed() {
        // Filter references a column the scan does not expose.
        let mut arena = ScalarArena::new();
        let scan = scan_opt(&mut arena, &["a"]);
        let filter = filter_opt(&mut arena, eq(col("zz"), int_lit(1)), scan);
        let rule = PushDownPredicateScan;
        let mut ctx = make_ctx(arena);
        let result = rule.apply(filter, &mut ctx).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "expected Unchanged when no conjunct can push"
        );
    }

    #[test]
    fn p4_scan_does_not_push_same_name_with_different_column_id() {
        let mut arena = ScalarArena::new();
        let scan = scan_opt(&mut arena, &["a"]);
        let filter = filter_opt(
            &mut arena,
            eq(col_with_id("a", ColumnId::new_for_test(77)), int_lit(1)),
            scan,
        );
        let rule = PushDownPredicateScan;
        let mut ctx = make_ctx(arena);
        let result = rule.apply(filter, &mut ctx).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "same name must not push when ColumnId is not produced by the scan"
        );
    }

    #[test]
    fn partial_pushdown_leaves_residual_filter() {
        let mut arena = ScalarArena::new();
        let scan = scan_opt(&mut arena, &["a"]);
        let pred = and(eq(col("a"), int_lit(1)), eq(col("zz"), int_lit(2)));
        let filter = filter_opt(&mut arena, pred, scan);
        let mut ctx = make_ctx(arena);
        let result = PushDownPredicateScan.apply(filter, &mut ctx).unwrap();
        match result {
            RewriteResult::Changed(out) => match &out.op {
                Operator::LogicalFilter(_) => match &out.children[0].op {
                    Operator::LogicalScan(s) => assert_eq!(s.predicates.len(), 1),
                    other => panic!("expected Scan under residual Filter, got {:?}", other),
                },
                other => panic!(
                    "expected Filter(Scan) for partial pushdown, got {:?}",
                    other
                ),
            },
            other => panic!("expected Changed, got {:?}", other),
        }
    }

    #[test]
    fn repeated_identical_predicate_dedups_to_one() {
        let mut arena = ScalarArena::new();
        let scan = scan_opt(&mut arena, &["a", "b"]);
        let filter = filter_opt(&mut arena, is_not_null(col("a")), scan);
        let mut ctx = make_ctx(arena);
        let after_round1 = PushDownPredicateScan.apply(filter, &mut ctx).unwrap();
        let scan_after_round1 = match after_round1 {
            RewriteResult::Changed(out) => match out.op.clone() {
                Operator::LogicalScan(s) => {
                    assert_eq!(s.predicates.len(), 1, "first push lands one predicate");
                    (out, s)
                }
                other => panic!("expected bare Scan after full pushdown, got {:?}", other),
            },
            other => panic!("expected Changed, got {:?}", other),
        };
        let (scan_out, _scan_op) = scan_after_round1;
        // Round 2: same predicate above scan that already carries it.
        let pred_id = {
            let arena_rc = ctx.scalar_arena();
            let mut arena = arena_rc.borrow_mut();
            crate::planner::optimizer_bridge::scalar::intern_typed(
                &mut arena,
                &is_not_null(col("a")),
                crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
            )
            .unwrap()
        };
        let filter2 = OptExpr::new(
            Operator::LogicalFilter(FilterOp { predicate: pred_id }),
            vec![scan_out],
        );
        let after_round2 = PushDownPredicateScan.apply(filter2, &mut ctx).unwrap();
        match after_round2 {
            RewriteResult::Changed(out) => match &out.op {
                Operator::LogicalScan(s) => assert_eq!(
                    s.predicates.len(),
                    1,
                    "re-pushing an identical predicate must not accumulate"
                ),
                other => panic!("expected bare Scan after dedup, got {:?}", other),
            },
            other => panic!("expected Changed, got {:?}", other),
        }
    }

    #[test]
    fn duplicate_conjuncts_in_one_filter_dedup() {
        let mut arena = ScalarArena::new();
        let scan = scan_opt(&mut arena, &["a"]);
        let pred = and(is_not_null(col("a")), is_not_null(col("a")));
        let filter = filter_opt(&mut arena, pred, scan);
        let mut ctx = make_ctx(arena);
        let result = PushDownPredicateScan.apply(filter, &mut ctx).unwrap();
        match result {
            RewriteResult::Changed(out) => match &out.op {
                Operator::LogicalScan(s) => assert_eq!(s.predicates.len(), 1),
                other => panic!("expected bare Scan, got {:?}", other),
            },
            other => panic!("expected Changed, got {:?}", other),
        }
    }

    #[test]
    fn distinct_predicates_are_not_collapsed() {
        let mut arena = ScalarArena::new();
        let scan = scan_opt(&mut arena, &["a"]);
        let pred = and(is_not_null(col("a")), eq(col("a"), int_lit(1)));
        let filter = filter_opt(&mut arena, pred, scan);
        let mut ctx = make_ctx(arena);
        let result = PushDownPredicateScan.apply(filter, &mut ctx).unwrap();
        match result {
            RewriteResult::Changed(out) => match &out.op {
                Operator::LogicalScan(s) => assert_eq!(
                    s.predicates.len(),
                    2,
                    "distinct predicates on the same column must be preserved"
                ),
                other => panic!("expected bare Scan, got {:?}", other),
            },
            other => panic!("expected Changed, got {:?}", other),
        }
    }
    fn bound_call(name: &str, arg: TypedExpr) -> TypedExpr {
        use novarocks_functions::{
            FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionResultType,
        };
        let arguments = [FunctionArgument::Value {
            value_type: arg.value_type.clone(),
            constant: None,
        }];
        let binding = crate::functions::builtin_engine_function_catalog()
            .resolve_bound_trusted(
                name,
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: 1,
                },
                crate::optimizer::test_optimizer_control(),
            )
            .unwrap();
        let FunctionResultType::Scalar(value_type) = binding.selected.result_type.clone() else {
            unreachable!()
        };
        let volatility = binding.semantics.volatility;
        TypedExpr {
            value_type,
            kind: ExprKind::FunctionCall {
                name: name.into(),
                args: vec![arg].into(),
                distinct: false,
                binding: crate::binding::SqlFunctionBinding::from_test(
                    binding,
                    novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                ),
                volatility,
            },
        }
    }

    fn ordered_parts(
        arena: &ScalarArena,
        filter: &OptExpr,
    ) -> Vec<crate::optimizer::scalar::ScalarId> {
        let Operator::LogicalFilter(filter) = &filter.op else {
            panic!("expected distinct Filter");
        };
        let mut parts = Vec::new();
        scalar_expr::split_conjuncts(arena, filter.predicate, &mut parts);
        parts
    }

    #[test]
    fn moves_only_the_total_necessary_term_across_selected_wait() {
        let mut arena = ScalarArena::new();
        let scan = scan_opt(&mut arena, &["a", "b"]);
        let checked = eq(
            TypedExpr {
                value_type: col("a").value_type,
                kind: ExprKind::BinaryOp {
                    left: novarocks_parser::ast::AstBox::new(int_lit(12)),
                    op: BinOp::Div,
                    right: novarocks_parser::ast::AstBox::new(col("a")),
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                },
            },
            int_lit(4),
        );
        let wait = bound_call("sleep", col("b"));
        let filter = filter_opt(
            &mut arena,
            and(wait, and(checked, eq(col("a"), int_lit(3)))),
            scan,
        );
        let parts = ordered_parts(&arena, &filter);
        assert!(arena.effects(parts[0]).permits_filter_row_elimination);
        assert!(arena.effects(parts[1]).expression.may_raise_row_error);
        let mut ctx = make_ctx(arena);
        let RewriteResult::Changed(out) = PushDownPredicateScan.apply(filter, &mut ctx).unwrap()
        else {
            panic!("total term moves across the original wait and checked division");
        };
        assert_eq!(
            ordered_parts(&ctx.scalar_arena().borrow(), &out),
            parts[..2]
        );
        let Operator::LogicalScan(scan) = &out.unary_input().op else {
            panic!("residual Filter input is the original Scan");
        };
        assert_eq!(scan.predicates, parts[2..]);
    }

    #[test]
    fn unconsumed_scan_necessary_term_precedes_existing_ordered_residuals() {
        let mut arena = ScalarArena::new();
        let mut scan = scan_opt(&mut arena, &["a", "b"]);
        let [wait, necessary] =
            [bound_call("sleep", col("b")), eq(col("a"), int_lit(3))].map(|predicate| {
                crate::planner::optimizer_bridge::scalar::intern_typed(
                    &mut arena,
                    &predicate,
                    crate::optimizer::test_optimizer_control(),
                )
                .unwrap()
            });
        let Operator::LogicalScan(scan_op) = &mut scan.op else {
            unreachable!()
        };
        // The id already occurs below the wait. The necessary term still needs
        // its earlier slot when the provider retains it as a SQL residual.
        scan_op.predicates = vec![wait, necessary];
        let filter = OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: necessary,
            }),
            vec![scan],
        );
        let mut ctx = make_ctx(arena);
        let RewriteResult::Changed(out) = PushDownPredicateScan.apply(filter, &mut ctx).unwrap()
        else {
            panic!("the already present necessary term can move");
        };
        let Operator::LogicalScan(scan) = &out.op else {
            panic!("no residual Filter");
        };
        assert_eq!(scan.predicates, [necessary, wait]);
    }

    #[test]
    fn necessary_term_cannot_cross_whole_invocation_data() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        let mut arena = ScalarArena::new();
        let mut scan = scan_opt(&mut arena, &["a", "doc"]);
        let doc_type = FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            true,
            ValueLogicalType::Json,
        )
        .unwrap();
        let Operator::LogicalScan(scan_op) = &mut scan.op else {
            unreachable!()
        };
        scan_op.columns[1].value_type = doc_type.clone();
        let mut doc = col("doc");
        doc.value_type = doc_type;
        let atomic = is_not_null(bound_call("__json_canonical_key", doc));
        let filter = filter_opt(&mut arena, and(atomic, eq(col("a"), int_lit(3))), scan);
        let parts = ordered_parts(&arena, &filter);
        assert!(arena.effects(parts[0]).expression.may_raise_invocation_data);
        assert!(!arena.effects(parts[0]).permits_filter_row_elimination);
        let mut ctx = make_ctx(arena);
        assert!(matches!(
            PushDownPredicateScan.apply(filter, &mut ctx).unwrap(),
            RewriteResult::Unchanged
        ));
    }

    #[test]
    fn scan_rejects_same_column_id_with_different_null_promise() {
        let mut arena = ScalarArena::new();
        let scan = scan_opt(&mut arena, &["a"]);
        let mut wrong = col("a");
        wrong.value_type = wrong.value_type.with_nullable(false).unwrap();
        let filter = filter_opt(&mut arena, eq(wrong, int_lit(1)), scan);
        assert!(matches!(
            PushDownPredicateScan
                .apply(filter, &mut make_ctx(arena))
                .unwrap(),
            RewriteResult::Unchanged
        ));
    }
}
