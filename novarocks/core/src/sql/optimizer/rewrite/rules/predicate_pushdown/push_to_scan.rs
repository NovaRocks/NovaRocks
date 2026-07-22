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

use std::collections::HashSet;

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::predicate_key as canonical_predicate_key;
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_output_ids_opt, wrap_remaining_filter_opt_scalar,
};
use crate::sql::optimizer::scalar_expr;

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

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
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
        let mut conjuncts = Vec::new();
        {
            let arena = arena_rc.borrow();
            scalar_expr::split_conjuncts(&arena, filter.predicate, &mut conjuncts);
        }

        // Build a temporary OptExpr to use collect_output_ids_opt.
        let scan_for_ids = OptExpr {
            op: Operator::LogicalScan(scan.clone()),
            children: vec![],
            required_output_columns: required_output_columns.clone(),
        };
        let mut scan_ids = collect_output_ids_opt(&scan_for_ids);
        scan_ids.remove(&crate::sql::column_id::ColumnId::UNSET);

        // Canonical keys of predicates already on the scan to avoid duplicate pushdown.
        let mut seen: HashSet<String> = {
            let arena = arena_rc.borrow();
            scan.predicates
                .iter()
                .map(|id| predicate_key(&arena, *id))
                .collect()
        };

        let mut pushed_any = false;
        let mut remaining = Vec::new();
        for conj in conjuncts {
            let Some(refs) = scalar_expr::collect_column_ids_strict(&arena_rc.borrow(), conj)
            else {
                remaining.push(conj);
                continue;
            };
            if refs.is_empty() || refs.iter().all(|id| scan_ids.contains(id)) {
                if seen.insert(predicate_key(&arena_rc.borrow(), conj)) {
                    scan.predicates.push(conj);
                }
                pushed_any = true;
            } else {
                remaining.push(conj);
            }
        }

        if !pushed_any {
            return Ok(RewriteResult::Unchanged);
        }

        let new_scan = OptExpr {
            op: Operator::LogicalScan(scan),
            children: vec![],
            required_output_columns,
        };

        let result =
            wrap_remaining_filter_opt_scalar(new_scan, remaining, &mut arena_rc.borrow_mut());
        Ok(RewriteResult::Changed(result))
    }
}

/// Canonical key for structural predicate equality.
fn predicate_key(
    arena: &crate::sql::optimizer::scalar::ScalarArena,
    expr: crate::sql::optimizer::scalar::ScalarId,
) -> String {
    canonical_predicate_key(arena, expr).as_str().to_string()
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{FilterOp, Operator, ScanOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::tree_binder::bind_tree;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;

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
            data_type: DataType::Int64,
            nullable: true,
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
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }

    fn eq(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Eq,
                right: Box::new(b),
            },
        }
    }

    fn and(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::And,
                right: Box::new(b),
            },
        }
    }

    fn is_not_null(e: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::IsNull {
                expr: Box::new(e),
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
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn scan_opt(arena: &mut ScalarArena, cols: &[&str]) -> OptExpr {
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
                    data_type: DataType::Int64,
                    nullable: true,
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
        let pred_id =
            crate::sql::planner::optimizer_bridge::scalar::intern_typed(arena, &predicate);
        OptExpr::new(
            Operator::LogicalFilter(FilterOp { predicate: pred_id }),
            vec![child],
        )
    }

    fn make_ctx(arena: ScalarArena) -> RewriteContext {
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
        assert!(bind_tree(&rule.pattern(), &filter).is_some());
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
        assert!(bind_tree(&rule.pattern(), &scan).is_none());
    }

    #[test]
    fn pattern_rejects_filter_over_project() {
        let mut arena = ScalarArena::new();
        let scan = scan_opt(&mut arena, &["a"]);
        let project = OptExpr::new(
            Operator::LogicalProject(crate::sql::optimizer::operator::ProjectOp {
                items: vec![],
                output_qualifier: None,
            }),
            vec![scan],
        );
        let filter = filter_opt(&mut arena, eq(col("a"), int_lit(1)), project);
        let rule = PushDownPredicateScan;
        assert!(bind_tree(&rule.pattern(), &filter).is_none());
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
        let (scan_out, scan_op) = scan_after_round1;
        // Round 2: same predicate above scan that already carries it.
        let pred_id = {
            let arena_rc = ctx.scalar_arena();
            let mut arena = arena_rc.borrow_mut();
            crate::sql::planner::optimizer_bridge::scalar::intern_typed(
                &mut arena,
                &is_not_null(col("a")),
            )
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
}
