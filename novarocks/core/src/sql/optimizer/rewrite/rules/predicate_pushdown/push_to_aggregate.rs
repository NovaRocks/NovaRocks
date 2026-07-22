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

//! PushDownPredicateAggregate — `Filter(Aggregate)` rewrite.
//!
//! Pushes conjuncts whose refs are entirely GROUP BY key columns below
//! the aggregate. Predicates referencing aggregate outputs (computed
//! expressions) remain above. Constant predicates stay above too —
//! legacy does not push them because aggregate pushability requires at
//! least one GROUP-BY-key reference (`!refs.is_empty()` guard, deliberate
//! asymmetry vs. Project/Scan).
//!
//! Mirrors legacy `push_predicates_through_aggregate`. Does not recurse.
//!
//! Migrated to `OptExpr` / `LogicalRewriteRule`.

use std::collections::HashSet;

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{FilterOp, Operator};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::wrap_remaining_filter_opt_scalar;
use crate::sql::optimizer::scalar::ScalarNode;
use crate::sql::optimizer::scalar_expr;

pub(crate) struct PushDownPredicateAggregate;

impl LogicalRewriteRule for PushDownPredicateAggregate {
    fn name(&self) -> &'static str {
        "PushDownPredicateAggregate"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Filter,
            children: vec![Pattern::Op {
                kind: OpKind::Aggregate,
                children: vec![Pattern::MultiLeaf],
            }],
        }
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        if expr.children.is_empty() {
            return false;
        }
        let input = expr.unary_input();
        if input.children.is_empty() {
            return false;
        }
        !aggregate_child_is_repeat(input.unary_input())
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            mut children,
            required_output_columns: _,
        } = expr;
        let Operator::LogicalFilter(filter_op) = op else {
            return Ok(RewriteResult::Unchanged);
        };
        if children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let aggregate_expr = children.remove(0);
        let OptExpr {
            op: agg_op,
            children: agg_children_owned,
            required_output_columns: aggregate_required_output_columns,
        } = aggregate_expr;
        let mut agg_children = agg_children_owned;
        let Operator::LogicalAggregate(agg) = agg_op else {
            return Ok(RewriteResult::Unchanged);
        };
        if agg_children.len() != 1 {
            return Ok(RewriteResult::Unchanged);
        }
        let aggregate_input = agg_children.remove(0);

        // ROLLUP/CUBE/GROUPING SETS guard: a Repeat below the aggregate
        // synthesizes subtotal rows where GROUP BY key columns are NULL in the
        // aggregate's *output*. A predicate that holds on the output does NOT
        // hold on the aggregate's input, so pushing it below would drop subtotal
        // rows (wrong results).
        if aggregate_child_is_repeat(&aggregate_input) {
            return Ok(RewriteResult::Unchanged);
        }

        let arena_rc = ctx.scalar_arena();
        let mut arena = arena_rc.borrow_mut();

        // GROUP BY key ColumnIds — only bare ColumnRef items contribute
        // pushable ids; computed GROUP BY expressions do not.
        let group_by_ids: HashSet<ColumnId> = agg
            .group_by
            .iter()
            .filter_map(|&id| match arena.node(id) {
                ScalarNode::ColumnRef(column_id) if *column_id != ColumnId::UNSET => {
                    Some(*column_id)
                }
                _ => None,
            })
            .collect();

        let mut conjuncts = Vec::new();
        scalar_expr::split_conjuncts(&arena, filter_op.predicate, &mut conjuncts);
        let mut pushable = Vec::new();
        let mut remaining = Vec::new();
        for conj in conjuncts {
            let refs = scalar_expr::collect_column_ids_strict(&arena, conj);
            // Keep the `!refs.is_empty()` guard: constant predicates (empty
            // refs) are not pushed through aggregates.
            if let Some(refs) = refs
                && !refs.is_empty()
                && refs.iter().all(|id| group_by_ids.contains(id))
            {
                pushable.push(conj);
            } else {
                remaining.push(conj);
            }
        }

        if pushable.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }

        let Some(pushed_id) = scalar_expr::combine_conjuncts(&mut arena, pushable) else {
            return Ok(RewriteResult::Unchanged);
        };
        let new_child = OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: pushed_id,
            }),
            vec![aggregate_input],
        );
        let mut new_agg_expr = OptExpr::new(Operator::LogicalAggregate(agg), vec![new_child]);
        new_agg_expr.required_output_columns = aggregate_required_output_columns;

        let result = wrap_remaining_filter_opt_scalar(new_agg_expr, remaining, &mut arena);
        Ok(RewriteResult::Changed(result))
    }
}

/// True if the aggregate's input is (or passes through to) a Repeat node —
/// i.e. this is a ROLLUP / CUBE / GROUPING SETS aggregate whose GROUP BY keys
/// can be NULL in its output, so output-level predicates must not be pushed
/// below it.
fn aggregate_child_is_repeat(expr: &OptExpr) -> bool {
    match &expr.op {
        Operator::LogicalRepeat(_) => true,
        Operator::LogicalFilter(_) | Operator::LogicalProject(_) => {
            if expr.children.is_empty() {
                false
            } else {
                aggregate_child_is_repeat(expr.unary_input())
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::TypedExpr;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        AggregateOutputLayout, LogicalAggregateOp, RepeatOp, ScalarAggregateSpec, ScanOp,
    };
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::tree_binder::bind_tree;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use arrow::datatypes::DataType;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn test_col_id(name: &str) -> ColumnId {
        match name {
            "a" => ColumnId::new_for_test(1),
            "b" => ColumnId::new_for_test(2),
            "sum_b" => ColumnId::new_for_test(3),
            _ => ColumnId::new_for_test(100),
        }
    }

    fn col_typed_expr(name: &str) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(name),
                qualifier: None,
                column: name.into(),
            },
        }
    }

    fn int_lit_expr(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }

    fn eq_expr(a: TypedExpr, b: TypedExpr) -> TypedExpr {
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

    fn output_col(name: &str) -> OutputColumn {
        OutputColumn {
            column_id: test_col_id(name),
            name: name.into(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn make_scan(arena: &mut ScalarArena) -> OptExpr {
        let table = TableDef {
            name: "t".into(),
            columns: vec![
                ColumnDef {
                    name: "a".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "b".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        };
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table,
            alias: None,
            stats_ref: None,
            columns: vec![output_col("a"), output_col("b")],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn make_repeat(input: OptExpr) -> OptExpr {
        OptExpr::new(
            Operator::LogicalRepeat(RepeatOp {
                repeat_column_ref_list: vec![],
                repeat_column_ref_ids: vec![],
                grouping_ids: vec![],
                all_rollup_columns: vec![],
                all_rollup_column_ids: vec![],
                grouping_key_aliases: vec![],
                grouping_fn_args: vec![],
                grouping_fn_arg_ids: vec![],
                grouping_fn_ids: vec![],
            }),
            vec![input],
        )
    }

    fn make_agg(arena: &mut ScalarArena, input: OptExpr) -> OptExpr {
        let group_by = vec![intern_typed(arena, &col_typed_expr("a"))];
        let count_spec = ScalarAggregateSpec {
            output_column_id: test_col_id("sum_b"),
            name: "sum".into(),
            args: vec![intern_typed(arena, &col_typed_expr("b"))],
            distinct: false,
            order_by: vec![],
        };
        let aggregates = vec![count_spec];
        let output_columns = vec![output_col("a"), output_col("sum_b")];
        let output_layout = AggregateOutputLayout::new(
            output_columns
                .iter()
                .take(group_by.len())
                .cloned()
                .collect(),
            output_columns
                .iter()
                .skip(group_by.len())
                .cloned()
                .collect(),
        );
        let agg_op =
            LogicalAggregateOp::single(group_by, aggregates, output_layout, output_columns);
        OptExpr::new(Operator::LogicalAggregate(agg_op), vec![input])
    }

    fn make_ctx(arena: ScalarArena) -> RewriteContext {
        let mut ctx = RewriteContext::for_query(std::iter::empty::<String>());
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));
        ctx
    }

    // Test 1: WHERE a = 1, GROUP BY a, SUM(b) → predicate is pushable below the aggregate.
    // Expected shape: Aggregate(Filter(Scan))
    #[test]
    fn pushes_group_by_column_predicate() {
        let mut arena = ScalarArena::new();
        let scan = make_scan(&mut arena);
        let agg = make_agg(&mut arena, scan);
        let filter_pred = intern_typed(&mut arena, &eq_expr(col_typed_expr("a"), int_lit_expr(1)));
        let filter = OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: filter_pred,
            }),
            vec![agg],
        );

        let rule = PushDownPredicateAggregate;
        let mut ctx = make_ctx(arena);
        assert!(bind_tree(&rule.pattern(), &filter).is_some());
        assert!(rule.matches(&filter, &ctx));
        let result = rule.apply(filter, &mut ctx).unwrap();
        let RewriteResult::Changed(out) = result else {
            panic!("expected Changed result");
        };

        // Expected: Aggregate(Filter(Scan))
        match &out.op {
            Operator::LogicalAggregate(_) => match &out.unary_input().op {
                Operator::LogicalFilter(_) => match &out.unary_input().unary_input().op {
                    Operator::LogicalScan(_) => {}
                    other => panic!("expected Scan under Filter, got {:?}", other),
                },
                other => panic!("expected Filter under Aggregate, got {:?}", other),
            },
            other => panic!("expected Aggregate at top, got {:?}", other),
        }
    }

    // Test 2: WHERE sum_b = 100, GROUP BY a, SUM(b)
    // sum_b is an aggregate output column, not a GROUP BY key → not pushable.
    // Rule must return Unchanged.
    #[test]
    fn does_not_push_aggregate_output_predicate() {
        let mut arena = ScalarArena::new();
        let scan = make_scan(&mut arena);
        let agg = make_agg(&mut arena, scan);
        let filter_pred = intern_typed(
            &mut arena,
            &eq_expr(col_typed_expr("sum_b"), int_lit_expr(100)),
        );
        let filter = OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: filter_pred,
            }),
            vec![agg],
        );

        let rule = PushDownPredicateAggregate;
        let mut ctx = make_ctx(arena);
        assert!(bind_tree(&rule.pattern(), &filter).is_some());
        assert!(rule.matches(&filter, &ctx));
        let result = rule.apply(filter, &mut ctx).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "aggregate output predicate must not be pushed below the aggregate"
        );
    }

    // Test 3: WHERE 1 = 1 (constant predicate — no column refs)
    // The `!refs.is_empty()` guard keeps this above. Must return Unchanged.
    #[test]
    fn does_not_push_constant_predicate() {
        let mut arena = ScalarArena::new();
        let scan = make_scan(&mut arena);
        let agg = make_agg(&mut arena, scan);
        let filter_pred = intern_typed(&mut arena, &eq_expr(int_lit_expr(1), int_lit_expr(1)));
        let filter = OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: filter_pred,
            }),
            vec![agg],
        );

        let rule = PushDownPredicateAggregate;
        let mut ctx = make_ctx(arena);
        assert!(bind_tree(&rule.pattern(), &filter).is_some());
        assert!(rule.matches(&filter, &ctx));
        let result = rule.apply(filter, &mut ctx).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "constant predicate must not be pushed through an aggregate"
        );
    }

    #[test]
    fn pattern_matches_filter_aggregate_but_matches_rejects_repeat_child() {
        let mut arena = ScalarArena::new();
        let scan = make_scan(&mut arena);
        let repeat = make_repeat(scan);
        let agg = make_agg(&mut arena, repeat);
        let filter_pred = intern_typed(&mut arena, &eq_expr(col_typed_expr("a"), int_lit_expr(1)));
        let filter = OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: filter_pred,
            }),
            vec![agg],
        );

        let rule = PushDownPredicateAggregate;
        let ctx = make_ctx(arena);
        assert!(bind_tree(&rule.pattern(), &filter).is_some());
        assert!(!rule.matches(&filter, &ctx));
    }
}
