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

use std::collections::HashSet;

use crate::sql::column_id::ColumnId;
use crate::sql::common::JoinKind;
use crate::sql::optimizer::operator::{FilterOp, LogicalJoinOp, Operator};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::deriver::derive_inner_join_predicates;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
    PredicateGroup, PredicateKey, PredicateOrigin, predicate_key,
};
use crate::sql::optimizer::rewrite::rules::utils::collect_output_ids_opt;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId};
use crate::sql::optimizer::scalar_expr;

pub(crate) struct JoinPredicateMoveAround;

impl LogicalRewriteRule for JoinPredicateMoveAround {
    fn name(&self) -> &'static str {
        "JoinPredicateMoveAround"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Join,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        let join = join_payload_after_pattern_gate(expr);
        matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) && join.condition.is_some()
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            mut children,
            required_output_columns,
        } = expr;
        let Operator::LogicalJoin(join) = op else {
            return Ok(RewriteResult::Unchanged);
        };
        if children.len() != 2 {
            return Ok(RewriteResult::Unchanged);
        }
        if !matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
            return Ok(RewriteResult::Unchanged);
        }
        let condition_id = match join.condition {
            Some(id) => id,
            None => return Ok(RewriteResult::Unchanged),
        };

        let arena_rc = ctx.scalar_arena();
        let mut arena = arena_rc.borrow_mut();

        let right = children.remove(1);
        let left = children.remove(0);

        let left_ids = collect_output_ids_opt(&left);
        let right_ids = collect_output_ids_opt(&right);

        let join_groups =
            PredicateGroup::from_predicate(&arena, condition_id, PredicateOrigin::JoinCondition);

        let mut child_groups = Vec::new();
        collect_child_predicate_groups(&left, &mut child_groups, &arena);
        collect_child_predicate_groups(&right, &mut child_groups, &arena);

        let derived = derive_inner_join_predicates(
            &mut arena,
            &left_ids,
            &right_ids,
            &join_groups,
            &child_groups,
        );

        let left_existing = existing_child_predicate_keys(&left, &arena);
        let right_existing = existing_child_predicate_keys(&right, &arena);

        let mut left_fresh = Vec::new();
        let mut right_fresh = Vec::new();

        for group in derived {
            match classify_group_side(&group, &left_ids, &right_ids) {
                Some(ChildSide::Left) if !left_existing.contains(&group.key) => {
                    left_fresh.push(group.expr);
                }
                Some(ChildSide::Right) if !right_existing.contains(&group.key) => {
                    right_fresh.push(group.expr);
                }
                _ => {}
            }
        }

        if left_fresh.is_empty() && right_fresh.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }

        let new_left = if left_fresh.is_empty() {
            left
        } else {
            let predicate =
                scalar_expr::combine_conjuncts(&mut arena, left_fresh).expect("non-empty");
            OptExpr::new(Operator::LogicalFilter(FilterOp { predicate }), vec![left])
        };
        let new_right = if right_fresh.is_empty() {
            right
        } else {
            let predicate =
                scalar_expr::combine_conjuncts(&mut arena, right_fresh).expect("non-empty");
            OptExpr::new(Operator::LogicalFilter(FilterOp { predicate }), vec![right])
        };

        let new_join = OptExpr {
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: join.join_type,
                condition: join.condition,
            }),
            children: vec![new_left, new_right],
            required_output_columns,
        };

        Ok(RewriteResult::Changed(new_join))
    }
}

fn join_payload_after_pattern_gate(expr: &OptExpr) -> &LogicalJoinOp {
    let Operator::LogicalJoin(join) = &expr.op else {
        unreachable!("JoinPredicateMoveAround::matches requires Join pattern pre-gate");
    };
    join
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChildSide {
    Left,
    Right,
}

fn classify_group_side(
    group: &PredicateGroup,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<ChildSide> {
    if group.referenced_ids.is_empty() {
        return None;
    }

    let all_left = group.referenced_ids.iter().all(|id| left_ids.contains(id));
    let all_right = group.referenced_ids.iter().all(|id| right_ids.contains(id));
    match (all_left, all_right) {
        (true, false) => Some(ChildSide::Left),
        (false, true) => Some(ChildSide::Right),
        _ => None,
    }
}

fn collect_child_predicate_groups(
    expr: &OptExpr,
    out: &mut Vec<PredicateGroup>,
    arena: &ScalarArena,
) {
    match &expr.op {
        Operator::LogicalFilter(filter) => {
            out.extend(PredicateGroup::from_predicate(
                arena,
                filter.predicate,
                PredicateOrigin::Filter,
            ));
            if !expr.children.is_empty() {
                collect_child_predicate_groups(expr.unary_input(), out, arena);
            }
        }
        Operator::LogicalScan(scan) => {
            for &pred_id in &scan.predicates {
                out.extend(PredicateGroup::from_predicate(
                    arena,
                    pred_id,
                    PredicateOrigin::Filter,
                ));
            }
        }
        Operator::LogicalProject(_) | Operator::LogicalSort(_) | Operator::LogicalLimit(_) => {
            if !expr.children.is_empty() {
                collect_child_predicate_groups(expr.unary_input(), out, arena);
            }
        }
        Operator::LogicalJoin(join)
            if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) =>
        {
            if let Some(cond_id) = join.condition {
                out.extend(PredicateGroup::from_predicate(
                    arena,
                    cond_id,
                    PredicateOrigin::JoinCondition,
                ));
            }
            if expr.children.len() >= 2 {
                collect_child_predicate_groups(expr.left(), out, arena);
                collect_child_predicate_groups(expr.right(), out, arena);
            }
        }
        _ => {}
    }
}

fn existing_child_predicate_keys(expr: &OptExpr, arena: &ScalarArena) -> HashSet<PredicateKey> {
    let mut keys = HashSet::new();
    collect_existing_child_predicate_keys(expr, &mut keys, arena);
    keys
}

fn collect_existing_child_predicate_keys(
    expr: &OptExpr,
    out: &mut HashSet<PredicateKey>,
    arena: &ScalarArena,
) {
    match &expr.op {
        Operator::LogicalFilter(filter) => {
            collect_top_level_conjunct_keys(arena, filter.predicate, out);
            if !expr.children.is_empty() {
                collect_existing_child_predicate_keys(expr.unary_input(), out, arena);
            }
        }
        Operator::LogicalScan(scan) => {
            for &pred_id in &scan.predicates {
                collect_top_level_conjunct_keys(arena, pred_id, out);
            }
        }
        Operator::LogicalProject(_) | Operator::LogicalSort(_) | Operator::LogicalLimit(_) => {
            if !expr.children.is_empty() {
                collect_existing_child_predicate_keys(expr.unary_input(), out, arena);
            }
        }
        Operator::LogicalJoin(join)
            if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) =>
        {
            if let Some(cond_id) = join.condition {
                collect_top_level_conjunct_keys(arena, cond_id, out);
            }
            if expr.children.len() >= 2 {
                collect_existing_child_predicate_keys(expr.left(), out, arena);
                collect_existing_child_predicate_keys(expr.right(), out, arena);
            }
        }
        _ => {}
    }
}

fn collect_top_level_conjunct_keys(
    arena: &ScalarArena,
    expr: ScalarId,
    out: &mut HashSet<PredicateKey>,
) {
    let mut conjuncts = Vec::new();
    scalar_expr::split_conjuncts(arena, expr, &mut conjuncts);
    for conjunct in conjuncts {
        out.insert(predicate_key(arena, conjunct));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{LogicalJoinOp, ScanOp};
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

    fn col_id(id: u32) -> ColumnId {
        ColumnId::new_for_test(id)
    }

    fn col_expr(alias: &str, name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int32,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id: col_id(id),
                qualifier: Some(alias.to_string()),
                column: name.to_string(),
            },
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }

    fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: true,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
        }
    }

    fn and(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: true,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
        }
    }

    fn output_col(name: &str, id: u32) -> OutputColumn {
        OutputColumn {
            column_id: col_id(id),
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: true,
            is_internal: false,
        }
    }

    fn make_scan(alias: &str, cols: &[(&str, u32)]) -> OptExpr {
        let table = TableDef {
            name: alias.to_string(),
            columns: cols
                .iter()
                .map(|(name, _)| ColumnDef {
                    name: name.to_string(),
                    data_type: DataType::Int32,
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
        };
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".to_string(),
            table,
            alias: Some(alias.to_string()),
            stats_ref: None,
            columns: cols
                .iter()
                .map(|(name, id)| output_col(name, *id))
                .collect(),
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn make_filter(arena: &mut ScalarArena, predicate: TypedExpr, child: OptExpr) -> OptExpr {
        let pred_id = intern_typed(arena, &predicate);
        OptExpr::new(
            Operator::LogicalFilter(FilterOp { predicate: pred_id }),
            vec![child],
        )
    }

    fn make_inner_join(
        arena: &mut ScalarArena,
        condition: TypedExpr,
        left: OptExpr,
        right: OptExpr,
    ) -> OptExpr {
        let cond_id = intern_typed(arena, &condition);
        OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(cond_id),
            }),
            vec![left, right],
        )
    }

    fn make_ctx(arena: ScalarArena) -> RewriteContext {
        let mut ctx = RewriteContext::for_query(std::iter::empty::<String>());
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));
        ctx
    }

    fn assert_right_is_filter(expr: &OptExpr) {
        assert!(
            matches!(expr.right().op, Operator::LogicalFilter(_)),
            "expected right child to be Filter, got {:?}",
            expr.right().op
        );
    }

    fn assert_left_is_filter(expr: &OptExpr) {
        assert!(
            matches!(expr.left().op, Operator::LogicalFilter(_)),
            "expected left child to be Filter, got {:?}",
            expr.left().op
        );
    }

    #[test]
    fn derives_opposite_side_filter_from_child_filter_and_join_equality() {
        let mut arena = ScalarArena::new();
        let left_scan = make_scan("l", &[("a", 1)]);
        let left_filter = make_filter(&mut arena, eq(col_expr("l", "a", 1), int_lit(5)), left_scan);
        let right_scan = make_scan("r", &[("b", 2)]);
        let join = make_inner_join(
            &mut arena,
            eq(col_expr("l", "a", 1), col_expr("r", "b", 2)),
            left_filter,
            right_scan,
        );

        let mut ctx = make_ctx(arena);
        let result = JoinPredicateMoveAround.apply(join, &mut ctx).unwrap();
        let RewriteResult::Changed(out) = result else {
            panic!("expected Changed result");
        };
        assert_right_is_filter(&out);
    }

    #[test]
    fn skips_left_outer_nullable_side_derivation() {
        let mut arena = ScalarArena::new();
        let left_scan = make_scan("l", &[("a", 1)]);
        let left_filter = make_filter(&mut arena, eq(col_expr("l", "a", 1), int_lit(5)), left_scan);
        let right_scan = make_scan("r", &[("b", 2)]);
        let cond_id = intern_typed(
            &mut arena,
            &eq(col_expr("l", "a", 1), col_expr("r", "b", 2)),
        );
        let join = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::LeftOuter,
                condition: Some(cond_id),
            }),
            vec![left_filter, right_scan],
        );

        let mut ctx = make_ctx(arena);
        let result = JoinPredicateMoveAround.apply(join, &mut ctx).unwrap();
        assert!(matches!(result, RewriteResult::Unchanged));
    }

    #[test]
    fn skips_when_derived_filter_already_exists_on_child() {
        let mut arena = ScalarArena::new();
        let left_scan = make_scan("l", &[("a", 1)]);
        let left_filter = make_filter(&mut arena, eq(col_expr("l", "a", 1), int_lit(5)), left_scan);
        let right_scan = make_scan("r", &[("b", 2)]);
        let right_filter = make_filter(
            &mut arena,
            eq(col_expr("r", "b", 2), int_lit(5)),
            right_scan,
        );
        let join = make_inner_join(
            &mut arena,
            eq(col_expr("l", "a", 1), col_expr("r", "b", 2)),
            left_filter,
            right_filter,
        );

        let mut ctx = make_ctx(arena);
        let result = JoinPredicateMoveAround.apply(join, &mut ctx).unwrap();
        assert!(matches!(result, RewriteResult::Unchanged));
    }

    #[test]
    fn skips_when_derived_filter_exists_as_top_level_and_conjunct() {
        let mut arena = ScalarArena::new();
        let left_scan = make_scan("l", &[("a", 1)]);
        let left_filter = make_filter(&mut arena, eq(col_expr("l", "a", 1), int_lit(5)), left_scan);
        let right_scan = make_scan("r", &[("b", 2), ("c", 3)]);
        let right_filter = make_filter(
            &mut arena,
            and(
                eq(col_expr("r", "b", 2), int_lit(5)),
                eq(col_expr("r", "c", 3), int_lit(9)),
            ),
            right_scan,
        );
        let join = make_inner_join(
            &mut arena,
            eq(col_expr("l", "a", 1), col_expr("r", "b", 2)),
            left_filter,
            right_filter,
        );

        let mut ctx = make_ctx(arena);
        let result = JoinPredicateMoveAround.apply(join, &mut ctx).unwrap();
        assert!(matches!(result, RewriteResult::Unchanged));
    }

    #[test]
    fn derives_from_nested_inner_join_child_filter() {
        let mut arena = ScalarArena::new();
        let b_scan = make_scan("b", &[("k", 2)]);
        let b_filter = make_filter(&mut arena, eq(col_expr("b", "k", 2), int_lit(7)), b_scan);
        let a_scan = make_scan("a", &[("k", 1)]);
        let left_child = make_inner_join(
            &mut arena,
            eq(col_expr("a", "k", 1), col_expr("b", "k", 2)),
            a_scan,
            b_filter,
        );
        let c_scan = make_scan("c", &[("k", 3)]);
        let join = make_inner_join(
            &mut arena,
            eq(col_expr("b", "k", 2), col_expr("c", "k", 3)),
            left_child,
            c_scan,
        );

        let mut ctx = make_ctx(arena);
        let result = JoinPredicateMoveAround.apply(join, &mut ctx).unwrap();
        let RewriteResult::Changed(out) = result else {
            panic!("expected Changed result");
        };
        assert_right_is_filter(&out);
    }

    #[test]
    fn outer_child_join_condition_does_not_hide_fresh_parent_filter() {
        let mut arena = ScalarArena::new();
        let a_scan = make_scan("a", &[("k", 1)]);
        let b_scan = make_scan("b", &[("k", 2)]);
        let left_cond_id = intern_typed(
            &mut arena,
            &and(
                eq(col_expr("a", "k", 1), col_expr("b", "k", 2)),
                eq(col_expr("b", "k", 2), int_lit(7)),
            ),
        );
        let left_child = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::LeftOuter,
                condition: Some(left_cond_id),
            }),
            vec![a_scan, b_scan],
        );
        let c_scan = make_scan("c", &[("k", 3)]);
        let join = make_inner_join(
            &mut arena,
            and(
                eq(col_expr("b", "k", 2), col_expr("c", "k", 3)),
                eq(col_expr("c", "k", 3), int_lit(7)),
            ),
            left_child,
            c_scan,
        );

        let mut ctx = make_ctx(arena);
        let result = JoinPredicateMoveAround.apply(join, &mut ctx).unwrap();
        let RewriteResult::Changed(out) = result else {
            panic!("expected Changed result");
        };
        assert_left_is_filter(&out);
    }

    #[test]
    fn pattern_rejects_non_join_structure() {
        let scan = make_scan("l", &[("a", 1)]);
        let rule = JoinPredicateMoveAround;

        assert!(
            bind_tree(&rule.pattern(), &scan).is_none(),
            "JoinPredicateMoveAround pattern must only match Join roots"
        );
    }
}
