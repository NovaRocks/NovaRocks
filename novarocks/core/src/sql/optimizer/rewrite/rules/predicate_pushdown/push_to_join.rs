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

//! PushDownPredicateJoin rule — `Filter(Join)` and `Join` (condition pushdown).
//!
//! Migrated to `OptExpr` / `LogicalRewriteRule`. Predicate classification and
//! derivation operate directly on memo-owned `ScalarId`s, and tree construction
//! remains local to the `OptExpr` rewrite path.

use std::collections::HashSet;

use crate::sql::column_id::ColumnId;
use crate::sql::common::{BinOp, JoinKind};
use crate::sql::optimizer::operator::{FilterOp, LogicalJoinOp, Operator};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::deriver::derive_inner_join_predicates;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
    PredicateGroup, PredicateOrigin, predicate_key as canonical_predicate_key,
};
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_output_ids_opt, wrap_remaining_filter_opt_scalar,
};
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::scalar_expr;

pub(crate) struct PushDownPredicateJoin;

impl LogicalRewriteRule for PushDownPredicateJoin {
    fn name(&self) -> &'static str {
        "PushDownPredicateJoin"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    // Keep the default Leaf pattern: this rule has a disjunctive structural
    // shape, `Filter(Join)` or `Join` with an inline condition. The current
    // Pattern language has no `Or`, and splitting the rule would change the
    // rule-name and disable-rule surface.
    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        (matches!(&expr.op, Operator::LogicalFilter(_))
            && !expr.children.is_empty()
            && matches!(&expr.unary_input().op, Operator::LogicalJoin(_)))
            || matches!(&expr.op, Operator::LogicalJoin(join) if join.condition.is_some())
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let arena_rc = ctx.scalar_arena();
        let mut arena = arena_rc.borrow_mut();

        let OptExpr {
            op,
            mut children,
            required_output_columns,
        } = expr;

        match op {
            Operator::LogicalFilter(filter_op) => {
                if children.len() != 1 {
                    return Ok(RewriteResult::Unchanged);
                }
                let join_expr = children.remove(0);
                let OptExpr {
                    op: join_op_kind,
                    children: join_children_owned,
                    required_output_columns: join_req,
                } = join_expr;
                let mut join_children = join_children_owned;
                let Operator::LogicalJoin(join_op) = join_op_kind else {
                    return Ok(RewriteResult::Unchanged);
                };
                if join_children.len() != 2 {
                    return Ok(RewriteResult::Unchanged);
                }
                let right = join_children.remove(1);
                let left = join_children.remove(0);

                let (new_join_expr, changed) = push_filter_predicates_opt(
                    filter_op.predicate,
                    join_op,
                    left,
                    right,
                    join_req,
                    &mut arena,
                );
                if changed {
                    Ok(RewriteResult::Changed(new_join_expr))
                } else {
                    Ok(RewriteResult::Unchanged)
                }
            }
            Operator::LogicalJoin(join_op) => {
                if children.len() != 2 {
                    return Ok(RewriteResult::Unchanged);
                }
                let right = children.remove(1);
                let left = children.remove(0);
                match push_join_condition_predicates_opt(
                    join_op,
                    left,
                    right,
                    required_output_columns,
                    &mut arena,
                ) {
                    Some(result) => Ok(RewriteResult::Changed(result)),
                    None => Ok(RewriteResult::Unchanged),
                }
            }
            _ => Ok(RewriteResult::Unchanged),
        }
    }
}

// ---------------------------------------------------------------------------
// Local re-implementation of the join pushdown logic in OptExpr output terms.
// Predicate classification and derivation stay ScalarId-native; only the tree
// construction is specific to OptExpr.
// ---------------------------------------------------------------------------

fn push_filter_predicates_opt(
    predicate: ScalarId,
    join: LogicalJoinOp,
    left: OptExpr,
    right: OptExpr,
    required_output_columns: Option<HashSet<ColumnId>>,
    arena: &mut ScalarArena,
) -> (OptExpr, bool) {
    let mut left_ids = collect_output_ids_opt(&left);
    let mut right_ids = collect_output_ids_opt(&right);
    left_ids.remove(&ColumnId::UNSET);
    right_ids.remove(&ColumnId::UNSET);

    let filter_groups = PredicateGroup::from_predicate(arena, predicate, PredicateOrigin::Filter);
    let join_groups = join
        .condition
        .map(|cond_id| {
            PredicateGroup::from_predicate(arena, cond_id, PredicateOrigin::JoinCondition)
        })
        .unwrap_or_default();

    let mut conjuncts = Vec::new();
    scalar_expr::split_conjuncts(arena, predicate, &mut conjuncts);

    if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
        let derived = derive_inner_join_predicates(
            arena,
            &left_ids,
            &right_ids,
            &join_groups,
            &filter_groups,
        );
        append_new_derived_conjuncts_opt(
            &mut conjuncts,
            derived,
            &left,
            &right,
            &left_ids,
            &right_ids,
            arena,
        );
    }

    let mut left_preds = Vec::new();
    let mut right_preds = Vec::new();
    let mut join_preds = Vec::new();
    let mut remaining = Vec::new();

    for conj in conjuncts {
        let Some((in_left, in_right)) =
            classify_sides_by_column_ids(arena, conj, &left_ids, &right_ids)
        else {
            remaining.push(conj);
            continue;
        };

        match (in_left, in_right) {
            (true, false) => left_preds.push(conj),
            (false, true) => match join.join_type {
                JoinKind::Inner
                | JoinKind::Cross
                | JoinKind::RightOuter
                | JoinKind::RightSemi
                | JoinKind::RightAnti => {
                    right_preds.push(conj);
                }
                _ => remaining.push(conj),
            },
            (true, true) => {
                if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
                    let (implied_left, implied_right) =
                        extract_implied_or_side_filters(arena, conj, &left_ids, &right_ids);
                    for pred in implied_left {
                        if !subtree_has_predicate_opt(&left, pred, arena) {
                            left_preds.push(pred);
                        }
                    }
                    for pred in implied_right {
                        if !subtree_has_predicate_opt(&right, pred, arena) {
                            right_preds.push(pred);
                        }
                    }
                }

                if matches!(
                    join.join_type,
                    JoinKind::LeftOuter
                        | JoinKind::LeftSemi
                        | JoinKind::LeftAnti
                        | JoinKind::RightOuter
                        | JoinKind::FullOuter
                ) {
                    remaining.push(conj);
                } else {
                    let (factored, or_remaining) =
                        factor_common_eq_from_or(arena, conj, &left_ids, &right_ids);
                    if !factored.is_empty() {
                        join_preds.extend(factored);
                        if let Some(rem) = or_remaining {
                            remaining.push(rem);
                        }
                    } else {
                        join_preds.push(conj);
                    }
                }
            }
            (false, false) => {
                left_preds.push(conj);
            }
        }
    }

    // For RIGHT OUTER joins, left-side predicates cannot be pushed below.
    if matches!(
        join.join_type,
        JoinKind::RightOuter | JoinKind::RightSemi | JoinKind::RightAnti
    ) {
        remaining.append(&mut left_preds);
    }

    // For FULL OUTER joins, neither side can receive pushed predicates.
    if matches!(join.join_type, JoinKind::FullOuter) {
        remaining.append(&mut left_preds);
        remaining.append(&mut right_preds);
    }

    let pushed_any = !left_preds.is_empty() || !right_preds.is_empty() || !join_preds.is_empty();

    let new_left = if left_preds.is_empty() {
        left
    } else {
        let pushed_id = scalar_expr::combine_conjuncts(arena, left_preds).expect("non-empty");
        OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: pushed_id,
            }),
            vec![left],
        )
    };

    let new_right = if right_preds.is_empty() {
        right
    } else {
        let pushed_id = scalar_expr::combine_conjuncts(arena, right_preds).expect("non-empty");
        OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: pushed_id,
            }),
            vec![right],
        )
    };

    // Merge new join predicates with the existing join condition.
    let new_condition = merge_join_conditions(arena, join.condition, join_preds);

    // Upgrade CROSS JOIN to INNER when join predicates were extracted.
    let new_join_type = if join.join_type == JoinKind::Cross && new_condition.is_some() {
        JoinKind::Inner
    } else {
        join.join_type
    };

    let mut new_join = OptExpr::new(
        Operator::LogicalJoin(LogicalJoinOp {
            join_type: new_join_type,
            condition: new_condition,
        }),
        vec![new_left, new_right],
    );
    new_join.required_output_columns = required_output_columns;

    let result = wrap_remaining_filter_opt_scalar(new_join, remaining, arena);
    (result, pushed_any)
}

fn push_join_condition_predicates_opt(
    join: LogicalJoinOp,
    left: OptExpr,
    right: OptExpr,
    required_output_columns: Option<HashSet<ColumnId>>,
    arena: &mut ScalarArena,
) -> Option<OptExpr> {
    if !matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
        return None;
    }

    let cond_id = join.condition?;

    let mut left_ids = collect_output_ids_opt(&left);
    let mut right_ids = collect_output_ids_opt(&right);
    left_ids.remove(&ColumnId::UNSET);
    right_ids.remove(&ColumnId::UNSET);

    let condition_groups =
        PredicateGroup::from_predicate(arena, cond_id, PredicateOrigin::JoinCondition);
    let mut conjuncts = Vec::new();
    scalar_expr::split_conjuncts(arena, cond_id, &mut conjuncts);

    let derived = derive_inner_join_predicates(
        arena,
        &left_ids,
        &right_ids,
        &condition_groups,
        &condition_groups,
    );
    append_new_derived_conjuncts_opt(
        &mut conjuncts,
        derived,
        &left,
        &right,
        &left_ids,
        &right_ids,
        arena,
    );

    let mut left_preds = Vec::new();
    let mut right_preds = Vec::new();
    let mut residual_preds = Vec::new();

    for conj in conjuncts {
        let Some((in_left, in_right)) =
            classify_sides_by_column_ids(arena, conj, &left_ids, &right_ids)
        else {
            residual_preds.push(conj);
            continue;
        };

        match (in_left, in_right) {
            (true, false) => left_preds.push(conj),
            (false, true) => right_preds.push(conj),
            (false, false) => left_preds.push(conj),
            (true, true) => residual_preds.push(conj),
        }
    }

    let pushed_any = !left_preds.is_empty() || !right_preds.is_empty();
    let new_condition = if residual_preds.is_empty() {
        None
    } else {
        scalar_expr::combine_conjuncts(arena, residual_preds)
    };
    let upgrades_cross = join.join_type == JoinKind::Cross && new_condition.is_some();

    if !pushed_any && !upgrades_cross {
        return None;
    }

    let new_left = if left_preds.is_empty() {
        left
    } else {
        let pushed_id = scalar_expr::combine_conjuncts(arena, left_preds).expect("non-empty");
        OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: pushed_id,
            }),
            vec![left],
        )
    };

    let new_right = if right_preds.is_empty() {
        right
    } else {
        let pushed_id = scalar_expr::combine_conjuncts(arena, right_preds).expect("non-empty");
        OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: pushed_id,
            }),
            vec![right],
        )
    };

    let new_join_type = if upgrades_cross {
        JoinKind::Inner
    } else {
        join.join_type
    };

    let mut result = OptExpr::new(
        Operator::LogicalJoin(LogicalJoinOp {
            join_type: new_join_type,
            condition: new_condition,
        }),
        vec![new_left, new_right],
    );
    result.required_output_columns = required_output_columns;
    Some(result)
}

fn classify_sides_by_column_ids(
    arena: &ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<(bool, bool)> {
    let ids = scalar_expr::collect_column_ids_strict(arena, expr)?;
    if ids.is_empty() {
        return Some((false, false));
    }

    let mut in_left = false;
    let mut in_right = false;
    for id in ids {
        match (left_ids.contains(&id), right_ids.contains(&id)) {
            (true, false) => in_left = true,
            (false, true) => in_right = true,
            _ => return None,
        }
    }
    Some((in_left, in_right))
}

fn append_new_derived_conjuncts_opt(
    conjuncts: &mut Vec<ScalarId>,
    derived: Vec<PredicateGroup>,
    left: &OptExpr,
    right: &OptExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    arena: &ScalarArena,
) {
    let mut seen: HashSet<String> = conjuncts
        .iter()
        .map(|expr| predicate_key_str(arena, *expr))
        .collect();
    for group in derived {
        if derived_exists_below_child_opt(group.expr, left, right, left_ids, right_ids, arena) {
            continue;
        }
        if seen.insert(predicate_key_str(arena, group.expr)) {
            conjuncts.push(group.expr);
        }
    }
}

fn derived_exists_below_child_opt(
    expr: ScalarId,
    left: &OptExpr,
    right: &OptExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    arena: &ScalarArena,
) -> bool {
    let Some(ids) = scalar_expr::collect_column_ids_strict(arena, expr) else {
        return false;
    };
    if ids.is_empty() {
        return false;
    }
    if ids.iter().all(|id| left_ids.contains(id)) {
        return subtree_has_predicate_opt(left, expr, arena);
    }
    if ids.iter().all(|id| right_ids.contains(id)) {
        return subtree_has_predicate_opt(right, expr, arena);
    }
    false
}

fn subtree_has_predicate_opt(expr: &OptExpr, pred: ScalarId, arena: &ScalarArena) -> bool {
    let key = predicate_key_str(arena, pred);
    subtree_has_predicate_key_opt(expr, &key, arena)
}

fn subtree_has_predicate_key_opt(expr: &OptExpr, key: &str, arena: &ScalarArena) -> bool {
    match &expr.op {
        Operator::LogicalScan(scan) => scan
            .predicates
            .iter()
            .any(|&pred_id| predicate_has_conjunct_key(arena, pred_id, key)),
        Operator::LogicalFilter(filter) => {
            predicate_has_conjunct_key(arena, filter.predicate, key)
                || expr
                    .children
                    .iter()
                    .any(|child| subtree_has_predicate_key_opt(child, key, arena))
        }
        Operator::LogicalJoin(join) => {
            join.condition
                .map(|cond_id| predicate_has_conjunct_key(arena, cond_id, key))
                .unwrap_or(false)
                || expr
                    .children
                    .iter()
                    .any(|child| subtree_has_predicate_key_opt(child, key, arena))
        }
        _ => expr
            .children
            .iter()
            .any(|child| subtree_has_predicate_key_opt(child, key, arena)),
    }
}

fn predicate_has_conjunct_key(arena: &ScalarArena, expr: ScalarId, key: &str) -> bool {
    let mut conjuncts = Vec::new();
    scalar_expr::split_conjuncts(arena, expr, &mut conjuncts);
    conjuncts
        .into_iter()
        .any(|conjunct| predicate_key_str(arena, conjunct) == key)
}

fn predicate_key_str(arena: &ScalarArena, expr: ScalarId) -> String {
    canonical_predicate_key(arena, expr).as_str().to_string()
}

fn merge_join_conditions(
    arena: &mut ScalarArena,
    existing: Option<ScalarId>,
    new_preds: Vec<ScalarId>,
) -> Option<ScalarId> {
    let mut all = Vec::new();
    let mut seen = HashSet::new();
    if let Some(cond) = existing {
        let mut conjuncts = Vec::new();
        scalar_expr::split_conjuncts(arena, cond, &mut conjuncts);
        for pred in conjuncts {
            if seen.insert(predicate_key_str(arena, pred)) {
                all.push(pred);
            }
        }
    }
    for pred in new_preds {
        if seen.insert(predicate_key_str(arena, pred)) {
            all.push(pred);
        }
    }
    scalar_expr::combine_conjuncts(arena, all)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PredicateSide {
    Left,
    Right,
}

fn extract_implied_or_side_filters(
    arena: &mut ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> (Vec<ScalarId>, Vec<ScalarId>) {
    let mut branches = Vec::new();
    scalar_expr::split_disjuncts(arena, expr, &mut branches);
    if branches.len() < 2 {
        return (Vec::new(), Vec::new());
    }
    let branch_count = branches.len();

    let mut left_terms = Vec::with_capacity(branches.len());
    let mut right_terms = Vec::with_capacity(branches.len());
    for branch in branches {
        let mut left_conjuncts = Vec::new();
        let mut right_conjuncts = Vec::new();
        let mut conjuncts = Vec::new();
        scalar_expr::split_conjuncts(arena, branch, &mut conjuncts);
        for conjunct in conjuncts {
            match classify_implied_filter_side(arena, conjunct, left_ids, right_ids) {
                Some(PredicateSide::Left) => left_conjuncts.push(conjunct),
                Some(PredicateSide::Right) => right_conjuncts.push(conjunct),
                None => {}
            }
        }

        if !left_conjuncts.is_empty() {
            left_terms
                .push(scalar_expr::combine_conjuncts(arena, left_conjuncts).expect("non-empty"));
        }
        if !right_conjuncts.is_empty() {
            right_terms
                .push(scalar_expr::combine_conjuncts(arena, right_conjuncts).expect("non-empty"));
        }
    }

    let left_filters = if left_terms.len() == branch_count {
        scalar_expr::combine_disjuncts(arena, left_terms)
            .into_iter()
            .collect()
    } else {
        Vec::new()
    };
    let right_filters = if right_terms.len() == branch_count {
        scalar_expr::combine_disjuncts(arena, right_terms)
            .into_iter()
            .collect()
    } else {
        Vec::new()
    };
    (left_filters, right_filters)
}

fn classify_implied_filter_side(
    arena: &ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<PredicateSide> {
    let (in_left, in_right) = classify_sides_by_column_ids(arena, expr, left_ids, right_ids)?;
    match (in_left, in_right) {
        (true, false) => Some(PredicateSide::Left),
        (false, true) => Some(PredicateSide::Right),
        _ => None,
    }
}

fn factor_common_eq_from_or(
    arena: &mut ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> (Vec<ScalarId>, Option<ScalarId>) {
    let mut branches = Vec::new();
    scalar_expr::split_disjuncts(arena, expr, &mut branches);
    if branches.len() < 2 {
        return (vec![], None);
    }

    let branch_conjuncts: Vec<Vec<ScalarId>> = branches
        .iter()
        .map(|branch| {
            let mut conjuncts = Vec::new();
            scalar_expr::split_conjuncts(arena, *branch, &mut conjuncts);
            conjuncts
        })
        .collect();

    let mut common_eqs = Vec::new();
    if let Some(first) = branch_conjuncts.first() {
        for candidate in first {
            if !is_cross_side_eq(arena, *candidate, left_ids, right_ids) {
                continue;
            }
            let in_all = branch_conjuncts[1..]
                .iter()
                .all(|conjs| conjs.iter().any(|c| expr_eq(*c, *candidate)));
            if in_all {
                common_eqs.push(*candidate);
            }
        }
    }

    if common_eqs.is_empty() {
        return (vec![], None);
    }

    let mut new_branches = Vec::new();
    for branch in &branch_conjuncts {
        let remaining: Vec<ScalarId> = branch
            .iter()
            .filter(|c| !common_eqs.iter().any(|eq| expr_eq(**c, *eq)))
            .copied()
            .collect();
        if remaining.is_empty() {
            new_branches.push(scalar_expr::bool_literal(arena, true));
        } else {
            new_branches.push(scalar_expr::combine_conjuncts(arena, remaining).expect("non-empty"));
        }
    }

    let or_remaining = if new_branches
        .iter()
        .all(|branch| scalar_expr::is_true_literal(arena, *branch))
    {
        None
    } else {
        scalar_expr::combine_disjuncts(arena, new_branches)
    };

    (common_eqs, or_remaining)
}

fn is_cross_side_eq(
    arena: &ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> bool {
    if let ScalarNode::BinaryOp {
        left,
        op: BinOp::Eq,
        right,
    } = arena.node(expr)
    {
        let l_id = match arena.node(*left) {
            ScalarNode::ColumnRef(column_id) if *column_id != ColumnId::UNSET => Some(*column_id),
            _ => None,
        };
        let r_id = match arena.node(*right) {
            ScalarNode::ColumnRef(column_id) if *column_id != ColumnId::UNSET => Some(*column_id),
            _ => None,
        };
        match (l_id, r_id) {
            (Some(l), Some(r)) => {
                (left_ids.contains(&l) && right_ids.contains(&r))
                    || (left_ids.contains(&r) && right_ids.contains(&l))
            }
            _ => false,
        }
    } else {
        false
    }
}

fn expr_eq(a: ScalarId, b: ScalarId) -> bool {
    a == b
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::optimizer::operator::ScanOp;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::planner::optimizer_bridge::scalar::{intern_typed, materialize};
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

    fn col_id(id: u32) -> ColumnId {
        ColumnId::new_for_test(id)
    }

    fn col(alias: &str, name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: col_id(id),
                qualifier: Some(alias.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn int_lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn and(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn output_col(name: &str, id: u32) -> OutputColumn {
        OutputColumn {
            column_id: col_id(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn scan(alias: &str, cols: &[(&str, u32)]) -> OptExpr {
        let table = TableDef {
            name: alias.to_string(),
            columns: cols
                .iter()
                .map(|(name, _)| ColumnDef {
                    name: name.to_string(),
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

    fn make_ctx(arena: ScalarArena) -> RewriteContext {
        let mut ctx = RewriteContext::for_query(std::iter::empty::<String>());
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));
        ctx
    }

    fn predicate_debug(ctx: &RewriteContext, predicate: ScalarId) -> String {
        let arena_rc = ctx.scalar_arena();
        let arena = arena_rc.borrow();
        format!("{:?}", materialize(&arena, predicate).kind)
    }

    fn extract_filter_predicate(expr: &OptExpr) -> ScalarId {
        let Operator::LogicalFilter(filter) = &expr.op else {
            panic!("expected LogicalFilter, got {:?}", expr.op);
        };
        filter.predicate
    }

    fn join_condition(expr: &OptExpr) -> Option<ScalarId> {
        let Operator::LogicalJoin(join) = &expr.op else {
            panic!("expected LogicalJoin, got {:?}", expr.op);
        };
        join.condition
    }

    #[test]
    fn filter_join_pushes_filter_and_derived_opposite_side_predicate() {
        let mut arena = ScalarArena::new();
        let join_condition = intern_typed(&mut arena, &eq(col("l", "a", 1), col("r", "b", 2)));
        let join = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(join_condition),
            }),
            vec![scan("l", &[("a", 1)]), scan("r", &[("b", 2)])],
        );
        let filter_id = intern_typed(&mut arena, &eq(col("l", "a", 1), int_lit(7)));
        let input = OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: filter_id,
            }),
            vec![join],
        );

        let mut ctx = make_ctx(arena);
        let result = PushDownPredicateJoin.apply(input, &mut ctx).unwrap();
        let RewriteResult::Changed(out) = result else {
            panic!("expected Changed result");
        };

        assert!(matches!(out.op, Operator::LogicalJoin(_)));
        assert!(matches!(out.left().op, Operator::LogicalFilter(_)));
        assert!(matches!(out.right().op, Operator::LogicalFilter(_)));

        let left_pred = predicate_debug(&ctx, extract_filter_predicate(out.left()));
        let right_pred = predicate_debug(&ctx, extract_filter_predicate(out.right()));
        assert!(left_pred.contains("\"a\""));
        assert!(left_pred.contains("Int(7)"));
        assert!(right_pred.contains("\"b\""));
        assert!(right_pred.contains("Int(7)"));
    }

    #[test]
    fn join_condition_pushdown_keeps_cross_side_residual_and_pushes_side_filters() {
        let mut arena = ScalarArena::new();
        let condition = intern_typed(
            &mut arena,
            &and(
                eq(col("l", "a", 1), col("r", "b", 2)),
                eq(col("l", "a", 1), int_lit(7)),
            ),
        );
        let input = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            vec![scan("l", &[("a", 1)]), scan("r", &[("b", 2)])],
        );

        let mut ctx = make_ctx(arena);
        let result = PushDownPredicateJoin.apply(input, &mut ctx).unwrap();
        let RewriteResult::Changed(out) = result else {
            panic!("expected Changed result");
        };

        assert!(matches!(out.left().op, Operator::LogicalFilter(_)));
        assert!(matches!(out.right().op, Operator::LogicalFilter(_)));

        let residual = join_condition(&out).expect("cross-side equality should remain on join");
        let residual = predicate_debug(&ctx, residual);
        assert!(residual.contains("\"a\""));
        assert!(residual.contains("\"b\""));
        assert!(!residual.contains("Int(7)"));

        let left_pred = predicate_debug(&ctx, extract_filter_predicate(out.left()));
        let right_pred = predicate_debug(&ctx, extract_filter_predicate(out.right()));
        assert!(left_pred.contains("\"a\""));
        assert!(left_pred.contains("Int(7)"));
        assert!(right_pred.contains("\"b\""));
        assert!(right_pred.contains("Int(7)"));
    }
}
