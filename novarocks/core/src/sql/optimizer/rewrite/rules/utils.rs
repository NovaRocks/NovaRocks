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

//! Shared expression / plan utilities for the query rewrite rules and any cascades
//! code that needs small AST helpers. Moved from
//! `src/sql/optimizer/expr_utils.rs` in Phase 4 of the optimizer
//! unification; contents unchanged.

use std::collections::HashSet;

use crate::sql::column_id::ColumnId;
use crate::sql::common::BinOp;
use crate::sql::optimizer::operator::{FilterOp, LogicalJoinOp, Operator};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

/// Qualified column reference: (qualifier, column), both lowercase.
pub(crate) type QualifiedRef = (Option<String>, String);

pub(crate) fn collect_scalar_column_id_refs_strict(
    arena: &ScalarArena,
    expr: ScalarId,
) -> Option<HashSet<ColumnId>> {
    let mut out = HashSet::new();
    collect_scalar_column_id_refs_strict_inner(arena, expr, &mut out)?;
    Some(out)
}

fn collect_scalar_column_id_refs_strict_inner(
    arena: &ScalarArena,
    expr: ScalarId,
    out: &mut HashSet<ColumnId>,
) -> Option<()> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => {
            if *column_id == ColumnId::UNSET {
                return None;
            }
            out.insert(*column_id);
        }
        ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            collect_scalar_column_id_refs_strict_inner(arena, *left, out)?;
            collect_scalar_column_id_refs_strict_inner(arena, *right, out)?;
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => {
            collect_scalar_column_id_refs_strict_inner(arena, *child, out)?;
        }
        ScalarNode::FunctionCall { args, .. } | ScalarNode::AggregateCall { args, .. } => {
            for arg in args {
                collect_scalar_column_id_refs_strict_inner(arena, *arg, out)?;
            }
            if let ScalarNode::AggregateCall { order_by, .. } = arena.node(expr) {
                for key in order_by {
                    collect_scalar_column_id_refs_strict_inner(arena, key.expr, out)?;
                }
            }
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            collect_scalar_column_id_refs_strict_inner(arena, *body, out)?;
        }
        ScalarNode::InList { child, list, .. } => {
            collect_scalar_column_id_refs_strict_inner(arena, *child, out)?;
            for item in list {
                collect_scalar_column_id_refs_strict_inner(arena, *item, out)?;
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            collect_scalar_column_id_refs_strict_inner(arena, *child, out)?;
            collect_scalar_column_id_refs_strict_inner(arena, *low, out)?;
            collect_scalar_column_id_refs_strict_inner(arena, *high, out)?;
        }
        ScalarNode::Like { child, pattern, .. } => {
            collect_scalar_column_id_refs_strict_inner(arena, *child, out)?;
            collect_scalar_column_id_refs_strict_inner(arena, *pattern, out)?;
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_scalar_column_id_refs_strict_inner(arena, *operand, out)?;
            }
            for (when, then) in when_then {
                collect_scalar_column_id_refs_strict_inner(arena, *when, out)?;
                collect_scalar_column_id_refs_strict_inner(arena, *then, out)?;
            }
            if let Some(else_expr) = else_expr {
                collect_scalar_column_id_refs_strict_inner(arena, *else_expr, out)?;
            }
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_scalar_column_id_refs_strict_inner(arena, *arg, out)?;
            }
            for expr in partition_by {
                collect_scalar_column_id_refs_strict_inner(arena, *expr, out)?;
            }
            for key in order_by {
                collect_scalar_column_id_refs_strict_inner(arena, key.expr, out)?;
            }
        }
    }
    Some(())
}

/// ScalarId-native equi-join key pair for the OptExpr rewrite path.
///
/// Operands are ids of the unwrapped inner `ScalarNode::ColumnRef`
/// (Cast/Nested peeled).
#[derive(Debug)]
pub(crate) struct ScalarJoinEquiKey {
    pub(crate) left: ScalarId,
    pub(crate) right: ScalarId,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
}

fn unwrap_scalar_column_ref(arena: &ScalarArena, expr: ScalarId) -> Option<ScalarId> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(_) => Some(expr),
        ScalarNode::Cast { child, .. } | ScalarNode::Nested(child) => {
            unwrap_scalar_column_ref(arena, *child)
        }
        _ => None,
    }
}

fn scalar_column_display_key(
    arena: &ScalarArena,
    expr: ScalarId,
) -> Option<(Option<String>, String)> {
    let ScalarNode::ColumnRef(column_id) = arena.node(expr) else {
        return None;
    };
    let display = arena.column_display(*column_id);
    Some((
        display.and_then(|item| item.qualifier.as_ref().map(|q| q.to_lowercase())),
        display
            .map(|item| item.column.to_lowercase())
            .unwrap_or_else(|| format!("col{}", column_id.0).to_lowercase()),
    ))
}

fn classify_scalar_operand(
    arena: &ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<crate::sql::column_id::ColumnId>,
    right_ids: &HashSet<crate::sql::column_id::ColumnId>,
    left_cols: &HashSet<QualifiedRef>,
    right_cols: &HashSet<QualifiedRef>,
) -> Option<(JoinSide, ScalarId)> {
    let inner_id = unwrap_scalar_column_ref(arena, expr)?;
    let ScalarNode::ColumnRef(column_id) = arena.node(inner_id) else {
        unreachable!("unwrap_scalar_column_ref only returns a ColumnRef scalar");
    };
    if *column_id != crate::sql::column_id::ColumnId::UNSET {
        match (left_ids.contains(column_id), right_ids.contains(column_id)) {
            (true, false) => return Some((JoinSide::Left, inner_id)),
            (false, true) => return Some((JoinSide::Right, inner_id)),
            _ => {}
        }
    }

    let key = scalar_column_display_key(arena, inner_id)?;
    match (left_cols.contains(&key), right_cols.contains(&key)) {
        (true, false) => Some((JoinSide::Left, inner_id)),
        (false, true) => Some((JoinSide::Right, inner_id)),
        _ => None,
    }
}

fn collect_join_equi_keys_opt(
    arena: &ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<crate::sql::column_id::ColumnId>,
    right_ids: &HashSet<crate::sql::column_id::ColumnId>,
    left_cols: &HashSet<QualifiedRef>,
    right_cols: &HashSet<QualifiedRef>,
    keys: &mut Vec<ScalarJoinEquiKey>,
) {
    match arena.node(expr) {
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_join_equi_keys_opt(
                arena, *left, left_ids, right_ids, left_cols, right_cols, keys,
            );
            collect_join_equi_keys_opt(
                arena, *right, left_ids, right_ids, left_cols, right_cols, keys,
            );
        }
        // Only strict `Eq`. `EqForNull` (<=>) is null-safe (NULL <=> NULL is
        // true), so deriving IS NOT NULL on its operands would change results;
        // it is intentionally excluded (matches StarRocks `isEqual()`).
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => match (
            classify_scalar_operand(arena, *left, left_ids, right_ids, left_cols, right_cols),
            classify_scalar_operand(arena, *right, left_ids, right_ids, left_cols, right_cols),
        ) {
            (Some((JoinSide::Left, le)), Some((JoinSide::Right, re)))
            | (Some((JoinSide::Right, re)), Some((JoinSide::Left, le))) => {
                keys.push(ScalarJoinEquiKey {
                    left: le,
                    right: re,
                });
            }
            _ => {}
        },
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// OptExpr-based helpers (new rewrite framework)
// ---------------------------------------------------------------------------

/// Return the ordered Vec of [`ColumnId`]s in the output schema of an `OptExpr`.
pub(crate) fn collect_output_ids_ordered_opt(expr: &OptExpr) -> Vec<ColumnId> {
    match &expr.op {
        Operator::LogicalScan(s) => s.columns.iter().map(|c| c.column_id).collect(),
        Operator::LogicalProject(p) => p
            .items
            .iter()
            .map(|i| i.output_column_id)
            .filter(|id| *id != ColumnId::UNSET)
            .collect(),
        Operator::LogicalAggregate(a) => a.output_columns.iter().map(|c| c.column_id).collect(),
        Operator::LogicalWindow(w) => w.output_columns.iter().map(|c| c.column_id).collect(),
        Operator::LogicalCTEProduce(p) => p.output_columns.iter().map(|c| c.column_id).collect(),
        Operator::LogicalCTEConsume(c) => c.output_columns.iter().map(|c| c.column_id).collect(),
        Operator::LogicalUnion(u) => u.output_columns.iter().map(|c| c.column_id).collect(),
        Operator::LogicalIntersect(i) => i.output_columns.iter().map(|c| c.column_id).collect(),
        Operator::LogicalExcept(e) => e.output_columns.iter().map(|c| c.column_id).collect(),
        Operator::LogicalValues(v) => v.columns.iter().map(|c| c.column_id).collect(),
        Operator::LogicalGenerateSeries(g) => {
            if g.output_column_id == ColumnId::UNSET {
                vec![]
            } else {
                vec![g.output_column_id]
            }
        }
        // Passthrough: node does not add or rename output ColumnIds.
        Operator::LogicalFilter(_)
        | Operator::LogicalSort(_)
        | Operator::LogicalLimit(_)
        | Operator::LogicalRepeat(_)
        | Operator::LogicalImvVersion(_) => collect_output_ids_ordered_opt(expr.unary_input()),
        Operator::LogicalImvDelta(delta) => {
            let mut ids = collect_output_ids_ordered_opt(expr.unary_input());
            if let Some(action_column) = delta.action_column
                && !ids.contains(&action_column)
            {
                ids.push(action_column);
            }
            ids
        }
        Operator::LogicalTableFunction(t) => {
            let mut ids = collect_output_ids_ordered_opt(expr.unary_input());
            ids.extend(t.output_columns.iter().map(|c| c.column_id));
            ids
        }
        Operator::LogicalJoin(_) => {
            let mut ids = collect_output_ids_ordered_opt(expr.left());
            ids.extend(collect_output_ids_ordered_opt(expr.right()));
            ids
        }
        Operator::LogicalCTEAnchor(_) => collect_output_ids_ordered_opt(expr.child(1)),
        Operator::LogicalAssertOneRow(_) => collect_output_ids_ordered_opt(expr.unary_input()),
        // Physical operators and any other variants are not expected during the RBO phase.
        _ => vec![],
    }
}

/// Return the set of [`ColumnId`]s in the output schema of an `OptExpr`.
pub(crate) fn collect_output_ids_opt(expr: &OptExpr) -> HashSet<ColumnId> {
    collect_output_ids_ordered_opt(expr).into_iter().collect()
}

/// Qualified column reference for `OptExpr`: (qualifier, column), both lowercase.
pub(crate) fn collect_qualified_output_columns_opt(expr: &OptExpr) -> HashSet<QualifiedRef> {
    let mut out = HashSet::new();
    collect_qualified_output_columns_opt_inner(expr, &mut out);
    out
}

fn collect_qualified_output_columns_opt_inner(expr: &OptExpr, out: &mut HashSet<QualifiedRef>) {
    match &expr.op {
        Operator::LogicalScan(s) => {
            let alias = s
                .alias
                .as_ref()
                .map(|a| a.to_lowercase())
                .or_else(|| Some(s.table.name.to_lowercase()));
            for c in &s.columns {
                let col = c.name.to_lowercase();
                if let Some(ref q) = alias {
                    out.insert((Some(q.clone()), col.clone()));
                }
                out.insert((None, col));
            }
        }
        Operator::LogicalFilter(_) => {
            collect_qualified_output_columns_opt_inner(expr.unary_input(), out)
        }
        Operator::LogicalProject(p) => {
            for item in &p.items {
                out.insert((None, item.output_name.to_lowercase()));
            }
        }
        Operator::LogicalJoin(_) => {
            collect_qualified_output_columns_opt_inner(expr.left(), out);
            collect_qualified_output_columns_opt_inner(expr.right(), out);
        }
        Operator::LogicalAggregate(a) => {
            for c in &a.output_columns {
                out.insert((None, c.name.to_lowercase()));
            }
        }
        Operator::LogicalSort(_) | Operator::LogicalLimit(_) => {
            collect_qualified_output_columns_opt_inner(expr.unary_input(), out)
        }
        Operator::LogicalWindow(w) => {
            for c in &w.output_columns {
                out.insert((None, c.name.to_lowercase()));
            }
        }
        Operator::LogicalUnion(_) => {
            if let Some(first) = expr.children.first() {
                collect_qualified_output_columns_opt_inner(first, out);
            }
        }
        Operator::LogicalIntersect(_) => {
            if let Some(first) = expr.children.first() {
                collect_qualified_output_columns_opt_inner(first, out);
            }
        }
        Operator::LogicalExcept(_) => {
            if let Some(first) = expr.children.first() {
                collect_qualified_output_columns_opt_inner(first, out);
            }
        }
        Operator::LogicalValues(v) => {
            for c in &v.columns {
                out.insert((None, c.name.to_lowercase()));
            }
        }
        Operator::LogicalGenerateSeries(g) => {
            out.insert((None, g.column_name.to_lowercase()));
        }
        Operator::LogicalTableFunction(t) => {
            collect_qualified_output_columns_opt_inner(expr.unary_input(), out);
            for col in &t.output_columns {
                out.insert((
                    t.alias.as_ref().map(|alias| alias.to_lowercase()),
                    col.name.to_lowercase(),
                ));
            }
        }
        Operator::LogicalCTEAnchor(_) => {
            collect_qualified_output_columns_opt_inner(expr.child(1), out);
        }
        Operator::LogicalCTEProduce(p) => {
            for col in &p.output_columns {
                out.insert((None, col.name.to_lowercase()));
            }
        }
        Operator::LogicalRepeat(_) => {
            collect_qualified_output_columns_opt_inner(expr.unary_input(), out)
        }
        Operator::LogicalCTEConsume(c) => {
            let alias_lower = c.alias.to_lowercase();
            for col in &c.output_columns {
                let col_name = col.name.to_lowercase();
                out.insert((Some(alias_lower.clone()), col_name.clone()));
                out.insert((None, col_name));
            }
        }
        Operator::LogicalAssertOneRow(_) => {
            collect_qualified_output_columns_opt_inner(expr.unary_input(), out)
        }
        // Physical operators are not expected during the RBO phase.
        _ => {}
    }
}

/// Wrap an `OptExpr` in a `LogicalFilter` if scalar conjuncts remain.
pub(crate) fn wrap_remaining_filter_opt_scalar(
    plan: OptExpr,
    remaining: Vec<ScalarId>,
    arena: &mut ScalarArena,
) -> OptExpr {
    if remaining.is_empty() {
        return plan;
    }
    let Some(predicate) = crate::sql::optimizer::scalar_expr::combine_conjuncts(arena, remaining)
    else {
        return plan;
    };
    OptExpr::new(Operator::LogicalFilter(FilterOp { predicate }), vec![plan])
}

/// Extract equi-join key pairs from an `OptExpr` join.
pub(crate) fn join_equi_keys_opt(
    join_op: &LogicalJoinOp,
    left: &OptExpr,
    right: &OptExpr,
    arena: &ScalarArena,
) -> Vec<ScalarJoinEquiKey> {
    let Some(cond_id) = join_op.condition else {
        return Vec::new();
    };
    let left_cols = collect_qualified_output_columns_opt(left);
    let right_cols = collect_qualified_output_columns_opt(right);
    let left_ids = collect_output_ids_opt(left);
    let right_ids = collect_output_ids_opt(right);
    let mut keys = Vec::new();
    collect_join_equi_keys_opt(
        arena,
        cond_id,
        &left_ids,
        &right_ids,
        &left_cols,
        &right_cols,
        &mut keys,
    );
    keys
}

#[cfg(test)]
pub(crate) use typed_legacy::{
    JoinEquiKey, collect_column_id_refs, collect_output_ids, collect_output_ids_ordered,
    join_equi_keys, split_and,
};

#[cfg(test)]
mod typed_legacy {
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use super::{JoinSide, QualifiedRef};
    use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::logical::*;
    use crate::sql::planner::payload::*;

    #[derive(Debug)]
    pub(crate) struct JoinEquiKey {
        pub(crate) left: TypedExpr,
        pub(crate) right: TypedExpr,
    }

    pub(crate) fn split_and(expr: TypedExpr) -> Vec<TypedExpr> {
        let mut out = Vec::new();
        split_and_inner(expr, &mut out);
        out
    }

    fn split_and_inner(expr: TypedExpr, out: &mut Vec<TypedExpr>) {
        match expr.kind {
            ExprKind::BinaryOp {
                left,
                op: BinOp::And,
                right,
            } => {
                split_and_inner(*left, out);
                split_and_inner(*right, out);
            }
            _ => out.push(expr),
        }
    }

    fn combine_and(mut exprs: Vec<TypedExpr>) -> TypedExpr {
        assert!(!exprs.is_empty());
        let mut result = exprs.pop().unwrap();
        while let Some(left) = exprs.pop() {
            result = TypedExpr {
                data_type: DataType::Boolean,
                nullable: left.nullable || result.nullable,
                kind: ExprKind::BinaryOp {
                    left: Box::new(left),
                    op: BinOp::And,
                    right: Box::new(result),
                },
            };
        }
        result
    }

    pub(crate) fn collect_column_id_refs(expr: &TypedExpr) -> HashSet<ColumnId> {
        let mut out = HashSet::new();
        collect_column_id_refs_inner(expr, &mut out);
        out
    }

    fn collect_column_id_refs_inner(expr: &TypedExpr, out: &mut HashSet<ColumnId>) {
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => {
                if *column_id != ColumnId::UNSET {
                    out.insert(*column_id);
                }
            }
            ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => {}
            ExprKind::BinaryOp { left, right, .. } => {
                collect_column_id_refs_inner(left, out);
                collect_column_id_refs_inner(right, out);
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. }
            | ExprKind::Nested(expr) => collect_column_id_refs_inner(expr, out),
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                for arg in args {
                    collect_column_id_refs_inner(arg, out);
                }
                if let ExprKind::AggregateCall { order_by, .. } = &expr.kind {
                    for item in order_by {
                        collect_column_id_refs_inner(&item.expr, out);
                    }
                }
            }
            ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
                collect_column_id_refs_inner(body, out);
            }
            ExprKind::InList { expr, list, .. } => {
                collect_column_id_refs_inner(expr, out);
                for item in list {
                    collect_column_id_refs_inner(item, out);
                }
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                collect_column_id_refs_inner(expr, out);
                collect_column_id_refs_inner(low, out);
                collect_column_id_refs_inner(high, out);
            }
            ExprKind::Like { expr, pattern, .. } => {
                collect_column_id_refs_inner(expr, out);
                collect_column_id_refs_inner(pattern, out);
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(operand) = operand {
                    collect_column_id_refs_inner(operand, out);
                }
                for (when, then) in when_then {
                    collect_column_id_refs_inner(when, out);
                    collect_column_id_refs_inner(then, out);
                }
                if let Some(else_expr) = else_expr {
                    collect_column_id_refs_inner(else_expr, out);
                }
            }
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    collect_column_id_refs_inner(arg, out);
                }
                for expr in partition_by {
                    collect_column_id_refs_inner(expr, out);
                }
                for item in order_by {
                    collect_column_id_refs_inner(&item.expr, out);
                }
            }
            ExprKind::SubqueryPlaceholder { .. } => {}
        }
    }

    pub(crate) fn collect_output_ids_ordered(plan: &LogicalPlanNode) -> Vec<ColumnId> {
        match &plan.kind {
            LogicalPlanKind::Scan(s) => s.columns.iter().map(|c| c.column_id).collect(),
            LogicalPlanKind::Project(p) => p
                .items
                .iter()
                .map(|item| item.output_column_id)
                .filter(|id| *id != ColumnId::UNSET)
                .collect(),
            LogicalPlanKind::Aggregate(a) => a.output_columns.iter().map(|c| c.column_id).collect(),
            LogicalPlanKind::Window(w) => w.output_columns.iter().map(|c| c.column_id).collect(),
            LogicalPlanKind::CTEProduce(p) => {
                p.output_columns.iter().map(|c| c.column_id).collect()
            }
            LogicalPlanKind::CTEConsume(c) => {
                c.output_columns.iter().map(|c| c.column_id).collect()
            }
            LogicalPlanKind::Union(u) => u.output_columns.iter().map(|c| c.column_id).collect(),
            LogicalPlanKind::Intersect(i) => i.output_columns.iter().map(|c| c.column_id).collect(),
            LogicalPlanKind::Except(e) => e.output_columns.iter().map(|c| c.column_id).collect(),
            LogicalPlanKind::Values(v) => v.columns.iter().map(|c| c.column_id).collect(),
            LogicalPlanKind::GenerateSeries(g) => {
                if g.output_column_id == ColumnId::UNSET {
                    vec![]
                } else {
                    vec![g.output_column_id]
                }
            }
            LogicalPlanKind::Filter(_)
            | LogicalPlanKind::Sort(_)
            | LogicalPlanKind::Limit(_)
            | LogicalPlanKind::Repeat(_) => collect_output_ids_ordered(plan.unary_input()),
            LogicalPlanKind::TableFunction(t) => {
                let mut ids = collect_output_ids_ordered(plan.unary_input());
                ids.extend(t.output_columns.iter().map(|c| c.column_id));
                ids
            }
            LogicalPlanKind::Join(_) => {
                let mut ids = collect_output_ids_ordered(plan.left());
                ids.extend(collect_output_ids_ordered(plan.right()));
                ids
            }
            LogicalPlanKind::CTEAnchor(_) => collect_output_ids_ordered(plan.child(1)),
            LogicalPlanKind::Apply(a) => {
                let mut ids = collect_output_ids_ordered(plan.left());
                ids.push(a.output_column.column_id);
                ids
            }
            LogicalPlanKind::AssertOneRow(_) => collect_output_ids_ordered(plan.unary_input()),
            LogicalPlanKind::ImvDelta(_) | LogicalPlanKind::ImvVersion(_) => {
                panic!("imv marker should not appear in non-IMV pruning")
            }
        }
    }

    pub(crate) fn collect_output_ids(plan: &LogicalPlanNode) -> HashSet<ColumnId> {
        collect_output_ids_ordered(plan).into_iter().collect()
    }

    fn collect_qualified_output_columns(plan: &LogicalPlanNode) -> HashSet<QualifiedRef> {
        let mut out = HashSet::new();
        collect_qualified_output_columns_inner(plan, &mut out);
        out
    }

    fn collect_qualified_output_columns_inner(
        plan: &LogicalPlanNode,
        out: &mut HashSet<QualifiedRef>,
    ) {
        match &plan.kind {
            LogicalPlanKind::Scan(s) => {
                let alias = s
                    .alias
                    .as_ref()
                    .map(|a| a.to_lowercase())
                    .or_else(|| Some(s.table.name.to_lowercase()));
                for c in &s.columns {
                    let col = c.name.to_lowercase();
                    if let Some(ref q) = alias {
                        out.insert((Some(q.clone()), col.clone()));
                    }
                    out.insert((None, col));
                }
            }
            LogicalPlanKind::Filter(_) | LogicalPlanKind::Sort(_) | LogicalPlanKind::Limit(_) => {
                collect_qualified_output_columns_inner(plan.unary_input(), out)
            }
            LogicalPlanKind::Project(p) => {
                for item in &p.items {
                    out.insert((None, item.output_name.to_lowercase()));
                }
            }
            LogicalPlanKind::Join(_) => {
                collect_qualified_output_columns_inner(plan.left(), out);
                collect_qualified_output_columns_inner(plan.right(), out);
            }
            LogicalPlanKind::Aggregate(a) => {
                for c in &a.output_columns {
                    out.insert((None, c.name.to_lowercase()));
                }
            }
            LogicalPlanKind::Values(v) => {
                for c in &v.columns {
                    out.insert((None, c.name.to_lowercase()));
                }
            }
            LogicalPlanKind::CTEAnchor(_) => {
                collect_qualified_output_columns_inner(plan.child(1), out);
            }
            LogicalPlanKind::CTEConsume(c) => {
                let alias_lower = c.alias.to_lowercase();
                for col in &c.output_columns {
                    let col_name = col.name.to_lowercase();
                    out.insert((Some(alias_lower.clone()), col_name.clone()));
                    out.insert((None, col_name));
                }
            }
            LogicalPlanKind::Apply(_) => collect_qualified_output_columns_inner(plan.left(), out),
            LogicalPlanKind::AssertOneRow(_) | LogicalPlanKind::Repeat(_) => {
                collect_qualified_output_columns_inner(plan.unary_input(), out)
            }
            _ => {}
        }
    }

    fn unwrap_column_ref(expr: &TypedExpr) -> Option<&TypedExpr> {
        match &expr.kind {
            ExprKind::ColumnRef { .. } => Some(expr),
            ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => unwrap_column_ref(expr),
            _ => None,
        }
    }

    fn classify_operand(
        expr: &TypedExpr,
        left_ids: &HashSet<ColumnId>,
        right_ids: &HashSet<ColumnId>,
        left_cols: &HashSet<QualifiedRef>,
        right_cols: &HashSet<QualifiedRef>,
    ) -> Option<(JoinSide, TypedExpr)> {
        let inner = unwrap_column_ref(expr)?;
        let ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } = &inner.kind
        else {
            unreachable!("unwrap_column_ref only returns a ColumnRef expression");
        };
        if *column_id != ColumnId::UNSET {
            match (left_ids.contains(column_id), right_ids.contains(column_id)) {
                (true, false) => return Some((JoinSide::Left, inner.clone())),
                (false, true) => return Some((JoinSide::Right, inner.clone())),
                _ => {}
            }
        }

        let key = (
            qualifier.as_ref().map(|q| q.to_lowercase()),
            column.to_lowercase(),
        );
        match (left_cols.contains(&key), right_cols.contains(&key)) {
            (true, false) => Some((JoinSide::Left, inner.clone())),
            (false, true) => Some((JoinSide::Right, inner.clone())),
            _ => None,
        }
    }

    pub(crate) fn join_equi_keys(
        join: &LogicalJoinNode,
        left: &LogicalPlanNode,
        right: &LogicalPlanNode,
    ) -> Vec<JoinEquiKey> {
        let Some(condition) = join.condition.as_ref() else {
            return Vec::new();
        };
        let left_cols = collect_qualified_output_columns(left);
        let right_cols = collect_qualified_output_columns(right);
        let left_ids = collect_output_ids(left);
        let right_ids = collect_output_ids(right);
        let mut keys = Vec::new();
        collect_join_equi_keys(
            condition,
            &left_ids,
            &right_ids,
            &left_cols,
            &right_cols,
            &mut keys,
        );
        keys
    }

    fn collect_join_equi_keys(
        expr: &TypedExpr,
        left_ids: &HashSet<ColumnId>,
        right_ids: &HashSet<ColumnId>,
        left_cols: &HashSet<QualifiedRef>,
        right_cols: &HashSet<QualifiedRef>,
        keys: &mut Vec<JoinEquiKey>,
    ) {
        match &expr.kind {
            ExprKind::BinaryOp {
                left,
                op: BinOp::And,
                right,
            } => {
                collect_join_equi_keys(left, left_ids, right_ids, left_cols, right_cols, keys);
                collect_join_equi_keys(right, left_ids, right_ids, left_cols, right_cols, keys);
            }
            ExprKind::BinaryOp {
                left,
                op: BinOp::Eq,
                right,
            } => match (
                classify_operand(left, left_ids, right_ids, left_cols, right_cols),
                classify_operand(right, left_ids, right_ids, left_cols, right_cols),
            ) {
                (Some((JoinSide::Left, le)), Some((JoinSide::Right, re)))
                | (Some((JoinSide::Right, re)), Some((JoinSide::Left, le))) => {
                    keys.push(JoinEquiKey {
                        left: le,
                        right: re,
                    });
                }
                _ => {}
            },
            _ => {}
        }
    }

    #[allow(dead_code)]
    fn _combine_and_for_legacy_tests(exprs: Vec<TypedExpr>) -> TypedExpr {
        combine_and(exprs)
    }
}

#[cfg(test)]
mod column_id_helper_tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::logical::*;
    use crate::sql::planner::payload::*;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;

    // -----------------------------------------------------------------------
    // collect_column_id_refs tests
    // -----------------------------------------------------------------------

    fn col_ref_expr(id: ColumnId) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: format!("c{}", id.0),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    #[test]
    fn simple_column_ref_collects_its_id() {
        let id = ColumnId::new_for_test(42);
        let expr = col_ref_expr(id);
        let result = collect_column_id_refs(&expr);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&id));
    }

    #[test]
    fn binary_op_collects_both_ids() {
        let id_left = ColumnId::new_for_test(1);
        let id_right = ColumnId::new_for_test(2);
        let expr = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref_expr(id_left)),
                op: crate::sql::analysis::BinOp::Eq,
                right: Box::new(col_ref_expr(id_right)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let result = collect_column_id_refs(&expr);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&id_left));
        assert!(result.contains(&id_right));
    }

    #[test]
    fn unset_column_ref_collects_nothing() {
        let expr = col_ref_expr(ColumnId::UNSET);
        let result = collect_column_id_refs(&expr);
        assert!(result.is_empty(), "UNSET must be excluded from the result");
    }

    #[test]
    fn scalar_column_ref_strict_collects_its_id() {
        let id = ColumnId::new_for_test(42);
        let mut arena = ScalarArena::new();
        let expr = arena.intern(ScalarNode::ColumnRef(id), DataType::Int32, false);

        let result = collect_scalar_column_id_refs_strict(&arena, expr)
            .expect("resolved scalar column refs should be collected");

        assert_eq!(result.len(), 1);
        assert!(result.contains(&id));
    }

    #[test]
    fn scalar_unset_column_ref_strict_returns_none() {
        let mut arena = ScalarArena::new();
        let expr = arena.intern(
            ScalarNode::ColumnRef(ColumnId::UNSET),
            DataType::Int32,
            false,
        );

        assert!(collect_scalar_column_id_refs_strict(&arena, expr).is_none());
    }

    // -----------------------------------------------------------------------
    // collect_output_ids / collect_output_ids_ordered tests
    // -----------------------------------------------------------------------

    fn make_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }
    }

    fn three_col_scan(ids: [ColumnId; 3]) -> LogicalPlanNode {
        let table = TableDef {
            name: "t".to_string(),
            columns: vec![
                ColumnDef {
                    name: "a".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "b".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "c".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
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
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "default".to_string(),
                table: table,
                alias: None,
                columns: vec![
                    make_output_column(ids[0], "a"),
                    make_output_column(ids[1], "b"),
                    make_output_column(ids[2], "c"),
                ],
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    #[test]
    fn scan_with_three_output_columns_returns_all_ids() {
        let ids = [
            ColumnId::new_for_test(1),
            ColumnId::new_for_test(2),
            ColumnId::new_for_test(3),
        ];
        let plan = three_col_scan(ids);
        let result = collect_output_ids(&plan);
        assert_eq!(result.len(), 3);
        for id in &ids {
            assert!(result.contains(id), "expected {:?} in result", id);
        }
    }

    #[test]
    fn scan_output_ids_ordered_preserves_column_order() {
        let ids = [
            ColumnId::new_for_test(10),
            ColumnId::new_for_test(20),
            ColumnId::new_for_test(30),
        ];
        let plan = three_col_scan(ids);
        let ordered = collect_output_ids_ordered(&plan);
        assert_eq!(ordered, vec![ids[0], ids[1], ids[2]]);
    }

    #[test]
    fn join_output_ids_are_left_then_right() {
        let left_ids = [
            ColumnId::new_for_test(1),
            ColumnId::new_for_test(2),
            ColumnId::new_for_test(3),
        ];
        let right_ids = [
            ColumnId::new_for_test(4),
            ColumnId::new_for_test(5),
            ColumnId::new_for_test(6),
        ];
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: crate::sql::analysis::JoinKind::Inner,
                condition: None,
            }),
            vec![three_col_scan(left_ids), three_col_scan(right_ids)],
            None,
        );
        let ordered = collect_output_ids_ordered(&plan);
        let expected: Vec<ColumnId> = left_ids.iter().chain(right_ids.iter()).copied().collect();
        assert_eq!(ordered, expected);
    }

    /// A Project with one passthrough ColumnRef item and one computed item
    /// (whose output_column_id is explicitly set) must return both ids.
    /// This validates that `collect_output_ids_ordered` now reads
    /// `output_column_id` instead of peeking at the expr's ColumnRef.
    #[test]
    fn project_passthrough_and_computed_both_collected() {
        use crate::sql::analysis::{BinOp, ProjectItem, TypedExpr};

        let pass_id = ColumnId::new_for_test(10);
        let comp_id = ColumnId::new_for_test(20);

        let scan = three_col_scan([
            pass_id,
            ColumnId::new_for_test(11),
            ColumnId::new_for_test(12),
        ]);

        // Passthrough item: expr is a ColumnRef with pass_id.
        let passthrough_item = ProjectItem {
            expr: col_ref_expr(pass_id),
            output_name: "a".to_string(),
            output_column_id: pass_id,
        };

        // Computed item: expr is a BinaryOp (not a ColumnRef), but output_column_id is set.
        let computed_item = ProjectItem {
            expr: TypedExpr {
                kind: crate::sql::analysis::ExprKind::BinaryOp {
                    left: Box::new(col_ref_expr(pass_id)),
                    op: BinOp::Add,
                    right: Box::new(col_ref_expr(ColumnId::new_for_test(11))),
                },
                data_type: DataType::Int32,
                nullable: false,
            },
            output_name: "computed".to_string(),
            output_column_id: comp_id,
        };

        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![passthrough_item, computed_item],
                output_qualifier: None,
            }),
            vec![scan],
            None,
        );

        let ordered = collect_output_ids_ordered(&plan);
        assert_eq!(
            ordered,
            vec![pass_id, comp_id],
            "both passthrough and computed output_column_ids must be returned"
        );
    }

    /// A Project item with UNSET output_column_id must be excluded
    /// (synthetic dict-slot items that are never addressed by pruning).
    #[test]
    fn project_unset_output_column_id_excluded() {
        use crate::sql::analysis::ProjectItem;

        let real_id = ColumnId::new_for_test(5);
        let scan = three_col_scan([
            real_id,
            ColumnId::new_for_test(6),
            ColumnId::new_for_test(7),
        ]);

        let real_item = ProjectItem {
            expr: col_ref_expr(real_id),
            output_name: "c".to_string(),
            output_column_id: real_id,
        };
        let unset_item = ProjectItem {
            expr: col_ref_expr(ColumnId::UNSET),
            output_name: "__synthetic".to_string(),
            output_column_id: ColumnId::UNSET,
        };

        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![real_item, unset_item],
                output_qualifier: None,
            }),
            vec![scan],
            None,
        );

        let ordered = collect_output_ids_ordered(&plan);
        assert_eq!(ordered, vec![real_id], "UNSET items must be filtered out");
    }

    #[test]
    fn generate_series_output_id_is_collected() {
        use crate::sql::planner::payload::PlanGenerateSeriesNode;

        let output_id = ColumnId::new_for_test(88);
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::GenerateSeries(PlanGenerateSeriesNode {
                start: 1,
                end: 3,
                step: 1,
                column_name: "x".to_string(),
                alias: Some("gs".to_string()),
                output_column_id: output_id,
            }),
            vec![],
            None,
        );

        let ordered = collect_output_ids_ordered(&plan);
        assert_eq!(
            ordered,
            vec![output_id],
            "GenerateSeries must expose its output ColumnId to pruning helpers"
        );
        let unordered = collect_output_ids(&plan);
        assert_eq!(unordered.len(), 1);
        assert!(unordered.contains(&output_id));
    }

    // ---------------------------------------------------------------------
    // join_equi_keys tests
    // ---------------------------------------------------------------------

    fn nullable_scan(alias: &str, table: &str, cols: &[(&str, u32)]) -> LogicalPlanNode {
        let column_defs = cols
            .iter()
            .map(|(name, _)| ColumnDef {
                name: name.to_string(),
                data_type: DataType::Int32,
                nullable: true,
                write_default: None,
                logical_type: None,
            })
            .collect();
        let output = cols
            .iter()
            .map(|(name, id)| OutputColumn {
                column_id: ColumnId::new_for_test(*id),
                name: name.to_string(),
                data_type: DataType::Int32,
                nullable: true,
                is_internal: false,
            })
            .collect();
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "default".to_string(),
                table: TableDef {
                    name: table.to_string(),
                    columns: column_defs,
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: Some(alias.to_string()),
                columns: output,
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn qcol(qualifier: &str, name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(qualifier.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: true,
        }
    }

    fn eq_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: crate::sql::analysis::BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn and_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: crate::sql::analysis::BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn two_table_join(condition: Option<TypedExpr>) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: crate::sql::analysis::JoinKind::Inner,
                condition,
            }),
            vec![
                nullable_scan("l", "tl", &[("a", 1), ("a2", 3)]),
                nullable_scan("r", "tr", &[("b", 2), ("b2", 4)]),
            ],
            None,
        )
    }

    fn test_join_equi_keys(join_plan: &LogicalPlanNode) -> Vec<JoinEquiKey> {
        let LogicalPlanKind::Join(join) = &join_plan.kind else {
            panic!("expected Join test plan");
        };
        join_equi_keys(join, join_plan.left(), join_plan.right())
    }

    fn test_join_equi_keys_opt(
        join_plan: &LogicalPlanNode,
    ) -> (Vec<ScalarJoinEquiKey>, ScalarArena) {
        let mut arena = ScalarArena::new();
        let expr = crate::sql::planner::optimizer_bridge::logical::to_optimizer_expr(
            join_plan, &mut arena,
        );
        let Operator::LogicalJoin(join) = &expr.op else {
            panic!("expected LogicalJoin test expression");
        };
        (
            join_equi_keys_opt(join, expr.left(), expr.right(), &arena),
            arena,
        )
    }

    fn scalar_column_qualifier(arena: &ScalarArena, expr: ScalarId) -> Option<String> {
        let ScalarNode::ColumnRef(column_id) = arena.node(expr) else {
            panic!("expected ColumnRef scalar");
        };
        arena
            .column_display(*column_id)
            .and_then(|display| display.qualifier.clone())
    }

    #[test]
    fn join_equi_keys_extracts_single_pair_oriented_left_right() {
        let join = two_table_join(Some(eq_expr(qcol("l", "a", 1), qcol("r", "b", 2))));
        let keys = test_join_equi_keys(&join);
        assert_eq!(keys.len(), 1);
        // left operand belongs to join.left, right operand to join.right.
        assert!(matches!(&keys[0].left.kind, ExprKind::ColumnRef { column, .. } if column == "a"));
        assert!(matches!(&keys[0].right.kind, ExprKind::ColumnRef { column, .. } if column == "b"));
    }

    #[test]
    fn join_equi_keys_orients_reversed_pair() {
        // r.b = l.a  -> still left=a, right=b
        let join = two_table_join(Some(eq_expr(qcol("r", "b", 2), qcol("l", "a", 1))));
        let keys = test_join_equi_keys(&join);
        assert_eq!(keys.len(), 1);
        assert!(matches!(&keys[0].left.kind, ExprKind::ColumnRef { column, .. } if column == "a"));
        assert!(matches!(&keys[0].right.kind, ExprKind::ColumnRef { column, .. } if column == "b"));
    }

    #[test]
    fn join_equi_keys_collects_each_and_conjunct() {
        let join = two_table_join(Some(and_expr(
            eq_expr(qcol("l", "a", 1), qcol("r", "b", 2)),
            eq_expr(qcol("l", "a2", 3), qcol("r", "b2", 4)),
        )));
        let keys = test_join_equi_keys(&join);
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn join_equi_keys_skips_non_equi_and_missing_condition() {
        assert!(test_join_equi_keys(&two_table_join(None)).is_empty());
        let gt = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(qcol("l", "a", 1)),
                op: crate::sql::analysis::BinOp::Gt,
                right: Box::new(qcol("r", "b", 2)),
            },
            data_type: DataType::Boolean,
            nullable: true,
        };
        assert!(test_join_equi_keys(&two_table_join(Some(gt))).is_empty());
    }

    #[test]
    fn join_equi_keys_peels_cast_wrapper() {
        let cast_col = TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(qcol("l", "a", 1)),
                target: DataType::Int64,
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        let join = two_table_join(Some(eq_expr(cast_col, qcol("r", "b", 2))));
        let keys = test_join_equi_keys(&join);
        assert_eq!(keys.len(), 1);
        assert!(matches!(&keys[0].left.kind, ExprKind::ColumnRef { column, .. } if column == "a"));
    }

    #[test]
    fn join_equi_keys_excludes_null_safe_eq() {
        let cond = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(qcol("l", "a", 1)),
                op: crate::sql::analysis::BinOp::EqForNull,
                right: Box::new(qcol("r", "b", 2)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        assert!(test_join_equi_keys(&two_table_join(Some(cond))).is_empty());
    }

    #[test]
    fn join_equi_keys_opt_excludes_null_safe_eq() {
        let cond = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(qcol("l", "a", 1)),
                op: crate::sql::analysis::BinOp::EqForNull,
                right: Box::new(qcol("r", "b", 2)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        assert!(
            test_join_equi_keys_opt(&two_table_join(Some(cond)))
                .0
                .is_empty()
        );
    }

    #[test]
    fn join_equi_keys_disambiguates_self_join_by_qualifier() {
        // q22 shape: same column name on both sides, distinct aliases.
        let join = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: crate::sql::analysis::JoinKind::LeftSemi,
                condition: Some(eq_expr(qcol("a", "k", 1), qcol("b", "k", 2))),
            }),
            vec![
                nullable_scan("a", "t", &[("k", 1)]),
                nullable_scan("b", "t", &[("k", 2)]),
            ],
            None,
        );
        let keys = test_join_equi_keys(&join);
        assert_eq!(keys.len(), 1);
        assert!(
            matches!(&keys[0].left.kind, ExprKind::ColumnRef { qualifier: Some(q), .. } if q == "a")
        );
        assert!(
            matches!(&keys[0].right.kind, ExprKind::ColumnRef { qualifier: Some(q), .. } if q == "b")
        );
    }

    #[test]
    fn join_equi_keys_opt_disambiguates_self_join_by_qualifier() {
        let join = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: crate::sql::analysis::JoinKind::LeftSemi,
                condition: Some(eq_expr(qcol("a", "k", 1), qcol("b", "k", 2))),
            }),
            vec![
                nullable_scan("a", "t", &[("k", 1)]),
                nullable_scan("b", "t", &[("k", 2)]),
            ],
            None,
        );
        let (keys, arena) = test_join_equi_keys_opt(&join);
        assert_eq!(keys.len(), 1);
        assert_eq!(
            scalar_column_qualifier(&arena, keys[0].left).as_deref(),
            Some("a")
        );
        assert_eq!(
            scalar_column_qualifier(&arena, keys[0].right).as_deref(),
            Some("b")
        );
    }

    fn derived_project_with_output_id(
        name: &str,
        source_id: u32,
        output_id: u32,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId::new_for_test(source_id),
                            qualifier: None,
                            column: format!("{name}_source"),
                        },
                        data_type: DataType::Int32,
                        nullable: true,
                    },
                    output_name: "k".to_string(),
                    output_column_id: ColumnId::new_for_test(output_id),
                }],
                output_qualifier: None,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Values(PlanValuesNode {
                    rows: vec![],
                    columns: vec![OutputColumn {
                        column_id: ColumnId::new_for_test(source_id),
                        name: format!("{name}_source"),
                        data_type: DataType::Int32,
                        nullable: true,
                        is_internal: false,
                    }],
                }),
                vec![],
                None,
            )],
            None,
        )
    }

    #[test]
    fn join_equi_keys_classifies_alias_free_project_outputs_by_column_id() {
        let join = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: crate::sql::analysis::JoinKind::Inner,
                condition: Some(eq_expr(qcol("a", "k", 101), qcol("b", "k", 202))),
            }),
            vec![
                derived_project_with_output_id("left", 11, 101),
                derived_project_with_output_id("right", 22, 202),
            ],
            None,
        );

        let keys = test_join_equi_keys(&join);
        assert_eq!(keys.len(), 1);
        assert!(
            matches!(&keys[0].left.kind, ExprKind::ColumnRef { column_id, qualifier: Some(q), column, .. }
                if *column_id == ColumnId::new_for_test(101) && q == "a" && column == "k")
        );
        assert!(
            matches!(&keys[0].right.kind, ExprKind::ColumnRef { column_id, qualifier: Some(q), column, .. }
                if *column_id == ColumnId::new_for_test(202) && q == "b" && column == "k")
        );
    }
}
