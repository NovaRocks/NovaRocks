use std::collections::HashSet;

use crate::sql::ir::{BinOp, ExprKind, TypedExpr};
use crate::sql::plan::*;

/// Split an expression on AND into a flat list of conjuncts.
pub(super) fn split_and(expr: TypedExpr) -> Vec<TypedExpr> {
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

/// Combine a list of conjuncts back into a single AND expression.
/// Panics if `exprs` is empty.
pub(super) fn combine_and(mut exprs: Vec<TypedExpr>) -> TypedExpr {
    assert!(!exprs.is_empty());
    let mut result = exprs.pop().unwrap();
    while let Some(left) = exprs.pop() {
        result = TypedExpr {
            data_type: arrow::datatypes::DataType::Boolean,
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

/// Collect all column names referenced in an expression (unqualified, lowercase).
pub(super) fn collect_column_refs(expr: &TypedExpr) -> Vec<&str> {
    let mut out = Vec::new();
    collect_column_refs_inner(expr, &mut out);
    out
}

fn collect_column_refs_inner<'a>(expr: &'a TypedExpr, out: &mut Vec<&'a str>) {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => out.push(column.as_str()),
        ExprKind::BinaryOp { left, right, .. } => {
            collect_column_refs_inner(left, out);
            collect_column_refs_inner(right, out);
        }
        ExprKind::UnaryOp { expr, .. } => collect_column_refs_inner(expr, out),
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_column_refs_inner(arg, out);
            }
        }
        ExprKind::AggregateCall { args, .. } => {
            for arg in args {
                collect_column_refs_inner(arg, out);
            }
        }
        ExprKind::Cast { expr, .. } => collect_column_refs_inner(expr, out),
        ExprKind::IsNull { expr, .. } => collect_column_refs_inner(expr, out),
        ExprKind::InList { expr, list, .. } => {
            collect_column_refs_inner(expr, out);
            for item in list {
                collect_column_refs_inner(item, out);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_column_refs_inner(expr, out);
            collect_column_refs_inner(low, out);
            collect_column_refs_inner(high, out);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_column_refs_inner(expr, out);
            collect_column_refs_inner(pattern, out);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_column_refs_inner(operand, out);
            }
            for (when, then) in when_then {
                collect_column_refs_inner(when, out);
                collect_column_refs_inner(then, out);
            }
            if let Some(else_expr) = else_expr {
                collect_column_refs_inner(else_expr, out);
            }
        }
        ExprKind::IsTruthValue { expr, .. } => collect_column_refs_inner(expr, out),
        ExprKind::Nested(inner) => collect_column_refs_inner(inner, out),
        ExprKind::Literal(_) => {}
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_column_refs_inner(arg, out);
            }
            for pb in partition_by {
                collect_column_refs_inner(pb, out);
            }
            for ob in order_by {
                collect_column_refs_inner(&ob.expr, out);
            }
        }
        // SubqueryPlaceholder should be rewritten before reaching here,
        // but handle gracefully as a no-op.
        ExprKind::SubqueryPlaceholder { .. } => {}
    }
}

/// Collect all column names available from a plan subtree (lowercase).
///
/// This is used by predicate pushdown to determine which side of a join a
/// predicate references.
pub(super) fn collect_output_columns(plan: &LogicalPlan) -> HashSet<String> {
    match plan {
        LogicalPlan::Scan(s) => s.columns.iter().map(|c| c.name.to_lowercase()).collect(),
        LogicalPlan::Filter(f) => collect_output_columns(&f.input),
        LogicalPlan::Project(p) => p
            .items
            .iter()
            .map(|item| item.output_name.to_lowercase())
            .collect(),
        LogicalPlan::Join(j) => {
            let mut cols = collect_output_columns(&j.left);
            cols.extend(collect_output_columns(&j.right));
            cols
        }
        LogicalPlan::Aggregate(a) => a
            .output_columns
            .iter()
            .map(|c| c.name.to_lowercase())
            .collect(),
        LogicalPlan::Sort(s) => collect_output_columns(&s.input),
        LogicalPlan::Limit(l) => collect_output_columns(&l.input),
        LogicalPlan::Window(w) => w
            .output_columns
            .iter()
            .map(|c| c.name.to_lowercase())
            .collect(),
        LogicalPlan::Union(u) => {
            if let Some(first) = u.inputs.first() {
                collect_output_columns(first)
            } else {
                HashSet::new()
            }
        }
        LogicalPlan::Intersect(i) => {
            if let Some(first) = i.inputs.first() {
                collect_output_columns(first)
            } else {
                HashSet::new()
            }
        }
        LogicalPlan::Except(e) => {
            if let Some(first) = e.inputs.first() {
                collect_output_columns(first)
            } else {
                HashSet::new()
            }
        }
        LogicalPlan::Values(v) => v.columns.iter().map(|c| c.name.to_lowercase()).collect(),
        LogicalPlan::GenerateSeries(g) => {
            let mut cols = HashSet::new();
            cols.insert(g.column_name.to_lowercase());
            cols
        }
        LogicalPlan::CTEAnchor(a) => collect_output_columns(&a.consumer),
        LogicalPlan::CTEProduce(p) => p
            .output_columns
            .iter()
            .map(|col| col.name.to_lowercase())
            .collect(),
        LogicalPlan::SubqueryAlias(s) => s
            .output_columns
            .iter()
            .map(|c| c.name.to_lowercase())
            .collect(),
        LogicalPlan::Repeat(r) => collect_output_columns(&r.input),
        LogicalPlan::CTEConsume(c) => c
            .output_columns
            .iter()
            .map(|col| col.name.to_lowercase())
            .collect(),
    }
}

/// Merge a parent's needed columns with additional column names.
pub(super) fn merge_needed(parent: Option<&HashSet<String>>, extra: &[&str]) -> HashSet<String> {
    let mut result = parent.cloned().unwrap_or_default();
    for col in extra {
        result.insert(col.to_lowercase());
    }
    result
}

/// Wrap a plan in a Filter if there are remaining (un-pushed) predicates.
pub(super) fn wrap_remaining_filter(plan: LogicalPlan, remaining: Vec<TypedExpr>) -> LogicalPlan {
    if remaining.is_empty() {
        plan
    } else {
        LogicalPlan::Filter(FilterNode {
            input: Box::new(plan),
            predicate: combine_and(remaining),
        })
    }
}

/// Qualified column reference: (qualifier, column), both lowercase.
pub(super) type QualifiedRef = (Option<String>, String);

/// Collect all column references in an expression, preserving qualifiers.
///
/// Unlike [`collect_column_refs`] which returns bare column names, this
/// function returns `(qualifier, column)` pairs so that self-join predicates
/// (where both sides have the same column names) can be properly classified.
pub(super) fn collect_qualified_column_refs(expr: &TypedExpr) -> Vec<QualifiedRef> {
    let mut out = Vec::new();
    collect_qualified_column_refs_inner(expr, &mut out);
    out
}

fn collect_qualified_column_refs_inner(expr: &TypedExpr, out: &mut Vec<QualifiedRef>) {
    match &expr.kind {
        ExprKind::ColumnRef { qualifier, column } => {
            out.push((
                qualifier.as_ref().map(|q| q.to_lowercase()),
                column.to_lowercase(),
            ));
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_qualified_column_refs_inner(left, out);
            collect_qualified_column_refs_inner(right, out);
        }
        ExprKind::UnaryOp { expr, .. } => collect_qualified_column_refs_inner(expr, out),
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_qualified_column_refs_inner(arg, out);
            }
        }
        ExprKind::AggregateCall { args, .. } => {
            for arg in args {
                collect_qualified_column_refs_inner(arg, out);
            }
        }
        ExprKind::Cast { expr, .. } => collect_qualified_column_refs_inner(expr, out),
        ExprKind::IsNull { expr, .. } => collect_qualified_column_refs_inner(expr, out),
        ExprKind::InList { expr, list, .. } => {
            collect_qualified_column_refs_inner(expr, out);
            for item in list {
                collect_qualified_column_refs_inner(item, out);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_qualified_column_refs_inner(expr, out);
            collect_qualified_column_refs_inner(low, out);
            collect_qualified_column_refs_inner(high, out);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_qualified_column_refs_inner(expr, out);
            collect_qualified_column_refs_inner(pattern, out);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_qualified_column_refs_inner(operand, out);
            }
            for (when, then) in when_then {
                collect_qualified_column_refs_inner(when, out);
                collect_qualified_column_refs_inner(then, out);
            }
            if let Some(else_expr) = else_expr {
                collect_qualified_column_refs_inner(else_expr, out);
            }
        }
        ExprKind::IsTruthValue { expr, .. } => collect_qualified_column_refs_inner(expr, out),
        ExprKind::Nested(inner) => collect_qualified_column_refs_inner(inner, out),
        ExprKind::Literal(_) => {}
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_qualified_column_refs_inner(arg, out);
            }
            for pb in partition_by {
                collect_qualified_column_refs_inner(pb, out);
            }
            for ob in order_by {
                collect_qualified_column_refs_inner(&ob.expr, out);
            }
        }
        ExprKind::SubqueryPlaceholder { .. } => {}
    }
}

/// Collect qualified output columns from a plan subtree.
///
/// Returns `(qualifier, column_name)` pairs where `qualifier` is the table
/// alias (for Scan nodes with an alias) or `None`.  Each column also yields a
/// bare `(None, column_name)` entry so that unqualified references still match.
pub(super) fn collect_qualified_output_columns(plan: &LogicalPlan) -> HashSet<QualifiedRef> {
    let mut out = HashSet::new();
    collect_qualified_output_columns_inner(plan, &mut out);
    out
}

fn collect_qualified_output_columns_inner(plan: &LogicalPlan, out: &mut HashSet<QualifiedRef>) {
    match plan {
        LogicalPlan::Scan(s) => {
            let alias = s
                .alias
                .as_ref()
                .map(|a| a.to_lowercase())
                .or_else(|| Some(s.table.name.to_lowercase()));
            for c in &s.columns {
                let col = c.name.to_lowercase();
                // Qualified entry: (alias, column)
                if let Some(ref q) = alias {
                    out.insert((Some(q.clone()), col.clone()));
                }
                // Bare entry: (None, column)
                out.insert((None, col));
            }
        }
        LogicalPlan::Filter(f) => collect_qualified_output_columns_inner(&f.input, out),
        LogicalPlan::Project(p) => {
            for item in &p.items {
                out.insert((None, item.output_name.to_lowercase()));
            }
        }
        LogicalPlan::Join(j) => {
            collect_qualified_output_columns_inner(&j.left, out);
            collect_qualified_output_columns_inner(&j.right, out);
        }
        LogicalPlan::Aggregate(a) => {
            for c in &a.output_columns {
                out.insert((None, c.name.to_lowercase()));
            }
        }
        LogicalPlan::Sort(s) => collect_qualified_output_columns_inner(&s.input, out),
        LogicalPlan::Limit(l) => collect_qualified_output_columns_inner(&l.input, out),
        LogicalPlan::Window(w) => {
            for c in &w.output_columns {
                out.insert((None, c.name.to_lowercase()));
            }
        }
        LogicalPlan::Union(u) => {
            if let Some(first) = u.inputs.first() {
                collect_qualified_output_columns_inner(first, out);
            }
        }
        LogicalPlan::Intersect(i) => {
            if let Some(first) = i.inputs.first() {
                collect_qualified_output_columns_inner(first, out);
            }
        }
        LogicalPlan::Except(e) => {
            if let Some(first) = e.inputs.first() {
                collect_qualified_output_columns_inner(first, out);
            }
        }
        LogicalPlan::Values(v) => {
            for c in &v.columns {
                out.insert((None, c.name.to_lowercase()));
            }
        }
        LogicalPlan::GenerateSeries(g) => {
            out.insert((None, g.column_name.to_lowercase()));
        }
        LogicalPlan::CTEAnchor(a) => {
            collect_qualified_output_columns_inner(&a.consumer, out);
        }
        LogicalPlan::CTEProduce(p) => {
            for col in &p.output_columns {
                out.insert((None, col.name.to_lowercase()));
            }
        }
        LogicalPlan::SubqueryAlias(s) => {
            let alias_lower = s.alias.to_lowercase();
            for c in &s.output_columns {
                let col = c.name.to_lowercase();
                out.insert((Some(alias_lower.clone()), col.clone()));
                out.insert((None, col));
            }
        }
        LogicalPlan::Repeat(r) => collect_qualified_output_columns_inner(&r.input, out),
        LogicalPlan::CTEConsume(c) => {
            let alias_lower = c.alias.to_lowercase();
            for col in &c.output_columns {
                let col_name = col.name.to_lowercase();
                out.insert((Some(alias_lower.clone()), col_name.clone()));
                out.insert((None, col_name));
            }
        }
    }
}
