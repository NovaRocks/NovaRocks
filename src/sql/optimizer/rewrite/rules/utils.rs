//! Shared expression / plan utilities for the query rewrite rules and any cascades
//! code that needs small AST helpers. Moved from
//! `src/sql/optimizer/expr_utils.rs` in Phase 4 of the optimizer
//! unification; contents unchanged.

use std::collections::HashSet;

use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};
use crate::sql::planner::plan::*;

/// Split an expression on AND into a flat list of conjuncts.
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

/// Combine a list of conjuncts back into a single AND expression.
/// Panics if `exprs` is empty.
pub(crate) fn combine_and(mut exprs: Vec<TypedExpr>) -> TypedExpr {
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
pub(crate) fn collect_column_refs(expr: &TypedExpr) -> Vec<&str> {
    let mut out = Vec::new();
    collect_column_refs_inner(expr, &mut out);
    out
}

fn collect_column_refs_inner<'a>(expr: &'a TypedExpr, out: &mut Vec<&'a str>) {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => out.push(column.as_str()),
        ExprKind::LambdaParamRef { .. } => {}
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
        ExprKind::LambdaFunction { body, .. } => collect_column_refs_inner(body, out),
        ExprKind::AggregateCall { args, order_by, .. } => {
            for arg in args {
                collect_column_refs_inner(arg, out);
            }
            for ob in order_by {
                collect_column_refs_inner(&ob.expr, out);
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
        ExprKind::Lambda { params, body } => {
            // Walk the body but drop lambda-bound parameter names so we do not
            // misclassify them as outer-column references. Outer column refs
            // remain visible (`x + v2` inside `array_map(x -> x + v2, ...)`
            // must still report `v2`).
            let mut nested: Vec<&'a str> = Vec::new();
            collect_column_refs_inner(body, &mut nested);
            let bound: std::collections::HashSet<String> =
                params.iter().map(|p| p.to_lowercase()).collect();
            for name in nested {
                if !bound.contains(&name.to_lowercase()) {
                    out.push(name);
                }
            }
        }
    }
}

/// Collect all column names available from a plan subtree (lowercase).
///
/// This is used by predicate pushdown to determine which side of a join a
/// predicate references.
pub(crate) fn collect_output_columns(plan: &LogicalPlan) -> HashSet<String> {
    match plan {
        LogicalPlan::Scan(s) => s.columns.iter().map(|c| c.name.to_lowercase()).collect(),
        LogicalPlan::Filter(f) => collect_output_columns(&f.input),
        LogicalPlan::Project(p) => p
            .items
            .iter()
            .map(|item| item.output_name.to_lowercase())
            .collect(),
        LogicalPlan::Join(j) => {
            let left_only = matches!(
                j.join_type,
                crate::sql::analysis::JoinKind::LeftSemi
                    | crate::sql::analysis::JoinKind::LeftAnti
                    | crate::sql::analysis::JoinKind::RightSemi
                    | crate::sql::analysis::JoinKind::RightAnti
            );
            if left_only {
                collect_output_columns(&j.left)
            } else {
                let mut cols = collect_output_columns(&j.left);
                cols.extend(collect_output_columns(&j.right));
                cols
            }
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
        LogicalPlan::TableFunction(t) => {
            let mut cols = collect_output_columns(&t.input);
            cols.extend(t.output_columns.iter().map(|c| c.name.to_lowercase()));
            cols
        }
        LogicalPlan::CTEAnchor(a) => collect_output_columns(&a.consumer),
        LogicalPlan::CTEProduce(p) => p
            .output_columns
            .iter()
            .map(|col| col.name.to_lowercase())
            .collect(),
        LogicalPlan::Repeat(r) => collect_output_columns(&r.input),
        LogicalPlan::CTEConsume(c) => c
            .output_columns
            .iter()
            .map(|col| col.name.to_lowercase())
            .collect(),
        LogicalPlan::Decode(d) => {
            // Decode replaces dict columns with their string counterparts
            // but otherwise passes through the child's output set.
            let mut cols = collect_output_columns(&d.input);
            for mapping in &d.mappings {
                cols.remove(&mapping.dict_column.to_lowercase());
                cols.insert(mapping.string_column.to_lowercase());
            }
            cols
        }
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

/// Collect every [`ColumnId`] referenced by an expression.
///
/// [`ColumnId::UNSET`] is deliberately excluded — an unresolved reference must
/// not constrain column pruning.
pub(crate) fn collect_column_id_refs(expr: &TypedExpr) -> HashSet<crate::sql::column_id::ColumnId> {
    let mut out = HashSet::new();
    collect_column_id_refs_inner(expr, &mut out);
    out
}

fn collect_column_id_refs_inner(
    expr: &TypedExpr,
    out: &mut HashSet<crate::sql::column_id::ColumnId>,
) {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => {
            if *column_id != crate::sql::column_id::ColumnId::UNSET {
                out.insert(*column_id);
            }
        }
        ExprKind::LambdaParamRef { .. } => {}
        ExprKind::BinaryOp { left, right, .. } => {
            collect_column_id_refs_inner(left, out);
            collect_column_id_refs_inner(right, out);
        }
        ExprKind::UnaryOp { expr, .. } => collect_column_id_refs_inner(expr, out),
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_column_id_refs_inner(arg, out);
            }
        }
        ExprKind::LambdaFunction { body, .. } => collect_column_id_refs_inner(body, out),
        ExprKind::AggregateCall { args, order_by, .. } => {
            for arg in args {
                collect_column_id_refs_inner(arg, out);
            }
            for ob in order_by {
                collect_column_id_refs_inner(&ob.expr, out);
            }
        }
        ExprKind::Cast { expr, .. } => collect_column_id_refs_inner(expr, out),
        ExprKind::IsNull { expr, .. } => collect_column_id_refs_inner(expr, out),
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
        ExprKind::IsTruthValue { expr, .. } => collect_column_id_refs_inner(expr, out),
        ExprKind::Nested(inner) => collect_column_id_refs_inner(inner, out),
        ExprKind::Literal(_) => {}
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_column_id_refs_inner(arg, out);
            }
            for pb in partition_by {
                collect_column_id_refs_inner(pb, out);
            }
            for ob in order_by {
                collect_column_id_refs_inner(&ob.expr, out);
            }
        }
        // SubqueryPlaceholder should be rewritten before reaching here,
        // but handle gracefully as a no-op.
        ExprKind::SubqueryPlaceholder { .. } => {}
        ExprKind::Lambda { body, .. } => {
            // Lambda-bound parameters are emitted as the distinct `LambdaParamRef`
            // variant (a no-op above), NOT as `ColumnRef`, so they never enter the
            // id set — no filtering needed here. Walk the body to capture outer
            // column refs the closure captures.
            collect_column_id_refs_inner(body, out);
        }
    }
}

/// Return the ordered list of [`ColumnId`]s in the output schema of a plan node.
///
/// This is the authoritative source for "which ColumnIds does this subtree produce",
/// used by the Phase-1 column-pruning tagging pass to split a parent's needed set
/// across join/set-op children.
pub(crate) fn collect_output_ids_ordered(
    plan: &LogicalPlan,
) -> Vec<crate::sql::column_id::ColumnId> {
    match plan {
        LogicalPlan::Scan(s) => s.columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Project(p) => p
            .items
            .iter()
            .map(|item| item.output_column_id)
            .filter(|id| *id != crate::sql::column_id::ColumnId::UNSET)
            .collect(),
        LogicalPlan::Aggregate(a) => a.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Window(w) => w.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::CTEProduce(p) => p.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::CTEConsume(c) => c.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Union(u) => u.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Intersect(i) => i.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Except(e) => e.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Decode(d) => d.output_columns.iter().map(|c| c.column_id).collect(),
        LogicalPlan::Values(v) => v.columns.iter().map(|c| c.column_id).collect(),
        // GenerateSeries has no ColumnId on its output slot (only a name string).
        LogicalPlan::GenerateSeries(_) => vec![],
        // Passthrough: the node does not add or rename output ColumnIds.
        LogicalPlan::Filter(f) => collect_output_ids_ordered(&f.input),
        LogicalPlan::Sort(s) => collect_output_ids_ordered(&s.input),
        LogicalPlan::Limit(l) => collect_output_ids_ordered(&l.input),
        LogicalPlan::Repeat(r) => collect_output_ids_ordered(&r.input),
        LogicalPlan::TableFunction(t) => {
            // TableFunction extends the input's output with its own output columns.
            let mut ids = collect_output_ids_ordered(&t.input);
            ids.extend(t.output_columns.iter().map(|c| c.column_id));
            ids
        }
        LogicalPlan::Join(j) => {
            // Join output = left output ids ++ right output ids (left first).
            let mut ids = collect_output_ids_ordered(&j.left);
            ids.extend(collect_output_ids_ordered(&j.right));
            ids
        }
        LogicalPlan::CTEAnchor(a) => collect_output_ids_ordered(&a.consumer),
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker should not appear in non-IMV pruning")
        }
    }
}

/// Return the set of [`ColumnId`]s in the output schema of a plan node.
///
/// `collect_output_ids(plan) = collect_output_ids_ordered(plan).into_iter().collect()`.
pub(crate) fn collect_output_ids(plan: &LogicalPlan) -> HashSet<crate::sql::column_id::ColumnId> {
    collect_output_ids_ordered(plan).into_iter().collect()
}

/// Merge a parent's needed columns with additional column names.
pub(crate) fn merge_needed(parent: Option<&HashSet<String>>, extra: &[&str]) -> HashSet<String> {
    let mut result = parent.cloned().unwrap_or_default();
    for col in extra {
        result.insert(col.to_lowercase());
    }
    result
}

/// Wrap a plan in a Filter if there are remaining (un-pushed) predicates.
pub(crate) fn wrap_remaining_filter(plan: LogicalPlan, remaining: Vec<TypedExpr>) -> LogicalPlan {
    if remaining.is_empty() {
        plan
    } else {
        LogicalPlan::Filter(FilterNode {
            input: Box::new(plan),
            predicate: combine_and(remaining),
            required_output_columns: None,
        })
    }
}

/// Qualified column reference: (qualifier, column), both lowercase.
pub(crate) type QualifiedRef = (Option<String>, String);

/// Collect all column references in an expression, preserving qualifiers.
///
/// Unlike [`collect_column_refs`] which returns bare column names, this
/// function returns `(qualifier, column)` pairs so that self-join predicates
/// (where both sides have the same column names) can be properly classified.
pub(crate) fn collect_qualified_column_refs(expr: &TypedExpr) -> Vec<QualifiedRef> {
    let mut out = Vec::new();
    collect_qualified_column_refs_inner(expr, &mut out);
    out
}

fn collect_qualified_column_refs_inner(expr: &TypedExpr, out: &mut Vec<QualifiedRef>) {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier, column, ..
        } => {
            out.push((
                qualifier.as_ref().map(|q| q.to_lowercase()),
                column.to_lowercase(),
            ));
        }
        ExprKind::LambdaParamRef { .. } => {}
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
        ExprKind::LambdaFunction { body, .. } => {
            collect_qualified_column_refs_inner(body, out);
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            for arg in args {
                collect_qualified_column_refs_inner(arg, out);
            }
            for ob in order_by {
                collect_qualified_column_refs_inner(&ob.expr, out);
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
        ExprKind::Lambda { params, body } => {
            // Filter out lambda-bound parameter names from collected refs.
            let mut nested = Vec::new();
            collect_qualified_column_refs_inner(body, &mut nested);
            let bound: std::collections::HashSet<String> =
                params.iter().map(|p| p.to_lowercase()).collect();
            for (qual, name) in nested {
                if qual.is_some() || !bound.contains(&name) {
                    out.push((qual, name));
                }
            }
        }
    }
}

/// Collect qualified output columns from a plan subtree.
///
/// Returns `(qualifier, column_name)` pairs where `qualifier` is the table
/// alias (for Scan nodes with an alias) or `None`.  Each column also yields a
/// bare `(None, column_name)` entry so that unqualified references still match.
pub(crate) fn collect_qualified_output_columns(plan: &LogicalPlan) -> HashSet<QualifiedRef> {
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
        LogicalPlan::TableFunction(t) => {
            collect_qualified_output_columns_inner(&t.input, out);
            for col in &t.output_columns {
                out.insert((
                    t.alias.as_ref().map(|alias| alias.to_lowercase()),
                    col.name.to_lowercase(),
                ));
            }
        }
        LogicalPlan::CTEAnchor(a) => {
            collect_qualified_output_columns_inner(&a.consumer, out);
        }
        LogicalPlan::CTEProduce(p) => {
            for col in &p.output_columns {
                out.insert((None, col.name.to_lowercase()));
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
        LogicalPlan::Decode(d) => {
            collect_qualified_output_columns_inner(&d.input, out);
            // Decode adds string output columns alongside the dict inputs.
            for mapping in &d.mappings {
                out.insert((None, mapping.string_column.to_lowercase()));
            }
        }
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

/// One equi-join key pair, with operands oriented so `left` comes from the
/// join's left child and `right` from the right child. Operands are the
/// unwrapped inner `ColumnRef` (Cast/Nested peeled).
#[derive(Debug)]
pub(crate) struct JoinEquiKey {
    pub(crate) left: TypedExpr,
    pub(crate) right: TypedExpr,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
}

/// Peel `Cast` / `Nested` wrappers and return the inner `ColumnRef` expr, if any.
fn unwrap_column_ref(expr: &TypedExpr) -> Option<&TypedExpr> {
    match &expr.kind {
        ExprKind::ColumnRef { .. } => Some(expr),
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) => unwrap_column_ref(expr),
        _ => None,
    }
}

/// Classify a join-condition operand as left/right child column, returning the
/// unwrapped inner `ColumnRef` clone. `None` if it is not a column unambiguously
/// owned by exactly one side (constants, expressions, ambiguous self-join refs).
fn classify_operand(
    expr: &TypedExpr,
    left_cols: &HashSet<QualifiedRef>,
    right_cols: &HashSet<QualifiedRef>,
) -> Option<(JoinSide, TypedExpr)> {
    let inner = unwrap_column_ref(expr)?;
    let ExprKind::ColumnRef {
        qualifier, column, ..
    } = &inner.kind
    else {
        unreachable!("unwrap_column_ref only returns a ColumnRef expression");
    };
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

/// Extract equi-join key pairs from a join's ON condition (lenient: walks the
/// top-level AND chain and keeps every `col = col` conjunct it can orient,
/// ignoring other conjuncts). Returns empty when there is no usable equi key.
pub(crate) fn join_equi_keys(join: &JoinNode) -> Vec<JoinEquiKey> {
    let Some(condition) = join.condition.as_ref() else {
        return Vec::new();
    };
    let left_cols = collect_qualified_output_columns(&join.left);
    let right_cols = collect_qualified_output_columns(&join.right);
    let mut keys = Vec::new();
    collect_join_equi_keys(condition, &left_cols, &right_cols, &mut keys);
    keys
}

fn collect_join_equi_keys(
    expr: &TypedExpr,
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
            collect_join_equi_keys(left, left_cols, right_cols, keys);
            collect_join_equi_keys(right, left_cols, right_cols, keys);
        }
        // Only strict `Eq`. `EqForNull` (<=>) is null-safe (NULL <=> NULL is
        // true), so deriving IS NOT NULL on its operands would change results;
        // it is intentionally excluded (matches StarRocks `isEqual()`).
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => match (
            classify_operand(left, left_cols, right_cols),
            classify_operand(right, left_cols, right_cols),
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

#[cfg(test)]
mod column_id_helper_tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

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

    fn three_col_scan(ids: [ColumnId; 3]) -> LogicalPlan {
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
        LogicalPlan::Scan(ScanNode {
            database: "default".to_string(),
            table,
            alias: None,
            columns: vec![
                make_output_column(ids[0], "a"),
                make_output_column(ids[1], "b"),
                make_output_column(ids[2], "c"),
            ],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
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
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(three_col_scan(left_ids)),
            right: Box::new(three_col_scan(right_ids)),
            join_type: crate::sql::analysis::JoinKind::Inner,
            condition: None,
            required_output_columns: None,
        });
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

        let plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![passthrough_item, computed_item],
            required_output_columns: None,
        });

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

        let plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![real_item, unset_item],
            required_output_columns: None,
        });

        let ordered = collect_output_ids_ordered(&plan);
        assert_eq!(ordered, vec![real_id], "UNSET items must be filtered out");
    }

    // ---------------------------------------------------------------------
    // join_equi_keys tests
    // ---------------------------------------------------------------------

    fn nullable_scan(alias: &str, table: &str, cols: &[(&str, u32)]) -> LogicalPlan {
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
        LogicalPlan::Scan(ScanNode {
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
            dict_columns: vec![],
            required_output_columns: None,
        })
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

    fn two_table_join(condition: Option<TypedExpr>) -> JoinNode {
        JoinNode {
            left: Box::new(nullable_scan("l", "tl", &[("a", 1), ("a2", 3)])),
            right: Box::new(nullable_scan("r", "tr", &[("b", 2), ("b2", 4)])),
            join_type: crate::sql::analysis::JoinKind::Inner,
            condition,
            required_output_columns: None,
        }
    }

    #[test]
    fn join_equi_keys_extracts_single_pair_oriented_left_right() {
        let join = two_table_join(Some(eq_expr(qcol("l", "a", 1), qcol("r", "b", 2))));
        let keys = join_equi_keys(&join);
        assert_eq!(keys.len(), 1);
        // left operand belongs to join.left, right operand to join.right.
        assert!(matches!(&keys[0].left.kind, ExprKind::ColumnRef { column, .. } if column == "a"));
        assert!(matches!(&keys[0].right.kind, ExprKind::ColumnRef { column, .. } if column == "b"));
    }

    #[test]
    fn join_equi_keys_orients_reversed_pair() {
        // r.b = l.a  -> still left=a, right=b
        let join = two_table_join(Some(eq_expr(qcol("r", "b", 2), qcol("l", "a", 1))));
        let keys = join_equi_keys(&join);
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
        let keys = join_equi_keys(&join);
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn join_equi_keys_skips_non_equi_and_missing_condition() {
        assert!(join_equi_keys(&two_table_join(None)).is_empty());
        let gt = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(qcol("l", "a", 1)),
                op: crate::sql::analysis::BinOp::Gt,
                right: Box::new(qcol("r", "b", 2)),
            },
            data_type: DataType::Boolean,
            nullable: true,
        };
        assert!(join_equi_keys(&two_table_join(Some(gt))).is_empty());
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
        let keys = join_equi_keys(&join);
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
        assert!(join_equi_keys(&two_table_join(Some(cond))).is_empty());
    }

    #[test]
    fn join_equi_keys_disambiguates_self_join_by_qualifier() {
        // q22 shape: same column name on both sides, distinct aliases.
        let join = JoinNode {
            left: Box::new(nullable_scan("a", "t", &[("k", 1)])),
            right: Box::new(nullable_scan("b", "t", &[("k", 2)])),
            join_type: crate::sql::analysis::JoinKind::LeftSemi,
            condition: Some(eq_expr(qcol("a", "k", 1), qcol("b", "k", 2))),
            required_output_columns: None,
        };
        let keys = join_equi_keys(&join);
        assert_eq!(keys.len(), 1);
        assert!(
            matches!(&keys[0].left.kind, ExprKind::ColumnRef { qualifier: Some(q), .. } if q == "a")
        );
        assert!(
            matches!(&keys[0].right.kind, ExprKind::ColumnRef { qualifier: Some(q), .. } if q == "b")
        );
    }
}
