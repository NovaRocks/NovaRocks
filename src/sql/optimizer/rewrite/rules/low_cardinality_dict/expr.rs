//! Expression helpers used by the low-cardinality dictionary rewrite.

use arrow::datatypes::DataType;

use crate::engine::dictionary::model::DictionarySnapshot;
use crate::sql::analysis::{ExprKind, TypedExpr};

use super::context::DictScope;

/// Allowlist of deterministic single-string-argument functions that can be
/// represented as a query-local derived dictionary on top of a dict-encoded
/// source column. Used by Task 8 item 4 (derived dictionary expressions).
///
/// TODO(post-Task-9): Derived dict expressions (upper/lower/trim/ltrim/rtrim)
/// require emitting `TExprNodeType::DICT_EXPR` into `FragmentBuildResult`,
/// extending `query_global_dict_exprs`, and threading new state from the
/// rewriter into codegen. Not exercised by the Task 9 SQL goldens — defer
/// until a query case actually demands it. See item 4 in the plan
/// (`docs/design/plans/2026-05-26-low-cardinality-dictionary-rewrite.md`).
#[allow(dead_code)]
pub(crate) const DERIVED_DICT_FUNCTIONS: &[&str] = &["upper", "lower", "trim", "ltrim", "rtrim"];

/// Allowlist of aggregate functions whose argument may consume a dict-id
/// slot directly (without a preceding Decode).
///
/// Only aggregates whose *result type* is independent of the dict-encoded
/// string argument are safe here:
///
/// * `count` — returns BIGINT regardless of input encoding.
/// * `approx_count_distinct` — returns BIGINT regardless of input encoding.
///
/// `DISTINCT` is safe for both functions because dict ids are 1:1 with
/// source strings (the dictionary is the encoding of the column's distinct
/// values), so distinct-on-id and distinct-on-string produce the same
/// cardinality.
///
/// TODO(task-8-min-max): `min` / `max` / `any_value` / `array_agg` look
/// tempting (their argument is a single string column), but rewriting them
/// to take a dict-id argument makes the aggregate emit `Int32` dict ids
/// while the declared `OutputColumn` is still the source string type
/// (Utf8 / LargeUtf8). Downstream consumers would silently see typed-as-
/// Utf8 values that are actually dict ids — a wrong-result bug. A correct
/// rewrite needs to also decode the aggregate's result column (similar to
/// the group-by Decode boundary in `rewrite_aggregate`), which is a non-
/// trivial refactor of the aggregate path. Deferred until that result-
/// column decode is built out.
pub(crate) const DICT_AGG_FUNCTIONS: &[&str] = &["count", "approx_count_distinct"];

/// True when two dictionary snapshots are compatible enough that a Join /
/// UNION ALL can safely compare and union their dict id columns directly,
/// without decoding either side first.
///
/// The check covers three fields:
///
/// * `owner.stable_key()` — same logical table.
/// * `version` — same on-disk encoding of the dictionary.
/// * `column_name` (case-insensitive) — same logical column.
///
/// `order_preserving` is intentionally NOT part of the key: equi-join /
/// UNION ALL semantics only need the encoding to agree, not the ordering
/// relation.
///
/// Two compatible snapshots must also encode NULL to the same id. Today
/// ANALYZE FULL always sets `null_id = 0`, so this is a global invariant
/// and the helper relies on it (it does not compare `null_id`
/// structurally). If a future code path produces a snapshot with
/// `null_id != 0`, this helper must be updated to compare `null_id` too —
/// otherwise dict-id equality on a NULL row would diverge from string
/// equality. The `debug_assert!` below pins the invariant in debug builds.
pub(crate) fn dict_keys_compatible(left: &DictionarySnapshot, right: &DictionarySnapshot) -> bool {
    debug_assert_eq!(
        left.null_id, right.null_id,
        "dict_keys_compatible relies on a shared null_id; ANALYZE FULL must use null_id = 0"
    );
    left.owner.stable_key() == right.owner.stable_key()
        && left.version == right.version
        && left.column_name.eq_ignore_ascii_case(&right.column_name)
}

pub(crate) fn is_string_like(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

/// If `expr` is a `ColumnRef`, return the bare column name (no
/// qualifier).
///
/// `TODO(task-8)`: used once Task 8 starts rewriting non-trivial
/// expressions (function calls over dict columns); kept here so the
/// helper lives next to its siblings.
#[allow(dead_code)]
pub(crate) fn column_ref_name(expr: &TypedExpr) -> Option<&str> {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => Some(column.as_str()),
        _ => None,
    }
}

/// Rewrite a top-level column reference to point at the dict column,
/// when `scope` exposes a binding for that column. The synthesized
/// node carries `DataType::Int32` and preserves the source nullability.
/// Non-`ColumnRef` expressions and unknown columns are returned
/// unchanged.
pub(crate) fn rewrite_column_ref_with_scope(expr: &TypedExpr, scope: &DictScope) -> TypedExpr {
    if let ExprKind::ColumnRef {
        column, qualifier, ..
    } = &expr.kind
        && let Some(binding) = scope.get(column)
    {
        return TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: binding.source_column_id,
                qualifier: qualifier.clone(),
                column: binding.dict_column.clone(),
            },
            data_type: DataType::Int32,
            nullable: expr.nullable,
        };
    }
    expr.clone()
}

/// True when `expr` references (anywhere in its tree) a column that has
/// a dict mapping in `scope`. Used by the rewriter to decide whether a
/// Project item must keep its string source available (i.e. insert a
/// Decode boundary) or can be rewritten to the dict column.
///
/// `TODO(task-8)`: consumed by Task 8 when the rewriter inspects
/// project items and join predicates for derived dictionary usage.
#[allow(dead_code)]
pub(crate) fn expr_references_string_column(expr: &TypedExpr, scope: &DictScope) -> bool {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => scope.get(column).is_some(),
        ExprKind::Literal(_) | ExprKind::LambdaParamRef { .. } => false,
        ExprKind::BinaryOp { left, right, .. } => {
            expr_references_string_column(left, scope)
                || expr_references_string_column(right, scope)
        }
        ExprKind::UnaryOp { expr, .. } => expr_references_string_column(expr, scope),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            args.iter().any(|a| expr_references_string_column(a, scope))
        }
        ExprKind::LambdaFunction { body, .. } => expr_references_string_column(body, scope),
        ExprKind::Cast { expr, .. } | ExprKind::IsNull { expr, .. } => {
            expr_references_string_column(expr, scope)
        }
        ExprKind::InList { expr, list, .. } => {
            expr_references_string_column(expr, scope)
                || list.iter().any(|e| expr_references_string_column(e, scope))
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            expr_references_string_column(expr, scope)
                || expr_references_string_column(low, scope)
                || expr_references_string_column(high, scope)
        }
        ExprKind::Like { expr, pattern, .. } => {
            expr_references_string_column(expr, scope)
                || expr_references_string_column(pattern, scope)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_deref()
                .is_some_and(|e| expr_references_string_column(e, scope))
                || when_then.iter().any(|(w, t)| {
                    expr_references_string_column(w, scope)
                        || expr_references_string_column(t, scope)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(|e| expr_references_string_column(e, scope))
        }
        ExprKind::IsTruthValue { expr, .. } | ExprKind::Nested(expr) => {
            expr_references_string_column(expr, scope)
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(|e| expr_references_string_column(e, scope))
                || partition_by
                    .iter()
                    .any(|e| expr_references_string_column(e, scope))
                || order_by
                    .iter()
                    .any(|s| expr_references_string_column(&s.expr, scope))
        }
        ExprKind::SubqueryPlaceholder { .. } => false,
        ExprKind::Lambda { body, .. } => expr_references_string_column(body, scope),
    }
}

/// Collect (lowercased) names of every column referenced anywhere in
/// `expr`. Exhaustive over all `ExprKind` variants (mirrors the recursion
/// of `expr_references_string_column`). Used by the collector's
/// blocklist walk to discover which columns a node consumes in an unsafe
/// position (i.e. one the rewriter does not retarget to the dict slot).
pub(crate) fn collect_all_columns(expr: &TypedExpr, out: &mut std::collections::BTreeSet<String>) {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => {
            out.insert(column.to_ascii_lowercase());
        }
        ExprKind::Literal(_)
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::SubqueryPlaceholder { .. } => {}
        ExprKind::BinaryOp { left, right, .. } => {
            collect_all_columns(left, out);
            collect_all_columns(right, out);
        }
        ExprKind::UnaryOp { expr, .. } => collect_all_columns(expr, out),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            for a in args {
                collect_all_columns(a, out);
            }
        }
        ExprKind::LambdaFunction { body, .. } => collect_all_columns(body, out),
        ExprKind::Cast { expr, .. } | ExprKind::IsNull { expr, .. } => {
            collect_all_columns(expr, out)
        }
        ExprKind::InList { expr, list, .. } => {
            collect_all_columns(expr, out);
            for e in list {
                collect_all_columns(e, out);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_all_columns(expr, out);
            collect_all_columns(low, out);
            collect_all_columns(high, out);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_all_columns(expr, out);
            collect_all_columns(pattern, out);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(e) = operand.as_deref() {
                collect_all_columns(e, out);
            }
            for (w, t) in when_then {
                collect_all_columns(w, out);
                collect_all_columns(t, out);
            }
            if let Some(e) = else_expr.as_deref() {
                collect_all_columns(e, out);
            }
        }
        ExprKind::IsTruthValue { expr, .. } | ExprKind::Nested(expr) => {
            collect_all_columns(expr, out)
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for e in args {
                collect_all_columns(e, out);
            }
            for e in partition_by {
                collect_all_columns(e, out);
            }
            for s in order_by {
                collect_all_columns(&s.expr, out);
            }
        }
        ExprKind::Lambda { body, .. } => collect_all_columns(body, out),
    }
}

/// Collect column names referenced by `expr` ONLY when they appear nested
/// inside a compound expression. A bare top-level `ColumnRef` contributes
/// nothing (it merely propagates the dict slot, a safe position), but
/// `f(col)` / `col + 1` / `col = x` contribute their columns (the column
/// is consumed by an operator the rewriter cannot run on dict ids).
pub(crate) fn collect_nested_columns(
    expr: &TypedExpr,
    out: &mut std::collections::BTreeSet<String>,
) {
    if matches!(expr.kind, ExprKind::ColumnRef { .. }) {
        return;
    }
    collect_all_columns(expr, out);
}
