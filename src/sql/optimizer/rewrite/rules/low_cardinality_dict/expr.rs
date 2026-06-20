//! Expression helpers used by the low-cardinality dictionary rewrite.

use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;
use crate::sql::common::DictionarySnapshot;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

use super::context::{DictBinding, DictScope};

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

pub(crate) fn resolve_column_ref<'a>(
    arena: &ScalarArena,
    expr: ScalarId,
    scope: &'a DictScope,
) -> Option<(&'a str, &'a DictBinding)> {
    let ScalarNode::ColumnRef(column_id) = arena.node(expr) else {
        return None;
    };
    scope
        .resolve_column_id(*column_id)
        .or_else(|| scope.resolve_either(&column_name(arena, *column_id)))
}

pub(crate) fn rewrite_column_ref_with_scope(
    arena: &mut ScalarArena,
    expr: ScalarId,
    scope: &DictScope,
) -> ScalarId {
    let Some((_, binding)) = resolve_column_ref(arena, expr, scope) else {
        return expr;
    };
    arena.remember_project_output_display(
        binding.source_column_id,
        None,
        binding.dict_column.clone(),
    );
    arena.intern(
        ScalarNode::ColumnRef(binding.source_column_id),
        DataType::Int32,
        arena.nullable(expr),
    )
}

pub(crate) fn collect_all_columns(
    arena: &ScalarArena,
    expr: ScalarId,
    out: &mut std::collections::BTreeSet<String>,
) {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => {
            out.insert(column_name(arena, *column_id).to_ascii_lowercase());
        }
        ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            collect_all_columns(arena, *left, out);
            collect_all_columns(arena, *right, out);
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::LambdaFunction { body: child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child)
        | ScalarNode::Lambda { body: child, .. } => {
            collect_all_columns(arena, *child, out);
        }
        ScalarNode::FunctionCall { args, .. } | ScalarNode::AggregateCall { args, .. } => {
            for arg in args {
                collect_all_columns(arena, *arg, out);
            }
        }
        ScalarNode::InList { child, list, .. } => {
            collect_all_columns(arena, *child, out);
            for item in list {
                collect_all_columns(arena, *item, out);
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            collect_all_columns(arena, *child, out);
            collect_all_columns(arena, *low, out);
            collect_all_columns(arena, *high, out);
        }
        ScalarNode::Like { child, pattern, .. } => {
            collect_all_columns(arena, *child, out);
            collect_all_columns(arena, *pattern, out);
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_all_columns(arena, *operand, out);
            }
            for (when, then) in when_then {
                collect_all_columns(arena, *when, out);
                collect_all_columns(arena, *then, out);
            }
            if let Some(else_expr) = else_expr {
                collect_all_columns(arena, *else_expr, out);
            }
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_all_columns(arena, *arg, out);
            }
            for expr in partition_by {
                collect_all_columns(arena, *expr, out);
            }
            for item in order_by {
                collect_all_columns(arena, item.expr, out);
            }
        }
    }
}

pub(crate) fn collect_nested_columns(
    arena: &ScalarArena,
    expr: ScalarId,
    out: &mut std::collections::BTreeSet<String>,
) {
    if matches!(arena.node(expr), ScalarNode::ColumnRef(_)) {
        return;
    }
    collect_all_columns(arena, expr, out);
}

fn column_name(arena: &ScalarArena, column_id: ColumnId) -> String {
    arena
        .column_display(column_id)
        .map(|display| display.column.clone())
        .unwrap_or_else(|| format!("col{}", column_id.0))
}
