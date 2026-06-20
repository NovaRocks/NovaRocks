#![allow(dead_code)] // Shared by rule migrations as TypedExpr callers move to ScalarId.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, LiteralValue, UnOp};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey};

pub(crate) fn column_id(arena: &ScalarArena, expr: ScalarId) -> Option<ColumnId> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(id) if *id != ColumnId::UNSET => Some(*id),
        _ => None,
    }
}

pub(crate) fn collect_column_ids_strict(
    arena: &ScalarArena,
    expr: ScalarId,
) -> Option<HashSet<ColumnId>> {
    let mut out = HashSet::new();
    collect_column_ids_strict_inner(arena, expr, &mut out)?;
    Some(out)
}

fn collect_column_ids_strict_inner(
    arena: &ScalarArena,
    expr: ScalarId,
    out: &mut HashSet<ColumnId>,
) -> Option<()> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(id) => {
            if *id == ColumnId::UNSET {
                return None;
            }
            out.insert(*id);
        }
        ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            collect_column_ids_strict_inner(arena, *left, out)?;
            collect_column_ids_strict_inner(arena, *right, out)?;
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => collect_column_ids_strict_inner(arena, *child, out)?,
        ScalarNode::FunctionCall { args, .. } => {
            for arg in args {
                collect_column_ids_strict_inner(arena, *arg, out)?;
            }
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            collect_column_ids_strict_inner(arena, *body, out)?;
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            for arg in args {
                collect_column_ids_strict_inner(arena, *arg, out)?;
            }
            for item in order_by {
                collect_column_ids_strict_inner(arena, item.expr, out)?;
            }
        }
        ScalarNode::InList { child, list, .. } => {
            collect_column_ids_strict_inner(arena, *child, out)?;
            for item in list {
                collect_column_ids_strict_inner(arena, *item, out)?;
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            collect_column_ids_strict_inner(arena, *child, out)?;
            collect_column_ids_strict_inner(arena, *low, out)?;
            collect_column_ids_strict_inner(arena, *high, out)?;
        }
        ScalarNode::Like { child, pattern, .. } => {
            collect_column_ids_strict_inner(arena, *child, out)?;
            collect_column_ids_strict_inner(arena, *pattern, out)?;
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_column_ids_strict_inner(arena, *operand, out)?;
            }
            for (when, then) in when_then {
                collect_column_ids_strict_inner(arena, *when, out)?;
                collect_column_ids_strict_inner(arena, *then, out)?;
            }
            if let Some(else_expr) = else_expr {
                collect_column_ids_strict_inner(arena, *else_expr, out)?;
            }
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_column_ids_strict_inner(arena, *arg, out)?;
            }
            for expr in partition_by {
                collect_column_ids_strict_inner(arena, *expr, out)?;
            }
            for item in order_by {
                collect_column_ids_strict_inner(arena, item.expr, out)?;
            }
        }
    }
    Some(())
}

pub(crate) fn split_conjuncts(arena: &ScalarArena, expr: ScalarId, out: &mut Vec<ScalarId>) {
    match arena.node(expr) {
        ScalarNode::Nested(inner) => split_conjuncts(arena, *inner, out),
        ScalarNode::BinaryOp {
            op: BinOp::And,
            left,
            right,
        } => {
            split_conjuncts(arena, *left, out);
            split_conjuncts(arena, *right, out);
        }
        _ => out.push(expr),
    }
}

pub(crate) fn combine_conjuncts(
    arena: &mut ScalarArena,
    mut exprs: Vec<ScalarId>,
) -> Option<ScalarId> {
    combine_binary_bool(arena, &mut exprs, BinOp::And)
}

pub(crate) fn split_disjuncts(arena: &ScalarArena, expr: ScalarId, out: &mut Vec<ScalarId>) {
    match arena.node(expr) {
        ScalarNode::Nested(inner) => split_disjuncts(arena, *inner, out),
        ScalarNode::BinaryOp {
            op: BinOp::Or,
            left,
            right,
        } => {
            split_disjuncts(arena, *left, out);
            split_disjuncts(arena, *right, out);
        }
        _ => out.push(expr),
    }
}

pub(crate) fn combine_disjuncts(
    arena: &mut ScalarArena,
    mut exprs: Vec<ScalarId>,
) -> Option<ScalarId> {
    combine_binary_bool(arena, &mut exprs, BinOp::Or)
}

fn combine_binary_bool(
    arena: &mut ScalarArena,
    exprs: &mut Vec<ScalarId>,
    op: BinOp,
) -> Option<ScalarId> {
    let mut result = exprs.pop()?;
    while let Some(next) = exprs.pop() {
        let nullable = arena.nullable(next) || arena.nullable(result);
        result = arena.intern(
            ScalarNode::BinaryOp {
                op,
                left: next,
                right: result,
            },
            DataType::Boolean,
            nullable,
        );
    }
    Some(result)
}

pub(crate) fn bool_literal(arena: &mut ScalarArena, value: bool) -> ScalarId {
    arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(value))),
        DataType::Boolean,
        false,
    )
}

pub(crate) fn int_literal(arena: &mut ScalarArena, value: i64) -> ScalarId {
    arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
        DataType::Int64,
        false,
    )
}

pub(crate) fn is_literal_count_arg(arena: &ScalarArena, expr: ScalarId) -> bool {
    matches!(
        arena.node(expr),
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(_)))
            | ScalarNode::Literal(HashableLiteral(LiteralValue::Null))
    )
}

pub(crate) fn contains_aggregate(arena: &ScalarArena, expr: ScalarId) -> bool {
    match arena.node(expr) {
        ScalarNode::AggregateCall { .. } => true,
        ScalarNode::BinaryOp { left, right, .. } => {
            contains_aggregate(arena, *left) || contains_aggregate(arena, *right)
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => contains_aggregate(arena, *child),
        ScalarNode::FunctionCall { args, .. } => {
            args.iter().any(|arg| contains_aggregate(arena, *arg))
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            contains_aggregate(arena, *body)
        }
        ScalarNode::InList { child, list, .. } => {
            contains_aggregate(arena, *child)
                || list.iter().any(|item| contains_aggregate(arena, *item))
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            contains_aggregate(arena, *child)
                || contains_aggregate(arena, *low)
                || contains_aggregate(arena, *high)
        }
        ScalarNode::Like { child, pattern, .. } => {
            contains_aggregate(arena, *child) || contains_aggregate(arena, *pattern)
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.is_some_and(|expr| contains_aggregate(arena, expr))
                || when_then.iter().any(|(when, then)| {
                    contains_aggregate(arena, *when) || contains_aggregate(arena, *then)
                })
                || else_expr.is_some_and(|expr| contains_aggregate(arena, expr))
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(|arg| contains_aggregate(arena, *arg))
                || partition_by
                    .iter()
                    .any(|expr| contains_aggregate(arena, *expr))
                || order_by
                    .iter()
                    .any(|item| contains_aggregate(arena, item.expr))
        }
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            false
        }
    }
}

pub(crate) fn sort_key_column_id(arena: &ScalarArena, key: &SortKey) -> Option<ColumnId> {
    column_id(arena, key.expr)
}

pub(crate) fn scalar_display_name(arena: &ScalarArena, expr: ScalarId) -> String {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => {
            if let Some(display) = arena.column_display(*column_id) {
                return display
                    .qualifier
                    .as_ref()
                    .map(|qualifier| format!("{qualifier}.{}", display.column))
                    .unwrap_or_else(|| display.column.clone());
            }
            format!("col{}", column_id.0)
        }
        ScalarNode::LambdaParamRef { name, .. } => name.clone(),
        ScalarNode::Literal(HashableLiteral(value)) => literal_display_name(value),
        ScalarNode::FunctionCall { name, args, .. } if name == "__array_literal" => {
            format!(
                "[{}]",
                args.iter()
                    .map(|arg| scalar_display_name(arena, *arg))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        ScalarNode::FunctionCall { name, args, .. } if name == "map" => {
            let mut parts = Vec::new();
            let mut iter = args.iter();
            while let Some(key) = iter.next() {
                let key_display = scalar_display_name(arena, *key);
                if let Some(value) = iter.next() {
                    parts.push(format!(
                        "{key_display}:{}",
                        scalar_display_name(arena, *value)
                    ));
                } else {
                    parts.push(key_display);
                }
            }
            format!("map{{{}}}", parts.join(","))
        }
        ScalarNode::FunctionCall { name, args, .. } => {
            format!(
                "{}({})",
                name.to_ascii_lowercase(),
                args.iter()
                    .map(|arg| scalar_display_name(arena, *arg))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        ScalarNode::LambdaFunction { params, body } => {
            let params = params
                .iter()
                .map(|param| param.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("({params}) -> {}", scalar_display_name(arena, *body))
        }
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => aggregate_display_name(arena, name, args, *distinct, order_by),
        ScalarNode::Cast { child, target } => {
            format!(
                "cast({} as {:?})",
                scalar_display_name(arena, *child),
                target
            )
        }
        ScalarNode::IsNull { child, negated } => {
            let child = scalar_display_name_with_parens(arena, *child);
            if *negated {
                format!("{child} IS NOT NULL")
            } else {
                format!("{child} IS NULL")
            }
        }
        ScalarNode::BinaryOp { left, op, right } => {
            format!(
                "{} {} {}",
                scalar_display_name_with_parens(arena, *left),
                bin_op_display(*op),
                scalar_display_name_with_parens(arena, *right)
            )
        }
        ScalarNode::UnaryOp { op, child } => match op {
            UnOp::Not => format!("NOT {}", scalar_display_name_with_parens(arena, *child)),
            UnOp::Negate => format!("-{}", scalar_display_name_with_parens(arena, *child)),
            UnOp::BitwiseNot => format!("~{}", scalar_display_name_with_parens(arena, *child)),
        },
        ScalarNode::Nested(child) => scalar_display_name(arena, *child),
        other => format!("{:?}", other),
    }
}

pub(crate) fn aggregate_display_name(
    arena: &ScalarArena,
    name: &str,
    args: &[ScalarId],
    distinct: bool,
    order_by: &[SortKey],
) -> String {
    let distinct = distinct || matches!(name, "array_agg_distinct");
    let display_name = canonical_agg_display_name(name);
    let args_display = if args.is_empty() {
        "*".to_string()
    } else {
        args.iter()
            .map(|arg| scalar_display_name(arena, *arg))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let mut out = if distinct {
        format!("{display_name}(DISTINCT {args_display}")
    } else {
        format!("{display_name}({args_display}")
    };

    let visible_order_by = order_by
        .iter()
        .filter(|item| !matches!(arena.node(item.expr), ScalarNode::Literal(_)))
        .collect::<Vec<_>>();
    if !visible_order_by.is_empty() {
        let order_by_display = visible_order_by
            .iter()
            .map(|item| sort_key_display_name(arena, item))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(" order by ");
        out.push_str(&order_by_display);
    }

    out.push(')');
    out
}

fn sort_key_display_name(arena: &ScalarArena, key: &SortKey) -> String {
    let mut out = scalar_display_name(arena, key.expr);
    out.push_str(if key.asc { " asc" } else { " desc" });
    if key.nulls_first != key.asc {
        out.push_str(if key.nulls_first {
            " nulls first"
        } else {
            " nulls last"
        });
    }
    out
}

fn literal_display_name(value: &LiteralValue) -> String {
    match value {
        LiteralValue::Null => "NULL".to_string(),
        LiteralValue::Bool(true) => "TRUE".to_string(),
        LiteralValue::Bool(false) => "FALSE".to_string(),
        LiteralValue::Int(value) => value.to_string(),
        LiteralValue::LargeInt(value) => value.to_string(),
        LiteralValue::Float(value) => value.to_string(),
        LiteralValue::Decimal(value) => value.clone(),
        LiteralValue::String(value) => format!("'{value}'"),
        LiteralValue::Binary(value) => format!("X'{}'", hex::encode_upper(value)),
    }
}

fn scalar_display_name_with_parens(arena: &ScalarArena, expr: ScalarId) -> String {
    match arena.node(expr) {
        ScalarNode::ColumnRef(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::Literal(_)
        | ScalarNode::FunctionCall { .. }
        | ScalarNode::AggregateCall { .. } => scalar_display_name(arena, expr),
        _ => format!("({})", scalar_display_name(arena, expr)),
    }
}

fn bin_op_display(op: BinOp) -> &'static str {
    match op {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Eq => "=",
        BinOp::Ne => "!=",
        BinOp::Lt => "<",
        BinOp::Le => "<=",
        BinOp::Gt => ">",
        BinOp::Ge => ">=",
        BinOp::EqForNull => "<=>",
        BinOp::And => "AND",
        BinOp::Or => "OR",
    }
}

fn canonical_agg_display_name(name: &str) -> &str {
    match name {
        "string_agg" => "group_concat",
        "array_agg_distinct" => "array_agg",
        "variance_samp" => "var_samp",
        "variance_pop" => "var_pop",
        other => other,
    }
}

pub(crate) fn is_true_literal(arena: &ScalarArena, expr: ScalarId) -> bool {
    matches!(
        arena.node(expr),
        ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(true)))
    )
}

pub(crate) fn contains_non_deterministic_function(arena: &ScalarArena, expr: ScalarId) -> bool {
    match arena.node(expr) {
        ScalarNode::FunctionCall { name, args, .. } => {
            let lower = name.to_lowercase();
            matches!(
                lower.as_str(),
                "rand"
                    | "random"
                    | "uuid"
                    | "now"
                    | "current_timestamp"
                    | "current_date"
                    | "curdate"
                    | "current_time"
                    | "curtime"
                    | "localtime"
                    | "localtimestamp"
                    | "utc_timestamp"
                    | "utc_time"
            ) || args
                .iter()
                .any(|arg| contains_non_deterministic_function(arena, *arg))
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            args.iter()
                .any(|arg| contains_non_deterministic_function(arena, *arg))
                || order_by
                    .iter()
                    .any(|item| contains_non_deterministic_function(arena, item.expr))
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter()
                .any(|arg| contains_non_deterministic_function(arena, *arg))
                || partition_by
                    .iter()
                    .any(|expr| contains_non_deterministic_function(arena, *expr))
                || order_by
                    .iter()
                    .any(|item| contains_non_deterministic_function(arena, item.expr))
        }
        ScalarNode::BinaryOp { left, right, .. } => {
            contains_non_deterministic_function(arena, *left)
                || contains_non_deterministic_function(arena, *right)
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => contains_non_deterministic_function(arena, *child),
        ScalarNode::InList { child, list, .. } => {
            contains_non_deterministic_function(arena, *child)
                || list
                    .iter()
                    .any(|item| contains_non_deterministic_function(arena, *item))
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            contains_non_deterministic_function(arena, *child)
                || contains_non_deterministic_function(arena, *low)
                || contains_non_deterministic_function(arena, *high)
        }
        ScalarNode::Like { child, pattern, .. } => {
            contains_non_deterministic_function(arena, *child)
                || contains_non_deterministic_function(arena, *pattern)
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.is_some_and(|expr| contains_non_deterministic_function(arena, expr))
                || when_then.iter().any(|(when, then)| {
                    contains_non_deterministic_function(arena, *when)
                        || contains_non_deterministic_function(arena, *then)
                })
                || else_expr.is_some_and(|expr| contains_non_deterministic_function(arena, expr))
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            contains_non_deterministic_function(arena, *body)
        }
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::sql::analysis::BinOp;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

    use super::*;

    fn col(arena: &mut ScalarArena, id: u32, nullable: bool) -> ScalarId {
        arena.intern(
            ScalarNode::ColumnRef(ColumnId(id)),
            DataType::Int64,
            nullable,
        )
    }

    #[test]
    fn strict_column_collection_rejects_unset_column_ref() {
        let mut arena = ScalarArena::new();
        let expr = arena.intern(
            ScalarNode::ColumnRef(ColumnId::UNSET),
            DataType::Int64,
            true,
        );

        assert_eq!(collect_column_ids_strict(&arena, expr), None);
    }

    #[test]
    fn split_and_combine_conjuncts_round_trip_column_refs() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1, true);
        let b = col(&mut arena, 2, false);
        let and = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left: a,
                right: b,
            },
            DataType::Boolean,
            false,
        );

        let mut parts = Vec::new();
        split_conjuncts(&arena, and, &mut parts);
        assert_eq!(parts, vec![a, b]);

        let rebuilt = combine_conjuncts(&mut arena, parts).unwrap();
        assert!(matches!(
            arena.node(rebuilt),
            ScalarNode::BinaryOp { op: BinOp::And, .. }
        ));
        assert_eq!(arena.data_type(rebuilt), &DataType::Boolean);
        assert!(arena.nullable(rebuilt));
    }
}
