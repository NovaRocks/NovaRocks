#![allow(dead_code)] // Shared by rule migrations as TypedExpr callers move to ScalarId.
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

use arrow::datatypes::DataType;

use crate::column_id::ColumnId;
use crate::common::{BinOp, LiteralValue, UnOp};
use crate::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey};

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
            function_order_by,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_column_ids_strict_inner(arena, *arg, out)?;
            }
            for item in function_order_by {
                collect_column_ids_strict_inner(arena, item.expr, out)?;
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
            ..
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
            ..
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
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
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
            ..
        } => aggregate_display_name(arena, name, args, *distinct, order_by),
        ScalarNode::Cast { child, target, .. } => {
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
        ScalarNode::BinaryOp {
            left, op, right, ..
        } => {
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
    contains_function_matching(arena, expr, |volatility| volatility.is_volatile())
}

pub(crate) fn contains_non_replica_deterministic_function(
    arena: &ScalarArena,
    expr: ScalarId,
) -> bool {
    contains_function_matching(arena, expr, |volatility| {
        !volatility.is_replica_deterministic()
    })
}

fn contains_function_matching(
    arena: &ScalarArena,
    expr: ScalarId,
    matches: fn(novarocks_functions::FunctionVolatility) -> bool,
) -> bool {
    match arena.node(expr) {
        ScalarNode::FunctionCall { args, .. } => {
            arena.function_volatility(expr).is_some_and(matches)
                || args
                    .iter()
                    .any(|arg| contains_function_matching(arena, *arg, matches))
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            args.iter()
                .any(|arg| contains_function_matching(arena, *arg, matches))
                || order_by
                    .iter()
                    .any(|item| contains_function_matching(arena, item.expr, matches))
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter()
                .any(|arg| contains_function_matching(arena, *arg, matches))
                || partition_by
                    .iter()
                    .any(|expr| contains_function_matching(arena, *expr, matches))
                || order_by
                    .iter()
                    .any(|item| contains_function_matching(arena, item.expr, matches))
        }
        ScalarNode::BinaryOp { left, right, .. } => {
            contains_function_matching(arena, *left, matches)
                || contains_function_matching(arena, *right, matches)
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => contains_function_matching(arena, *child, matches),
        ScalarNode::InList { child, list, .. } => {
            contains_function_matching(arena, *child, matches)
                || list
                    .iter()
                    .any(|item| contains_function_matching(arena, *item, matches))
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            contains_function_matching(arena, *child, matches)
                || contains_function_matching(arena, *low, matches)
                || contains_function_matching(arena, *high, matches)
        }
        ScalarNode::Like { child, pattern, .. } => {
            contains_function_matching(arena, *child, matches)
                || contains_function_matching(arena, *pattern, matches)
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.is_some_and(|expr| contains_function_matching(arena, expr, matches))
                || when_then.iter().any(|(when, then)| {
                    contains_function_matching(arena, *when, matches)
                        || contains_function_matching(arena, *then, matches)
                })
                || else_expr.is_some_and(|expr| contains_function_matching(arena, expr, matches))
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            contains_function_matching(arena, *body, matches)
        }
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            false
        }
    }
}

// Source-backed own Cast effects; unsupported frozen shapes and allocation
// failures are not row effects. Recursive casts use the shared scalar caster,
// while root Cast adds Date calendar conversion and the legacy SQL mode check.
fn cast_has_intrinsic_row_error(source: &DataType, target: &DataType, root: bool) -> bool {
    use arrow::datatypes::TimeUnit;
    if source == target || source == &DataType::Null || target == &DataType::Null {
        return false;
    }
    if root
        && source == &DataType::Date32
        && matches!(target, DataType::Float32 | DataType::Float64)
    {
        return true;
    }
    // ALLOW_THROW_EXCEPTION also governs Decimal-to-Decimal overflow. A
    // same-scale non-narrowing precision cast is total on the declared source
    // domain (including a carrier change). Other rescaling/narrowing profiles
    // remain a conservative effect upper bound, not a claim that every such
    // profile overflows. The statement flag is not a per-node frozen fact.
    let decimal_parts = |dtype: &DataType| match dtype {
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Some((*p, *s)),
        _ => None,
    };
    if root {
        if let (Some((sp, ss)), Some((tp, ts))) = (decimal_parts(source), decimal_parts(target)) {
            return ss != ts || tp < sp;
        }
    }
    // ALLOW_THROW_EXCEPTION is an existing statement setting rather than a
    // per-node Decimal policy. Conservatively preserve its lawful error route.
    if root
        && matches!(source, DataType::Float32 | DataType::Float64)
        && matches!(
            target,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    {
        return true;
    }
    if matches!(
        (source, target),
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Timestamp(TimeUnit::Nanosecond, _)
        )
    ) {
        return true;
    }
    if matches!(target, DataType::Utf8 | DataType::LargeUtf8) {
        // The exact top-level Timestamp -> Utf8 route has a custom formatter;
        // Arrow nested/LargeUtf8 formatting remains fallible on temporal values.
        if matches!((source, target), (DataType::Timestamp(..), DataType::Utf8)) {
            return false;
        }
        return arrow_format_has_row_error(source);
    }
    match (source, target) {
        (DataType::List(source), DataType::List(target)) => {
            cast_has_intrinsic_row_error(source.data_type(), target.data_type(), false)
        }
        (DataType::Struct(source), DataType::Struct(target)) if source.len() == target.len() => {
            let by_name = target
                .iter()
                .all(|field| source.iter().any(|s| s.name() == field.name()));
            target.iter().enumerate().any(|(index, target)| {
                let source = if by_name {
                    source.iter().find(|s| s.name() == target.name()).unwrap()
                } else {
                    &source[index]
                };
                cast_has_intrinsic_row_error(source.data_type(), target.data_type(), false)
            })
        }
        (DataType::Map(source, _), DataType::Map(target, _)) => {
            let (DataType::Struct(source), DataType::Struct(target)) =
                (source.data_type(), target.data_type())
            else {
                return false;
            };
            // MAP key/value conversion is positional, unlike STRUCT by-name matching.
            source.len() == 2
                && target.len() == 2
                && source.iter().zip(target.iter()).any(|(source, target)| {
                    cast_has_intrinsic_row_error(source.data_type(), target.data_type(), false)
                })
        }
        (DataType::List(source), DataType::Map(target, _)) => {
            cast_has_intrinsic_row_error(source.data_type(), target.data_type(), false)
        }
        _ => false,
    }
}

fn arrow_format_has_row_error(source: &DataType) -> bool {
    use arrow::datatypes::TimeUnit;
    match source {
        DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond,
            _,
        ) => true,
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            arrow_format_has_row_error(item.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|f| arrow_format_has_row_error(f.data_type())),
        DataType::Map(entries, _) => arrow_format_has_row_error(entries.data_type()),
        _ => false,
    }
}

/// Whether evaluating the scalar can expose an intrinsic row error or a checked
/// Decimal numeric overflow, including errors from any argument expression.
/// This effect is independent from function volatility.
pub(crate) fn can_fail(arena: &ScalarArena, expr: ScalarId) -> bool {
    use novarocks_type_contract::DecimalOverflowPolicy;
    let decimal = |data_type: &DataType| {
        matches!(
            data_type,
            DataType::Decimal128(..) | DataType::Decimal256(..)
        )
    };
    let own_error = match arena.node(expr) {
        ScalarNode::FunctionCall { binding, .. } => {
            assert_eq!(
                binding.kind,
                novarocks_functions::FunctionKind::Scalar,
                "a non-row binding cannot be consumed as a scalar function"
            );
            match binding.semantics.intrinsic_row_error {
                novarocks_type_contract::FunctionIntrinsicRowError::NoRowError => false,
                novarocks_type_contract::FunctionIntrinsicRowError::MayRaise => true,
                novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated => {
                    unreachable!("a scalar function cannot carry a non-row intrinsic fact")
                }
            }
        }
        ScalarNode::BinaryOp {
            op,
            decimal_overflow_policy,
            ..
        } => {
            // Legacy ALLOW_THROW_EXCEPTION can also raise for Decimal multiply.
            // It is not a per-node frozen fact, so OutputNull cannot prove this
            // operation error-free. This is MayRaise, not an unconditional error.
            decimal(arena.data_type(expr))
                && (*op == BinOp::Mul
                    || (*decimal_overflow_policy == DecimalOverflowPolicy::ReportError
                        && matches!(op, BinOp::Add | BinOp::Sub | BinOp::Div | BinOp::Mod)))
        }
        ScalarNode::Cast {
            child,
            decimal_overflow_policy,
            ..
        } => {
            cast_has_intrinsic_row_error(arena.data_type(*child), arena.data_type(expr), true)
                || (*decimal_overflow_policy == DecimalOverflowPolicy::ReportError
                    && novarocks_type_contract::is_checked_decimal_numeric_cast(
                        arena.data_type(*child),
                        arena.data_type(expr),
                    ))
        }
        _ => false,
    };
    if own_error {
        return true;
    }
    match arena.node(expr) {
        ScalarNode::FunctionCall { args, .. } => args.iter().any(|arg| can_fail(arena, *arg)),
        ScalarNode::AggregateCall { args, order_by, .. } => {
            args.iter().any(|arg| can_fail(arena, *arg))
                || order_by.iter().any(|item| can_fail(arena, item.expr))
        }
        ScalarNode::WindowCall {
            args,
            function_order_by,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(|arg| can_fail(arena, *arg))
                || function_order_by
                    .iter()
                    .any(|item| can_fail(arena, item.expr))
                || partition_by.iter().any(|expr| can_fail(arena, *expr))
                || order_by.iter().any(|item| can_fail(arena, item.expr))
        }
        ScalarNode::BinaryOp { left, right, .. } => {
            can_fail(arena, *left) || can_fail(arena, *right)
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => can_fail(arena, *child),
        ScalarNode::InList { child, list, .. } => {
            can_fail(arena, *child) || list.iter().any(|item| can_fail(arena, *item))
        }
        ScalarNode::Between {
            child, low, high, ..
        } => can_fail(arena, *child) || can_fail(arena, *low) || can_fail(arena, *high),
        ScalarNode::Like { child, pattern, .. } => {
            can_fail(arena, *child) || can_fail(arena, *pattern)
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.is_some_and(|expr| can_fail(arena, expr))
                || when_then
                    .iter()
                    .any(|(when, then)| can_fail(arena, *when) || can_fail(arena, *then))
                || else_expr.is_some_and(|expr| can_fail(arena, expr))
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            can_fail(arena, *body)
        }
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::analysis::BinOp;
    use crate::column_id::ColumnId;
    use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

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
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
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
    #[test]
    fn checked_error_effect_is_independent_from_function_volatility() {
        use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
        let mut arena = ScalarArena::new();
        let child = arena.intern(
            ScalarNode::ColumnRef(ColumnId(1)),
            DataType::Decimal128(38, 0),
            true,
        );
        let integer = arena.intern(ScalarNode::ColumnRef(ColumnId(2)), DataType::Int64, true);
        let nullable = arena.intern(
            ScalarNode::Cast {
                child: integer,
                target: DataType::Decimal128(9, 0),
                decimal_overflow_policy: OutputNull,
            },
            DataType::Decimal128(9, 0),
            true,
        );
        let throwing = arena.intern(
            ScalarNode::Cast {
                child,
                target: DataType::Decimal128(9, 0),
                decimal_overflow_policy: ReportError,
            },
            DataType::Decimal128(9, 0),
            true,
        );
        assert!(!can_fail(&arena, nullable));
        assert!(can_fail(&arena, throwing));
        assert!(!contains_non_deterministic_function(&arena, throwing));
        let predicate = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Eq,
                left: throwing,
                right: nullable,
                decimal_overflow_policy: OutputNull,
            },
            DataType::Boolean,
            true,
        );
        assert!(can_fail(&arena, predicate));
        assert!(!contains_non_deterministic_function(&arena, predicate));
    }
}

#[cfg(test)]
mod intrinsic_error_consumer_tests {
    use super::*;
    use crate::column_id::ColumnId;
    use crate::optimizer::scalar::{HashableLiteral, SortKey};
    use novarocks_functions::{
        FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionResultType,
        FunctionValueType,
    };

    fn actual_call(arena: &mut ScalarArena, name: &str, args: Vec<ScalarId>) -> ScalarId {
        let arguments = args
            .iter()
            .map(|id| FunctionArgument::Value {
                value_type: FunctionValueType::new(
                    arena.data_type(*id).clone(),
                    arena.nullable(*id),
                ),
                constant: None,
            })
            .collect::<Vec<_>>();
        let catalog = crate::functions::builtin_engine_function_catalog();
        let binding = catalog
            .resolve_bound_user(
                name,
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: args.len(),
                },
            )
            .unwrap();
        catalog
            .validate_bound(
                &binding,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: args.len(),
                },
            )
            .unwrap();
        let FunctionResultType::Scalar(output) = binding.selected.result_type.clone() else {
            panic!("scalar result")
        };
        let volatility = binding.semantics.volatility;
        arena.intern(
            ScalarNode::FunctionCall {
                name: name.into(),
                args,
                distinct: false,
                binding: binding.into(),
                volatility,
            },
            output.data_type,
            output.nullable,
        )
    }

    #[test]
    fn selected_intrinsic_fact_controls_effect_without_display_name_dispatch() {
        let mut arena = ScalarArena::new();
        let text = arena.intern(ScalarNode::ColumnRef(ColumnId(1)), DataType::Utf8, true);
        let boolean = arena.intern(ScalarNode::ColumnRef(ColumnId(2)), DataType::Boolean, true);
        let total = actual_call(&mut arena, "parse_json", vec![text]);
        let raises = actual_call(&mut arena, "assert_true", vec![boolean]);
        assert!(!can_fail(&arena, total));
        assert!(can_fail(&arena, raises));
        assert!(!contains_non_deterministic_function(&arena, raises));
        let mut renamed = arena.node(raises).clone();
        let ScalarNode::FunctionCall { name, .. } = &mut renamed else {
            unreachable!()
        };
        *name = "parse_json".into();
        let renamed = arena.intern(
            renamed,
            arena.data_type(raises).clone(),
            arena.nullable(raises),
        );
        assert!(can_fail(&arena, renamed));
    }

    #[test]
    fn independent_catching_fact_and_short_circuit_children_are_preserved() {
        let mut arena = ScalarArena::new();
        let boolean = arena.intern(ScalarNode::ColumnRef(ColumnId(2)), DataType::Boolean, true);
        let raises = actual_call(&mut arena, "assert_true", vec![boolean]);
        // A structurally valid custom ReturnsNull declaration may still expose its own
        // Result error. Catalog kind/fact validation is covered in binding tests.
        let mut catching = arena.node(raises).clone();
        let ScalarNode::FunctionCall { binding, .. } = &mut catching else {
            unreachable!()
        };
        let mut exact = binding.as_ref().clone();
        exact.semantics.failure_behavior =
            novarocks_functions::FunctionFailureBehavior::ReturnsNull;
        *binding = exact.into();
        let catching = arena.intern(catching, DataType::Boolean, true);
        assert!(can_fail(&arena, catching));
        let condition = arena.intern(
            ScalarNode::Literal(HashableLiteral(crate::analysis::LiteralValue::Bool(false))),
            DataType::Boolean,
            false,
        );
        let branch = arena.intern(
            ScalarNode::Case {
                operand: None,
                when_then: vec![(condition, catching)],
                else_expr: Some(boolean),
            },
            DataType::Boolean,
            true,
        );
        assert!(can_fail(&arena, branch));
        let wrapper = arena.intern(
            ScalarNode::IsNull {
                child: branch,
                negated: false,
            },
            DataType::Boolean,
            false,
        );
        assert!(can_fail(&arena, wrapper));
    }

    #[test]
    fn non_row_boundaries_retain_aggregate_and_window_ordering_child_effects() {
        let mut arena = ScalarArena::new();
        let text = arena.intern(ScalarNode::ColumnRef(ColumnId(1)), DataType::Utf8, true);
        let boolean = arena.intern(ScalarNode::ColumnRef(ColumnId(2)), DataType::Boolean, true);
        let total = actual_call(&mut arena, "parse_json", vec![text]);
        let raises = actual_call(&mut arena, "assert_true", vec![boolean]);
        let order = |expr| SortKey {
            expr,
            asc: true,
            nulls_first: true,
            display: None,
        };
        let aggregate =
            crate::functions::test_resolved_aggregate("array_agg", &[DataType::Boolean], false);
        assert_eq!(
            aggregate.semantics.intrinsic_row_error,
            novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated
        );
        let agg = arena.intern(
            ScalarNode::AggregateCall {
                name: "array_agg".into(),
                args: vec![boolean],
                distinct: false,
                order_by: vec![order(raises)],
                resolved: aggregate.clone(),
            },
            DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Boolean,
                true,
            ))),
            true,
        );
        assert!(can_fail(&arena, agg));
        let binding = aggregate;
        for (function_order, partition, over_order, expected) in [
            (total, text, total, false),
            (raises, text, total, true),
            (total, raises, total, true),
            (total, text, raises, true),
        ] {
            let window = arena.intern(
                ScalarNode::WindowCall {
                    name: "array_agg".into(),
                    args: vec![boolean],
                    distinct: false,
                    binding: binding.clone(),
                    function_order_by: vec![order(function_order)],
                    aggregate_binding: Some(binding.clone()),
                    partition_by: vec![partition],
                    order_by: vec![order(over_order)],
                    window_frame: None,
                    ignore_nulls: false,
                },
                DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Boolean,
                    true,
                ))),
                true,
            );
            assert_eq!(can_fail(&arena, window), expected);
        }
    }
}

#[cfg(test)]
mod intrinsic_cast_effect_tests {
    use super::*;
    use arrow::datatypes::{Field, TimeUnit};
    use novarocks_type_contract::DecimalOverflowPolicy;
    use std::sync::Arc;

    #[test]
    fn lawful_temporal_and_mode_errors_are_independent_of_decimal_null_policy() {
        for (source, target, expected) in [
            (DataType::Date32, DataType::Utf8, true),
            (DataType::Date32, DataType::Float64, true),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            (DataType::Float64, DataType::Int8, true),
            (DataType::Utf8, DataType::Int64, false),
            (DataType::Binary, DataType::Utf8, false),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Utf8,
                false,
            ),
            (DataType::Date32, DataType::Date32, false),
        ] {
            let mut arena = ScalarArena::new();
            let child = arena.intern(ScalarNode::ColumnRef(ColumnId(1)), source, true);
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let cast = arena.intern(
                    ScalarNode::Cast {
                        child,
                        target: target.clone(),
                        decimal_overflow_policy: policy,
                    },
                    target.clone(),
                    true,
                );
                assert_eq!(can_fail(&arena, cast), expected);
            }
        }
    }

    #[test]
    fn recursive_cast_effect_tracks_existing_name_mapping_and_metadata_identity() {
        let list = |ty| DataType::List(Arc::new(Field::new("item", ty, true)));
        let fields = |a, b| {
            DataType::Struct(vec![Field::new("a", a, true), Field::new("b", b, true)].into())
        };
        assert!(cast_has_intrinsic_row_error(
            &list(DataType::Date32),
            &list(DataType::Utf8),
            true
        ));
        let source = fields(DataType::Int64, DataType::Date32);
        let by_name = DataType::Struct(
            vec![
                Field::new("b", DataType::Utf8, true),
                Field::new("a", DataType::Int64, true),
            ]
            .into(),
        );
        assert!(cast_has_intrinsic_row_error(&source, &by_name, true));
        assert!(!cast_has_intrinsic_row_error(
            &list(DataType::Utf8),
            &list(DataType::Utf8),
            true
        ));
        // Root-only calendar and SQL-mode checks are not invented for shared recursive casts.
        assert!(!cast_has_intrinsic_row_error(
            &list(DataType::Date32),
            &list(DataType::Float64),
            true
        ));
        assert!(!cast_has_intrinsic_row_error(
            &list(DataType::Float64),
            &list(DataType::Int8),
            true
        ));
        assert!(cast_has_intrinsic_row_error(
            &list(DataType::Timestamp(TimeUnit::Microsecond, None)),
            &DataType::Utf8,
            true
        ));
    }
    #[test]
    fn decimal_multiply_preserves_legacy_error_effect_without_a_frozen_allow_flag() {
        use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
        let mut arena = ScalarArena::new();
        for dtype in [
            DataType::Decimal128(38, 0),
            DataType::Decimal256(76, 0),
            DataType::Float64,
        ] {
            let left = arena.intern(ScalarNode::ColumnRef(ColumnId(51)), dtype.clone(), true);
            let right = arena.intern(ScalarNode::ColumnRef(ColumnId(52)), dtype.clone(), true);
            for op in [BinOp::Add, BinOp::Sub, BinOp::Mul, BinOp::Div, BinOp::Mod] {
                for policy in [OutputNull, ReportError] {
                    let expr = arena.intern(
                        ScalarNode::BinaryOp {
                            op,
                            left,
                            right,
                            decimal_overflow_policy: policy,
                        },
                        dtype.clone(),
                        true,
                    );
                    let is_decimal =
                        matches!(dtype, DataType::Decimal128(..) | DataType::Decimal256(..));
                    assert_eq!(
                        can_fail(&arena, expr),
                        is_decimal && (op == BinOp::Mul || policy == ReportError),
                        "{dtype:?}/{op:?}/{policy:?}"
                    );
                    assert!(!contains_non_deterministic_function(&arena, expr));
                }
            }
        }
    }

    #[test]
    fn legacy_decimal_cast_effect_distinguishes_proven_same_scale_widening() {
        use novarocks_type_contract::DecimalOverflowPolicy::OutputNull;
        let mut arena = ScalarArena::new();
        for (source, target, expected) in [
            (
                DataType::Decimal128(38, 0),
                DataType::Decimal128(3, 0),
                true,
            ),
            (
                DataType::Decimal256(76, 0),
                DataType::Decimal128(38, 0),
                true,
            ),
            (DataType::Decimal128(5, 3), DataType::Decimal128(4, 2), true),
            (
                DataType::Decimal128(9, 2),
                DataType::Decimal128(18, 2),
                false,
            ),
            (
                DataType::Decimal128(38, 0),
                DataType::Decimal256(76, 0),
                false,
            ),
            (
                DataType::Decimal256(38, 0),
                DataType::Decimal128(38, 0),
                false,
            ),
            (
                DataType::Decimal128(9, 2),
                DataType::Decimal128(9, 2),
                false,
            ),
            (DataType::Int64, DataType::Decimal128(9, 0), false),
        ] {
            let child = arena.intern(ScalarNode::ColumnRef(ColumnId(61)), source.clone(), true);
            let cast = arena.intern(
                ScalarNode::Cast {
                    child,
                    target: target.clone(),
                    decimal_overflow_policy: OutputNull,
                },
                target.clone(),
                true,
            );
            assert_eq!(can_fail(&arena, cast), expected, "{source:?} -> {target:?}");
        }
    }
}
