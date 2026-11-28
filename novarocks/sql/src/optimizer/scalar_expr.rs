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
use crate::common::negation::{render_negation, scalar_operand};
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
    visit_column_refs(arena, expr, &mut |id, _| {
        out.insert(id);
        Some(())
    })?;
    Some(out)
}

/// Prove that every actual bound column use agrees with the destination port.
pub(crate) fn all_column_refs_match(
    arena: &ScalarArena,
    expr: ScalarId,
    mut matches: impl FnMut(ColumnId, &novarocks_type_contract::FunctionValueType) -> bool,
) -> bool {
    visit_column_refs(arena, expr, &mut |id, ty| matches(id, ty).then_some(())).is_some()
}

fn visit_column_refs(
    arena: &ScalarArena,
    expr: ScalarId,
    visit: &mut impl FnMut(ColumnId, &novarocks_type_contract::FunctionValueType) -> Option<()>,
) -> Option<()> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(id) => {
            if *id == ColumnId::UNSET {
                return None;
            }
            visit(*id, arena.value_type(expr))?;
        }
        ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) | ScalarNode::Constant(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            visit_column_refs(arena, *left, visit)?;
            visit_column_refs(arena, *right, visit)?;
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => visit_column_refs(arena, *child, visit)?,
        ScalarNode::FunctionCall { args, .. } => {
            for arg in args {
                visit_column_refs(arena, *arg, visit)?;
            }
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            visit_column_refs(arena, *body, visit)?;
        }
        ScalarNode::AggregateCall {
            args,
            order_by,
            filter,
            ..
        } => {
            if let Some(predicate) = filter {
                visit_column_refs(arena, *predicate, visit)?;
            }
            for arg in args {
                visit_column_refs(arena, *arg, visit)?;
            }
            for item in order_by {
                visit_column_refs(arena, item.expr, visit)?;
            }
        }
        ScalarNode::InList { child, list, .. } => {
            visit_column_refs(arena, *child, visit)?;
            for item in list {
                visit_column_refs(arena, *item, visit)?;
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            visit_column_refs(arena, *child, visit)?;
            visit_column_refs(arena, *low, visit)?;
            visit_column_refs(arena, *high, visit)?;
        }
        ScalarNode::Like { child, pattern, .. } => {
            visit_column_refs(arena, *child, visit)?;
            visit_column_refs(arena, *pattern, visit)?;
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(operand) = operand {
                visit_column_refs(arena, *operand, visit)?;
            }
            for (when, then) in when_then {
                visit_column_refs(arena, *when, visit)?;
                visit_column_refs(arena, *then, visit)?;
            }
            if let Some(else_expr) = else_expr {
                visit_column_refs(arena, *else_expr, visit)?;
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
                visit_column_refs(arena, *arg, visit)?;
            }
            for item in function_order_by {
                visit_column_refs(arena, item.expr, visit)?;
            }
            for expr in partition_by {
                visit_column_refs(arena, *expr, visit)?;
            }
            for item in order_by {
                visit_column_refs(arena, item.expr, visit)?;
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
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError> {
    combine_binary_bool(arena, &mut exprs, BinOp::And, control)
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
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError> {
    combine_binary_bool(arena, &mut exprs, BinOp::Or, control)
}

fn combine_binary_bool(
    arena: &mut ScalarArena,
    exprs: &mut Vec<ScalarId>,
    op: BinOp,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError> {
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    let Some(mut result) = exprs.pop() else {
        return Ok(None);
    };
    while let Some(next) = exprs.pop() {
        let nullable = arena.nullable(next) || arena.nullable(result);
        result = arena.intern_observed(
            ScalarNode::BinaryOp {
                op,
                left: next,
                right: result,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, nullable),
            control,
        )?;
    }
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    Ok(Some(result))
}

pub(crate) fn bool_literal(
    arena: &mut ScalarArena,
    value: bool,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    arena.intern_observed(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(value))),
        novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        control,
    )
}

/// Observe a fixed signed widening without changing either original binding.
/// This is the existing six total, order/NULL-preserving signed conversions;
/// narrowing, nominal, unsigned, temporal and floating domains are excluded.
pub(crate) fn lossless_signed_widening(
    source: &novarocks_type_contract::FunctionValueType,
    result: &novarocks_type_contract::FunctionValueType,
) -> bool {
    source.nullable() == result.nullable() && signed_widening_domains(source, result)
}

/// A non-null source cannot acquire a NULL value through this total widening.
/// A wider result NULL promise still belongs to its original cast author.
pub(crate) fn nonnull_signed_literal_widening(
    source: &novarocks_type_contract::FunctionValueType,
    result: &novarocks_type_contract::FunctionValueType,
) -> bool {
    !source.nullable() && signed_widening_domains(source, result)
}

fn signed_widening_domains(
    source: &novarocks_type_contract::FunctionValueType,
    result: &novarocks_type_contract::FunctionValueType,
) -> bool {
    use novarocks_type_contract::ValueLogicalType;
    source.logical_type() == ValueLogicalType::Physical
        && result.logical_type() == ValueLogicalType::Physical
        && matches!(
            (source.data_type(), result.data_type()),
            (
                DataType::Int8,
                DataType::Int16 | DataType::Int32 | DataType::Int64
            ) | (DataType::Int16, DataType::Int32 | DataType::Int64)
                | (DataType::Int32, DataType::Int64)
        )
}

pub(crate) fn signed_integer_fits(value: i64, data_type: &DataType) -> bool {
    match data_type {
        DataType::Int8 => i8::try_from(value).is_ok(),
        DataType::Int16 => i16::try_from(value).is_ok(),
        DataType::Int32 => i32::try_from(value).is_ok(),
        DataType::Int64 => true,
        _ => false,
    }
}

/// An optimization fact about an already signed literal/constant, not a fold.
/// Leave the source and every CAST in place. Strict widening permits at most
/// three CAST edges; no expression evaluation, allocation or failure retry.
pub(crate) fn signed_integer_literal(arena: &ScalarArena, mut expression: ScalarId) -> Option<i64> {
    loop {
        let value_type = arena.value_type(expression);
        if value_type.logical_type() != novarocks_type_contract::ValueLogicalType::Physical {
            return None;
        }
        match arena.node(expression) {
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value)))
                if !value_type.nullable()
                    && signed_integer_fits(*value, value_type.data_type()) =>
            {
                return Some(*value);
            }
            ScalarNode::Constant(value)
                if !value_type.nullable()
                    && signed_integer_fits(0, value_type.data_type())
                    && value.value_type() == value_type =>
            {
                return value.try_i64().ok().flatten();
            }
            ScalarNode::Cast { child, target, .. }
                if target == value_type.data_type()
                    && nonnull_signed_literal_widening(arena.value_type(*child), value_type) =>
            {
                expression = *child
            }
            _ => return None,
        }
    }
}

pub(crate) fn int_literal(
    arena: &mut ScalarArena,
    value: i64,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    arena.intern_observed(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        control,
    )
}

pub(crate) fn literal_count_value(
    arena: &ScalarArena,
    expr: ScalarId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<i64>, crate::compiler::SqlCompileError> {
    use crate::compiler::SqlCompileError;
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result: Result<Option<i64>, SqlCompileError> = (|| {
        let node = arena.node(expr);
        work.step()?;
        match node {
            ScalarNode::Literal(HashableLiteral(LiteralValue::Null)) => Ok(Some(0)),
            ScalarNode::Literal(_) => Ok(Some(1)),
            ScalarNode::Constant(value) => {
                if !arena
                    .value_type(expr)
                    .exactly_equals_observed(value.value_type(), || {
                        work.step()
                            .map_err(novarocks_functions::ConstantError::from)
                    })?
                {
                    return Err(SqlCompileError::InvalidRequest(
                        "COUNT constant source differs from its frozen scalar type".into(),
                    ));
                }
                work.flush()?;
                // COUNT observes logical SQL NULL at the original selected
                // ordinal. Encoded/nested backing and nominal domains remain
                // facts of the sole constant owner, not carrier guesses here.
                let null = value.is_null_observed(CompilePhase::Validate, control)?;
                Ok(Some(i64::from(!null)))
            }
            _ => Ok(None),
        }
    })();
    if matches!(
        result,
        Err(SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
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
            ..
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
        ScalarNode::ColumnRef(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::Literal(_)
        | ScalarNode::Constant(_) => false,
    }
}

pub(crate) fn sort_key_column_id(arena: &ScalarArena, key: &SortKey) -> Option<ColumnId> {
    column_id(arena, key.expr)
}

pub(crate) fn scalar_display_name(arena: &ScalarArena, expr: ScalarId) -> String {
    if let Some((operator, [left, right])) =
        crate::common::binary_operator::scalar_operands(arena, expr)
    {
        return format!(
            "{} {} {}",
            scalar_display_name_with_parens(arena, left),
            operator.sql(),
            scalar_display_name_with_parens(arena, right)
        );
    }
    if let Some(operand) = scalar_operand(arena, expr) {
        let needs_parentheses = crate::common::binary_operator::scalar_operands(arena, operand)
            .is_some()
            || !matches!(
                arena.node(operand),
                ScalarNode::ColumnRef(_)
                    | ScalarNode::LambdaParamRef { .. }
                    | ScalarNode::Literal(_)
                    | ScalarNode::Constant(_)
                    | ScalarNode::FunctionCall { .. }
                    | ScalarNode::AggregateCall { .. }
                    | ScalarNode::Cast { .. }
            );
        return render_negation(&scalar_display_name(arena, operand), needs_parentheses);
    }
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
        ScalarNode::Cast {
            child,
            target,
            cast_mode,
            ..
        } => {
            let name = if *cast_mode == Some(novarocks_type_contract::TrinoCastMode::Try) {
                "try_cast"
            } else {
                "cast"
            };
            format!(
                "{name}({} as {:?})",
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
        .filter(|item| {
            !matches!(
                arena.node(item.expr),
                ScalarNode::Literal(_) | ScalarNode::Constant(_)
            )
        })
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
    if crate::common::binary_operator::scalar_operands(arena, expr).is_some() {
        return format!("({})", scalar_display_name(arena, expr));
    }
    match arena.node(expr) {
        ScalarNode::ColumnRef(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::Literal(_)
        | ScalarNode::Constant(_)
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

pub(crate) fn is_true_literal(
    arena: &ScalarArena,
    expr: ScalarId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<bool, crate::compiler::SqlCompileError> {
    Ok(boolean_literal_value(arena, expr, control)? == Some(true))
}

fn boolean_literal_value(
    arena: &ScalarArena,
    expr: ScalarId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<bool>, crate::compiler::SqlCompileError> {
    use crate::compiler::SqlCompileError;
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase, ValueLogicalType};
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result: Result<Option<bool>, SqlCompileError> = (|| {
        let node = arena.node(expr);
        work.step()?;
        match node {
            ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(value))) => Ok(Some(*value)),
            ScalarNode::Constant(value) => {
                if !arena
                    .value_type(expr)
                    .exactly_equals_observed(value.value_type(), || {
                        work.step()
                            .map_err(novarocks_functions::ConstantError::from)
                    })?
                {
                    return Err(SqlCompileError::InvalidRequest(
                        "Boolean constant source differs from its frozen scalar type".into(),
                    ));
                }
                let physical_boolean = value.value_type().logical_type()
                    == ValueLogicalType::Physical
                    && *value.value_type().data_type() == DataType::Boolean;
                work.step()?;
                if !physical_boolean {
                    return Ok(None);
                }
                // Reuse the admitted canonical Boolean array and checked
                // selected ordinal. A wrong type is not a Boolean match;
                // corrupt carrier facts remain typed errors, rather than NULL.
                let selected = value.try_boolean();
                work.step()?;
                selected.map_err(SqlCompileError::from)
            }
            _ => Ok(None),
        }
    })();
    if matches!(
        result,
        Err(SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}

/// A projection can carry a checked row result or an explicit internal
/// assertion even when its column is absent from the requested output.
pub(crate) fn must_retain_projection(arena: &ScalarArena, expr: ScalarId) -> bool {
    is_internal_assertion(arena, expr) || arena.effects(expr).may_raise_data()
}

pub(crate) fn is_internal_assertion(arena: &ScalarArena, expr: ScalarId) -> bool {
    arena.effects(expr).has_internal_assertion
        || matches!(
            arena.node(expr),
            ScalarNode::FunctionCall { binding, .. }
                if binding.function_id.as_str() == crate::functions::ASSERT_TRUE_ID
        )
}

fn signed_integer_bounds(arena: &ScalarArena, mut expression: ScalarId) -> Option<(i64, i64)> {
    if let Some(literal) = signed_integer_literal(arena, expression) {
        return Some((literal, literal));
    }
    loop {
        let value_type = arena.value_type(expression);
        if let ScalarNode::Cast {
            child,
            target,
            cast_mode:
                None
                | Some(
                    novarocks_type_contract::TrinoCastMode::Explicit
                    | novarocks_type_contract::TrinoCastMode::Implicit,
                ),
            ..
        } = arena.node(expression)
            && target == value_type.data_type()
            && lossless_signed_widening(arena.value_type(*child), value_type)
        {
            expression = *child;
            continue;
        }
        if value_type.logical_type() != novarocks_type_contract::ValueLogicalType::Physical {
            return None;
        }
        return match value_type.data_type() {
            DataType::Int8 => Some((i64::from(i8::MIN), i64::from(i8::MAX))),
            DataType::Int16 => Some((i64::from(i16::MIN), i64::from(i16::MAX))),
            DataType::Int32 => Some((i64::from(i32::MIN), i64::from(i32::MAX))),
            DataType::Int64 => Some((i64::MIN, i64::MAX)),
            _ => None,
        };
    }
}

/// Prove that the selected signed operator cannot raise a row arithmetic error.
/// This does not change the selected overload or discharge an operand's effects.
pub(super) fn prove_signed_integer_operator_total(
    arena: &ScalarArena,
    result_type: &novarocks_type_contract::FunctionValueType,
    binding: &crate::binding::SqlFunctionBinding,
    args: &[ScalarId],
) -> Option<()> {
    use novarocks_functions::FunctionResultType;
    use novarocks_type_contract::{FunctionArgumentType, OperatorFunction, ValueLogicalType};

    let Some(operator @ (OperatorFunction::Add | OperatorFunction::Modulo)) =
        OperatorFunction::from_function_id(&binding.function_id)
    else {
        return None;
    };
    let [left, right] = args else { return None };
    let [
        FunctionArgumentType::Value(selected_left),
        FunctionArgumentType::Value(selected_right),
    ] = binding.selected.argument_types.as_slice()
    else {
        return None;
    };
    let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
        return None;
    };
    let carrier = result.data_type();
    if result != result_type
        || result.logical_type() != ValueLogicalType::Physical
        || !signed_integer_fits(0, carrier)
        || selected_left != arena.value_type(*left)
        || selected_right != arena.value_type(*right)
        || selected_left.logical_type() != ValueLogicalType::Physical
        || selected_right.logical_type() != ValueLogicalType::Physical
        || selected_left.data_type() != carrier
        || selected_right.data_type() != carrier
    {
        return None;
    }

    if operator == OperatorFunction::Modulo {
        let divisor = signed_integer_literal(arena, *right)?;
        // Keep the exceptional signed min/-1 division pair out of this proof.
        return (divisor != 0 && divisor != -1).then_some(());
    }

    let (left_min, left_max) = signed_integer_bounds(arena, *left)?;
    let (right_min, right_max) = signed_integer_bounds(arena, *right)?;
    let min = left_min.checked_add(right_min)?;
    let max = left_max.checked_add(right_max)?;
    (signed_integer_fits(min, carrier) && signed_integer_fits(max, carrier)).then_some(())
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
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, nullable),
        )
    }

    #[test]
    fn signed_literal_widening_observation_keeps_sources_and_refuses_other_domains() {
        use novarocks_type_contract::FunctionValueType as V;
        let mut arena = ScalarArena::new();
        let source = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(127))),
            V::new(DataType::Int32, false),
        );
        let cast = arena.intern(
            ScalarNode::Cast {
                cast_mode: None,
                child: source,
                target: DataType::Int64,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            V::new(DataType::Int64, false),
        );
        assert_eq!(signed_integer_literal(&arena, cast), Some(127));
        let nullable_cast = arena.intern(
            ScalarNode::Cast {
                cast_mode: None,
                child: source,
                target: DataType::Int64,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            V::new(DataType::Int64, true),
        );
        assert!(
            !lossless_signed_widening(arena.value_type(source), arena.value_type(nullable_cast)),
            "column contract remains strict"
        );
        assert_eq!(
            signed_integer_literal(&arena, nullable_cast),
            Some(127),
            "the result's NULL promise does not turn a known non-null source into NULL"
        );
        assert!(matches!(arena.node(cast), ScalarNode::Cast { child, .. } if *child == source));
        assert_eq!(arena.value_type(source), &V::new(DataType::Int32, false));
        for (input, output, nullable) in [
            (DataType::Int64, DataType::Int32, false),
            (DataType::Int32, DataType::Float64, false),
            (DataType::Date32, DataType::Int64, false),
            (DataType::Int32, DataType::Int64, true),
        ] {
            let child = arena.intern(
                ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
                V::new(input, nullable),
            );
            let cast = arena.intern(
                ScalarNode::Cast {
                    cast_mode: None,
                    child,
                    target: output.clone(),
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                },
                V::new(output, nullable),
            );
            assert_eq!(signed_integer_literal(&arena, cast), None);
        }
        let null = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Null)),
            V::new(DataType::Int32, true),
        );
        assert_eq!(signed_integer_literal(&arena, null), None);
    }

    #[test]
    fn strict_column_collection_rejects_unset_column_ref() {
        let mut arena = ScalarArena::new();
        let expr = arena.intern(
            ScalarNode::ColumnRef(ColumnId::UNSET),
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
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
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        );

        let mut parts = Vec::new();
        split_conjuncts(&arena, and, &mut parts);
        assert_eq!(parts, vec![a, b]);

        let rebuilt = combine_conjuncts(
            &mut arena,
            parts,
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap()
        .unwrap();
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
            novarocks_type_contract::FunctionValueType::new(DataType::Decimal128(38, 0), true),
        );
        let integer = arena.intern(
            ScalarNode::ColumnRef(ColumnId(2)),
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        );
        let nullable = arena.intern(
            ScalarNode::Cast {
                cast_mode: Some(novarocks_type_contract::TrinoCastMode::Try),
                child: integer,
                target: DataType::Decimal128(9, 0),
                decimal_overflow_policy: OutputNull,
            },
            novarocks_type_contract::FunctionValueType::new(DataType::Decimal128(9, 0), true),
        );
        let throwing = arena.intern(
            ScalarNode::Cast {
                cast_mode: Some(novarocks_type_contract::TrinoCastMode::Explicit),
                child,
                target: DataType::Decimal128(9, 0),
                decimal_overflow_policy: ReportError,
            },
            novarocks_type_contract::FunctionValueType::new(DataType::Decimal128(9, 0), true),
        );
        assert!(!arena.effects(nullable).may_raise_data());
        assert!(arena.effects(throwing).may_raise_data());
        assert!(
            !arena
                .effects(throwing)
                .expression
                .value_stability
                .is_volatile()
        );
        let predicate = arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Eq,
                left: throwing,
                right: nullable,
                decimal_overflow_policy: OutputNull,
            },
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
        );
        assert!(arena.effects(predicate).may_raise_data());
        assert!(
            !arena
                .effects(predicate)
                .expression
                .value_stability
                .is_volatile()
        );
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
                value_type: arena.value_type(*id).clone(),
                constant: None,
            })
            .collect::<Vec<_>>();
        let catalog = crate::functions::builtin_engine_function_catalog();
        let binding = catalog
            .resolve_bound_trusted(
                name,
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: args.len(),
                },
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        catalog
            .validate_bound(
                &binding,
                FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: args.len(),
                },
                &crate::compiler::SqlCompileControl::unbounded(),
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
                binding: crate::binding::SqlFunctionBinding::from_plan(
                    crate::binding::PlanBindingAuthority::Optimizer(
                        crate::optimizer::BindingAuthority(()),
                    ),
                    binding,
                    novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                ),
                volatility,
            },
            output,
        )
    }

    #[test]
    fn signed_add_proves_widened_operand_without_disabling_checked_integer_add() {
        use novarocks_type_contract::{
            DecimalOverflowPolicy, FunctionArgumentType, OperatorFunction, TrinoCastMode,
        };

        let mut arena = ScalarArena::new();
        let integer = arena.intern(
            ScalarNode::ColumnRef(ColumnId(1)),
            FunctionValueType::new(DataType::Int32, true),
        );
        let bigint_type = FunctionValueType::new(DataType::Int64, true);
        let [implicit, explicit, assignment] = [
            TrinoCastMode::Implicit,
            TrinoCastMode::Explicit,
            TrinoCastMode::StoreAssignment,
        ]
        .map(|cast_mode| {
            arena.intern(
                ScalarNode::Cast {
                    child: integer,
                    target: DataType::Int64,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    cast_mode: Some(cast_mode),
                },
                bigint_type.clone(),
            )
        });
        let bigint = arena.intern(ScalarNode::ColumnRef(ColumnId(2)), bigint_type.clone());
        let one = arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
            FunctionValueType::new(DataType::Int64, false),
        );
        let arguments = [implicit, one].map(|id| FunctionArgument::Value {
            value_type: arena.value_type(id).clone(),
            constant: None,
        });
        let request = FunctionBindingRequest {
            expected_result_type: None,
            arguments: &arguments,
            logical_argument_count: arguments.len(),
        };
        let control = crate::compiler::SqlCompileControl::unbounded();
        let catalog = crate::functions::builtin_engine_function_catalog();
        let operator = OperatorFunction::Add;
        let selected = catalog
            .resolve_bound_trusted_by_id(
                &operator.function_id(),
                FunctionKind::Scalar,
                request,
                &control,
            )
            .unwrap();
        catalog
            .validate_bound(&selected, request, &control)
            .unwrap();
        let FunctionResultType::Scalar(result) = &selected.selected.result_type else {
            panic!("scalar result")
        };
        assert_eq!(result, &bigint_type);
        assert_eq!(
            selected.selected.argument_types.as_slice(),
            &[
                FunctionArgumentType::Value(arena.value_type(implicit).clone()),
                FunctionArgumentType::Value(arena.value_type(one).clone()),
            ]
        );
        let result = result.clone();
        let volatility = selected.semantics.volatility;
        let binding = crate::binding::SqlFunctionBinding::from_plan(
            crate::optimizer::BINDING_AUTHORITY,
            selected,
            DecimalOverflowPolicy::OutputNull,
        );
        assert_eq!(
            binding.semantics.intrinsic_row_error,
            novarocks_type_contract::FunctionIntrinsicRowError::MayRaise
        );
        for (left, expected) in [
            (implicit, false),
            (explicit, false),
            (assignment, true),
            (bigint, true),
        ] {
            let expression = arena.intern(
                ScalarNode::FunctionCall {
                    name: operator.catalog_name().into(),
                    args: vec![left, one],
                    distinct: false,
                    binding: binding.clone(),
                    volatility,
                },
                result.clone(),
            );
            assert_eq!(arena.effects(expression).may_raise_data(), expected);
        }
    }

    #[test]
    fn signed_remainder_proves_nonzero_widened_divisor() {
        use novarocks_type_contract::{
            DecimalOverflowPolicy, FunctionArgumentType, OperatorFunction,
        };

        let mut arena = ScalarArena::new();
        let result_type = FunctionValueType::new(DataType::Int64, true);
        let dividend = arena.intern(ScalarNode::ColumnRef(ColumnId(1)), result_type.clone());
        let divisor_type = FunctionValueType::new(DataType::Int64, false);
        let arguments = [&result_type, &divisor_type].map(|value_type| FunctionArgument::Value {
            value_type: value_type.clone(),
            constant: None,
        });
        let request = FunctionBindingRequest {
            expected_result_type: None,
            arguments: &arguments,
            logical_argument_count: arguments.len(),
        };
        let control = crate::compiler::SqlCompileControl::unbounded();
        let catalog = crate::functions::builtin_engine_function_catalog();
        let operator = OperatorFunction::Modulo;
        let selected = catalog
            .resolve_bound_trusted_by_id(
                &operator.function_id(),
                FunctionKind::Scalar,
                request,
                &control,
            )
            .unwrap();
        catalog
            .validate_bound(&selected, request, &control)
            .unwrap();
        assert_eq!(
            selected.selected.result_type,
            FunctionResultType::Scalar(result_type.clone())
        );
        assert_eq!(
            selected.selected.argument_types.as_slice(),
            &[
                FunctionArgumentType::Value(result_type.clone()),
                FunctionArgumentType::Value(divisor_type.clone()),
            ]
        );
        let volatility = selected.semantics.volatility;
        let binding = crate::binding::SqlFunctionBinding::from_plan(
            crate::optimizer::BINDING_AUTHORITY,
            selected,
            DecimalOverflowPolicy::OutputNull,
        );
        assert_eq!(
            binding.semantics.intrinsic_row_error,
            novarocks_type_contract::FunctionIntrinsicRowError::MayRaise
        );
        for (literal, expected) in [(1000, false), (0, true), (-1, true)] {
            let original = arena.intern(
                ScalarNode::Literal(HashableLiteral(LiteralValue::Int(literal))),
                FunctionValueType::new(DataType::Int32, false),
            );
            let divisor = arena.intern(
                ScalarNode::Cast {
                    child: original,
                    target: DataType::Int64,
                    cast_mode: Some(novarocks_type_contract::TrinoCastMode::Implicit),
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                },
                divisor_type.clone(),
            );
            let expression = arena.intern(
                ScalarNode::FunctionCall {
                    name: operator.catalog_name().into(),
                    args: vec![dividend, divisor],
                    distinct: false,
                    binding: binding.clone(),
                    volatility,
                },
                result_type.clone(),
            );
            assert_eq!(arena.effects(expression).may_raise_data(), expected);
        }
    }

    #[test]
    fn selected_intrinsic_fact_controls_effect_without_display_name_dispatch() {
        let mut arena = ScalarArena::new();
        let text = arena.intern(
            ScalarNode::ColumnRef(ColumnId(1)),
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
        );
        let boolean = arena.intern(
            ScalarNode::ColumnRef(ColumnId(2)),
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
        );
        let total = actual_call(&mut arena, "__json_try_read_text", vec![text]);
        let raises = actual_call(&mut arena, "assert_true", vec![boolean]);
        assert!(!arena.effects(total).may_raise_data());
        assert!(arena.effects(raises).may_raise_data());
        assert!(
            !arena
                .effects(raises)
                .expression
                .value_stability
                .is_volatile()
        );
        let mut renamed = arena.node(raises).clone();
        let ScalarNode::FunctionCall { name, .. } = &mut renamed else {
            unreachable!()
        };
        *name = "__json_try_read_text".into();
        let renamed = arena.intern(renamed, arena.value_type(raises).clone());
        assert!(arena.effects(renamed).may_raise_data());
    }

    #[test]
    fn scalar_invocation_data_requires_the_installed_abi_and_ignores_display_name() {
        let mut arena = ScalarArena::new();
        let text = arena.intern(
            ScalarNode::ColumnRef(ColumnId(1)),
            FunctionValueType::new(DataType::Utf8, true),
        );
        let total = actual_call(&mut arena, "__json_try_read_text", vec![text]);
        assert!(!arena.effects(total).may_raise_data());
        let mut node = arena.node(total).clone();
        let ScalarNode::FunctionCall { binding, name, .. } = &mut node else {
            unreachable!()
        };
        let mut facts = binding.resolved().clone();
        facts.declared_scalar_invocation_data = true;
        facts.admitted_execution_abi = None;
        assert_eq!(
            facts.semantics.intrinsic_row_error,
            novarocks_type_contract::FunctionIntrinsicRowError::NoRowError
        );
        *binding = crate::binding::SqlFunctionBinding::from_plan(
            crate::optimizer::BINDING_AUTHORITY,
            facts,
            binding.decimal_overflow_policy(),
        );
        *name = "arbitrary_display_name".into();
        let positive = arena.intern(node, arena.value_type(total).clone());
        assert_ne!(positive, total);
        assert!(!arena.effects(positive).expression.may_raise_invocation_data);
        assert_eq!(arena.effects(positive), arena.effects(total));
        assert!(
            !arena
                .effects(positive)
                .expression
                .value_stability
                .is_volatile()
        );
    }

    #[test]
    fn independent_catching_fact_and_short_circuit_children_are_preserved() {
        let mut arena = ScalarArena::new();
        let boolean = arena.intern(
            ScalarNode::ColumnRef(ColumnId(2)),
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
        );
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
        *binding = crate::binding::SqlFunctionBinding::from_plan(
            crate::optimizer::BINDING_AUTHORITY,
            exact,
            binding.decimal_overflow_policy(),
        );
        let catching = arena.intern(
            catching,
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
        );
        assert!(arena.effects(catching).may_raise_data());
        let condition = arena.intern(
            ScalarNode::Literal(HashableLiteral(crate::analysis::LiteralValue::Bool(false))),
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        );
        let branch = arena.intern(
            ScalarNode::Case {
                ieee_float_equality: false,
                operand: None,
                when_then: vec![(condition, catching)],
                else_expr: Some(boolean),
            },
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
        );
        assert!(arena.effects(branch).may_raise_data());
        let wrapper = arena.intern(
            ScalarNode::IsNull {
                child: branch,
                negated: false,
            },
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        );
        assert!(arena.effects(wrapper).may_raise_data());
    }

    #[test]
    fn non_row_boundaries_retain_aggregate_and_window_ordering_child_effects() {
        let mut arena = ScalarArena::new();
        let text = arena.intern(
            ScalarNode::ColumnRef(ColumnId(1)),
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
        );
        let boolean = arena.intern(
            ScalarNode::ColumnRef(ColumnId(2)),
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
        );
        let total = actual_call(&mut arena, "__json_try_read_text", vec![text]);
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
                occurrence: None,
                filter: None,
                name: "array_agg".into(),
                args: vec![boolean],
                distinct: false,
                order_by: vec![order(raises)],
                resolved: aggregate.clone(),
            },
            novarocks_type_contract::FunctionValueType::new(
                DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Boolean,
                    true,
                ))),
                true,
            ),
        );
        assert!(arena.effects(agg).may_raise_data());
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
                novarocks_type_contract::FunctionValueType::new(
                    DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
                        "item",
                        DataType::Boolean,
                        true,
                    ))),
                    true,
                ),
            );
            assert_eq!(arena.effects(window).may_raise_data(), expected);
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
    fn lawful_temporal_and_cast_mode_errors_are_independent_of_decimal_null_policy() {
        use novarocks_type_contract::TrinoCastMode;
        for (source, target, mode, expected) in [
            (
                DataType::Date32,
                DataType::Utf8,
                TrinoCastMode::Explicit,
                true,
            ),
            (
                DataType::Float64,
                DataType::Int8,
                TrinoCastMode::Explicit,
                true,
            ),
            (DataType::Utf8, DataType::Int64, TrinoCastMode::Try, false),
            (
                DataType::Int64,
                DataType::Float64,
                TrinoCastMode::Explicit,
                false,
            ),
            (
                DataType::Date32,
                DataType::Date32,
                TrinoCastMode::Explicit,
                false,
            ),
        ] {
            let mut arena = ScalarArena::new();
            let child = arena.intern(
                ScalarNode::ColumnRef(ColumnId(1)),
                novarocks_type_contract::FunctionValueType::new(source.clone(), true),
            );
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let cast = arena.intern(
                    ScalarNode::Cast {
                        cast_mode: Some(mode),
                        child,
                        target: target.clone(),
                        decimal_overflow_policy: policy,
                    },
                    novarocks_type_contract::FunctionValueType::new(target.clone(), true),
                );
                assert_eq!(
                    arena.effects(cast).may_raise_data(),
                    expected,
                    "{source:?} to {target:?} in {mode:?}"
                );
            }
        }
    }

    #[test]
    fn capped_decimal_coercion_keeps_implicit_row_errors_visible() {
        use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType, TrinoCastMode};

        let list = |ty| DataType::List(Arc::new(Field::new("item", ty, true)));
        for (source, target, expected) in [
            (
                DataType::Decimal128(38, 0),
                DataType::Decimal128(38, 38),
                true,
            ),
            (
                list(DataType::Int64),
                list(DataType::Decimal128(38, 38)),
                true,
            ),
            (
                DataType::Date32,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            (
                DataType::Decimal128(7, 2),
                DataType::Decimal128(12, 2),
                false,
            ),
            (
                DataType::Decimal128(10, 0),
                DataType::Decimal128(12, 2),
                false,
            ),
            (DataType::Int32, DataType::Decimal128(12, 2), false),
            (
                list(DataType::Decimal128(10, 0)),
                list(DataType::Decimal128(12, 2)),
                false,
            ),
            (DataType::Int32, DataType::Int64, false),
        ] {
            let mut arena = ScalarArena::new();
            let child = arena.intern(
                ScalarNode::ColumnRef(ColumnId(1)),
                FunctionValueType::new(source, false),
            );
            let cast = arena.intern(
                ScalarNode::Cast {
                    child,
                    target: target.clone(),
                    cast_mode: Some(TrinoCastMode::Implicit),
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                },
                FunctionValueType::new(target, false),
            );
            assert_eq!(arena.effects(cast).may_raise_data(), expected);
        }
    }

    #[test]
    fn explicit_signed_cast_totality_requires_full_declared_domain() {
        use novarocks_type_contract::{FunctionValueType, TrinoCastMode};

        let query = crate::analysis::scalar_effects::ScalarEffectQuery {
            functions: None,
            allow_throw_exception: false,
            control: crate::optimizer::test_optimizer_control(),
        };
        for (source, target, expected) in [
            (DataType::Int8, DataType::Decimal128(3, 0), false),
            (DataType::Int16, DataType::Decimal128(5, 0), false),
            (DataType::Int32, DataType::Decimal128(10, 0), false),
            (DataType::Int64, DataType::Decimal128(19, 0), false),
            (DataType::Int32, DataType::Utf8, false),
            (DataType::Int64, DataType::Utf8, false),
            (DataType::Utf8, DataType::Int64, true),
            (DataType::Int32, DataType::Decimal128(9, 0), true),
            (DataType::Int32, DataType::Decimal128(12, 2), false),
            (DataType::Int32, DataType::Decimal128(11, 2), true),
            (
                DataType::Decimal128(10, 2),
                DataType::Decimal128(10, 0),
                false,
            ),
            (
                DataType::Decimal128(10, 2),
                DataType::Decimal128(8, 0),
                true,
            ),
            (DataType::Float64, DataType::Decimal128(38, 0), true),
        ] {
            let source = FunctionValueType::new(source, true);
            let target = FunctionValueType::new(target, true);
            assert_eq!(
                query
                    .cast(&source, &target, Some(TrinoCastMode::Explicit))
                    .unwrap()
                    .may_raise_data(),
                expected,
                "{source:?} to {target:?}"
            );
        }
    }

    #[test]
    fn recursive_cast_effects_preserve_the_original_nested_conversion_and_catch() {
        use novarocks_type_contract::{FunctionValueType, TrinoCastMode};
        let query = crate::analysis::scalar_effects::ScalarEffectQuery {
            functions: None,
            allow_throw_exception: false,
            control: crate::optimizer::test_optimizer_control(),
        };
        let list = |ty| {
            FunctionValueType::new(DataType::List(Arc::new(Field::new("item", ty, true))), true)
        };
        let pairs = [
            (list(DataType::Int64), list(DataType::Float64), false),
            (list(DataType::Utf8), list(DataType::Utf8), false),
            (list(DataType::Utf8), list(DataType::Int64), true),
            (list(DataType::Float64), list(DataType::Int8), true),
        ];
        for (source, result, raises) in pairs {
            assert_eq!(
                query
                    .cast(&source, &result, Some(TrinoCastMode::Explicit))
                    .unwrap()
                    .may_raise_data(),
                raises,
            );
            assert!(
                !query
                    .cast(&source, &result, Some(TrinoCastMode::Try))
                    .unwrap()
                    .may_raise_data()
            );
        }
    }

    #[test]
    fn decimal_arithmetic_effects_read_the_frozen_policy_and_allow_flag() {
        use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
        for allow_throw in [false, true] {
            let mut arena = ScalarArena::new();
            arena
                .bind_effect_environment(
                    crate::functions::test_function_catalog_snapshot(),
                    allow_throw,
                    crate::optimizer::test_optimizer_control(),
                )
                .unwrap();
            for dtype in [DataType::Decimal128(38, 0), DataType::Float64] {
                let left = arena.intern(
                    ScalarNode::ColumnRef(ColumnId(51)),
                    novarocks_type_contract::FunctionValueType::new(dtype.clone(), true),
                );
                let right = arena.intern(
                    ScalarNode::ColumnRef(ColumnId(52)),
                    novarocks_type_contract::FunctionValueType::new(dtype.clone(), true),
                );
                for op in [BinOp::Add, BinOp::Sub, BinOp::Mul, BinOp::Div, BinOp::Mod] {
                    let result = match dtype {
                        DataType::Decimal128(p, s) => {
                            novarocks_type_contract::decimal_arithmetic_result_type(
                                p,
                                s,
                                p,
                                s,
                                crate::compiler::selected_scalar_call::arithmetic_operator(op)
                                    .unwrap(),
                            )
                            .unwrap()
                        }
                        _ => dtype.clone(),
                    };
                    for policy in [OutputNull, ReportError] {
                        let expr = arena.intern(
                            ScalarNode::BinaryOp {
                                op,
                                left,
                                right,
                                decimal_overflow_policy: policy,
                            },
                            novarocks_type_contract::FunctionValueType::new(result.clone(), true),
                        );
                        let is_decimal =
                            matches!(dtype, DataType::Decimal128(..) | DataType::Decimal256(..));
                        assert_eq!(
                            arena.effects(expr).may_raise_data(),
                            is_decimal
                                && (op == BinOp::Mul && allow_throw || policy == ReportError),
                            "{dtype:?}/{op:?}/{policy:?}"
                        );
                        assert!(!arena.effects(expr).expression.value_stability.is_volatile());
                    }
                }
            }
        }
    }

    #[test]
    fn implicit_decimal_cast_effect_distinguishes_proven_same_scale_widening() {
        use novarocks_type_contract::DecimalOverflowPolicy::OutputNull;
        let mut arena = ScalarArena::new();
        for (source, target, expected) in [
            (
                DataType::Decimal128(38, 0),
                DataType::Decimal128(3, 0),
                true,
            ),
            (DataType::Decimal128(5, 3), DataType::Decimal128(4, 2), true),
            (
                DataType::Decimal128(9, 2),
                DataType::Decimal128(18, 2),
                false,
            ),
            (
                DataType::Decimal128(9, 2),
                DataType::Decimal128(9, 2),
                false,
            ),
            (DataType::Int32, DataType::Decimal128(10, 0), false),
        ] {
            let child = arena.intern(
                ScalarNode::ColumnRef(ColumnId(61)),
                novarocks_type_contract::FunctionValueType::new(source.clone(), true),
            );
            let cast = arena.intern(
                ScalarNode::Cast {
                    cast_mode: Some(novarocks_type_contract::TrinoCastMode::Implicit),
                    child,
                    target: target.clone(),
                    decimal_overflow_policy: OutputNull,
                },
                novarocks_type_contract::FunctionValueType::new(target.clone(), true),
            );
            assert_eq!(
                arena.effects(cast).may_raise_data(),
                expected,
                "{source:?} -> {target:?}"
            );
        }
    }
}

#[cfg(test)]
mod selected_boolean_consumer_tests {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray, Int64Array, LargeBinaryArray};
    use novarocks_functions::{ConstantPool, ConstantValue};
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<u32>>,
        refuse: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::Validate);
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            trace.push(units);
            match self.refuse.clone() {
                Some((at, error)) if trace.len() == at + 1 => Err(error),
                _ => Ok(()),
            }
        }
    }
    fn constant(array: ArrayRef, logical: ValueLogicalType, ordinal: u32) -> ConstantValue {
        let ty = FunctionValueType::try_with_logical_type(array.data_type().clone(), true, logical)
            .unwrap();
        let field = Arc::new(
            ty.try_to_field("actual.constant").unwrap().with_metadata(
                ty.try_to_field("actual.constant")
                    .unwrap()
                    .metadata()
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .chain([("provider.fact".to_owned(), "kept".to_owned())])
                    .collect::<std::collections::HashMap<String, String>>(),
            ),
        );
        ConstantPool::try_new(
            field,
            ty,
            array.to_data(),
            crate::constant::test_constant_policy(),
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap()
        .value(ordinal)
        .unwrap()
    }
    fn intern(arena: &mut ScalarArena, value: ConstantValue) -> ScalarId {
        let ty = value.value_type().clone();
        arena
            .intern_observed(ScalarNode::Constant(value), ty, &Control::default())
            .unwrap()
    }

    #[test]
    fn selected_count_uses_logical_null_and_not_boolean_or_json_payload_truth() {
        let mut arena = ScalarArena::new();
        let source: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None]));
        for (value, expected) in [
            (constant(source.clone(), ValueLogicalType::Physical, 1), 0),
            (constant(source, ValueLogicalType::Physical, 0), 1),
            (
                constant(
                    Arc::new(BooleanArray::from(vec![false])),
                    ValueLogicalType::Physical,
                    0,
                ),
                1,
            ),
            (
                constant(
                    Arc::new(LargeBinaryArray::from(vec![b"\x01\x20".as_slice()])),
                    ValueLogicalType::Json,
                    0,
                ),
                1,
            ),
        ] {
            let ty = value.value_type().clone();
            let ordinal = value.ordinal();
            let id = intern(&mut arena, value);
            assert_eq!(
                literal_count_value(&arena, id, &Control::default()).unwrap(),
                Some(expected)
            );
            assert_eq!(arena.value_type(id), &ty);
            let ScalarNode::Constant(actual) = arena.node(id) else {
                panic!("expected selected constant")
            };
            assert_eq!(actual.ordinal(), ordinal);
            assert_eq!(actual.field().metadata()["provider.fact"], "kept");
        }
        let column = arena.intern(
            ScalarNode::ColumnRef(crate::column_id::ColumnId::new_for_test(91)),
            FunctionValueType::new(DataType::Int64, true),
        );
        assert_eq!(
            literal_count_value(&arena, column, &Control::default()).unwrap(),
            None
        );
    }

    #[test]
    fn selected_count_original_control_refusal_preserves_prefix_and_arena() {
        let mut arena = ScalarArena::new();
        let id = intern(
            &mut arena,
            constant(
                Arc::new(Int64Array::from(vec![Some(7), None])),
                ValueLogicalType::Physical,
                1,
            ),
        );
        let baseline = Control::default();
        assert_eq!(literal_count_value(&arena, id, &baseline).unwrap(), Some(0));
        let trace = baseline.trace.into_inner().unwrap();
        let nodes = arena.node_count();
        for at in 0..trace.len() {
            for (cause, expected) in [
                (
                    CompileControlError::Cancelled,
                    crate::compiler::SqlCompileError::Cancelled,
                ),
                (
                    CompileControlError::DeadlineExceeded,
                    crate::compiler::SqlCompileError::DeadlineExceeded,
                ),
                (
                    CompileControlError::ResourceExhausted,
                    crate::compiler::SqlCompileError::ResourceExhausted,
                ),
            ] {
                let control = Control {
                    refuse: Some((at, cause)),
                    ..Control::default()
                };
                assert_eq!(
                    literal_count_value(&arena, id, &control).unwrap_err(),
                    expected
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                assert_eq!(arena.node_count(), nodes);
            }
        }
    }

    #[test]
    fn selected_boolean_true_false_and_typed_null_keep_actual_ordinal_and_source_type() {
        let source: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false), Some(true), None]));
        let mut arena = ScalarArena::new();
        for (ordinal, expected) in [(0, Some(false)), (1, Some(true)), (2, None)] {
            let value = constant(source.clone(), ValueLogicalType::Physical, ordinal);
            let expected_type = value.value_type().clone();
            let id = intern(&mut arena, value);
            assert_eq!(arena.value_type(id), &expected_type);
            assert_eq!(
                boolean_literal_value(&arena, id, &Control::default()).unwrap(),
                expected
            );
            assert_eq!(
                is_true_literal(&arena, id, &Control::default()).unwrap(),
                expected == Some(true)
            );
            let ScalarNode::Constant(actual) = arena.node(id) else {
                panic!("expected checked constant")
            };
            assert_eq!(actual.ordinal(), ordinal);
            assert_eq!(actual.field().metadata()["provider.fact"], "kept");
        }
    }

    #[test]
    fn selected_boolean_wrong_types_are_no_match_and_false_source_metadata_is_rejected() {
        let mut arena = ScalarArena::new();
        for value in [
            constant(
                Arc::new(Int64Array::from(vec![1])),
                ValueLogicalType::Physical,
                0,
            ),
            constant(
                Arc::new(LargeBinaryArray::from(vec![b"\x01\x42\x01".as_slice()])),
                ValueLogicalType::Json,
                0,
            ),
        ] {
            let id = intern(&mut arena, value);
            assert_eq!(
                boolean_literal_value(&arena, id, &Control::default()).unwrap(),
                None
            );
            assert!(!is_true_literal(&arena, id, &Control::default()).unwrap());
        }
        let value = constant(
            Arc::new(BooleanArray::from(vec![true])),
            ValueLogicalType::Physical,
            0,
        );
        let error = arena
            .intern_observed(
                ScalarNode::Constant(value),
                FunctionValueType::new(DataType::Boolean, false),
                &Control::default(),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            crate::compiler::SqlCompileError::Compilation(_)
        ));
    }

    #[test]
    fn selected_boolean_each_original_callback_refusal_has_no_replay_or_publication() {
        let mut arena = ScalarArena::new();
        let id = intern(
            &mut arena,
            constant(
                Arc::new(BooleanArray::from(vec![false, true])),
                ValueLogicalType::Physical,
                1,
            ),
        );
        let baseline = Control::default();
        assert!(is_true_literal(&arena, id, &baseline).unwrap());
        let trace = baseline.trace.into_inner().unwrap();
        assert_eq!(trace[0], 0);
        assert!(trace.last().copied().unwrap() > 0);
        let nodes = arena.node_count();
        for at in 0..trace.len() {
            for (cause, expected) in [
                (
                    CompileControlError::Cancelled,
                    crate::compiler::SqlCompileError::Cancelled,
                ),
                (
                    CompileControlError::DeadlineExceeded,
                    crate::compiler::SqlCompileError::DeadlineExceeded,
                ),
                (
                    CompileControlError::ResourceExhausted,
                    crate::compiler::SqlCompileError::ResourceExhausted,
                ),
            ] {
                let control = Control {
                    refuse: Some((at, cause)),
                    ..Control::default()
                };
                assert_eq!(is_true_literal(&arena, id, &control).unwrap_err(), expected);
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                assert_eq!(arena.node_count(), nodes);
            }
        }
    }
}
