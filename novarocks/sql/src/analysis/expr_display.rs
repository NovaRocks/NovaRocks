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

//! Observed display labels for analyzed expressions.
//!
//! These labels preserve SQL/path spelling. Expression identity belongs to
//! `expr_identity` and ColumnId; diagnostic text is never a selected-value key.

use crate::analysis::{self as query_ir, BinOp, ExprKind, TypedExpr, UnOp};
use crate::common::negation::{
    negation_delimiters, typed_operand, typed_operand_has_lower_precedence,
};
use crate::compiler::SqlCompileError;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

pub(crate) fn typed_expr_display_name(
    expr: &TypedExpr,
    control: &dyn PureCompileControl,
) -> Result<String, SqlCompileError> {
    render(control, |out| expression(expr, false, out))
}

pub(crate) fn agg_call_display_name_from_parts(
    name: &str,
    args: &[TypedExpr],
    distinct: bool,
    order_by: &[query_ir::SortItem],
    control: &dyn PureCompileControl,
) -> Result<String, SqlCompileError> {
    render(control, |out| {
        aggregate(name, args, distinct, order_by, out)
    })
}

fn render(
    control: &dyn PureCompileControl,
    body: impl FnOnce(&mut DisplayOutput<'_, '_>) -> Result<(), SqlCompileError>,
) -> Result<String, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let mut text = String::new();
    let result = body(&mut DisplayOutput {
        text: &mut text,
        work: &mut work,
    });
    if matches!(
        &result,
        Err(SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted)
    ) {
        return result.map(|()| text);
    }
    work.finish()?;
    result?;
    Ok(text)
}

struct DisplayOutput<'a, 'c> {
    text: &'a mut String,
    work: &'a mut CompileCheckpoints<'c>,
}
impl DisplayOutput<'_, '_> {
    fn append(&mut self, source: &str) -> Result<(), SqlCompileError> {
        self.text
            .len()
            .checked_add(source.len())
            .ok_or(SqlCompileError::ResourceExhausted)?;
        self.text
            .try_reserve(source.len())
            .map_err(|_| SqlCompileError::ResourceExhausted)?;
        let mut start = 0;
        while start < source.len() {
            let mut end = start.saturating_add(256).min(source.len());
            while !source.is_char_boundary(end) {
                end -= 1;
            }
            self.text.push_str(&source[start..end]);
            for _ in start..end {
                self.work.step()?;
            }
            start = end;
        }
        Ok(())
    }
}

fn checked_constant(
    expr: &TypedExpr,
    value: &novarocks_functions::ConstantValue,
    out: &mut DisplayOutput<'_, '_>,
) -> Result<(), SqlCompileError> {
    if !expr
        .value_type
        .exactly_equals_observed::<novarocks_functions::ConstantError>(value.value_type(), || {
            out.work.step().map_err(Into::into)
        })?
    {
        return Err(SqlCompileError::InvalidRequest(
            "constant source differs from its analyzed display type".into(),
        ));
    }
    Ok(())
}

fn constant_label(
    expr: &TypedExpr,
    value: &novarocks_functions::ConstantValue,
    out: &mut DisplayOutput<'_, '_>,
) -> Result<String, SqlCompileError> {
    checked_constant(expr, value, out)?;
    out.work.flush()?;
    let label = crate::constant::format_constant_observed(value, out.work.control())?;
    out.work.flush()?;
    Ok(label)
}

#[recursive::recursive]
fn expression(
    expr: &TypedExpr,
    array_item: bool,
    out: &mut DisplayOutput<'_, '_>,
) -> Result<(), SqlCompileError> {
    out.work.step()?;
    if let Some((operator, [left, right])) = crate::common::binary_operator::typed_operands(expr) {
        with_parens(left, out)?;
        out.append(" ")?;
        out.append(operator.sql())?;
        out.append(" ")?;
        return with_parens(right, out);
    }
    if let Some(operand) = typed_operand(expr) {
        if let ExprKind::Constant(value) = &operand.kind {
            out.work.step()?;
            let label = constant_label(operand, value, out)?;
            let (prefix, suffix) = negation_delimiters(false, &label);
            out.append(prefix)?;
            out.append(&label)?;
            return out.append(suffix);
        }
        let needs_parentheses = typed_operand_has_lower_precedence(operand)
            || typed_operand(operand).is_some()
            || matches!(
                operand.kind,
                ExprKind::Literal(
                    query_ir::LiteralValue::Int(_)
                        | query_ir::LiteralValue::LargeInt(_)
                        | query_ir::LiteralValue::Float(_)
                        | query_ir::LiteralValue::Decimal(_)
                )
            );
        let first_piece = match &operand.kind {
            ExprKind::ColumnRef {
                qualifier, column, ..
            } => qualifier.as_deref().unwrap_or(column),
            ExprKind::LambdaParamRef { name, .. } => name,
            _ => "",
        };
        let (prefix, suffix) = negation_delimiters(needs_parentheses, first_piece);
        out.append(prefix)?;
        expression(operand, false, out)?;
        return out.append(suffix);
    }
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier, column, ..
        } => {
            if let Some(q) = qualifier {
                out.append(q)?;
                out.append(".")?;
            }
            out.append(column)
        }
        ExprKind::LambdaParamRef { name, .. } => out.append(name),
        ExprKind::Literal(value) => match value {
            query_ir::LiteralValue::Null => out.append("NULL"),
            query_ir::LiteralValue::Bool(v) => out.append(if array_item {
                if *v { "true" } else { "false" }
            } else if *v {
                "TRUE"
            } else {
                "FALSE"
            }),
            query_ir::LiteralValue::Int(v) => out.append(&v.to_string()),
            query_ir::LiteralValue::LargeInt(v) => out.append(&v.to_string()),
            query_ir::LiteralValue::Float(v) => out.append(&v.to_string()),
            query_ir::LiteralValue::Decimal(v) => out.append(v),
            query_ir::LiteralValue::String(v) => {
                out.append("'")?;
                out.append(v)?;
                out.append("'")
            }
            query_ir::LiteralValue::Binary(bytes) => {
                out.append("X'")?;
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                for byte in bytes {
                    let pair = [HEX[usize::from(byte >> 4)], HEX[usize::from(byte & 15)]];
                    // Each pair is ASCII, so its UTF-8 conversion is bounded.
                    out.append(std::str::from_utf8(&pair).expect("hex pair is ASCII"))?;
                }
                out.append("'")
            }
        },
        ExprKind::Constant(value) => {
            let label = constant_label(expr, value, out)?;
            // Boolean case is display spelling only, never value identity.
            if !array_item
                && value.value_type().logical_type()
                    == novarocks_type_contract::ValueLogicalType::Physical
                && *value.value_type().data_type() == arrow::datatypes::DataType::Boolean
            {
                out.append(match label.as_str() {
                    "true" => "TRUE",
                    "false" => "FALSE",
                    other => other,
                })
            } else {
                out.append(&label)
            }
        }
        ExprKind::FunctionCall { name, args, .. } if name == "__array_literal" => {
            out.append("[")?;
            expressions(args, ", ", true, out)?;
            out.append("]")
        }
        ExprKind::FunctionCall { name, args, .. } if name == "map" => {
            out.append("map{")?;
            for (index, pair) in args.chunks(2).enumerate() {
                if index > 0 {
                    out.append(",")?;
                }
                expression(&pair[0], true, out)?;
                if let Some(value) = pair.get(1) {
                    out.append(":")?;
                    expression(value, true, out)?;
                }
            }
            out.append("}")
        }
        ExprKind::FunctionCall { name, args, .. } => function(name, args, out),
        ExprKind::LambdaFunction { params, body } => {
            out.append("(")?;
            for (index, param) in params.iter().enumerate() {
                if index > 0 {
                    out.append(", ")?;
                }
                out.append(&param.name)?;
            }
            out.append(") -> ")?;
            expression(body, false, out)
        }
        ExprKind::Lambda { params, body } => {
            out.append("(")?;
            for (index, param) in params.iter().enumerate() {
                if index > 0 {
                    out.append(", ")?;
                }
                out.append(param)?;
            }
            out.append(") -> ")?;
            expression(body, false, out)
        }
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            filter,
            ..
        } => {
            aggregate(name, args, *distinct, order_by, out)?;
            if let Some(predicate) = filter {
                out.append(" FILTER (WHERE ")?;
                expression(predicate, false, out)?;
                out.append(")")?;
            }
            Ok(())
        }
        ExprKind::Cast {
            expr: inner,
            target,
            cast_mode: None,
            ..
        } if matches!(target, arrow::datatypes::DataType::List(_))
            && matches!(&inner.kind, ExprKind::FunctionCall { name, .. } if name == "__array_literal") =>
        {
            expression(inner, false, out)
        }
        ExprKind::Cast {
            expr: inner,
            target,
            cast_mode,
            ..
        } => {
            out.append(
                if *cast_mode == Some(novarocks_type_contract::TrinoCastMode::Try) {
                    "try_cast("
                } else {
                    "cast("
                },
            )?;
            expression(inner, false, out)?;
            out.append(" as ")?;
            // Arrow's type formatter remains opaque. It contains no value pool;
            // observe both boundaries and every copied output byte.
            out.work.flush()?;
            let label = format!("{target:?}");
            out.work.flush()?;
            out.append(&label)?;
            out.append(")")
        }
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => {
            with_parens(inner, out)?;
            out.append(if *negated { " IS NOT NULL" } else { " IS NULL" })
        }
        ExprKind::BinaryOp {
            left, op, right, ..
        } => {
            with_parens(left, out)?;
            out.append(" ")?;
            out.append(bin_op_display(*op))?;
            out.append(" ")?;
            with_parens(right, out)
        }
        ExprKind::UnaryOp { op, expr: inner } => {
            out.append(match op {
                UnOp::Not => "NOT ",
            })?;
            with_parens(inner, out)
        }
        ExprKind::InList {
            expr: inner,
            list,
            negated,
        } => {
            with_parens(inner, out)?;
            out.append(if *negated { " NOT IN (" } else { " IN (" })?;
            expressions(list, ", ", false, out)?;
            out.append(")")
        }
        ExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => {
            with_parens(inner, out)?;
            out.append(if *negated {
                " NOT BETWEEN "
            } else {
                " BETWEEN "
            })?;
            with_parens(low, out)?;
            out.append(" AND ")?;
            with_parens(high, out)
        }
        ExprKind::Like {
            expr: inner,
            pattern,
            negated,
        } => {
            with_parens(inner, out)?;
            out.append(if *negated { " NOT LIKE " } else { " LIKE " })?;
            with_parens(pattern, out)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            out.append("CASE")?;
            if let Some(value) = operand {
                out.append(" ")?;
                expression(value, false, out)?;
            }
            for (when, then) in when_then {
                out.append(" WHEN ")?;
                expression(when, false, out)?;
                out.append(" THEN ")?;
                expression(then, false, out)?;
            }
            if let Some(value) = else_expr {
                out.append(" ELSE ")?;
                expression(value, false, out)?;
            }
            out.append(" END")
        }
        ExprKind::IsTruthValue {
            expr: inner,
            value,
            negated,
        } => {
            with_parens(inner, out)?;
            out.append(if *negated { " IS NOT " } else { " IS " })?;
            out.append(if *value { "TRUE" } else { "FALSE" })
        }
        ExprKind::Nested(inner) => {
            out.append("(")?;
            expression(inner, false, out)?;
            out.append(")")
        }
        ExprKind::WindowCall {
            name,
            args,
            distinct,
            function_order_by,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
            ..
        } => {
            aggregate(name, args, *distinct, function_order_by, out)?;
            if *ignore_nulls {
                out.append(" IGNORE NULLS")?;
            }
            out.append(" OVER (")?;
            if !partition_by.is_empty() {
                out.append("PARTITION BY ")?;
                expressions(partition_by, ", ", false, out)?;
            }
            if !order_by.is_empty() {
                if !partition_by.is_empty() {
                    out.append(" ")?;
                }
                out.append("ORDER BY ")?;
                sorts(order_by, true, false, out)?;
            }
            if let Some(frame) = window_frame {
                if !partition_by.is_empty() || !order_by.is_empty() {
                    out.append(" ")?;
                }
                out.append(match frame.frame_type {
                    query_ir::WindowFrameType::Rows => "ROWS BETWEEN ",
                    query_ir::WindowFrameType::Range => "RANGE BETWEEN ",
                })?;
                bound(&frame.start, out)?;
                out.append(" AND ")?;
                bound(&frame.end, out)?;
            }
            out.append(")")
        }
        ExprKind::SubqueryPlaceholder { id, kind, .. } => {
            // This is an intermediate source address, not an expression key.
            out.append(match kind {
                query_ir::SubqueryKind::Scalar => "SCALAR SUBQUERY ",
                query_ir::SubqueryKind::Exists { negated: false } => "EXISTS SUBQUERY ",
                query_ir::SubqueryKind::Exists { negated: true } => "NOT EXISTS SUBQUERY ",
                query_ir::SubqueryKind::InSubquery { negated: false } => "IN SUBQUERY ",
                query_ir::SubqueryKind::InSubquery { negated: true } => "NOT IN SUBQUERY ",
            })?;
            out.append(&id.to_string())
        }
    }
}

fn expressions(
    args: &[TypedExpr],
    separator: &str,
    array_item: bool,
    out: &mut DisplayOutput<'_, '_>,
) -> Result<(), SqlCompileError> {
    for (index, arg) in args.iter().enumerate() {
        if index > 0 {
            out.append(separator)?;
        }
        expression(arg, array_item, out)?;
    }
    Ok(())
}
fn with_parens(expr: &TypedExpr, out: &mut DisplayOutput<'_, '_>) -> Result<(), SqlCompileError> {
    let parens = crate::common::binary_operator::typed_operands(expr).is_some()
        || !matches!(
            expr.kind,
            ExprKind::ColumnRef { .. }
                | ExprKind::LambdaParamRef { .. }
                | ExprKind::Literal(_)
                | ExprKind::Constant(_)
                | ExprKind::FunctionCall { .. }
                | ExprKind::AggregateCall { .. }
        );
    if parens {
        out.append("(")?;
    }
    expression(expr, false, out)?;
    if parens {
        out.append(")")?;
    }
    Ok(())
}
fn typed_string_literal<'a>(
    expr: &'a TypedExpr,
    out: &mut DisplayOutput<'_, '_>,
) -> Result<Option<&'a str>, SqlCompileError> {
    match &expr.kind {
        ExprKind::Literal(query_ir::LiteralValue::String(value)) => Ok(Some(value)),
        ExprKind::Constant(value) => {
            checked_constant(expr, value, out)?;
            if value.value_type().logical_type()
                != novarocks_type_contract::ValueLogicalType::Physical
                || !matches!(
                    value.value_type().data_type(),
                    arrow::datatypes::DataType::Utf8
                        | arrow::datatypes::DataType::LargeUtf8
                        | arrow::datatypes::DataType::Utf8View
                )
            {
                return Ok(None);
            }
            out.work.flush()?;
            let value =
                value.try_utf8_borrowed_observed(CompilePhase::LowerProgram, out.work.control())?;
            out.work.flush()?;
            Ok(value)
        }
        _ => Ok(None),
    }
}
fn function(
    name: &str,
    args: &[TypedExpr],
    out: &mut DisplayOutput<'_, '_>,
) -> Result<(), SqlCompileError> {
    if name == "__row_field"
        && let [source, ordinal] = args
        && let ExprKind::Literal(query_ir::LiteralValue::Int(ordinal)) = ordinal.kind
    {
        expression(source, false, out)?;
        out.append("[")?;
        out.append(&(ordinal + 1).to_string())?;
        return out.append("]");
    }
    match name {
        "__struct_subfield" | "__array_struct_subfield" if args.len() == 2 => {
            if let Some(field) = typed_string_literal(&args[1], out)? {
                expression(&args[0], false, out)?;
                out.append(".")?;
                out.append(field)
            } else {
                function_fallback(name, args, out)
            }
        }
        "__array_element_at" | "__map_element_at" if args.len() == 2 => {
            expression(&args[0], false, out)?;
            out.append("[")?;
            expression(&args[1], false, out)?;
            out.append("]")
        }
        _ => function_fallback(name, args, out),
    }
}
fn function_fallback(
    name: &str,
    args: &[TypedExpr],
    out: &mut DisplayOutput<'_, '_>,
) -> Result<(), SqlCompileError> {
    out.append(name)?;
    out.append("(")?;
    expressions(args, ", ", false, out)?;
    out.append(")")
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
fn aggregate(
    name: &str,
    args: &[TypedExpr],
    distinct: bool,
    order_by: &[query_ir::SortItem],
    out: &mut DisplayOutput<'_, '_>,
) -> Result<(), SqlCompileError> {
    let group_concat = matches!(name, "group_concat" | "string_agg");
    out.append(canonical_agg_display_name(name))?;
    out.append("(")?;
    if distinct || name == "array_agg_distinct" {
        out.append("DISTINCT ")?;
    }
    let (values, separator) = if group_concat {
        args.split_last()
            .map(|(last, rest)| (rest, Some(last)))
            .unwrap_or((&[], None))
    } else {
        (args, None)
    };
    if !group_concat && values.is_empty() {
        out.append("*")?;
    }
    expressions(
        values,
        if group_concat { "," } else { ", " },
        group_concat,
        out,
    )?;
    let mut visible = false;
    for item in order_by {
        out.work.step()?;
        // Both syntax and materialized constants retain the prior omission of
        // constant aggregate-local order keys from display text.
        if matches!(item.expr.kind, ExprKind::Literal(_) | ExprKind::Constant(_)) {
            if let ExprKind::Constant(value) = &item.expr.kind {
                checked_constant(&item.expr, value, out)?;
            }
            continue;
        }
        if !visible {
            out.append(if group_concat {
                " ORDER BY "
            } else {
                " order by "
            })?;
        } else {
            out.append(", ")?;
        }
        sort(item, group_concat, group_concat, out)?;
        visible = true;
    }
    if group_concat {
        out.append(" SEPARATOR ")?;
        if let Some(separator) = separator {
            expression(separator, true, out)?;
        } else {
            out.append("','")?;
        }
    }
    out.append(")")
}
fn sorts(
    items: &[query_ir::SortItem],
    uppercase: bool,
    array_item: bool,
    out: &mut DisplayOutput<'_, '_>,
) -> Result<(), SqlCompileError> {
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            out.append(", ")?;
        }
        sort(item, uppercase, array_item, out)?;
    }
    Ok(())
}
fn sort(
    item: &query_ir::SortItem,
    uppercase: bool,
    array_item: bool,
    out: &mut DisplayOutput<'_, '_>,
) -> Result<(), SqlCompileError> {
    expression(&item.expr, array_item, out)?;
    out.append(match (uppercase, item.asc) {
        (true, true) => " ASC",
        (true, false) => " DESC",
        (false, true) => " asc",
        (false, false) => " desc",
    })?;
    if item.nulls_first != item.asc {
        out.append(match (uppercase, item.nulls_first) {
            (true, true) => " NULLS FIRST",
            (true, false) => " NULLS LAST",
            (false, true) => " nulls first",
            (false, false) => " nulls last",
        })?;
    }
    Ok(())
}
fn bound(
    bound: &query_ir::WindowBound,
    out: &mut DisplayOutput<'_, '_>,
) -> Result<(), SqlCompileError> {
    match bound {
        query_ir::WindowBound::UnboundedPreceding => out.append("UNBOUNDED PRECEDING"),
        query_ir::WindowBound::UnboundedFollowing => out.append("UNBOUNDED FOLLOWING"),
        query_ir::WindowBound::CurrentRow => out.append("CURRENT ROW"),
        query_ir::WindowBound::Preceding(value) => {
            out.append(&value.to_string())?;
            out.append(" PRECEDING")
        }
        query_ir::WindowBound::Following(value) => {
            out.append(&value.to_string())?;
            out.append(" FOLLOWING")
        }
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

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{
        agg_call_display_name_from_parts as observed_aggregate,
        typed_expr_display_name as observed_display,
    };

    fn typed_expr_display_name(expr: &TypedExpr) -> String {
        observed_display(expr, &crate::compiler::SqlCompileControl::unbounded()).unwrap()
    }
    fn agg_call_display_name_from_parts(
        name: &str,
        args: &[TypedExpr],
        distinct: bool,
        order_by: &[crate::analysis::SortItem],
    ) -> String {
        observed_aggregate(
            name,
            args,
            distinct,
            order_by,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap()
    }
    use crate::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: crate::column_id::ColumnId::UNSET,
                qualifier: None,
                column: name.to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        }
    }

    fn string_lit(value: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(value.to_string())),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
        }
    }

    fn int_lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        }
    }

    fn float_lit(value: f64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Float(value)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Float64, false),
        }
    }

    #[test]
    fn typed_expr_display_name_uses_negation_identity_and_keeps_operand_boundaries() {
        let negate = |operand: TypedExpr| {
            let operator = novarocks_type_contract::OperatorFunction::Negation;
            let ty = operand.value_type.clone();
            let arguments = [novarocks_functions::FunctionArgument::Value {
                value_type: ty.clone(),
                constant: None,
            }];
            let resolved =
                novarocks_functions::builtin::catalogue::builtin_engine_function_catalog()
                    .resolve_bound_trusted_by_id(
                        &operator.function_id(),
                        novarocks_functions::FunctionKind::Scalar,
                        novarocks_functions::FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: arguments.len(),
                            expected_result_type: None,
                        },
                        &crate::compiler::SqlCompileControl::unbounded(),
                    )
                    .unwrap();
            TypedExpr {
                kind: ExprKind::FunctionCall {
                    name: "fixture_display_name".into(),
                    args: vec![operand].into(),
                    distinct: false,
                    volatility: resolved.semantics.volatility,
                    binding: crate::binding::SqlFunctionBinding::new(
                        resolved,
                        novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                    ),
                },
                value_type: ty,
            }
        };
        let addition = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: novarocks_parser::ast::AstBox::new(col("a")),
                right: novarocks_parser::ast::AstBox::new(col("b")),
                op: BinOp::Add,
                decimal_overflow_policy:
                    novarocks_type_contract::DecimalOverflowPolicy::ReportError,
            },
            value_type: col("a").value_type,
        };
        for (operand, expected) in [
            (col("x"), "-x"),
            (addition, "-(a + b)"),
            (negate(col("x")), "-(-x)"),
            (int_lit(1), "-(1)"),
            (int_lit(-1), "-(-1)"),
        ] {
            assert_eq!(typed_expr_display_name(&negate(operand)), expected);
        }
    }

    #[test]
    fn typed_expr_display_name_formats_struct_subfield_like_starrocks() {
        let args = vec![col("c13"), string_lit("a")];
        let expr = TypedExpr {
            kind: ExprKind::FunctionCall {
                binding: crate::analysis::test_function_binding(
                    "__struct_subfield",
                    &args,
                    DataType::Int64,
                    true,
                    crate::functions::FunctionVolatility::Immutable,
                ),
                volatility: crate::functions::builtin_function_volatility("__struct_subfield"),
                name: "__struct_subfield".to_string(),
                args: (args).into(),
                distinct: false,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        };
        assert_eq!(typed_expr_display_name(&expr), "c13.a");
    }

    #[test]
    fn typed_expr_display_name_formats_collection_access_like_starrocks() {
        let args = vec![col("c11"), int_lit(0)];
        let expr = TypedExpr {
            kind: ExprKind::FunctionCall {
                binding: crate::analysis::test_function_binding(
                    "__array_element_at",
                    &args,
                    DataType::Int64,
                    true,
                    crate::functions::FunctionVolatility::Immutable,
                ),
                volatility: crate::functions::builtin_function_volatility("__array_element_at"),
                name: "__array_element_at".to_string(),
                args: (args).into(),
                distinct: false,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        };
        assert_eq!(typed_expr_display_name(&expr), "c11[0]");
    }

    #[test]
    fn typed_expr_display_name_formats_is_not_null_with_inner_parens() {
        let expr = TypedExpr {
            kind: ExprKind::IsNull {
                expr: novarocks_parser::ast::AstBox::new(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: novarocks_parser::ast::AstBox::new(col("v4")),
                        op: BinOp::Add,
                        right: novarocks_parser::ast::AstBox::new(col("v4")),
                        decimal_overflow_policy:
                            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        true,
                    ),
                }),
                negated: true,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        };
        assert_eq!(typed_expr_display_name(&expr), "(v4 + v4) IS NOT NULL");
    }

    #[test]
    fn agg_call_display_name_preserves_struct_field_paths() {
        let args = vec![col("c13"), string_lit("a")];
        let arg = TypedExpr {
            kind: ExprKind::FunctionCall {
                binding: crate::analysis::test_function_binding(
                    "__struct_subfield",
                    &args,
                    DataType::Int64,
                    true,
                    crate::functions::FunctionVolatility::Immutable,
                ),
                volatility: crate::functions::builtin_function_volatility("__struct_subfield"),
                name: "__struct_subfield".to_string(),
                args: (args).into(),
                distinct: false,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        };
        assert_eq!(
            agg_call_display_name_from_parts(
                "percentile_approx_weighted",
                &[arg, col("c1"), float_lit(0.5)],
                false,
                &[],
            ),
            "percentile_approx_weighted(c13.a, c1, 0.5)"
        );
    }

    #[test]
    fn agg_call_display_name_preserves_array_unique_agg_name() {
        assert_eq!(
            agg_call_display_name_from_parts("array_unique_agg", &[col("s_1")], false, &[]),
            "array_unique_agg(s_1)"
        );
    }
}

#[cfg(test)]
#[path = "expr_display/constant_tests.rs"]
mod constant_tests;
