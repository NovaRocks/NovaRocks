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

use super::*;

use std::collections::{BTreeMap, BTreeSet};

use arrow_schema::{DataType, IntervalUnit, TimeUnit};

use crate::{
    AggregatePhase, ExprId, ExprKind, Fragment, FunctionKind, NodeId, NodeKind, ValueOrigin,
    ValueType,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExpressionParentRole {
    BoundLambdaArgument,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExpressionParentReference {
    pub(crate) parent: ExprId,
    pub(crate) role: ExpressionParentRole,
}

pub(crate) fn expression_parent_references(
    parent: &crate::ExprNode,
    output: &mut Vec<(ExprId, ExpressionParentRole)>,
) {
    match &parent.kind {
        ExprKind::FunctionCall { function, args } => {
            for (ordinal, argument) in args.iter().copied().enumerate() {
                let role = if matches!(
                    function.argument_types.get(ordinal),
                    Some(crate::FunctionArgumentType::Lambda { .. })
                ) {
                    ExpressionParentRole::BoundLambdaArgument
                } else {
                    ExpressionParentRole::Other
                };
                output.push((argument, role));
            }
        }
        ExprKind::WindowCall {
            function,
            args,
            function_order_by,
            frame,
            ..
        } => {
            for (ordinal, argument) in args.iter().copied().enumerate() {
                let role = if matches!(
                    function.argument_types.get(ordinal),
                    Some(crate::FunctionArgumentType::Lambda { .. })
                ) {
                    ExpressionParentRole::BoundLambdaArgument
                } else {
                    ExpressionParentRole::Other
                };
                output.push((argument, role));
            }
            output.extend(
                function_order_by
                    .iter()
                    .map(|item| (item.expr, ExpressionParentRole::Other)),
            );
            if let Some(frame) = frame {
                for bound in [&frame.start, &frame.end] {
                    if let crate::WindowBound::Preceding(expression)
                    | crate::WindowBound::Following(expression) = bound
                    {
                        output.push((*expression, ExpressionParentRole::Other));
                    }
                }
            }
        }
        _ => {
            let mut references = Vec::new();
            parent.kind.expression_references(&mut references);
            output.extend(
                references
                    .into_iter()
                    .map(|reference| (reference, ExpressionParentRole::Other)),
            );
        }
    }
}

pub(crate) fn validate_expression(
    fragment: &Fragment,
    expression: &crate::ExprNode,
    expression_scopes: &BTreeMap<NodeId, VisibleInputIndex>,
    window_roots: &BTreeSet<ExprId>,
    operator_roots: &BTreeSet<ExprId>,
    parents: &[ExpressionParentReference],
    errors: &mut ValidationContext,
) {
    let path = format!(
        "fragments[{}].expressions[{}]",
        fragment.id().get(),
        expression.id.get()
    );
    let owner = fragment.nodes().get(&expression.owner);
    if owner.is_none() {
        errors.push(ValidationError::new(
            &path,
            format!("owner node {} is not defined", expression.owner.get()),
        ));
    }
    if let Some(scope) = expression.lambda_scope {
        match fragment.expressions().get(scope) {
            Some(scope_node)
                if scope_node.owner == expression.owner
                    && matches!(scope_node.kind, ExprKind::Lambda { .. }) => {}
            _ => errors.push(ValidationError::new(
                &path,
                "expression lambda scope is not a lambda owned by the same physical node",
            )),
        }
    }
    let mut references = Vec::new();
    expression.kind.expression_references(&mut references);
    for reference in references {
        match fragment.expressions().get(reference) {
            Some(child) if child.owner != expression.owner => errors.push(ValidationError::new(
                &path,
                "expression dependency crosses a physical-node scope",
            )),
            Some(child)
                if !expression_reference_scope_allowed(fragment, expression, reference, child) =>
            {
                errors.push(ValidationError::new(
                    &path,
                    "expression dependency crosses a lambda lexical scope",
                ));
            }
            Some(_) => {}
            None => errors.push(ValidationError::new(
                &path,
                format!("expression {} is not defined", reference.get()),
            )),
        }
    }
    match &expression.kind {
        ExprKind::Value(value) => match fragment.values().get(value) {
            Some(definition) if definition.ty != expression.ty => {
                errors.push(ValidationError::new(
                    &path,
                    "value-reference type differs from the value definition",
                ))
            }
            Some(_) => {
                if owner.is_some()
                    && expression_scopes
                        .get(&expression.owner)
                        .is_some_and(|scope| !scope.contains(value))
                {
                    errors.push(ValidationError::new(
                        &path,
                        "value reference is outside its owner node input scope",
                    ));
                }
            }
            None => errors.push(ValidationError::new(
                &path,
                format!("value {} is not defined", value.get()),
            )),
        },
        ExprKind::Literal(literal) => validate_literal_type(literal, &expression.ty, &path, errors),
        ExprKind::LambdaParameter { lambda, ordinal } => {
            match fragment.expressions().get(*lambda) {
                Some(crate::ExprNode {
                    kind:
                        ExprKind::Lambda {
                            parameter_types, ..
                        },
                    ..
                }) if usize::try_from(*ordinal)
                    .ok()
                    .and_then(|ordinal| parameter_types.get(ordinal))
                    == Some(&expression.ty) => {}
                Some(_) => errors.push(ValidationError::new(
                    &path,
                    "lambda parameter does not reference a matching lambda owner",
                )),
                None => errors.push(ValidationError::new(
                    &path,
                    "lambda parameter owner is not defined",
                )),
            }
            // A parameter is written inside its own lambda, or inside one
            // nested in it: `array_map(x -> array_map(y -> x + y, ys), xs)`
            // reads `x` from the scope the inner lambda opens. So the lambda
            // it names is the scope it stands in or one enclosing that.
            if !lambda_scope_contains(fragment, expression.lambda_scope, *lambda) {
                errors.push(ValidationError::new(
                    &path,
                    "lambda parameter stands outside the lambda it names",
                ));
            }
        }
        ExprKind::Lambda {
            parameter_types,
            body,
        } => {
            let invalid_parent = parents
                .iter()
                .find(|parent| parent.role != ExpressionParentRole::BoundLambdaArgument);
            if operator_roots.contains(&expression.id)
                || parents.is_empty()
                || invalid_parent.is_some()
            {
                let detail = invalid_parent.map_or_else(String::new, |parent| {
                    format!(
                        "; expression {} references it outside a bound lambda argument position",
                        parent.parent.get()
                    )
                });
                errors.push(ValidationError::new(
                    &path,
                    format!(
                        "lambda must appear only as an exact bound-function lambda argument{detail}"
                    ),
                ));
            }
            if parameter_types.is_empty() {
                errors.push(ValidationError::new(&path, "lambda has no parameters"));
            }
            if let Some(body) = fragment.expressions().get(*body)
                && body.ty != expression.ty
            {
                errors.push(ValidationError::new(
                    &path,
                    "lambda type differs from its body type",
                ));
            }
        }
        ExprKind::Unary { op, expr } => {
            if let Some(input) = fragment.expressions().get(*expr) {
                // An operator answers in its operand's type and may admit null
                // its operand does not -- negating the smallest integer has no
                // answer, and a plan's nullability widens on the way out. It
                // may not admit less.
                let widens = expression.ty.nullable || !input.ty.nullable;
                let valid = match op {
                    crate::UnaryOperator::Plus | crate::UnaryOperator::Minus => {
                        is_numeric(&input.ty.data_type)
                            && input.ty.data_type == expression.ty.data_type
                            && widens
                    }
                    crate::UnaryOperator::Not => {
                        input.ty.data_type == DataType::Boolean
                            && expression.ty.data_type == DataType::Boolean
                            && widens
                    }
                    crate::UnaryOperator::BitwiseNot => {
                        is_integer(&input.ty.data_type)
                            && input.ty.data_type == expression.ty.data_type
                            && widens
                    }
                };
                if !valid {
                    errors.push(ValidationError::new(
                        &path,
                        "unary expression types are inconsistent with its operator",
                    ));
                }
            }
        }
        ExprKind::Binary { left, op, right } => {
            if let (Some(left), Some(right)) = (
                fragment.expressions().get(*left),
                fragment.expressions().get(*right),
            ) {
                validate_binary_types(left, *op, right, expression, &path, errors);
            }
        }
        ExprKind::Conjunction { args } | ExprKind::Disjunction { args } => {
            validate_boolean_connective_types(fragment, args, expression, &path, errors);
        }
        ExprKind::FunctionCall { function, args } => {
            if function.kind != FunctionKind::Scalar {
                errors.push(ValidationError::new(
                    &path,
                    "scalar call has non-scalar binding",
                ));
            }
            validate_function_call(fragment, expression, function, args, &path, errors);
        }
        ExprKind::WindowCall {
            function,
            distinct,
            args,
            function_order_by,
            frame,
            aggregate_binding,
            ..
        } => {
            if !window_roots.contains(&expression.id)
                || !parents.is_empty()
                || expression.lambda_scope.is_some()
            {
                let detail = parents.first().map_or_else(String::new, |parent| {
                    format!("; expression {} also references it", parent.parent.get())
                });
                errors.push(ValidationError::new(
                    &path,
                    format!(
                        "window call must be a top-level expression of its owning Window node{detail}"
                    ),
                ));
            }
            match function.kind {
                FunctionKind::Window => {
                    if aggregate_binding.is_some() || *distinct || !function_order_by.is_empty() {
                        errors.push(ValidationError::new(
                            &path,
                            "window function carries aggregate-only semantics",
                        ));
                    }
                    validate_function_call(fragment, expression, function, args, &path, errors);
                }
                FunctionKind::Aggregate => match aggregate_binding {
                    Some(binding)
                        if binding.function == *function
                            && binding.phase == AggregatePhase::Single =>
                    {
                        validate_aggregate_arguments(
                            fragment,
                            binding,
                            args,
                            function_order_by,
                            &path,
                            errors,
                        );
                        if expression.ty != function.result_type {
                            errors.push(ValidationError::new(
                                &path,
                                "aggregate window type differs from its bound result type",
                            ));
                        }
                    }
                    _ => errors.push(ValidationError::new(
                        &path,
                        "aggregate window requires the exact single-phase aggregate binding",
                    )),
                },
                _ => errors.push(ValidationError::new(
                    &path,
                    "window call has invalid function kind",
                )),
            }
            if let Some(frame) = frame {
                validate_window_frame(fragment, frame, &path, errors);
            }
        }
        ExprKind::Cast { expr, target } => {
            // A cast names the type it produces, and may admit null where its
            // input does not -- the conversion itself can fail, and the
            // statement may stand this value where null is admitted. It may
            // not claim the reverse: a null-admitting input does not stop
            // admitting null by being converted.
            if &expression.ty.data_type != target {
                // Both types, because which half drifted is the diagnosis.
                errors.push(ValidationError::new(
                    &path,
                    format!(
                        "cast result type {:?} differs from its target {target:?}",
                        expression.ty.data_type
                    ),
                ));
            } else if fragment
                .expressions()
                .get(*expr)
                .is_some_and(|input| input.ty.nullable && !expression.ty.nullable)
            {
                errors.push(ValidationError::new(
                    &path,
                    "cast stops admitting null its input admits",
                ));
            }
        }
        ExprKind::IsNull { .. } | ExprKind::IsTruthValue { .. } => {
            require_non_nullable_boolean(&expression.ty, &path, errors);
        }
        ExprKind::InList { expr, list, .. } => {
            require_boolean_result(&expression.ty, &path, errors);
            if let Some(input) = fragment.expressions().get(*expr) {
                let nullable = input.ty.nullable
                    || list.iter().any(|candidate| {
                        fragment
                            .expressions()
                            .get(*candidate)
                            .is_some_and(|candidate| candidate.ty.nullable)
                    });
                if nullable && !expression.ty.nullable {
                    errors.push(ValidationError::new(
                        &path,
                        "IN-list result stops admitting null its operands admit",
                    ));
                }
                for candidate in list {
                    if fragment
                        .expressions()
                        .get(*candidate)
                        .is_some_and(|candidate| candidate.ty.data_type != input.ty.data_type)
                    {
                        errors.push(ValidationError::new(
                            &path,
                            "IN-list candidate type differs from its input",
                        ));
                    }
                }
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            require_boolean_result(&expression.ty, &path, errors);
            if let Some(input) = fragment.expressions().get(*expr) {
                let nullable = [*low, *high].into_iter().any(|bound| {
                    fragment
                        .expressions()
                        .get(bound)
                        .is_some_and(|bound| bound.ty.nullable)
                }) || input.ty.nullable;
                if nullable && !expression.ty.nullable {
                    errors.push(ValidationError::new(
                        &path,
                        "BETWEEN result stops admitting null its operands admit",
                    ));
                }
                for bound in [*low, *high] {
                    if fragment
                        .expressions()
                        .get(bound)
                        .is_some_and(|bound| bound.ty.data_type != input.ty.data_type)
                    {
                        errors.push(ValidationError::new(
                            &path,
                            "BETWEEN bound type differs from its input",
                        ));
                    }
                }
            }
        }
        ExprKind::Like { expr, pattern, .. } => {
            require_boolean_result(&expression.ty, &path, errors);
            let nullable = [*expr, *pattern].into_iter().any(|id| {
                fragment
                    .expressions()
                    .get(id)
                    .is_some_and(|value| value.ty.nullable)
            });
            if nullable && !expression.ty.nullable {
                errors.push(ValidationError::new(
                    &path,
                    "LIKE result stops admitting null its operands admit",
                ));
            }
            if [*expr, *pattern].into_iter().any(|id| {
                fragment
                    .expressions()
                    .get(id)
                    .is_some_and(|value| !is_utf8(&value.ty.data_type))
            }) {
                errors.push(ValidationError::new(
                    &path,
                    "LIKE operands must use a UTF-8 type",
                ));
            }
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => validate_case_types(
            fragment, expression, *operand, when_then, *else_expr, &path, errors,
        ),
    }
}

/// A literal's declared type.
///
/// The value itself is exact and never null, so the type only has to name it:
/// declaring it where null is admitted widens it, which is sound, and is how
/// an exact value stands in a position the statement types conservatively.
/// Declaring the null literal as non-nullable is the narrowing, and is the
/// one refused here.
pub(crate) fn validate_literal_type(
    literal: &crate::LiteralValue,
    ty: &ValueType,
    path: &str,
    errors: &mut ValidationContext,
) {
    let valid = match literal {
        crate::LiteralValue::Null => ty.nullable,
        crate::LiteralValue::Boolean(_) => ty.data_type == DataType::Boolean,
        crate::LiteralValue::Int64(_) => ty.data_type == DataType::Int64,
        crate::LiteralValue::UInt64(_) => ty.data_type == DataType::UInt64,
        crate::LiteralValue::Float64Bits(_) => ty.data_type == DataType::Float64,
        crate::LiteralValue::LargeInt(_) => {
            novarocks_type_contract::is_largeint_data_type(&ty.data_type)
        }
        crate::LiteralValue::Decimal128(_) => {
            matches!(ty.data_type, DataType::Decimal128(_, _))
        }
        crate::LiteralValue::Decimal256(_) => {
            matches!(ty.data_type, DataType::Decimal256(_, _))
        }
        crate::LiteralValue::Utf8(_) => is_utf8(&ty.data_type),
        crate::LiteralValue::Binary(_) => {
            matches!(
                ty.data_type,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            )
        }
        crate::LiteralValue::Date32(_) => ty.data_type == DataType::Date32,
        crate::LiteralValue::Time64(_) => {
            matches!(
                ty.data_type,
                DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
            )
        }
        crate::LiteralValue::Timestamp(_) => {
            matches!(ty.data_type, DataType::Timestamp(_, _))
        }
        crate::LiteralValue::IntervalMonthDayNano(_) => {
            ty.data_type == DataType::Interval(IntervalUnit::MonthDayNano)
        }
    };
    if !valid {
        // Which literal and which type: the two together are the whole
        // diagnosis, and a plan has many literals.
        errors.push(ValidationError::new(
            path,
            format!(
                "literal {} differs from its declared type {:?}",
                literal_kind_name(literal),
                ty.data_type
            ),
        ));
    }
}

/// What a literal says it is, for a message that names it.
const fn literal_kind_name(literal: &crate::LiteralValue) -> &'static str {
    match literal {
        crate::LiteralValue::Null => "NULL",
        crate::LiteralValue::Boolean(_) => "boolean",
        crate::LiteralValue::Int64(_) => "int64",
        crate::LiteralValue::UInt64(_) => "uint64",
        crate::LiteralValue::Float64Bits(_) => "float64",
        crate::LiteralValue::LargeInt(_) => "largeint",
        crate::LiteralValue::Decimal128(_) => "decimal128",
        crate::LiteralValue::Decimal256(_) => "decimal256",
        crate::LiteralValue::Utf8(_) => "utf8",
        crate::LiteralValue::Binary(_) => "binary",
        crate::LiteralValue::Date32(_) => "date32",
        crate::LiteralValue::Time64(_) => "time64",
        crate::LiteralValue::Timestamp(_) => "timestamp",
        crate::LiteralValue::IntervalMonthDayNano(_) => "interval",
    }
}

pub(crate) fn expression_reference_scope_allowed(
    fragment: &Fragment,
    parent: &crate::ExprNode,
    child_id: ExprId,
    child: &crate::ExprNode,
) -> bool {
    if matches!(parent.kind, ExprKind::Lambda { body, .. } if body == child_id) {
        return child.lambda_scope == Some(parent.id);
    }
    if let ExprKind::LambdaParameter { lambda, .. } = child.kind {
        return lambda_scope_contains(fragment, parent.lambda_scope, lambda);
    }
    child.lambda_scope == parent.lambda_scope
}

pub(crate) fn lambda_scope_contains(
    fragment: &Fragment,
    mut scope: Option<ExprId>,
    expected: ExprId,
) -> bool {
    let mut visited = BTreeSet::new();
    while let Some(id) = scope {
        if id == expected {
            return true;
        }
        if !visited.insert(id) {
            return false;
        }
        scope = fragment
            .expressions()
            .get(id)
            .and_then(|expression| expression.lambda_scope);
    }
    false
}

/// Types an n-ary `AND`/`OR`.
///
/// Arity is checked here rather than left open: a connective over fewer than
/// two arguments would give the same predicate two spellings, and the point of
/// making the connective n-ary is to have exactly one.
pub(crate) fn validate_boolean_connective_types(
    fragment: &Fragment,
    args: &[ExprId],
    output: &crate::ExprNode,
    path: &str,
    errors: &mut ValidationContext,
) {
    if args.len() < 2 {
        errors.push(ValidationError::new(
            path,
            "boolean connective requires at least two arguments",
        ));
        return;
    }
    let mut nullable = false;
    for (ordinal, arg) in args.iter().enumerate() {
        let Some(arg) = fragment.expressions().get(*arg) else {
            continue;
        };
        if arg.ty.data_type != DataType::Boolean {
            errors.push(ValidationError::new(
                path,
                format!("boolean connective argument {ordinal} is not boolean"),
            ));
        }
        nullable |= arg.ty.nullable;
    }
    // Three-valued `AND` and `OR` answer null where an argument does, and a
    // plan's nullability widens on the way out. What they may not do is
    // answer less than their arguments admit.
    if output.ty.data_type != DataType::Boolean || (nullable && !output.ty.nullable) {
        errors.push(ValidationError::new(
            path,
            "boolean connective result type is inconsistent with its arguments",
        ));
    }
}

pub(crate) fn validate_binary_types(
    left: &crate::ExprNode,
    op: crate::BinaryOperator,
    right: &crate::ExprNode,
    output: &crate::ExprNode,
    path: &str,
    errors: &mut ValidationContext,
) {
    let same_inputs = left.ty.data_type == right.ty.data_type;
    // An operator may admit null neither operand does -- arithmetic answers
    // with null where it cannot answer with a number, and a plan's nullability
    // widens on the way out. It may not admit less than its operands do.
    let nullable = left.ty.nullable || right.ty.nullable;
    let nullability_widens = |output: bool| output || !nullable;
    let valid = match op {
        crate::BinaryOperator::Add
        | crate::BinaryOperator::Subtract
        | crate::BinaryOperator::Multiply
        | crate::BinaryOperator::Divide
        | crate::BinaryOperator::Modulo => {
            let operation = match op {
                crate::BinaryOperator::Add => novarocks_type_contract::ArithmeticOperator::Add,
                crate::BinaryOperator::Subtract => {
                    novarocks_type_contract::ArithmeticOperator::Subtract
                }
                crate::BinaryOperator::Multiply => {
                    novarocks_type_contract::ArithmeticOperator::Multiply
                }
                crate::BinaryOperator::Divide => {
                    novarocks_type_contract::ArithmeticOperator::Divide
                }
                crate::BinaryOperator::Modulo => {
                    novarocks_type_contract::ArithmeticOperator::Modulo
                }
                _ => unreachable!(),
            };
            novarocks_type_contract::arithmetic_result_type_with_op(
                &left.ty.data_type,
                &right.ty.data_type,
                operation,
            )
            .as_ref()
            .is_some_and(|expected| expected == &output.ty.data_type)
                && nullability_widens(output.ty.nullable)
        }
        crate::BinaryOperator::Eq
        | crate::BinaryOperator::NotEq
        | crate::BinaryOperator::Lt
        | crate::BinaryOperator::LtEq
        | crate::BinaryOperator::Gt
        | crate::BinaryOperator::GtEq => {
            same_inputs
                && output.ty.data_type == DataType::Boolean
                && nullability_widens(output.ty.nullable)
        }
        crate::BinaryOperator::EqForNull => {
            same_inputs && output.ty.data_type == DataType::Boolean && !output.ty.nullable
        }
        crate::BinaryOperator::BitAnd
        | crate::BinaryOperator::BitOr
        | crate::BinaryOperator::BitXor => {
            same_inputs
                && is_integer(&left.ty.data_type)
                && output.ty.data_type == left.ty.data_type
                && nullability_widens(output.ty.nullable)
        }
    };
    if !valid {
        errors.push(ValidationError::new(
            path,
            "binary expression types are inconsistent with its operator",
        ));
    }
}

pub(crate) fn validate_case_types(
    fragment: &Fragment,
    expression: &crate::ExprNode,
    operand: Option<ExprId>,
    when_then: &[(ExprId, ExprId)],
    else_expr: Option<ExprId>,
    path: &str,
    errors: &mut ValidationContext,
) {
    if when_then.is_empty() {
        errors.push(ValidationError::new(
            path,
            "CASE expression has no branches",
        ));
    }
    let operand_type = operand.and_then(|id| fragment.expressions().get(id).map(|node| &node.ty));
    for (when, then) in when_then {
        if let Some(when) = fragment.expressions().get(*when) {
            let valid = operand_type
                .map(|operand| operand.data_type == when.ty.data_type)
                .unwrap_or(when.ty.data_type == DataType::Boolean);
            if !valid {
                errors.push(ValidationError::new(
                    path,
                    match operand_type {
                        Some(operand) => format!(
                            "CASE compares {:?} against a branch of {:?}",
                            operand.data_type, when.ty.data_type
                        ),
                        None => format!("CASE condition is {:?}, not a boolean", when.ty.data_type),
                    },
                ));
            }
        }
        if fragment
            .expressions()
            .get(*then)
            .is_some_and(|then| then.ty.data_type != expression.ty.data_type)
        {
            errors.push(ValidationError::new(
                path,
                "CASE result type differs from its output",
            ));
        }
    }
    if let Some(else_expr) = else_expr
        && fragment
            .expressions()
            .get(else_expr)
            .is_some_and(|otherwise| otherwise.ty.data_type != expression.ty.data_type)
    {
        errors.push(ValidationError::new(
            path,
            "CASE ELSE type differs from its output",
        ));
    }
    let result_nullable = else_expr.is_none()
        || when_then.iter().any(|(_, then)| {
            fragment
                .expressions()
                .get(*then)
                .is_some_and(|then| then.ty.nullable)
        })
        || else_expr.is_some_and(|otherwise| {
            fragment
                .expressions()
                .get(otherwise)
                .is_some_and(|otherwise| otherwise.ty.nullable)
        });
    if result_nullable && !expression.ty.nullable {
        errors.push(ValidationError::new(
            path,
            "CASE result stops admitting null its branches admit",
        ));
    }
}

pub(crate) fn validate_window_frame(
    fragment: &Fragment,
    frame: &crate::WindowFrame,
    path: &str,
    errors: &mut ValidationContext,
) {
    if matches!(frame.start, crate::WindowBound::UnboundedFollowing)
        || matches!(frame.end, crate::WindowBound::UnboundedPreceding)
        || window_bound_position(fragment, &frame.start)
            > window_bound_position(fragment, &frame.end)
    {
        errors.push(ValidationError::new(
            path,
            "window frame start follows its end",
        ));
    }
    for bound in [&frame.start, &frame.end] {
        let expression = match bound {
            crate::WindowBound::Preceding(expression)
            | crate::WindowBound::Following(expression) => Some(*expression),
            crate::WindowBound::UnboundedPreceding
            | crate::WindowBound::CurrentRow
            | crate::WindowBound::UnboundedFollowing => None,
        };
        let Some(expression) = expression else {
            continue;
        };
        if window_row_offset(fragment, expression) == Some(0) {
            errors.push(ValidationError::new(
                path,
                "zero window-frame offset must be canonicalized to CURRENT ROW",
            ));
        }
        let valid = fragment
            .expressions()
            .get(expression)
            .is_some_and(|expression| {
                !expression.ty.nullable
                    && (frame.units == crate::WindowFrameUnits::Range
                        || window_offset_literal_is_valid(&expression.kind, frame.units))
            });
        if !valid {
            errors.push(ValidationError::new(
                path,
                "window frame offset must be a non-negative non-null literal of the required domain",
            ));
        }
    }
    if frame.units == crate::WindowFrameUnits::Range && frame_has_offset(frame) {
        errors.push(ValidationError::new(
            path,
            "RANGE window offsets require typed order-key arithmetic not represented by contract revision 1",
        ));
    }
    let offsets_are_ordered = match (&frame.start, &frame.end) {
        (crate::WindowBound::Preceding(start), crate::WindowBound::Preceding(end)) => {
            window_row_offset(fragment, *start)
                .zip(window_row_offset(fragment, *end))
                .is_none_or(|(start, end)| start >= end)
        }
        (crate::WindowBound::Following(start), crate::WindowBound::Following(end)) => {
            window_row_offset(fragment, *start)
                .zip(window_row_offset(fragment, *end))
                .is_none_or(|(start, end)| start <= end)
        }
        _ => true,
    };
    if !offsets_are_ordered {
        errors.push(ValidationError::new(
            path,
            "window frame start follows its end after comparing exact offsets",
        ));
    }
}

pub(crate) fn frame_has_offset(frame: &crate::WindowFrame) -> bool {
    [&frame.start, &frame.end].iter().any(|bound| {
        matches!(
            bound,
            crate::WindowBound::Preceding(_) | crate::WindowBound::Following(_)
        )
    })
}

pub(crate) fn window_bound_position(fragment: &Fragment, bound: &crate::WindowBound) -> u8 {
    match bound {
        crate::WindowBound::UnboundedPreceding => 0,
        crate::WindowBound::Preceding(expression)
            if window_row_offset(fragment, *expression) == Some(0) =>
        {
            2
        }
        crate::WindowBound::Preceding(_) => 1,
        crate::WindowBound::CurrentRow => 2,
        crate::WindowBound::Following(expression)
            if window_row_offset(fragment, *expression) == Some(0) =>
        {
            2
        }
        crate::WindowBound::Following(_) => 3,
        crate::WindowBound::UnboundedFollowing => 4,
    }
}

pub(crate) fn window_offset_literal_is_valid(
    kind: &ExprKind,
    units: crate::WindowFrameUnits,
) -> bool {
    match (units, kind) {
        (
            crate::WindowFrameUnits::Rows | crate::WindowFrameUnits::Groups,
            ExprKind::Literal(crate::LiteralValue::UInt64(_)),
        ) => true,
        (
            crate::WindowFrameUnits::Rows | crate::WindowFrameUnits::Groups,
            ExprKind::Literal(crate::LiteralValue::Int64(value)),
        ) => *value >= 0,
        _ => false,
    }
}

pub(crate) fn window_row_offset(fragment: &Fragment, expression: ExprId) -> Option<u64> {
    match &fragment.expressions().get(expression)?.kind {
        ExprKind::Literal(crate::LiteralValue::UInt64(value)) => Some(*value),
        ExprKind::Literal(crate::LiteralValue::Int64(value)) => u64::try_from(*value).ok(),
        _ => None,
    }
}

pub(crate) fn require_boolean_result(ty: &ValueType, path: &str, errors: &mut ValidationContext) {
    if ty.data_type != DataType::Boolean {
        errors.push(ValidationError::new(
            path,
            "expression result is not Boolean",
        ));
    }
}

pub(crate) fn require_non_nullable_boolean(
    ty: &ValueType,
    path: &str,
    errors: &mut ValidationContext,
) {
    if ty.data_type != DataType::Boolean || ty.nullable {
        errors.push(ValidationError::new(
            path,
            "expression result must be non-nullable Boolean",
        ));
    }
}

pub(crate) fn is_utf8(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

pub(crate) fn is_integer(ty: &DataType) -> bool {
    // LARGEINT is an integer that happens to need sixteen bytes; it is stored
    // as fixed-size binary, and a rule that reads the storage instead of the
    // type refuses `-x` and `~x` on it.
    novarocks_type_contract::is_largeint_data_type(ty)
        || matches!(
            ty,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        )
}

pub(crate) fn is_numeric(ty: &DataType) -> bool {
    is_integer(ty)
        || matches!(
            ty,
            DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        )
}

pub(crate) fn validate_function_call(
    fragment: &Fragment,
    expression: &crate::ExprNode,
    function: &crate::BoundFunction,
    args: &[ExprId],
    path: &str,
    errors: &mut ValidationContext,
) {
    if !function
        .intrinsic_row_error
        .is_valid_for_kind(function.kind)
    {
        errors.push(ValidationError::new(
            path,
            "bound function intrinsic row-error fact differs from its kind",
        ));
    }
    validate_function_arguments(
        fragment,
        &function.function_id,
        &function.argument_types,
        args,
        path,
        errors,
    );
    if expression.ty != function.result_type {
        errors.push(ValidationError::new(
            path,
            "function expression type differs from the bound result type",
        ));
    }
}

pub(crate) fn validate_function_arguments(
    fragment: &Fragment,
    function: &crate::FunctionId,
    expected: &[crate::FunctionArgumentType],
    args: &[ExprId],
    path: &str,
    errors: &mut ValidationContext,
) {
    if expected.len() != args.len() {
        errors.push(ValidationError::new(
            path,
            format!(
                "bound function expects {} arguments, got {}",
                expected.len(),
                args.len()
            ),
        ));
        return;
    }
    for (ordinal, (expected, argument)) in expected.iter().zip(args).enumerate() {
        let valid = fragment.expressions().get(*argument).is_some_and(|actual| {
            match (expected, &actual.kind) {
                (crate::FunctionArgumentType::Value(_), ExprKind::Lambda { .. }) => false,
                // A parameter that accepts null accepts a value that never
                // writes one; the mismatch is the other way round.
                (crate::FunctionArgumentType::Value(expected), _) => {
                    novarocks_type_contract::fits_nested_nullability(
                        &actual.ty.data_type,
                        &expected.data_type,
                    ) && (expected.nullable || !actual.ty.nullable)
                }
                (
                    crate::FunctionArgumentType::Lambda {
                        parameter_types,
                        result_type,
                    },
                    ExprKind::Lambda {
                        parameter_types: actual_parameters,
                        ..
                    },
                ) => actual_parameters == parameter_types && &actual.ty == result_type,
                (crate::FunctionArgumentType::Lambda { .. }, _) => false,
            }
        });
        if !valid {
            let actual = fragment
                .expressions()
                .get(*argument)
                .map_or_else(|| "absent".to_string(), |actual| format!("{:?}", actual.ty));
            errors.push(ValidationError::new(
                path,
                format!(
                    "function `{}` argument {ordinal} shape differs from its bound signature: bound {expected:?}, got {actual}",
                    function.as_str()
                ),
            ));
        }
    }
}

pub(crate) fn validate_expression_acyclic(fragment: &Fragment, errors: &mut ValidationContext) {
    let path = format!("fragments[{}].expressions", fragment.id().get());
    let mut remaining_dependencies = BTreeMap::new();
    let mut dependents: BTreeMap<ExprId, Vec<ExprId>> = BTreeMap::new();
    let mut depths = BTreeMap::new();
    let mut ready = Vec::new();

    for (id, expression) in fragment.expressions().iter() {
        let mut references = Vec::new();
        expression.kind.expression_references(&mut references);
        references.retain(|reference| fragment.expressions().get(*reference).is_some());
        references.sort_unstable();
        references.dedup();
        remaining_dependencies.insert(*id, references.len());
        if references.is_empty() {
            ready.push(*id);
            depths.insert(*id, 1_usize);
        }
        for reference in references {
            dependents.entry(reference).or_default().push(*id);
        }
    }

    let mut processed = 0_usize;
    while let Some(id) = ready.pop() {
        processed += 1;
        let depth = depths.get(&id).copied().unwrap_or(1);
        if let Some(users) = dependents.get(&id) {
            for user in users {
                let candidate = depth.saturating_add(1);
                depths
                    .entry(*user)
                    .and_modify(|current| *current = (*current).max(candidate))
                    .or_insert(candidate);
                if let Some(remaining) = remaining_dependencies.get_mut(user) {
                    *remaining -= 1;
                    if *remaining == 0 {
                        ready.push(*user);
                    }
                }
            }
        }
    }

    if processed != fragment.expressions().len() {
        errors.push(ValidationError::new(
            &path,
            "expression graph contains a cycle",
        ));
    }
    if depths.values().copied().max().unwrap_or(0) > errors.limits().expression_semantic_depth {
        errors.push(ValidationError::resource_limit(
            &path,
            format!(
                "expression semantic depth exceeds {}",
                errors.limits().expression_semantic_depth
            ),
        ));
    }
}

pub(crate) fn validate_lambda_scope_acyclic(fragment: &Fragment, errors: &mut ValidationContext) {
    let path = format!("fragments[{}].expressions", fragment.id().get());
    let mut complete = BTreeSet::new();
    for (start, _) in fragment.expressions().iter() {
        if complete.contains(start) {
            continue;
        }
        let mut current = Some(*start);
        let mut local = BTreeSet::new();
        while let Some(id) = current {
            if complete.contains(&id) {
                break;
            }
            if !local.insert(id) {
                errors.push(ValidationError::new(
                    &path,
                    "lambda lexical-scope graph contains a cycle",
                ));
                break;
            }
            current = fragment
                .expressions()
                .get(id)
                .and_then(|expression| expression.lambda_scope);
        }
        complete.extend(local);
    }
}

pub(crate) fn validate_expression_reachability(
    fragment: &Fragment,
    errors: &mut ValidationContext,
) {
    let path = format!("fragments[{}].expressions", fragment.id().get());
    let mut pending = Vec::new();
    for node in fragment.nodes().values() {
        node.kind.expression_references(&mut pending);
        if let NodeKind::Scan { derived_values, .. } = &node.kind {
            pending.extend(derived_values.iter().filter_map(|value| {
                fragment
                    .values()
                    .get(value)
                    .and_then(|definition| match definition.origin {
                        ValueOrigin::Expr { node: owner, expr } if owner == node.id => Some(expr),
                        _ => None,
                    })
            }));
        }
    }
    let mut reachable = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !reachable.insert(id) {
            continue;
        }
        if let Some(expression) = fragment.expressions().get(id) {
            expression.kind.expression_references(&mut pending);
        }
    }
    if reachable.len() != fragment.expressions().len() {
        errors.push(ValidationError::new(
            &path,
            "expression arena contains definitions unreachable from operator roots",
        ));
    }
}

pub(crate) fn validate_aggregate_arguments(
    fragment: &Fragment,
    binding: &crate::AggregateBinding,
    args: &[ExprId],
    order_by: &[crate::SortExpr],
    path: &str,
    errors: &mut ValidationContext,
) {
    if !binding
        .function
        .intrinsic_row_error
        .is_valid_for_kind(binding.function.kind)
    {
        errors.push(ValidationError::new(
            path,
            "bound aggregate intrinsic row-error fact differs from its kind",
        ));
    }
    match binding.phase {
        AggregatePhase::Single | AggregatePhase::Partial { .. } => {
            let logical_count =
                usize::try_from(binding.logical_argument_count).unwrap_or(usize::MAX);
            if logical_count != args.len()
                || binding.function.argument_types.len() != args.len() + order_by.len()
            {
                errors.push(ValidationError::new(
                    path,
                    "aggregate logical/ORDER BY channel counts differ from the bound signature",
                ));
            } else {
                if binding.function.argument_types[logical_count..]
                    .iter()
                    .any(|argument| matches!(argument, crate::FunctionArgumentType::Lambda { .. }))
                {
                    errors.push(ValidationError::new(
                        path,
                        "aggregate ORDER BY update channels must be scalar values",
                    ));
                }
                let inputs = args
                    .iter()
                    .copied()
                    .chain(order_by.iter().map(|item| item.expr))
                    .collect::<Vec<_>>();
                validate_function_arguments(
                    fragment,
                    &binding.function.function_id,
                    &binding.function.argument_types,
                    &inputs,
                    path,
                    errors,
                );
            }
        }
        AggregatePhase::Intermediate { .. } | AggregatePhase::Final { .. } => {
            if args.len() != 1 || !order_by.is_empty() {
                errors.push(ValidationError::new(
                    path,
                    "state-consuming aggregate phase requires exactly one state input",
                ));
            } else if let Some(argument) = fragment.expressions().get(args[0])
                && (argument.ty.data_type != binding.intermediate_type.data_type
                    || (binding.intermediate_type.nullable && !argument.ty.nullable))
            {
                // The carrier the state travelled in may admit null the
                // phase before it never wrote -- a state crossing an
                // exchange is declared by the column layout, not by the
                // binding. It may not claim the reverse.
                errors.push(ValidationError::new(
                    path,
                    "aggregate state input differs from its bound intermediate type",
                ));
            }
        }
    }
}

pub(crate) fn validate_expression_values_on_port(
    fragment: &Fragment,
    roots: &[ExprId],
    port_values: &ValuePortIndex,
    path: &str,
    errors: &mut ValidationContext,
) {
    let mut visited = BTreeSet::new();
    let mut pending = roots.to_vec();
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        let Some(expression) = fragment.expressions().get(id) else {
            continue;
        };
        if let ExprKind::Value(value) = expression.kind
            && !port_values.contains(&value)
        {
            errors.push(ValidationError::new(
                path,
                format!(
                    "join key expression references value {} from the wrong input",
                    value.get()
                ),
            ));
        }
        expression.kind.expression_references(&mut pending);
    }
}
