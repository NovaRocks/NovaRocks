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

use arrow_schema::DataType;
use novarocks_type_contract::{
    arithmetic_result_value_type_with_op, is_integer_value_type, is_numeric_value_type,
};

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
        // Exact table/address/value checks are mandatory at plan/package publication.
        ExprKind::Constant(_) => {}
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
                let widens = expression.ty.nullable() || !input.ty.nullable();
                // Strict unary NULL preserves the actual nullable NULL domain
                // and its child evaluation; it is not a numeric coercion.
                let strict_null = input.ty.logical_type()
                    == novarocks_type_contract::ValueLogicalType::Physical
                    && *input.ty.data_type() == DataType::Null
                    && input.ty.nullable();
                let valid = match op {
                    crate::UnaryOperator::Plus | crate::UnaryOperator::Minus => {
                        (is_numeric_value_type(&input.ty) || strict_null)
                            && input.ty.same_value_domain(&expression.ty)
                            && widens
                    }
                    crate::UnaryOperator::Not => {
                        novarocks_type_contract::supports_boolean_negation_value_type(&input.ty)
                            && *expression.ty.data_type() == DataType::Boolean
                            && widens
                    }
                    crate::UnaryOperator::BitwiseNot => {
                        (is_integer_value_type(&input.ty) || strict_null)
                            && input.ty.same_value_domain(&expression.ty)
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
        ExprKind::Binary {
            left,
            op,
            right,
            decimal_overflow_policy,
            allow_throw_exception,
        } => {
            let arithmetic = matches!(
                op,
                crate::BinaryOperator::Add
                    | crate::BinaryOperator::Subtract
                    | crate::BinaryOperator::Multiply
                    | crate::BinaryOperator::Divide
                    | crate::BinaryOperator::Modulo
            );
            if arithmetic != allow_throw_exception.is_some()
                || allow_throw_exception.is_some_and(|reference| {
                    reference.expected_key
                        != novarocks_type_contract::SemanticParameterKey::AllowThrowException
                })
            {
                errors.push(ValidationError::new(
                    &path,
                    "binary ALLOW_THROW_EXCEPTION reference differs from its operator profile",
                ));
            }
            if !matches!(
                op,
                crate::BinaryOperator::Add
                    | crate::BinaryOperator::Subtract
                    | crate::BinaryOperator::Multiply
                    | crate::BinaryOperator::Divide
                    | crate::BinaryOperator::Modulo
            ) && *decimal_overflow_policy
                != novarocks_type_contract::DecimalOverflowPolicy::OutputNull
            {
                errors.push(ValidationError::new(
                    &path,
                    "non-arithmetic binary expression requires OutputNull decimal policy",
                ));
            }
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
                        if binding.function.signature_matches(function)
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
        ExprKind::Cast {
            expr,
            target,
            decimal_overflow_policy,
            allow_throw_exception,
        } => {
            if allow_throw_exception.expected_key
                != novarocks_type_contract::SemanticParameterKey::AllowThrowException
            {
                errors.push(ValidationError::new(
                    &path,
                    "CAST requires an ALLOW_THROW_EXCEPTION semantic reference",
                ));
            }
            if let Some(input) = fragment.expressions().get(*expr)
                && !novarocks_type_contract::is_default_json_structure_cast(
                    &input.ty,
                    &expression.ty,
                )
                && !novarocks_type_contract::preserves_value_domains_observed(
                    input.ty.value_type(),
                    expression.ty.value_type(),
                    |_| Ok::<_, novarocks_type_contract::ValueTypeError>(()),
                )
                .unwrap_or(false)
            {
                errors.push(ValidationError::new(
                    &path,
                    "carrier CAST cannot establish or erase a logical identity",
                ));
            }
            if let Some(input) = fragment.expressions().get(*expr)
                && !novarocks_type_contract::decimal_error_policy_cast_supported(
                    input.ty.data_type(),
                    target,
                    *decimal_overflow_policy,
                )
            {
                errors.push(ValidationError::new(
                    &path,
                    "ReportError decimal CAST does not support nested type conversion",
                ));
            }
            // A cast names the type it produces, and may admit null where its
            // input does not -- the conversion itself can fail, and the
            // statement may stand this value where null is admitted. It may
            // not claim the reverse: a null-admitting input does not stop
            // admitting null by being converted.
            if !novarocks_type_contract::arrow_data_types_exact(expression.ty.data_type(), target) {
                // Both types, because which half drifted is the diagnosis.
                errors.push(ValidationError::new(
                    &path,
                    format!(
                        "cast result type {:?} differs from its target {target:?}",
                        expression.ty.data_type()
                    ),
                ));
            } else if fragment
                .expressions()
                .get(*expr)
                .is_some_and(|input| input.ty.nullable() && !expression.ty.nullable())
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
                let nullable = input.ty.nullable()
                    || list.iter().any(|candidate| {
                        fragment
                            .expressions()
                            .get(*candidate)
                            .is_some_and(|candidate| candidate.ty.nullable())
                    });
                if nullable && !expression.ty.nullable() {
                    errors.push(ValidationError::new(
                        &path,
                        "IN-list result stops admitting null its operands admit",
                    ));
                }
                for candidate in list {
                    if fragment
                        .expressions()
                        .get(*candidate)
                        .is_some_and(|candidate| !candidate.ty.same_value_domain(&input.ty))
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
                        .is_some_and(|bound| bound.ty.nullable())
                }) || input.ty.nullable();
                if nullable && !expression.ty.nullable() {
                    errors.push(ValidationError::new(
                        &path,
                        "BETWEEN result stops admitting null its operands admit",
                    ));
                }
                for bound in [*low, *high] {
                    if fragment
                        .expressions()
                        .get(bound)
                        .is_some_and(|bound| !bound.ty.same_value_domain(&input.ty))
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
                    .is_some_and(|value| value.ty.nullable())
            });
            if nullable && !expression.ty.nullable() {
                errors.push(ValidationError::new(
                    &path,
                    "LIKE result stops admitting null its operands admit",
                ));
            }
            if [*expr, *pattern].into_iter().any(|id| {
                fragment
                    .expressions()
                    .get(id)
                    .is_some_and(|value| !is_utf8(value.ty.data_type()))
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
        if *arg.ty.data_type() != DataType::Boolean {
            errors.push(ValidationError::new(
                path,
                format!("boolean connective argument {ordinal} is not boolean"),
            ));
        }
        nullable |= arg.ty.nullable();
    }
    // Three-valued `AND` and `OR` answer null where an argument does, and a
    // plan's nullability widens on the way out. What they may not do is
    // answer less than their arguments admit.
    if *output.ty.data_type() != DataType::Boolean || (nullable && !output.ty.nullable()) {
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
    let same_inputs = left.ty.same_value_domain(&right.ty);
    // An operator may admit null neither operand does -- arithmetic answers
    // with null where it cannot answer with a number, and a plan's nullability
    // widens on the way out. It may not admit less than its operands do.
    let nullable = left.ty.nullable() || right.ty.nullable();
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
            arithmetic_result_value_type_with_op(&left.ty, &right.ty, operation).is_some_and(
                |expected| {
                    output.ty.same_value_domain(&expected)
                        && nullability_widens(output.ty.nullable())
                },
            )
        }
        crate::BinaryOperator::Eq
        | crate::BinaryOperator::NotEq
        | crate::BinaryOperator::Lt
        | crate::BinaryOperator::LtEq
        | crate::BinaryOperator::Gt
        | crate::BinaryOperator::GtEq => {
            same_inputs
                && *output.ty.data_type() == DataType::Boolean
                && nullability_widens(output.ty.nullable())
        }
        crate::BinaryOperator::EqForNull => {
            same_inputs && *output.ty.data_type() == DataType::Boolean && !output.ty.nullable()
        }
        crate::BinaryOperator::BitAnd
        | crate::BinaryOperator::BitOr
        | crate::BinaryOperator::BitXor => {
            same_inputs
                && is_integer_value_type(&left.ty)
                && output.ty.same_value_domain(&left.ty)
                && nullability_widens(output.ty.nullable())
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
                .map(|operand| operand.same_value_domain(&when.ty))
                .unwrap_or(*when.ty.data_type() == DataType::Boolean);
            if !valid {
                errors.push(ValidationError::new(
                    path,
                    match operand_type {
                        Some(operand) => format!(
                            "CASE compares {:?} against a branch of {:?}",
                            operand.data_type(),
                            when.ty.data_type()
                        ),
                        None => {
                            format!("CASE condition is {:?}, not a boolean", when.ty.data_type())
                        }
                    },
                ));
            }
        }
        if fragment.expressions().get(*then).is_some_and(|then| {
            !super::value_bindings_match_ignoring_root_nullability(&then.ty, &expression.ty)
        }) {
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
            .is_some_and(|otherwise| {
                !super::value_bindings_match_ignoring_root_nullability(
                    &otherwise.ty,
                    &expression.ty,
                )
            })
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
                .is_some_and(|then| then.ty.nullable())
        })
        || else_expr.is_some_and(|otherwise| {
            fragment
                .expressions()
                .get(otherwise)
                .is_some_and(|otherwise| otherwise.ty.nullable())
        });
    if result_nullable && !expression.ty.nullable() {
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
        let valid = fragment
            .expressions()
            .get(expression)
            .is_some_and(|expression| {
                !expression.ty.nullable()
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
}

pub(crate) fn frame_has_offset(frame: &crate::WindowFrame) -> bool {
    [&frame.start, &frame.end].iter().any(|bound| {
        matches!(
            bound,
            crate::WindowBound::Preceding(_) | crate::WindowBound::Following(_)
        )
    })
}

pub(crate) fn window_bound_position(_fragment: &Fragment, bound: &crate::WindowBound) -> u8 {
    match bound {
        crate::WindowBound::UnboundedPreceding => 0,
        crate::WindowBound::Preceding(_) => 1,
        crate::WindowBound::CurrentRow => 2,
        crate::WindowBound::Following(_) => 3,
        crate::WindowBound::UnboundedFollowing => 4,
    }
}

pub(crate) fn window_offset_literal_is_valid(
    kind: &ExprKind,
    units: crate::WindowFrameUnits,
) -> bool {
    matches!(
        (units, kind),
        (
            crate::WindowFrameUnits::Rows | crate::WindowFrameUnits::Groups,
            ExprKind::Constant(_),
        )
    )
}

pub(crate) fn require_boolean_result(ty: &ValueType, path: &str, errors: &mut ValidationContext) {
    if *ty.data_type() != DataType::Boolean {
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
    if *ty.data_type() != DataType::Boolean || ty.nullable() {
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

/// Only the legacy publication path requires original legacy provenance.
/// Package construction separately checks actual FrozenFragmentCalls and never
/// substitutes these projections for the occurrence's complete effect facts.
pub(crate) fn validate_legacy_binding_metadata(
    metadata: Option<&crate::LegacyBindingMetadata>,
    kind: crate::FunctionKind,
    path: &str,
    invalid_kind_message: &'static str,
    errors: &mut ValidationContext,
) {
    if !errors.checks_output_and_effect_proofs() {
        return;
    }
    match metadata {
        None => errors.push(ValidationError::unsupported_capability(
            path,
            "legacy publication requires original binding metadata; use frozen package admission for an exact signature",
        )),
        Some(metadata) if !metadata.intrinsic_row_error.is_valid_for_kind(kind) => {
            errors.push(ValidationError::new(path, invalid_kind_message));
        }
        Some(_) => {}
    }
}

pub(crate) fn validate_function_call(
    fragment: &Fragment,
    expression: &crate::ExprNode,
    function: &crate::BoundFunction,
    args: &[ExprId],
    path: &str,
    errors: &mut ValidationContext,
) {
    validate_legacy_binding_metadata(
        function.legacy_metadata.as_ref(),
        function.kind,
        path,
        "bound function intrinsic row-error fact differs from its kind",
        errors,
    );
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
                    actual.ty.fits_value_type(expected)
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
                ) => {
                    actual_parameters.as_ref() == parameter_types.as_slice()
                        && &actual.ty == result_type
                }
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
    validate_legacy_binding_metadata(
        binding.function.legacy_metadata.as_ref(),
        binding.function.kind,
        path,
        "bound aggregate intrinsic row-error fact differs from its kind",
        errors,
    );
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
                && (!super::value_bindings_match_ignoring_root_nullability(
                    &argument.ty,
                    &binding.intermediate_type,
                ) || (binding.intermediate_type.nullable() && !argument.ty.nullable()))
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

#[cfg(test)]
mod value_arithmetic_tests {
    use super::*;
    use crate::tests::constant_fixture::ConstantFixture;
    use crate::{
        BinaryOperator, FragmentBuilder, FragmentId, FragmentSink, PipelineDopDomain, UnaryOperator,
    };
    use novarocks_type_contract::ValueLogicalType;

    fn largeint(nullable: bool) -> ValueType {
        ValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            nullable,
            ValueLogicalType::LargeInt,
        )
        .unwrap()
    }

    // Exercise the actual local binary gate, independently of literal encoding
    // and whole-plan topology. No owner refinement or runtime claim is made.
    fn binary_valid(
        left: ValueType,
        op: BinaryOperator,
        right: ValueType,
        output: ValueType,
    ) -> bool {
        let node = |id, ty| crate::ExprNode {
            id: ExprId::new(id),
            owner: NodeId::new(0),
            lambda_scope: None,
            ty,
            kind: ExprKind::Value(crate::ValueId::new(id)),
        };
        let mut errors = ValidationContext::new();
        validate_binary_types(
            &node(0, left),
            op,
            &node(1, right),
            &node(2, output),
            "binary",
            &mut errors,
        );
        errors.is_empty()
    }

    #[test]
    fn arithmetic_gate_preserves_exact_domain_and_only_root_nullable_widening() {
        assert!(binary_valid(
            largeint(false),
            BinaryOperator::Add,
            ValueType::new(DataType::Int64, false),
            largeint(false)
        ));
        assert!(binary_valid(
            largeint(false),
            BinaryOperator::Add,
            ValueType::new(DataType::Int64, false),
            largeint(true)
        ));
        assert!(!binary_valid(
            largeint(true),
            BinaryOperator::Add,
            ValueType::new(DataType::Int64, false),
            largeint(false)
        ));
        assert!(!binary_valid(
            largeint(false),
            BinaryOperator::Add,
            ValueType::new(DataType::Int64, false),
            ValueType::new(DataType::FixedSizeBinary(16), true)
        ));
        assert!(binary_valid(
            largeint(false),
            BinaryOperator::Divide,
            largeint(false),
            ValueType::new(DataType::Float64, true)
        ));
        assert!(!binary_valid(
            largeint(false),
            BinaryOperator::Divide,
            largeint(false),
            largeint(true)
        ));
        assert!(binary_valid(
            ValueType::new(DataType::Decimal128(38, 15), true),
            BinaryOperator::Subtract,
            largeint(false),
            ValueType::new(DataType::Decimal256(55, 15), true)
        ));
        assert!(!binary_valid(
            ValueType::new(DataType::Decimal128(38, 15), true),
            BinaryOperator::Multiply,
            largeint(false),
            ValueType::new(DataType::Decimal256(55, 15), true)
        ));
    }

    #[test]
    fn binary_numeric_gates_reject_opaque_fixed16_and_preserve_unsigned_bitwise() {
        for opaque in [
            ValueType::new(DataType::FixedSizeBinary(16), false),
            ValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
        ] {
            assert!(!binary_valid(
                opaque.clone(),
                BinaryOperator::Add,
                largeint(false),
                largeint(true)
            ));
            assert!(!binary_valid(
                opaque.clone(),
                BinaryOperator::BitAnd,
                opaque.clone(),
                opaque
            ));
        }
        for dtype in [
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            assert!(binary_valid(
                ValueType::new(dtype.clone(), false),
                BinaryOperator::BitXor,
                ValueType::new(dtype.clone(), true),
                ValueType::new(dtype, true)
            ));
        }
    }

    fn unary_fragment(
        input: ValueType,
        output: ValueType,
        op: UnaryOperator,
        nonliteral_null_child: bool,
    ) -> Result<(Fragment, crate::ConstantPools), ValidationErrors> {
        let mut constants = ConstantFixture::new();
        let mut builder = FragmentBuilder::new(FragmentId::new(0));
        let node = builder.reserve_node_id().unwrap();
        let mut child = builder
            .add_expression(node, input.clone(), constants.null(&input))
            .unwrap();
        if nonliteral_null_child {
            child = builder
                .add_expression(
                    node,
                    input,
                    ExprKind::Unary {
                        op: UnaryOperator::Plus,
                        expr: child,
                    },
                )
                .unwrap();
        }
        let root = builder
            .add_expression(node, output.clone(), ExprKind::Unary { op, expr: child })
            .unwrap();
        let value = builder
            .add_value(
                output,
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .add_values(
                node,
                vec![vec![root].into_boxed_slice()].into_boxed_slice(),
                vec![value].into_boxed_slice(),
            )
            .unwrap();
        let fragment = builder.finish_definition(
            node,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            crate::StagePlacementPolicy::Singleton,
        )?;
        Ok((fragment, constants.into_pools()))
    }

    #[test]
    fn checked_unary_fragments_preserve_decimal_uint_largeint_and_strict_null() {
        for dtype in [
            DataType::Float16,
            DataType::Decimal32(7, 2),
            DataType::Decimal64(10, 2),
            DataType::Decimal128(38, 2),
            DataType::Decimal256(76, 2),
            DataType::UInt64,
        ] {
            let ty = ValueType::new(dtype, true);
            unary_fragment(ty.clone(), ty, UnaryOperator::Minus, false).unwrap();
        }
        unary_fragment(
            largeint(true),
            largeint(true),
            UnaryOperator::BitwiseNot,
            false,
        )
        .unwrap();
        for op in [
            UnaryOperator::Plus,
            UnaryOperator::Minus,
            UnaryOperator::BitwiseNot,
        ] {
            let ty = ValueType::new(DataType::Null, true);
            let (fragment, _constants) = unary_fragment(ty.clone(), ty.clone(), op, true).unwrap();
            assert_eq!(fragment.expressions().len(), 3);
            assert!(fragment.expressions().iter().all(|(_, expr)| expr.ty == ty));
        }
        for opaque in [
            ValueType::new(DataType::FixedSizeBinary(16), true),
            ValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
        ] {
            for op in [UnaryOperator::Minus, UnaryOperator::BitwiseNot] {
                let errors = unary_fragment(opaque.clone(), opaque.clone(), op, false).unwrap_err();
                assert!(errors.errors().iter().any(|error| error.message()
                    == "unary expression types are inconsistent with its operator"));
            }
        }
        assert!(
            unary_fragment(
                ValueType::new(DataType::Null, true),
                ValueType::new(DataType::Int64, true),
                UnaryOperator::Minus,
                false
            )
            .is_err()
        );
    }
    #[test]
    fn numeric_not_keeps_the_original_successful_domain_and_nullability() {
        for carrier in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(18, 4),
        ] {
            {
                let nullable = true;
                assert!(
                    unary_fragment(
                        ValueType::new(carrier.clone(), nullable),
                        ValueType::new(DataType::Boolean, nullable),
                        UnaryOperator::Not,
                        false
                    )
                    .is_ok()
                );
            }
            assert!(
                unary_fragment(
                    ValueType::new(carrier, true),
                    ValueType::new(DataType::Boolean, false),
                    UnaryOperator::Not,
                    false
                )
                .is_err()
            );
        }
        for carrier in [
            DataType::UInt64,
            DataType::Decimal256(50, 4),
            DataType::Date32,
            DataType::Null,
            DataType::FixedSizeBinary(16),
        ] {
            assert!(
                unary_fragment(
                    ValueType::new(carrier, true),
                    ValueType::new(DataType::Boolean, true),
                    UnaryOperator::Not,
                    false
                )
                .is_err()
            );
        }
    }
}
