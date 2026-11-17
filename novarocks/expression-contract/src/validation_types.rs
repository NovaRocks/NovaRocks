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

use crate::{ExprId, ExprKind, ValueType};
use arrow_schema::DataType;
use novarocks_type_contract::{
    FunctionArgumentType, FunctionKind, arithmetic_result_value_shape_with_op,
    is_integer_value_type, is_numeric_value_type,
};
use std::collections::BTreeSet;

/// Borrow the first original diagnosis until the enclosing validator returns.
/// Successful checks never build a path, message, or copied type payload.
pub(crate) struct TypeIssue<'a> {
    pub(crate) definition: ExprId,
    pub(crate) message: TypeIssueMessage<'a>,
}
pub(crate) enum TypeIssueMessage<'a> {
    Static(&'static str),
    LambdaPlacement(Option<ExprId>),
    CastTarget {
        actual: &'a DataType,
        target: &'a DataType,
    },
    BooleanArgument(usize),
    CaseComparison {
        operand: Option<&'a DataType>,
        when: &'a DataType,
    },
    ArgumentCount {
        expected: usize,
        actual: usize,
    },
    FunctionArgument {
        function: &'a novarocks_type_contract::FunctionId,
        ordinal: usize,
        expected: &'a FunctionArgumentType,
        actual: Option<&'a ValueType>,
    },
}
impl<'a> From<&'static str> for TypeIssueMessage<'a> {
    fn from(message: &'static str) -> Self {
        Self::Static(message)
    }
}
impl std::fmt::Display for TypeIssueMessage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static(message) => f.write_str(message),
            Self::LambdaPlacement(parent) => {
                f.write_str("lambda must appear only as an exact bound-function lambda argument")?;
                if let Some(parent) = parent {
                    write!(
                        f,
                        "; expression {} references it outside a bound lambda argument position",
                        parent.get()
                    )?;
                }
                Ok(())
            }
            Self::CastTarget { actual, target } => write!(
                f,
                "cast result type {actual:?} differs from its target {target:?}"
            ),
            Self::BooleanArgument(ordinal) => {
                write!(f, "boolean connective argument {ordinal} is not boolean")
            }
            Self::CaseComparison {
                operand: Some(operand),
                when,
            } => write!(f, "CASE compares {operand:?} against a branch of {when:?}"),
            Self::CaseComparison {
                operand: None,
                when,
            } => write!(f, "CASE condition is {when:?}, not a boolean"),
            Self::ArgumentCount { expected, actual } => write!(
                f,
                "bound function expects {expected} arguments, got {actual}"
            ),
            Self::FunctionArgument {
                function,
                ordinal,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "function `{}` argument {ordinal} shape differs from its bound signature: bound {expected:?}, got ",
                    function.as_str()
                )?;
                if let Some(actual) = actual {
                    write!(f, "{actual:?}")
                } else {
                    f.write_str("absent")
                }
            }
        }
    }
}
impl<'a> TypeIssue<'a> {
    fn new(definition: ExprId, message: impl Into<TypeIssueMessage<'a>>) -> Self {
        Self {
            definition,
            message: message.into(),
        }
    }
}
#[derive(Default)]
pub(crate) struct TypeChecks<'a> {
    pub(crate) issue: Option<TypeIssue<'a>>,
}
impl<'a> TypeChecks<'a> {
    fn push(&mut self, issue: TypeIssue<'a>) {
        if self.issue.is_none() {
            self.issue = Some(issue);
        }
    }
}
pub(crate) fn value_bindings_match_ignoring_root_nullability(
    left: &ValueType,
    right: &ValueType,
) -> bool {
    left.same_value_domain(right) && left.typed_binding().layout() == right.typed_binding().layout()
}

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
                    Some(FunctionArgumentType::Lambda { .. })
                ) {
                    ExpressionParentRole::BoundLambdaArgument
                } else {
                    ExpressionParentRole::Other
                };
                output.push((argument, role));
            }
        }
        _ => {
            // The caller has already reserved one output slot per reference.
            // Borrow the same ordered edges instead of allocating a second Vec.
            let result = parent.kind.expression_references_observed(|reference| {
                output.push((reference, ExpressionParentRole::Other));
                Ok::<_, std::convert::Infallible>(())
            });
            match result {
                Ok(()) => {}
                Err(never) => match never {},
            }
        }
    }
}

pub(crate) fn validate_expression_types<'a>(
    definitions: &'a [crate::ExprNode],
    expression: &'a crate::ExprNode,
    operator_roots: &BTreeSet<ExprId>,
    parents: &[ExpressionParentReference],
    errors: &mut TypeChecks<'a>,
) {
    let path = expression.id;
    match &expression.kind {
        ExprKind::Value(_) => {}
        // Exact table/address/value checks are mandatory at plan/package publication.
        ExprKind::Constant(_) => {}
        ExprKind::LambdaParameter { lambda, ordinal } => {
            match definitions.get((*lambda).get() as usize) {
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
                Some(_) => errors.push(TypeIssue::new(
                    path,
                    "lambda parameter does not reference a matching lambda owner",
                )),
                None => errors.push(TypeIssue::new(
                    path,
                    "lambda parameter owner is not defined",
                )),
            }
            // A parameter is written inside its own lambda, or inside one
            // nested in it: `array_map(x -> array_map(y -> x + y, ys), xs)`
            // reads `x` from the scope the inner lambda opens. So the lambda
            // it names is the scope it stands in or one enclosing that.
            if !lambda_scope_contains(definitions, expression.lambda_scope, *lambda) {
                errors.push(TypeIssue::new(
                    path,
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
                errors.push(TypeIssue::new(
                    path,
                    TypeIssueMessage::LambdaPlacement(invalid_parent.map(|parent| parent.parent)),
                ));
            }
            if parameter_types.is_empty() {
                errors.push(TypeIssue::new(path, "lambda has no parameters"));
            }
            if let Some(body) = definitions.get((*body).get() as usize)
                && body.ty != expression.ty
            {
                errors.push(TypeIssue::new(
                    path,
                    "lambda type differs from its body type",
                ));
            }
        }
        ExprKind::Unary { op, expr } => {
            if let Some(input) = definitions.get((*expr).get() as usize) {
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
                    errors.push(TypeIssue::new(
                        path,
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
                errors.push(TypeIssue::new(
                    path,
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
                errors.push(TypeIssue::new(
                    path,
                    "non-arithmetic binary expression requires OutputNull decimal policy",
                ));
            }
            if let (Some(left), Some(right)) = (
                definitions.get((*left).get() as usize),
                definitions.get((*right).get() as usize),
            ) {
                validate_binary_types(left, *op, right, expression, path, errors);
            }
        }
        ExprKind::Conjunction { args } | ExprKind::Disjunction { args } => {
            validate_boolean_connective_types(definitions, args, expression, path, errors);
        }
        ExprKind::FunctionCall { function, args } => {
            if function.kind != FunctionKind::Scalar {
                errors.push(TypeIssue::new(path, "scalar call has non-scalar binding"));
            }
            validate_function_call(definitions, expression, function, args, path, errors);
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
                errors.push(TypeIssue::new(
                    path,
                    "CAST requires an ALLOW_THROW_EXCEPTION semantic reference",
                ));
            }
            if let Some(input) = definitions.get((*expr).get() as usize)
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
                errors.push(TypeIssue::new(
                    path,
                    "carrier CAST cannot establish or erase a logical identity",
                ));
            }
            if let Some(input) = definitions.get((*expr).get() as usize)
                && !novarocks_type_contract::decimal_error_policy_cast_supported(
                    input.ty.data_type(),
                    target,
                    *decimal_overflow_policy,
                )
            {
                errors.push(TypeIssue::new(
                    path,
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
                errors.push(TypeIssue::new(
                    path,
                    TypeIssueMessage::CastTarget {
                        actual: expression.ty.data_type(),
                        target,
                    },
                ));
            } else if definitions
                .get((*expr).get() as usize)
                .is_some_and(|input| input.ty.nullable() && !expression.ty.nullable())
            {
                errors.push(TypeIssue::new(
                    path,
                    "cast stops admitting null its input admits",
                ));
            }
        }
        ExprKind::IsNull { .. } | ExprKind::IsTruthValue { .. } => {
            require_non_nullable_boolean(&expression.ty, path, errors);
        }
        ExprKind::InList { expr, list, .. } => {
            require_boolean_result(&expression.ty, path, errors);
            if let Some(input) = definitions.get((*expr).get() as usize) {
                let nullable = input.ty.nullable()
                    || list.iter().any(|candidate| {
                        definitions
                            .get((*candidate).get() as usize)
                            .is_some_and(|candidate| candidate.ty.nullable())
                    });
                if nullable && !expression.ty.nullable() {
                    errors.push(TypeIssue::new(
                        path,
                        "IN-list result stops admitting null its operands admit",
                    ));
                }
                for candidate in list {
                    if definitions
                        .get((*candidate).get() as usize)
                        .is_some_and(|candidate| !candidate.ty.same_value_domain(&input.ty))
                    {
                        errors.push(TypeIssue::new(
                            path,
                            "IN-list candidate type differs from its input",
                        ));
                    }
                }
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            require_boolean_result(&expression.ty, path, errors);
            if let Some(input) = definitions.get((*expr).get() as usize) {
                let nullable = [*low, *high].into_iter().any(|bound| {
                    definitions
                        .get((bound).get() as usize)
                        .is_some_and(|bound| bound.ty.nullable())
                }) || input.ty.nullable();
                if nullable && !expression.ty.nullable() {
                    errors.push(TypeIssue::new(
                        path,
                        "BETWEEN result stops admitting null its operands admit",
                    ));
                }
                for bound in [*low, *high] {
                    if definitions
                        .get((bound).get() as usize)
                        .is_some_and(|bound| !bound.ty.same_value_domain(&input.ty))
                    {
                        errors.push(TypeIssue::new(
                            path,
                            "BETWEEN bound type differs from its input",
                        ));
                    }
                }
            }
        }
        ExprKind::Like { expr, pattern, .. } => {
            require_boolean_result(&expression.ty, path, errors);
            let nullable = [*expr, *pattern].into_iter().any(|id| {
                definitions
                    .get((id).get() as usize)
                    .is_some_and(|value| value.ty.nullable())
            });
            if nullable && !expression.ty.nullable() {
                errors.push(TypeIssue::new(
                    path,
                    "LIKE result stops admitting null its operands admit",
                ));
            }
            if [*expr, *pattern].into_iter().any(|id| {
                definitions
                    .get((id).get() as usize)
                    .is_some_and(|value| !is_utf8(value.ty.data_type()))
            }) {
                errors.push(TypeIssue::new(path, "LIKE operands must use a UTF-8 type"));
            }
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => validate_case_types(
            definitions,
            expression,
            *operand,
            when_then,
            *else_expr,
            path,
            errors,
        ),
    }
}

pub(crate) fn expression_reference_scope_allowed(
    definitions: &[crate::ExprNode],
    parent: &crate::ExprNode,
    child_id: ExprId,
    child: &crate::ExprNode,
) -> bool {
    if matches!(parent.kind, ExprKind::Lambda { body, .. } if body == child_id) {
        return child.lambda_scope == Some(parent.id);
    }
    if let ExprKind::LambdaParameter { lambda, .. } = child.kind {
        return lambda_scope_contains(definitions, parent.lambda_scope, lambda);
    }
    child.lambda_scope == parent.lambda_scope
}

pub(crate) fn lambda_scope_contains(
    definitions: &[crate::ExprNode],
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
        scope = definitions
            .get((id).get() as usize)
            .and_then(|expression| expression.lambda_scope);
    }
    false
}

/// Types an n-ary `AND`/`OR`.
///
/// Arity is checked here rather than left open: a connective over fewer than
/// two arguments would give the same predicate two spellings, and the point of
/// making the connective n-ary is to have exactly one.
pub(crate) fn validate_boolean_connective_types<'a>(
    definitions: &'a [crate::ExprNode],
    args: &[ExprId],
    output: &'a crate::ExprNode,
    path: ExprId,
    errors: &mut TypeChecks<'a>,
) {
    if args.len() < 2 {
        errors.push(TypeIssue::new(
            path,
            "boolean connective requires at least two arguments",
        ));
        return;
    }
    let mut nullable = false;
    for (ordinal, arg) in args.iter().enumerate() {
        let Some(arg) = definitions.get((*arg).get() as usize) else {
            continue;
        };
        if *arg.ty.data_type() != DataType::Boolean {
            errors.push(TypeIssue::new(
                path,
                TypeIssueMessage::BooleanArgument(ordinal),
            ));
        }
        nullable |= arg.ty.nullable();
    }
    // Three-valued `AND` and `OR` answer null where an argument does, and a
    // plan's nullability widens on the way out. What they may not do is
    // answer less than their arguments admit.
    if *output.ty.data_type() != DataType::Boolean || (nullable && !output.ty.nullable()) {
        errors.push(TypeIssue::new(
            path,
            "boolean connective result type is inconsistent with its arguments",
        ));
    }
}

pub(crate) fn validate_binary_types<'a>(
    left: &crate::ExprNode,
    op: crate::BinaryOperator,
    right: &crate::ExprNode,
    output: &'a crate::ExprNode,
    path: ExprId,
    errors: &mut TypeChecks<'a>,
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
            arithmetic_result_value_shape_with_op(&left.ty, &right.ty, operation).is_some_and(
                |(carrier, domain)| {
                    output.ty.logical_type() == domain
                        && novarocks_type_contract::arrow_data_types_exact(
                            output.ty.data_type(),
                            &carrier,
                        )
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
        errors.push(TypeIssue::new(
            path,
            "binary expression types are inconsistent with its operator",
        ));
    }
}

pub(crate) fn validate_case_types<'a>(
    definitions: &'a [crate::ExprNode],
    expression: &'a crate::ExprNode,
    operand: Option<ExprId>,
    when_then: &[(ExprId, ExprId)],
    else_expr: Option<ExprId>,
    path: ExprId,
    errors: &mut TypeChecks<'a>,
) {
    if when_then.is_empty() {
        errors.push(TypeIssue::new(path, "CASE expression has no branches"));
    }
    let operand_type =
        operand.and_then(|id| definitions.get((id).get() as usize).map(|node| &node.ty));
    for (when, then) in when_then {
        if let Some(when) = definitions.get((*when).get() as usize) {
            let valid = operand_type
                .map(|operand| operand.same_value_domain(&when.ty))
                .unwrap_or(*when.ty.data_type() == DataType::Boolean);
            if !valid {
                errors.push(TypeIssue::new(
                    path,
                    TypeIssueMessage::CaseComparison {
                        operand: operand_type.map(ValueType::data_type),
                        when: when.ty.data_type(),
                    },
                ));
            }
        }
        if definitions.get((*then).get() as usize).is_some_and(|then| {
            !value_bindings_match_ignoring_root_nullability(&then.ty, &expression.ty)
        }) {
            errors.push(TypeIssue::new(
                path,
                "CASE result type differs from its output",
            ));
        }
    }
    if let Some(else_expr) = else_expr
        && definitions
            .get((else_expr).get() as usize)
            .is_some_and(|otherwise| {
                !value_bindings_match_ignoring_root_nullability(&otherwise.ty, &expression.ty)
            })
    {
        errors.push(TypeIssue::new(
            path,
            "CASE ELSE type differs from its output",
        ));
    }
    let result_nullable = else_expr.is_none()
        || when_then.iter().any(|(_, then)| {
            definitions
                .get((*then).get() as usize)
                .is_some_and(|then| then.ty.nullable())
        })
        || else_expr.is_some_and(|otherwise| {
            definitions
                .get((otherwise).get() as usize)
                .is_some_and(|otherwise| otherwise.ty.nullable())
        });
    if result_nullable && !expression.ty.nullable() {
        errors.push(TypeIssue::new(
            path,
            "CASE result stops admitting null its branches admit",
        ));
    }
}

pub(crate) fn require_boolean_result(ty: &ValueType, path: ExprId, errors: &mut TypeChecks<'_>) {
    if *ty.data_type() != DataType::Boolean {
        errors.push(TypeIssue::new(path, "expression result is not Boolean"));
    }
}

pub(crate) fn require_non_nullable_boolean(
    ty: &ValueType,
    path: ExprId,
    errors: &mut TypeChecks<'_>,
) {
    if *ty.data_type() != DataType::Boolean || ty.nullable() {
        errors.push(TypeIssue::new(
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

pub(crate) fn validate_function_call<'a>(
    definitions: &'a [crate::ExprNode],
    expression: &'a crate::ExprNode,
    function: &'a crate::BoundFunction,
    args: &[ExprId],
    path: ExprId,
    errors: &mut TypeChecks<'a>,
) {
    validate_function_arguments(
        definitions,
        &function.function_id,
        &function.argument_types,
        args,
        path,
        errors,
    );
    if expression.ty != function.result_type {
        errors.push(TypeIssue::new(
            path,
            "function expression type differs from the bound result type",
        ));
    }
}

pub(crate) fn validate_function_arguments<'a>(
    definitions: &'a [crate::ExprNode],
    function: &'a novarocks_type_contract::FunctionId,
    expected: &'a [FunctionArgumentType],
    args: &[ExprId],
    path: ExprId,
    errors: &mut TypeChecks<'a>,
) {
    if expected.len() != args.len() {
        errors.push(TypeIssue::new(
            path,
            TypeIssueMessage::ArgumentCount {
                expected: expected.len(),
                actual: args.len(),
            },
        ));
        return;
    }
    for (ordinal, (expected, argument)) in expected.iter().zip(args).enumerate() {
        let valid = definitions
            .get((*argument).get() as usize)
            .is_some_and(|actual| {
                match (expected, &actual.kind) {
                    (FunctionArgumentType::Value(_), ExprKind::Lambda { .. }) => false,
                    // A parameter that accepts null accepts a value that never
                    // writes one; the mismatch is the other way round.
                    (FunctionArgumentType::Value(expected), _) => {
                        actual.ty.fits_value_type(expected)
                    }
                    (
                        FunctionArgumentType::Lambda {
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
                    (FunctionArgumentType::Lambda { .. }, _) => false,
                }
            });
        if !valid {
            errors.push(TypeIssue::new(
                path,
                TypeIssueMessage::FunctionArgument {
                    function,
                    ordinal,
                    expected,
                    actual: definitions
                        .get(argument.get() as usize)
                        .map(|actual| &actual.ty),
                },
            ));
        }
    }
}
