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

use crate::unit_creation::{UnitValidationCreation, ValidationFailure};
use crate::{ExprId, ExprKind, ValueType};
use arrow_schema::DataType;
use novarocks_type_contract::CompileCheckpoints;
use novarocks_type_contract::{
    FunctionArgumentType, FunctionKind, OperatorFunction, arithmetic_result_value_shape_with_op,
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
// The old pure predicates classified a type-author refusal as false. Keep
// that semantic result, while preserving every real control/creator failure.
fn original_type_predicate<'a, H>(
    result: Result<bool, ValidationFailure<'a, H>>,
) -> Result<bool, ValidationFailure<'a, H>> {
    match result {
        Err(ValidationFailure::Owner(crate::UnitError::Type(_))) => Ok(false),
        result => result,
    }
}
fn same_domain<'a, H>(
    left: &ValueType,
    right: &ValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ValidationFailure<'a, H>> {
    original_type_predicate(
        left.same_value_domain_observed(right, || work.step().map_err(ValidationFailure::from)),
    )
}
fn exact_type<'a, H>(
    left: &ValueType,
    right: &ValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ValidationFailure<'a, H>> {
    original_type_predicate(
        left.exactly_equals_observed(right, || work.step().map_err(ValidationFailure::from)),
    )
}
fn value_bindings_match_ignoring_root_nullability<'a, H>(
    left: &ValueType,
    right: &ValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ValidationFailure<'a, H>> {
    Ok(same_domain(left, right, work)?
        && left
            .typed_binding()
            .layout()
            .equals_observed(right.typed_binding().layout(), || {
                work.step().map_err(ValidationFailure::from)
            })?)
}
fn value_types_equal<'a, H>(
    left: &[ValueType],
    right: &[ValueType],
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ValidationFailure<'a, H>> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        if !exact_type(left, right, work)? {
            return Ok(false);
        }
    }
    Ok(true)
}
fn matches_definition<'a, H>(
    definitions: &[crate::ExprNode],
    id: ExprId,
    matches: impl FnOnce(&crate::ExprNode) -> Result<bool, ValidationFailure<'a, H>>,
) -> Result<bool, ValidationFailure<'a, H>> {
    definitions
        .get(id.get() as usize)
        .map(matches)
        .transpose()
        .map(|result| result.unwrap_or(false))
}
fn any_definition<'a, H>(
    definitions: &[crate::ExprNode],
    ids: impl Iterator<Item = ExprId>,
    predicate: impl Fn(&crate::ExprNode) -> bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ValidationFailure<'a, H>> {
    for id in ids {
        let matched = definitions.get(id.get() as usize).is_some_and(&predicate);
        work.step()?;
        if matched {
            return Ok(true);
        }
    }
    Ok(false)
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

pub(crate) fn validate_expression_types<'a, H>(
    definitions: &'a [crate::ExprNode],
    expression: &'a crate::ExprNode,
    operator_roots: &BTreeSet<ExprId>,
    parents: &[ExpressionParentReference],
    errors: &mut TypeChecks<'a>,
    creation: &mut UnitValidationCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ValidationFailure<'a, H>> {
    let path = expression.id;
    match &expression.kind {
        ExprKind::Value(crate::ValueAddress::Input(_)) => {}
        ExprKind::Value(crate::ValueAddress::Local(binding)) => {
            let matches = matches_definition(definitions, *binding, |owner| {
                let ExprKind::Let { value, .. } = owner.kind else {
                    return Ok(false);
                };
                if owner.lambda_scope != expression.lambda_scope {
                    return Ok(false);
                }
                matches_definition(definitions, value, |value| {
                    exact_type(&value.ty, &expression.ty, work)
                })
            })?;
            if !matches {
                errors.push(TypeIssue::new(
                    path,
                    "local value does not reference a matching Let initializer in its lambda scope",
                ));
            }
        }
        ExprKind::Let { body, .. } => {
            if !matches_definition(definitions, *body, |body| {
                exact_type(&body.ty, &expression.ty, work)
            })? {
                errors.push(TypeIssue::new(
                    path,
                    "Let result differs from its complete body type",
                ));
            }
        }
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
                }) if match usize::try_from(*ordinal)
                    .ok()
                    .and_then(|ordinal| parameter_types.get(ordinal))
                {
                    Some(parameter) => exact_type(parameter, &expression.ty, work)?,
                    None => false,
                } => {}
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
            if !lambda_scope_contains(
                definitions,
                expression.lambda_scope,
                *lambda,
                creation,
                work,
            )? {
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
                && !exact_type(&body.ty, &expression.ty, work)?
            {
                errors.push(TypeIssue::new(
                    path,
                    "lambda type differs from its body type",
                ));
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
                validate_binary_types(left, *op, right, expression, path, errors, work)?;
            }
        }
        ExprKind::Conjunction { args } | ExprKind::Disjunction { args } => {
            validate_boolean_connective_types(definitions, args, expression, path, errors, work)?;
        }
        ExprKind::FunctionCall { function, args } => {
            if function.kind != FunctionKind::Scalar {
                errors.push(TypeIssue::new(path, "scalar call has non-scalar binding"));
            }
            validate_function_call(definitions, expression, function, args, path, errors, work)?;
        }
        ExprKind::Cast {
            expr,
            target,
            cast_mode,
        } => {
            if let Some(input) = definitions.get((*expr).get() as usize)
                && *cast_mode == novarocks_type_contract::TrinoCastMode::Implicit
                && !novarocks_type_contract::is_null_list_to_json_cast(&input.ty, &expression.ty)
                && !novarocks_type_contract::preserves_value_domains_observed(
                    input.ty.value_type(),
                    expression.ty.value_type(),
                    |units| {
                        for _ in 0..units {
                            work.step()?;
                        }
                        Ok::<_, ValidationFailure<'a, H>>(())
                    },
                )?
            {
                errors.push(TypeIssue::new(
                    path,
                    "carrier CAST cannot establish or erase a logical identity",
                ));
            }
            // A cast names the type it produces, and may admit null where its
            // input does not -- the conversion itself can fail, and the
            // statement may stand this value where null is admitted. It may
            // not claim the reverse: a null-admitting input does not stop
            // admitting null by being converted.
            if !original_type_predicate(novarocks_type_contract::arrow_data_types_exact_observed(
                expression.ty.data_type(),
                target,
                || work.step().map_err(ValidationFailure::from),
            ))? {
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
        ExprKind::IsNull { .. } => {
            require_non_nullable_boolean(&expression.ty, path, errors);
        }
        ExprKind::Case {
            ieee_float_equality,
            operand,
            when_then,
            else_expr,
        } => validate_case_types(
            definitions,
            expression,
            *operand,
            *ieee_float_equality,
            when_then,
            *else_expr,
            path,
            errors,
            work,
        )?,
    }
    Ok(())
}

pub(crate) fn expression_reference_scope_allowed<'a, H>(
    definitions: &'a [crate::ExprNode],
    parent: &crate::ExprNode,
    child_id: ExprId,
    child: &crate::ExprNode,
    creation: &mut UnitValidationCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ValidationFailure<'a, H>> {
    if matches!(parent.kind, ExprKind::Lambda { body, .. } if body == child_id) {
        return Ok(child.lambda_scope == Some(parent.id));
    }
    if let ExprKind::LambdaParameter { lambda, .. } = child.kind {
        return lambda_scope_contains(definitions, parent.lambda_scope, lambda, creation, work);
    }
    Ok(child.lambda_scope == parent.lambda_scope)
}

pub(crate) fn lambda_scope_contains<'a, H>(
    definitions: &'a [crate::ExprNode],
    mut scope: Option<ExprId>,
    expected: ExprId,
    creation: &mut UnitValidationCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ValidationFailure<'a, H>> {
    let mut visited = BTreeSet::new();
    while let Some(id) = scope {
        work.step()?;
        if id == expected {
            return Ok(true);
        }
        if !creation.set_insert(&mut visited, id, work)? {
            return Ok(false);
        }
        scope = definitions
            .get(id.get() as usize)
            .and_then(|expression| expression.lambda_scope);
    }
    Ok(false)
}

/// Types an n-ary `AND`/`OR`.
///
/// Arity is checked here rather than left open: a connective over fewer than
/// two arguments would give the same predicate two spellings, and the point of
/// making the connective n-ary is to have exactly one.
pub(crate) fn validate_boolean_connective_types<'a, H>(
    definitions: &'a [crate::ExprNode],
    args: &[ExprId],
    output: &'a crate::ExprNode,
    path: ExprId,
    errors: &mut TypeChecks<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ValidationFailure<'a, H>> {
    if args.len() < 2 {
        errors.push(TypeIssue::new(
            path,
            "boolean connective requires at least two arguments",
        ));
        return Ok(());
    }
    let mut nullable = false;
    let mut has_immediate_constant_source = false;
    for (ordinal, arg) in args.iter().enumerate() {
        work.step()?;
        let Some(arg) = definitions.get((*arg).get() as usize) else {
            continue;
        };
        if *arg.ty.data_type() != DataType::Boolean
            || arg.ty.logical_type() != novarocks_type_contract::ValueLogicalType::Physical
        {
            errors.push(TypeIssue::new(
                path,
                TypeIssueMessage::BooleanArgument(ordinal),
            ));
        }
        nullable |= arg.ty.nullable();
        has_immediate_constant_source |= matches!(arg.kind, ExprKind::Constant(_));
    }
    // No external pool value is available in the neutral unit. Only an
    // immediate ConstantReference can carry a later deciding-value proof.
    // SQL and BE must check that exact value through the shared source author
    // before accepting a narrower result; this is not a NULL coercion.
    if *output.ty.data_type() != DataType::Boolean
        || output.ty.logical_type() != novarocks_type_contract::ValueLogicalType::Physical
        || (nullable && !output.ty.nullable() && !has_immediate_constant_source)
    {
        errors.push(TypeIssue::new(
            path,
            "boolean connective result type is inconsistent with its arguments",
        ));
    }
    Ok(())
}

pub(crate) fn validate_binary_types<'a, H>(
    left: &crate::ExprNode,
    op: crate::BinaryOperator,
    right: &crate::ExprNode,
    output: &'a crate::ExprNode,
    path: ExprId,
    errors: &mut TypeChecks<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ValidationFailure<'a, H>> {
    let same_inputs = same_domain(&left.ty, &right.ty, work)?;
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
                // A Null carrier produces SQL NULL even when the source's
                // outer declaration is required. TruthOnly cannot erase it.
                && (output.ty.nullable()
                    || (*left.ty.data_type()!=DataType::Null && *right.ty.data_type()!=DataType::Null))
        }
        crate::BinaryOperator::EqForNull => {
            same_inputs && *output.ty.data_type() == DataType::Boolean && !output.ty.nullable()
        }
    };
    if !valid {
        errors.push(TypeIssue::new(
            path,
            "binary expression types are inconsistent with its operator",
        ));
    }
    Ok(())
}

pub(crate) fn validate_case_types<'a, H>(
    definitions: &'a [crate::ExprNode],
    expression: &'a crate::ExprNode,
    operand: Option<ExprId>,
    ieee_float_equality: bool,
    when_then: &[(ExprId, ExprId)],
    else_expr: Option<ExprId>,
    path: ExprId,
    errors: &mut TypeChecks<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ValidationFailure<'a, H>> {
    if when_then.is_empty() {
        errors.push(TypeIssue::new(path, "CASE expression has no branches"));
    }
    let operand_type =
        operand.and_then(|id| definitions.get((id).get() as usize).map(|node| &node.ty));
    if ieee_float_equality
        && !operand_type.is_some_and(|ty| {
            ty.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
                && matches!(ty.data_type(), DataType::Float32 | DataType::Float64)
        })
    {
        errors.push(TypeIssue::new(
            path,
            "CASE IEEE equality requires a floating operand",
        ));
    }
    for (when, then) in when_then {
        if let Some(when) = definitions.get((*when).get() as usize) {
            let valid = match operand_type {
                Some(operand) => same_domain(operand, &when.ty, work)?,
                None => *when.ty.data_type() == DataType::Boolean,
            };
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
        if matches_definition(definitions, *then, |then| {
            Ok(!value_bindings_match_ignoring_root_nullability(
                &then.ty,
                &expression.ty,
                work,
            )?)
        })? {
            errors.push(TypeIssue::new(
                path,
                "CASE result type differs from its output",
            ));
        }
    }
    if let Some(else_expr) = else_expr
        && matches_definition(definitions, else_expr, |otherwise| {
            Ok(!value_bindings_match_ignoring_root_nullability(
                &otherwise.ty,
                &expression.ty,
                work,
            )?)
        })?
    {
        errors.push(TypeIssue::new(
            path,
            "CASE ELSE type differs from its output",
        ));
    }
    let result_nullable = else_expr.is_none()
        || any_definition(
            definitions,
            when_then.iter().map(|(_, then)| *then),
            |then| then.ty.nullable(),
            work,
        )?
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
    Ok(())
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

pub(crate) fn validate_function_call<'a, H>(
    definitions: &'a [crate::ExprNode],
    expression: &'a crate::ExprNode,
    function: &'a crate::BoundFunction,
    args: &[ExprId],
    path: ExprId,
    errors: &mut TypeChecks<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ValidationFailure<'a, H>> {
    validate_function_arguments(
        definitions,
        &function.function_id,
        &function.argument_types,
        args,
        path,
        errors,
        work,
    )?;
    if !exact_type(&expression.ty, &function.result_type, work)? {
        errors.push(TypeIssue::new(
            path,
            "function expression type differs from the bound result type",
        ));
    }
    Ok(())
}

pub(crate) fn validate_function_arguments<'a, H>(
    definitions: &'a [crate::ExprNode],
    function: &'a novarocks_type_contract::FunctionId,
    expected: &'a [FunctionArgumentType],
    args: &[ExprId],
    path: ExprId,
    errors: &mut TypeChecks<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ValidationFailure<'a, H>> {
    let group = match expected {
        [
            FunctionArgumentType::Value(_),
            group @ FunctionArgumentType::CandidateGroup { .. },
        ] if function.as_str() == OperatorFunction::InList.function_id_text() => Some(group),
        _ => None,
    };
    let valid_arity = if group.is_some() {
        args.len() >= expected.len()
    } else {
        args.len() == expected.len()
    };
    if !valid_arity {
        errors.push(TypeIssue::new(
            path,
            TypeIssueMessage::ArgumentCount {
                expected: expected.len(),
                actual: args.len(),
            },
        ));
        return Ok(());
    }
    let scalar_arguments = expected.len() - usize::from(group.is_some());
    for (ordinal, (expected, argument)) in expected[..scalar_arguments].iter().zip(args).enumerate()
    {
        let valid = matches_definition(definitions, *argument, |actual| {
            Ok(match (expected, &actual.kind) {
                (FunctionArgumentType::Value(_), ExprKind::Lambda { .. }) => false,
                // Preserve the original parameter/root NULL widening rule.
                (FunctionArgumentType::Value(expected), _) => {
                    original_type_predicate(actual.ty.fits_value_type_observed(expected, || {
                        work.step().map_err(ValidationFailure::from)
                    }))?
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
                    value_types_equal(actual_parameters, parameter_types.as_slice(), work)?
                        && exact_type(&actual.ty, result_type, work)?
                }
                (
                    FunctionArgumentType::Lambda { .. }
                    | FunctionArgumentType::CandidateGroup { .. },
                    _,
                ) => false,
            })
        })?;
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
    if group.is_some()
        && let [
            FunctionArgumentType::Value(probe_type),
            group @ FunctionArgumentType::CandidateGroup { element_type },
        ] = expected
    {
        if !value_bindings_match_ignoring_root_nullability(probe_type, element_type, work)? {
            errors.push(TypeIssue::new(
                path,
                "probe and candidate group must share a physical value domain",
            ));
        }
        let mut nullable = false;
        for (ordinal, &argument) in args.iter().enumerate().skip(1) {
            let actual = definitions.get(argument.get() as usize);
            if let Some(actual) = actual {
                nullable |= actual.ty.nullable();
                if !matches!(actual.kind, ExprKind::Lambda { .. })
                    && value_bindings_match_ignoring_root_nullability(
                        &actual.ty,
                        element_type,
                        work,
                    )?
                {
                    work.step()?;
                    continue;
                }
            }
            errors.push(TypeIssue::new(
                path,
                TypeIssueMessage::FunctionArgument {
                    function,
                    ordinal,
                    expected: group,
                    actual: actual.map(|actual| &actual.ty),
                },
            ));
            work.step()?;
        }
        if nullable != element_type.nullable() {
            errors.push(TypeIssue::new(
                path,
                "candidate group root nullability differs from its members",
            ));
        }
    }
    Ok(())
}
