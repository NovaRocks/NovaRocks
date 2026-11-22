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

use novarocks_expression::typing::{
    self, CarrierConversion, ChildForm, ChildTyping, Conversion, LiteralEmission, LiteralSyntax,
    NodeShape, NodeTyping, ScalarBindingCatalog, TypedLiteral, TypingContext, TypingError,
};
use novarocks_functions::{
    ConstantPolicy, FunctionArgument, FunctionBindingError, FunctionBindingRequest,
    FunctionResultType, ResolvedFunctionBinding,
};
use novarocks_parser::{Span, ast};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, FunctionId, FunctionOverloadId,
    FunctionValueType, OperatorFunction, PureCompileControl,
};

use super::AnalyzerContext;
use super::resolve_expr::{BoundScalarCall, signed_int_literal_value};
use crate::analysis::{ExprKind, LiteralValue, TypedExpr, function_argument};
use crate::analyze_error::AnalyzeError;
use crate::binding::SqlFunctionBinding;
use crate::compiler::SqlFunctionCatalog;

struct StatementScalarCatalog<'a>(&'a dyn SqlFunctionCatalog);

impl ScalarBindingCatalog for StatementScalarCatalog<'_> {
    fn select_scalar(
        &self,
        name: &str,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        if let Some(expected) = request.expected_result_type {
            self.0.resolve_scalar_binding_with_expected_result(
                name,
                request.arguments,
                expected,
                control,
            )
        } else {
            self.0
                .resolve_scalar_binding(name, request.arguments, control)
        }
    }

    fn select_operator(
        &self,
        operator: OperatorFunction,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.0
            .resolve_scalar_operator_binding(operator, request, control)
    }

    fn select_exact_scalar(
        &self,
        function: &FunctionId,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.0
            .finish_scalar_binding(function, overload, request, control)
    }

    fn select_value_conversion(
        &self,
        argument: &FunctionArgument,
        target: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.0
            .resolve_value_conversion_binding(argument, target, control)
    }
}

impl AnalyzerContext<'_> {
    pub(super) fn type_scalar_call(
        &self,
        shape: NodeShape<'_>,
        args: Vec<TypedExpr>,
        span: Span,
    ) -> Result<BoundScalarCall, AnalyzeError> {
        type_scalar_call_with_catalog(
            self.function_catalog,
            shape,
            args,
            self.sql_semantics.sql_mode().decimal_overflow_policy(),
            self.constant_policy,
            self.control,
            span,
        )
    }

    pub(super) fn type_value_node(
        &self,
        shape: NodeShape<'_>,
        args: Vec<TypedExpr>,
        span: Span,
    ) -> Result<(FunctionValueType, Vec<TypedExpr>), AnalyzeError> {
        type_value_node_with_catalog(
            self.function_catalog,
            shape,
            args,
            self.sql_semantics.sql_mode().decimal_overflow_policy(),
            self.constant_policy,
            self.control,
            span,
        )
    }

    pub(super) fn type_literal(&self, literal: &ast::Literal) -> Result<TypedExpr, AnalyzeError> {
        let syntax = match &literal.kind {
            ast::LiteralKind::Null => LiteralSyntax::Null,
            ast::LiteralKind::Boolean(value) => LiteralSyntax::Boolean(*value),
            ast::LiteralKind::Long { parsed_value, .. } => LiteralSyntax::Integer(*parsed_value),
            ast::LiteralKind::Double(value) => LiteralSyntax::Double(value.get()),
            ast::LiteralKind::Decimal(text) => LiteralSyntax::Decimal(text),
            ast::LiteralKind::String(text) => LiteralSyntax::String(text),
            ast::LiteralKind::Binary(bytes) => LiteralSyntax::Binary(bytes),
            ast::LiteralKind::Generic(value) => LiteralSyntax::TypedString {
                name: &value.type_name,
                value: &value.value,
            },
            ast::LiteralKind::Interval(_) => LiteralSyntax::Interval,
        };
        self.with_typing(literal.span, |context, work| {
            let NodeTyping::Literal(emission) =
                typing::type_node(context, NodeShape::Literal(syntax), &[], work)?
            else {
                return Err(TypingError::InvalidShape("expected a typed literal"));
            };
            Ok(match emission {
                LiteralEmission::Direct(value) => literal_expression(value, false),
                LiteralEmission::Cast {
                    input,
                    declared_result,
                    conversion,
                } => {
                    let mut expression =
                        cast_expression(literal_expression(input, false), conversion);
                    expression.value_type = declared_result;
                    expression
                }
            })
        })
    }

    pub(super) fn coerce_simple_case_float_operand(
        &self,
        value: TypedExpr,
        target: &FunctionValueType,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        self.with_typing(span, |context, work| {
            let mut children = typed_children(context, std::slice::from_ref(&value), false, work)?;
            let child = children.pop().expect("one simple CASE operand");
            let target = target.with_nullable(value.value_type.nullable())?;
            let conversion = typing::operator_decimal_float(context, &child, &target, work)?;
            let mut args = apply_conversions(context, vec![value], vec![conversion], work)?;
            Ok(args.pop().expect("one converted simple CASE operand"))
        })
    }

    fn with_typing<T>(
        &self,
        span: Span,
        action: impl FnOnce(&TypingContext<'_>, &mut CompileCheckpoints<'_>) -> Result<T, TypingError>,
    ) -> Result<T, AnalyzeError> {
        with_typing(
            self.function_catalog,
            self.sql_semantics.sql_mode().decimal_overflow_policy(),
            self.constant_policy,
            self.control,
            span,
            action,
        )
    }
}

pub(super) fn type_value_node_with_catalog(
    catalog: &dyn SqlFunctionCatalog,
    shape: NodeShape<'_>,
    args: Vec<TypedExpr>,
    decimal_overflow_policy: DecimalOverflowPolicy,
    constant_policy: ConstantPolicy,
    control: &dyn PureCompileControl,
    span: Span,
) -> Result<(FunctionValueType, Vec<TypedExpr>), AnalyzeError> {
    with_typing(
        catalog,
        decimal_overflow_policy,
        constant_policy,
        control,
        span,
        |context, work| {
            let check_branch_constants = matches!(shape, NodeShape::SearchedCase { .. });
            let children = typed_children(context, &args, check_branch_constants, work)?;
            let NodeTyping::Value {
                result,
                conversions,
            } = typing::type_node(context, shape, &children, work)?
            else {
                return Err(TypingError::InvalidShape("expected a typed value node"));
            };
            Ok((result, apply_conversions(context, args, conversions, work)?))
        },
    )
}

pub(super) fn type_scalar_call_with_catalog(
    catalog: &dyn SqlFunctionCatalog,
    shape: NodeShape<'_>,
    args: Vec<TypedExpr>,
    decimal_overflow_policy: DecimalOverflowPolicy,
    constant_policy: ConstantPolicy,
    control: &dyn PureCompileControl,
    span: Span,
) -> Result<BoundScalarCall, AnalyzeError> {
    with_typing(
        catalog,
        decimal_overflow_policy,
        constant_policy,
        control,
        span,
        |context, work| {
            let children = typed_children(context, &args, true, work)?;
            let NodeTyping::Call {
                selected,
                conversions,
            } = typing::type_node(context, shape, &children, work)
                .map_err(|error| scalar_selection_error(error, shape, &args))?
            else {
                return Err(TypingError::InvalidShape("expected a bound scalar call"));
            };
            let args = apply_conversions(context, args, conversions, work)?;
            let converted = typed_children(context, &args, true, work)?;
            let binding = typing::finish_call(context, &selected, &converted, work)?;
            Ok(BoundScalarCall {
                args,
                binding: SqlFunctionBinding::new(binding, context.decimal_overflow_policy),
            })
        },
    )
}

fn scalar_selection_error(
    error: TypingError,
    shape: NodeShape<'_>,
    args: &[TypedExpr],
) -> TypingError {
    match (shape, error) {
        (NodeShape::CatalogCall { name, .. }, TypingError::Binding(error))
            if !matches!(
                error,
                FunctionBindingError::UnavailableImplementation(_)
                    | FunctionBindingError::VariantTargetType(_)
            ) =>
        {
            let arg_types: Vec<_> = args.iter().map(|arg| arg.value_type.data_type()).collect();
            TypingError::TypeMismatch(format!(
                "cannot bind scalar function `{name}` for argument types {arg_types:?}: {error}"
            ))
        }
        (_, error) => error,
    }
}

fn with_typing<T>(
    catalog: &dyn SqlFunctionCatalog,
    decimal_overflow_policy: DecimalOverflowPolicy,
    constant_policy: ConstantPolicy,
    control: &dyn PureCompileControl,
    span: Span,
    action: impl FnOnce(&TypingContext<'_>, &mut CompileCheckpoints<'_>) -> Result<T, TypingError>,
) -> Result<T, AnalyzeError> {
    let catalog = StatementScalarCatalog(catalog);
    let context = TypingContext {
        catalog: &catalog,
        constant_policy,
        decimal_overflow_policy,
    };
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
        .map_err(AnalyzeError::control)?;
    let result = action(&context, &mut work);
    work.finish_result(result, TypingError::Control)
        .map_err(|error| analyze_typing_error(error, span))
}

fn analyze_typing_error(error: TypingError, span: Span) -> AnalyzeError {
    match error {
        TypingError::TypeMismatch(message) => AnalyzeError::type_mismatch(message, span),
        TypingError::InvalidLiteral(message) => AnalyzeError::invalid_literal(message, span),
        TypingError::Unsupported(message) => AnalyzeError::unsupported_expression(message, span),
        TypingError::Control(error) => AnalyzeError::control(error),
        TypingError::Binding(error @ FunctionBindingError::AmbiguousOverload) => {
            AnalyzeError::ambiguous_function_call(error, span)
        }
        TypingError::Binding(error) => AnalyzeError::function_binding(error).at_type_mismatch(span),
        TypingError::Constant(error) => {
            AnalyzeError::function_binding(error.into()).at_type_mismatch(span)
        }
        error => AnalyzeError::internal(error.to_string()).at_type_mismatch(span),
    }
}

fn typed_children(
    context: &TypingContext<'_>,
    children: &[TypedExpr],
    include_constants: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<ChildTyping>, TypingError> {
    children
        .iter()
        .map(|child| {
            work.step()?;
            let form = match &child.kind {
                ExprKind::Literal(LiteralValue::Int(value)) => ChildForm::IntegerLiteral(*value),
                ExprKind::Literal(LiteralValue::Null) => ChildForm::NullLiteral,
                _ => signed_int_literal_value(child)
                    .map_or(ChildForm::Other, ChildForm::NegatedIntegerLiteral),
            };
            let constant = if include_constants {
                work.flush()?;
                let FunctionArgument::Value { constant, .. } =
                    function_argument(child, context.constant_policy, work.control())?
                else {
                    return Err(TypingError::InvalidShape(
                        "a typed scalar node cannot take a lambda",
                    ));
                };
                constant
            } else {
                None
            };
            Ok(ChildTyping {
                value_type: child.value_type.clone(),
                form,
                constant,
            })
        })
        .collect()
}

fn apply_conversions(
    context: &TypingContext<'_>,
    args: Vec<TypedExpr>,
    conversions: Vec<Conversion>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<TypedExpr>, TypingError> {
    if args.len() != conversions.len() {
        return Err(TypingError::InvalidShape(
            "scalar typing returned the wrong number of conversions",
        ));
    }
    args.into_iter().zip(conversions).map(|(source, conversion)| {
        work.step()?;
        Ok(match conversion {
            Conversion::Keep => source,
            Conversion::RetypeLiteral(literal) => {
                let materialized = matches!(source.kind, ExprKind::Constant(_));
                literal_expression(literal, materialized)
            }
            Conversion::Cast(conversion) => cast_expression(source, conversion),
            Conversion::ValueDomain(conversion) => {
                let FunctionResultType::Scalar(result) = &conversion.binding.selected.result_type else {
                    return Err(TypingError::InvalidShape("value conversion did not return a scalar"));
                };
                let result = result.clone();
                let volatility = conversion.binding.semantics.volatility;
                let binding = match conversion.origin {
                    typing::ValueConversionOrigin::ValueDomain => {
                        SqlFunctionBinding::new_with_conversion_constraint(
                            conversion.binding,
                            context.decimal_overflow_policy,
                            conversion.expected_result,
                            conversion.target,
                        )
                    }
                    typing::ValueConversionOrigin::ArrayCommonCoercion => {
                        SqlFunctionBinding::new_with_array_common_constraint(
                            conversion.binding,
                            context.decimal_overflow_policy,
                            conversion.expected_result,
                        )
                    }
                    typing::ValueConversionOrigin::OperatorDecimalFloat => {
                        SqlFunctionBinding::new_with_operator_decimal_float_constraint(
                            conversion.binding,
                            context.decimal_overflow_policy,
                            conversion.expected_result,
                        )
                    }
                };
                let expression = TypedExpr {
                    kind: ExprKind::FunctionCall {
                        name: novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_NAME.into(),
                        binding,
                        volatility,
                        args: vec![source].into(),
                        distinct: false,
                    },
                    value_type: result,
                };
                if let Some(tail) = conversion.tail { cast_expression(expression, tail) } else { expression }
            }
        })
    }).collect()
}

fn literal_expression(literal: TypedLiteral, materialized: bool) -> TypedExpr {
    TypedExpr {
        kind: if materialized {
            ExprKind::Constant(literal.constant)
        } else {
            ExprKind::Literal(literal.value)
        },
        value_type: literal.value_type,
    }
}

fn cast_expression(source: TypedExpr, conversion: CarrierConversion) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Cast {
            expr: ast::AstBox::new(source),
            target: conversion.target.data_type().clone(),
            decimal_overflow_policy: conversion.decimal_overflow_policy,
            array_failure_mode: None,
        },
        value_type: conversion.target,
    }
}

#[cfg(any(test, novarocks_dev_controls))]
pub(crate) fn negation_for_test(
    operand: TypedExpr,
    control: &dyn PureCompileControl,
) -> Result<TypedExpr, AnalyzeError> {
    let mut call = type_scalar_call_with_catalog(
        crate::functions::builtin_sql_function_catalog(),
        NodeShape::Operator(OperatorFunction::Negation),
        vec![operand],
        DecimalOverflowPolicy::OutputNull,
        crate::constant::test_constant_policy(),
        control,
        Span::new(0, 0),
    )?;
    Ok(crate::common::negation::typed_call(
        call.binding,
        call.args.pop().expect("one negation operand"),
    ))
}
