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
    ConstantValidation, FunctionArgument, FunctionBindingError, FunctionBindingRequest,
    FunctionResultType, HigherOrderBindingCandidate, ResolvedFunctionBinding,
};
use novarocks_parser::{Span, ast};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, FunctionId, FunctionOverloadId,
    FunctionValueType, OperatorFunction, PureCompileControl,
};

use super::resolve_expr::{BoundScalarCall, signed_int_literal_value};
use super::{AnalyzerContext, BindingFinalizer};
use crate::analysis::{ExprKind, LiteralValue, TypedExpr, function_argument};
use crate::analyze_error::AnalyzeError;
use crate::binding::SqlResultConstraintOrigin as Origin;
use crate::compiler::SqlFunctionCatalog;

struct StatementScalarCatalog<'a>(&'a dyn SqlFunctionCatalog);

impl ScalarBindingCatalog for StatementScalarCatalog<'_> {
    fn begin_higher_order_binding(
        &self,
        name: &str,
        ordinary_ordinal: usize,
        ordinary: &FunctionArgument,
        lambda_ordinal: usize,
        lambda_arity: usize,
        control: &dyn PureCompileControl,
    ) -> Result<HigherOrderBindingCandidate, FunctionBindingError> {
        self.0.begin_higher_order_binding(
            name,
            ordinary_ordinal,
            ordinary,
            lambda_ordinal,
            lambda_arity,
            control,
        )
    }

    fn finish_higher_order_binding(
        &self,
        candidate: &HigherOrderBindingCandidate,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.0
            .finish_higher_order_binding(candidate, request, control)
    }

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

    fn select_trusted_scalar(
        &self,
        function: &FunctionId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.0
            .resolve_trusted_scalar_binding(function, request, control)
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
    pub(super) fn begin_collection_lambda(
        &self,
        name: &str,
        collection: TypedExpr,
        arity: usize,
        span: Span,
    ) -> Result<(HigherOrderBindingCandidate, TypedExpr), AnalyzeError> {
        let args = vec![collection];
        let ordinals = self.described_arguments(&args);
        let (candidate, mut converted) = self.with_typing(span, |context, work| {
            let children = typed_children(context, &args, true, work)?;
            let (candidate, conversion) =
                typing::begin_collection_lambda(context, name, &children[0], arity, work)?;
            let converted = apply_conversions(
                args,
                vec![conversion],
                &self.binding_finalizer(),
                &[],
                span,
                work,
            )?;
            Ok((candidate, converted))
        })?;
        self.observe_argument_types(ordinals, converted.iter().map(|arg| Some(&arg.value_type)))?;
        Ok((
            candidate,
            converted.pop().expect("one converted collection"),
        ))
    }

    pub(super) fn finish_collection_lambda(
        &self,
        candidate: &HigherOrderBindingCandidate,
        collection: TypedExpr,
        lambda: TypedExpr,
        span: Span,
    ) -> Result<BoundScalarCall, AnalyzeError> {
        self.with_typing(span, |context, work| {
            let mut args = vec![collection];
            let children = typed_children(context, &args, true, work)?;
            let binding = typing::finish_collection_lambda(
                context,
                candidate,
                &children[0],
                &lambda.value_type,
                work,
            )?;
            let binding =
                self.binding_finalizer()
                    .finish(binding, "higher-order function", span)?;
            args.push(lambda);
            Ok(BoundScalarCall { args, binding })
        })
    }

    pub(super) fn described_arguments(&self, args: &[TypedExpr]) -> Option<Vec<Option<usize>>> {
        let factory = self.factory.borrow();
        let description = factory.prepared_description.as_ref()?;
        Some(
            args.iter()
                .map(|arg| description.parameter_ordinal(arg))
                .collect(),
        )
    }

    pub(super) fn observe_argument_types<'a>(
        &self,
        ordinals: Option<Vec<Option<usize>>>,
        types: impl IntoIterator<Item = Option<&'a FunctionValueType>>,
    ) -> Result<(), AnalyzeError> {
        if let Some(ordinals) = ordinals {
            let mut factory = self.factory.borrow_mut();
            let description = factory
                .prepared_description
                .as_mut()
                .expect("original description");
            for (ordinal, target) in ordinals.into_iter().zip(types) {
                self.check_control()?;
                if let (Some(ordinal), Some(target)) = (ordinal, target) {
                    description.coerce_parameter(ordinal, target)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn observe_implicit_type(
        &self,
        source: &TypedExpr,
        target: &FunctionValueType,
    ) -> Result<(), AnalyzeError> {
        if let Some(description) = &mut self.factory.borrow_mut().prepared_description
            && let Some(ordinal) = description.parameter_ordinal(source)
        {
            description.coerce_parameter(ordinal, target)?;
        }
        Ok(())
    }

    pub(super) fn type_scalar_call(
        &self,
        shape: NodeShape<'_>,
        args: Vec<TypedExpr>,
        span: Span,
    ) -> Result<BoundScalarCall, AnalyzeError> {
        self.type_scalar_call_at_arguments(shape, args, &[], span)
    }

    pub(super) fn type_scalar_call_at_arguments(
        &self,
        shape: NodeShape<'_>,
        args: Vec<TypedExpr>,
        originals: &[&ast::Expr],
        span: Span,
    ) -> Result<BoundScalarCall, AnalyzeError> {
        let ordinals = self.described_arguments(&args);
        let result = type_scalar_call_with_finalizer(
            &self.binding_finalizer(),
            shape,
            args,
            originals,
            self.constant_policy,
            span,
        )?;
        self.observe_argument_types(
            ordinals,
            result.args.iter().map(|arg| Some(&arg.value_type)),
        )?;
        Ok(result)
    }

    pub(super) fn type_value_node(
        &self,
        shape: NodeShape<'_>,
        args: Vec<TypedExpr>,
        span: Span,
    ) -> Result<(FunctionValueType, Vec<TypedExpr>), AnalyzeError> {
        let ordinals = self.described_arguments(&args);
        let (result, converted) = self.with_typing(span, |context, work| {
            let check_branch_constants = matches!(shape, NodeShape::SearchedCase { .. });
            let children = typed_children(context, &args, check_branch_constants, work)?;
            let NodeTyping::Value {
                result,
                conversions,
            } = typing::type_node(context, shape, &children, work)?
            else {
                return Err(TypingError::InvalidShape("expected a typed value node").into());
            };
            Ok((
                result,
                apply_conversions(
                    args,
                    conversions,
                    &self.binding_finalizer(),
                    &[],
                    span,
                    work,
                )?,
            ))
        })?;
        self.observe_argument_types(ordinals, converted.iter().map(|arg| Some(&arg.value_type)))?;
        Ok((result, converted))
    }

    pub(super) fn type_extract(
        &self,
        field: ast::ExtractField,
        source: TypedExpr,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        self.with_typing(span, |context, work| {
            work.flush()?;
            let argument = function_argument(&source, context.constant_policy, work.control())?;
            let selected = typing::bind_extract(context.catalog, field, &argument, work.control())?;
            let FunctionResultType::Scalar(value_type) = &selected.selected.result_type else {
                return Err(TypingError::InvalidShape("EXTRACT result is not scalar").into());
            };
            Ok(TypedExpr {
                value_type: value_type.clone(),
                kind: ExprKind::FunctionCall {
                    name: novarocks_parser::extract_call_name(field).into(),
                    volatility: selected.semantics.volatility,
                    binding: self.binding_finalizer().finish(
                        selected,
                        novarocks_parser::extract_call_name(field),
                        span,
                    )?,
                    args: vec![source].into(),
                    distinct: false,
                },
            })
        })
    }

    pub(super) fn type_session_expression(
        &self,
        expression: &ast::Expr,
    ) -> Result<TypedExpr, AnalyzeError> {
        let span = expression.span();
        if self.effects.observe_caller_session(span)? {
            // This declaration can guide scope selection without projecting a
            // caller value. An adopted attempt is refused before it escapes.
            let value_type = FunctionValueType::new(
                arrow::datatypes::DataType::Utf8,
                !matches!(expression, ast::Expr::CurrentUser(_)),
            );
            let name = "<caller session>";
            return Ok(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: self.alloc_column_id(None, name.into(), value_type.clone()),
                    qualifier: None,
                    column: name.into(),
                },
                value_type,
            });
        }
        let session = self.sql_semantics.statement_session().ok_or_else(|| {
            AnalyzeError::unsupported_expression(
                "Session expressions require an admitted caller session",
                span,
            )
        })?;
        let (value, nullable) = match expression {
            ast::Expr::CurrentUser(_) => (Some(session.principal()), false),
            ast::Expr::CurrentCatalog(_) => (session.catalog(), true),
            ast::Expr::CurrentSchema(_) => (session.schema(), true),
            _ => unreachable!("session expression dispatched by the original syntax"),
        };
        self.with_typing(span, |context, work| {
            let value_type = FunctionValueType::new(arrow::datatypes::DataType::Utf8, nullable);
            let literal =
                value.map_or(LiteralValue::Null, |text| LiteralValue::String(text.into()));
            work.flush()?;
            let constant = typing::admit_syntax_constant(
                &literal,
                &value_type,
                context.constant_policy,
                work.control(),
            )?;
            Ok(TypedExpr {
                kind: ExprKind::Constant(constant),
                value_type,
            })
        })
    }

    pub(super) fn type_clock(&self, expression: &ast::Expr) -> Result<TypedExpr, AnalyzeError> {
        let span = expression.span();
        self.with_typing(span, |context, work| {
            let (syntax, target) = typing::clock_syntax(expression)?
                .ok_or(TypingError::InvalidShape("expected temporal clock syntax"))?;
            bind_temporal_syntax(self, context, syntax, target, Vec::new(), span, work)
        })
    }

    pub(super) fn type_at_zone(
        &self,
        mut source: TypedExpr,
        zone: Option<TypedExpr>,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        let (syntax, zoned_source, target) = typing::at_zone_syntax(
            &source.value_type,
            zone.as_ref().map(|expression| &expression.value_type),
        )
        .map_err(|cause| analyze_typing_error(cause, span))?;
        if source.value_type != zoned_source {
            source = self.type_temporal_cast(source, zoned_source, false, span)?;
        }
        let mut arguments = vec![source];
        arguments.extend(zone);
        self.with_typing(span, |context, work| {
            bind_temporal_syntax(self, context, syntax, target, arguments, span, work)
        })
    }

    pub(super) fn type_temporal_cast(
        &self,
        source: TypedExpr,
        target: FunctionValueType,
        safe: bool,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        authored_temporal_cast(
            &self.binding_finalizer(),
            self.constant_policy,
            source,
            target,
            safe,
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
            ast::LiteralKind::Interval(value) => LiteralSyntax::Interval(value),
        };
        self.with_typing(literal.span, |context, work| {
            let NodeTyping::Literal(emission) =
                typing::type_node(context, NodeShape::Literal(syntax), &[], work)?
            else {
                return Err(TypingError::InvalidShape("expected a typed literal").into());
            };
            Ok(match emission {
                LiteralEmission::Direct(value) => literal_expression(value, false),
                LiteralEmission::Constant { value_type, value } => TypedExpr {
                    kind: ExprKind::Constant(value),
                    value_type,
                },
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
            let mut args = apply_conversions(
                vec![value],
                vec![conversion],
                &self.binding_finalizer(),
                &[],
                span,
                work,
            )?;
            Ok(args.pop().expect("one converted simple CASE operand"))
        })
    }

    fn with_typing<T>(
        &self,
        span: Span,
        action: impl FnOnce(
            &TypingContext<'_>,
            &mut CompileCheckpoints<'_>,
        ) -> Result<T, SqlTypingError>,
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

fn bind_temporal_syntax(
    analyzer: &AnalyzerContext<'_>,
    context: &TypingContext<'_>,
    syntax: typing::TemporalSyntax,
    target: FunctionValueType,
    arguments: Vec<TypedExpr>,
    span: Span,
    work: &mut CompileCheckpoints<'_>,
) -> Result<TypedExpr, SqlTypingError> {
    let request_arguments = arguments
        .iter()
        .map(|argument| {
            work.step()?;
            work.flush()?;
            Ok(function_argument(
                argument,
                context.constant_policy,
                work.control(),
            )?)
        })
        .collect::<Result<Vec<_>, TypingError>>()?;
    let selected = typing::bind_temporal_syntax(
        context.catalog,
        syntax,
        &request_arguments,
        &target,
        work.control(),
    )?;
    let volatility = selected.semantics.volatility;
    let binding = analyzer.binding_finalizer().finish_with(
        selected,
        novarocks_functions::builtin::temporal_context::FUNCTION_NAME,
        span,
        Origin::TemporalSelected {
            own_nullable: false,
        },
        Some(target.clone()),
    )?;
    Ok(TypedExpr {
        value_type: target,
        kind: ExprKind::FunctionCall {
            name: novarocks_functions::builtin::temporal_context::FUNCTION_NAME.into(),
            volatility,
            binding,
            args: arguments.into(),
            distinct: false,
        },
    })
}

pub(super) fn authored_temporal_cast(
    finalizer: &BindingFinalizer<'_>,
    constant_policy: ConstantValidation,
    source: TypedExpr,
    target: FunctionValueType,
    safe: bool,
    span: Span,
) -> Result<TypedExpr, AnalyzeError> {
    with_typing(
        finalizer.catalog,
        finalizer.decimal_overflow_policy,
        constant_policy,
        finalizer.control,
        span,
        |context, work| {
            use novarocks_functions::builtin::value_conversion as owner;
            let nullable = safe || source.value_type.nullable();
            let target = target.with_nullable(nullable)?;
            let argument = function_argument(&source, constant_policy, work.control())?;
            work.flush()?;
            let binding = typing::select_temporal_cast(
                context.catalog,
                &argument,
                &target,
                safe,
                work.control(),
            )?
            .ok_or(TypingError::InvalidShape(
                "temporal CAST has no temporal type",
            ))?;
            let volatility = binding.semantics.volatility;
            let binding = finalizer.finish_with(
                binding,
                "CAST",
                span,
                Origin::TemporalSelected { own_nullable: safe },
                Some(target.clone()),
            )?;
            Ok(TypedExpr {
                value_type: target,
                kind: ExprKind::FunctionCall {
                    name: owner::VALUE_CONVERSION_NAME.into(),
                    volatility,
                    binding,
                    args: vec![source].into(),
                    distinct: false,
                },
            })
        },
    )
}

#[cfg(test)]
pub(super) fn type_value_node_with_catalog(
    catalog: &dyn SqlFunctionCatalog,
    shape: NodeShape<'_>,
    args: Vec<TypedExpr>,
    decimal_overflow_policy: DecimalOverflowPolicy,
    constant_policy: ConstantValidation,
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
                return Err(TypingError::InvalidShape("expected a typed value node").into());
            };
            Ok((
                result,
                apply_conversions(
                    args,
                    conversions,
                    &BindingFinalizer::ordinary(catalog, decimal_overflow_policy, control),
                    &[],
                    span,
                    work,
                )?,
            ))
        },
    )
}

pub(super) fn type_scalar_call_with_catalog(
    catalog: &dyn SqlFunctionCatalog,
    shape: NodeShape<'_>,
    args: Vec<TypedExpr>,
    decimal_overflow_policy: DecimalOverflowPolicy,
    constant_policy: ConstantValidation,
    control: &dyn PureCompileControl,
    span: Span,
) -> Result<BoundScalarCall, AnalyzeError> {
    type_scalar_call_with_finalizer(
        &BindingFinalizer::ordinary(catalog, decimal_overflow_policy, control),
        shape,
        args,
        &[],
        constant_policy,
        span,
    )
}

fn type_scalar_call_with_finalizer(
    finalizer: &BindingFinalizer<'_>,
    shape: NodeShape<'_>,
    args: Vec<TypedExpr>,
    originals: &[&ast::Expr],
    constant_policy: ConstantValidation,
    span: Span,
) -> Result<BoundScalarCall, AnalyzeError> {
    with_typing(
        finalizer.catalog,
        finalizer.decimal_overflow_policy,
        constant_policy,
        finalizer.control,
        span,
        |context, work| {
            let children = typed_children(context, &args, true, work)?;
            let NodeTyping::Call {
                selected,
                conversions,
            } = typing::type_node(context, shape, &children, work)
                .map_err(|error| scalar_selection_error(error, shape, &args))?
            else {
                return Err(TypingError::InvalidShape("expected a bound scalar call").into());
            };
            let args = apply_conversions(args, conversions, finalizer, originals, span, work)?;
            let converted = typed_children(context, &args, true, work)?;
            let binding = typing::finish_call(context, &selected, &converted, work)?;
            let name = match shape {
                NodeShape::CatalogCall { name, .. } => name,
                _ => "scalar expression",
            };
            let binding = if matches!(shape, NodeShape::Row { .. }) {
                let target = selected.expected_result().ok_or(TypingError::InvalidShape(
                    "ROW constructor lost its original result declaration",
                ))?;
                finalizer.finish_with(
                    binding,
                    name,
                    span,
                    Origin::RowConstructor,
                    Some(target.clone()),
                )
            } else {
                finalizer.finish(binding, name, span)
            }?;
            Ok(BoundScalarCall { args, binding })
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
                    | FunctionBindingError::ValueType(
                        novarocks_type_contract::ValueTypeError::InvalidCharacterWidth { .. }
                            | novarocks_type_contract::ValueTypeError::TooDeep
                    )
                    | FunctionBindingError::UnknownFunction
                    | FunctionBindingError::OperatorNotFound
                    | FunctionBindingError::HiddenFunction
                    | FunctionBindingError::NoMatchingOverload
                    | FunctionBindingError::AmbiguousOverload
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
    constant_policy: ConstantValidation,
    control: &dyn PureCompileControl,
    span: Span,
    action: impl FnOnce(&TypingContext<'_>, &mut CompileCheckpoints<'_>) -> Result<T, SqlTypingError>,
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
    work.finish_result(result, |error| {
        SqlTypingError::Analysis(AnalyzeError::control(error))
    })
    .map_err(|error| match error {
        SqlTypingError::Typing(cause) => analyze_typing_error(cause, span),
        SqlTypingError::Analysis(cause) => cause,
    })
}

enum SqlTypingError {
    Typing(TypingError),
    Analysis(AnalyzeError),
}

impl<E: Into<TypingError>> From<E> for SqlTypingError {
    fn from(error: E) -> Self {
        Self::Typing(error.into())
    }
}

impl From<AnalyzeError> for SqlTypingError {
    fn from(error: AnalyzeError) -> Self {
        Self::Analysis(error)
    }
}

pub(super) fn analyze_typing_error(error: TypingError, span: Span) -> AnalyzeError {
    match error {
        TypingError::TypeMismatch(message) => AnalyzeError::type_mismatch(message, span),
        TypingError::InvalidLiteral(message) => AnalyzeError::invalid_literal(message, span),
        TypingError::NumericOutOfRange(message) => AnalyzeError::at(
            crate::analyze_error::AnalyzeErrorKind::NumericOutOfRange,
            message,
            span,
        ),
        TypingError::Unsupported(message) => AnalyzeError::unsupported_expression(message, span),
        TypingError::ValueType(novarocks_type_contract::ValueTypeError::TooDeep) => {
            AnalyzeError::unsupported_type_depth().at_type_mismatch(span)
        }
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
    args: Vec<TypedExpr>,
    conversions: Vec<Conversion>,
    finalizer: &BindingFinalizer<'_>,
    originals: &[&ast::Expr],
    span: Span,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<TypedExpr>, SqlTypingError> {
    if args.len() != conversions.len() {
        return Err(TypingError::InvalidShape(
            "scalar typing returned the wrong number of conversions",
        )
        .into());
    }
    args.into_iter().zip(conversions).enumerate().map(|(ordinal, (source, conversion))| {
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
                    return Err(TypingError::InvalidShape("value conversion did not return a scalar").into());
                };
                let result = result.clone();
                let volatility = conversion.binding.semantics.volatility;
                let origin = match conversion.origin {
                    typing::ValueConversionOrigin::ValueDomain => Origin::ValueDomainConversion {
                        final_target: conversion.target,
                    },
                    typing::ValueConversionOrigin::ArrayCommonCoercion => Origin::ArrayCommonCoercion,
                    typing::ValueConversionOrigin::OperatorDecimalFloat => Origin::OperatorDecimalFloatCoercion,
                };
                let conversion_span = originals.get(ordinal).map_or(span, |source| source.span());
                let binding = finalizer.finish_with(
                    conversion.binding,
                    "implicit CAST",
                    conversion_span,
                    origin,
                    Some(conversion.expected_result),
                )?;
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
            cast_mode: conversion.mode,
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
