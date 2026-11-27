// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::{TextColumn, TextContext, TextError, register_constant};
use crate::testkit::claim_author::{legacy_scalar_function, physical_call_request};
use crate::typing::{
    CarrierConversion, ChildForm, ChildTyping, Conversion, LiteralEmission, LiteralSyntax,
    LiteralValue, NodeShape, NodeTyping, TypedLiteral, TypingContext, TypingError, finish_call,
    syntax_arithmetic, syntax_comparison, syntax_distinct_from, type_node,
};
use arrow::datatypes::DataType;
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::{BoundExprSetBuilder, ExprId, ExprKind, ValueId};
use novarocks_functions::{
    FunctionArgument, FunctionKind, FunctionResultType, ResolvedFunctionBinding,
};
use novarocks_parser::{Span, ast, canonical_call};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, FunctionParameterTypes, FunctionValueType,
    OperatorFunction,
};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct TextTyping<'a> {
    columns: &'a [TextColumn],
    by_name: BTreeMap<String, usize>,
    used: BTreeSet<usize>,
    context: TypingContext<'a>,
    functions: &'a novarocks_functions::EngineFunctionCatalog,
    lambda_frames: Vec<BTreeMap<String, (u32, FunctionValueType)>>,
}

impl<'a> TextTyping<'a> {
    pub(super) fn new(
        columns: &'a [TextColumn],
        context: &TextContext<'a>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, TextError> {
        let mut by_name = BTreeMap::new();
        let mut by_value = BTreeSet::new();
        for (ordinal, column) in columns.iter().enumerate() {
            work.step()?;
            if by_name
                .insert(column.name.to_ascii_lowercase(), ordinal)
                .is_some()
                || !by_value.insert(column.value)
            {
                return Err(TextError::Layout(
                    "duplicate input column name or value identity",
                ));
            }
        }
        let functions = context.functions.metadata();
        Ok(Self {
            columns,
            by_name,
            used: BTreeSet::new(),
            context: TypingContext {
                catalog: functions,
                constant_policy: context.constant_policy,
                decimal_overflow_policy: context.decimal_overflow_policy,
            },
            functions,
            lambda_frames: Vec::new(),
        })
    }

    pub(super) fn used_ordinals(&self) -> Vec<usize> {
        self.used.iter().copied().collect()
    }

    pub(super) fn lower(
        &mut self,
        expr: &ast::Expr,
        work: &mut CompileCheckpoints<'_>,
        depth: usize,
    ) -> Result<TextNode, TextError> {
        work.step()?;
        // Stop at the standalone unit's existing expression depth before
        // descending further on the host stack. N-ary terms share a level.
        if depth > crate::MAX_STATIC_EXPRESSION_DEPTH {
            return Err(TextError::Unit(
                novarocks_expression_contract::UnitError::Resource("expression semantic depth"),
            ));
        }
        let span = expr.span();
        let next = depth + 1;
        if let ast::Expr::Extract(extract) = expr {
            let source = self.lower(&extract.expression, work, next)?;
            work.flush()?;
            let binding = crate::typing::bind_extract(
                self.context.catalog,
                extract.field,
                &source.typing.argument(),
                work.control(),
            )
            .map_err(|cause| typing_error(cause, span))?;
            return Ok(TextNode::call(binding, None, vec![source]));
        }
        if let Some(call) = canonical_call(expr).map_err(TextError::UnsupportedSyntax)? {
            let shape = if call.name == "row" {
                Some(NodeShape::Row {
                    fields: call.row_fields,
                })
            } else if matches!(expr, ast::Expr::Coalesce(_)) {
                Some(NodeShape::Coalesce)
            } else {
                None
            };
            if let Some(shape) = shape {
                let children = call
                    .arguments
                    .into_iter()
                    .map(|arg| self.lower(arg, work, next))
                    .collect::<Result<Vec<_>, _>>()?;
                return self.type_call(shape, children, span, work);
            }
            return self.lower_call(call.name, call.arguments, span, work, next);
        }
        if let Some((syntax, target)) =
            crate::typing::clock_syntax(expr).map_err(|cause| typing_error(cause, span))?
        {
            work.flush()?;
            let binding = crate::typing::bind_temporal_syntax(
                self.context.catalog,
                syntax,
                &[],
                &target,
                work.control(),
            )
            .map_err(|cause| typing_error(cause, span))?;
            return Ok(TextNode::call(binding, Some(target), Vec::new()));
        }
        match expr {
            ast::Expr::Identifier(identifier) => {
                if !self.lambda_frames.is_empty() {
                    let name = identifier.value.to_lowercase();
                    for (parent, frame) in self.lambda_frames.iter().rev().enumerate() {
                        work.step()?;
                        if let Some((ordinal, value_type)) = frame.get(&name) {
                            return Ok(TextNode::new(
                                value_type.clone(),
                                TextKind::LambdaParameter {
                                    parent,
                                    ordinal: *ordinal,
                                },
                            ));
                        }
                    }
                }
                let Some(&ordinal) = self.by_name.get(&identifier.value.to_ascii_lowercase())
                else {
                    return Err(TextError::UnknownColumn {
                        name: identifier.value.clone(),
                        span,
                    });
                };
                self.used.insert(ordinal);
                let column = &self.columns[ordinal];
                Ok(TextNode::new(
                    column.value_type.clone(),
                    TextKind::Input(column.value),
                ))
            }
            ast::Expr::Literal(literal) => self.lower_literal(literal, work),
            ast::Expr::FunctionCall(call) => {
                let [name] = &call.name.parts[..] else {
                    return Err(legacy("qualified function call", span));
                };
                if call.window.is_some() {
                    return Err(legacy("window call", span));
                }
                if call.distinct
                    || call.order_by.is_some()
                    || call.filter.is_some()
                    || call.null_treatment.is_some()
                    || call.processing_mode.is_some()
                    || call.arguments.iter().any(|arg| arg.name.is_some())
                {
                    return Err(legacy("function call modifiers", span));
                }
                // The hidden overlay name is only reachable through dedicated syntax.
                let name = if name.value.eq_ignore_ascii_case("$overlay") {
                    "\"$overlay\""
                } else {
                    &name.value
                };
                self.lower_call(
                    name,
                    call.arguments.iter().map(|arg| &arg.value),
                    span,
                    work,
                    next,
                )
            }
            ast::Expr::Unary(unary) if unary.operator == ast::UnaryOperator::Minus => {
                let child = self.lower(&unary.expression, work, next)?;
                let form = negated_integer_form(&child.typing);
                let operator = crate::typing::negation_operator(&child.typing.value_type);
                let mut node =
                    self.type_call(NodeShape::Operator(operator), vec![child], span, work)?;
                node.typing.form = form;
                Ok(node)
            }
            ast::Expr::Unary(unary) if unary.operator == ast::UnaryOperator::Not => {
                let child = self.lower(&unary.expression, work, next)?;
                self.type_call(
                    NodeShape::Operator(novarocks_type_contract::OperatorFunction::BooleanNot),
                    vec![child],
                    span,
                    work,
                )
            }
            ast::Expr::Unary(unary) if unary.operator == ast::UnaryOperator::Plus => {
                let child = self.lower(&unary.expression, work, next)?;
                let (_, mut children) =
                    self.type_value(NodeShape::UnaryPlus, vec![child], span, work)?;
                Ok(children.pop().expect("one unary plus operand"))
            }
            ast::Expr::Binary(binary) => {
                let left = self.lower(&binary.left, work, next)?;
                let right = self.lower(&binary.right, work, next)?;
                use novarocks_type_contract::ArithmeticOperator as A;
                let operation = match binary.operator {
                    ast::BinaryOperator::Add => A::Add,
                    ast::BinaryOperator::Subtract => A::Subtract,
                    ast::BinaryOperator::Multiply => A::Multiply,
                    ast::BinaryOperator::Divide => A::Divide,
                    ast::BinaryOperator::Modulo => A::Modulo,
                };
                let operator = crate::typing::temporal_binary_operator(
                    operation,
                    &left.typing.value_type,
                    &right.typing.value_type,
                )
                .map_err(|cause| typing_error(cause, span))?
                .unwrap_or_else(|| syntax_arithmetic(binary.operator));
                self.type_call(NodeShape::Operator(operator), vec![left, right], span, work)
            }
            ast::Expr::AtLocal(node) => {
                let source = self.lower(&node.value, work, next)?;
                self.type_at_zone(source, None, span, work)
            }
            ast::Expr::AtTimeZone(node) => {
                let source = self.lower(&node.value, work, next)?;
                let zone = self.lower(&node.time_zone, work, next)?;
                self.type_at_zone(source, Some(zone), span, work)
            }
            ast::Expr::IntervalValueExpression(node) => {
                let left = self.lower(&node.left, work, next)?;
                let right = self.lower(&node.right, work, next)?;
                let [left_type, right_type, target] = crate::typing::qualified_datetime_difference(
                    &left.typing.value_type,
                    &right.typing.value_type,
                    &node.data_type,
                )
                .map_err(|cause| typing_error(cause, span))?;
                let left = self.qualified_cast(left, left_type, node.left.span(), work)?;
                let right = self.qualified_cast(right, right_type, node.right.span(), work)?;
                let difference = self.type_call(
                    NodeShape::Operator(OperatorFunction::TemporalSubtract),
                    vec![left, right],
                    span,
                    work,
                )?;
                self.qualified_cast(difference, target, span, work)
            }
            ast::Expr::Cast(cast) => {
                use crate::typing::declaration::resolve_trino_type;
                use novarocks_type_contract::TrinoCastMode;

                let input = self.lower(&cast.expression, work, next)?;
                let target =
                    resolve_trino_type(&cast.data_type, true, work.control()).map_err(|cause| {
                        let span = cause.span;
                        let error = if let Some(control) = cause.control.clone() {
                            TypingError::Control(control)
                        } else {
                            TypingError::TypeMismatch(cause.to_string())
                        };
                        typing_error(error, span)
                    })?;
                let mode = if cast.safe {
                    TrinoCastMode::Try
                } else {
                    TrinoCastMode::Explicit
                };
                let temporal_target = target
                    .with_nullable(cast.safe || input.typing.value_type.nullable())
                    .map_err(|cause| typing_error(cause.into(), span))?;
                work.flush()?;
                if let Some(binding) = crate::typing::select_temporal_cast(
                    self.context.catalog,
                    &input.typing.argument(),
                    &temporal_target,
                    cast.safe,
                    work.control(),
                )
                .map_err(|cause| typing_error(cause, span))?
                {
                    return Ok(TextNode::call(binding, Some(temporal_target), vec![input]));
                }
                let conversion = crate::typing::type_cast(
                    &self.context,
                    &input.typing.value_type,
                    target,
                    mode,
                    work,
                )
                .map_err(|cause| typing_error(cause, span))?;
                Ok(TextNode::cast(input, conversion))
            }
            ast::Expr::Dereference(dereference) => {
                let base = self.lower(&dereference.base, work, next)?;
                use crate::typing::{RowFieldResolution, resolve_row_field_name};
                let field = dereference.field.as_ref().ok_or_else(|| {
                    typing_error(
                        TypingError::InvalidShape("ROW dereference requires a field name"),
                        span,
                    )
                })?;
                let ordinal =
                    match resolve_row_field_name(&base.typing.value_type, &field.value, || {
                        work.step()
                    })? {
                        RowFieldResolution::Found(ordinal) => ordinal,
                        RowFieldResolution::NotRow => {
                            return Err(typing_error(
                                TypingError::InvalidShape("Field access requires a ROW value"),
                                span,
                            ));
                        }
                        RowFieldResolution::Missing => {
                            return Err(typing_error(
                                TypingError::InvalidShape("ROW field does not exist"),
                                span,
                            ));
                        }
                        RowFieldResolution::Ambiguous => {
                            return Err(typing_error(
                                TypingError::InvalidShape("Ambiguous row field reference"),
                                span,
                            ));
                        }
                    };
                self.row_field(base, ordinal, span, work)
            }
            ast::Expr::Subscript(subscript) => {
                let base = self.lower(&subscript.base, work, next)?;
                if let DataType::Struct(fields) = base.typing.value_type.data_type() {
                    let ast::Expr::Literal(ast::Literal {
                        kind: ast::LiteralKind::Long { parsed_value, .. },
                        ..
                    }) = &subscript.index
                    else {
                        return Err(typing_error(
                            TypingError::InvalidShape(
                                "Subscript expression on ROW requires a constant index",
                            ),
                            span,
                        ));
                    };
                    let ordinal = i32::try_from(*parsed_value)
                        .ok()
                        .and_then(|index| usize::try_from(i64::from(index) - 1).ok())
                        .filter(|&ordinal| ordinal < fields.len())
                        .ok_or_else(|| {
                            typing_error(
                                TypingError::InvalidShape("Invalid ROW field subscript"),
                                span,
                            )
                        })?;
                    return self.row_field(base, ordinal, span, work);
                }
                let index = self.lower(&subscript.index, work, next)?;
                self.type_call(
                    NodeShape::Operator(OperatorFunction::ArraySubscript),
                    vec![base, index],
                    span,
                    work,
                )
            }
            ast::Expr::Logical(logical) => {
                let operator = logical.operator;
                let mut pending: Vec<_> = logical.terms.iter().rev().collect();
                let mut children = Vec::new();
                while let Some(term) = pending.pop() {
                    work.step()?;
                    if let ast::Expr::Logical(inner) = term
                        && inner.operator == operator
                    {
                        pending.extend(inner.terms.iter().rev());
                    } else {
                        children.push(self.lower(term, work, next)?);
                    }
                }
                let shape = match operator {
                    ast::LogicalOperator::And => NodeShape::And,
                    ast::LogicalOperator::Or => NodeShape::Or,
                };
                let (ty, children) = self.type_value(shape, children, span, work)?;
                Ok(TextNode::new(ty, TextKind::Logical(operator, children)))
            }
            ast::Expr::Predicated(node) => match &node.predicate {
                ast::Predicate::Comparison(predicate) => {
                    let left = self.lower(&node.value, work, next)?;
                    let right = self.lower(&predicate.right, work, next)?;
                    self.type_call(
                        NodeShape::Operator(syntax_comparison(predicate.operator)),
                        vec![left, right],
                        span,
                        work,
                    )
                }
                ast::Predicate::DistinctFrom(predicate) => {
                    let left = self.lower(&node.value, work, next)?;
                    let right = self.lower(&predicate.right, work, next)?;
                    self.type_call(
                        NodeShape::Operator(syntax_distinct_from(predicate.negated)),
                        vec![left, right],
                        span,
                        work,
                    )
                }
                ast::Predicate::IsNull(predicate) => {
                    let child = self.lower(&node.value, work, next)?;
                    let (ty, mut children) =
                        self.type_value(NodeShape::IsNull, vec![child], span, work)?;
                    let child = Box::new(children.pop().expect("IS NULL has one operand"));
                    Ok(TextNode::new(
                        ty,
                        TextKind::IsNull(child, predicate.negated),
                    ))
                }
                predicate => Err(legacy(predicate_name(predicate), span)),
            },
            ast::Expr::SearchedCase(case) => {
                let mut children = Vec::new();
                for clause in &case.when_clauses {
                    let ast::WhenMatch::Operand(condition) = &clause.match_value else {
                        return Err(legacy("partial WHEN predicate", clause.span));
                    };
                    children.push(self.lower(condition, work, next)?);
                    children.push(self.lower(&clause.result, work, next)?);
                }
                if let Some(default) = &case.default_value {
                    children.push(self.lower(default, work, next)?);
                }
                let (ty, children) = self.type_value(
                    NodeShape::SearchedCase {
                        when_count: case.when_clauses.len(),
                        has_else: case.default_value.is_some(),
                    },
                    children,
                    span,
                    work,
                )?;
                Ok(TextNode::new(ty, TextKind::Case(children)))
            }
            other => Err(legacy(construct_name(other), span)),
        }
    }

    fn qualified_cast(
        &self,
        node: TextNode,
        target: FunctionValueType,
        span: Span,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<TextNode, TextError> {
        if node.typing.value_type == target {
            return Ok(node);
        }
        work.flush()?;
        let binding = crate::typing::select_temporal_cast(
            self.context.catalog,
            &node.typing.argument(),
            &target,
            false,
            work.control(),
        )
        .map_err(|cause| typing_error(cause, span))?
        .ok_or_else(|| legacy("qualified temporal CAST", span))?;
        Ok(TextNode::call(binding, Some(target), vec![node]))
    }

    fn lower_call<'expr>(
        &mut self,
        name: &str,
        arguments: impl IntoIterator<Item = &'expr ast::Expr>,
        span: Span,
        work: &mut CompileCheckpoints<'_>,
        depth: usize,
    ) -> Result<TextNode, TextError> {
        let name = name.to_ascii_lowercase();
        if self
            .functions
            .definition(&name, FunctionKind::Scalar)
            .is_none()
            && [
                FunctionKind::Aggregate,
                FunctionKind::Window,
                FunctionKind::Table,
            ]
            .into_iter()
            .any(|kind| self.functions.definition(&name, kind).is_some())
        {
            return Err(legacy(name, span));
        }
        let arguments = arguments.into_iter().collect::<Vec<_>>();
        if arguments
            .iter()
            .any(|arg| matches!(arg, ast::Expr::Lambda(_)))
        {
            if self
                .functions
                .definition(&name, FunctionKind::Scalar)
                .is_none()
                && let Some(overload) =
                    novarocks_functions::builtin::collection_lambda::unavailable_overload(&name)
            {
                return Err(typing_error(
                    novarocks_functions::FunctionBindingError::UnavailableImplementation(overload)
                        .into(),
                    span,
                ));
            }
            let [collection, ast::Expr::Lambda(lambda)] = arguments.as_slice() else {
                return Err(typing_error(
                    novarocks_functions::FunctionBindingError::NoMatchingOverload.into(),
                    span,
                ));
            };
            return self.lower_collection_lambda(&name, collection, lambda, span, work, depth);
        }
        let children = arguments
            .into_iter()
            .map(|arg| self.lower(arg, work, depth))
            .collect::<Result<Vec<_>, _>>()?;
        self.type_call(
            NodeShape::CatalogCall {
                name: &name,
                expected_result: None,
            },
            children,
            span,
            work,
        )
    }

    fn lower_collection_lambda(
        &mut self,
        name: &str,
        collection: &ast::Expr,
        lambda: &ast::LambdaExpr,
        span: Span,
        work: &mut CompileCheckpoints<'_>,
        depth: usize,
    ) -> Result<TextNode, TextError> {
        let collection = self.lower(collection, work, depth)?;
        let (candidate, conversion) = crate::typing::begin_collection_lambda(
            &self.context,
            name,
            &collection.typing,
            lambda.arguments.len(),
            work,
        )
        .map_err(|cause| typing_error(cause, span))?;
        let collection = apply_conversion(collection, conversion);
        let parameter_types = candidate.parameter_types();
        if lambda.arguments.len() != parameter_types.len() {
            return Err(typing_error(
                TypingError::InvalidShape("higher-order candidate changed the lambda arity"),
                lambda.span,
            ));
        }
        let mut frame = BTreeMap::new();
        for (ordinal, (argument, value_type)) in lambda
            .arguments
            .iter()
            .zip(parameter_types.iter())
            .enumerate()
        {
            work.step()?;
            let ordinal =
                u32::try_from(ordinal).map_err(|_| CompileControlError::ResourceExhausted)?;
            if frame
                .insert(
                    argument.name.value.to_lowercase(),
                    (ordinal, value_type.clone()),
                )
                .is_some()
            {
                return Err(typing_error(
                    TypingError::TypeMismatch(format!(
                        "Ambiguous lambda parameter: {}",
                        argument.name.value
                    )),
                    argument.span,
                ));
            }
        }
        self.lambda_frames.push(frame);
        let body = self.lower(&lambda.body, work, depth + 1);
        self.lambda_frames.pop();
        let body = body?;
        let binding = crate::typing::finish_collection_lambda(
            &self.context,
            &candidate,
            &collection.typing,
            &body.typing.value_type,
            work,
        )
        .map_err(|cause| typing_error(cause, span))?;
        if binding.admitted_execution_abi.is_none() {
            return Err(typing_error(
                novarocks_functions::FunctionBindingError::UnavailableImplementation(
                    binding.selected.overload.clone(),
                )
                .into(),
                span,
            ));
        }
        let lambda = TextNode::new(
            body.typing.value_type.clone(),
            TextKind::Lambda {
                parameter_types: candidate.parameter_types().clone(),
                body: Box::new(body),
            },
        );
        Ok(TextNode::call(binding, None, vec![collection, lambda]))
    }

    fn lower_literal(
        &self,
        literal: &ast::Literal,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<TextNode, TextError> {
        let syntax = match &literal.kind {
            ast::LiteralKind::Null => LiteralSyntax::Null,
            ast::LiteralKind::Boolean(value) => LiteralSyntax::Boolean(*value),
            ast::LiteralKind::Long { parsed_value, .. } => LiteralSyntax::Integer(*parsed_value),
            ast::LiteralKind::Double(value) => LiteralSyntax::Double(value.get()),
            ast::LiteralKind::Decimal(value) => LiteralSyntax::Decimal(value),
            ast::LiteralKind::String(value) => LiteralSyntax::String(value),
            ast::LiteralKind::Binary(value) => LiteralSyntax::Binary(value),
            ast::LiteralKind::Generic(value) => LiteralSyntax::TypedString {
                name: &value.type_name,
                value: &value.value,
            },
            ast::LiteralKind::Interval(value) => LiteralSyntax::Interval(value),
        };
        let NodeTyping::Literal(emission) =
            type_node(&self.context, NodeShape::Literal(syntax), &[], work)
                .map_err(|cause| typing_error(cause, literal.span))?
        else {
            unreachable!("literal typing returned an incompatible node");
        };
        Ok(match emission {
            LiteralEmission::Direct(value) => TextNode::literal(value),
            LiteralEmission::Constant { value_type, value } => TextNode {
                typing: ChildTyping {
                    value_type,
                    constant: Some(value),
                    form: ChildForm::Other,
                },
                kind: TextKind::Literal,
            },
            LiteralEmission::Cast {
                input,
                declared_result,
                conversion,
            } => {
                let mut node = TextNode::cast(TextNode::literal(input), conversion);
                node.typing.value_type = declared_result;
                node
            }
        })
    }

    fn type_at_zone(
        &self,
        mut source: TextNode,
        zone: Option<TextNode>,
        span: Span,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<TextNode, TextError> {
        let (syntax, zoned, target) = crate::typing::at_zone_syntax(
            &source.typing.value_type,
            zone.as_ref().map(|node| &node.typing.value_type),
        )
        .map_err(|cause| typing_error(cause, span))?;
        work.flush()?;
        if source.typing.value_type != zoned {
            let binding = crate::typing::select_temporal_cast(
                self.context.catalog,
                &source.typing.argument(),
                &zoned,
                false,
                work.control(),
            )
            .map_err(|cause| typing_error(cause, span))?
            .ok_or_else(|| legacy("AT TIME ZONE source conversion", span))?;
            source = TextNode::call(binding, Some(zoned), vec![source]);
        }
        let mut children = vec![source];
        children.extend(zone);
        let arguments = children
            .iter()
            .map(|node| node.typing.argument())
            .collect::<Vec<_>>();
        let binding = crate::typing::bind_temporal_syntax(
            self.context.catalog,
            syntax,
            &arguments,
            &target,
            work.control(),
        )
        .map_err(|cause| typing_error(cause, span))?;
        Ok(TextNode::call(binding, Some(target), children))
    }

    fn row_field(
        &self,
        base: TextNode,
        ordinal: usize,
        span: Span,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<TextNode, TextError> {
        let value = i64::try_from(ordinal).map_err(|_| {
            typing_error(
                TypingError::InvalidShape("ROW field ordinal exceeds INTEGER"),
                span,
            )
        })?;
        let index = self.lower_literal(
            &ast::Literal {
                kind: ast::LiteralKind::Long {
                    spelling: value.to_string(),
                    parsed_value: value,
                },
                span,
            },
            work,
        )?;
        self.type_call(NodeShape::RowField, vec![base, index], span, work)
    }

    fn type_call(
        &self,
        shape: NodeShape<'_>,
        mut children: Vec<TextNode>,
        span: Span,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<TextNode, TextError> {
        let NodeTyping::Call {
            selected,
            conversions,
        } = self.type_children(shape, &children, span, work)?
        else {
            unreachable!("call typing returned an incompatible node");
        };
        selected.operand_order().apply(&mut children);
        let children = apply_conversions(conversions, children);
        let facts = children
            .iter()
            .map(|child| child.typing.clone())
            .collect::<Vec<_>>();
        let binding = finish_call(&self.context, &selected, &facts, work)
            .map_err(|cause| typing_error(cause, span))?;
        Ok(TextNode::call(
            binding,
            selected.expected_result().cloned(),
            children,
        ))
    }

    fn type_value(
        &self,
        shape: NodeShape<'_>,
        children: Vec<TextNode>,
        span: Span,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(FunctionValueType, Vec<TextNode>), TextError> {
        let NodeTyping::Value {
            result,
            conversions,
        } = self.type_children(shape, &children, span, work)?
        else {
            unreachable!("structure typing returned an incompatible node");
        };
        Ok((result, apply_conversions(conversions, children)))
    }

    fn type_children(
        &self,
        shape: NodeShape<'_>,
        children: &[TextNode],
        span: Span,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<NodeTyping, TextError> {
        let facts = children
            .iter()
            .map(|child| child.typing.clone())
            .collect::<Vec<_>>();
        type_node(&self.context, shape, &facts, work).map_err(|cause| typing_error(cause, span))
    }
}

fn legacy(construct: impl Into<String>, span: Span) -> TextError {
    TextError::NotTypedByLayer {
        construct: construct.into(),
        span,
    }
}

fn typing_error(cause: TypingError, span: Span) -> TextError {
    match cause {
        TypingError::Control(cause) => TextError::Control(cause),
        cause => TextError::Typing { cause, span },
    }
}

fn negated_integer_form(child: &ChildTyping) -> ChildForm {
    let (ChildForm::IntegerLiteral(value) | ChildForm::NegatedIntegerLiteral(value)) = child.form
    else {
        return ChildForm::Other;
    };
    let negated = match child.value_type.data_type() {
        DataType::Int8 => i8::try_from(value)
            .ok()
            .and_then(i8::checked_neg)
            .map(i64::from),
        DataType::Int16 => i16::try_from(value)
            .ok()
            .and_then(i16::checked_neg)
            .map(i64::from),
        DataType::Int32 => i32::try_from(value)
            .ok()
            .and_then(i32::checked_neg)
            .map(i64::from),
        DataType::Int64 => value.checked_neg(),
        _ => None,
    };
    negated.map_or(ChildForm::Other, ChildForm::NegatedIntegerLiteral)
}

fn apply_conversions(conversions: Vec<Conversion>, children: Vec<TextNode>) -> Vec<TextNode> {
    assert_eq!(
        conversions.len(),
        children.len(),
        "conversion per source child"
    );
    children
        .into_iter()
        .zip(conversions)
        .map(|(child, conversion)| apply_conversion(child, conversion))
        .collect()
}

fn apply_conversion(child: TextNode, conversion: Conversion) -> TextNode {
    match conversion {
        Conversion::Keep => child,
        Conversion::RetypeLiteral(value) => TextNode::literal(value),
        Conversion::Cast(conversion) => TextNode::cast(child, conversion),
        Conversion::ValueDomain(conversion) => {
            let result = TextNode::call(
                conversion.binding,
                Some(conversion.expected_result),
                vec![child],
            );
            match conversion.tail {
                Some(tail) => TextNode::cast(result, tail),
                None => result,
            }
        }
    }
}

pub(super) struct TextNode {
    typing: ChildTyping,
    kind: TextKind,
}

enum TextKind {
    Input(ValueId),
    LambdaParameter {
        parent: usize,
        ordinal: u32,
    },
    Lambda {
        parameter_types: FunctionParameterTypes,
        body: Box<TextNode>,
    },
    Literal,
    Call {
        binding: Box<ResolvedFunctionBinding>,
        expected_result: Option<FunctionValueType>,
        arguments: Vec<TextNode>,
    },
    Cast {
        input: Box<TextNode>,
        conversion: CarrierConversion,
    },
    IsNull(Box<TextNode>, bool),
    Logical(ast::LogicalOperator, Vec<TextNode>),
    Case(Vec<TextNode>),
}

impl TextNode {
    fn new(value_type: FunctionValueType, kind: TextKind) -> Self {
        Self {
            typing: ChildTyping {
                value_type,
                constant: None,
                form: ChildForm::Other,
            },
            kind,
        }
    }

    fn literal(value: TypedLiteral) -> Self {
        let form = match value.value {
            LiteralValue::Null => ChildForm::NullLiteral,
            LiteralValue::Int(integer) => ChildForm::IntegerLiteral(integer),
            _ => ChildForm::Other,
        };
        Self {
            typing: ChildTyping {
                value_type: value.value_type,
                constant: Some(value.constant),
                form,
            },
            kind: TextKind::Literal,
        }
    }

    fn cast(input: TextNode, conversion: CarrierConversion) -> Self {
        Self::new(
            conversion.target.clone(),
            TextKind::Cast {
                input: Box::new(input),
                conversion,
            },
        )
    }

    fn call(
        binding: ResolvedFunctionBinding,
        expected_result: Option<FunctionValueType>,
        arguments: Vec<TextNode>,
    ) -> Self {
        let FunctionResultType::Scalar(ty) = &binding.selected.result_type else {
            unreachable!("the node has a final scalar binding");
        };
        Self::new(
            ty.clone(),
            TextKind::Call {
                binding: Box::new(binding),
                expected_result,
                arguments,
            },
        )
    }

    pub(super) fn emit(
        &self,
        draft: &mut BoundExprSetBuilder,
        constants: &mut ConstantPools,
        context: &TextContext<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ExprId, TextError> {
        self.emit_in_scope(draft, constants, context, &mut Vec::new(), work)
    }

    fn emit_in_scope(
        &self,
        draft: &mut BoundExprSetBuilder,
        constants: &mut ConstantPools,
        context: &TextContext<'_>,
        lambda_frames: &mut Vec<ExprId>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ExprId, TextError> {
        work.step()?;
        let lambda_scope = lambda_frames.last().copied();
        let mut ty = self.typing.value_type.clone();
        let kind = match &self.kind {
            TextKind::Input(value) => ExprKind::input(*value),
            TextKind::LambdaParameter { parent, ordinal } => ExprKind::LambdaParameter {
                lambda: *lambda_frames
                    .iter()
                    .rev()
                    .nth(*parent)
                    .ok_or(TextError::Layout(
                        "lambda parameter has no active lexical frame",
                    ))?,
                ordinal: *ordinal,
            },
            TextKind::Lambda {
                parameter_types,
                body,
            } => {
                let lambda = draft.reserve()?;
                lambda_frames.push(lambda);
                let body = body.emit_in_scope(draft, constants, context, lambda_frames, work);
                lambda_frames.pop();
                let body = body?;
                draft.insert(
                    lambda,
                    lambda_scope,
                    emitted_type(draft, body).clone(),
                    ExprKind::Lambda {
                        parameter_types: parameter_types.iter().cloned().collect(),
                        body,
                    },
                )?;
                return Ok(lambda);
            }
            TextKind::Literal => ExprKind::Constant(register_constant(
                constants,
                self.typing
                    .constant
                    .as_ref()
                    .expect("typed literal includes its checked constant"),
            )),
            TextKind::Cast { input, conversion } => {
                let expr = input.emit_in_scope(draft, constants, context, lambda_frames, work)?;
                let input_type = emitted_type(draft, expr);
                ty = conversion
                    .target
                    .with_nullable(conversion.target.nullable() || input_type.nullable())
                    .map_err(TextError::ValueType)?;
                ExprKind::Cast {
                    expr,
                    target: ty.data_type().clone(),
                    cast_mode: conversion
                        .mode
                        .unwrap_or(novarocks_type_contract::TrinoCastMode::Implicit),
                }
            }
            TextKind::IsNull(input, negated) => ExprKind::IsNull {
                expr: input.emit_in_scope(draft, constants, context, lambda_frames, work)?,
                negated: *negated,
            },
            TextKind::Logical(operator, children) => {
                let args = children
                    .iter()
                    .map(|child| {
                        child.emit_in_scope(draft, constants, context, lambda_frames, work)
                    })
                    .collect::<Result<Box<_>, _>>()?;
                if args.iter().any(|id| emitted_type(draft, *id).nullable()) {
                    ty = ty.with_nullable(true).map_err(TextError::ValueType)?;
                }
                match operator {
                    ast::LogicalOperator::And => ExprKind::Conjunction { args },
                    ast::LogicalOperator::Or => ExprKind::Disjunction { args },
                }
            }
            TextKind::Case(children) => {
                let (pairs, otherwise) = children.as_chunks::<2>();
                let when_then = pairs
                    .iter()
                    .map(|[condition, branch]| {
                        Ok((
                            condition.emit_in_scope(
                                draft,
                                constants,
                                context,
                                lambda_frames,
                                work,
                            )?,
                            branch.emit_in_scope(draft, constants, context, lambda_frames, work)?,
                        ))
                    })
                    .collect::<Result<Box<_>, TextError>>()?;
                let else_expr = otherwise
                    .first()
                    .map(|child| {
                        child.emit_in_scope(draft, constants, context, lambda_frames, work)
                    })
                    .transpose()?;
                ExprKind::Case {
                    ieee_float_equality: false,
                    operand: None,
                    when_then,
                    else_expr,
                }
            }
            TextKind::Call {
                binding,
                expected_result,
                arguments,
            } => {
                let args = arguments
                    .iter()
                    .map(|child| {
                        child.emit_in_scope(draft, constants, context, lambda_frames, work)
                    })
                    .collect::<Result<Box<_>, _>>()?;
                let mut changed = false;
                let row_constructor = binding.function_id.as_str() == "builtin.scalar/row/v1";
                let mut request_args = Vec::with_capacity(args.len());
                for (&id, argument) in args.iter().zip(arguments) {
                    work.step()?;
                    let emitted = draft.definition(id).expect("the child was just emitted");
                    changed |= if row_constructor
                        || binding.admitted_execution_abi
                            == Some(novarocks_functions::PureKernelAbi::HigherOrderV1)
                    {
                        emitted.ty != argument.typing.value_type
                    } else {
                        emitted.ty.nullable() != argument.typing.value_type.nullable()
                    };
                    request_args.push(match &emitted.kind {
                        ExprKind::Lambda {
                            parameter_types,
                            body,
                        } => FunctionArgument::Lambda {
                            parameter_types: FunctionParameterTypes::from_pure_boxed(
                                parameter_types.clone(),
                            ),
                            result_type: emitted_type(draft, *body).clone(),
                        },
                        _ => FunctionArgument::Value {
                            value_type: emitted.ty.clone(),
                            constant: argument.typing.constant.clone(),
                        },
                    });
                }
                let widened_target;
                let expected_result = if changed && row_constructor {
                    let original = expected_result.as_ref().ok_or_else(|| typing_error(
                        TypingError::InvalidShape("ROW constructor lost its original declaration"),
                        novarocks_parser::SYNTHETIC,
                    ))?;
                    widened_target = Some(crate::typing::rebind_row_constructor_result(
                        original, &request_args, work,
                    ).map_err(|cause| typing_error(cause, novarocks_parser::SYNTHETIC))?);
                    widened_target.as_ref()
                } else if changed
                    && binding.selected.overload.as_str()
                        == novarocks_functions::builtin::value_conversion::OPERATOR_DECIMAL_FLOAT_COERCION
                {
                    let [FunctionArgument::Value { value_type, .. }] = &request_args[..] else {
                        return Err(typing_error(
                            TypingError::InvalidShape("operator coercion requires its original operand"),
                            novarocks_parser::SYNTHETIC,
                        ));
                    };
                    widened_target = expected_result
                        .as_ref()
                        .map(|target| target.with_nullable(target.nullable() || value_type.nullable()))
                        .transpose()
                        .map_err(TextError::ValueType)?;
                    widened_target.as_ref()
                } else {
                    expected_result.as_ref()
                };
                let revised;
                let binding = if changed {
                    work.flush()?;
                    let request = novarocks_functions::FunctionBindingRequest {
                        arguments: &request_args,
                        logical_argument_count: request_args.len(),
                        expected_result_type: expected_result,
                    };
                    revised = context
                        .functions
                        .metadata()
                        .resolve_bound_trusted_at_overload(
                            &binding.function_id,
                            FunctionKind::Scalar,
                            &binding.selected.overload,
                            request,
                            work.control(),
                        )
                        .map_err(|cause| typing_error(cause.into(), novarocks_parser::SYNTHETIC))?;
                    let FunctionResultType::Scalar(result) = &revised.selected.result_type else {
                        unreachable!("the exact scalar overload has a scalar result")
                    };
                    ty = result.clone();
                    &revised
                } else {
                    binding.as_ref()
                };
                let mut function = legacy_scalar_function(binding, &ty);
                let declaration = context
                    .functions
                    .metadata()
                    .pure_overload_declaration_observed(
                        &binding.function_id,
                        binding.kind,
                        &binding.selected.overload,
                        work.control(),
                    )
                    .map_err(|error| match error {
                        novarocks_functions::FunctionSpecializationFailure::Control(cause) => {
                            TextError::Control(cause)
                        }
                        novarocks_functions::FunctionSpecializationFailure::Binding(cause) => {
                            typing_error(cause.into(), novarocks_parser::SYNTHETIC)
                        }
                        other => typing_error(
                            TypingError::Unsupported(other.to_string()),
                            novarocks_parser::SYNTHETIC,
                        ),
                    })?;
                let mut references = Vec::new();
                for &key in &declaration.effects().environment_dependencies {
                    let mut matching = None;
                    for (&id, value) in context.parameters.entries() {
                        work.step()?;
                        if value.key() == key && matching.replace(id).is_some() {
                            return Err(TextError::Layout("ambiguous temporal parameter"));
                        }
                    }
                    let id = matching.ok_or(TextError::Layout("missing temporal parameter"))?;
                    references.push(novarocks_type_contract::SemanticParameterRef {
                        id,
                        expected_key: key,
                    });
                }
                function
                    .legacy_metadata
                    .as_mut()
                    .expect("original author metadata")
                    .semantic_parameters = references.into_boxed_slice();
                let id = draft.add(lambda_scope, ty, ExprKind::FunctionCall { function, args })?;
                let request = physical_call_request(&request_args, expected_result, constants);
                draft.request(id, request)?;
                return Ok(id);
            }
        };
        Ok(draft.add(lambda_scope, ty, kind)?)
    }
}

fn emitted_type(draft: &BoundExprSetBuilder, id: ExprId) -> &FunctionValueType {
    &draft.definition(id).expect("the child was just emitted").ty
}

fn predicate_name(predicate: &ast::Predicate) -> &'static str {
    match predicate {
        ast::Predicate::Comparison(_) => "comparison",
        ast::Predicate::DistinctFrom(_) => "IS DISTINCT FROM",
        ast::Predicate::Between(_) => "BETWEEN",
        ast::Predicate::BooleanTest(_) => "truth-value predicate",
        ast::Predicate::IsNull(_) => "IS NULL",
        ast::Predicate::Like(_) => "LIKE",
        ast::Predicate::In(_) => "IN",
        ast::Predicate::Overlaps(_) => "OVERLAPS",
        ast::Predicate::QuantifiedComparison(_) => "quantified comparison",
        ast::Predicate::Match(_) => "MATCH",
    }
}

fn construct_name(expr: &ast::Expr) -> &'static str {
    use ast::Expr;
    match expr {
        Expr::JsonConstructor(_) => "JSON constructor",
        Expr::JsonExists(_) => "JSON_EXISTS",
        Expr::JsonValue(_) => "JSON_VALUE",
        Expr::JsonQuery(_) => "JSON_QUERY",
        Expr::JsonSerialize(_) => "JSON_SERIALIZE",
        Expr::JsonObject(_) => "JSON_OBJECT",
        Expr::JsonArray(_) => "JSON_ARRAY",
        Expr::AllRows(_) => "wildcard",
        Expr::Parameter(_) => "parameter",
        Expr::Try(_) => "TRY",
        Expr::Format(_) => "FORMAT",
        Expr::AtLocal(_) => "AT LOCAL",
        Expr::AtTimeZone(_) => "AT TIME ZONE",
        Expr::GroupingOperation(_) => "GROUPING",
        Expr::Overlay(_) => "OVERLAY",
        Expr::IntervalValueExpression(_) => "interval expression",
        Expr::MethodCall(_) | Expr::StaticMethodCall(_) => "qualified function or method call",
        Expr::CurrentCatalog(_) => "CURRENT_CATALOG",
        Expr::CurrentDate(_) => "CURRENT_DATE",
        Expr::CurrentPath(_) => "CURRENT_PATH",
        Expr::CurrentSchema(_) => "CURRENT_SCHEMA",
        Expr::CurrentUser(_) => "CURRENT_USER",
        Expr::CurrentTime(_) => "CURRENT_TIME",
        Expr::CurrentTimestamp(_) => "CURRENT_TIMESTAMP",
        Expr::LocalTime(_) => "LOCALTIME",
        Expr::LocalTimestamp(_) => "LOCALTIMESTAMP",
        Expr::WindowOperation(_) => "window operation",
        Expr::Unary(unary) => match unary.operator {
            ast::UnaryOperator::Not => "NOT",
            ast::UnaryOperator::Plus => "unary plus",
            ast::UnaryOperator::Minus => "unary minus",
        },
        Expr::Binary(_) => "binary operator",
        Expr::InList(_) => "IN list",
        Expr::Exists(_) => "EXISTS",
        Expr::SimpleCase(_) => "simple CASE",
        Expr::Unique(_) => "UNIQUE",
        Expr::Cast(_) => "CAST",
        Expr::Subquery(_) => "subquery",
        Expr::Array(_) => "ARRAY",
        Expr::Lambda(_) => "lambda",
        Expr::Subscript(_) | Expr::ArrayWildcardSubscript(_) => "subscript",
        Expr::Dereference(_) => "qualified name or field reference",
        Expr::Identifier(_)
        | Expr::Literal(_)
        | Expr::FunctionCall(_)
        | Expr::Extract(_)
        | Expr::If(_)
        | Expr::NullIf(_)
        | Expr::Coalesce(_)
        | Expr::Trim(_)
        | Expr::Logical(_)
        | Expr::Predicated(_)
        | Expr::SearchedCase(_)
        | Expr::Row(_) => {
            unreachable!("supported expression kinds are handled by the text skeleton")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{HarnessControl, constant_policy};
    use novarocks_type_contract::{
        CompilePhase, DecimalOverflowPolicy, SemanticParameters, TemporalPrecision, TemporalType,
    };

    #[test]
    fn temporal_text_literals_emit_a_typed_constant_before_call_authorship() {
        let functions = novarocks_functions::testkit::shared_installed_builtin_owner_catalogue();
        let parameters = SemanticParameters::default();
        let context = TextContext {
            functions: &functions,
            constant_policy: constant_policy(),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            parameters: &parameters,
        };
        let precision = TemporalPrecision::WHOLE_SECONDS;
        for (source, expected) in [
            ("TIME '01:02:03'", TemporalType::Time(precision)),
            (
                "TIMESTAMP '1970-01-01 00:00:01'",
                TemporalType::Timestamp(precision),
            ),
        ] {
            let ast = novarocks_parser::parser::parse_expression(source).unwrap();
            let mut work =
                CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
            let mut text = TextTyping::new(&[], &context, &mut work).unwrap();
            let node = text.lower(&ast, &mut work, 1).unwrap();
            let mut draft = BoundExprSetBuilder::new(super::super::unit_limits());
            let mut constants = ConstantPools::empty();
            let root = node
                .emit(&mut draft, &mut constants, &context, &mut work)
                .unwrap();
            let node = draft.definition(root).unwrap();
            let ExprKind::Constant(reference) = node.kind else {
                panic!("the temporal literal emits a typed constant");
            };
            let value = constants
                .resolve_observed(reference, &node.ty, &mut work)
                .unwrap();
            assert_eq!(value.value_type().value_type().temporal(), Some(expected));
            assert_eq!(value.value_type().data_type(), &expected.carrier());
            assert!(!value.value_type().nullable());
            work.finish().unwrap();
        }
    }
}
