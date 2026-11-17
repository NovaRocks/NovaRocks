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

//! The single scalar lowering author, borrowing the original node emission.
//! Relational topology and publication remain with ContractLoweringVisitor.
use super::*;

pub(super) struct ScalarEmission<'emission, 'control> {
    pub(super) root_allow_throw_exception: bool,
    pub(super) constant_policy: novarocks_constant_contract::ConstantPolicy,
    pub(super) root_allow_throw_exception_used: &'emission mut bool,
    pub(super) functions: &'emission Arc<dyn crate::compiler::SqlFunctionCatalog>,
    pub(super) control: &'control dyn PureCompileControl,
    pub(super) work: &'emission mut CompileCheckpoints<'control>,
    pub(super) current_fragment: FragmentId,
    pub(super) constant_emission: &'emission mut constant_emission::SqlConstantEmission,
    pub(super) lambda_scope: &'emission mut Vec<LoweringLambdaScope>,
    pub(super) fragments: &'emission mut BTreeMap<FragmentId, FragmentBuilder>,
    pub(super) definition_order: &'emission mut BTreeMap<FragmentId, NullableConstructionOrder>,
    pub(super) call_sources: &'emission mut SqlLogicalSourceJournal,
}
impl ScalarEmission<'_, '_> {
    fn fragment_mut(&mut self) -> &mut FragmentBuilder {
        self.fragments
            .get_mut(&self.current_fragment)
            .expect("the current fragment is allocated before lowering")
    }

    pub(super) fn root_allow_throw_reference(&mut self) -> SemanticParameterRef {
        *self.root_allow_throw_exception_used = true;
        SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::AllowThrowException,
        }
    }

    pub(super) fn value_declared_type_in(
        &mut self,
        fragment: FragmentId,
        value: ValueId,
    ) -> Result<ValueType, ContractLoweringError> {
        self.fragments
            .get_mut(&fragment)
            .and_then(|fragment| fragment.value(value))
            .map(|definition| definition.ty.clone())
            .ok_or(ContractLoweringError::IdentitySpaceExhausted("value"))
    }

    pub(super) fn lower_derived_variant_expression(
        &mut self,
        owner: NodeId,
        source_value: ValueId,
        source: &Arc<crate::common::variant_source::DerivedVariantSource>,
        expected_result: &ValueType,
    ) -> Result<ExprId, ContractLoweringError> {
        let binding = source.captured().binding().resolved();
        let source_type = self.value_declared_type(source_value)?;
        let source_expression =
            self.add_scoped_expression(owner, source_type, ContractExprKind::Value(source_value))?;
        let path_expression = self.lower_constant_expression(
            owner,
            source.canonical_path(),
            ValueType::new(DataType::Utf8, false),
        )?;
        let type_expression = self.lower_constant_expression(
            owner,
            source.type_literal(),
            ValueType::new(DataType::Utf8, false),
        )?;
        let mut channels = self.reserve_call_channels(3)?;
        channels.push(LoweredOperationalChannel {
            expression: source_expression,
            role: SqlOperationalChannelRole::ValueWithoutConstant,
        });
        self.work.step()?;
        channels.push(LoweredOperationalChannel {
            expression: path_expression,
            role: SqlOperationalChannelRole::DerivedVariantPath,
        });
        self.work.step()?;
        channels.push(LoweredOperationalChannel {
            expression: type_expression,
            role: SqlOperationalChannelRole::DerivedVariantTypeLiteral,
        });
        self.work.step()?;
        self.work.flush()?;
        let channels = channels.into_boxed_slice();
        self.work.step()?;
        let arguments = [source_expression, path_expression, type_expression];
        let canonical = self.author_canonical_call_selection(
            owner,
            source.captured(),
            Some(SqlExpressionCallKind::DerivedVariant),
            Some(source.as_ref()),
            &arguments,
            &channels,
        )?;
        let selected = canonical.selected();
        let novarocks_functions::FunctionResultType::Scalar(result) = &selected.result_type else {
            return Err(ContractLoweringError::InvalidFunctionBinding {
                detail: "canonical derived VARIANT owner selected a relation result".into(),
            });
        };
        if selected.aggregate.is_some() || result != expected_result {
            return Err(ContractLoweringError::InvalidFunctionBinding {
                detail: "canonical derived VARIANT result differs from its exact descriptor".into(),
            });
        }
        self.work.flush()?;
        let function = bound_function_from_selection(binding, selected, result);
        self.work.step()?;
        self.work.flush()?;
        let result_type = result.clone();
        self.work.step()?;
        self.work.flush()?;
        let expression = self.add_scoped_expression(
            owner,
            result_type.clone(),
            ContractExprKind::FunctionCall {
                function,
                args: Box::from(arguments),
            },
        )?;
        self.work.flush()?;
        let source = Arc::clone(source);
        self.work.step()?;
        self.work.flush()?;
        self.record_expression_source_with_data(
            owner,
            expression,
            LoweredExpressionLogicalSource::DerivedVariant(source),
            SqlExpressionCallKind::DerivedVariant,
            channels,
            Some(canonical),
        )?;
        Ok(expression)
    }

    pub(super) fn lower_boolean_connective(
        &mut self,
        owner: NodeId,
        root: &TypedExpr,
        connective: BinOp,
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<Box<[ExprId]>, ContractLoweringError> {
        let mut operands: Vec<&TypedExpr> = Vec::new();
        let mut pending: Vec<&TypedExpr> = vec![root];
        while let Some(current) = pending.pop() {
            match &current.kind {
                ExprKind::BinaryOp {
                    left, op, right, ..
                } if *op == connective => {
                    pending.push(right);
                    pending.push(left);
                }
                _ => operands.push(current),
            }
        }
        let mut args = Vec::with_capacity(operands.len());
        for operand in operands {
            args.push(self.lower_expression(owner, operand, visible)?);
        }
        Ok(args.into_boxed_slice())
    }

    pub(super) fn local_derived_value_expression(
        &mut self,
        owner: NodeId,
        value: ValueId,
    ) -> Result<Option<ExprId>, ContractLoweringError> {
        let fragment = &self.fragments[&self.current_fragment];
        let definition = fragment
            .value(value)
            .ok_or(ContractLoweringError::IdentitySpaceExhausted("value"))?;
        let ValueOrigin::Expr { node, root } = definition.origin else {
            return Ok(None);
        };
        if node != owner {
            return Ok(None);
        }
        let unit = super::super::unit_source::UnitDefinitions::from_draft(fragment, owner);
        let expression = unit.root(root).map(|root| root.definition).ok_or(
            ContractLoweringError::IdentitySpaceExhausted("scan derived root"),
        )?;
        let original =
            unit.definition(expression)
                .ok_or(ContractLoweringError::IdentitySpaceExhausted(
                    "scan derived definition",
                ))?;
        let scope = self.lambda_scope.last().map(|scope| scope.lambda);
        self.work.step()?;
        if !original.ty.exactly_equals_observed(&definition.ty, || {
            self.work.step().map_err(ContractLoweringError::from)
        })? {
            return Err(ContractLoweringError::InvalidFunctionBinding {
                detail: "local derived definition differs from its actual output type".into(),
            });
        }
        let entry = self
            .call_sources
            .expression_entries
            .get(&ScalarDefinitionAddress::new(
                ScalarSourceSite::Node {
                    fragment: self.current_fragment,
                    node: owner,
                },
                expression,
            ))
            .ok_or(ContractLoweringError::InvalidFunctionBinding {
                detail: "local derived value has no original source".into(),
            })?;
        let LoweredExpressionLogicalSource::DerivedVariant(source) = &entry.captured else {
            return Err(ContractLoweringError::InvalidFunctionBinding {
                detail: "local derived value has another producer".into(),
            });
        };
        if original.lambda_scope == scope {
            return Ok(Some(expression));
        }
        let raw = entry
            .channels
            .first()
            .and_then(|channel| unit.definition(channel.expression))
            .and_then(|definition| match definition.kind {
                ContractExprKind::Value(value) => Some(value),
                _ => None,
            })
            .ok_or(ContractLoweringError::InvalidFunctionBinding {
                detail: "derived source has no actual raw input value".into(),
            })?;
        self.work.flush()?;
        let source = Arc::clone(source);
        let expected = definition.ty.clone();
        self.work.step()?;
        self.lower_derived_variant_expression(owner, raw, &source, &expected)
            .map(Some)
    }

    pub(super) fn lower_expression(
        &mut self,
        owner: NodeId,
        expression: &TypedExpr,
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<ExprId, ContractLoweringError> {
        let result = self.lower_expression_inner(owner, expression, visible);
        if matches!(
            &result,
            Err(ContractLoweringError::Control(_)
                | ContractLoweringError::ConstantSource(
                    novarocks_constant_contract::ConstantError::Limit(_)
                ))
        ) {
            return result;
        }
        if result.is_err() {
            // Observe an ordinary expression refusal's completed tail. The
            // original control refusal remains primary and has no after-work.
            self.work.flush()?;
        }
        result
    }

    pub(super) fn lower_expression_inner(
        &mut self,
        owner: NodeId,
        expression: &TypedExpr,
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<ExprId, ContractLoweringError> {
        self.work.step()?;
        if let ExprKind::Literal(literal) = &expression.kind {
            return self.lower_literal_expression(owner, literal, expression);
        }
        let mut ty = expression_type(expression);
        let mut captured_call = None;
        let kind = match &expression.kind {
            ExprKind::ColumnRef { column_id, .. } => {
                // A reference is the value, so it is typed by the value's own
                // definition rather than by what the statement was analyzed
                // to say about it. The two differ where an operator between
                // the two changed it: a grouping set nulls a key the analyzer
                // had already read as never null.
                let value = visible
                    .get(column_id)
                    .copied()
                    .ok_or(ContractLoweringError::UnknownColumnReference(*column_id))?;
                if let Some(definition) = self.local_derived_value_expression(owner, value)? {
                    return Ok(definition);
                }
                ty = self.value_declared_type(value)?;
                ContractExprKind::Value(value)
            }
            ExprKind::Literal(_) => unreachable!("literal expressions return before dispatch"),
            ExprKind::Constant(value) => return self.lower_constant_expression(owner, value, ty),
            ExprKind::BinaryOp {
                op: op @ (BinOp::And | BinOp::Or),
                ..
            } => {
                let args = self.lower_boolean_connective(owner, expression, *op, visible)?;
                if matches!(op, BinOp::And) {
                    ContractExprKind::Conjunction { args }
                } else {
                    ContractExprKind::Disjunction { args }
                }
            }
            ExprKind::BinaryOp {
                left,
                op,
                right,
                decimal_overflow_policy,
            } => {
                let lowered_left = self.lower_expression(owner, left, visible)?;
                let lowered_right = self.lower_expression(owner, right, visible)?;
                // A comparison answers about one type. Its operands were
                // reconciled while the statement was analyzed -- an untyped
                // NULL, a narrower integer -- and the plan states the
                // comparison it actually performs rather than two sides the
                // reader has to reconcile again. Arithmetic keeps its own
                // operands: its result type is derived from the pair.
                let (lowered_left, lowered_right) = if is_comparison_operator(*op) {
                    let left_type = self.expression_value_type(owner, lowered_left)?;
                    let right_type = self.expression_value_type(owner, lowered_right)?;
                    let compared = self.common_comparison_value_type(&left_type, &right_type)?;
                    (
                        self.cast_expression_to(owner, lowered_left, &compared)?,
                        self.cast_expression_to(owner, lowered_right, &compared)?,
                    )
                } else {
                    (lowered_left, lowered_right)
                };
                let allow_throw_exception =
                    crate::compiler::selected_scalar_call::arithmetic_allows_throw(*op)
                        .then(|| self.root_allow_throw_reference());
                let op = lower_binary_operator(*op);
                ContractExprKind::Binary {
                    allow_throw_exception,
                    left: lowered_left,
                    op,
                    right: lowered_right,
                    decimal_overflow_policy: *decimal_overflow_policy,
                }
            }
            ExprKind::UnaryOp { op, expr } => ContractExprKind::Unary {
                op: lower_unary_operator(*op),
                expr: self.lower_expression(owner, expr, visible)?,
            },
            ExprKind::Cast {
                expr,
                decimal_overflow_policy,
                ..
            } => {
                return self.lower_expression_as(
                    owner,
                    expr,
                    visible,
                    &ty,
                    *decimal_overflow_policy,
                );
            }
            ExprKind::IsNull { expr, negated } => ContractExprKind::IsNull {
                expr: self.lower_expression(owner, expr, visible)?,
                negated: *negated,
            },
            ExprKind::InList {
                expr,
                list,
                negated,
            } => {
                // A membership test compares one value against many, so all
                // of them are compared as one type. The engine widens them at
                // run time; the contract states the comparison it performs,
                // so the widening is written down here -- the same way a
                // range test and a comparison operator already do.
                let input = self.lower_expression(owner, expr, visible)?;
                let candidates = list
                    .iter()
                    .map(|item| self.lower_expression(owner, item, visible))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut compared = self.expression_value_type(owner, input)?;
                for candidate in &candidates {
                    let other = self.expression_value_type(owner, *candidate)?;
                    compared = self.common_comparison_value_type(&compared, &other)?;
                }
                let list = candidates
                    .into_iter()
                    .map(|candidate| self.cast_expression_to(owner, candidate, &compared))
                    .collect::<Result<Vec<_>, _>>()?;
                ContractExprKind::InList {
                    expr: self.cast_expression_to(owner, input, &compared)?,
                    list: list.into_boxed_slice(),
                    negated: *negated,
                }
            }
            ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } => {
                // A range test compares one value against two bounds, so all
                // three are compared as one type. The engine widens them at
                // run time; the contract states the comparison it performs,
                // so the widening is written down here.
                let input = self.lower_expression(owner, expr, visible)?;
                let low = self.lower_expression(owner, low, visible)?;
                let high = self.lower_expression(owner, high, visible)?;
                let mut compared = self.expression_value_type(owner, input)?;
                for operand in [low, high] {
                    let other = self.expression_value_type(owner, operand)?;
                    compared = self.common_comparison_value_type(&compared, &other)?;
                }
                ContractExprKind::Between {
                    expr: self.cast_expression_to(owner, input, &compared)?,
                    low: self.cast_expression_to(owner, low, &compared)?,
                    high: self.cast_expression_to(owner, high, &compared)?,
                    negated: *negated,
                }
            }
            ExprKind::Like {
                expr,
                pattern,
                negated,
            } => ContractExprKind::Like {
                expr: self.lower_expression(owner, expr, visible)?,
                pattern: self.lower_expression(owner, pattern, visible)?,
                negated: *negated,
            },
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                // Every branch answers with the type the whole expression
                // answers with. A branch that says only NULL, or says a
                // narrower number than its siblings, was reconciled while the
                // statement was analyzed; carry that reconciliation rather
                // than leaving each branch its own type.
                let mut operand = operand
                    .as_deref()
                    .map(|item| self.lower_expression(owner, item, visible))
                    .transpose()?;
                let mut whens = Vec::with_capacity(when_then.len());
                for (when, _) in when_then {
                    whens.push(self.lower_expression(owner, when, visible)?);
                }
                // A simple CASE compares its operand against every label, so
                // the plan states the one type it compares them in the same
                // way a comparison operator does -- an INT operand against a
                // BIGINT label is one comparison, not two types.
                if let Some(subject) = operand {
                    let mut compared = self.expression_value_type(owner, subject)?;
                    for when in &whens {
                        let when_type = self.expression_value_type(owner, *when)?;
                        compared = self.common_comparison_value_type(&compared, &when_type)?;
                    }
                    operand = Some(self.cast_expression_to(owner, subject, &compared)?);
                    for when in &mut whens {
                        *when = self.cast_expression_to(owner, *when, &compared)?;
                    }
                }
                let mut branches = Vec::with_capacity(when_then.len());
                for ((_, then), when) in when_then.iter().zip(whens) {
                    branches.push((
                        when,
                        self.lower_expression_as(
                            owner,
                            then,
                            visible,
                            &ty,
                            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                        )?,
                    ));
                }
                let else_expr = match else_expr.as_deref() {
                    Some(item) => Some(self.lower_expression_as(
                        owner,
                        item,
                        visible,
                        &ty,
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    )?),
                    None => None,
                };
                ContractExprKind::Case {
                    operand,
                    when_then: branches.into_boxed_slice(),
                    else_expr,
                }
            }
            ExprKind::IsTruthValue {
                expr,
                value,
                negated,
            } => ContractExprKind::IsTruthValue {
                expr: self.lower_expression(owner, expr, visible)?,
                value: *value,
                negated: *negated,
            },
            ExprKind::FunctionCall {
                args,
                distinct,
                binding,
                volatility,
                name,
            } => {
                if *distinct {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: "scalar call carries DISTINCT".to_string(),
                    });
                }
                if binding.kind != novarocks_physical_plan::FunctionKind::Scalar {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: format!("scalar expression carries {:?} binding", binding.kind),
                    });
                }
                if binding.logical_argument_count != args.len()
                    || binding.selected.argument_types.len() != args.len()
                {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: format!(
                            "binding arity logical={} typed={} differs from expression arity {}",
                            binding.logical_argument_count,
                            binding.selected.argument_types.len(),
                            args.len()
                        ),
                    });
                }
                if *volatility != binding.semantics.volatility {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: "legacy volatility differs from the exact binding".to_string(),
                    });
                }
                let novarocks_functions::FunctionResultType::Scalar(result_type) =
                    &binding.selected.result_type
                else {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: "scalar expression carries a relation result".to_string(),
                    });
                };
                let result_type = full_source_type(result_type);
                if result_type != ty {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: format!(
                            "function {name}: binding result {result_type:?} differs from expression result {ty:?}"
                        ),
                    });
                }
                self.work.flush()?;
                let captured = capture_logical_call_arguments(
                    binding,
                    args.len(),
                    args.iter(),
                    self.constant_policy,
                    self.control,
                )?;
                self.work.flush()?;
                let mut argument_types = Vec::new();
                argument_types
                    .try_reserve_exact(args.len())
                    .map_err(|_| CompileControlError::ResourceExhausted)?;
                self.work.step()?;
                self.work.flush()?;
                for argument in binding.selected.argument_types.iter() {
                    argument_types.push(full_source_argument(argument));
                    self.work.step()?;
                }
                self.work.flush()?;
                let argument_types = argument_types.into_boxed_slice();
                self.work.step()?;
                self.work.flush()?;
                let mut lowered_args = Vec::new();
                lowered_args
                    .try_reserve_exact(args.len())
                    .map_err(|_| CompileControlError::ResourceExhausted)?;
                self.work.step()?;
                self.work.flush()?;
                let mut channels =
                    self.reserve_call_channels(captured.request().arguments.len())?;
                for (ordinal, (argument, expected)) in
                    args.iter().zip(argument_types.iter()).enumerate()
                {
                    let lowered = self.lower_captured_scalar_argument(
                        owner, argument, ordinal, &captured, visible, expected,
                    )?;
                    lowered_args.push(lowered.expression);
                    self.work.step()?;
                    channels.push(lowered);
                    self.work.step()?;
                }
                self.work.flush()?;
                let lowered_args = lowered_args.into_boxed_slice();
                self.work.step()?;
                self.work.flush()?;
                self.work.flush()?;
                let channels = channels.into_boxed_slice();
                self.work.step()?;
                let canonical = self.author_canonical_call_selection(
                    owner,
                    &captured,
                    Some(SqlExpressionCallKind::Scalar),
                    None,
                    &lowered_args,
                    &channels,
                )?;
                let selected = canonical.selected();
                let novarocks_functions::FunctionResultType::Scalar(result) = &selected.result_type
                else {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: "canonical scalar owner selected a relation result".into(),
                    });
                };
                self.work.flush()?;
                let argument_types = selected.argument_types.clone();
                self.work.step()?;
                self.work.flush()?;
                let result_type = result.clone();
                self.work.step()?;
                self.work.flush()?;
                ty = result_type.clone();
                self.work.step()?;
                captured_call = Some((captured, channels, canonical));
                ContractExprKind::FunctionCall {
                    function: BoundFunction {
                        legacy_metadata: Some(
                            novarocks_expression_contract::LegacyBindingMetadata {
                                semantic_parameters: Box::default(),
                                volatility: binding.semantics.volatility,
                                argument_evaluation: binding.semantics.argument_evaluation,
                                failure_behavior: binding.semantics.failure_behavior,
                                intrinsic_row_error: binding.semantics.intrinsic_row_error,
                            },
                        ),
                        function_id: binding.function_id.clone(),
                        overload: binding.selected.overload.clone(),
                        kind: binding.kind,
                        argument_types,
                        result_type,
                    },
                    args: lowered_args,
                }
            }
            ExprKind::LambdaFunction { params, body } => {
                // The lambda's identity exists before its body, because every
                // parameter reference inside names it.
                let lambda = self.reserve_definition(owner)?;
                // A parameter's type is the plan's, the same way every other
                // type it states is: a provider's decoration on a nested
                // field would make the declaration and the references to it
                // read as two different types.
                let parameter_types = params
                    .iter()
                    .map(|param| full_source_type(&param.value_type))
                    .collect::<Vec<_>>();
                let enclosing = self.lambda_scope.last().map(|scope| scope.lambda);
                self.lambda_scope.push(LoweringLambdaScope {
                    lambda,
                    parameter_slots: params.iter().map(|param| param.slot_id).collect(),
                    parameter_types: parameter_types.clone(),
                });
                let body_result = self.lower_expression(owner, body, visible);
                self.lambda_scope.pop();
                let body_id = body_result?;
                let body_type = self.expression_value_type(owner, body_id)?;
                self.fragment_mut().unit(owner)?.insert(
                    lambda,
                    enclosing,
                    body_type,
                    ContractExprKind::Lambda {
                        parameter_types: parameter_types.into_boxed_slice(),
                        body: body_id,
                    },
                )?;
                return Ok(lambda);
            }
            ExprKind::LambdaParamRef { name, slot_id } => {
                if self.lambda_scope.is_empty() {
                    return Err(ContractLoweringError::InvalidLambda {
                        detail: format!("lambda parameter `{name}` stands outside a lambda"),
                    });
                }
                // A name is resolved by the innermost lambda that declares
                // it, so an inner body reading an outer parameter names the
                // lambda that owns it rather than the one it stands in.
                let (lambda, ordinal, declared) = self
                    .lambda_scope
                    .iter()
                    .rev()
                    .find_map(|scope| {
                        let ordinal = scope
                            .parameter_slots
                            .iter()
                            .position(|slot| slot == slot_id)?;
                        Some((
                            scope.lambda,
                            ordinal,
                            scope.parameter_types.get(ordinal)?.clone(),
                        ))
                    })
                    .ok_or_else(|| ContractLoweringError::InvalidLambda {
                        detail: format!(
                            "lambda parameter `{name}` is not one any open lambda declares"
                        ),
                    })?;
                // The reference is the parameter, so it is typed by the
                // declaration it names -- not by what the statement was
                // analyzed to say at this position.
                ty = declared;
                ContractExprKind::LambdaParameter {
                    lambda,
                    ordinal: u32::try_from(ordinal).map_err(|_| {
                        ContractLoweringError::InvalidLambda {
                            detail: "lambda parameter ordinal exceeds u32".to_string(),
                        }
                    })?,
                }
            }
            ExprKind::Nested(inner) => return self.lower_expression(owner, inner, visible),
            other => {
                return Err(ContractLoweringError::UnsupportedExpression {
                    kind: expression_kind_name(other),
                });
            }
        };
        // Nullability widens on the way out. An operator whose operand may be
        // null may answer null, whatever the statement was analyzed to say
        // about an operand that has since passed through something that nulls
        // it. Only the kinds that answer about their operands' values widen;
        // `IS NULL` answers about the absence itself and never does.
        if !ty.nullable() && kind_follows_operand_nullability(&kind) {
            let mut operands = Vec::new();
            collect_operand_expressions(&kind, &mut operands);
            for operand in operands {
                if self.expression_value_type(owner, operand)?.nullable() {
                    ty = ty
                        .with_nullable(true)
                        .expect("validated root NULL contract");
                    break;
                }
            }
        }
        let emitted = self.add_scoped_expression(owner, ty, kind)?;
        if let Some((captured, channels, canonical)) = captured_call {
            self.record_expression_source(
                owner,
                emitted,
                captured,
                SqlExpressionCallKind::Scalar,
                channels,
                Some(canonical),
            )?;
        }
        Ok(emitted)
    }

    pub(super) fn author_canonical_call_selection(
        &mut self,
        owner: NodeId,
        captured: &CapturedLogicalCallArguments,
        kind: Option<SqlExpressionCallKind>,
        derived: Option<&crate::common::variant_source::DerivedVariantSource>,
        arguments: &[ExprId],
        channels: &[LoweredOperationalChannel],
    ) -> Result<Arc<CanonicalCallOperationalRequest>, ContractLoweringError> {
        self.work.flush()?;
        let projection = project_emitted_call_arguments_observed(
            EmittedOperationalCall {
                expressions: super::super::unit_source::UnitDefinitions::from_draft(
                    &self.fragments[&self.current_fragment],
                    owner,
                ),
                pools: self.constant_emission.pools(),
                owner: ScalarUnitOwner::Node(owner),
                lambda_scope: match kind {
                    Some(
                        SqlExpressionCallKind::Scalar
                        | SqlExpressionCallKind::ValueConversion
                        | SqlExpressionCallKind::DerivedVariant,
                    ) => self.lambda_scope.last().map(|scope| scope.lambda),
                    _ => None,
                },
                kind,
                captured: CapturedOperationalSource::call(captured),
                derived,
                arguments,
                channels,
            },
            self.control,
        )
        .map_err(ContractLoweringError::from);
        let operational = self.completed_specialization_result(projection)?;
        let logical_count = captured.request().logical_argument_count;
        let selected = self.select_canonical_operational_request(
            captured.binding(),
            logical_count,
            &operational,
        )?;
        self.work.flush()?;
        let binding = captured.binding().clone();
        self.work.step()?;
        self.work.flush()?;
        let canonical = Arc::new(CanonicalCallOperationalRequest {
            binding,
            arguments: operational,
            logical_count,
            selected,
            result_constraint: captured.binding().result_constraint().cloned(),
        });
        self.work.step()?;
        self.work.flush()?;
        Ok(canonical)
    }

    pub(super) fn select_canonical_operational_request(
        &mut self,
        binding: &SqlFunctionBinding,
        logical_argument_count: usize,
        arguments: &[novarocks_functions::FunctionArgument],
    ) -> Result<Arc<novarocks_functions::FunctionBindingSelection>, ContractLoweringError> {
        self.select_canonical_request_with_constraint(
            binding,
            logical_argument_count,
            arguments,
            binding.result_constraint(),
        )
    }

    pub(super) fn select_canonical_request_with_constraint(
        &mut self,
        binding: &SqlFunctionBinding,
        logical_argument_count: usize,
        arguments: &[novarocks_functions::FunctionArgument],
        constraint: Option<&novarocks_functions::FunctionValueType>,
    ) -> Result<Arc<novarocks_functions::FunctionBindingSelection>, ContractLoweringError> {
        self.work.flush()?;
        let selection = crate::compiler::selected_scalar_call::canonical_scalar_request_observed(
            self.functions.as_ref(),
            binding,
            logical_argument_count,
            arguments,
            constraint,
            self.control,
        )
        .map_err(|error| match error {
            novarocks_functions::FunctionBindingError::Control(cause) => {
                ContractLoweringError::Control(cause)
            }
            error => ContractLoweringError::InvalidFunctionBinding {
                detail: format!("canonical call selection: {error}"),
            },
        });
        self.completed_specialization_result(selection)
    }

    pub(super) fn completed_specialization_result<T>(
        &mut self,
        outcome: Result<T, ContractLoweringError>,
    ) -> Result<T, ContractLoweringError> {
        if matches!(&outcome, Err(ContractLoweringError::Control(_))) {
            return outcome;
        }
        self.work.step()?;
        self.work.flush()?;
        outcome
    }

    pub(super) fn record_expression_source(
        &mut self,
        owner: NodeId,
        emitted: ExprId,
        captured: CapturedLogicalCallArguments,
        kind: SqlExpressionCallKind,
        channels: Box<[LoweredOperationalChannel]>,
        canonical_operational: Option<Arc<CanonicalCallOperationalRequest>>,
    ) -> Result<(), ContractLoweringError> {
        self.record_expression_source_with_data(
            owner,
            emitted,
            LoweredExpressionLogicalSource::Owned(captured),
            kind,
            channels,
            canonical_operational,
        )
    }

    pub(super) fn record_expression_source_with_data(
        &mut self,
        owner: NodeId,
        emitted: ExprId,
        captured: LoweredExpressionLogicalSource,
        kind: SqlExpressionCallKind,
        channels: Box<[LoweredOperationalChannel]>,
        canonical_operational: Option<Arc<CanonicalCallOperationalRequest>>,
    ) -> Result<(), ContractLoweringError> {
        self.work.flush()?;
        let mut arguments = Vec::new();
        arguments
            .try_reserve_exact(captured.request().arguments.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.work.step()?;
        self.work.flush()?;
        let lambda_scope = self.lambda_scope.last().map(|scope| scope.lambda);
        let source = self.fragments[&self.current_fragment]
            .construction_unit(owner)
            .and_then(|unit| unit.definition(emitted))
            .ok_or(ContractLoweringError::IdentitySpaceExhausted("expression"))?;
        let args = match (&source.kind, kind) {
            (
                ContractExprKind::FunctionCall { args, .. },
                SqlExpressionCallKind::Scalar
                | SqlExpressionCallKind::ValueConversion
                | SqlExpressionCallKind::DerivedVariant,
            ) => args.as_ref(),
            _ => {
                return Err(ContractLoweringError::InvalidFunctionBinding {
                    detail: "captured expression source has a different emission lifecycle".into(),
                });
            }
        };
        let count = args.len();
        self.work.step()?;
        if count != channels.len() || count != captured.request().arguments.len() {
            return Err(ContractLoweringError::InvalidFunctionBinding {
                detail: "recorded call channel count differs from its original emission".into(),
            });
        }
        for (argument, channel) in args.iter().copied().zip(channels.iter()) {
            let same = argument == channel.expression;
            self.work.step()?;
            if !same {
                return Err(ContractLoweringError::InvalidFunctionBinding {
                    detail: "recorded call channel differs from its original expression".into(),
                });
            }
            arguments.push(argument);
            self.work.step()?;
        }
        self.work.flush()?;
        let arguments = arguments.into_boxed_slice();
        self.work.step()?;
        self.work.flush()?;
        let key = ScalarDefinitionAddress::new(
            ScalarSourceSite::Node {
                fragment: self.current_fragment,
                node: owner,
            },
            emitted,
        );
        let duplicate = self.call_sources.expression_entries.contains_key(&key);
        self.work.step()?;
        if duplicate {
            return Err(ContractLoweringError::InvalidFunctionBinding {
                detail: "scalar source emission is duplicated".into(),
            });
        }
        // Map allocation and retained request/type/backing coexistence remain
        // caller admission obligations, not an independently granted wallet.
        self.call_sources.expression_entries.insert(
            key,
            LoweredExpressionSourceEntry {
                kind,
                canonical_operational,
                captured,
                owner: ScalarUnitOwner::Node(owner),
                lambda_scope,
                arguments,
                channels,
            },
        );
        self.work.step()?;
        Ok(())
    }

    pub(super) fn lower_captured_scalar_argument(
        &mut self,
        owner: NodeId,
        argument: &TypedExpr,
        ordinal: usize,
        captured: &CapturedLogicalCallArguments,
        visible: &BTreeMap<ColumnId, ValueId>,
        expected: &FunctionArgumentType,
    ) -> Result<LoweredOperationalChannel, ContractLoweringError> {
        // Preserve the original binding's canonical bare-NULL conversion.
        // The retained original request still says what analysis authored.
        if let Some(target) = crate::compiler::selected_scalar_call::canonical_null_target(
            &argument.value_type,
            expected,
        ) {
            let (expression, value) =
                self.author_literal_expression_with_source(owner, &LiteralValue::Null, target)?;
            let channel = LoweredOperationalChannel {
                expression,
                role: SqlOperationalChannelRole::CanonicalNull { value },
            };
            self.work.step()?;
            return Ok(channel);
        }
        if matches!(argument.kind, ExprKind::Literal(_) | ExprKind::Constant(_)) {
            self.work.step()?;
            let Some(novarocks_functions::FunctionArgument::Value {
                constant: Some(value),
                ..
            }) = captured.request().arguments.get(ordinal)
            else {
                return Err(ContractLoweringError::InvalidFunctionBinding {
                    detail: "literal scalar argument has no captured constant source".into(),
                });
            };
            let expression =
                self.lower_constant_expression(owner, value, expression_type(argument))?;
            return self.original_emission_channel(expression, ordinal, captured);
        }
        let expression = self.lower_bound_argument(owner, argument, visible, expected)?;
        self.original_emission_channel(expression, ordinal, captured)
    }

    pub(super) fn lower_captured_unbound_argument(
        &mut self,
        owner: NodeId,
        expression: &TypedExpr,
        ordinal: usize,
        captured: &CapturedLogicalCallArguments,
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<LoweredOperationalChannel, ContractLoweringError> {
        if matches!(
            expression.kind,
            ExprKind::Literal(_) | ExprKind::Constant(_)
        ) {
            let argument = captured.request().arguments.get(ordinal);
            self.work.step()?;
            let Some(novarocks_functions::FunctionArgument::Value {
                constant: Some(value),
                ..
            }) = argument
            else {
                return Err(ContractLoweringError::InvalidFunctionBinding {
                    detail: "literal call channel has no captured constant source".into(),
                });
            };
            let emitted =
                self.lower_constant_expression(owner, value, expression_type(expression))?;
            return self.original_emission_channel(emitted, ordinal, captured);
        }
        let emitted = self.lower_expression(owner, expression, visible)?;
        self.original_emission_channel(emitted, ordinal, captured)
    }

    pub(super) fn original_emission_channel(
        &mut self,
        expression: ExprId,
        ordinal: usize,
        captured: &CapturedLogicalCallArguments,
    ) -> Result<LoweredOperationalChannel, ContractLoweringError> {
        let argument = captured.request().arguments.get(ordinal);
        self.work.step()?;
        let role = match argument {
            Some(novarocks_functions::FunctionArgument::Value { constant: None, .. }) => {
                SqlOperationalChannelRole::ValueWithoutConstant
            }
            Some(novarocks_functions::FunctionArgument::Value {
                constant: Some(_), ..
            }) => SqlOperationalChannelRole::CapturedConstant,
            Some(novarocks_functions::FunctionArgument::Lambda { .. }) => {
                SqlOperationalChannelRole::Lambda
            }
            None => {
                return Err(ContractLoweringError::InvalidFunctionBinding {
                    detail: "emitted call channel has no original request occurrence".into(),
                });
            }
        };
        self.work.step()?;
        Ok(LoweredOperationalChannel { expression, role })
    }

    pub(super) fn reserve_call_channels(
        &mut self,
        count: usize,
    ) -> Result<Vec<LoweredOperationalChannel>, ContractLoweringError> {
        if count > novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        std::alloc::Layout::array::<LoweredOperationalChannel>(count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.work.step()?;
        self.work.flush()?;
        let mut channels = Vec::new();
        channels
            .try_reserve_exact(count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.work.step()?;
        self.work.flush()?;
        Ok(channels)
    }

    pub(super) fn reserve_definition(
        &mut self,
        owner: NodeId,
    ) -> Result<ExprId, ContractLoweringError> {
        let fragment = self.current_fragment;
        let count = self
            .definition_order
            .get(&fragment)
            .map_or(0, |order| order.definitions);
        if count == novarocks_physical_plan::PlanLimits::FROZEN.fragment_expressions {
            return Err(novarocks_expression_contract::UnitError::Resource(
                "fragment expression definition limit exceeded",
            )
            .into());
        }
        // This scratch only remembers the original construction sequence.
        // It does not assign a global expression ID or grant source access.
        self.work.flush()?;
        self.definition_order
            .entry(fragment)
            .or_default()
            .events
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.work.flush()?;
        let id = self.fragment_mut().unit(owner)?.reserve()?;
        let order = self
            .definition_order
            .get_mut(&fragment)
            .expect("reserved original construction journal");
        order.definitions += 1;
        order.events.push(NullableEmission::Definition {
            node: owner,
            definition: id,
        });
        self.work.step()?;
        Ok(id)
    }

    pub(super) fn insert_definition(
        &mut self,
        owner: NodeId,
        scope: Option<ExprId>,
        ty: ValueType,
        kind: ContractExprKind,
    ) -> Result<ExprId, ContractLoweringError> {
        let id = self.reserve_definition(owner)?;
        self.fragment_mut()
            .unit(owner)?
            .insert(id, scope, ty, kind)?;
        Ok(id)
    }

    pub(super) fn add_unscoped_expression(
        &mut self,
        owner: NodeId,
        ty: ValueType,
        kind: ContractExprKind,
    ) -> Result<ExprId, ContractLoweringError> {
        self.insert_definition(owner, None, ty, kind)
    }

    pub(super) fn root_definition(
        &mut self,
        owner: NodeId,
        definition: ExprId,
        demand: novarocks_type_contract::EvaluationDemand,
        scope: novarocks_expression_contract::RootScope,
    ) -> Result<novarocks_expression_contract::RootOrdinal, ContractLoweringError> {
        Ok(self
            .fragment_mut()
            .unit(owner)?
            .root(definition, demand, scope)?)
    }

    pub(super) fn add_scoped_expression(
        &mut self,
        owner: NodeId,
        ty: ValueType,
        kind: ContractExprKind,
    ) -> Result<ExprId, ContractLoweringError> {
        let scope = self.lambda_scope.last().map(|scope| scope.lambda);
        self.insert_definition(owner, scope, ty, kind)
    }

    pub(super) fn lower_bound_argument(
        &mut self,
        owner: NodeId,
        argument: &TypedExpr,
        visible: &BTreeMap<ColumnId, ValueId>,
        expected: &FunctionArgumentType,
    ) -> Result<ExprId, ContractLoweringError> {
        if let FunctionArgumentType::Value(expected) = expected
            && *argument.value_type.data_type() == DataType::Null
            && *expected.data_type() != DataType::Null
        {
            return self.author_literal_expression(
                owner,
                &LiteralValue::Null,
                (expected.clone())
                    .with_nullable(true)
                    .expect("validated root NULL contract"),
            );
        }
        self.lower_expression(owner, argument, visible)
    }

    pub(super) fn value_declared_type(
        &mut self,
        value: ValueId,
    ) -> Result<ValueType, ContractLoweringError> {
        self.fragment_mut()
            .value(value)
            .map(|definition| definition.ty.clone())
            .ok_or(ContractLoweringError::IdentitySpaceExhausted("value"))
    }

    pub(super) fn expression_value_type(
        &mut self,
        owner: NodeId,
        expr: ExprId,
    ) -> Result<ValueType, ContractLoweringError> {
        self.fragments
            .get(&self.current_fragment)
            .and_then(|fragment| fragment.construction_unit(owner))
            .and_then(|unit| unit.definition(expr))
            .map(|expression| expression.ty.clone())
            .ok_or(ContractLoweringError::IdentitySpaceExhausted("expression"))
    }

    pub(super) fn lower_expression_as(
        &mut self,
        owner: NodeId,
        expression: &TypedExpr,
        visible: &BTreeMap<ColumnId, ValueId>,
        target: &ValueType,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
    ) -> Result<ExprId, ContractLoweringError> {
        if let Some(source) = self.null_constant_type(expression)?
            && source != *target
        {
            let mut target = target.clone();
            target = target
                .with_nullable(true)
                .expect("validated root NULL contract");
            return self.author_literal_expression(owner, &LiteralValue::Null, target);
        }
        let lowered = self.lower_expression(owner, expression, visible)?;
        // A carrier conversion preserves an actual source NULL, including a
        // value null-extended after analysis. Keep that fact before the CASE
        // or CAST owner publishes its final output type.
        if self.expression_value_type(owner, lowered)?.nullable() && !target.nullable() {
            self.work.flush()?;
            let mut target = target.clone();
            self.work.step()?;
            self.work.flush()?;
            target = target
                .with_nullable(true)
                .expect("validated root NULL contract");
            self.convert_expression_to(owner, lowered, &target, policy)
        } else {
            self.convert_expression_to(owner, lowered, target, policy)
        }
    }

    pub(super) fn null_constant_type(
        &mut self,
        expression: &TypedExpr,
    ) -> Result<Option<ValueType>, ContractLoweringError> {
        match &expression.kind {
            ExprKind::Literal(LiteralValue::Null) => {
                let mut ty = expression_type(expression);
                ty = ty
                    .with_nullable(true)
                    .expect("validated root NULL contract");
                Ok(Some(ty))
            }
            ExprKind::Constant(value) => {
                self.work.flush()?;
                let null = value.is_null_observed(CompilePhase::Validate, self.control)?;
                self.work.flush()?;
                Ok(null.then(|| expression_type(expression)))
            }
            _ => Ok(None),
        }
    }

    pub(super) fn convert_expression_to(
        &mut self,
        owner: NodeId,
        expression: ExprId,
        target: &ValueType,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
    ) -> Result<ExprId, ContractLoweringError> {
        let result = self.convert_expression_to_inner(owner, expression, target, policy);
        if matches!(&result, Err(ContractLoweringError::Control(_))) {
            return result;
        }
        self.work.flush()?;
        result
    }

    pub(super) fn convert_expression_to_inner(
        &mut self,
        owner: NodeId,
        expression: ExprId,
        target: &ValueType,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
    ) -> Result<ExprId, ContractLoweringError> {
        let source = self.expression_value_type(owner, expression)?;
        let plan = crate::compiler::selected_scalar_call::plan_conversion(
            self.functions.as_ref(),
            &source,
            target,
            policy,
            self.root_allow_throw_exception,
            self.control,
        )
        .map_err(|error| match error {
            novarocks_functions::FunctionBindingError::Control(cause) => {
                ContractLoweringError::Control(cause)
            }
            other => ContractLoweringError::InvalidFunctionBinding {
                detail: format!(
                    "{other} (assignment fragment={:?}, owner={owner:?}, expression={expression:?}, source-domain={:?}, target-domain={:?}, source-carrier={:?}, target-carrier={:?}; source={source:?}, target={target:?})",
                    self.current_fragment,
                    source.logical_type(),
                    target.logical_type(),
                    source.data_type(),
                    target.data_type(),
                ),
            },
        })?;
        use crate::compiler::selected_scalar_call::PlannedConversion;
        let (conversion, carrier) = match plan {
            PlannedConversion::Identity => return Ok(expression),
            PlannedConversion::Carrier(step) => (None, Some(step)),
            PlannedConversion::ValueConversion { binding, tail } => (Some(binding), tail),
        };
        let expression = if let Some(binding) = conversion {
            let argument = novarocks_functions::FunctionArgument::Value {
                value_type: source.clone(),
                constant: None,
            };
            self.work.step()?;
            self.work.flush()?;
            let mut arguments = Vec::new();
            arguments
                .try_reserve_exact(1)
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            self.work.step()?;
            arguments.push(argument);
            self.work.step()?;
            self.work.flush()?;
            let captured = move_authored_call_arguments_observed(
                &binding,
                1,
                arguments,
                self.constant_policy,
                self.control,
            )?;
            self.work.flush()?;
            let resolved = binding.resolved();
            let mut channels = self.reserve_call_channels(1)?;
            channels.push(LoweredOperationalChannel {
                expression,
                role: SqlOperationalChannelRole::ValueWithoutConstant,
            });
            self.work.step()?;
            self.work.flush()?;
            let channels = channels.into_boxed_slice();
            self.work.step()?;
            let canonical = self.author_canonical_call_selection(
                owner,
                &captured,
                Some(SqlExpressionCallKind::ValueConversion),
                None,
                &[expression],
                &channels,
            )?;
            let selected = canonical.selected();
            let novarocks_functions::FunctionResultType::Scalar(result) = &selected.result_type
            else {
                return Err(ContractLoweringError::InvalidFunctionBinding {
                    detail: "canonical conversion owner selected a relation result".into(),
                });
            };
            if selected.aggregate.is_some()
                || captured.binding().result_constraint() != Some(result)
            {
                return Err(ContractLoweringError::InvalidFunctionBinding {
                    detail:
                        "canonical conversion result differs from its original intermediate target"
                            .into(),
                });
            }
            self.work.flush()?;
            let function = bound_function_from_selection(resolved, selected, result);
            self.work.step()?;
            self.work.flush()?;
            let result_type = result.clone();
            self.work.step()?;
            self.work.flush()?;
            let emitted = self.add_scoped_expression(
                owner,
                result_type,
                ContractExprKind::FunctionCall {
                    function,
                    args: Box::from([expression]),
                },
            )?;
            self.record_expression_source(
                owner,
                emitted,
                captured,
                SqlExpressionCallKind::ValueConversion,
                channels,
                Some(canonical),
            )?;
            emitted
        } else {
            expression
        };
        let Some(carrier) = carrier else {
            return Ok(expression);
        };
        let allow_throw_exception = self.root_allow_throw_reference();
        self.add_scoped_expression(
            owner,
            carrier.target().clone(),
            ContractExprKind::Cast {
                allow_throw_exception,
                expr: expression,
                target: carrier.target().data_type().clone(),
                decimal_overflow_policy: carrier.decimal_overflow_policy(),
            },
        )
    }

    pub(super) fn common_comparison_value_type(
        &mut self,
        left: &ValueType,
        right: &ValueType,
    ) -> Result<ValueType, ContractLoweringError> {
        self.work.step()?;
        let result = crate::compiler::selected_scalar_call::comparison_type(left, right);
        self.control.checkpoint(CompilePhase::Validate, 0)?;
        result.map_err(ContractLoweringError::ComparisonCoercion)
    }

    pub(super) fn cast_expression_to(
        &mut self,
        owner: NodeId,
        expr: ExprId,
        target: &ValueType,
    ) -> Result<ExprId, ContractLoweringError> {
        let current = self.expression_value_type(owner, expr)?;
        let target = crate::compiler::selected_scalar_call::comparison_target(target, &current);
        // This port retains the existing late comparison OutputNull policy.
        // Request-owned policy freezing is a separate pending semantic gate.
        self.convert_expression_to(
            owner,
            expr,
            &target,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        )
    }

    pub(super) fn lower_constant_expression(
        &mut self,
        owner: NodeId,
        value: &novarocks_constant_contract::ConstantValue,
        ty: ValueType,
    ) -> Result<ExprId, ContractLoweringError> {
        let reference = self.register_constant_source(value, &ty)?;
        self.add_scoped_expression(owner, ty, ContractExprKind::Constant(reference))
    }

    pub(super) fn register_constant_source(
        &mut self,
        value: &novarocks_constant_contract::ConstantValue,
        ty: &ValueType,
    ) -> Result<novarocks_physical_plan::ConstantReference, ContractLoweringError> {
        self.constant_emission.register(value, ty, self.work)
    }

    pub(super) fn author_utf8_expression(
        &mut self,
        owner: NodeId,
        text: &str,
    ) -> Result<ExprId, ContractLoweringError> {
        let ty = ValueType::new(DataType::Utf8, false);
        self.work.flush()?;
        let field = std::sync::Arc::new(ty.try_to_field("literal")?);
        let value = novarocks_constant_contract::ConstantValue::from_utf8(
            field,
            ty.clone(),
            text,
            self.constant_policy,
            CompilePhase::Validate,
            self.control,
        )?;
        self.work.flush()?;
        self.lower_constant_expression(owner, &value, ty)
    }

    pub(super) fn author_literal_expression(
        &mut self,
        owner: NodeId,
        literal: &LiteralValue,
        ty: ValueType,
    ) -> Result<ExprId, ContractLoweringError> {
        self.author_literal_expression_with_source(owner, literal, ty)
            .map(|(expression, _)| expression)
    }

    pub(super) fn author_literal_expression_with_source(
        &mut self,
        owner: NodeId,
        literal: &LiteralValue,
        ty: ValueType,
    ) -> Result<(ExprId, novarocks_constant_contract::ConstantValue), ContractLoweringError> {
        self.work.flush()?;
        let value = crate::constant::admit_syntax_constant(
            literal,
            &ty,
            self.constant_policy,
            self.control,
        )?;
        self.work.flush()?;
        let expression = self.lower_constant_expression(owner, &value, ty)?;
        Ok((expression, value))
    }

    pub(super) fn lower_literal_expression(
        &mut self,
        owner: NodeId,
        literal: &LiteralValue,
        expression: &TypedExpr,
    ) -> Result<ExprId, ContractLoweringError> {
        let mut ty = expression_type(expression);
        if matches!(literal, LiteralValue::Null) {
            ty = ty
                .with_nullable(true)
                .expect("validated root NULL contract");
        }
        self.author_literal_expression(owner, literal, ty)
    }
}
