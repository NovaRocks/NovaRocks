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

/// A short loan of the original fragment-local construction-order slot.
/// Taking the Entry does not install a vacant value before the reserve gate.
pub(super) struct NodeScalarOrder<'emission> {
    slot: Option<
        std::collections::btree_map::Entry<'emission, FragmentId, NullableConstructionOrder>,
    >,
    value: Option<&'emission mut NullableConstructionOrder>,
}
impl<'emission> NodeScalarOrder<'emission> {
    pub(super) fn new(
        slot: std::collections::btree_map::Entry<'emission, FragmentId, NullableConstructionOrder>,
    ) -> Self {
        Self {
            slot: Some(slot),
            value: None,
        }
    }
    fn definition_count(&self) -> usize {
        if let Some(value) = self.value.as_deref() {
            return value.definitions;
        }
        match self
            .slot
            .as_ref()
            .expect("the actual order slot remains borrowed")
        {
            std::collections::btree_map::Entry::Occupied(entry) => entry.get().definitions,
            std::collections::btree_map::Entry::Vacant(_) => 0,
        }
    }
    fn get_mut(&mut self) -> &mut NullableConstructionOrder {
        if self.value.is_none() {
            let slot = self
                .slot
                .take()
                .expect("the actual order slot remains borrowed");
            self.value = Some(slot.or_default());
        }
        self.value
            .as_deref_mut()
            .expect("the original order was installed")
    }
}

/// One actual node, its original fragment and its original shared order.
pub(super) struct NodeScalarStorage<'emission> {
    pub(super) fragment_id: FragmentId,
    pub(super) node: NodeId,
    pub(super) fragment: &'emission mut FragmentBuilder,
    pub(super) order: NodeScalarOrder<'emission>,
}

impl NodeScalarStorage<'_> {
    pub(super) fn definitions(&self) -> super::super::unit_source::UnitDefinitions<'_> {
        super::super::unit_source::UnitDefinitions::from_draft(self.fragment, self.node)
    }
    pub(super) fn source_site(&self) -> ScalarSourceSite {
        ScalarSourceSite::Node {
            fragment: self.fragment_id,
            node: self.node,
        }
    }
    pub(super) fn current_value_type(&self, value: ValueId) -> Option<&ValueType> {
        self.fragment.value(value).map(|value| &value.ty)
    }
    pub(super) fn type_editor(
        &mut self,
    ) -> Result<novarocks_expression_contract::UnitTypeEditor<'_>, ContractLoweringError> {
        Ok(self.fragment.unit(self.node)?.types_mut())
    }
}

pub(super) struct StandaloneScalarStorage<'emission> {
    pub(super) brand: &'emission super::super::unit_source::StandaloneSourceBrand,
    pub(super) draft: &'emission mut novarocks_expression_contract::BoundExprSetBuilder,
    pub(super) events: &'emission mut Vec<ExprId>,
    pub(super) value_types: &'emission BTreeMap<ValueId, ValueType>,
}

pub(super) enum ScalarStorage<'emission> {
    Node(NodeScalarStorage<'emission>),
    Standalone(StandaloneScalarStorage<'emission>),
}
impl ScalarStorage<'_> {
    pub(super) fn definitions(&self) -> super::super::unit_source::UnitDefinitions<'_> {
        match self {
            Self::Node(storage) => storage.definitions(),
            Self::Standalone(storage) => super::super::unit_source::UnitDefinitions::from_builder(
                ScalarUnitOwner::Standalone(storage.brand.clone()),
                storage.draft,
            ),
        }
    }
    pub(super) fn source_site(&self) -> ScalarSourceSite {
        match self {
            Self::Node(storage) => storage.source_site(),
            Self::Standalone(storage) => ScalarSourceSite::Standalone(storage.brand.clone()),
        }
    }
    pub(super) fn current_value_type(&self, value: ValueId) -> Option<&ValueType> {
        match self {
            Self::Node(storage) => storage.current_value_type(value),
            Self::Standalone(storage) => storage.value_types.get(&value),
        }
    }
    pub(super) fn type_editor(
        &mut self,
    ) -> Result<novarocks_expression_contract::UnitTypeEditor<'_>, ContractLoweringError> {
        Ok(self.unit_mut()?.types_mut())
    }
    fn unit_mut(
        &mut self,
    ) -> Result<&mut novarocks_expression_contract::BoundExprSetBuilder, ContractLoweringError>
    {
        match self {
            Self::Node(storage) => Ok(storage.fragment.unit(storage.node)?),
            Self::Standalone(storage) => Ok(storage.draft),
        }
    }
    fn fragment_definition_count(&self) -> Option<usize> {
        match self {
            Self::Node(storage) => Some(storage.order.definition_count()),
            Self::Standalone(_) => None,
        }
    }
    fn reserve_definition_event(&mut self) -> Result<(), ContractLoweringError> {
        match self {
            Self::Node(storage) => storage.order.get_mut().events.try_reserve(1),
            Self::Standalone(storage) => storage.events.try_reserve(1),
        }
        .map_err(|_| CompileControlError::ResourceExhausted.into())
    }
    fn charge_definition(&mut self) {
        if let Self::Node(storage) = self {
            storage.order.get_mut().definitions += 1;
        }
    }
    fn record_definition(&mut self, definition: ExprId) {
        match self {
            Self::Node(storage) => {
                let order = storage.order.get_mut();
                order.events.push(NullableEmission::Definition {
                    node: storage.node,
                    definition,
                });
            }
            Self::Standalone(storage) => storage.events.push(definition),
        }
    }
}

struct LoweredControlExpansion {
    positive: ExprId,
    conjunction: ExprId,
    branches: [ExprId; 2],
}

fn expanded_conjunction(expression: &TypedExpr, includes_not: bool) -> Option<[&TypedExpr; 2]> {
    let ExprKind::BooleanConnective {
        left, op, right, ..
    } = &expression.kind
    else {
        return None;
    };
    let expected = if includes_not {
        crate::common::BooleanOp::ExpandedNegatedConjunction
    } else {
        crate::common::BooleanOp::ExpandedConjunction
    };
    (*op == expected).then_some([left, right])
}

pub(super) struct ScalarEmission<'emission, 'control> {
    pub(super) constant_policy: novarocks_constant_contract::ConstantValidation,
    pub(super) parameters: &'emission mut BTreeMap<SemanticParameterId, SemanticParameterValue>,
    pub(super) functions: &'emission Arc<dyn crate::compiler::SqlFunctionCatalog>,
    pub(super) control: &'control dyn PureCompileControl,
    pub(super) work: &'emission mut CompileCheckpoints<'control>,
    pub(super) constant_emission: &'emission mut constant_emission::SqlConstantEmission,
    pub(super) lambda_scope: &'emission mut Vec<LoweringLambdaScope>,
    pub(super) local_scope: Vec<(crate::analysis::LocalBindingId, ExprId, ValueType)>,
    pub(super) storage: ScalarStorage<'emission>,
    pub(super) call_sources: &'emission mut SqlLogicalSourceJournal,
}
impl ScalarEmission<'_, '_> {
    pub(super) fn lower_derived_variant_expression(
        &mut self,
        source_value: ValueId,
        source: &Arc<crate::common::variant_source::DerivedVariantSource>,
        expected_result: &ValueType,
    ) -> Result<ExprId, ContractLoweringError> {
        let binding = source.captured().binding().resolved();
        let source_type = self.value_declared_type(source_value)?;
        let source_expression =
            self.add_scoped_expression(source_type, ContractExprKind::input(source_value))?;
        let path_expression = self.lower_constant_expression(
            source.canonical_path(),
            ValueType::new(DataType::Utf8, false),
        )?;
        let type_expression = self.lower_constant_expression(
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
        root: &TypedExpr,
        connective: crate::common::BooleanOp,
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<Box<[ExprId]>, ContractLoweringError> {
        let mut operands: Vec<&TypedExpr> = Vec::new();
        let mut pending: Vec<&TypedExpr> = vec![root];
        while let Some(current) = pending.pop() {
            match &current.kind {
                ExprKind::BooleanConnective {
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
            args.push(self.lower_expression(operand, visible)?);
        }
        Ok(args.into_boxed_slice())
    }

    pub(super) fn local_derived_value_expression(
        &mut self,
        value: ValueId,
    ) -> Result<Option<ExprId>, ContractLoweringError> {
        let ScalarStorage::Node(storage) = &self.storage else {
            // Standalone inputs are caller-declared values and have no local definition.
            return Ok(None);
        };
        let owner = storage.node;
        let fragment = &*storage.fragment;
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
                    fragment: storage.fragment_id,
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
            .and_then(|definition| definition.kind.input_value())
            .ok_or(ContractLoweringError::InvalidFunctionBinding {
                detail: "derived source has no actual raw input value".into(),
            })?;
        self.work.flush()?;
        let source = Arc::clone(source);
        let expected = definition.ty.clone();
        self.work.step()?;
        self.lower_derived_variant_expression(raw, &source, &expected)
            .map(Some)
    }

    pub(super) fn lower_expression(
        &mut self,
        expression: &TypedExpr,
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<ExprId, ContractLoweringError> {
        let result = self.lower_expression_inner(expression, visible);
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

    fn lower_control_expansion(
        &mut self,
        expression: &TypedExpr,
        includes_not: bool,
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<Option<LoweredControlExpansion>, ContractLoweringError> {
        if let Some(operands) = expanded_conjunction(expression, includes_not) {
            let (conjunction, branches) = self.lower_expanded_conjunction(operands, visible)?;
            return Ok(Some(LoweredControlExpansion {
                positive: conjunction,
                conjunction,
                branches,
            }));
        }
        if let ExprKind::Let {
            binding,
            initializer,
            body,
        } = &expression.kind
            && let Some(operands) = expanded_conjunction(body, includes_not)
        {
            let (positive, (conjunction, branches)) =
                self.lower_let(*binding, initializer, visible, |emission| {
                    let (conjunction, branches) =
                        emission.lower_expanded_conjunction(operands, visible)?;
                    Ok((conjunction, (conjunction, branches)))
                })?;
            return Ok(Some(LoweredControlExpansion {
                positive,
                conjunction,
                branches,
            }));
        }
        Ok(None)
    }

    fn lower_expanded_conjunction(
        &mut self,
        [left, right]: [&TypedExpr; 2],
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<(ExprId, [ExprId; 2]), ContractLoweringError> {
        let left = self.lower_expression(left, visible)?;
        let right = self.lower_expression(right, visible)?;
        let branches = [left, right];
        let nullable = self.expression_value_type(left)?.nullable()
            || self.expression_value_type(right)?.nullable();
        let conjunction = self.add_scoped_expression(
            ValueType::new(DataType::Boolean, nullable),
            ContractExprKind::Conjunction {
                args: Box::new(branches),
            },
        )?;
        Ok((conjunction, branches))
    }

    fn lower_let<T>(
        &mut self,
        binding: crate::analysis::LocalBindingId,
        initializer: &TypedExpr,
        visible: &BTreeMap<ColumnId, ValueId>,
        lower_body: impl FnOnce(&mut Self) -> Result<(ExprId, T), ContractLoweringError>,
    ) -> Result<(ExprId, T), ContractLoweringError> {
        let initializer = self.lower_expression(initializer, visible)?;
        let initializer_type = self.expression_value_type(initializer)?;
        let definition = self.reserve_definition_identity()?;
        self.local_scope
            .push((binding, definition, initializer_type));
        let body = lower_body(self);
        self.local_scope.pop();
        let (body, ancillary) = body?;
        let body_type = self.expression_value_type(body)?;
        let scope = self.lambda_scope.last().map(|scope| scope.lambda);
        self.storage.unit_mut()?.insert(
            definition,
            scope,
            body_type,
            ContractExprKind::Let {
                value: initializer,
                body,
            },
        )?;
        self.record_definition_event(definition)?;
        Ok((definition, ancillary))
    }

    fn record_control_expansion(
        &mut self,
        root: ExprId,
        expansion: LoweredControlExpansion,
    ) -> Result<(), ContractLoweringError> {
        self.work.flush()?;
        self.storage.unit_mut()?.record_control_expansion(
            root,
            expansion.positive,
            expansion.conjunction,
            expansion.branches,
        )?;
        self.work.step()?;
        Ok(())
    }

    #[recursive::recursive]
    pub(super) fn lower_expression_inner(
        &mut self,
        expression: &TypedExpr,
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<ExprId, ContractLoweringError> {
        self.work.step()?;
        if let ExprKind::Literal(literal) = &expression.kind {
            return self.lower_literal_expression(literal, expression);
        }
        if let Some(expansion) = self.lower_control_expansion(expression, false, visible)? {
            let result = expansion.positive;
            self.record_control_expansion(result, expansion)?;
            return Ok(result);
        }
        let mut ty = expression_type(expression);
        let mut captured_call = None;
        let mut control_expansion = None;
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
                if let Some(definition) = self.local_derived_value_expression(value)? {
                    return Ok(definition);
                }
                ty = self.value_declared_type(value)?;
                ContractExprKind::input(value)
            }
            ExprKind::Literal(_) => unreachable!("literal expressions return before dispatch"),
            ExprKind::Constant(value) => return self.lower_constant_expression(value, ty),
            ExprKind::BooleanConnective {
                op: op @ (crate::common::BooleanOp::And | crate::common::BooleanOp::Or),
                ..
            } => {
                let args = self.lower_boolean_connective(expression, *op, visible)?;
                if matches!(op, crate::common::BooleanOp::And) {
                    ContractExprKind::Conjunction { args }
                } else {
                    ContractExprKind::Disjunction { args }
                }
            }
            ExprKind::Cast {
                expr,
                decimal_overflow_policy,
                cast_mode,
                ..
            } => {
                return self.lower_expression_as(
                    expr,
                    visible,
                    &ty,
                    *decimal_overflow_policy,
                    *cast_mode,
                );
            }
            ExprKind::IsNull { expr, negated } => ContractExprKind::IsNull {
                expr: self.lower_expression(expr, visible)?,
                negated: *negated,
            },
            ExprKind::Case {
                ieee_float_equality,
                operand,
                when_then,
                else_expr,
            } => {
                let operand = operand
                    .as_deref()
                    .map(|item| self.lower_expression(item, visible))
                    .transpose()?;
                let mut branches = Vec::with_capacity(when_then.len());
                for (when, then) in when_then {
                    let when = self.lower_expression(when, visible)?;
                    let then = self.lower_expression(then, visible)?;
                    branches.push((when, then));
                }
                let else_expr = else_expr
                    .as_deref()
                    .map(|item| self.lower_expression(item, visible))
                    .transpose()?;
                ContractExprKind::Case {
                    ieee_float_equality: *ieee_float_equality,
                    operand,
                    when_then: branches.into_boxed_slice(),
                    else_expr,
                }
            }
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
                let grouped = matches!(
                    binding.selected.argument_types.as_ref(),
                    [
                        FunctionArgumentType::Value(_),
                        FunctionArgumentType::CandidateGroup { .. }
                    ]
                );
                let logical_count = if grouped { 2 } else { args.len() };
                if binding.logical_argument_count != logical_count
                    || binding.selected.argument_types.len() != logical_count
                    || grouped && args.len() < 2
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
                if ty.nullable() && self.has_strict_constant_null(binding, args.iter())? {
                    return self.author_literal_expression(&LiteralValue::Null, ty);
                }
                self.work.flush()?;
                let captured = capture_logical_call_arguments(
                    binding,
                    logical_count,
                    args.iter(),
                    self.constant_policy,
                    self.control,
                )?;
                self.work.flush()?;
                let mut lowered_args = Vec::new();
                lowered_args
                    .try_reserve_exact(args.len())
                    .map_err(|_| CompileControlError::ResourceExhausted)?;
                self.work.step()?;
                self.work.flush()?;
                let mut channels = if grouped {
                    reserve_ordered_channels(self.work, args.len())?
                } else {
                    self.reserve_call_channels(args.len())?
                };
                for (ordinal, argument) in args.iter().enumerate() {
                    let outer_not = ordinal == 0
                        && crate::common::negation::is_boolean_not(&binding.function_id);
                    let expansion = if outer_not {
                        self.lower_control_expansion(argument, true, visible)?
                    } else {
                        None
                    };
                    let lowered = if let Some(expansion) = expansion {
                        let channel =
                            self.original_emission_channel(expansion.positive, ordinal, &captured)?;
                        control_expansion = Some(expansion);
                        channel
                    } else if grouped && ordinal > 0 {
                        let [
                            _,
                            novarocks_functions::FunctionArgument::CandidateGroup {
                                members, ..
                            },
                        ] = captured.request().arguments
                        else {
                            return Err(ContractLoweringError::InvalidFunctionBinding {
                                detail: "IN has no captured candidate group".into(),
                            });
                        };
                        let member = members.get(ordinal - 1).ok_or(
                            ContractLoweringError::InvalidFunctionBinding {
                                detail: "IN has no captured candidate at its source ordinal".into(),
                            },
                        )?;
                        self.lower_group_candidate(argument, member, visible)?
                    } else {
                        let expected = &binding.selected.argument_types[ordinal];
                        self.lower_captured_scalar_argument(
                            argument, ordinal, &captured, visible, expected,
                        )?
                    };
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
                let mut semantic_parameters = Vec::new();
                for (reference, value) in binding.statement_time_parameters() {
                    self.work.step()?;
                    match self.parameters.entry(reference.id) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if *entry.get() == value => {}
                        std::collections::btree_map::Entry::Occupied(_) => {
                            return Err(ContractLoweringError::InvalidFunctionBinding {
                                detail: "One statement body cannot freeze conflicting time facts"
                                    .into(),
                            });
                        }
                    }
                    semantic_parameters.push(reference);
                }
                captured_call = Some((captured, channels, canonical));
                ContractExprKind::FunctionCall {
                    function: BoundFunction {
                        legacy_metadata: Some(
                            novarocks_expression_contract::LegacyBindingMetadata {
                                semantic_parameters: semantic_parameters.into_boxed_slice(),
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
            ExprKind::Let {
                binding,
                initializer,
                body,
            } => {
                let (definition, ()) =
                    self.lower_let(*binding, initializer, visible, |emission| {
                        let body = emission.lower_expression(body, visible)?;
                        Ok((body, ()))
                    })?;
                return Ok(definition);
            }
            ExprKind::LocalValue(binding) => {
                let (_, definition, initializer_type) = self
                    .local_scope
                    .iter()
                    .rev()
                    .find(|(id, _, _)| id == binding)
                    .ok_or(ContractLoweringError::InvalidFunctionBinding {
                        detail: "local value stands outside its lexical body".into(),
                    })?;
                ty = initializer_type.clone();
                ContractExprKind::Value(novarocks_expression_contract::ValueAddress::Local(
                    *definition,
                ))
            }
            ExprKind::LambdaFunction { params, body } => {
                // The lambda's identity exists before its body, because every
                // parameter reference inside names it.
                let lambda = self.reserve_definition()?;
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
                let body_result = self.lower_expression(body, visible);
                self.lambda_scope.pop();
                let body_id = body_result?;
                let body_type = self.expression_value_type(body_id)?;
                self.storage.unit_mut()?.insert(
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
            ExprKind::Nested(inner) => return self.lower_expression(inner, visible),
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
                if self.expression_value_type(operand)?.nullable() {
                    ty = ty
                        .with_nullable(true)
                        .expect("validated root NULL contract");
                    break;
                }
            }
        }
        let emitted = self.add_scoped_expression(ty, kind)?;
        if let Some(expansion) = control_expansion {
            self.record_control_expansion(emitted, expansion)?;
        }
        if let Some((captured, channels, canonical)) = captured_call {
            self.record_expression_source(
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
        captured: &CapturedLogicalCallArguments,
        kind: Option<SqlExpressionCallKind>,
        derived: Option<&crate::common::variant_source::DerivedVariantSource>,
        arguments: &[ExprId],
        channels: &[LoweredOperationalChannel],
    ) -> Result<Arc<CanonicalCallOperationalRequest>, ContractLoweringError> {
        self.work.flush()?;
        let projection = project_emitted_call_arguments_observed(
            EmittedOperationalCall {
                expressions: self.storage.definitions(),
                pools: self.constant_emission.pools(),
                owner: self.storage.source_site().owner(),
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
        select_canonical_operational_request(
            self.functions,
            self.control,
            self.work,
            binding,
            logical_argument_count,
            arguments,
        )
    }

    pub(super) fn completed_specialization_result<T>(
        &mut self,
        outcome: Result<T, ContractLoweringError>,
    ) -> Result<T, ContractLoweringError> {
        completed_specialization_result(self.work, outcome)
    }

    pub(super) fn record_expression_source(
        &mut self,
        emitted: ExprId,
        captured: CapturedLogicalCallArguments,
        kind: SqlExpressionCallKind,
        channels: Box<[LoweredOperationalChannel]>,
        canonical_operational: Option<Arc<CanonicalCallOperationalRequest>>,
    ) -> Result<(), ContractLoweringError> {
        self.record_expression_source_with_data(
            emitted,
            LoweredExpressionLogicalSource::Owned(captured),
            kind,
            channels,
            canonical_operational,
        )
    }

    pub(super) fn record_expression_source_with_data(
        &mut self,
        emitted: ExprId,
        captured: LoweredExpressionLogicalSource,
        kind: SqlExpressionCallKind,
        channels: Box<[LoweredOperationalChannel]>,
        canonical_operational: Option<Arc<CanonicalCallOperationalRequest>>,
    ) -> Result<(), ContractLoweringError> {
        self.work.flush()?;
        let source_count =
            crate::binding::scalar_source_count(captured.request()).ok_or_else(|| {
                ContractLoweringError::InvalidFunctionBinding {
                    detail: "recorded scalar source has an invalid request count".into(),
                }
            })?;
        let mut arguments = Vec::new();
        arguments
            .try_reserve_exact(source_count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.work.step()?;
        self.work.flush()?;
        let lambda_scope = self.lambda_scope.last().map(|scope| scope.lambda);
        let source = self
            .storage
            .definitions()
            .definition(emitted)
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
        if count != channels.len() || count != source_count {
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
        let key = ScalarDefinitionAddress::new(self.storage.source_site(), emitted);
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
                owner: self.storage.source_site().owner(),
                lambda_scope,
                arguments,
                channels,
            },
        );
        self.work.step()?;
        Ok(())
    }

    fn lower_group_candidate(
        &mut self,
        expression: &TypedExpr,
        candidate: &novarocks_functions::FunctionCandidate,
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<LoweredOperationalChannel, ContractLoweringError> {
        let (expression, role) = match &candidate.constant {
            Some(value) => (
                self.lower_constant_expression(value, expression_type(expression))?,
                SqlOperationalChannelRole::CapturedConstant,
            ),
            None => (
                self.lower_expression(expression, visible)?,
                SqlOperationalChannelRole::ValueWithoutConstant,
            ),
        };
        self.work.step()?;
        Ok(LoweredOperationalChannel { expression, role })
    }

    pub(super) fn lower_captured_scalar_argument(
        &mut self,
        argument: &TypedExpr,
        ordinal: usize,
        captured: &CapturedLogicalCallArguments,
        visible: &BTreeMap<ColumnId, ValueId>,
        expected: &FunctionArgumentType,
    ) -> Result<LoweredOperationalChannel, ContractLoweringError> {
        if let FunctionArgumentType::Value(expected) = expected
            && argument.value_type.data_type() == &DataType::Null
            && expected.data_type() != &DataType::Null
        {
            return Err(ContractLoweringError::InvalidFunctionBinding {
                detail: "typed scalar argument did not retain its selected NULL conversion".into(),
            });
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
            let expression = self.lower_constant_expression(value, expression_type(argument))?;
            return self.original_emission_channel(expression, ordinal, captured);
        }
        let expression = self.lower_expression(argument, visible)?;
        self.original_emission_channel(expression, ordinal, captured)
    }

    pub(super) fn lower_captured_unbound_argument(
        &mut self,
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
            let emitted = self.lower_constant_expression(value, expression_type(expression))?;
            return self.original_emission_channel(emitted, ordinal, captured);
        }
        let emitted = self.lower_expression(expression, visible)?;
        self.original_emission_channel(emitted, ordinal, captured)
    }

    pub(super) fn original_emission_channel(
        &mut self,
        expression: ExprId,
        ordinal: usize,
        captured: &CapturedLogicalCallArguments,
    ) -> Result<LoweredOperationalChannel, ContractLoweringError> {
        original_emission_channel(self.work, expression, ordinal, captured)
    }

    pub(super) fn reserve_call_channels(
        &mut self,
        count: usize,
    ) -> Result<Vec<LoweredOperationalChannel>, ContractLoweringError> {
        reserve_call_channels(self.work, count)
    }

    pub(super) fn reserve_definition(&mut self) -> Result<ExprId, ContractLoweringError> {
        self.check_fragment_definition_limit()?;
        self.work.flush()?;
        self.storage.reserve_definition_event()?;
        self.work.flush()?;
        let id = self.storage.unit_mut()?.reserve()?;
        self.storage.charge_definition();
        self.storage.record_definition(id);
        self.work.step()?;
        Ok(id)
    }

    fn check_fragment_definition_limit(&self) -> Result<(), ContractLoweringError> {
        let count = self.storage.fragment_definition_count();
        if count == Some(novarocks_physical_plan::PlanLimits::FROZEN.fragment_expressions) {
            return Err(novarocks_expression_contract::UnitError::Resource(
                "fragment expression definition limit exceeded",
            )
            .into());
        }
        Ok(())
    }

    fn reserve_definition_identity(&mut self) -> Result<ExprId, ContractLoweringError> {
        self.check_fragment_definition_limit()?;
        self.work.flush()?;
        let id = self.storage.unit_mut()?.reserve()?;
        self.storage.charge_definition();
        self.work.step()?;
        Ok(id)
    }

    fn record_definition_event(&mut self, id: ExprId) -> Result<(), ContractLoweringError> {
        // A Let claims its address before the body. The type event follows
        // actual dependencies, so a late body type can update the binding.
        self.work.flush()?;
        self.storage.reserve_definition_event()?;
        self.work.flush()?;
        self.storage.record_definition(id);
        self.work.step()?;
        Ok(())
    }

    pub(super) fn insert_definition(
        &mut self,
        scope: Option<ExprId>,
        ty: ValueType,
        kind: ContractExprKind,
    ) -> Result<ExprId, ContractLoweringError> {
        let id = self.reserve_definition()?;
        self.storage.unit_mut()?.insert(id, scope, ty, kind)?;
        Ok(id)
    }

    pub(super) fn add_unscoped_expression(
        &mut self,
        ty: ValueType,
        kind: ContractExprKind,
    ) -> Result<ExprId, ContractLoweringError> {
        self.insert_definition(None, ty, kind)
    }

    pub(super) fn root_definition(
        &mut self,
        definition: ExprId,
        demand: novarocks_type_contract::EvaluationDemand,
        scope: novarocks_expression_contract::RootScope,
    ) -> Result<novarocks_expression_contract::RootOrdinal, ContractLoweringError> {
        Ok(self.storage.unit_mut()?.root(definition, demand, scope)?)
    }

    pub(super) fn add_scoped_expression(
        &mut self,
        ty: ValueType,
        kind: ContractExprKind,
    ) -> Result<ExprId, ContractLoweringError> {
        let scope = self.lambda_scope.last().map(|scope| scope.lambda);
        self.insert_definition(scope, ty, kind)
    }

    pub(super) fn value_declared_type(
        &mut self,
        value: ValueId,
    ) -> Result<ValueType, ContractLoweringError> {
        self.storage
            .current_value_type(value)
            .cloned()
            .ok_or(ContractLoweringError::IdentitySpaceExhausted("value"))
    }

    pub(super) fn expression_value_type(
        &mut self,
        expr: ExprId,
    ) -> Result<ValueType, ContractLoweringError> {
        self.storage
            .definitions()
            .definition(expr)
            .map(|expression| expression.ty.clone())
            .ok_or(ContractLoweringError::IdentitySpaceExhausted("expression"))
    }

    pub(super) fn lower_expression_as(
        &mut self,
        expression: &TypedExpr,
        visible: &BTreeMap<ColumnId, ValueId>,
        target: &ValueType,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
        cast_mode: Option<novarocks_type_contract::TrinoCastMode>,
    ) -> Result<ExprId, ContractLoweringError> {
        if cast_mode.is_none()
            && let Some(source) = self.null_constant_type(expression)?
            && source != *target
        {
            let mut target = target.clone();
            target = target
                .with_nullable(true)
                .expect("validated root NULL contract");
            return self.author_literal_expression(&LiteralValue::Null, target);
        }
        let lowered = self.lower_expression(expression, visible)?;
        // A carrier conversion preserves an actual source NULL, including a
        // value null-extended after analysis. Keep that fact before the CASE
        // or CAST owner publishes its final output type.
        if self.expression_value_type(lowered)?.nullable() && !target.nullable() {
            self.work.flush()?;
            let mut target = target.clone();
            self.work.step()?;
            self.work.flush()?;
            target = target
                .with_nullable(true)
                .expect("validated root NULL contract");
            self.convert_expression_to(lowered, &target, policy, cast_mode)
        } else {
            self.convert_expression_to(lowered, target, policy, cast_mode)
        }
    }

    pub(super) fn null_constant_type(
        &mut self,
        expression: &TypedExpr,
    ) -> Result<Option<ValueType>, ContractLoweringError> {
        null_constant_type(self.control, self.work, expression)
    }

    pub(super) fn convert_expression_to(
        &mut self,
        expression: ExprId,
        target: &ValueType,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
        cast_mode: Option<novarocks_type_contract::TrinoCastMode>,
    ) -> Result<ExprId, ContractLoweringError> {
        let result = self.convert_expression_to_inner(expression, target, policy, cast_mode);
        if matches!(&result, Err(ContractLoweringError::Control(_))) {
            return result;
        }
        self.work.flush()?;
        result
    }

    pub(super) fn convert_expression_to_inner(
        &mut self,
        expression: ExprId,
        target: &ValueType,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
        cast_mode: Option<novarocks_type_contract::TrinoCastMode>,
    ) -> Result<ExprId, ContractLoweringError> {
        let site = self.storage.source_site();
        let source = self.expression_value_type(expression)?;
        let plan = crate::compiler::selected_scalar_call::plan_conversion(
            crate::planner::BINDING_AUTHORITY,
            self.functions.as_ref(),
            &source,
            target,
            policy,
            cast_mode,
            self.control,
        )
        .map_err(|error| match error {
            novarocks_functions::FunctionBindingError::Control(cause) => {
                ContractLoweringError::Control(cause)
            }
            other => ContractLoweringError::InvalidFunctionBinding {
                detail: match &site {
                    ScalarSourceSite::Node { fragment, node: owner } => format!(
                        "{other} (assignment fragment={fragment:?}, owner={owner:?}, expression={expression:?}, source-domain={:?}, target-domain={:?}, source-carrier={:?}, target-carrier={:?}; source={source:?}, target={target:?})",
                        source.logical_type(), target.logical_type(), source.data_type(), target.data_type(),
                    ),
                    ScalarSourceSite::Standalone(_) => format!(
                        "{other} (standalone scalar assignment expression={expression:?}, source={source:?}, target={target:?})"
                    ),
                },
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
                result_type,
                ContractExprKind::FunctionCall {
                    function,
                    args: Box::from([expression]),
                },
            )?;
            self.record_expression_source(
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
        self.add_scoped_expression(
            carrier.target().clone(),
            ContractExprKind::Cast {
                cast_mode: carrier.cast_mode(),
                expr: expression,
                target: carrier.target().data_type().clone(),
            },
        )
    }

    pub(super) fn common_comparison_value_type(
        &mut self,
        left: &ValueType,
        right: &ValueType,
    ) -> Result<ValueType, ContractLoweringError> {
        common_comparison_value_type(self.control, self.work, left, right)
    }

    pub(super) fn cast_expression_to(
        &mut self,
        expr: ExprId,
        target: &ValueType,
    ) -> Result<ExprId, ContractLoweringError> {
        let current = self.expression_value_type(expr)?;
        let target = crate::compiler::selected_scalar_call::comparison_target(target, &current);
        // This port retains the existing late comparison OutputNull policy.
        // Request-owned policy freezing is a separate pending semantic gate.
        self.convert_expression_to(
            expr,
            &target,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            None,
        )
    }

    pub(super) fn lower_constant_expression(
        &mut self,
        value: &novarocks_constant_contract::ConstantValue,
        ty: ValueType,
    ) -> Result<ExprId, ContractLoweringError> {
        let reference = self.register_constant_source(value, &ty)?;
        self.add_scoped_expression(ty, ContractExprKind::Constant(reference))
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
        self.lower_constant_expression(&value, ty)
    }

    pub(super) fn author_literal_expression(
        &mut self,
        literal: &LiteralValue,
        ty: ValueType,
    ) -> Result<ExprId, ContractLoweringError> {
        self.author_literal_expression_with_source(literal, ty)
            .map(|(expression, _)| expression)
    }

    pub(super) fn author_literal_expression_with_source(
        &mut self,
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
        let expression = self.lower_constant_expression(&value, ty)?;
        Ok((expression, value))
    }

    fn has_strict_constant_null<'expr>(
        &mut self,
        binding: &SqlFunctionBinding,
        arguments: impl Iterator<Item = &'expr TypedExpr>,
    ) -> Result<bool, ContractLoweringError> {
        use novarocks_functions::FunctionSpecializationFailure;

        let mut declaration: Option<novarocks_functions::PureOverloadDeclaration<'_>> = None;
        for (position, mut argument) in arguments.enumerate() {
            let is_null = loop {
                self.work.step()?;
                match &argument.kind {
                    ExprKind::Literal(LiteralValue::Null) => break true,
                    ExprKind::Constant(value) => {
                        break value.is_null_observed(
                            CompilePhase::FunctionSpecialization,
                            self.control,
                        )?;
                    }
                    ExprKind::Nested(expr) | ExprKind::Cast { expr, .. } => argument = expr,
                    _ => break false,
                }
            };
            if !is_null {
                continue;
            }
            if declaration.is_none() {
                let selected = self
                    .functions
                    .pure_overload_declaration_observed(
                        &binding.function_id,
                        binding.kind,
                        &binding.selected.overload,
                        self.control,
                    )
                    .map_err(|cause| match cause {
                        FunctionSpecializationFailure::Control(cause) => cause.into(),
                        FunctionSpecializationFailure::Binding(cause) => cause.into(),
                        cause => ContractLoweringError::InvalidFunctionBinding {
                            detail: cause.to_string(),
                        },
                    })?;
                selected.admit_selected_lifecycle_observed(
                    &binding.selected,
                    binding.logical_argument_count,
                    novarocks_functions::PureCallLifecycle::Scalar,
                    self.control,
                )?;
                declaration = Some(selected);
            }
            let effects = declaration
                .as_ref()
                .expect("known NULL resolved its declaration")
                .effects();
            if crate::binding::strict_null_argument(effects, position) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn lower_literal_expression(
        &mut self,
        literal: &LiteralValue,
        expression: &TypedExpr,
    ) -> Result<ExprId, ContractLoweringError> {
        let mut ty = expression_type(expression);
        if matches!(literal, LiteralValue::Null) {
            ty = ty
                .with_nullable(true)
                .expect("validated root NULL contract");
        }
        self.author_literal_expression(literal, ty)
    }
}

pub(super) fn select_canonical_operational_request(
    functions: &Arc<dyn crate::compiler::SqlFunctionCatalog>,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
    binding: &SqlFunctionBinding,
    logical_argument_count: usize,
    arguments: &[novarocks_functions::FunctionArgument],
) -> Result<Arc<novarocks_functions::FunctionBindingSelection>, ContractLoweringError> {
    select_canonical_request_with_constraint(
        functions,
        control,
        work,
        binding,
        logical_argument_count,
        arguments,
        binding.result_constraint(),
    )
}

pub(super) fn select_canonical_request_with_constraint(
    functions: &Arc<dyn crate::compiler::SqlFunctionCatalog>,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
    binding: &SqlFunctionBinding,
    logical_argument_count: usize,
    arguments: &[novarocks_functions::FunctionArgument],
    constraint: Option<&novarocks_functions::FunctionValueType>,
) -> Result<Arc<novarocks_functions::FunctionBindingSelection>, ContractLoweringError> {
    work.flush()?;
    let selection = crate::compiler::selected_scalar_call::canonical_scalar_request_observed(
        functions.as_ref(),
        binding,
        logical_argument_count,
        arguments,
        constraint,
        control,
    )
    .map_err(|error| match error {
        novarocks_functions::FunctionBindingError::Control(cause) => {
            ContractLoweringError::Control(cause)
        }
        error => ContractLoweringError::InvalidFunctionBinding {
            detail: format!("canonical call selection: {error}"),
        },
    });
    completed_specialization_result(work, selection)
}

pub(super) fn completed_specialization_result<T>(
    work: &mut CompileCheckpoints<'_>,
    outcome: Result<T, ContractLoweringError>,
) -> Result<T, ContractLoweringError> {
    if matches!(&outcome, Err(ContractLoweringError::Control(_))) {
        return outcome;
    }
    work.step()?;
    work.flush()?;
    outcome
}

pub(super) fn original_emission_channel(
    work: &mut CompileCheckpoints<'_>,
    expression: ExprId,
    ordinal: usize,
    captured: &CapturedLogicalCallArguments,
) -> Result<LoweredOperationalChannel, ContractLoweringError> {
    let argument = captured.request().arguments.get(ordinal);
    work.step()?;
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
        Some(novarocks_functions::FunctionArgument::CandidateGroup { .. }) | None => {
            return Err(ContractLoweringError::InvalidFunctionBinding {
                detail: "emitted call channel has no original request occurrence".into(),
            });
        }
    };
    work.step()?;
    Ok(LoweredOperationalChannel { expression, role })
}

pub(super) fn reserve_call_channels(
    work: &mut CompileCheckpoints<'_>,
    count: usize,
) -> Result<Vec<LoweredOperationalChannel>, ContractLoweringError> {
    if count > novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    reserve_ordered_channels(work, count)
}

fn reserve_ordered_channels(
    work: &mut CompileCheckpoints<'_>,
    count: usize,
) -> Result<Vec<LoweredOperationalChannel>, ContractLoweringError> {
    std::alloc::Layout::array::<LoweredOperationalChannel>(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.step()?;
    work.flush()?;
    let mut channels = Vec::new();
    channels
        .try_reserve_exact(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.step()?;
    work.flush()?;
    Ok(channels)
}

pub(super) fn null_constant_type(
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
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
            work.flush()?;
            let null = value.is_null_observed(CompilePhase::Validate, control)?;
            work.flush()?;
            Ok(null.then(|| expression_type(expression)))
        }
        _ => Ok(None),
    }
}

pub(super) fn common_comparison_value_type(
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
    left: &ValueType,
    right: &ValueType,
) -> Result<ValueType, ContractLoweringError> {
    work.step()?;
    let result = crate::compiler::selected_scalar_call::comparison_type(left, right);
    control.checkpoint(CompilePhase::Validate, 0)?;
    result.map_err(ContractLoweringError::ComparisonCoercion)
}

pub(super) fn freeze_semantic_parameters(
    parameters: BTreeMap<SemanticParameterId, SemanticParameterValue>,
) -> Result<SemanticParameters, ContractLoweringError> {
    SemanticParameters::try_new(parameters).map_err(|error| {
        ContractLoweringError::InvalidFunctionBinding {
            detail: error.to_string(),
        }
    })
}
