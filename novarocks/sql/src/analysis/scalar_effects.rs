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

// Design: ADR-0280 (docs/adr/ADR-0280-scalar-effects-and-relational-evaluation-demand-govern-sql-rewrites.md)
use crate::binding::SqlFunctionBinding;
use crate::common::BinOp;
use crate::compiler::selected_scalar_call::{
    PlannedConversion, arithmetic_operator, comparison_operator, comparison_target,
    comparison_type, membership_operand_can_return_null, plan_conversion,
};
use crate::compiler::{SqlCompileError, SqlFunctionCatalog};
use novarocks_functions::{
    ArithmeticPrepareError, CastPrepareError, ComparisonPrepareError, FunctionBindingError,
    FunctionSpecializationFailure, PreparedArithmeticRecipe, PreparedCastRecipe,
    PreparedComparisonRecipe, PreparedNativeBetweenRecipe, PreparedNativeInListRecipe,
    PreparedNativeLikeRecipe, PreparedNullSafeComparisonRecipe, ResolvedFunctionBinding,
    StaticScalarCallEffects,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    ExpressionEffects, FunctionValueType, FunctionVolatility, NativeBetweenPlan, ObservableEffects,
    PureCompileControl, TrinoCastMode,
};

/// Definition facts are independent of a use's position. A rewrite must
/// still prove its own demand, input and NULL domain at that position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ScalarEffects {
    pub(crate) expression: ExpressionEffects,
    pub(crate) permits_filter_row_elimination: bool,
    pub(crate) has_internal_assertion: bool,
}

impl ScalarEffects {
    pub(crate) const PURE: Self = Self::primitive(ExpressionEffects::PURE_VALUE);
    pub(crate) const UNKNOWN: Self = Self {
        expression: ExpressionEffects {
            value_stability: FunctionVolatility::Volatile,
            may_raise_row_error: true,
            may_raise_invocation_data: true,
            has_instance_state: true,
            observable_effects: ObservableEffects {
                warnings: true,
                ..ObservableEffects::NONE
            },
        },
        permits_filter_row_elimination: false,
        has_internal_assertion: false,
    };

    const fn primitive(expression: ExpressionEffects) -> Self {
        Self {
            expression,
            permits_filter_row_elimination: !expression.may_raise_invocation_data,
            has_internal_assertion: false,
        }
    }

    pub(crate) fn join(self, child: Self) -> Self {
        Self {
            expression: self.expression.join(child.expression),
            permits_filter_row_elimination: self.permits_filter_row_elimination
                && child.permits_filter_row_elimination,
            has_internal_assertion: self.has_internal_assertion || child.has_internal_assertion,
        }
    }

    pub(crate) const fn may_raise_data(self) -> bool {
        self.expression.may_raise_row_error || self.expression.may_raise_invocation_data
    }

    /// A later filter can omit this occurrence only if the omission cannot
    /// hide a required error, effect, or internal check on a rejected row.
    pub(crate) const fn can_skip_for_earlier_filter(self) -> bool {
        !self.has_internal_assertion
            && self.permits_filter_row_elimination
            && self.expression.permits_earlier_evaluation()
    }

    pub(crate) const fn can_share(self) -> bool {
        !self.has_internal_assertion
            && self.expression.permits_earlier_evaluation()
            && self.expression.value_stability.is_replica_deterministic()
    }
}

pub(crate) struct ScalarEffectQuery<'a> {
    pub(crate) functions: Option<&'a dyn SqlFunctionCatalog>,
    pub(crate) allow_throw_exception: bool,
    pub(crate) control: &'a dyn PureCompileControl,
}

type Result<T> = std::result::Result<T, SqlCompileError>;

impl ScalarEffectQuery<'_> {
    pub(crate) fn scalar_call(
        &self,
        binding: &SqlFunctionBinding,
    ) -> Result<Option<StaticScalarCallEffects>> {
        if self.functions.is_none() {
            return Ok(binding.static_scalar_effects());
        }
        self.selected_call(binding.resolved())
    }

    pub(crate) fn selected_call(
        &self,
        binding: &ResolvedFunctionBinding,
    ) -> Result<Option<StaticScalarCallEffects>> {
        let Some(functions) = self.functions else {
            return Ok(None);
        };
        let declaration = match functions.pure_overload_declaration_observed(
            &binding.function_id,
            binding.kind,
            &binding.selected.overload,
            self.control,
        ) {
            Ok(declaration) => declaration,
            Err(FunctionSpecializationFailure::MissingPureImplementation(_)) => return Ok(None),
            Err(FunctionSpecializationFailure::Control(error)) => return Err(error.into()),
            Err(FunctionSpecializationFailure::Binding(error)) => {
                return unavailable_binding(error);
            }
            Err(error) => {
                return Err(SqlCompileError::CompilationSource(
                    novarocks_failure_contract::SharedFailureCause::new(error),
                ));
            }
        };
        match declaration.static_scalar_effects_observed(
            &binding.function_id,
            &binding.selected,
            self.control,
        ) {
            Ok(effects) => Ok(Some(effects)),
            Err(error) => unavailable_binding(error),
        }
    }

    pub(crate) fn cast(
        &self,
        source: &FunctionValueType,
        target: &FunctionValueType,
        mode: Option<TrinoCastMode>,
    ) -> Result<ScalarEffects> {
        if let Some(mode) = mode {
            return self.carrier_cast(source, target, mode);
        }
        if source == target || *source.data_type() == arrow::datatypes::DataType::Null {
            return Ok(ScalarEffects::PURE);
        }
        let conversion = match plan_conversion(self.functions, source, target, None, self.control) {
            Ok(conversion) => conversion,
            Err(error) => return unavailable_binding::<()>(error).map(|_| ScalarEffects::UNKNOWN),
        };
        match conversion {
            PlannedConversion::Identity => Ok(ScalarEffects::PURE),
            PlannedConversion::Carrier(step) => {
                self.carrier_cast(source, step.target(), step.cast_mode())
            }
            PlannedConversion::ValueConversion {
                binding,
                intermediate,
                tail,
            } => {
                let Some(call) = self.selected_call(&binding)? else {
                    return Ok(ScalarEffects::UNKNOWN);
                };
                let mut effects = call_effects(call, &binding.function_id);
                if let Some(step) = tail {
                    effects = effects.join(self.carrier_cast(
                        &intermediate,
                        step.target(),
                        step.cast_mode(),
                    )?);
                }
                Ok(effects)
            }
        }
    }

    fn carrier_cast(
        &self,
        source: &FunctionValueType,
        target: &FunctionValueType,
        mode: TrinoCastMode,
    ) -> Result<ScalarEffects> {
        match PreparedCastRecipe::try_new_trino(source, target, mode, self.control) {
            Ok(recipe) => Ok(ScalarEffects::primitive(recipe.static_own_effects())),
            Err(error) => primitive_refusal(
                error.control_error(),
                matches!(error, CastPrepareError::Kernel(_)),
                &error,
            ),
        }
    }

    fn common_inputs<'a>(
        &self,
        first: &FunctionValueType,
        others: impl IntoIterator<Item = &'a FunctionValueType>,
    ) -> Result<Option<FunctionValueType>> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Validate)?;
        let mut common = first.clone();
        for other in others {
            work.step()?;
            let Ok(value) = comparison_type(&common, other) else {
                work.finish()?;
                return Ok(None);
            };
            common = value;
        }
        work.finish()?;
        Ok(Some(common))
    }

    fn compared_value(
        &self,
        source: &FunctionValueType,
        common: &FunctionValueType,
    ) -> Result<(FunctionValueType, ScalarEffects)> {
        let target = comparison_target(common, source);
        let effects = self.cast(source, &target, None)?;
        Ok((target, effects))
    }

    pub(crate) fn comparison_casts_can_share(
        &self,
        left: &FunctionValueType,
        right: &FunctionValueType,
    ) -> Result<bool> {
        let Some(common) = self.common_inputs(left, [right])? else {
            return Ok(false);
        };
        let (_, left_cast) = self.compared_value(left, &common)?;
        let (_, right_cast) = self.compared_value(right, &common)?;
        Ok(left_cast.join(right_cast).can_share())
    }

    pub(crate) fn binary(
        &self,
        op: BinOp,
        left: &FunctionValueType,
        right: &FunctionValueType,
        result: &FunctionValueType,
        policy: DecimalOverflowPolicy,
    ) -> Result<ScalarEffects> {
        if matches!(op, BinOp::And | BinOp::Or) {
            return Ok(ScalarEffects::PURE);
        }
        if let Some(op) = arithmetic_operator(op) {
            return match PreparedArithmeticRecipe::try_new(
                op,
                left,
                right,
                result,
                policy,
                self.allow_throw_exception,
                self.control,
            ) {
                Ok(recipe) => Ok(ScalarEffects::primitive(recipe.static_own_effects())),
                Err(error) => primitive_refusal(
                    error.control_error(),
                    matches!(error, ArithmeticPrepareError::Kernel(_)),
                    &error,
                ),
            };
        }
        let Some(common) = self.common_inputs(left, [right])? else {
            return Ok(ScalarEffects::UNKNOWN);
        };
        let (left, left_cast) = self.compared_value(left, &common)?;
        let (right, right_cast) = self.compared_value(right, &common)?;
        let comparison = match comparison_operator(op) {
            Some(operator) => {
                PreparedComparisonRecipe::try_new(operator, &left, &right, self.control)
                    .map(|recipe| recipe.static_own_effects())
            }
            None => {
                debug_assert_eq!(op, BinOp::EqForNull);
                PreparedNullSafeComparisonRecipe::try_new(&left, &right, self.control)
                    .map(|recipe| recipe.static_own_effects())
            }
        };
        Ok(comparison_effects(comparison)?
            .join(left_cast)
            .join(right_cast))
    }

    pub(crate) fn in_list<'a>(
        &self,
        source: &FunctionValueType,
        candidates: impl ExactSizeIterator<Item = &'a FunctionValueType> + Clone,
        result: &FunctionValueType,
    ) -> Result<ScalarEffects> {
        let Some(common) = self.common_inputs(source, candidates.clone())? else {
            return Ok(ScalarEffects::UNKNOWN);
        };
        let (source, mut effects) = self.compared_value(source, &common)?;
        let mut targets = Vec::new();
        targets
            .try_reserve_exact(candidates.len())
            .map_err(|_| SqlCompileError::ResourceExhausted)?;
        for candidate in candidates {
            let (target, conversion) = self.compared_value(candidate, &common)?;
            effects = effects.join(conversion);
            targets.push(target);
        }
        // Lowering widens the declared result if equality can produce NULL
        // independently of the original argument declarations.
        let mut nullable = result.nullable() || source.nullable();
        for target in &targets {
            if nullable {
                break;
            }
            nullable = match membership_operand_can_return_null(&source, target, self.control) {
                Ok(nullable) => nullable,
                Err(error) => return comparison_effects(Err(error)),
            };
        }
        let result = result
            .with_nullable(nullable)
            .expect("validated Boolean result");
        Ok(effects.join(comparison_effects(
            PreparedNativeInListRecipe::static_effects_for_profile(
                &source,
                targets.iter(),
                &result,
                self.control,
            ),
        )?))
    }

    pub(crate) fn between(
        &self,
        source: &FunctionValueType,
        low: &FunctionValueType,
        high: &FunctionValueType,
        result: &FunctionValueType,
        negated: bool,
    ) -> Result<ScalarEffects> {
        let Some(common) = self.common_inputs(source, [low, high])? else {
            return Ok(ScalarEffects::UNKNOWN);
        };
        let (source, source_cast) = self.compared_value(source, &common)?;
        let (low, low_cast) = self.compared_value(low, &common)?;
        let (high, high_cast) = self.compared_value(high, &common)?;
        Ok(comparison_effects(
            PreparedNativeBetweenRecipe::try_new(
                NativeBetweenPlan::new(negated),
                &source,
                &low,
                &high,
                result,
                self.control,
            )
            .map(|recipe| recipe.static_own_effects()),
        )?
        .join(source_cast)
        .join(low_cast)
        .join(high_cast))
    }

    pub(crate) fn like(
        &self,
        source: &FunctionValueType,
        pattern: &FunctionValueType,
        result: &FunctionValueType,
        negated: bool,
    ) -> Result<ScalarEffects> {
        comparison_effects(
            PreparedNativeLikeRecipe::try_new(negated, source, pattern, result, self.control)
                .map(|recipe| recipe.static_own_effects()),
        )
    }

    pub(crate) fn simple_case<'a>(
        &self,
        operand: &FunctionValueType,
        labels: impl ExactSizeIterator<Item = &'a FunctionValueType> + Clone,
        values: impl Iterator<Item = &'a FunctionValueType>,
        result: &FunctionValueType,
    ) -> Result<ScalarEffects> {
        let Some(common) = self.common_inputs(operand, labels.clone())? else {
            return Ok(ScalarEffects::UNKNOWN);
        };
        let (operand, mut effects) = self.compared_value(operand, &common)?;
        let mut targets = Vec::new();
        targets
            .try_reserve_exact(labels.len())
            .map_err(|_| SqlCompileError::ResourceExhausted)?;
        for label in labels {
            let (target, conversion) = self.compared_value(label, &common)?;
            effects = effects.join(conversion);
            targets.push(target);
        }
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Validate)?;
        let intrinsic = novarocks_expression::case_static_own_effects(
            Some(&operand),
            targets.iter().map(Ok),
            &mut work,
            |error| match error.control_error() {
                Some(error) => SqlCompileError::from(error),
                None => SqlCompileError::Compilation(error.to_string()),
            },
        )?;
        work.finish()?;
        for value in values {
            let target = result
                .with_nullable(result.nullable() || value.nullable())
                .expect("validated CASE result");
            effects = effects.join(self.cast(value, &target, None)?);
        }
        Ok(effects.join(ScalarEffects::primitive(intrinsic)))
    }
}

pub(crate) const fn call_demands_values(control: ArgumentControl) -> bool {
    !matches!(
        control,
        ArgumentControl::TypeOnly | ArgumentControl::NoArguments
    )
}

pub(crate) fn call_effects(
    call: StaticScalarCallEffects,
    function: &novarocks_type_contract::FunctionId,
) -> ScalarEffects {
    let has_internal_assertion = function.as_str() == crate::functions::ASSERT_TRUE_ID;
    ScalarEffects {
        expression: call.own_effects,
        permits_filter_row_elimination: !has_internal_assertion
            && (call.permits_filter_row_elimination
                || call.own_effects.permits_boolean_reordering()),
        has_internal_assertion,
    }
}

fn unavailable_binding<T>(error: FunctionBindingError) -> Result<Option<T>> {
    match error {
        FunctionBindingError::MissingBindingDeclaration
        | FunctionBindingError::UnknownFunction
        | FunctionBindingError::UnknownOverload(_)
        | FunctionBindingError::NoMatchingOverload
        | FunctionBindingError::UnavailableImplementation(_) => Ok(None),
        other => Err(other.into()),
    }
}

fn comparison_effects(
    result: std::result::Result<ExpressionEffects, ComparisonPrepareError>,
) -> Result<ScalarEffects> {
    match result {
        Ok(effects) => Ok(ScalarEffects::primitive(effects)),
        Err(error) => primitive_refusal(
            error.control_error(),
            matches!(error, ComparisonPrepareError::Kernel(_)),
            &error,
        ),
    }
}

fn primitive_refusal(
    control: Option<CompileControlError>,
    invariant: bool,
    error: &impl std::fmt::Display,
) -> Result<ScalarEffects> {
    if let Some(control) = control {
        return Err(control.into());
    }
    if invariant {
        return Err(SqlCompileError::Compilation(error.to_string()));
    }
    Ok(ScalarEffects::UNKNOWN)
}

pub(crate) fn as_analyze_error(error: SqlCompileError) -> crate::analyze_error::AnalyzeError {
    use crate::analyze_error::AnalyzeError;
    match error {
        SqlCompileError::Cancelled => AnalyzeError::control(CompileControlError::Cancelled),
        SqlCompileError::DeadlineExceeded => {
            AnalyzeError::control(CompileControlError::DeadlineExceeded)
        }
        SqlCompileError::ResourceExhausted => {
            AnalyzeError::control(CompileControlError::ResourceExhausted)
        }
        SqlCompileError::CompilationSource(source) => AnalyzeError::shared_domain_source(source),
        SqlCompileError::Analyze(error) => error,
        error => AnalyzeError::internal(error.to_string()),
    }
}

impl ScalarEffectQuery<'_> {
    #[recursive::recursive]
    pub(crate) fn typed(&self, expr: &super::TypedExpr) -> Result<ScalarEffects> {
        use super::ExprKind;
        let ty = &expr.value_type;
        let children = |inputs: &[super::TypedExpr]| self.typed_children(inputs.iter());
        self.control.checkpoint(CompilePhase::Validate, 0)?;
        let effects = match &expr.kind {
            ExprKind::ColumnRef { .. }
            | ExprKind::LambdaParamRef { .. }
            | ExprKind::Literal(_)
            | ExprKind::Constant(_) => ScalarEffects::PURE,
            ExprKind::SubqueryPlaceholder { .. } => ScalarEffects::UNKNOWN,
            ExprKind::FunctionCall { binding, args, .. } => {
                let Some(own) = self.scalar_call(binding)? else {
                    return Ok(ScalarEffects::UNKNOWN);
                };
                let effects = call_effects(own, &binding.function_id);
                if call_demands_values(own.argument_control) {
                    effects.join(children(args)?)
                } else {
                    effects
                }
            }
            ExprKind::BinaryOp {
                left,
                right,
                op,
                decimal_overflow_policy,
            } => self
                .binary(
                    *op,
                    &left.value_type,
                    &right.value_type,
                    ty,
                    *decimal_overflow_policy,
                )?
                .join(self.typed(left)?)
                .join(self.typed(right)?),
            ExprKind::Cast {
                expr, cast_mode, ..
            } => self
                .cast(&expr.value_type, ty, *cast_mode)?
                .join(self.typed(expr)?),
            ExprKind::InList { expr, list, .. } => self
                .in_list(&expr.value_type, list.iter().map(|arg| &arg.value_type), ty)?
                .join(self.typed(expr)?)
                .join(children(list)?),
            ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } => self
                .between(
                    &expr.value_type,
                    &low.value_type,
                    &high.value_type,
                    ty,
                    *negated,
                )?
                .join(self.typed(expr)?)
                .join(self.typed(low)?)
                .join(self.typed(high)?),
            ExprKind::Like {
                expr,
                pattern,
                negated,
            } => self
                .like(&expr.value_type, &pattern.value_type, ty, *negated)?
                .join(self.typed(expr)?)
                .join(self.typed(pattern)?),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
                ..
            } => {
                let own = if let Some(operand) = operand {
                    self.simple_case(
                        &operand.value_type,
                        when_then.iter().map(|(when, _)| &when.value_type),
                        when_then
                            .iter()
                            .map(|(_, then)| &then.value_type)
                            .chain(else_expr.iter().map(|branch| &branch.value_type)),
                        ty,
                    )?
                } else {
                    ScalarEffects::PURE
                };
                own.join(
                    self.typed_children(
                        operand
                            .as_deref()
                            .into_iter()
                            .chain(when_then.iter().flat_map(|(when, then)| [when, then]))
                            .chain(else_expr.as_deref()),
                    )?,
                )
            }
            ExprKind::AggregateCall {
                args,
                order_by,
                filter,
                ..
            } => self.typed_children(
                args.iter()
                    .chain(order_by.iter().map(|key| &key.expr))
                    .chain(filter.as_deref()),
            )?,
            ExprKind::WindowCall { .. } => ScalarEffects::UNKNOWN,
            ExprKind::UnaryOp { expr: input, .. }
            | ExprKind::IsNull { expr: input, .. }
            | ExprKind::IsTruthValue { expr: input, .. }
            | ExprKind::Nested(input)
            | ExprKind::LambdaFunction { body: input, .. }
            | ExprKind::Lambda { body: input, .. } => self.typed(input)?,
        };
        Ok(effects)
    }

    fn typed_children<'a>(
        &self,
        mut children: impl Iterator<Item = &'a super::TypedExpr>,
    ) -> Result<ScalarEffects> {
        children.try_fold(ScalarEffects::PURE, |effects, child| {
            Ok(effects.join(self.typed(child)?))
        })
    }
}

#[cfg(test)]
mod tests;
