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

//! Per-use claims and effect summaries. Children are authored before parents.
//! Non-call nodes take their own effect from the Functions recipe.

use super::controls::installed_scalar_control_shape;
use super::error::{
    CallAuthorError, InvariantRefusal, arithmetic_author_error, cast_author_error,
    comparison_author_error,
};
use super::{CallAuthorInputs, UnitCallAuthorship, scalar_binding_selection};
use novarocks_constant_contract::{ConstantError, ConstantTableError};
use novarocks_expression_contract::{
    BinaryOperator, CallRequestMaterializationError, ExprId, ExprKind, ExprNode,
    ExpressionCallClaim, SealedUnit, TemporalSourceProjectionError, UnaryOperator,
};
use novarocks_functions::{
    CallArgumentUses, CallEffectInput, PreparedArithmeticRecipe, PreparedCastRecipe,
    PreparedComparisonRecipe, PreparedNullSafeComparisonRecipe, PureCallPreparation, PureKernelAbi,
    ScopedExpressionEffects, TemporalSourceChannel,
};
use novarocks_type_contract::{
    ArrowTypeProfile, CallProofScope, CompileCheckpoints, CompileControlError, CompilePhase,
    ControlShape, ExpressionEffectContext, ExpressionUseId, SemanticParameterValue,
    SemanticParameters, arrow_data_types_exact_with_profile_observed,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub fn author_unit_calls(
    unit: &SealedUnit,
    inputs: &CallAuthorInputs<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<UnitCallAuthorship, CallAuthorError> {
    work.step()?;
    let mut owned =
        CompileCheckpoints::try_new(work.control(), CompilePhase::FunctionSpecialization)?;
    let result = compose(unit, inputs, &mut owned);
    let result = owned.finish_result(result, CallAuthorError::from);
    work.step()?;
    result
}

fn compose(
    unit: &SealedUnit,
    inputs: &CallAuthorInputs<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<UnitCallAuthorship, CallAuthorError> {
    validate_static_shapes(unit, inputs, work)?;
    let requests = materialize_requests(unit, inputs, work)?;
    let flow = unit.control_flow();
    let mut summaries = BTreeMap::new();
    let mut claims = BTreeMap::new();
    let mut active = BTreeSet::new();
    let mut stack = Vec::new();
    work.flush()?;
    stack
        .try_reserve_exact(novarocks_type_contract::MAX_CONTROL_DEPTH)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    for &root in unit.root_use_ids() {
        active.insert(root);
        stack.push((root, 0usize));
        work.step()?;
        while let Some(&(id, ordinal)) = stack.last() {
            let invocation = flow
                .uses()
                .get(&id)
                .ok_or(CallAuthorError::effects_invalid("missing actual use"))?;
            work.step()?;
            if let Some(&child) = invocation.arguments.get(ordinal) {
                stack.last_mut().expect("current borrowed frame").1 += 1;
                let completed = summaries.contains_key(&child);
                work.step()?;
                let unique = !completed && active.insert(child);
                work.step()?;
                if !unique {
                    return Err(CallAuthorError::effects_invalid(
                        "actual use edges contain a shared or cyclic invocation",
                    ));
                }
                if stack.len() == novarocks_type_contract::MAX_CONTROL_DEPTH {
                    return Err(CallAuthorError::effects_invalid(
                        "actual use depth exceeds the checked control bound",
                    ));
                }
                stack.push((child, 0));
                work.step()?;
                continue;
            }
            let source =
                unit.definition(invocation.definition)
                    .ok_or(CallAuthorError::effects_invalid(
                        "missing actual definition",
                    ))?;
            work.step()?;
            let summary = if requests.contains_key(&source.id) {
                author_call(
                    unit,
                    inputs,
                    source,
                    id,
                    &requests,
                    &summaries,
                    work,
                    &mut claims,
                )?
            } else {
                author_primitive(unit, inputs, source, invocation, &summaries, work)?
            };
            summaries.insert(id, summary);
            active.remove(&id);
            stack.pop();
            work.step()?;
        }
    }
    let call_uses = flow
        .uses()
        .iter()
        .filter(|(_, invocation)| {
            unit.definition(invocation.definition)
                .is_some_and(|source| matches!(source.kind, ExprKind::FunctionCall { .. }))
        })
        .count();
    if summaries.len() != flow.uses().len() || claims.len() != call_uses {
        return Err(CallAuthorError::effects_invalid(
            "actual expression effects have incomplete coverage",
        ));
    }
    Ok(UnitCallAuthorship { claims, summaries })
}

fn validate_static_shapes(
    unit: &SealedUnit,
    inputs: &CallAuthorInputs<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), CallAuthorError> {
    for source in unit.definitions() {
        work.step()?;
        match &source.kind {
            ExprKind::Like {
                expr,
                pattern,
                negated,
            } => {
                work.flush()?;
                novarocks_functions::PreparedNativeLikeRecipe::try_new(
                    *negated,
                    &definition(unit, *expr)?.ty,
                    &definition(unit, *pattern)?.ty,
                    &source.ty,
                    work.control(),
                )
                .map_err(comparison_author_error)?;
                work.flush()?;
            }
            ExprKind::InList { expr, list, .. } => {
                work.flush()?;
                novarocks_functions::PreparedNativeInListRecipe::validate_borrowed_profile(
                    &definition(unit, *expr)?.ty,
                    list.iter().map(|id| {
                        unit.definition(*id)
                            .map(|node| &node.ty)
                            .expect("checked static IN candidate")
                    }),
                    &source.ty,
                    work.control(),
                )
                .map_err(comparison_author_error)?;
                work.flush()?;
            }
            ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } => {
                work.flush()?;
                novarocks_functions::PreparedNativeBetweenRecipe::try_new(
                    novarocks_type_contract::NativeBetweenPlan::new(*negated),
                    &definition(unit, *expr)?.ty,
                    &definition(unit, *low)?.ty,
                    &definition(unit, *high)?.ty,
                    &source.ty,
                    work.control(),
                )
                .map_err(comparison_author_error)?;
                work.flush()?;
            }
            _ => {}
        }
        if !matches!(
            source.kind,
            ExprKind::Conjunction { .. } | ExprKind::Disjunction { .. }
        ) {
            continue;
        }
        let valid = novarocks_expression_contract::boolean_connective_nullability_observed(
            source,
            |id| unit.definition(id),
            |operand, work| {
                let ExprKind::Constant(reference) = operand.kind else {
                    return Err(CallAuthorError::effects_invalid(
                        "Boolean source is not an immediate constant",
                    ));
                };
                let constant = inputs
                    .environment
                    .constants
                    .resolve_observed(reference, &operand.ty, work)
                    .map_err(table_error)?;
                constant.try_boolean().map_err(constant_error)
            },
            work,
        )?;
        if !valid {
            return Err(CallAuthorError::effects_invalid(
                "Boolean connective result discards nullable operands without a deciding constant",
            ));
        }
    }
    Ok(())
}

fn materialize_requests<'a>(
    unit: &'a SealedUnit,
    inputs: &CallAuthorInputs<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    BTreeMap<ExprId, novarocks_expression_contract::MaterializedCallRequest<'a>>,
    CallAuthorError,
> {
    let mut requests = BTreeMap::new();
    for source in unit.definitions() {
        if !matches!(source.kind, ExprKind::FunctionCall { .. }) {
            continue;
        }
        let Some(request) = unit.requests().get(&source.id) else {
            return Err(CallAuthorError::Invariant(InvariantRefusal::Source(
                "call definition has no original request",
            )));
        };
        if request.constant_policy != inputs.constant_policy {
            return Err(CallAuthorError::Invariant(InvariantRefusal::ConstantPolicy));
        }
        let materialized = novarocks_expression_contract::materialize_call_request_observed(
            request,
            inputs.environment.constants,
            work,
            novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS,
        )
        .map_err(materialization_error)?;
        requests.insert(source.id, materialized);
        work.step()?;
    }
    Ok(requests)
}

fn author_call(
    unit: &SealedUnit,
    inputs: &CallAuthorInputs<'_>,
    source: &ExprNode,
    use_id: ExpressionUseId,
    requests: &BTreeMap<ExprId, novarocks_expression_contract::MaterializedCallRequest<'_>>,
    summaries: &BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    work: &mut CompileCheckpoints<'_>,
    claims: &mut BTreeMap<ExpressionUseId, ExpressionCallClaim>,
) -> Result<ScopedExpressionEffects, CallAuthorError> {
    let flow = unit.control_flow();
    let invocation = flow
        .uses()
        .get(&use_id)
        .ok_or(CallAuthorError::scalar_invalid("missing scalar use"))?;
    let ExprKind::FunctionCall { function, args } = &source.kind else {
        return Err(CallAuthorError::scalar_invalid(
            "scalar occurrence source is not an actual function call",
        ));
    };
    let request = requests
        .get(&source.id)
        .ok_or(CallAuthorError::scalar_invalid(
            "scalar occurrence and static request have different source definitions",
        ))?;
    let declaration = inputs
        .functions
        .metadata()
        .pure_overload_declaration_observed(
            &function.function_id,
            function.kind,
            &function.overload,
            work.control(),
        )
        .map_err(|error| CallAuthorError::specialization(error, true))?;
    let abi = declaration.implementation().abi;
    if !matches!(
        abi,
        PureKernelAbi::ScalarV1
            | PureKernelAbi::ScalarInvocationV1
            | PureKernelAbi::ControlIntrinsicV1
    ) {
        return Err(CallAuthorError::effects(format!(
            "Scalar(UnsupportedAbi({abi:?}))"
        )));
    }
    let selection = scalar_binding_selection(function);
    let selected_control = if abi == PureKernelAbi::ScalarInvocationV1 {
        declaration
            .selected_argument_control_observed(
                &function.function_id,
                selection.as_ref(),
                request.request().logical_argument_count,
                work.control(),
            )
            .map_err(|error| {
                CallAuthorError::specialization(
                    novarocks_functions::FunctionSpecializationFailure::Binding(error),
                    true,
                )
            })?
    } else {
        declaration.effects().argument_control
    };
    let temporal = unit.temporal_source(use_id);
    let shape = match selected_control {
        novarocks_type_contract::ArgumentControl::TemporalSource(kind) => {
            let source_plan = temporal.ok_or(CallAuthorError::scalar_invalid(
                "missing same-emission temporal source facts",
            ))?;
            let projected = match novarocks_expression_contract::temporal_source_definitions_from_lookup_observed(
                kind,
                |id| unit.definition(id),
                args,
                work,
            ) {
                Ok(projected) => projected,
                Err(TemporalSourceProjectionError::Control(cause)) => {
                    return Err(CallAuthorError::Control(cause));
                }
                Err(TemporalSourceProjectionError::Invalid(_)) => {
                    return Err(CallAuthorError::scalar_invalid(
                        "invalid original temporal source projection",
                    ));
                }
            };
            if source_plan.facts != projected.facts {
                return Err(CallAuthorError::scalar_invalid(
                    "stale temporal source roles, definitions or facts",
                ));
            }
            Some(ControlShape::TemporalSource(source_plan.facts.shape()))
        }
        control => {
            if temporal.is_some() {
                return Err(CallAuthorError::scalar_invalid(
                    "temporal sources on an ordinary scalar call",
                ));
            }
            installed_scalar_control_shape(control, args.len())
        }
    };
    let shape = shape.ok_or(CallAuthorError::scalar_invalid(
        "installed scalar owner has no scalar control shape",
    ))?;
    let type_only = matches!(shape, ControlShape::TypeOnly | ControlShape::NoArguments);
    let expected_edges = if type_only {
        0
    } else {
        temporal.map_or(args.len(), |source| source.sources.len())
    };
    if invocation.control != shape || invocation.arguments.len() != expected_edges {
        return Err(CallAuthorError::scalar_invalid(
            "actual scalar control differs from its installed owner",
        ));
    }
    let decimal = *inputs
        .decimal_overflow
        .get(&source.id)
        .ok_or(CallAuthorError::Invariant(
            InvariantRefusal::MissingOverflow(source.id),
        ))?;
    let mut argument_uses = Vec::new();
    let mut source_channels = Vec::new();
    let mut children = ScopedExpressionEffects::pure_value(invocation.context);
    let count = temporal.map_or(args.len(), |source| source.sources.len());
    for ordinal in 0..count {
        let definition_id = match temporal {
            Some(source) => source.sources[ordinal].definition,
            None => args[ordinal],
        };
        if type_only {
            argument_uses.push(None);
        } else {
            let child_id = invocation.arguments[ordinal];
            let child = flow
                .uses()
                .get(&child_id)
                .ok_or(CallAuthorError::scalar_invalid(
                    "ordered scalar argument use has a different source definition",
                ))?;
            if child.definition != definition_id {
                return Err(CallAuthorError::scalar_invalid(
                    "ordered scalar argument use has a different source definition",
                ));
            }
            let effects = summaries
                .get(&child_id)
                .copied()
                .ok_or(CallAuthorError::effects(format!(
                    "Scalar(MissingChildEffects({child_id:?}))"
                )))?;
            children = children
                .join_control_argument(effects, flow, ordinal)
                .map_err(|error| CallAuthorError::effects(format!("Effects({error:?})")))?;
            if let Some(source) = temporal {
                let channel = &source.sources[ordinal];
                if channel.use_id != child_id {
                    return Err(CallAuthorError::scalar_invalid(
                        "temporal source use differs from its ordered argument edge",
                    ));
                }
                let node = definition(unit, definition_id)?;
                source_channels.push(TemporalSourceChannel {
                    role: channel.role,
                    context: child.context,
                    value_type: &node.ty,
                });
            }
            argument_uses.push(Some(child_id));
        }
        work.step()?;
    }
    let (regexp_count_pattern_source, to_base64_byte_source) =
        source_facts(function, args, unit, work)?;
    let argument_uses = match temporal {
        Some(source) => CallArgumentUses::TemporalSources {
            facts: &source.facts,
            channels: &source_channels,
        },
        None => match regexp_count_pattern_source {
            Some(source) => CallArgumentUses::RegexpCountPattern {
                source,
                channels: &argument_uses,
            },
            None => match to_base64_byte_source {
                Some(source) => CallArgumentUses::ToBase64Bytes {
                    source,
                    channels: &argument_uses,
                },
                None => CallArgumentUses::SelectedChannels(&argument_uses),
            },
        },
    };
    let preparation = if matches!(
        abi,
        PureKernelAbi::ScalarV1 | PureKernelAbi::ScalarInvocationV1
    ) {
        PureCallPreparation::Scalar {
            arguments: children,
        }
    } else {
        PureCallPreparation::ControlIntrinsic {
            arguments: children,
        }
    };
    let prepared = inputs
        .functions
        .prepare_fresh(
            CallEffectInput {
                context: invocation.context.into(),
                argument_uses,
                function_id: &function.function_id,
                kind: function.kind,
                selected: selection.as_ref(),
                request: request.request(),
                environment: &[],
                parameters: inputs.environment.parameters,
                decimal_overflow_policy: decimal,
                proof_scope: CallProofScope::Region(invocation.context.region),
            },
            Arc::clone(&selection),
            preparation,
            work.control(),
        )
        .map_err(|error| CallAuthorError::specialization(error, true))?;
    let effects = prepared
        .effects()
        .for_use(invocation.context)
        .map_err(|error| CallAuthorError::effects(format!("Effects({error:?})")))?;
    let claim = ExpressionCallClaim {
        context: invocation.context,
        effects: prepared.call_contract().effects().clone(),
        decimal_overflow_policy: decimal,
        regexp_count_pattern_source,
        to_base64_byte_source,
        temporal_source: temporal.cloned(),
    };
    if claims.insert(use_id, claim).is_some() {
        return Err(CallAuthorError::effects_invalid(
            "scalar invocation has repeated call claims",
        ));
    }
    Ok(ScopedExpressionEffects::primitive(
        invocation.context,
        effects,
    ))
}

fn source_facts(
    function: &novarocks_expression_contract::BoundFunction,
    args: &[ExprId],
    unit: &SealedUnit,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (
        Option<novarocks_type_contract::RegexpCountPatternSource>,
        Option<novarocks_type_contract::ToBase64ByteSource>,
    ),
    CallAuthorError,
> {
    let name = function.function_id.as_str();
    if name == "builtin.scalar/regexp_count/v1" {
        return match novarocks_expression_contract::regexp_count_pattern_source_observed(
            unit.definitions(),
            args,
            work,
        ) {
            Ok(source) => Ok((Some(source), None)),
            Err(TemporalSourceProjectionError::Control(cause)) => {
                Err(CallAuthorError::Control(cause))
            }
            Err(TemporalSourceProjectionError::Invalid(message)) => {
                Err(CallAuthorError::scalar_invalid(message))
            }
        };
    }
    if name == "builtin.scalar/to_base64/v1" {
        return match novarocks_expression_contract::to_base64_byte_source_observed(
            unit.definitions(),
            args,
            work,
        ) {
            Ok(source) => Ok((None, Some(source))),
            Err(TemporalSourceProjectionError::Control(cause)) => {
                Err(CallAuthorError::Control(cause))
            }
            Err(TemporalSourceProjectionError::Invalid(message)) => {
                Err(CallAuthorError::scalar_invalid(message))
            }
        };
    }
    Ok((None, None))
}

fn author_primitive(
    unit: &SealedUnit,
    inputs: &CallAuthorInputs<'_>,
    source: &ExprNode,
    invocation: &novarocks_type_contract::ExpressionInvocation<ExprId>,
    summaries: &BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScopedExpressionEffects, CallAuthorError> {
    let mut summary = own_effects(unit, inputs, source, invocation.context, work)?;
    let flow = unit.control_flow();
    let mut next = 0;
    source.kind.invocation_references_observed(|definition| {
        let child =
            invocation
                .arguments
                .get(next)
                .copied()
                .ok_or(CallAuthorError::effects_invalid(
                    "source argument has no actual invocation edge",
                ))?;
        let child_use = flow.uses().get(&child);
        if child_use.is_none_or(|use_| use_.definition != definition) {
            return Err(CallAuthorError::effects_invalid(
                "source and actual argument order differ",
            ));
        }
        let effects = summaries
            .get(&child)
            .copied()
            .ok_or(CallAuthorError::effects_invalid(
                "actual child effects were not authored",
            ))?;
        summary = summary
            .join_control_argument(effects, flow, next)
            .map_err(|error| CallAuthorError::effects(format!("Effects({error:?})")))?;
        next += 1;
        work.step()?;
        Ok(())
    })?;
    if next != invocation.arguments.len() {
        return Err(CallAuthorError::effects_invalid(
            "actual invocation has extra argument edges",
        ));
    }
    Ok(summary)
}

fn own_effects(
    unit: &SealedUnit,
    inputs: &CallAuthorInputs<'_>,
    source: &ExprNode,
    context: ExpressionEffectContext,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScopedExpressionEffects, CallAuthorError> {
    let allow = |reference, parameters: &SemanticParameters| match parameters
        .require(reference)
        .map_err(|error| CallAuthorError::effects(format!("Parameter({error:?})")))?
    {
        SemanticParameterValue::AllowThrowException(value) => Ok(*value),
        _ => Err(CallAuthorError::effects_invalid(
            "primitive parameter is not ALLOW_THROW_EXCEPTION",
        )),
    };
    work.step()?;
    Ok(match &source.kind {
        ExprKind::Value(_) => ScopedExpressionEffects::pure_value(context),
        ExprKind::Constant(reference) => {
            inputs
                .environment
                .constants
                .resolve_observed(*reference, &source.ty, work)
                .map_err(table_error)?;
            ScopedExpressionEffects::pure_value(context)
        }
        ExprKind::Like {
            expr,
            pattern,
            negated,
        } => {
            let recipe = novarocks_functions::PreparedNativeLikeRecipe::try_new(
                *negated,
                &definition(unit, *expr)?.ty,
                &definition(unit, *pattern)?.ty,
                &source.ty,
                work.control(),
            )
            .map_err(comparison_author_error)?;
            recipe.own_effects(context)
        }
        ExprKind::InList {
            expr,
            list,
            negated,
        } => {
            let mut candidates = Vec::with_capacity(list.len());
            for id in list {
                candidates.push(&definition(unit, *id)?.ty);
                work.step()?;
            }
            let recipe = novarocks_functions::PreparedNativeInListRecipe::try_new(
                *negated,
                &definition(unit, *expr)?.ty,
                &candidates,
                &source.ty,
                work.control(),
            )
            .map_err(comparison_author_error)?;
            recipe.own_effects(context)
        }
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let recipe = novarocks_functions::PreparedNativeBetweenRecipe::try_new(
                novarocks_type_contract::NativeBetweenPlan::new(*negated),
                &definition(unit, *expr)?.ty,
                &definition(unit, *low)?.ty,
                &definition(unit, *high)?.ty,
                &source.ty,
                work.control(),
            )
            .map_err(comparison_author_error)?;
            recipe.own_effects(context)
        }
        ExprKind::Unary {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let recipe = novarocks_functions::PreparedNativeNegateRecipe::try_new(
                &definition(unit, *expr)?.ty,
                &source.ty,
                work.control(),
            )
            .map_err(arithmetic_author_error)?;
            recipe.own_effects(context)
        }
        ExprKind::Unary {
            op: UnaryOperator::BitwiseNot,
            expr,
        } => {
            let recipe = novarocks_functions::PreparedNativeBitNotRecipe::try_new(
                &definition(unit, *expr)?.ty,
                &source.ty,
                work.control(),
            )
            .map_err(arithmetic_author_error)?;
            recipe.own_effects(context)
        }
        ExprKind::Unary {
            op: UnaryOperator::Not,
            ..
        }
        | ExprKind::IsNull { .. }
        | ExprKind::Conjunction { .. }
        | ExprKind::Disjunction { .. }
        | ExprKind::Case { .. } => ScopedExpressionEffects::pure_value(context),
        ExprKind::Cast {
            expr,
            target,
            decimal_overflow_policy,
            allow_throw_exception,
        } => {
            if !arrow_data_types_exact_with_profile_observed::<CallAuthorError>(
                target,
                source.ty.data_type(),
                ArrowTypeProfile::CanonicalValue,
                || work.step().map_err(Into::into),
            )? {
                return Err(CallAuthorError::effects_invalid(
                    "cast source result differs from its explicit target",
                ));
            }
            let child = definition(unit, *expr)?;
            let allow = allow(*allow_throw_exception, inputs.environment.parameters)?;
            let recipe = PreparedCastRecipe::try_new(
                novarocks_functions::CastOperation::Carrier,
                &child.ty,
                &source.ty,
                *decimal_overflow_policy,
                allow,
                work.control(),
            )
            .map_err(|error| {
                cast_author_error(
                    error,
                    child.ty.data_type().clone(),
                    source.ty.data_type().clone(),
                )
            })?;
            recipe.own_effects(context)
        }
        ExprKind::Binary {
            op,
            left,
            right,
            decimal_overflow_policy,
            allow_throw_exception,
        } => {
            let left = definition(unit, *left)?;
            let right = definition(unit, *right)?;
            if let Some(operator) = op.arithmetic_operator() {
                let reference = allow_throw_exception.ok_or(CallAuthorError::effects_invalid(
                    "arithmetic has no explicit ALLOW_THROW_EXCEPTION reference",
                ))?;
                let allow = allow(reference, inputs.environment.parameters)?;
                let recipe = PreparedArithmeticRecipe::try_new(
                    operator,
                    &left.ty,
                    &right.ty,
                    &source.ty,
                    *decimal_overflow_policy,
                    allow,
                    work.control(),
                )
                .map_err(arithmetic_author_error)?;
                recipe.own_effects(context)
            } else if let Some(operator) = op.comparison_operator() {
                let recipe = PreparedComparisonRecipe::try_new(
                    operator,
                    &left.ty,
                    &right.ty,
                    work.control(),
                )
                .map_err(comparison_author_error)?;
                recipe.own_effects(context)
            } else if *op == BinaryOperator::EqForNull {
                let recipe =
                    PreparedNullSafeComparisonRecipe::try_new(&left.ty, &right.ty, work.control())
                        .map_err(comparison_author_error)?;
                recipe.own_effects(context)
            } else {
                return Err(CallAuthorError::effects(format!(
                    "UnsupportedExpression({:?}, Binary({op:?}))",
                    source.id
                )));
            }
        }
        other => {
            return Err(CallAuthorError::effects(format!(
                "UnsupportedExpression({:?}, {})",
                source.id,
                expression_kind_label(other)
            )));
        }
    })
}

fn expression_kind_label(kind: &ExprKind) -> String {
    let debug = format!("{kind:?}");
    let end = debug.find([' ', '{', '(']).unwrap_or(debug.len());
    match kind {
        ExprKind::Unary { op, .. } => format!("Unary({op:?})"),
        _ => debug[..end].to_string(),
    }
}

fn definition(unit: &SealedUnit, id: ExprId) -> Result<&ExprNode, CallAuthorError> {
    unit.definition(id).ok_or(CallAuthorError::effects_invalid(
        "missing primitive operand",
    ))
}

fn table_error(error: ConstantTableError) -> CallAuthorError {
    match error {
        ConstantTableError::Control(cause)
        | ConstantTableError::Constant(ConstantError::Control(cause)) => {
            CallAuthorError::Control(cause)
        }
        ConstantTableError::Constant(limit @ ConstantError::Limit(_)) => {
            CallAuthorError::Capability(super::error::CapabilityRefusal::ConstantLimit(limit))
        }
        ConstantTableError::Constant(other) => {
            CallAuthorError::effects(format!("Argument(Constant({other:?}))"))
        }
        other => CallAuthorError::effects(format!("Argument(Reference(Table({other:?})))")),
    }
}

fn constant_error(error: ConstantError) -> CallAuthorError {
    match error {
        ConstantError::Control(cause) => CallAuthorError::Control(cause),
        limit @ ConstantError::Limit(_) => {
            CallAuthorError::Capability(super::error::CapabilityRefusal::ConstantLimit(limit))
        }
        other => CallAuthorError::effects(format!("Argument(Constant({other:?}))")),
    }
}

fn materialization_error(error: CallRequestMaterializationError) -> CallAuthorError {
    match error {
        CallRequestMaterializationError::Control(cause) => CallAuthorError::Control(cause),
        CallRequestMaterializationError::Reference(cause) => table_error(cause),
        CallRequestMaterializationError::Invalid(detail) => {
            CallAuthorError::effects(format!("Request(Invalid({detail:?}))"))
        }
    }
}
