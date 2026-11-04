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

//! Fresh runtime-occurrence effects from actual ordered source/use edges.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_functions::{
    ArithmeticPrepareError, CastOperation, CastPrepareError, ComparisonPrepareError,
    ConstantPolicy, PreparedArithmeticRecipe, PreparedCastRecipe, PreparedComparisonRecipe,
    PreparedNullSafeComparisonRecipe, ScopedExpressionEffects,
};
use novarocks_physical_plan::{
    BinaryOperator, ConstantPools, ExprId, ExprKind, ExprNode, Fragment, FrozenPhysicalCall,
    PhysicalRootUses, RootUseBindingError, UnaryOperator,
};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    EffectContractError, ExpressionUseId, PureCompileControl, SemanticParameterError,
    SemanticParameterRef, SemanticParameterValue, SemanticParameters, ValueTypeError,
    arrow_data_types_exact_with_profile_observed,
};

use super::{
    lowered_draft::{SqlAuthoredPhysicalPlan, SqlSourceJournalError},
    physical_call_arguments::{PhysicalArgumentError, author_physical_argument_observed},
    physical_scalar_occurrences::{
        PhysicalScalarOccurrenceError, PhysicalScalarOccurrenceInput,
        prepare_physical_scalar_occurrence_observed,
    },
    physical_scalar_requests::{
        AuthoredPhysicalScalarRequest, PhysicalScalarRequestError,
        author_physical_scalar_request_from_journal_observed,
        author_physical_scalar_request_observed,
    },
    physical_window_occurrences::{
        PhysicalWindowOccurrenceError, PhysicalWindowOccurrenceInput,
        prepare_physical_window_occurrence_observed,
    },
    physical_window_requests::{
        AuthoredPhysicalWindowRequest, PhysicalWindowRequestError,
        author_physical_window_request_from_journal_observed,
        author_physical_window_request_observed,
    },
};
use crate::compiler::SqlFunctionCatalog;

#[cfg(test)]
#[path = "physical_expression_effects_tests.rs"]
mod tests;

/// SQL-authored facts for one invocation. The actual immutable source loan
/// prevents a stale journal entry from authenticating rewritten contents by ID.
/// Definitions may have several uses with different scopes and policies.
pub(crate) struct PhysicalCallSourceScope<'a> {
    pub source: &'a ExprNode,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub environment: &'a [SemanticParameterRef],
    pub proof_scope: CallProofScope,
}

pub(crate) struct PhysicalExpressionEffectsInput<'a> {
    pub fragment: &'a Fragment,
    pub roots: &'a PhysicalRootUses,
    pub constants: &'a ConstantPools,
    pub parameters: &'a SemanticParameters,
    pub literal_policy: ConstantPolicy,
    pub temporal_sources:
        Option<&'a BTreeMap<ExpressionUseId, novarocks_type_contract::TemporalSourcePlan<ExprId>>>,
    pub call_scopes: &'a BTreeMap<ExpressionUseId, PhysicalCallSourceScope<'a>>,
}

/// These are expression-use facts only. Relational/HOF lifecycles,
/// dead-definition capability admission and complete Package publication
/// remain mandatory separate owners; this result is not an executable plan.
#[derive(Debug)]
pub(crate) struct AuthoredPhysicalExpressionEffects {
    pub summaries: BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    pub calls: Vec<FrozenPhysicalCall>,
}

#[derive(Debug)]
pub(crate) enum PhysicalExpressionEffectsError {
    Control(CompileControlError),
    Roots(RootUseBindingError),
    Argument(PhysicalArgumentError),
    Journal(SqlSourceJournalError),
    Request(PhysicalScalarRequestError),
    Scalar(PhysicalScalarOccurrenceError),
    WindowRequest(PhysicalWindowRequestError),
    Window(PhysicalWindowOccurrenceError),
    Cast(CastPrepareError),
    /// A cast between these carriers has no prepared recipe.
    UnsupportedCast {
        source: CastPrepareError,
        from: arrow::datatypes::DataType,
        to: arrow::datatypes::DataType,
    },
    Arithmetic(ArithmeticPrepareError),
    Comparison(ComparisonPrepareError),
    Effects(EffectContractError),
    Parameter(SemanticParameterError),
    Type(ValueTypeError),
    MissingCallScope(ExpressionUseId),
    InvalidSource(&'static str),
    /// An expression kind without an effect author, with its kind label.
    UnsupportedExpression(ExprId, Box<str>),
}
impl From<CompileControlError> for PhysicalExpressionEffectsError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<EffectContractError> for PhysicalExpressionEffectsError {
    fn from(error: EffectContractError) -> Self {
        Self::Effects(error)
    }
}
impl From<SemanticParameterError> for PhysicalExpressionEffectsError {
    fn from(error: SemanticParameterError) -> Self {
        Self::Parameter(error)
    }
}
impl From<ValueTypeError> for PhysicalExpressionEffectsError {
    fn from(error: ValueTypeError) -> Self {
        Self::Type(error)
    }
}
macro_rules! source_error {
    ($error:ident, $variant:ident) => {
        impl From<$error> for PhysicalExpressionEffectsError {
            fn from(error: $error) -> Self {
                match error {
                    $error::Control(cause) => Self::Control(cause),
                    other => Self::$variant(other),
                }
            }
        }
    };
}
source_error!(SqlSourceJournalError, Journal);
source_error!(RootUseBindingError, Roots);
source_error!(PhysicalArgumentError, Argument);
source_error!(PhysicalScalarRequestError, Request);
source_error!(PhysicalScalarOccurrenceError, Scalar);
source_error!(PhysicalWindowRequestError, WindowRequest);
source_error!(PhysicalWindowOccurrenceError, Window);

enum AuthoredPhysicalExpressionRequest<'a> {
    Scalar(AuthoredPhysicalScalarRequest<'a>),
    Window(AuthoredPhysicalWindowRequest<'a>),
}
macro_rules! recipe_error {
    ($error:ident, $variant:ident) => {
        impl From<$error> for PhysicalExpressionEffectsError {
            fn from(error: $error) -> Self {
                match error {
                    $error::Control(cause)
                    | $error::Kernel(novarocks_functions::KernelFailure::CompileControl(cause)) => {
                        Self::Control(cause)
                    }
                    other => Self::$variant(other),
                }
            }
        }
    };
}
recipe_error!(CastPrepareError, Cast);
recipe_error!(ArithmeticPrepareError, Arithmetic);
recipe_error!(ComparisonPrepareError, Comparison);

/// Revalidate actual root/control correspondence, then walk borrowed use edges
/// child-first. Numeric identity order has no dependency meaning. Each use is
/// authored once; shared definitions retain distinct invocation contexts.
/// Static requests are shared only within this original immutable source loan.
/// No old summary, legacy binding effect or package default supplies a fact.
///
/// Caller admission covers maps, bounded stack, request/type clones and owner
/// preparation coexistence. Cooperative work and fallible Vec storage are not
/// a funding grant. An originating error starts no fallible completion check;
/// successful facts still observe the original footer.
pub(crate) fn author_physical_expression_effects_observed(
    input: PhysicalExpressionEffectsInput<'_>,
    functions: &dyn SqlFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalExpressionEffects, PhysicalExpressionEffectsError> {
    compose_expression_effects_observed(input, None, functions, control)
}

/// Production SQL borrows the original journal and immutable catalogue. Missing
/// or foreign source facts never fall back to direct physical reconstruction.
pub(crate) fn author_sql_expression_effects_observed(
    owner: &SqlAuthoredPhysicalPlan,
    input: PhysicalExpressionEffectsInput<'_>,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalExpressionEffects, PhysicalExpressionEffectsError> {
    compose_expression_effects_observed(
        input,
        Some(owner),
        owner.function_catalog().as_ref(),
        control,
    )
}

fn compose_expression_effects_observed<'source>(
    input: PhysicalExpressionEffectsInput<'source>,
    owner: Option<&'source SqlAuthoredPhysicalPlan>,
    functions: &dyn SqlFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalExpressionEffects, PhysicalExpressionEffectsError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        if let Some(owner) = owner {
            let original = owner.plan().fragments().get(&input.fragment.id());
            work.step()?;
            let same = original.is_some_and(|fragment| std::ptr::eq(fragment, input.fragment))
                && std::ptr::eq(owner.plan().constants(), input.constants)
                && std::ptr::eq(owner.plan().parameters(), input.parameters)
                && std::ptr::eq(owner.function_catalog().as_ref(), functions);
            work.step()?;
            if !same {
                return Err(PhysicalExpressionEffectsError::InvalidSource(
                    "SQL expression composition loans a foreign plan, pool, parameter or catalogue source",
                ));
            }
        }
        work.flush()?;
        input.roots.validate_fragment(input.fragment, control)?;
        work.flush()?;
        let flow = input.roots.flow();
        let definitions = input.fragment.expressions();
        // Exact scope coverage is checked before any owner preparation. A
        // matching topology is not proof of matching immutable source contents.
        for (&id, scope) in input.call_scopes {
            let source = flow
                .uses()
                .get(&id)
                .and_then(|use_| definitions.get(use_.definition));
            work.step()?;
            let matching_source = source.is_some_and(|source| {
                std::ptr::eq(source, scope.source)
                    && matches!(
                        source.kind,
                        ExprKind::FunctionCall { .. } | ExprKind::WindowCall { .. }
                    )
            });
            work.step()?;
            if !matching_source {
                return Err(PhysicalExpressionEffectsError::InvalidSource(
                    "call scope does not loan this actual source invocation",
                ));
            }
        }
        let mut requests = BTreeMap::new();
        for (&id, invocation) in flow.uses() {
            let source = definitions.get(invocation.definition).ok_or(
                PhysicalExpressionEffectsError::InvalidSource("missing source definition"),
            )?;
            work.step()?;
            if matches!(
                source.kind,
                ExprKind::FunctionCall { .. } | ExprKind::WindowCall { .. }
            ) {
                let has_scope = input.call_scopes.contains_key(&id);
                work.step()?;
                if !has_scope {
                    return Err(PhysicalExpressionEffectsError::MissingCallScope(id));
                }
                if let std::collections::btree_map::Entry::Vacant(entry) = requests.entry(source.id)
                {
                    let request = if matches!(source.kind, ExprKind::FunctionCall { .. }) {
                        AuthoredPhysicalExpressionRequest::Scalar(match owner {
                            Some(owner) => {
                                let entry = owner.checked_expression_call_source_observed(
                                    input.fragment,
                                    source,
                                    &mut work,
                                )?;
                                author_physical_scalar_request_from_journal_observed(
                                    &entry, &mut work,
                                )?
                            }
                            None => author_physical_scalar_request_observed(
                                source,
                                definitions,
                                input.constants,
                                input.literal_policy,
                                &mut work,
                            )?,
                        })
                    } else {
                        AuthoredPhysicalExpressionRequest::Window(match owner {
                            Some(owner) => {
                                let entry = owner.checked_expression_call_source_observed(
                                    input.fragment,
                                    source,
                                    &mut work,
                                )?;
                                author_physical_window_request_from_journal_observed(
                                    &entry,
                                    input.constants,
                                    &mut work,
                                )?
                            }
                            None => author_physical_window_request_observed(
                                source,
                                input.fragment,
                                input.constants,
                                input.literal_policy,
                                &mut work,
                            )?,
                        })
                    };
                    entry.insert(request);
                    work.step()?;
                }
                if owner.is_some() {
                    let scope = &input.call_scopes[&id];
                    let request = &requests[&source.id];
                    let (decimal, constant) = match request {
                        AuthoredPhysicalExpressionRequest::Scalar(request) => (
                            request.captured_decimal_overflow_policy(),
                            request.captured_constant_policy(),
                        ),
                        AuthoredPhysicalExpressionRequest::Window(request) => (
                            request.captured_decimal_overflow_policy(),
                            request.captured_constant_policy(),
                        ),
                    };
                    let same = decimal == Some(scope.decimal_overflow_policy)
                        && constant == Some(input.literal_policy);
                    work.step()?;
                    if !same {
                        return Err(PhysicalExpressionEffectsError::InvalidSource(
                            "SQL expression occurrence changes its original captured policy",
                        ));
                    }
                }
            }
            work.step()?;
        }
        let mut summaries = BTreeMap::new();
        let mut calls = Vec::new();
        let mut active = BTreeSet::new();
        let mut stack = Vec::new();
        work.flush()?;
        stack
            .try_reserve_exact(novarocks_type_contract::MAX_CONTROL_DEPTH)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        calls
            .try_reserve_exact(input.call_scopes.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for &root in flow.root_use_ids() {
            active.insert(root);
            stack.push((root, 0usize));
            work.step()?;
            while let Some(&(id, ordinal)) = stack.last() {
                let invocation =
                    flow.uses()
                        .get(&id)
                        .ok_or(PhysicalExpressionEffectsError::InvalidSource(
                            "missing actual use",
                        ))?;
                work.step()?;
                if let Some(&child) = invocation.arguments.get(ordinal) {
                    stack.last_mut().expect("current borrowed frame").1 += 1;
                    let completed = summaries.contains_key(&child);
                    work.step()?;
                    let unique = !completed && active.insert(child);
                    work.step()?;
                    if !unique {
                        return Err(PhysicalExpressionEffectsError::InvalidSource(
                            "actual use edges contain a shared or cyclic invocation",
                        ));
                    }
                    let too_deep = stack.len() == novarocks_type_contract::MAX_CONTROL_DEPTH;
                    work.step()?;
                    if too_deep {
                        return Err(PhysicalExpressionEffectsError::InvalidSource(
                            "actual use depth exceeds the checked control bound",
                        ));
                    }
                    stack.push((child, 0));
                    work.step()?;
                    continue;
                }
                let source = definitions.get(invocation.definition).ok_or(
                    PhysicalExpressionEffectsError::InvalidSource("missing actual definition"),
                )?;
                work.step()?;
                let summary = if let Some(request) = requests.get(&source.id) {
                    let scope = &input.call_scopes[&id];
                    let (frozen, summary) = match request {
                        AuthoredPhysicalExpressionRequest::Scalar(request) => {
                            let fresh = prepare_physical_scalar_occurrence_observed(
                                PhysicalScalarOccurrenceInput {
                                    source,
                                    definitions: Some(definitions),
                                    temporal_source: input
                                        .temporal_sources
                                        .and_then(|sources| sources.get(&id)),
                                    request,
                                    flow,
                                    use_id: id,
                                    child_effects: &summaries,
                                    parameters: input.parameters,
                                    environment: scope.environment,
                                    decimal_overflow_policy: scope.decimal_overflow_policy,
                                    proof_scope: scope.proof_scope,
                                },
                                functions,
                                &mut work,
                            )?;
                            (fresh.frozen, fresh.preparation.effects())
                        }
                        AuthoredPhysicalExpressionRequest::Window(request) => {
                            let fresh = prepare_physical_window_occurrence_observed(
                                PhysicalWindowOccurrenceInput {
                                    source,
                                    request,
                                    flow,
                                    use_id: id,
                                    child_effects: &summaries,
                                    parameters: input.parameters,
                                    environment: scope.environment,
                                    decimal_overflow_policy: scope.decimal_overflow_policy,
                                    proof_scope: scope.proof_scope,
                                },
                                functions,
                                &mut work,
                            )?;
                            (fresh.frozen, fresh.preparation.effects())
                        }
                    };
                    calls.push(frozen);
                    work.step()?;
                    summary
                } else {
                    let mut summary = primitive_own_effects(
                        source,
                        invocation.context,
                        &input,
                        control,
                        &mut work,
                    )?;
                    // Reuse the source's sole ordered child vocabulary, rather
                    // than reconstruct CASE ordinals or eager operand lists.
                    let mut next = 0;
                    source.kind.invocation_references_observed(|definition| {
                        let child = invocation.arguments.get(next).copied();
                        work.step()?;
                        let child = child.ok_or(PhysicalExpressionEffectsError::InvalidSource(
                            "source argument has no actual invocation edge",
                        ))?;
                        let child_use = flow.uses().get(&child);
                        work.step()?;
                        if child_use.is_none_or(|use_| use_.definition != definition) {
                            return Err(PhysicalExpressionEffectsError::InvalidSource(
                                "source and actual argument order differ",
                            ));
                        }
                        let effects = summaries.get(&child).copied();
                        work.step()?;
                        summary = summary.join_control_argument(
                            effects.ok_or(PhysicalExpressionEffectsError::InvalidSource(
                                "actual child effects were not authored",
                            ))?,
                            flow,
                            next,
                        )?;
                        next += 1;
                        work.step()?;
                        Ok::<_, PhysicalExpressionEffectsError>(())
                    })?;
                    if next != invocation.arguments.len() {
                        return Err(PhysicalExpressionEffectsError::InvalidSource(
                            "actual invocation has extra argument edges",
                        ));
                    }
                    work.step()?;
                    summary
                };
                summaries.insert(id, summary);
                active.remove(&id);
                stack.pop();
                work.step()?;
            }
        }
        let complete =
            summaries.len() == flow.uses().len() && calls.len() == input.call_scopes.len();
        work.step()?;
        if !complete {
            return Err(PhysicalExpressionEffectsError::InvalidSource(
                "actual expression effects have incomplete coverage",
            ));
        }
        Ok(AuthoredPhysicalExpressionEffects { summaries, calls })
    })();
    work.finish_result(result, PhysicalExpressionEffectsError::from)
}

fn primitive_own_effects(
    source: &ExprNode,
    context: novarocks_type_contract::ExpressionEffectContext,
    input: &PhysicalExpressionEffectsInput<'_>,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScopedExpressionEffects, PhysicalExpressionEffectsError> {
    let operand =
        |id| {
            input.fragment.expressions().get(id).ok_or(
                PhysicalExpressionEffectsError::InvalidSource("missing primitive operand"),
            )
        };
    let allow = |reference| match input.parameters.require(reference)? {
        SemanticParameterValue::AllowThrowException(value) => Ok(*value),
        _ => Err(PhysicalExpressionEffectsError::InvalidSource(
            "primitive parameter is not ALLOW_THROW_EXCEPTION",
        )),
    };
    work.step()?;
    Ok(match &source.kind {
        ExprKind::Value(_) => materialized_value_effects(context),
        ExprKind::Literal(_) | ExprKind::Constant(_) => {
            // Resolve/construct the actual leaf even when it is not a function
            // argument. NULL remains a checked constant, never a missing fact.
            author_physical_argument_observed(
                source,
                input.constants,
                input.literal_policy,
                CompilePhase::FunctionSpecialization,
                work,
            )?;
            ScopedExpressionEffects::pure_value(context)
        }
        ExprKind::Like {
            expr,
            pattern,
            negated,
        } => {
            work.flush()?;
            let recipe = novarocks_functions::PreparedNativeLikeRecipe::try_new(
                *negated,
                &operand(*expr)?.ty,
                &operand(*pattern)?.ty,
                &source.ty,
                control,
            )?;
            work.flush()?;
            recipe.own_effects(context)
        }
        ExprKind::InList {
            expr,
            list,
            negated,
        } => {
            work.flush()?;
            let mut candidates = Vec::with_capacity(list.len());
            for id in list {
                candidates.push(&operand(*id)?.ty);
                work.step()?;
            }
            work.flush()?;
            let recipe = novarocks_functions::PreparedNativeInListRecipe::try_new(
                *negated,
                &operand(*expr)?.ty,
                &candidates,
                &source.ty,
                control,
            )?;
            work.flush()?;
            recipe.own_effects(context)
        }
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => {
            work.flush()?;
            let recipe = novarocks_functions::PreparedNativeBetweenRecipe::try_new(
                novarocks_type_contract::NativeBetweenPlan::new(*negated),
                &operand(*expr)?.ty,
                &operand(*low)?.ty,
                &operand(*high)?.ty,
                &source.ty,
                control,
            )?;
            work.flush()?;
            recipe.own_effects(context)
        }
        ExprKind::Unary {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let child = operand(*expr)?;
            work.flush()?;
            let recipe = novarocks_functions::PreparedNativeNegateRecipe::try_new(
                &child.ty, &source.ty, control,
            )?;
            work.flush()?;
            recipe.own_effects(context)
        }
        ExprKind::Unary {
            op: UnaryOperator::BitwiseNot,
            expr,
        } => {
            let child = operand(*expr)?;
            work.flush()?;
            let recipe = novarocks_functions::PreparedNativeBitNotRecipe::try_new(
                &child.ty, &source.ty, control,
            )?;
            work.flush()?;
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
            if !arrow_data_types_exact_with_profile_observed::<PhysicalExpressionEffectsError>(
                target,
                source.ty.data_type(),
                novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
                || work.step().map_err(Into::into),
            )? {
                return Err(PhysicalExpressionEffectsError::InvalidSource(
                    "cast source result differs from its explicit target",
                ));
            }
            let child = operand(*expr);
            work.step()?;
            let child = child?;
            let allow = allow(*allow_throw_exception);
            work.step()?;
            let allow = allow?;
            work.flush()?;
            let recipe = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &child.ty,
                &source.ty,
                *decimal_overflow_policy,
                allow,
                control,
            )
            .map_err(|error| match error {
                cause @ CastPrepareError::Unsupported => {
                    PhysicalExpressionEffectsError::UnsupportedCast {
                        source: cause,
                        from: child.ty.data_type().clone(),
                        to: source.ty.data_type().clone(),
                    }
                }
                other => other.into(),
            })?;
            work.flush()?;
            recipe.own_effects(context)
        }
        ExprKind::Binary {
            op,
            left,
            right,
            decimal_overflow_policy,
            allow_throw_exception,
        } => {
            let left = operand(*left);
            work.step()?;
            let left = left?;
            let right = operand(*right);
            work.step()?;
            let right = right?;
            if let Some(operator) = op.arithmetic_operator() {
                let reference =
                    allow_throw_exception.ok_or(PhysicalExpressionEffectsError::InvalidSource(
                        "arithmetic has no explicit ALLOW_THROW_EXCEPTION reference",
                    ));
                work.step()?;
                let allow = allow(reference?);
                work.step()?;
                let allow = allow?;
                work.flush()?;
                let recipe = PreparedArithmeticRecipe::try_new(
                    operator,
                    &left.ty,
                    &right.ty,
                    &source.ty,
                    *decimal_overflow_policy,
                    allow,
                    control,
                )?;
                work.flush()?;
                recipe.own_effects(context)
            } else if let Some(operator) = op.comparison_operator() {
                work.flush()?;
                let recipe =
                    PreparedComparisonRecipe::try_new(operator, &left.ty, &right.ty, control)?;
                work.flush()?;
                recipe.own_effects(context)
            } else if *op == BinaryOperator::EqForNull {
                work.flush()?;
                let recipe =
                    PreparedNullSafeComparisonRecipe::try_new(&left.ty, &right.ty, control)?;
                work.flush()?;
                recipe.own_effects(context)
            } else {
                return Err(PhysicalExpressionEffectsError::UnsupportedExpression(
                    source.id,
                    format!("Binary({op:?})").into(),
                ));
            }
        }
        other => {
            return Err(PhysicalExpressionEffectsError::UnsupportedExpression(
                source.id,
                expression_kind_label(other),
            ));
        }
    })
}

/// The variant name of an expression kind, for refusals that must say which
/// kind has no effect author without dumping its operands.
fn expression_kind_label(kind: &ExprKind) -> Box<str> {
    let debug = format!("{kind:?}");
    let end = debug.find([' ', '{', '(']).unwrap_or(debug.len());
    match kind {
        ExprKind::Unary { op, .. } => format!("Unary({op:?})").into(),
        _ => debug[..end].into(),
    }
}

/// The original Value leaf author is shared by authenticated materialized
/// Writer channels. Upstream evaluation effects stay with their own operators.
pub(crate) fn materialized_value_effects(
    context: novarocks_type_contract::ExpressionEffectContext,
) -> ScopedExpressionEffects {
    ScopedExpressionEffects::pure_value(context)
}
