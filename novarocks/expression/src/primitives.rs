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

//! Mandatory primitive recipes derived from the same checked use/type chain.
//! No external side table, name resolver or runtime preparation is accepted.

use crate::creation::{ProgramCreation, ProgramCreationFailure};
use crate::{CompiledParts, StaticExprKind};
use novarocks_functions::{
    ComparisonOperator, ComparisonPrepareError, FunctionArgumentType, FunctionValueType,
    PreparedComparisonRecipe, PreparedCompositeComparisonRecipe,
};
use novarocks_type_contract::ExpressionUseId;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, OperatorFunction,
    PureCompileControl, ValueSchemaCreationFactory, ValueSchemaCreationScope,
    ValueTypeStorageLifetime,
};
use std::{collections::BTreeMap, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ComparisonSite {
    CaseWhen {
        occurrence: ExpressionUseId,
        arm: u32,
    },
}

#[derive(Debug)]
pub(crate) enum PreparedNodeComparison {
    Ordinary(PreparedComparisonRecipe),
    Composite(PreparedCompositeComparisonRecipe),
}

pub(crate) fn case_own_effects<'a, E: From<CompileControlError>>(
    operand: Option<&FunctionValueType>,
    labels: impl Iterator<Item = Result<&'a FunctionValueType, E>>,
    context: novarocks_type_contract::ExpressionEffectContext,
    work: &mut CompileCheckpoints<'_>,
    comparison_error: impl Fn(ComparisonPrepareError) -> E,
) -> Result<novarocks_functions::ScopedExpressionEffects, E> {
    let mut effects = novarocks_type_contract::ExpressionEffects::PURE_VALUE;
    if let Some(operand) = operand.filter(|ty| PreparedCompositeComparisonRecipe::is_composite(ty))
    {
        for label in labels {
            work.step()?;
            if let Some(recipe) = PreparedCompositeComparisonRecipe::try_new_for_operator(
                OperatorFunction::Equal,
                [operand, label?],
                work.control(),
            )
            .map_err(&comparison_error)?
            {
                effects.may_raise_row_error |= recipe.may_raise_row_error();
            }
        }
    }
    Ok(novarocks_functions::ScopedExpressionEffects::primitive(
        context, effects,
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramPrimitiveError {
    Control(CompileControlError),
    Comparison(ComparisonPrepareError),
    Cast(novarocks_functions::CastPrepareError),
    Invalid(&'static str),
}
impl From<CompileControlError> for ProgramPrimitiveError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<ComparisonPrepareError> for ProgramPrimitiveError {
    fn from(value: ComparisonPrepareError) -> Self {
        match value {
            ComparisonPrepareError::Control(cause)
            | ComparisonPrepareError::Kernel(novarocks_functions::KernelFailure::CompileControl(
                cause,
            )) => Self::Control(cause),
            original => Self::Comparison(original),
        }
    }
}
impl fmt::Display for ProgramPrimitiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Comparison(e) => e.fmt(f),
            Self::Cast(e) => e.fmt(f),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for ProgramPrimitiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Comparison(e) => Some(e),
            Self::Cast(e) => Some(e),
            Self::Invalid(_) => None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CreatedPrimitiveMap<K, V> {
    pub(crate) entries: BTreeMap<K, V>,
    pub(crate) _storage: ValueTypeStorageLifetime,
}
impl<K, V> std::ops::Deref for CreatedPrimitiveMap<K, V> {
    type Target = BTreeMap<K, V>;
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}
impl<H> From<ProgramPrimitiveError> for ProgramCreationFailure<ProgramPrimitiveError, H> {
    fn from(error: ProgramPrimitiveError) -> Self {
        match error {
            ProgramPrimitiveError::Control(error) => Self::Control(error),
            error => Self::Owner(error),
        }
    }
}
impl<H> From<ComparisonPrepareError> for ProgramCreationFailure<ProgramPrimitiveError, H> {
    fn from(error: ComparisonPrepareError) -> Self {
        ProgramPrimitiveError::from(error).into()
    }
}
impl<H> From<novarocks_functions::CastPrepareError>
    for ProgramCreationFailure<ProgramPrimitiveError, H>
{
    fn from(error: novarocks_functions::CastPrepareError) -> Self {
        ProgramPrimitiveError::from(error).into()
    }
}

fn before_recipe_insert<K: Ord, V, H>(
    recipes: &BTreeMap<K, V>,
    retained: &mut ProgramCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramCreationFailure<ProgramPrimitiveError, H>> {
    retained.before_tree_insert::<ProgramPrimitiveError, K, V>(recipes.len(), work)
}

pub(crate) fn compile_comparisons_with_factory<H, F: ValueSchemaCreationFactory<H>>(
    checked: &CompiledParts,
    _temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    control: &dyn PureCompileControl,
) -> Result<
    CreatedPrimitiveMap<ComparisonSite, PreparedNodeComparison>,
    ProgramCreationFailure<ProgramPrimitiveError, H>,
> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let mut scope = match factory {
        Some(factory) => Some(
            factory
                .begin_scope()
                .map_err(ProgramCreationFailure::Creator)?,
        ),
        None => None,
    };
    let mut retained = ProgramCreation {
        creator: scope
            .as_mut()
            .map(|scope| scope as &mut dyn ValueSchemaCreationScope<H>),
    };
    let result = compile_core(checked, control, &mut retained, &mut work);
    if !matches!(result, Ok(_) | Err(ProgramCreationFailure::Owner(_))) {
        return result.map(|entries| CreatedPrimitiveMap {
            entries,
            _storage: retained.storage(),
        });
    }
    work.finish()?;
    Ok(CreatedPrimitiveMap {
        entries: result?,
        _storage: retained.storage(),
    })
}

fn compile_core<H>(
    checked: &CompiledParts,
    control: &dyn PureCompileControl,
    retained: &mut ProgramCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    BTreeMap<ComparisonSite, PreparedNodeComparison>,
    ProgramCreationFailure<ProgramPrimitiveError, H>,
> {
    let mut recipes = BTreeMap::new();
    {
        let flow = &checked.flow;
        let definitions = &checked.definitions;
        let types = &checked.types;
        let value =
            |id: crate::CompiledExprId| -> Result<&FunctionValueType, ProgramPrimitiveError> {
                match types.get(id.index()) {
                    Some(FunctionArgumentType::Value(ty)) => Ok(ty),
                    _ => Err(ProgramPrimitiveError::Invalid(
                        "primitive requires an exact value type",
                    )),
                }
            };
        for (&use_id, invocation) in flow.uses() {
            work.step()?;
            let occurrence = use_id;
            let kind = definitions
                .node(invocation.definition)
                .ok_or(ProgramPrimitiveError::Invalid(
                    "missing primitive definition",
                ))?
                .kind();
            match kind {
                StaticExprKind::Case {
                    ieee_float_equality,
                    has_case_expr: true,
                    has_else_expr,
                    children,
                } => {
                    let ControlShape::Case {
                        simple: true,
                        arms,
                        has_else,
                    } = invocation.control
                    else {
                        return Err(ProgramPrimitiveError::Invalid(
                            "simple CASE differs from actual control shape",
                        )
                        .into());
                    };
                    let count = (arms as usize)
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(1 + usize::from(has_else)))
                        .ok_or(ProgramPrimitiveError::Invalid("CASE arity overflow"))?;
                    if arms == 0
                        || has_else != *has_else_expr
                        || count != children.len()
                        || invocation.arguments.len() != count
                    {
                        return Err(ProgramPrimitiveError::Invalid(
                            "simple CASE differs from its ordered children",
                        )
                        .into());
                    }
                    for (ordinal, definition) in children.iter().enumerate() {
                        if flow.uses()[&invocation.arguments[ordinal]].definition != *definition {
                            return Err(ProgramPrimitiveError::Invalid(
                                "simple CASE child differs from actual use",
                            )
                            .into());
                        }
                        work.step()?;
                    }
                    for arm in 0..arms {
                        work.flush()?;
                        let left = value(children[0])?;
                        let right = value(children[1 + arm as usize * 2])?;
                        let recipe = if *ieee_float_equality {
                            PreparedNodeComparison::Ordinary(
                                PreparedComparisonRecipe::try_new_ieee_float_equality(
                                    left, right, control,
                                )?,
                            )
                        } else if let Some(recipe) =
                            PreparedCompositeComparisonRecipe::try_new_for_operator(
                                OperatorFunction::Equal,
                                [left, right],
                                control,
                            )?
                        {
                            PreparedNodeComparison::Composite(recipe)
                        } else {
                            PreparedNodeComparison::Ordinary(prepare_comparison_node(
                                ComparisonOperator::Eq,
                                left,
                                right,
                                control,
                            )?)
                        };
                        before_recipe_insert(&recipes, retained, work)?;
                        recipes.insert(ComparisonSite::CaseWhen { occurrence, arm }, recipe);
                        work.step()?;
                    }
                }
                _ => {}
            }
        }
    }
    Ok(recipes)
}

impl From<novarocks_functions::CastPrepareError> for ProgramPrimitiveError {
    fn from(value: novarocks_functions::CastPrepareError) -> Self {
        match value {
            novarocks_functions::CastPrepareError::Control(cause)
            | novarocks_functions::CastPrepareError::Kernel(
                novarocks_functions::KernelFailure::CompileControl(cause),
            ) => Self::Control(cause),
            original => Self::Cast(original),
        }
    }
}

pub(crate) fn compile_casts_with_factory<H, F: ValueSchemaCreationFactory<H>>(
    checked: &CompiledParts,
    _temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    control: &dyn PureCompileControl,
) -> Result<
    CreatedPrimitiveMap<ExpressionUseId, novarocks_functions::PreparedCastRecipe>,
    ProgramCreationFailure<ProgramPrimitiveError, H>,
> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let mut scope = match factory {
        Some(factory) => Some(
            factory
                .begin_scope()
                .map_err(ProgramCreationFailure::Creator)?,
        ),
        None => None,
    };
    let mut retained = ProgramCreation {
        creator: scope
            .as_mut()
            .map(|scope| scope as &mut dyn ValueSchemaCreationScope<H>),
    };
    let result = (|| {
        let mut recipes = BTreeMap::new();
        {
            let flow = &checked.flow;
            let definitions = &checked.definitions;
            let types = &checked.types;
            let value = |id: crate::CompiledExprId| match types.get(id.index()) {
                Some(FunctionArgumentType::Value(value)) => Ok(value),
                _ => Err(ProgramPrimitiveError::Invalid(
                    "cast requires complete value types",
                )),
            };
            for (&use_id, invocation) in flow.uses() {
                work.step()?;
                let kind = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramPrimitiveError::Invalid("missing cast definition"))?
                    .kind();
                let StaticExprKind::PreparedCast {
                    operation,
                    child,
                    cast_mode,
                } = kind
                else {
                    continue;
                };
                if invocation.control != ControlShape::Eager
                    || invocation.arguments.len() != 1
                    || flow.uses()[&invocation.arguments[0]].definition != *child
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "cast differs from its actual operand occurrence",
                    )
                    .into());
                }
                work.flush()?;
                let recipe = prepare_cast_node(
                    *operation,
                    value(*child)?,
                    value(invocation.definition)?,
                    *cast_mode,
                    control,
                )?;
                before_recipe_insert(&recipes, &mut retained, &mut work)?;
                recipes.insert(use_id, recipe);
                work.step()?;
            }
        }
        Ok(recipes)
    })();
    if !matches!(result, Ok(_) | Err(ProgramCreationFailure::Owner(_))) {
        return result.map(|entries| CreatedPrimitiveMap {
            entries,
            _storage: retained.storage(),
        });
    }
    work.finish()?;
    Ok(CreatedPrimitiveMap {
        entries: result?,
        _storage: retained.storage(),
    })
}

/// Prepare one ordinary comparison with the same exact domains as a program use.
pub fn prepare_comparison_node(
    operator: ComparisonOperator,
    left: &FunctionValueType,
    right: &FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<PreparedComparisonRecipe, ProgramPrimitiveError> {
    Ok(PreparedComparisonRecipe::try_new(
        operator, left, right, control,
    )?)
}
pub fn prepare_cast_node(
    _operation: novarocks_functions::CastOperation,
    source: &FunctionValueType,
    result: &FunctionValueType,
    cast_mode: novarocks_type_contract::TrinoCastMode,
    control: &dyn PureCompileControl,
) -> Result<novarocks_functions::PreparedCastRecipe, ProgramPrimitiveError> {
    Ok(novarocks_functions::PreparedCastRecipe::try_new_trino(
        source, result, cast_mode, control,
    )?)
}
