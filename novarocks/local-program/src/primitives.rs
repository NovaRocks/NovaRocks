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
use crate::{ProgramLexicalBindings, ProgramUseRef, StaticExprKind};
use novarocks_functions::{
    ComparisonOperator, ComparisonPrepareError, FunctionArgumentType, FunctionValueType,
    PreparedComparisonRecipe,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, PureCompileControl,
    ValueLogicalType, ValueSchemaCreationFactory, ValueSchemaCreationScope,
    ValueTypeStorageLifetime,
};
use std::{collections::BTreeMap, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ProgramComparisonSite {
    Binary(ProgramUseRef),
    BetweenLower(ProgramUseRef),
    BetweenUpper(ProgramUseRef),
    CaseWhen { occurrence: ProgramUseRef, arm: u32 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramPrimitiveError {
    Control(CompileControlError),
    Comparison(ComparisonPrepareError),
    Arithmetic(novarocks_functions::ArithmeticPrepareError),
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
            Self::Arithmetic(e) => e.fmt(f),
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
            Self::Arithmetic(e) => Some(e),
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
impl<H> From<novarocks_functions::ArithmeticPrepareError>
    for ProgramCreationFailure<ProgramPrimitiveError, H>
{
    fn from(error: novarocks_functions::ArithmeticPrepareError) -> Self {
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
pub(crate) fn pure_primitive_failure(
    error: ProgramCreationFailure<ProgramPrimitiveError, std::convert::Infallible>,
) -> ProgramPrimitiveError {
    match error {
        ProgramCreationFailure::Control(error) => ProgramPrimitiveError::Control(error),
        ProgramCreationFailure::Owner(error) => error,
        ProgramCreationFailure::Resource(_) | ProgramCreationFailure::Capacity(_) => {
            ProgramPrimitiveError::Control(CompileControlError::ResourceExhausted)
        }
        ProgramCreationFailure::Creator(never) => match never {},
    }
}
fn arithmetic_creation_failure<H>(
    error: novarocks_functions::ArithmeticCreationFailure<H>,
) -> ProgramCreationFailure<ProgramPrimitiveError, H> {
    use novarocks_functions::{
        ArithmeticCreationFailure as Failure, ArithmeticTypeCreationFailure as TypeFailure,
    };
    use novarocks_type_contract::ValueConstructionFailure as Construction;
    match error {
        Failure::Prepare(error) => ProgramPrimitiveError::from(error).into(),
        Failure::Construction(Construction::Observer(TypeFailure::Control(error))) => ProgramCreationFailure::Control(error),
        Failure::Construction(Construction::Observer(TypeFailure::Creator(error))) => ProgramCreationFailure::Creator(error),
        Failure::Construction(Construction::Layout(error)) => ProgramCreationFailure::Resource(error),
        Failure::Construction(Construction::Vec(error)) => match error {
            novarocks_type_contract::ControlResourceError::Control(error) => ProgramCreationFailure::Control(error),
            // Canonical Float64 has no collection construction. A future
            // unknown collection route must fail its original source model.
            novarocks_type_contract::ControlResourceError::SourceModel(_) =>
                ProgramCreationFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel),
        },
        Failure::Construction(Construction::HashMap(error)) => match error {
            novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::Arithmetic(_) =>
                ProgramCreationFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic),
            novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::SourceModel(_) =>
                ProgramCreationFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel),
        },
        Failure::Construction(Construction::BTree(error)) => match error {
            novarocks_type_contract::owned_resources::btree::BTreeResourceError::Arithmetic(_) =>
                ProgramCreationFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic),
            novarocks_type_contract::owned_resources::btree::BTreeResourceError::SourceModel(_) =>
                ProgramCreationFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel),
        },
        Failure::Construction(Construction::ValueType(_) | Construction::Rule(_)) =>
            ProgramPrimitiveError::Arithmetic(novarocks_functions::ArithmeticPrepareError::TypeMismatch).into(),
    }
}
fn native_inlist_creation_failure<H>(
    error: novarocks_functions::NativeInListCreationFailure<H>,
) -> ProgramCreationFailure<ProgramPrimitiveError, H> {
    match error {
        novarocks_functions::NativeInListCreationFailure::Control(error) => {
            ProgramCreationFailure::Control(error)
        }
        novarocks_functions::NativeInListCreationFailure::Creator(error) => {
            ProgramCreationFailure::Creator(error)
        }
        novarocks_functions::NativeInListCreationFailure::Resource(error) => {
            ProgramCreationFailure::Resource(error)
        }
        novarocks_functions::NativeInListCreationFailure::Owner(error) => {
            ProgramPrimitiveError::from(error).into()
        }
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
    checked: &ProgramLexicalBindings,
    _temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    control: &dyn PureCompileControl,
) -> Result<
    CreatedPrimitiveMap<ProgramComparisonSite, PreparedComparisonRecipe>,
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
    checked: &ProgramLexicalBindings,
    control: &dyn PureCompileControl,
    retained: &mut ProgramCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    BTreeMap<ProgramComparisonSite, PreparedComparisonRecipe>,
    ProgramCreationFailure<ProgramPrimitiveError, H>,
> {
    let typed = checked.channels().expressions();
    let snapshot = typed.resolved_calls().snapshot();
    let mut recipes = BTreeMap::new();
    for (&arena, flow) in snapshot.flows() {
        let definitions = &snapshot.roots().arenas()[&arena];
        let types = &typed.types()[&arena];
        let value =
            |id: crate::ProgramExprId| -> Result<&FunctionValueType, ProgramPrimitiveError> {
                match types.get(id.index()) {
                    Some(FunctionArgumentType::Value(ty)) => Ok(ty),
                    _ => Err(ProgramPrimitiveError::Invalid(
                        "primitive requires an exact value type",
                    )),
                }
            };
        for (&use_id, invocation) in flow.uses() {
            work.step()?;
            let occurrence = ProgramUseRef { arena, use_id };
            let kind = definitions
                .node(invocation.definition)
                .ok_or(ProgramPrimitiveError::Invalid(
                    "missing primitive definition",
                ))?
                .kind();
            match kind {
                StaticExprKind::PreparedBetween {
                    plan,
                    operand,
                    low,
                    high,
                } => {
                    if invocation.control
                        != (ControlShape::Between {
                            negated: plan.negated(),
                        })
                        || invocation.arguments.len() != 4
                    {
                        return Err(ProgramPrimitiveError::Invalid(
                            "BETWEEN differs from its four ordered uses",
                        )
                        .into());
                    }
                    for (ordinal, role) in plan.sources().into_iter().enumerate() {
                        let expected = match role {
                            novarocks_type_contract::BetweenSourceRole::Operand => *operand,
                            novarocks_type_contract::BetweenSourceRole::Lower => *low,
                            novarocks_type_contract::BetweenSourceRole::Upper => *high,
                        };
                        if flow.uses()[&invocation.arguments[ordinal]].definition != expected {
                            return Err(ProgramPrimitiveError::Invalid(
                                "BETWEEN source occurrence differs",
                            )
                            .into());
                        }
                        work.step()?;
                    }
                    work.flush()?;
                    let recipe = novarocks_functions::PreparedNativeBetweenRecipe::try_new(
                        *plan,
                        value(*operand)?,
                        value(*low)?,
                        value(*high)?,
                        value(invocation.definition)?,
                        control,
                    )?;
                    work.flush()?;
                    before_recipe_insert(&recipes, retained, work)?;
                    recipes.insert(
                        ProgramComparisonSite::BetweenLower(occurrence),
                        recipe.lower().clone(),
                    );
                    before_recipe_insert(&recipes, retained, work)?;
                    recipes.insert(
                        ProgramComparisonSite::BetweenUpper(occurrence),
                        recipe.upper().clone(),
                    );
                    work.step()?;
                }
                kind if kind.ordinary_comparison().is_some() => {
                    let (operator, left, right) =
                        kind.ordinary_comparison().expect("checked comparison kind");
                    if invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 2
                        || flow.uses()[&invocation.arguments[0]].definition != left
                        || flow.uses()[&invocation.arguments[1]].definition != right
                    {
                        return Err(ProgramPrimitiveError::Invalid(
                            "comparison differs from its actual ordered occurrence",
                        )
                        .into());
                    }
                    work.flush()?;
                    let recipe =
                        prepare_comparison_node(operator, value(left)?, value(right)?, control)?;
                    let result = value(invocation.definition)?;
                    if *result.data_type() != arrow_schema::DataType::Boolean
                        || result.logical_type() != ValueLogicalType::Physical
                        || (recipe.nullable_result() && !result.nullable())
                    {
                        return Err(ProgramPrimitiveError::Invalid(
                            "comparison result loses successful SQL NULL or its Boolean domain",
                        )
                        .into());
                    }
                    before_recipe_insert(&recipes, retained, work)?;
                    recipes.insert(ProgramComparisonSite::Binary(occurrence), recipe);
                    work.step()?;
                }
                StaticExprKind::Case {
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
                        let recipe = prepare_comparison_node(
                            ComparisonOperator::Eq,
                            value(children[0])?,
                            value(children[1 + arm as usize * 2])?,
                            control,
                        )?;
                        before_recipe_insert(&recipes, retained, work)?;
                        recipes.insert(ProgramComparisonSite::CaseWhen { occurrence, arm }, recipe);
                        work.step()?;
                    }
                }
                _ => {}
            }
        }
    }
    Ok(recipes)
}

impl From<novarocks_functions::ArithmeticPrepareError> for ProgramPrimitiveError {
    fn from(value: novarocks_functions::ArithmeticPrepareError) -> Self {
        match value {
            novarocks_functions::ArithmeticPrepareError::Control(cause)
            | novarocks_functions::ArithmeticPrepareError::Kernel(
                novarocks_functions::KernelFailure::CompileControl(cause),
            ) => Self::Control(cause),
            original => Self::Arithmetic(original),
        }
    }
}

pub(crate) fn compile_arithmetic_with_factory<H, F: ValueSchemaCreationFactory<H>>(
    checked: &ProgramLexicalBindings,
    _temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    control: &dyn PureCompileControl,
) -> Result<
    CreatedPrimitiveMap<ProgramUseRef, novarocks_functions::PreparedArithmeticRecipe>,
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
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let mut recipes = BTreeMap::new();
        for (&arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[&arena];
            let types = &typed.types()[&arena];
            let value = |id: crate::ProgramExprId| match types.get(id.index()) {
                Some(FunctionArgumentType::Value(value)) => Ok(value),
                _ => Err(ProgramPrimitiveError::Invalid(
                    "arithmetic requires complete value types",
                )),
            };
            for (&use_id, invocation) in flow.uses() {
                work.step()?;
                let kind = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramPrimitiveError::Invalid(
                        "missing arithmetic definition",
                    ))?
                    .kind();
                let StaticExprKind::PreparedArithmetic {
                    operator,
                    left,
                    right,
                    decimal_overflow_policy,
                    allow_throw_exception,
                } = kind
                else {
                    continue;
                };
                if invocation.control != ControlShape::Eager
                    || invocation.arguments.len() != 2
                    || flow.uses()[&invocation.arguments[0]].definition != *left
                    || flow.uses()[&invocation.arguments[1]].definition != *right
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "arithmetic differs from its actual ordered occurrence",
                    )
                    .into());
                }
                work.flush()?;
                let recipe = match factory {
                    Some(factory) => {
                        novarocks_functions::PreparedArithmeticRecipe::try_new_with_creator_in(
                            *operator,
                            value(*left)?,
                            value(*right)?,
                            value(invocation.definition)?,
                            *decimal_overflow_policy,
                            *allow_throw_exception,
                            factory,
                            control,
                        )
                        .map_err(arithmetic_creation_failure)?
                    }
                    None => prepare_arithmetic_node(
                        *operator,
                        value(*left)?,
                        value(*right)?,
                        value(invocation.definition)?,
                        *decimal_overflow_policy,
                        *allow_throw_exception,
                        control,
                    )?,
                };
                before_recipe_insert(&recipes, &mut retained, &mut work)?;
                recipes.insert(ProgramUseRef { arena, use_id }, recipe);
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
    checked: &ProgramLexicalBindings,
    _temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    control: &dyn PureCompileControl,
) -> Result<
    CreatedPrimitiveMap<ProgramUseRef, novarocks_functions::PreparedCastRecipe>,
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
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let mut recipes = BTreeMap::new();
        for (&arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[&arena];
            let types = &typed.types()[&arena];
            let value = |id: crate::ProgramExprId| match types.get(id.index()) {
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
                    decimal_overflow_policy,
                    allow_throw_exception,
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
                    *decimal_overflow_policy,
                    *allow_throw_exception,
                    control,
                )?;
                before_recipe_insert(&recipes, &mut retained, &mut work)?;
                recipes.insert(ProgramUseRef { arena, use_id }, recipe);
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

pub(crate) fn compile_null_safe_comparisons_with_factory<H, F: ValueSchemaCreationFactory<H>>(
    checked: &ProgramLexicalBindings,
    _temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    control: &dyn PureCompileControl,
) -> Result<
    CreatedPrimitiveMap<ProgramUseRef, novarocks_functions::PreparedNullSafeComparisonRecipe>,
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
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let mut recipes = BTreeMap::new();
        for (&arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[&arena];
            let types = &typed.types()[&arena];
            let value = |id: crate::ProgramExprId| match types.get(id.index()) {
                Some(FunctionArgumentType::Value(value)) => Ok(value),
                _ => Err(ProgramPrimitiveError::Invalid(
                    "null-safe comparison requires complete value types",
                )),
            };
            for (&use_id, invocation) in flow.uses() {
                work.step()?;
                let kind = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramPrimitiveError::Invalid(
                        "missing null-safe comparison definition",
                    ))?
                    .kind();
                let StaticExprKind::PreparedNullSafeComparison { left, right } = kind else {
                    continue;
                };
                if invocation.control != ControlShape::Eager
                    || invocation.arguments.len() != 2
                    || flow.uses()[&invocation.arguments[0]].definition != *left
                    || flow.uses()[&invocation.arguments[1]].definition != *right
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "null-safe comparison differs from its actual ordered occurrence",
                    )
                    .into());
                }
                let result = value(invocation.definition)?;
                if *result.data_type() != arrow_schema::DataType::Boolean
                    || result.logical_type() != ValueLogicalType::Physical
                    || result.nullable()
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "null-safe comparison requires nonnullable Physical Boolean result",
                    )
                    .into());
                }
                work.flush()?;
                let recipe =
                    prepare_null_safe_comparison_node(value(*left)?, value(*right)?, control)?;
                before_recipe_insert(&recipes, &mut retained, &mut work)?;
                recipes.insert(ProgramUseRef { arena, use_id }, recipe);
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

pub(crate) fn compile_native_negate_with_factory<H, F: ValueSchemaCreationFactory<H>>(
    checked: &ProgramLexicalBindings,
    _temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    control: &dyn PureCompileControl,
) -> Result<
    CreatedPrimitiveMap<ProgramUseRef, novarocks_functions::PreparedNativeNegateRecipe>,
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
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let mut recipes = BTreeMap::new();
        for (&arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[&arena];
            let types = &typed.types()[&arena];
            let value = |id: crate::ProgramExprId| match types.get(id.index()) {
                Some(FunctionArgumentType::Value(value)) => Ok(value),
                _ => Err(ProgramPrimitiveError::Invalid(
                    "arithmetic requires complete value types",
                )),
            };
            for (&use_id, invocation) in flow.uses() {
                work.step()?;
                let kind = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramPrimitiveError::Invalid(
                        "missing arithmetic definition",
                    ))?
                    .kind();
                let StaticExprKind::PreparedNativeNegate(child) = kind else {
                    continue;
                };
                if invocation.control != ControlShape::Eager
                    || invocation.arguments.len() != 1
                    || flow.uses()[&invocation.arguments[0]].definition != *child
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "native negate differs from its actual ordered occurrence",
                    )
                    .into());
                }
                work.flush()?;
                let recipe = prepare_native_negate_node(
                    value(*child)?,
                    value(invocation.definition)?,
                    control,
                )?;
                before_recipe_insert(&recipes, &mut retained, &mut work)?;
                recipes.insert(ProgramUseRef { arena, use_id }, recipe);
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

pub(crate) fn compile_native_bitnot_with_factory<H, F: ValueSchemaCreationFactory<H>>(
    checked: &ProgramLexicalBindings,
    _temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    control: &dyn PureCompileControl,
) -> Result<
    CreatedPrimitiveMap<ProgramUseRef, novarocks_functions::PreparedNativeBitNotRecipe>,
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
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let mut recipes = BTreeMap::new();
        for (&arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[&arena];
            let types = &typed.types()[&arena];
            let value = |id: crate::ProgramExprId| match types.get(id.index()) {
                Some(FunctionArgumentType::Value(value)) => Ok(value),
                _ => Err(ProgramPrimitiveError::Invalid(
                    "arithmetic requires complete value types",
                )),
            };
            for (&use_id, invocation) in flow.uses() {
                work.step()?;
                let kind = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramPrimitiveError::Invalid(
                        "missing arithmetic definition",
                    ))?
                    .kind();
                let StaticExprKind::PreparedNativeBitNot(child) = kind else {
                    continue;
                };
                if invocation.control != ControlShape::Eager
                    || invocation.arguments.len() != 1
                    || flow.uses()[&invocation.arguments[0]].definition != *child
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "native BitwiseNot differs from its actual ordered occurrence",
                    )
                    .into());
                }
                work.flush()?;
                let recipe = prepare_native_bitnot_node(
                    value(*child)?,
                    value(invocation.definition)?,
                    control,
                )?;
                before_recipe_insert(&recipes, &mut retained, &mut work)?;
                recipes.insert(ProgramUseRef { arena, use_id }, recipe);
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

pub(crate) fn compile_native_inlist_with_factory<H, F: ValueSchemaCreationFactory<H>>(
    checked: &ProgramLexicalBindings,
    temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    control: &dyn PureCompileControl,
) -> Result<
    CreatedPrimitiveMap<ProgramUseRef, novarocks_functions::PreparedNativeInListRecipe>,
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
    let mut temporary = ProgramCreation { creator: temporary };
    let result = (|| {
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let mut recipes = BTreeMap::new();
        for (&arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[&arena];
            let types = &typed.types()[&arena];
            let value = |id: crate::ProgramExprId| match types.get(id.index()) {
                Some(FunctionArgumentType::Value(value)) => Ok(value),
                _ => Err(ProgramPrimitiveError::Invalid(
                    "IN requires complete value types",
                )),
            };
            for (&use_id, invocation) in flow.uses() {
                work.step()?;
                let kind = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramPrimitiveError::Invalid("missing IN definition"))?
                    .kind();
                let StaticExprKind::PreparedInList {
                    child,
                    values,
                    is_not_in,
                } = kind
                else {
                    continue;
                };
                if invocation.control
                    != (ControlShape::Membership {
                        negated: *is_not_in,
                    })
                    || invocation.arguments.len() != values.len() + 1
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "IN differs from its ordered occurrence",
                    )
                    .into());
                }
                let mut candidates = temporary
                    .reserve::<ProgramPrimitiveError, &FunctionValueType>(
                        values.len(),
                        &mut work,
                    )?;
                for (ordinal, expected) in std::iter::once(child).chain(values.iter()).enumerate() {
                    if flow.uses()[&invocation.arguments[ordinal]].definition != *expected {
                        return Err(
                            ProgramPrimitiveError::Invalid("IN source occurrence differs").into(),
                        );
                    }
                    if ordinal > 0 {
                        candidates.push(value(*expected)?);
                    }
                    work.step()?;
                }
                work.flush()?;
                let source = value(*child)?;
                let result_type = value(invocation.definition)?;
                let recipe = match factory {
                    Some(factory) => {
                        // The parameter collection may be cloned independently
                        // of this map. Only its own scope follows that loan.
                        let mut recipe_scope = factory
                            .begin_scope()
                            .map_err(ProgramCreationFailure::Creator)?;
                        novarocks_functions::PreparedNativeInListRecipe::try_new_with_creator_in(
                            *is_not_in,
                            source,
                            &candidates,
                            result_type,
                            &mut recipe_scope,
                            control,
                        )
                        .map_err(native_inlist_creation_failure)?
                    }
                    None => novarocks_functions::PreparedNativeInListRecipe::try_new(
                        *is_not_in,
                        source,
                        &candidates,
                        result_type,
                        control,
                    )?,
                };
                before_recipe_insert(&recipes, &mut retained, &mut work)?;
                recipes.insert(ProgramUseRef { arena, use_id }, recipe);
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

pub(crate) fn compile_native_like_with_factory<H, F: ValueSchemaCreationFactory<H>>(
    checked: &ProgramLexicalBindings,
    _temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    control: &dyn PureCompileControl,
) -> Result<
    CreatedPrimitiveMap<ProgramUseRef, novarocks_functions::PreparedNativeLikeRecipe>,
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
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let mut recipes = BTreeMap::new();
        for (&arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[&arena];
            let types = &typed.types()[&arena];
            let value = |id: crate::ProgramExprId| match types.get(id.index()) {
                Some(FunctionArgumentType::Value(v)) => Ok(v),
                _ => Err(ProgramPrimitiveError::Invalid(
                    "LIKE requires complete value types",
                )),
            };
            for (&use_id, invocation) in flow.uses() {
                work.step()?;
                let kind = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramPrimitiveError::Invalid("missing LIKE definition"))?
                    .kind();
                let StaticExprKind::PreparedLike {
                    text,
                    pattern,
                    negated,
                } = kind
                else {
                    continue;
                };
                if invocation.control != ControlShape::Eager || invocation.arguments.len() != 2 {
                    return Err(ProgramPrimitiveError::Invalid(
                        "LIKE differs from original Eager2",
                    )
                    .into());
                }
                for (ordinal, id) in [text, pattern].into_iter().enumerate() {
                    if flow.uses()[&invocation.arguments[ordinal]].definition != *id {
                        return Err(ProgramPrimitiveError::Invalid(
                            "LIKE original ordered use differs",
                        )
                        .into());
                    }
                    work.step()?;
                }
                work.flush()?;
                let recipe = novarocks_functions::PreparedNativeLikeRecipe::try_new(
                    *negated,
                    value(*text)?,
                    value(*pattern)?,
                    value(invocation.definition)?,
                    control,
                )?;
                before_recipe_insert(&recipes, &mut retained, &mut work)?;
                recipes.insert(ProgramUseRef { arena, use_id }, recipe);
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
pub fn prepare_null_safe_comparison_node(
    left: &FunctionValueType,
    right: &FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<novarocks_functions::PreparedNullSafeComparisonRecipe, ProgramPrimitiveError> {
    Ok(novarocks_functions::PreparedNullSafeComparisonRecipe::try_new(left, right, control)?)
}
pub fn prepare_cast_node(
    operation: novarocks_functions::CastOperation,
    source: &FunctionValueType,
    result: &FunctionValueType,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
    allow_throw_exception: bool,
    control: &dyn PureCompileControl,
) -> Result<novarocks_functions::PreparedCastRecipe, ProgramPrimitiveError> {
    Ok(novarocks_functions::PreparedCastRecipe::try_new(
        operation,
        source,
        result,
        policy,
        allow_throw_exception,
        control,
    )?)
}
pub fn prepare_arithmetic_node(
    operator: novarocks_type_contract::ArithmeticOperator,
    left: &FunctionValueType,
    right: &FunctionValueType,
    result: &FunctionValueType,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
    allow_throw_exception: bool,
    control: &dyn PureCompileControl,
) -> Result<novarocks_functions::PreparedArithmeticRecipe, ProgramPrimitiveError> {
    Ok(novarocks_functions::PreparedArithmeticRecipe::try_new(
        operator,
        left,
        right,
        result,
        policy,
        allow_throw_exception,
        control,
    )?)
}
pub fn prepare_native_negate_node(
    source: &FunctionValueType,
    result: &FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<novarocks_functions::PreparedNativeNegateRecipe, ProgramPrimitiveError> {
    Ok(novarocks_functions::PreparedNativeNegateRecipe::try_new(
        source, result, control,
    )?)
}
pub fn prepare_native_bitnot_node(
    source: &FunctionValueType,
    result: &FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<novarocks_functions::PreparedNativeBitNotRecipe, ProgramPrimitiveError> {
    Ok(novarocks_functions::PreparedNativeBitNotRecipe::try_new(
        source, result, control,
    )?)
}

#[cfg(test)]
mod primitive_source_projection_tests {
    use super::*;
    use novarocks_failure_contract::{FailureProjection, FailureReason, domain};
    use novarocks_functions::{
        ArithmeticPrepareError as A, CastPrepareError as C, ComparisonPrepareError as B,
        KernelFailure,
    };
    use std::{error::Error, num::NonZeroU16};

    #[test]
    fn primitive_preparation_keeps_bare_kernel_stops_capacity_and_domain_sources() {
        for original in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::Capacity(novarocks_functions::KernelCapacityRefusal::new(
                FailureReason::new(domain::MEMORY, NonZeroU16::new(8).unwrap()),
                16,
                8,
                16,
            )),
            KernelFailure::from(novarocks_type_contract::ValueTypeError::TooDeep),
            KernelFailure::from(
                novarocks_type_contract::ValueTypeError::InvalidLogicalCarrier(
                    ValueLogicalType::Json,
                ),
            ),
        ] {
            let expected = original.failure_fact();
            for error in [
                ProgramPrimitiveError::from(A::Kernel(original.clone())),
                ProgramPrimitiveError::from(B::Kernel(original.clone())),
                ProgramPrimitiveError::from(C::Kernel(original.clone())),
            ] {
                assert!(!matches!(error, ProgramPrimitiveError::Control(_)));
                assert_eq!(error.failure_fact(), expected);
                let kernel = error
                    .source()
                    .unwrap()
                    .source()
                    .unwrap()
                    .downcast_ref::<KernelFailure>()
                    .unwrap();
                // Kernel Source equality retains the same shared occurrence,
                // not just a replacement with equal diagnostic text.
                assert_eq!(kernel, &original);
            }
        }
    }

    #[test]
    fn primitive_preparation_unwraps_only_actual_caller_control_channels() {
        for original in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
            CompileControlError::from_domain(novarocks_type_contract::ValueTypeError::TooDeep),
        ] {
            let expected = original.failure_fact();
            for error in [
                ProgramPrimitiveError::from(A::Control(original.clone())),
                ProgramPrimitiveError::from(B::Control(original.clone())),
                ProgramPrimitiveError::from(C::Control(original.clone())),
                ProgramPrimitiveError::from(A::Kernel(KernelFailure::CompileControl(
                    original.clone(),
                ))),
                ProgramPrimitiveError::from(B::Kernel(KernelFailure::CompileControl(
                    original.clone(),
                ))),
                ProgramPrimitiveError::from(C::Kernel(KernelFailure::CompileControl(
                    original.clone(),
                ))),
            ] {
                assert_eq!(error.failure_fact(), expected);
                assert_eq!(
                    error
                        .source()
                        .unwrap()
                        .downcast_ref::<CompileControlError>(),
                    Some(&original)
                );
                assert!(
                    matches!(error, ProgramPrimitiveError::Control(actual) if actual == original)
                );
            }
        }
    }
}
