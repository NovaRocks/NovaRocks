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

//! Physical definition and occurrence lowering for the initial executable
//! expression slice. Unsupported shapes remain explicit compiler errors.
//! This is not the complete expression compiler or a host allocation grant.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    error::Error,
    fmt,
    sync::Arc,
};

use novarocks_functions::{
    CallEffectInput, ConstantError, ConstantPolicy, ConstantValue, FunctionBindingError,
    FunctionBindingSelection, FunctionResultType, FunctionSpecializationFailure,
    PureCallPreparation, PureCallSpecialization, PureEngineFunctionCatalog,
    ScopedExpressionEffects,
};
use novarocks_local_program::{
    ExpressionsCompileError, ImmutableExpressions, ProgramCallSite, ProgramExprId,
    ProgramExpressionArena, ProgramUseRef, StaticExprKind, StaticExprNode,
};
#[cfg(test)]
use novarocks_physical_plan::LiteralValue;
use novarocks_physical_plan::{
    ConstantReferenceError, ExprId, ExprKind, ExprNode, FragmentPackage, PhysicalCallDefinition,
    PhysicalCallSite,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompileControlError, CompilePhase, ControlShape,
    EffectContractError, ExpressionUseId, FunctionArgumentType, FunctionKind,
    MAX_CONTROL_DEFINITIONS, MAX_CONTROL_DEPTH, PureCompileControl, ValueTypeError,
};

/// Dense definitions retain the complete source types independently of the
/// old carrier-only StaticExprNode projection. No caller control is retained.
pub(crate) struct LoweredExpressions {
    pub arena: Arc<ImmutableExpressions>,
    pub ids: BTreeMap<ExprId, ProgramExprId>,
    pub types: novarocks_type_contract::FunctionArgumentTypes,
    pub union_ids: BTreeMap<novarocks_physical_plan::NodeId, Vec<Vec<ProgramExprId>>>,
    /// Each scan's runtime-filter consumer key reads, in its binding order.
    pub runtime_filter_key_ids: BTreeMap<novarocks_physical_plan::NodeId, Vec<ProgramExprId>>,
}

#[derive(Debug)]
pub(crate) enum ExpressionLoweringError {
    Construction(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    BTree(novarocks_type_contract::owned_resources::btree::BTreeResourceError),
    Publication(
        novarocks_local_program::ProgramCreationFailure<
            std::convert::Infallible,
            std::convert::Infallible,
        >,
    ),
    Control(CompileControlError),
    Constant(ConstantError),
    Reference(ConstantReferenceError),
    Binding(FunctionBindingError),
    Type(ValueTypeError),
    Expressions(ExpressionsCompileError),
    Specialization(FunctionSpecializationFailure),
    Effects(EffectContractError),
    Arithmetic(novarocks_functions::ArithmeticPrepareError),
    Cast(novarocks_functions::CastPrepareError),
    Comparison(novarocks_functions::ComparisonPrepareError),
    UnsupportedExpression(ExprId),
    UnsupportedCall(PhysicalCallSite),
    Invalid(&'static str),
}
impl ExpressionLoweringError {
    /// This new model source uses the existing Owner channel, but retains the
    /// primary construction priority of the previous Resource envelope even
    /// when SourceModel's neutral class is Invariant rather than Resource.
    pub(crate) fn is_primary_construction(&self) -> bool {
        matches!(self, Self::BTree(_))
    }

    pub(crate) fn is_unsupported(&self) -> bool {
        matches!(
            self,
            Self::UnsupportedExpression(_)
                | Self::UnsupportedCall(_)
                | Self::Cast(novarocks_functions::CastPrepareError::Unsupported)
                | Self::Arithmetic(novarocks_functions::ArithmeticPrepareError::Unsupported)
                | Self::Comparison(novarocks_functions::ComparisonPrepareError::Unsupported)
                | Self::Binding(FunctionBindingError::UnavailableImplementation(_))
                | Self::Specialization(FunctionSpecializationFailure::MissingPureImplementation(_))
                | Self::Specialization(FunctionSpecializationFailure::Binding(
                    FunctionBindingError::UnavailableImplementation(_)
                ))
        )
    }
}
impl fmt::Display for ExpressionLoweringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Construction(source) => source.fmt(f),
            Self::BTree(source) => source.fmt(f),
            Self::Publication(source) => source.fmt(f),
            Self::Control(e) => e.fmt(f),
            Self::Constant(e) => e.fmt(f),
            Self::Reference(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Expressions(e) => e.fmt(f),
            Self::Specialization(e) => e.fmt(f),
            Self::Effects(e) => e.fmt(f),
            Self::Arithmetic(e) => e.fmt(f),
            Self::Cast(e) => e.fmt(f),
            Self::Comparison(e) => e.fmt(f),
            Self::UnsupportedExpression(id) => {
                write!(f, "unsupported physical expression {}", id.get())
            }
            Self::UnsupportedCall(site) => write!(f, "unsupported physical call {site:?}"),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}
impl Error for ExpressionLoweringError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Construction(source) => Some(source),
            Self::BTree(source) => Some(source),
            Self::Publication(source) => Some(source),
            Self::Control(e) => Some(e),
            Self::Constant(e) => Some(e),
            Self::Reference(e) => Some(e),
            Self::Binding(e) => Some(e),
            Self::Type(e) => Some(e),
            Self::Expressions(e) => Some(e),
            Self::Specialization(e) => Some(e),
            Self::Effects(e) => Some(e),
            Self::Arithmetic(e) => Some(e),
            Self::Cast(e) => Some(e),
            Self::Comparison(e) => Some(e),
            _ => None,
        }
    }
}
impl From<novarocks_type_contract::owned_resources::btree::BTreeResourceError>
    for ExpressionLoweringError
{
    fn from(source: novarocks_type_contract::owned_resources::btree::BTreeResourceError) -> Self {
        Self::BTree(source)
    }
}
impl From<novarocks_functions::ArithmeticPrepareError> for ExpressionLoweringError {
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
impl From<novarocks_functions::CastPrepareError> for ExpressionLoweringError {
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
impl From<novarocks_functions::ComparisonPrepareError> for ExpressionLoweringError {
    fn from(value: novarocks_functions::ComparisonPrepareError) -> Self {
        match value {
            novarocks_functions::ComparisonPrepareError::Control(cause)
            | novarocks_functions::ComparisonPrepareError::Kernel(
                novarocks_functions::KernelFailure::CompileControl(cause),
            ) => Self::Control(cause),
            original => Self::Comparison(original),
        }
    }
}
impl From<CompileControlError> for ExpressionLoweringError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<ConstantError> for ExpressionLoweringError {
    fn from(e: ConstantError) -> Self {
        match e {
            ConstantError::Control(e) => Self::Control(e),
            other => Self::Constant(other),
        }
    }
}
impl From<ConstantReferenceError> for ExpressionLoweringError {
    fn from(error: ConstantReferenceError) -> Self {
        match error {
            ConstantReferenceError::Control(cause)
            | ConstantReferenceError::Constant(ConstantError::Control(cause)) => {
                Self::Control(cause)
            }
            other => Self::Reference(other),
        }
    }
}
impl From<FunctionBindingError> for ExpressionLoweringError {
    fn from(e: FunctionBindingError) -> Self {
        match e {
            FunctionBindingError::Control(e) => Self::Control(e),
            other => Self::Binding(other),
        }
    }
}
impl From<ValueTypeError> for ExpressionLoweringError {
    fn from(e: ValueTypeError) -> Self {
        Self::Type(e)
    }
}
impl From<ExpressionsCompileError> for ExpressionLoweringError {
    fn from(e: ExpressionsCompileError) -> Self {
        match e {
            ExpressionsCompileError::Control(e) => Self::Control(e),
            other => Self::Expressions(other),
        }
    }
}
impl From<EffectContractError> for ExpressionLoweringError {
    fn from(e: EffectContractError) -> Self {
        Self::Effects(e)
    }
}
impl From<FunctionSpecializationFailure> for ExpressionLoweringError {
    fn from(e: FunctionSpecializationFailure) -> Self {
        match e {
            FunctionSpecializationFailure::Control(e)
            | FunctionSpecializationFailure::Binding(FunctionBindingError::Control(e))
            | FunctionSpecializationFailure::Kernel(
                novarocks_functions::KernelFailure::CompileControl(e),
            ) => Self::Control(e),
            other => Self::Specialization(other),
        }
    }
}

fn finish<T>(
    result: Result<T, ExpressionLoweringError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<T, ExpressionLoweringError> {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    // The refusal already happened inside its original owner. A later
    // footer observes pending work only after ordinary validation failures.
    if result.as_ref().is_err_and(|cause| {
        matches!(cause, ExpressionLoweringError::Control(_))
            || cause.is_primary_construction()
            || matches!(
                cause.failure_fact().class(),
                FailureClass::Resource | FailureClass::Control
            )
    }) {
        return result;
    }
    work.flush()?;
    result
}

/// Lower every actual definition, including unused definitions, without
/// creating invocation tokens for definitions with no evaluation use. IDs are
/// topologically assigned; sparse numeric order is never dependency order.
#[cfg(test)]
pub(crate) fn lower_expressions(
    package: &FragmentPackage,
    policy: ConstantPolicy,
    inputs: &BTreeMap<ExprId, crate::channels::ResolvedInput>,
    control: &dyn PureCompileControl,
) -> Result<LoweredExpressions, ExpressionLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core::<std::convert::Infallible, crate::creation::NoFactory>(
        package,
        policy,
        inputs,
        &BTreeMap::new(),
        &BTreeMap::new(),
        control,
        &mut work,
        None::<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<std::convert::Infallible>>,
        None,
    );
    finish(
        result.map_err(ExpressionCreationFailure::into_pure),
        &mut work,
    )
}

/// Lower every actual definition, then author one slot-read definition per
/// Union normalization or join selection source and per scan runtime-filter
/// consumer key, each typed exactly as the channel it reads.
pub(crate) fn lower_expressions_with_unions(
    package: &FragmentPackage,
    policy: ConstantPolicy,
    inputs: &BTreeMap<ExprId, crate::channels::ResolvedInput>,
    unions: &BTreeMap<novarocks_physical_plan::NodeId, Vec<crate::channels::UnionChannelBranch>>,
    runtime_filter_keys: &BTreeMap<
        novarocks_physical_plan::NodeId,
        Vec<crate::channels::UnionChannelSource>,
    >,
    control: &dyn PureCompileControl,
) -> Result<LoweredExpressions, ExpressionLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core::<std::convert::Infallible, crate::creation::NoFactory>(
        package,
        policy,
        inputs,
        unions,
        runtime_filter_keys,
        control,
        &mut work,
        None::<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<std::convert::Infallible>>,
        None,
    );
    finish(
        result.map_err(ExpressionCreationFailure::into_pure),
        &mut work,
    )
}

fn case_arity(
    operand: Option<ExprId>,
    when_then: &[(ExprId, ExprId)],
    else_expr: Option<ExprId>,
) -> Result<usize, ExpressionLoweringError> {
    when_then
        .len()
        .checked_mul(2)
        .and_then(|count| {
            count.checked_add(usize::from(operand.is_some()) + usize::from(else_expr.is_some()))
        })
        .filter(|count| {
            !when_then.is_empty() && *count <= novarocks_type_contract::MAX_CONTROL_USE_REFERENCES
        })
        .ok_or(ExpressionLoweringError::Invalid(
            "CASE arity is invalid or exceeds the expression bound",
        ))
}

// Constant-time access preserves the physical tuple order without cloning its
// edge list on every dependency continuation (which would be quadratic).
fn case_child(
    operand: Option<ExprId>,
    when_then: &[(ExprId, ExprId)],
    else_expr: Option<ExprId>,
    ordinal: usize,
) -> Option<ExprId> {
    if operand.is_some() && ordinal == 0 {
        return operand;
    }
    let ordinal = ordinal.checked_sub(usize::from(operand.is_some()))?;
    match when_then.get(ordinal / 2) {
        Some(&(when, then)) => Some(if ordinal.is_multiple_of(2) {
            when
        } else {
            then
        }),
        None if when_then.len().checked_mul(2) == Some(ordinal) => else_expr,
        _ => None,
    }
}

#[derive(Debug)]
pub(crate) enum ExpressionCreationFailure<H> {
    Control(CompileControlError),
    Creator(H),
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    Owner(ExpressionLoweringError),
}
impl<H> From<CompileControlError> for ExpressionCreationFailure<H> {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl<H> From<ExpressionLoweringError> for ExpressionCreationFailure<H> {
    fn from(e: ExpressionLoweringError) -> Self {
        match e {
            ExpressionLoweringError::Control(c) => Self::Control(c),
            e => Self::Owner(e),
        }
    }
}
macro_rules! expression_creation_cause {
    ($t:ty) => {
        impl<H> From<$t> for ExpressionCreationFailure<H> {
            fn from(e: $t) -> Self {
                ExpressionLoweringError::from(e).into()
            }
        }
    };
}
expression_creation_cause!(ValueTypeError);
expression_creation_cause!(novarocks_type_contract::owned_resources::btree::BTreeResourceError);
expression_creation_cause!(ConstantError);
expression_creation_cause!(ConstantReferenceError);
expression_creation_cause!(ExpressionsCompileError);
expression_creation_cause!(novarocks_functions::ArithmeticPrepareError);
expression_creation_cause!(novarocks_functions::CastPrepareError);
expression_creation_cause!(novarocks_functions::ComparisonPrepareError);
impl<H> From<EffectContractError> for ExpressionCreationFailure<H> {
    fn from(error: EffectContractError) -> Self {
        ExpressionLoweringError::from(error).into()
    }
}
impl<H> From<FunctionBindingError> for ExpressionCreationFailure<H> {
    fn from(error: FunctionBindingError) -> Self {
        ExpressionLoweringError::from(error).into()
    }
}
impl<H> From<FunctionSpecializationFailure> for ExpressionCreationFailure<H> {
    fn from(error: FunctionSpecializationFailure) -> Self {
        ExpressionLoweringError::from(error).into()
    }
}
impl<H> From<novarocks_functions::FunctionBindingCreationFailure<H>>
    for ExpressionCreationFailure<H>
{
    fn from(error: novarocks_functions::FunctionBindingCreationFailure<H>) -> Self {
        use novarocks_functions::{
            FunctionBindingConstructionResource as R, FunctionBindingCreationFailure as F,
        };
        use novarocks_type_contract::{
            ControlResourceError,
            owned_resources::{hashmap::HashMapResourceError, layout::LayoutResourceError as L},
        };
        match error {
            F::Binding(error) => error.into(),
            F::Creator(cause) => Self::Creator(cause),
            F::Resource(R::Layout(cause)) => Self::Resource(cause),
            F::Resource(R::BTree(cause)) => cause.into(),
            F::Resource(R::Vec(ControlResourceError::Control(cause))) => Self::Control(cause),
            F::Resource(R::Vec(ControlResourceError::SourceModel(_)))
            | F::Resource(R::HashMap(HashMapResourceError::SourceModel(_))) => {
                Self::Resource(L::SourceModel)
            }
            F::Resource(R::HashMap(HashMapResourceError::Arithmetic(_))) => {
                Self::Resource(L::Arithmetic)
            }
            F::UnconvertedResolverOwner => Self::Owner(ExpressionLoweringError::Invalid(
                "binding construction owner is not converted",
            )),
        }
    }
}
impl<H> From<novarocks_functions::CheckedCallPreparationFailure<H>>
    for ExpressionCreationFailure<H>
{
    fn from(error: novarocks_functions::CheckedCallPreparationFailure<H>) -> Self {
        use novarocks_functions::{CheckedCallPreparationFailure as F, CheckedOwnerFailure as O};
        match error {
            F::Binding(cause) => cause.into(),
            F::Preparation(O::Original(cause)) => cause.into(),
            F::Preparation(O::UnconvertedOwner) => Self::Owner(ExpressionLoweringError::Invalid(
                "installed preparation owner is not converted",
            )),
        }
    }
}

pub(crate) type CallCreation<'a, F> = Option<(&'a F, &'a ConstantPolicy)>;
pub(crate) fn validate_call_created<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    catalog: &novarocks_functions::EngineFunctionCatalog,
    function: &novarocks_functions::FunctionId,
    kind: FunctionKind,
    selected: &FunctionBindingSelection,
    request: novarocks_functions::FunctionBindingRequest<'_>,
    control: &dyn PureCompileControl,
    creation: CallCreation<'_, F>,
) -> Result<(), ExpressionCreationFailure<H>> {
    match creation {
        Some((factory, _)) => catalog
            .validate_frozen_selection_with_factory_in(
                function, kind, selected, request, factory, control,
            )
            .map_err(Into::into),
        None => catalog
            .validate_frozen_selection(function, kind, selected, request, control)
            .map_err(Into::into),
    }
}
pub(crate) fn prepare_call_created<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    catalog: &PureEngineFunctionCatalog,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: &novarocks_type_contract::CallEffects,
    options: PureCallPreparation,
    control: &dyn PureCompileControl,
    creation: CallCreation<'_, F>,
) -> Result<PureCallSpecialization, ExpressionCreationFailure<H>> {
    match creation {
        Some((factory, policy)) => {
            // The definition was fully validated by this immutable compiler's
            // first pass. Expected types in this preparation get a separate
            // temporary owner; they never enlarge a published type invoice.
            let mut scope = factory
                .begin_scope()
                .map_err(ExpressionCreationFailure::Creator)?;
            catalog
                .prepare_frozen_with_creator_in(
                    input, selected, frozen, options, policy, &mut scope, control,
                )
                .map_err(Into::into)
        }
        None => catalog
            .prepare_frozen(input, selected, frozen, options, control)
            .map_err(Into::into),
    }
}
impl<H> From<novarocks_functions::ArithmeticCreationFailure<H>> for ExpressionCreationFailure<H> {
    fn from(error: novarocks_functions::ArithmeticCreationFailure<H>) -> Self {
        use novarocks_functions::{
            ArithmeticCreationFailure as F, ArithmeticTypeCreationFailure as T,
        };
        use novarocks_type_contract::{
            ControlResourceError, ValueConstructionFailure as C,
            owned_resources::{hashmap::HashMapResourceError, layout::LayoutResourceError as L},
        };
        match error {
            F::Prepare(cause) => cause.into(),
            F::Construction(C::Observer(T::Control(cause))) => Self::Control(cause),
            F::Construction(C::Observer(T::Creator(cause))) => Self::Creator(cause),
            F::Construction(C::Layout(cause)) => Self::Resource(cause),
            F::Construction(C::BTree(cause)) => cause.into(),
            F::Construction(C::Vec(ControlResourceError::Control(cause))) => Self::Control(cause),
            F::Construction(C::Vec(ControlResourceError::SourceModel(_)))
            | F::Construction(C::HashMap(HashMapResourceError::SourceModel(_))) => {
                Self::Resource(L::SourceModel)
            }
            F::Construction(C::HashMap(HashMapResourceError::Arithmetic(_))) => {
                Self::Resource(L::Arithmetic)
            }
            F::Construction(C::ValueType(_) | C::Rule(_)) => {
                novarocks_functions::ArithmeticPrepareError::TypeMismatch.into()
            }
        }
    }
}
fn prepare_arithmetic_created<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    operator: novarocks_type_contract::ArithmeticOperator,
    left: &novarocks_type_contract::FunctionValueType,
    right: &novarocks_type_contract::FunctionValueType,
    result: &novarocks_type_contract::FunctionValueType,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
    allow: bool,
    control: &dyn PureCompileControl,
    factory: Option<&F>,
) -> Result<novarocks_functions::PreparedArithmeticRecipe, ExpressionCreationFailure<H>> {
    match factory {
        Some(factory) => novarocks_functions::PreparedArithmeticRecipe::try_new_with_creator_in(
            operator, left, right, result, policy, allow, factory, control,
        )
        .map_err(Into::into),
        None => novarocks_functions::PreparedArithmeticRecipe::try_new(
            operator, left, right, result, policy, allow, control,
        )
        .map_err(Into::into),
    }
}
impl ExpressionCreationFailure<std::convert::Infallible> {
    fn into_pure(self) -> ExpressionLoweringError {
        match self {
            Self::Control(c) => ExpressionLoweringError::Control(c),
            Self::Creator(never) => match never {},
            Self::Resource(source) => ExpressionLoweringError::Construction(source),
            Self::Owner(e) => e,
        }
    }
}
fn observe_type_layout<H>(
    layout: std::alloc::Layout,
    creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ExpressionCreationFailure<H>> {
    if let Some(c) = creation {
        work.flush()?;
        c.request_layout(layout)
            .map_err(ExpressionCreationFailure::Creator)?;
    }
    Ok(())
}
fn observe_type_push<H>(
    types: &Vec<FunctionArgumentType>,
    creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ExpressionCreationFailure<H>> {
    if creation.is_none() {
        return Ok(());
    }
    let facts =
        novarocks_type_contract::owned_resources::vec::push_growth::<FunctionArgumentType>(types)
            .map_err(|e| {
            match e {
        novarocks_type_contract::ControlResourceError::Control(c) => {
            ExpressionCreationFailure::Control(c)
        }
        novarocks_type_contract::ControlResourceError::SourceModel(_) => {
            ExpressionCreationFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
            )
        }
    }
        })?;
    if let Some(layout) = facts.requested_backing {
        observe_type_layout(layout, creation, work)?;
    }
    Ok(())
}
pub(crate) fn lower_expressions_with_unions_with_creator<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    package: &FragmentPackage,
    policy: ConstantPolicy,
    inputs: &BTreeMap<ExprId, crate::channels::ResolvedInput>,
    unions: &BTreeMap<novarocks_physical_plan::NodeId, Vec<crate::channels::UnionChannelBranch>>,
    runtime_filter_keys: &BTreeMap<
        novarocks_physical_plan::NodeId,
        Vec<crate::channels::UnionChannelSource>,
    >,
    creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
    factory: &F,
    control: &dyn PureCompileControl,
) -> Result<LoweredExpressions, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(
        package,
        policy,
        inputs,
        unions,
        runtime_filter_keys,
        control,
        &mut work,
        Some(creation),
        Some(factory),
    )
    .map_err(|e| match e {
        ExpressionCreationFailure::Control(c) => crate::FragmentCreationError::Control(c),
        ExpressionCreationFailure::Creator(c) => crate::FragmentCreationError::Creator(c),
        ExpressionCreationFailure::Resource(c) => crate::FragmentCreationError::Resource(c),
        ExpressionCreationFailure::Owner(c) => crate::FragmentCreationError::from(c),
    });
    if matches!(
        &result,
        Err(crate::FragmentCreationError::Control(_)
            | crate::FragmentCreationError::Creator(_)
            | crate::FragmentCreationError::Resource(_))
    ) || matches!(&result, Err(crate::FragmentCreationError::Compile(source))
        if source.is_primary_construction())
    {
        return result;
    }
    work.flush()?;
    result
}
fn lower_core<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    policy: ConstantPolicy,
    inputs: &BTreeMap<ExprId, crate::channels::ResolvedInput>,
    unions: &BTreeMap<novarocks_physical_plan::NodeId, Vec<crate::channels::UnionChannelBranch>>,
    runtime_filter_keys: &BTreeMap<
        novarocks_physical_plan::NodeId,
        Vec<crate::channels::UnionChannelSource>,
    >,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
    mut creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
) -> Result<LoweredExpressions, ExpressionCreationFailure<H>> {
    let source = package.fragment().expressions();
    let mut count = source.len();
    for branches in unions.values() {
        for branch in branches {
            count = count
                .checked_add(branch.sources.len())
                .ok_or(CompileControlError::ResourceExhausted)?;
            work.step()?;
        }
    }
    for keys in runtime_filter_keys.values() {
        count = count
            .checked_add(keys.len())
            .ok_or(CompileControlError::ResourceExhausted)?;
        work.step()?;
    }
    if count > MAX_CONTROL_DEFINITIONS {
        return Err(
            ExpressionLoweringError::Invalid("expression definition limit exceeded").into(),
        );
    }
    let mut ids = BTreeMap::new();
    let mut nodes = Vec::new();
    let mut types = Vec::new();
    let mut active = BTreeSet::new();
    // At most one frame per dependency depth. Child lists are borrowed and
    // walked one edge at a time, rather than cloned or enqueued wholesale.
    let mut stack = Vec::new();
    for (&root, definition) in source.iter() {
        // A window call is a relational call of its Window node, never a
        // scalar definition: its arguments are lowered as their own roots.
        if ids.contains_key(&root) || matches!(definition.kind, ExprKind::WindowCall { .. }) {
            work.step()?;
            continue;
        }
        active.insert(root);
        stack.push((root, 0usize));
        work.step()?;
        while let Some(&(id, next)) = stack.last() {
            let node = source.get(id).ok_or(ExpressionLoweringError::Invalid(
                "missing physical expression",
            ))?;
            if node.id != id {
                return Err(ExpressionLoweringError::Invalid(
                    "physical expression key differs from ID",
                )
                .into());
            }
            let child = match &node.kind {
                ExprKind::Literal(_) | ExprKind::Constant(_) | ExprKind::Value(_) => None,
                ExprKind::FunctionCall { args, .. }
                | ExprKind::Conjunction { args }
                | ExprKind::Disjunction { args } => args.get(next).copied(),
                ExprKind::Unary {
                    op:
                        novarocks_physical_plan::UnaryOperator::Not
                        | novarocks_physical_plan::UnaryOperator::Minus
                        | novarocks_physical_plan::UnaryOperator::BitwiseNot,
                    expr,
                }
                | ExprKind::IsNull { expr, .. }
                | ExprKind::Cast { expr, .. } => (next == 0).then_some(*expr),
                ExprKind::Binary {
                    op:
                        novarocks_physical_plan::BinaryOperator::Eq
                        | novarocks_physical_plan::BinaryOperator::EqForNull
                        | novarocks_physical_plan::BinaryOperator::NotEq
                        | novarocks_physical_plan::BinaryOperator::Lt
                        | novarocks_physical_plan::BinaryOperator::LtEq
                        | novarocks_physical_plan::BinaryOperator::Gt
                        | novarocks_physical_plan::BinaryOperator::GtEq
                        | novarocks_physical_plan::BinaryOperator::Add
                        | novarocks_physical_plan::BinaryOperator::Subtract
                        | novarocks_physical_plan::BinaryOperator::Multiply
                        | novarocks_physical_plan::BinaryOperator::Divide
                        | novarocks_physical_plan::BinaryOperator::Modulo,
                    left,
                    right,
                    ..
                } => [*left, *right].get(next).copied(),
                ExprKind::Like { expr, pattern, .. } => [*expr, *pattern].get(next).copied(),
                ExprKind::Between {
                    expr, low, high, ..
                } => [*expr, *low, *high].get(next).copied(),
                ExprKind::InList { expr, list, .. } => {
                    if next == 0 {
                        Some(*expr)
                    } else {
                        list.get(next - 1).copied()
                    }
                }
                ExprKind::Case {
                    operand,
                    when_then,
                    else_expr,
                } => case_child(*operand, when_then, *else_expr, next),
                _ => return Err(ExpressionLoweringError::UnsupportedExpression(id).into()),
            };
            if let Some(child) = child {
                let frame = stack
                    .last_mut()
                    .ok_or(ExpressionLoweringError::Invalid("missing dependency frame"))?;
                frame.1 += 1;
                if !ids.contains_key(&child) {
                    if !active.insert(child) {
                        return Err(
                            ExpressionLoweringError::Invalid("cyclic physical expression").into(),
                        );
                    }
                    if stack.len() >= MAX_CONTROL_DEPTH {
                        return Err(ExpressionLoweringError::Invalid(
                            "expression dependency depth exceeded",
                        )
                        .into());
                    }
                    stack.push((child, 0));
                }
                work.step()?;
                continue;
            }
            let kind = match &node.kind {
                ExprKind::Value(_) => {
                    // The channel owner has validated the exact physical
                    // Value, child scope and complete source type. A missing
                    // mapping cannot be repaired by guessing a slot ID.
                    let input = inputs.get(&id).ok_or(ExpressionLoweringError::Invalid(
                        "missing resolved physical input",
                    ))?;
                    StaticExprKind::SlotId(input.slot)
                }
                ExprKind::Constant(reference) => StaticExprKind::Constant(
                    package
                        .constants()
                        .resolve_observed(*reference, &node.ty, work)?,
                ),
                ExprKind::Literal(literal) => {
                    StaticExprKind::Constant(novarocks_physical_plan::literal_constant_observed::<
                        ExpressionLoweringError,
                    >(
                        literal,
                        &node.ty,
                        policy,
                        CompilePhase::LowerProgram,
                        work,
                    )?)
                }
                ExprKind::Conjunction { args } | ExprKind::Disjunction { args } => {
                    if args.is_empty()
                        || node.ty.data_type() != &arrow_schema::DataType::Boolean
                        || node.ty.logical_type()
                            != novarocks_type_contract::ValueLogicalType::Physical
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "Boolean connective has an invalid exact result or arity",
                        )
                        .into());
                    }
                    let mut local_args = Vec::with_capacity(args.len());
                    for child in args {
                        let child_source = source
                            .get(*child)
                            .ok_or(ExpressionLoweringError::Invalid("missing Boolean operand"))?;
                        if child_source.ty.data_type() != &arrow_schema::DataType::Boolean
                            || child_source.ty.logical_type()
                                != novarocks_type_contract::ValueLogicalType::Physical
                        {
                            return Err(ExpressionLoweringError::Invalid(
                                "Boolean operand differs from its exact physical Boolean type",
                            )
                            .into());
                        }
                        local_args.push(*ids.get(child).ok_or(
                            ExpressionLoweringError::Invalid("Boolean operand was not lowered"),
                        )?);
                        work.step()?;
                    }
                    if matches!(node.kind, ExprKind::Conjunction { .. }) {
                        StaticExprKind::NaryAnd { args: local_args }
                    } else {
                        StaticExprKind::NaryOr { args: local_args }
                    }
                }
                ExprKind::Unary {
                    op: novarocks_physical_plan::UnaryOperator::Minus,
                    expr,
                } => {
                    let child = *ids.get(expr).ok_or(ExpressionLoweringError::Invalid(
                        "native negate operand was not lowered",
                    ))?;
                    let source_child = source.get(*expr).ok_or(
                        ExpressionLoweringError::Invalid("native negate source operand is missing"),
                    )?;
                    work.flush()?;
                    novarocks_functions::PreparedNativeNegateRecipe::try_new(
                        &source_child.ty,
                        &node.ty,
                        control,
                    )?;
                    work.flush()?;
                    StaticExprKind::PreparedNativeNegate(child)
                }
                ExprKind::Unary {
                    op: novarocks_physical_plan::UnaryOperator::BitwiseNot,
                    expr,
                } => {
                    let child = *ids.get(expr).ok_or(ExpressionLoweringError::Invalid(
                        "native BitwiseNot operand was not lowered",
                    ))?;
                    let source_child =
                        source.get(*expr).ok_or(ExpressionLoweringError::Invalid(
                            "native BitwiseNot source operand is missing",
                        ))?;
                    work.flush()?;
                    novarocks_functions::PreparedNativeBitNotRecipe::try_new(
                        &source_child.ty,
                        &node.ty,
                        control,
                    )?;
                    work.flush()?;
                    StaticExprKind::PreparedNativeBitNot(child)
                }
                ExprKind::Unary {
                    op: novarocks_physical_plan::UnaryOperator::Not,
                    expr,
                }
                | ExprKind::IsNull { expr, .. } => {
                    let child = *ids.get(expr).ok_or(ExpressionLoweringError::Invalid(
                        "unary operand was not lowered",
                    ))?;
                    match &node.kind {
                        ExprKind::Unary { .. } => StaticExprKind::Not(child),
                        ExprKind::IsNull { negated: false, .. } => StaticExprKind::IsNull(child),
                        ExprKind::IsNull { negated: true, .. } => StaticExprKind::IsNotNull(child),
                        _ => unreachable!("checked unary kind"),
                    }
                }
                ExprKind::Cast {
                    expr,
                    target,
                    decimal_overflow_policy,
                    allow_throw_exception,
                } => {
                    let child = *ids.get(expr).ok_or(ExpressionLoweringError::Invalid(
                        "cast operand was not lowered",
                    ))?;
                    let authored_allow = package
                        .parameters()
                        .require(*allow_throw_exception)
                        .map_err(|_| {
                            ExpressionLoweringError::Invalid("missing cast parameter source")
                        })?;
                    let novarocks_type_contract::SemanticParameterValue::AllowThrowException(allow) =
                        authored_allow
                    else {
                        return Err(ExpressionLoweringError::Invalid(
                            "cast parameter has a foreign key",
                        )
                        .into());
                    };
                    if !novarocks_type_contract::arrow_data_types_exact_observed::<
                        ExpressionLoweringError,
                    >(target, node.ty.data_type(), || {
                        work.step().map_err(ExpressionLoweringError::Control)
                    })? {
                        return Err(ExpressionLoweringError::Invalid(
                            "cast target differs from result",
                        )
                        .into());
                    }
                    work.flush()?;
                    novarocks_functions::PreparedCastRecipe::try_new(
                        novarocks_functions::CastOperation::Carrier,
                        &source
                            .get(*expr)
                            .ok_or(ExpressionLoweringError::Invalid("missing cast source"))?
                            .ty,
                        &node.ty,
                        *decimal_overflow_policy,
                        *allow,
                        control,
                    )?;
                    StaticExprKind::PreparedCast {
                        operation: novarocks_functions::CastOperation::Carrier,
                        child,
                        decimal_overflow_policy: *decimal_overflow_policy,
                        allow_throw_exception: *allow,
                    }
                }
                ExprKind::Binary {
                    op,
                    left,
                    right,
                    decimal_overflow_policy,
                    allow_throw_exception,
                } if arithmetic_operator(*op).is_some() => {
                    let local_left = *ids.get(left).ok_or(ExpressionLoweringError::Invalid(
                        "arithmetic left operand was not lowered",
                    ))?;
                    let local_right = *ids.get(right).ok_or(ExpressionLoweringError::Invalid(
                        "arithmetic right operand was not lowered",
                    ))?;
                    let reference =
                        allow_throw_exception.ok_or(ExpressionLoweringError::Invalid(
                            "arithmetic has no admitted ALLOW_THROW reference",
                        ))?;
                    let novarocks_type_contract::SemanticParameterValue::AllowThrowException(
                        authored_allow,
                    ) = package.parameters().require(reference).map_err(|_| {
                        ExpressionLoweringError::Invalid("arithmetic ALLOW_THROW source differs")
                    })?
                    else {
                        return Err(ExpressionLoweringError::Invalid(
                            "arithmetic ALLOW_THROW key differs",
                        )
                        .into());
                    };
                    let operator = arithmetic_operator(*op).expect("checked arithmetic operator");
                    work.step()?;
                    work.flush()?;
                    // Every definition is prepared, even when it has no actual use.
                    // The source table, never the legacy arena flag, authors this value.
                    prepare_arithmetic_created(
                        operator,
                        &source
                            .get(*left)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing arithmetic left source",
                            ))?
                            .ty,
                        &source
                            .get(*right)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing arithmetic right source",
                            ))?
                            .ty,
                        &node.ty,
                        *decimal_overflow_policy,
                        *authored_allow,
                        control,
                        factory,
                    )?;
                    StaticExprKind::PreparedArithmetic {
                        operator,
                        left: local_left,
                        right: local_right,
                        decimal_overflow_policy: *decimal_overflow_policy,
                        allow_throw_exception: *authored_allow,
                    }
                }
                ExprKind::Binary {
                    op: novarocks_physical_plan::BinaryOperator::EqForNull,
                    left,
                    right,
                    ..
                } => {
                    let local_left = *ids.get(left).ok_or(ExpressionLoweringError::Invalid(
                        "null-safe left operand was not lowered",
                    ))?;
                    let local_right = *ids.get(right).ok_or(ExpressionLoweringError::Invalid(
                        "null-safe right operand was not lowered",
                    ))?;
                    if node.ty.data_type() != &arrow_schema::DataType::Boolean
                        || node.ty.logical_type()
                            != novarocks_type_contract::ValueLogicalType::Physical
                        || node.ty.nullable()
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "null-safe comparison requires nonnullable Physical Boolean result",
                        )
                        .into());
                    }
                    work.flush()?;
                    novarocks_functions::PreparedNullSafeComparisonRecipe::try_new(
                        &source
                            .get(*left)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing null-safe left source",
                            ))?
                            .ty,
                        &source
                            .get(*right)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing null-safe right source",
                            ))?
                            .ty,
                        control,
                    )?;
                    StaticExprKind::PreparedNullSafeComparison {
                        left: local_left,
                        right: local_right,
                    }
                }
                ExprKind::Binary {
                    op:
                        novarocks_physical_plan::BinaryOperator::Eq
                        | novarocks_physical_plan::BinaryOperator::NotEq
                        | novarocks_physical_plan::BinaryOperator::Lt
                        | novarocks_physical_plan::BinaryOperator::LtEq
                        | novarocks_physical_plan::BinaryOperator::Gt
                        | novarocks_physical_plan::BinaryOperator::GtEq,
                    left,
                    right,
                    ..
                } => {
                    let left = *ids.get(left).ok_or(ExpressionLoweringError::Invalid(
                        "comparison left operand was not lowered",
                    ))?;
                    let right = *ids.get(right).ok_or(ExpressionLoweringError::Invalid(
                        "comparison right operand was not lowered",
                    ))?;
                    let ExprKind::Binary { op, .. } = &node.kind else {
                        unreachable!("checked comparison kind")
                    };
                    StaticExprKind::from_comparison(
                        comparison_operator(*op).expect("checked ordinary comparison operator"),
                        left,
                        right,
                    )
                }
                ExprKind::Like {
                    expr,
                    pattern,
                    negated,
                } => {
                    let text = *ids.get(expr).ok_or(ExpressionLoweringError::Invalid(
                        "LIKE source was not lowered",
                    ))?;
                    let pattern = *ids.get(pattern).ok_or(ExpressionLoweringError::Invalid(
                        "LIKE pattern was not lowered",
                    ))?;
                    StaticExprKind::PreparedLike {
                        text,
                        pattern,
                        negated: *negated,
                    }
                }
                ExprKind::InList {
                    expr,
                    list,
                    negated,
                } => {
                    let child = *ids.get(expr).ok_or(ExpressionLoweringError::Invalid(
                        "IN source was not lowered",
                    ))?;
                    let mut values = Vec::with_capacity(list.len());
                    for id in list {
                        values.push(*ids.get(id).ok_or(ExpressionLoweringError::Invalid(
                            "IN candidate was not lowered",
                        ))?);
                        work.step()?;
                    }
                    StaticExprKind::PreparedInList {
                        child,
                        values,
                        is_not_in: *negated,
                    }
                }
                ExprKind::Between {
                    expr,
                    low,
                    high,
                    negated,
                } => {
                    let operand = *ids.get(expr).ok_or(ExpressionLoweringError::Invalid(
                        "BETWEEN operand was not lowered",
                    ))?;
                    let low = *ids.get(low).ok_or(ExpressionLoweringError::Invalid(
                        "BETWEEN lower was not lowered",
                    ))?;
                    let high = *ids.get(high).ok_or(ExpressionLoweringError::Invalid(
                        "BETWEEN upper was not lowered",
                    ))?;
                    StaticExprKind::PreparedBetween {
                        plan: novarocks_type_contract::NativeBetweenPlan::new(*negated),
                        operand,
                        low,
                        high,
                    }
                }
                ExprKind::Case {
                    operand,
                    when_then,
                    else_expr,
                } => {
                    let count = case_arity(*operand, when_then, *else_expr)?;
                    let mut children = Vec::with_capacity(count);
                    for ordinal in 0..count {
                        let child = case_child(*operand, when_then, *else_expr, ordinal).ok_or(
                            ExpressionLoweringError::Invalid("missing actual CASE child"),
                        )?;
                        children.push(*ids.get(&child).ok_or(ExpressionLoweringError::Invalid(
                            "CASE child was not lowered",
                        ))?);
                        work.step()?;
                    }
                    StaticExprKind::Case {
                        has_case_expr: operand.is_some(),
                        has_else_expr: else_expr.is_some(),
                        children,
                    }
                }
                ExprKind::FunctionCall { args, .. } => {
                    let mut local_args = Vec::with_capacity(args.len());
                    for child in args {
                        local_args.push(*ids.get(child).ok_or(
                            ExpressionLoweringError::Invalid(
                                "expression dependency was not lowered",
                            ),
                        )?);
                        work.step()?;
                    }
                    StaticExprKind::BoundCall { args: local_args }
                }
                _ => return Err(ExpressionLoweringError::UnsupportedExpression(id).into()),
            };
            work.flush()?;
            let ty = node.ty.clone_observed(|| work.step())?;
            nodes.push(StaticExprNode::new_value_typed(
                kind,
                ty.clone_observed(|| work.step())?,
            ));
            observe_type_push(&types, crate::creation::borrow_scope(&mut creation), work)?;
            types.push(FunctionArgumentType::Value(ty));
            work.flush()?;
            ids.insert(id, ProgramExprId::new(nodes.len() - 1));
            active.remove(&id);
            stack.pop();
            work.step()?;
        }
    }
    let mut union_ids = BTreeMap::new();
    std::alloc::Layout::array::<StaticExprNode>(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    std::alloc::Layout::array::<FunctionArgumentType>(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    nodes
        .try_reserve_exact(count - nodes.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    if count > types.capacity() {
        observe_type_layout(
            std::alloc::Layout::array::<FunctionArgumentType>(count)
                .map_err(|_| CompileControlError::ResourceExhausted)?,
            crate::creation::borrow_scope(&mut creation),
            work,
        )?;
    }
    types
        .try_reserve_exact(count - types.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    for (&owner, branches) in unions {
        let mut rows = Vec::new();
        rows.try_reserve_exact(branches.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for branch in branches {
            let mut definitions = Vec::new();
            definitions
                .try_reserve_exact(branch.sources.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            for source in &branch.sources {
                work.flush()?;
                definitions.push(ProgramExprId::new(nodes.len()));
                nodes.push(StaticExprNode::new_value_typed(
                    StaticExprKind::SlotId(source.input.slot),
                    source.ty.clone_observed(|| work.step())?,
                ));
                observe_type_push(&types, crate::creation::borrow_scope(&mut creation), work)?;
                types.push(FunctionArgumentType::Value(
                    source.ty.clone_observed(|| work.step())?,
                ));
                work.step()?;
                work.flush()?;
            }
            rows.push(definitions);
            work.step()?;
        }
        union_ids.insert(owner, rows);
        work.step()?;
    }
    // A scan consumer key reads one occurrence of its scan's own output port.
    let mut runtime_filter_key_ids = BTreeMap::new();
    for (&scan, keys) in runtime_filter_keys {
        let mut definitions = Vec::new();
        definitions
            .try_reserve_exact(keys.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for key in keys {
            work.flush()?;
            definitions.push(ProgramExprId::new(nodes.len()));
            nodes.push(StaticExprNode::new_value_typed(
                StaticExprKind::SlotId(key.input.slot),
                key.ty.clone_observed(|| work.step())?,
            ));
            observe_type_push(&types, crate::creation::borrow_scope(&mut creation), work)?;
            types.push(FunctionArgumentType::Value(
                key.ty.clone_observed(|| work.step())?,
            ));
            work.step()?;
            work.flush()?;
        }
        runtime_filter_key_ids.insert(scan, definitions);
        work.step()?;
    }
    work.flush()?;
    // No legacy exception or session-timezone capability is authored here.
    // Frozen semantic parameters remain on exact prepared call contracts.
    let arena = Arc::new(ImmutableExpressions::try_new_for_compile(
        nodes,
        false,
        HashMap::new(),
        None,
        control,
    )?);
    work.flush()?;
    let types = crate::creation::publish_argument_types(
        types,
        crate::creation::borrow_scope(&mut creation),
        work,
    )
    .map_err(|e| {
        use novarocks_local_program::ProgramCreationFailure as Failure;
        match e {
            Failure::Control(c) => ExpressionCreationFailure::Control(c),
            Failure::Creator(c) => ExpressionCreationFailure::Creator(c),
            Failure::Resource(c) => ExpressionCreationFailure::Resource(c),
            Failure::Capacity(source) => {
                ExpressionCreationFailure::Owner(ExpressionLoweringError::Publication(
                    novarocks_local_program::ProgramCreationFailure::Capacity(source),
                ))
            }
            Failure::Owner(never) => match never {},
        }
    })?;
    Ok(LoweredExpressions {
        arena,
        ids,
        types,
        union_ids,
        runtime_filter_key_ids,
    })
}

/// Prepare actual expression occurrences using exact frozen identities and
/// facts. Ordinary eager/type-only and installed IF/COALESCE control calls
/// keep their exact lifecycle. Other guarded, lambda and relational protocols
/// remain explicit pending cases rather than becoming ordinary scalar calls.
#[cfg(test)]
pub(crate) fn prepare_calls(
    package: &FragmentPackage,
    lowered: &LoweredExpressions,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<Vec<(ProgramCallSite, PureCallSpecialization)>, ExpressionLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = prepare_core::<std::convert::Infallible, crate::creation::NoFactory>(
        package, lowered, functions, None, control, &mut work, None,
    )
    .map_err(ExpressionCreationFailure::into_pure);
    finish(result, &mut work)
}

/// Prepare every expression occurrence and then every relational Aggregate
/// call. `nodes` names the local node each physical node lowers to; an
/// aggregate call's argument effects are its prepared argument roots'.
pub(crate) fn prepare_calls_with_aggregates(
    package: &FragmentPackage,
    lowered: &LoweredExpressions,
    functions: &PureEngineFunctionCatalog,
    nodes: &BTreeMap<novarocks_physical_plan::NodeId, novarocks_local_program::ProgramNodeId>,
    control: &dyn PureCompileControl,
) -> Result<Vec<(ProgramCallSite, PureCallSpecialization)>, ExpressionLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = prepare_core::<std::convert::Infallible, crate::creation::NoFactory>(
        package,
        lowered,
        functions,
        Some(nodes),
        control,
        &mut work,
        None,
    )
    .map_err(ExpressionCreationFailure::into_pure);
    finish(result, &mut work)
}

pub(crate) fn prepare_calls_with_aggregates_with_creator<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    package: &FragmentPackage,
    lowered: &LoweredExpressions,
    functions: &PureEngineFunctionCatalog,
    nodes: &BTreeMap<novarocks_physical_plan::NodeId, novarocks_local_program::ProgramNodeId>,
    policy: &ConstantPolicy,
    factory: &F,
    control: &dyn PureCompileControl,
) -> Result<Vec<(ProgramCallSite, PureCallSpecialization)>, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = prepare_core(
        package,
        lowered,
        functions,
        Some(nodes),
        control,
        &mut work,
        Some((factory, policy)),
    )
    .map_err(|error| match error {
        ExpressionCreationFailure::Control(cause) => crate::FragmentCreationError::Control(cause),
        ExpressionCreationFailure::Creator(cause) => crate::FragmentCreationError::Creator(cause),
        ExpressionCreationFailure::Resource(cause) => crate::FragmentCreationError::Resource(cause),
        ExpressionCreationFailure::Owner(cause) => crate::FragmentCreationError::from(cause),
    });
    crate::creation::finish_attempt(result, work)
}

fn prepare_core<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    lowered: &LoweredExpressions,
    functions: &PureEngineFunctionCatalog,
    aggregate_nodes: Option<
        &BTreeMap<novarocks_physical_plan::NodeId, novarocks_local_program::ProgramNodeId>,
    >,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
    creation: CallCreation<'_, F>,
) -> Result<Vec<(ProgramCallSite, PureCallSpecialization)>, ExpressionCreationFailure<H>> {
    // The private intermediate is checked against this exact borrowed source
    // again. A same-sized mapping from a foreign package is not authority.
    // Window calls are the only definitions without a scalar local owner.
    let mut window_calls = 0usize;
    for (_, definition) in package.fragment().expressions().iter() {
        if matches!(definition.kind, ExprKind::WindowCall { .. }) {
            window_calls += 1;
        }
        work.step()?;
    }
    if lowered.ids.len().checked_add(window_calls) != Some(package.fragment().expressions().len())
        || lowered.types.len() != lowered.arena.nodes().len()
    {
        return Err(ExpressionLoweringError::Invalid("lowered definition coverage differs").into());
    }
    let mut local_ids = BTreeSet::new();
    for (&physical, definition) in package.fragment().expressions().iter() {
        if matches!(definition.kind, ExprKind::WindowCall { .. }) {
            if lowered.ids.contains_key(&physical) {
                return Err(ExpressionLoweringError::Invalid(
                    "window call was lowered as a scalar definition",
                )
                .into());
            }
            work.step()?;
            continue;
        }
        let local_id = *lowered
            .ids
            .get(&physical)
            .ok_or(ExpressionLoweringError::Invalid(
                "missing lowered definition",
            ))?;
        if !local_ids.insert(local_id) {
            return Err(
                ExpressionLoweringError::Invalid("distinct definitions share a local ID").into(),
            );
        }
        let Some(FunctionArgumentType::Value(ty)) = lowered.types.get(local_id.index()) else {
            return Err(
                ExpressionLoweringError::Invalid("missing complete local value type").into(),
            );
        };
        // The shared walker observes borrowed work before its operations.
        // These are cancellation observations, not fabricated completed units.
        work.flush()?;
        let same = definition
            .ty
            .exactly_equals_observed::<ExpressionLoweringError>(ty, || {
                control
                    .checkpoint(CompilePhase::FunctionSpecialization, 0)
                    .map_err(ExpressionLoweringError::Control)
            })?;
        work.flush()?;
        if !same {
            return Err(
                ExpressionLoweringError::Invalid("lowered full type differs from source").into(),
            );
        }
        if let ExprKind::Constant(reference) = &definition.kind {
            let node = lowered
                .arena
                .node(local_id)
                .ok_or(ExpressionLoweringError::Invalid(
                    "missing lowered constant definition",
                ))?;
            let StaticExprKind::Constant(value) = node.kind() else {
                return Err(ExpressionLoweringError::Invalid(
                    "constant reference lost its checked source",
                )
                .into());
            };
            let source = package
                .constants()
                .resolve_observed(*reference, &definition.ty, work)?;
            work.flush()?;
            let same_type = definition
                .ty
                .exactly_equals_observed::<ExpressionLoweringError>(value.value_type(), || {
                    control
                        .checkpoint(CompilePhase::FunctionSpecialization, 0)
                        .map_err(ExpressionLoweringError::Control)
                })?;
            work.flush()?;
            // This is exact source retention, not semantic selected-value
            // equality: a private mapping cannot substitute another backing.
            if !same_type
                || value.pool().backing_identity() != source.pool().backing_identity()
                || value.ordinal() != source.ordinal()
            {
                return Err(ConstantReferenceError::InvalidConsumer(
                    "lowered constant reference differs from its checked source",
                )
                .into());
            }
        }
        work.step()?;
    }
    // Static validation covers every declared call, including dead definitions
    // and TypeOnly children. It uses no effects or invocation demand, and must
    // not prepare a stateful implementation merely to validate its signature.
    let mut selected = BTreeMap::<ExprId, Arc<FunctionBindingSelection>>::new();
    let mut requests = BTreeMap::new();
    for (&id, definition) in package.fragment().expressions().iter() {
        if let ExprKind::FunctionCall { function, args } = &definition.kind {
            if args.len() > novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            if function.kind != FunctionKind::Scalar {
                return Err(ExpressionLoweringError::UnsupportedExpression(id).into());
            }
            let local_id = *lowered
                .ids
                .get(&id)
                .ok_or(ExpressionLoweringError::Invalid(
                    "missing local call definition",
                ))?;
            let Some(StaticExprKind::BoundCall { args: local_args }) =
                lowered.arena.node(local_id).map(StaticExprNode::kind)
            else {
                return Err(ExpressionLoweringError::Invalid("lowered call shape differs").into());
            };
            if args.len() != local_args.len() || args.len() != function.argument_types.len() {
                return Err(ExpressionLoweringError::Invalid("static call arity differs").into());
            }
            work.flush()?;
            let result_matches = function
                .result_type
                .exactly_equals_observed::<ExpressionLoweringError>(&definition.ty, || {
                    control
                        .checkpoint(CompilePhase::FunctionSpecialization, 0)
                        .map_err(ExpressionLoweringError::Control)
                })?;
            work.flush()?;
            if !result_matches {
                return Err(ExpressionLoweringError::Invalid(
                    "static call result differs from its definition",
                )
                .into());
            }
            for (ordinal, child) in args.iter().enumerate() {
                let child_id = *lowered
                    .ids
                    .get(child)
                    .ok_or(ExpressionLoweringError::Invalid(
                        "missing static call argument",
                    ))?;
                if local_args[ordinal] != child_id {
                    return Err(ExpressionLoweringError::Invalid(
                        "ordered static arguments differ",
                    )
                    .into());
                }
                let child_source = package.fragment().expressions().get(*child).ok_or(
                    ExpressionLoweringError::Invalid("missing physical static argument"),
                )?;
                let child_node =
                    lowered
                        .arena
                        .node(child_id)
                        .ok_or(ExpressionLoweringError::Invalid(
                            "missing local static argument",
                        ))?;
                work.flush()?;
                // Check the independent physical/local projection. It cannot
                // supply the original binding request's constant channel.
                literal_argument(child_source, child_node, control)?;
                work.flush()?;
                work.step()?;
            }
            let source_request = package
                .fragment()
                .call_requests()
                .get(PhysicalCallDefinition::Expression(id));
            work.step()?;
            let source_request = source_request.ok_or(ExpressionLoweringError::Invalid(
                "missing original static call request",
            ))?;
            let request = crate::original_requests::materialize_call_request_created(
                source_request,
                package.constants(),
                work,
                creation,
            )?;
            work.flush()?;
            let selection = Arc::new(FunctionBindingSelection {
                overload: function.overload.clone(),
                argument_types: function.argument_types.clone(),
                result_type: FunctionResultType::Scalar(function.result_type.clone()),
                aggregate: None,
            });
            work.flush()?;
            validate_call_created(
                functions.metadata(),
                &function.function_id,
                function.kind,
                selection.as_ref(),
                request.request(),
                control,
                creation,
            )?;
            work.flush()?;
            selected.insert(id, selection);
            work.step()?;
            work.flush()?;
            requests.insert(id, request);
            work.step()?;
            work.flush()?;
        }
        work.step()?;
    }
    let flow = package.expression_uses().flow();
    // Relational Aggregate, Table, Window and writer calls are prepared after every
    // expression occurrence; every other relational lifecycle stays explicit.
    for &site in package.calls().entries().keys() {
        let admitted = match site {
            PhysicalCallSite::Expression(id) => {
                let window = flow.uses().get(&id).is_some_and(|invocation| {
                    package
                        .fragment()
                        .expressions()
                        .get(invocation.definition)
                        .is_some_and(|definition| {
                            matches!(definition.kind, ExprKind::WindowCall { .. })
                        })
                });
                !window || aggregate_nodes.is_some()
            }
            PhysicalCallSite::Aggregate { .. }
            | PhysicalCallSite::TopNState { .. }
            | PhysicalCallSite::Table { .. }
            | PhysicalCallSite::WriterPartial { .. }
            | PhysicalCallSite::WriterFinal { .. } => aggregate_nodes.is_some(),
        };
        if !admitted {
            return Err(ExpressionLoweringError::UnsupportedCall(site).into());
        }
        work.step()?;
    }
    let mut effects = BTreeMap::<ExpressionUseId, ScopedExpressionEffects>::new();
    let mut tokens = BTreeMap::new();
    let mut active = BTreeSet::new();
    let mut stack = Vec::new();
    for &root in flow.uses().keys() {
        if effects.contains_key(&root) {
            work.step()?;
            continue;
        }
        active.insert(root);
        stack.push((root, 0usize));
        work.step()?;
        while let Some(&(id, next)) = stack.last() {
            let invocation = flow
                .uses()
                .get(&id)
                .ok_or(ExpressionLoweringError::Invalid(
                    "missing expression occurrence",
                ))?;
            if let Some(&child) = invocation.arguments.get(next) {
                let frame = stack
                    .last_mut()
                    .ok_or(ExpressionLoweringError::Invalid("missing invocation frame"))?;
                frame.1 += 1;
                if !effects.contains_key(&child) {
                    if !active.insert(child) {
                        return Err(ExpressionLoweringError::Invalid(
                            "cyclic expression occurrence",
                        )
                        .into());
                    }
                    if stack.len() >= MAX_CONTROL_DEPTH {
                        return Err(ExpressionLoweringError::Invalid(
                            "expression occurrence depth exceeded",
                        )
                        .into());
                    }
                    stack.push((child, 0));
                }
                work.step()?;
                continue;
            }
            let source = package
                .fragment()
                .expressions()
                .get(invocation.definition)
                .ok_or(ExpressionLoweringError::Invalid(
                    "missing invocation definition",
                ))?;
            // A window call occurrence is retired from the local flow; its
            // Window node prepares it from its arguments' effects below.
            if matches!(source.kind, ExprKind::WindowCall { .. }) {
                active.remove(&id);
                stack.pop();
                work.step()?;
                continue;
            }
            let local_id = *lowered
                .ids
                .get(&source.id)
                .ok_or(ExpressionLoweringError::Invalid(
                    "invocation definition was not lowered",
                ))?;
            let local = lowered
                .arena
                .node(local_id)
                .ok_or(ExpressionLoweringError::Invalid("missing local definition"))?;
            let scoped = match (&source.kind, local.kind()) {
                (ExprKind::Value(_), StaticExprKind::SlotId(_)) => {
                    if invocation.control != ControlShape::Eager || !invocation.arguments.is_empty()
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "input occurrence has arguments or control",
                        )
                        .into());
                    }
                    ScopedExpressionEffects::pure_value(invocation.context)
                }
                (ExprKind::Literal(_) | ExprKind::Constant(_), StaticExprKind::Constant(_)) => {
                    if invocation.control != ControlShape::Eager || !invocation.arguments.is_empty()
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "literal occurrence has arguments or control",
                        )
                        .into());
                    }
                    ScopedExpressionEffects::pure_value(invocation.context)
                }
                (
                    ExprKind::Like {
                        expr,
                        pattern,
                        negated,
                    },
                    StaticExprKind::PreparedLike {
                        text,
                        pattern: local_pattern,
                        negated: local_negated,
                    },
                ) => {
                    if negated != local_negated
                        || invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 2
                        || lowered.ids.get(expr) != Some(text)
                        || lowered.ids.get(pattern) != Some(local_pattern)
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "LIKE differs from original Eager2 source",
                        )
                        .into());
                    }
                    let definitions = package.fragment().expressions();
                    let ty = |id| {
                        definitions.get(id).map(|node| &node.ty).ok_or(
                            ExpressionLoweringError::Invalid("missing original LIKE source"),
                        )
                    };
                    work.flush()?;
                    let recipe = novarocks_functions::PreparedNativeLikeRecipe::try_new(
                        *negated,
                        ty(*expr)?,
                        ty(*pattern)?,
                        &source.ty,
                        control,
                    )?;
                    work.flush()?;
                    let mut combined = recipe.own_effects(invocation.context);
                    for (ordinal, definition) in [expr, pattern].into_iter().enumerate() {
                        let child = invocation.arguments[ordinal];
                        if flow.uses()[&child].definition != *definition {
                            return Err(ExpressionLoweringError::Invalid(
                                "LIKE source use differs",
                            )
                            .into());
                        }
                        combined = combined.join_control_argument(
                            *effects.get(&child).ok_or(ExpressionLoweringError::Invalid(
                                "LIKE child effects were not prepared",
                            ))?,
                            flow,
                            ordinal,
                        )?;
                        work.step()?;
                    }
                    combined
                }
                (
                    ExprKind::InList {
                        expr,
                        list,
                        negated,
                    },
                    StaticExprKind::PreparedInList {
                        child,
                        values,
                        is_not_in,
                    },
                ) => {
                    if *is_not_in != *negated
                        || invocation.control != (ControlShape::Membership { negated: *negated })
                        || invocation.arguments.len() != list.len() + 1
                        || lowered.ids.get(expr) != Some(child)
                        || list.len() != values.len()
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "IN differs from its original ordered source",
                        )
                        .into());
                    }
                    let definitions = package.fragment().expressions();
                    let source_type = |id| {
                        definitions
                            .get(id)
                            .map(|n| &n.ty)
                            .ok_or(ExpressionLoweringError::Invalid("missing IN source"))
                    };
                    for (ordinal, id) in std::iter::once(expr).chain(list.iter()).enumerate() {
                        let local = lowered.ids.get(id).ok_or(ExpressionLoweringError::Invalid(
                            "missing lowered IN source",
                        ))?;
                        if flow.uses()[&invocation.arguments[ordinal]].definition != *id {
                            return Err(ExpressionLoweringError::Invalid(
                                "IN source occurrence differs",
                            )
                            .into());
                        }
                        if ordinal > 0 {
                            if values[ordinal - 1] != *local {
                                return Err(ExpressionLoweringError::Invalid(
                                    "IN candidate definition differs",
                                )
                                .into());
                            }
                            source_type(*id)?;
                        }
                        work.step()?;
                    }
                    work.flush()?;
                    let mut combined =
                        novarocks_functions::PreparedNativeInListRecipe::refine_borrowed_profile(
                            source_type(*expr)?,
                            list.iter()
                                .map(|id| source_type(*id).expect("checked immutable IN source")),
                            &source.ty,
                            invocation.context,
                            control,
                        )?;
                    work.flush()?;
                    for (ordinal, use_id) in invocation.arguments.iter().enumerate() {
                        let child = *effects.get(use_id).ok_or(
                            ExpressionLoweringError::Invalid("missing IN candidate effects"),
                        )?;
                        combined = combined
                            .join_control_argument(child, flow, ordinal)
                            .map_err(|_| {
                                ExpressionLoweringError::Invalid(
                                    "IN candidate effects differ from ordered domain",
                                )
                            })?;
                        work.step()?;
                    }
                    combined
                }
                (
                    ExprKind::Between {
                        expr,
                        low,
                        high,
                        negated,
                    },
                    StaticExprKind::PreparedBetween {
                        plan,
                        operand,
                        low: local_low,
                        high: local_high,
                    },
                ) => {
                    if plan.negated() != *negated
                        || invocation.control != (ControlShape::Between { negated: *negated })
                        || invocation.arguments.len() != 4
                        || lowered.ids.get(expr) != Some(operand)
                        || lowered.ids.get(low) != Some(local_low)
                        || lowered.ids.get(high) != Some(local_high)
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "BETWEEN differs from its frozen source",
                        )
                        .into());
                    }
                    let definitions = package.fragment().expressions();
                    let source_type = |id| {
                        definitions
                            .get(id)
                            .map(|node| &node.ty)
                            .ok_or(ExpressionLoweringError::Invalid("missing BETWEEN source"))
                    };
                    work.flush()?;
                    let recipe = novarocks_functions::PreparedNativeBetweenRecipe::try_new(
                        *plan,
                        source_type(*expr)?,
                        source_type(*low)?,
                        source_type(*high)?,
                        &source.ty,
                        control,
                    )?;
                    work.flush()?;
                    let mut combined = recipe.own_effects(invocation.context);
                    for (ordinal, role) in plan.sources().into_iter().enumerate() {
                        let expected = match role {
                            novarocks_type_contract::BetweenSourceRole::Operand => *expr,
                            novarocks_type_contract::BetweenSourceRole::Lower => *low,
                            novarocks_type_contract::BetweenSourceRole::Upper => *high,
                        };
                        let child_use = invocation.arguments[ordinal];
                        if flow.uses()[&child_use].definition != expected {
                            return Err(ExpressionLoweringError::Invalid(
                                "BETWEEN ordered source use differs",
                            )
                            .into());
                        }
                        combined = combined.join_control_argument(
                            *effects
                                .get(&child_use)
                                .ok_or(ExpressionLoweringError::Invalid(
                                    "BETWEEN child effects were not prepared",
                                ))?,
                            flow,
                            ordinal,
                        )?;
                        work.step()?;
                    }
                    combined
                }
                (
                    ExprKind::Unary {
                        op: novarocks_physical_plan::UnaryOperator::Minus,
                        expr,
                    },
                    StaticExprKind::PreparedNativeNegate(local_child),
                ) => {
                    if invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 1
                        || lowered.ids.get(expr) != Some(local_child)
                        || flow.uses()[&invocation.arguments[0]].definition != *expr
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "native negate differs from its actual ordered source",
                        )
                        .into());
                    }
                    let child = package.fragment().expressions().get(*expr).ok_or(
                        ExpressionLoweringError::Invalid("missing native negate source"),
                    )?;
                    work.flush()?;
                    let recipe = novarocks_functions::PreparedNativeNegateRecipe::try_new(
                        &child.ty, &source.ty, control,
                    )?;
                    work.flush()?;
                    recipe
                        .own_effects(invocation.context)
                        .join_control_argument(
                            *effects.get(&invocation.arguments[0]).ok_or(
                                ExpressionLoweringError::Invalid(
                                    "native negate child effects were not prepared",
                                ),
                            )?,
                            flow,
                            0,
                        )?
                }
                (
                    ExprKind::Unary {
                        op: novarocks_physical_plan::UnaryOperator::BitwiseNot,
                        expr,
                    },
                    StaticExprKind::PreparedNativeBitNot(local_child),
                ) => {
                    if invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 1
                        || lowered.ids.get(expr) != Some(local_child)
                        || flow.uses()[&invocation.arguments[0]].definition != *expr
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "native BitwiseNot differs from its actual ordered source",
                        )
                        .into());
                    }
                    let child = package.fragment().expressions().get(*expr).ok_or(
                        ExpressionLoweringError::Invalid("missing native BitwiseNot source"),
                    )?;
                    work.flush()?;
                    let recipe = novarocks_functions::PreparedNativeBitNotRecipe::try_new(
                        &child.ty, &source.ty, control,
                    )?;
                    work.flush()?;
                    recipe
                        .own_effects(invocation.context)
                        .join_control_argument(
                            *effects.get(&invocation.arguments[0]).ok_or(
                                ExpressionLoweringError::Invalid(
                                    "native BitwiseNot child effects were not prepared",
                                ),
                            )?,
                            flow,
                            0,
                        )?
                }
                (
                    ExprKind::Unary {
                        op: novarocks_physical_plan::UnaryOperator::Not,
                        expr,
                    },
                    StaticExprKind::Not(local_child),
                )
                | (
                    ExprKind::IsNull {
                        expr,
                        negated: false,
                    },
                    StaticExprKind::IsNull(local_child),
                )
                | (
                    ExprKind::IsNull {
                        expr,
                        negated: true,
                    },
                    StaticExprKind::IsNotNull(local_child),
                ) => {
                    if invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 1
                        || lowered.ids.get(expr) != Some(local_child)
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "actual unary control or operand differs",
                        )
                        .into());
                    }
                    let child = invocation.arguments[0];
                    if flow.uses()[&child].definition != *expr {
                        return Err(ExpressionLoweringError::Invalid(
                            "actual unary operand occurrence differs",
                        )
                        .into());
                    }
                    ScopedExpressionEffects::pure_value(invocation.context).join_control_argument(
                        *effects.get(&child).ok_or(ExpressionLoweringError::Invalid(
                            "unary operand effects were not prepared",
                        ))?,
                        flow,
                        0,
                    )?
                }
                (
                    ExprKind::Cast {
                        expr,
                        target,
                        decimal_overflow_policy,
                        allow_throw_exception,
                    },
                    StaticExprKind::PreparedCast {
                        operation,
                        child,
                        decimal_overflow_policy: local_policy,
                        allow_throw_exception: local_allow,
                    },
                ) => {
                    let source_allow = package
                        .parameters()
                        .require(*allow_throw_exception)
                        .map_err(|_| {
                            ExpressionLoweringError::Invalid("cast parameter source differs")
                        })?;
                    if *operation != novarocks_functions::CastOperation::Carrier
                        || decimal_overflow_policy != local_policy
                        || source_allow
                            != &novarocks_type_contract::SemanticParameterValue::AllowThrowException(
                                *local_allow,
                            )
                        || invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 1
                        || lowered.ids.get(expr) != Some(child)
                        || flow.uses()[&invocation.arguments[0]].definition != *expr
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "cast occurrence differs from its frozen source",
                        )
                        .into());
                    }
                    if !novarocks_type_contract::arrow_data_types_exact_observed::<
                        ExpressionLoweringError,
                    >(target, source.ty.data_type(), || {
                        work.step().map_err(ExpressionLoweringError::Control)
                    })? {
                        return Err(ExpressionLoweringError::Invalid(
                            "cast result differs from frozen target",
                        )
                        .into());
                    }
                    work.flush()?;
                    let recipe = novarocks_functions::PreparedCastRecipe::try_new(
                        *operation,
                        &package
                            .fragment()
                            .expressions()
                            .get(*expr)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing cast operand source",
                            ))?
                            .ty,
                        &source.ty,
                        *local_policy,
                        *local_allow,
                        control,
                    )?;
                    recipe
                        .own_effects(invocation.context)
                        .join_control_argument(
                            *effects.get(&invocation.arguments[0]).ok_or(
                                ExpressionLoweringError::Invalid(
                                    "cast child effects were not prepared",
                                ),
                            )?,
                            flow,
                            0,
                        )?
                }
                (
                    ExprKind::Binary {
                        op,
                        left,
                        right,
                        decimal_overflow_policy,
                        allow_throw_exception,
                    },
                    StaticExprKind::PreparedArithmetic {
                        operator,
                        left: local_left,
                        right: local_right,
                        decimal_overflow_policy: local_policy,
                        allow_throw_exception: local_allow,
                    },
                ) => {
                    let reference = allow_throw_exception.ok_or(
                        ExpressionLoweringError::Invalid("missing arithmetic parameter reference"),
                    )?;
                    let source_allow = package.parameters().require(reference).map_err(|_| {
                        ExpressionLoweringError::Invalid("arithmetic parameter source differs")
                    })?;
                    if arithmetic_operator(*op) != Some(*operator)
                        || decimal_overflow_policy != local_policy
                        || source_allow
                            != &novarocks_type_contract::SemanticParameterValue::AllowThrowException(
                                *local_allow,
                            )
                        || invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 2
                        || lowered.ids.get(left) != Some(local_left)
                        || lowered.ids.get(right) != Some(local_right)
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "arithmetic occurrence differs from its frozen source",
                        )
                        .into());
                    }
                    let definitions = package.fragment().expressions();
                    work.flush()?;
                    let recipe = prepare_arithmetic_created(
                        *operator,
                        &definitions
                            .get(*left)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing arithmetic left source",
                            ))?
                            .ty,
                        &definitions
                            .get(*right)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing arithmetic right source",
                            ))?
                            .ty,
                        &source.ty,
                        *local_policy,
                        *local_allow,
                        control,
                        creation.map(|(factory, _)| factory),
                    )?;
                    let mut combined = recipe.own_effects(invocation.context);
                    for (ordinal, physical) in [*left, *right].into_iter().enumerate() {
                        let child_use = invocation.arguments[ordinal];
                        if flow.uses()[&child_use].definition != physical {
                            return Err(ExpressionLoweringError::Invalid(
                                "actual ordered arithmetic use differs",
                            )
                            .into());
                        }
                        combined = combined.join_control_argument(
                            *effects
                                .get(&child_use)
                                .ok_or(ExpressionLoweringError::Invalid(
                                    "arithmetic child effects were not prepared",
                                ))?,
                            flow,
                            ordinal,
                        )?;
                        work.step()?;
                    }
                    combined
                }
                (
                    ExprKind::Binary {
                        op, left, right, ..
                    },
                    kind,
                ) if (kind.ordinary_comparison().is_some()
                    && comparison_operator(*op)
                        == kind.ordinary_comparison().map(|parts| parts.0))
                    || (*op == novarocks_physical_plan::BinaryOperator::EqForNull
                        && matches!(kind, StaticExprKind::PreparedNullSafeComparison { .. })) =>
                {
                    let (local_left, local_right) = match kind {
                        StaticExprKind::PreparedNullSafeComparison { left, right } => {
                            (*left, *right)
                        }
                        _ => {
                            let (_, left, right) =
                                kind.ordinary_comparison().expect("checked comparison kind");
                            (left, right)
                        }
                    };
                    if invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 2
                        || lowered.ids.get(left) != Some(&local_left)
                        || lowered.ids.get(right) != Some(&local_right)
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "actual comparison control or operands differ",
                        )
                        .into());
                    }
                    let mut combined =
                        if matches!(kind, StaticExprKind::PreparedNullSafeComparison { .. }) {
                            work.flush()?;
                            novarocks_functions::PreparedNullSafeComparisonRecipe::try_new(
                                &package
                                    .fragment()
                                    .expressions()
                                    .get(*left)
                                    .ok_or(ExpressionLoweringError::Invalid(
                                        "missing null-safe left source",
                                    ))?
                                    .ty,
                                &package
                                    .fragment()
                                    .expressions()
                                    .get(*right)
                                    .ok_or(ExpressionLoweringError::Invalid(
                                        "missing null-safe right source",
                                    ))?
                                    .ty,
                                control,
                            )?
                            .own_effects(invocation.context)
                        } else {
                            let (operator, _, _) =
                                kind.ordinary_comparison().expect("checked comparison kind");
                            let definitions = package.fragment().expressions();
                            work.flush()?;
                            let recipe = novarocks_functions::PreparedComparisonRecipe::try_new(
                                operator,
                                &definitions
                                    .get(*left)
                                    .ok_or(ExpressionLoweringError::Invalid(
                                        "missing ordinary comparison left source",
                                    ))?
                                    .ty,
                                &definitions
                                    .get(*right)
                                    .ok_or(ExpressionLoweringError::Invalid(
                                        "missing ordinary comparison right source",
                                    ))?
                                    .ty,
                                control,
                            )?;
                            work.flush()?;
                            recipe.own_effects(invocation.context)
                        };
                    for (ordinal, physical) in [*left, *right].into_iter().enumerate() {
                        let child_use = invocation.arguments[ordinal];
                        if flow.uses()[&child_use].definition != physical {
                            return Err(ExpressionLoweringError::Invalid(
                                "actual ordered comparison use differs",
                            )
                            .into());
                        }
                        combined = combined.join_control_argument(
                            *effects
                                .get(&child_use)
                                .ok_or(ExpressionLoweringError::Invalid(
                                    "comparison child effects were not prepared",
                                ))?,
                            flow,
                            ordinal,
                        )?;
                        work.step()?;
                    }
                    combined
                }
                (
                    ExprKind::Case {
                        operand,
                        when_then,
                        else_expr,
                    },
                    StaticExprKind::Case {
                        has_case_expr,
                        has_else_expr,
                        children,
                    },
                ) => {
                    let count = case_arity(*operand, when_then, *else_expr)?;
                    let expected = ControlShape::Case {
                        simple: operand.is_some(),
                        arms: u32::try_from(when_then.len()).map_err(|_| {
                            ExpressionLoweringError::Invalid("CASE arm count is not representable")
                        })?,
                        has_else: else_expr.is_some(),
                    };
                    if invocation.control != expected
                        || *has_case_expr != operand.is_some()
                        || *has_else_expr != else_expr.is_some()
                        || children.len() != count
                        || invocation.arguments.len() != count
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "actual CASE control or children differ",
                        )
                        .into());
                    }
                    let mut combined = ScopedExpressionEffects::pure_value(invocation.context);
                    for (ordinal, local_child) in children.iter().enumerate() {
                        let source_child = case_child(*operand, when_then, *else_expr, ordinal)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing actual CASE operand",
                            ))?;
                        let child_use = invocation.arguments[ordinal];
                        if lowered.ids.get(&source_child) != Some(local_child)
                            || flow.uses()[&child_use].definition != source_child
                        {
                            return Err(ExpressionLoweringError::Invalid(
                                "ordered CASE occurrence differs",
                            )
                            .into());
                        }
                        combined = combined.join_control_argument(
                            *effects
                                .get(&child_use)
                                .ok_or(ExpressionLoweringError::Invalid(
                                    "CASE child effects were not prepared",
                                ))?,
                            flow,
                            ordinal,
                        )?;
                        work.step()?;
                    }
                    combined
                }
                (ExprKind::Conjunction { args }, StaticExprKind::NaryAnd { args: local_args })
                | (ExprKind::Disjunction { args }, StaticExprKind::NaryOr { args: local_args }) => {
                    let expected = if matches!(source.kind, ExprKind::Conjunction { .. }) {
                        ControlShape::Conjunction
                    } else {
                        ControlShape::Disjunction
                    };
                    if invocation.control != expected
                        || args.len() != invocation.arguments.len()
                        || args.len() != local_args.len()
                        || args.is_empty()
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "actual Boolean connective control or arity differs",
                        )
                        .into());
                    }
                    let mut combined = ScopedExpressionEffects::pure_value(invocation.context);
                    for (ordinal, &child) in args.iter().enumerate() {
                        let child_use = invocation.arguments[ordinal];
                        if flow.uses()[&child_use].definition != child {
                            return Err(ExpressionLoweringError::Invalid(
                                "ordered Boolean operand occurrences differ",
                            )
                            .into());
                        }
                        combined = combined.join_control_argument(
                            *effects
                                .get(&child_use)
                                .ok_or(ExpressionLoweringError::Invalid(
                                    "Boolean operand effects were not prepared",
                                ))?,
                            flow,
                            ordinal,
                        )?;
                        work.step()?;
                    }
                    combined
                }
                (
                    ExprKind::FunctionCall { function, args },
                    StaticExprKind::BoundCall { args: local_args },
                ) => {
                    let site = PhysicalCallSite::Expression(id);
                    if function.kind != FunctionKind::Scalar {
                        return Err(ExpressionLoweringError::UnsupportedCall(site).into());
                    }
                    let frozen = package.calls().entries().get(&site).ok_or(
                        ExpressionLoweringError::Invalid("missing frozen expression call"),
                    )?;
                    if frozen.context != invocation.context {
                        return Err(ExpressionLoweringError::Invalid(
                            "frozen occurrence context differs",
                        )
                        .into());
                    }
                    let (type_only, control_intrinsic) =
                        match (invocation.control, frozen.effects.argument_control) {
                            (ControlShape::Eager, ArgumentControl::Eager) => (false, false),
                            (ControlShape::TypeOnly, ArgumentControl::TypeOnly) => (true, false),
                            (ControlShape::NoArguments, ArgumentControl::NoArguments) => {
                                (true, false)
                            }
                            (ControlShape::If, ArgumentControl::If)
                            | (ControlShape::Coalesce, ArgumentControl::Coalesce) => (false, true),
                            (
                                ControlShape::TemporalSource(shape),
                                ArgumentControl::TemporalSource(kind),
                            ) if shape.kind() == kind => (false, true),
                            _ => return Err(ExpressionLoweringError::UnsupportedCall(site).into()),
                        };
                    if args.len() != local_args.len()
                        || function.argument_types.len() != args.len()
                        || invocation.arguments.len()
                            != if type_only {
                                0
                            } else {
                                frozen
                                    .temporal_source
                                    .as_ref()
                                    .map_or(args.len(), |source| source.sources.len())
                            }
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "call argument arity differs",
                        )
                        .into());
                    }
                    let request =
                        requests
                            .get(&source.id)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "call definition was not statically validated",
                            ))?;
                    let selection =
                        selected
                            .get(&source.id)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "call selection was not statically validated",
                            ))?;
                    let mut argument_uses = Vec::with_capacity(args.len());
                    let mut source_channels = Vec::new();
                    if let Some(source) = &frozen.temporal_source {
                        source_channels
                            .try_reserve_exact(source.sources.len())
                            .map_err(|_| CompileControlError::ResourceExhausted)?;
                        work.flush()?;
                    }
                    let mut children = ScopedExpressionEffects::pure_value(invocation.context);
                    let count = frozen
                        .temporal_source
                        .as_ref()
                        .map_or(args.len(), |source| source.sources.len());
                    for ordinal in 0..count {
                        let child = frozen.temporal_source.as_ref().map_or_else(
                            || args[ordinal],
                            |source| source.sources[ordinal].definition,
                        );
                        if type_only {
                            argument_uses.push(None);
                        } else {
                            let child_use = invocation.arguments[ordinal];
                            let child_invocation = flow.uses().get(&child_use).ok_or(
                                ExpressionLoweringError::Invalid(
                                    "missing call argument occurrence",
                                ),
                            )?;
                            if child_invocation.definition != child {
                                return Err(ExpressionLoweringError::Invalid(
                                    "ordered physical call arguments differ",
                                )
                                .into());
                            }
                            children = children.join_control_argument(
                                *effects.get(&child_use).ok_or(
                                    ExpressionLoweringError::Invalid(
                                        "argument effects were not prepared",
                                    ),
                                )?,
                                flow,
                                ordinal,
                            )?;
                            if let Some(source_plan) = &frozen.temporal_source {
                                let source = &source_plan.sources[ordinal];
                                if source.use_id != child_use {
                                    return Err(ExpressionLoweringError::Invalid(
                                        "temporal source use differs from ordered child edge",
                                    )
                                    .into());
                                }
                                let source_node =
                                    package.fragment().expressions().get(child).ok_or(
                                        ExpressionLoweringError::Invalid(
                                            "missing temporal source definition",
                                        ),
                                    )?;
                                source_channels.push(novarocks_functions::TemporalSourceChannel {
                                    role: source.role,
                                    context: child_invocation.context,
                                    value_type: &source_node.ty,
                                });
                            }
                            argument_uses.push(Some(child_use));
                        }
                        work.step()?;
                    }
                    let regexp_count_pattern_source = if function.function_id.as_str()
                        == "builtin.scalar/regexp_count/v1"
                    {
                        let expected =
                            novarocks_physical_plan::regexp_count_pattern_source_observed(
                                package.fragment().expressions(),
                                args,
                                work,
                            )
                            .map_err(|error| match error {
                                novarocks_physical_plan::TemporalSourceProjectionError::Control(
                                    cause,
                                ) => ExpressionLoweringError::Control(cause),
                                novarocks_physical_plan::TemporalSourceProjectionError::Invalid(
                                    message,
                                ) => ExpressionLoweringError::Invalid(message),
                            })?;
                        if frozen.regexp_count_pattern_source != Some(expected) {
                            return Err(ExpressionLoweringError::Invalid(
                                "regexp_count frozen source differs from actual native-v1 projection",
                            ).into());
                        }
                        Some(expected)
                    } else {
                        if frozen.regexp_count_pattern_source.is_some() {
                            return Err(ExpressionLoweringError::Invalid(
                                "foreign regexp_count pattern source",
                            )
                            .into());
                        }
                        None
                    };
                    let to_base64_byte_source =
                        if function.function_id.as_str() == "builtin.scalar/to_base64/v1" {
                            let expected = novarocks_physical_plan::to_base64_byte_source_observed(
                                package.fragment().expressions(),
                                args,
                                work,
                            )
                            .map_err(|error| match error {
                                novarocks_physical_plan::TemporalSourceProjectionError::Control(
                                    cause,
                                ) => ExpressionLoweringError::Control(cause),
                                novarocks_physical_plan::TemporalSourceProjectionError::Invalid(
                                    message,
                                ) => ExpressionLoweringError::Invalid(message),
                            })?;
                            if frozen.to_base64_byte_source != Some(expected) {
                                return Err(ExpressionLoweringError::Invalid(
                                "to_base64 frozen source differs from actual native-v1 projection",
                            ).into());
                            }
                            Some(expected)
                        } else {
                            if frozen.to_base64_byte_source.is_some() {
                                return Err(ExpressionLoweringError::Invalid(
                                    "foreign to_base64 byte source",
                                )
                                .into());
                            }
                            None
                        };
                    let input = CallEffectInput {
                        context: frozen.context,
                        argument_uses: match &frozen.temporal_source {
                            Some(source) => {
                                novarocks_functions::CallArgumentUses::TemporalSources {
                                    facts: &source.facts,
                                    channels: &source_channels,
                                }
                            }
                            None => match regexp_count_pattern_source {
                                Some(source) => {
                                    novarocks_functions::CallArgumentUses::RegexpCountPattern {
                                        source,
                                        channels: &argument_uses,
                                    }
                                }
                                None => match to_base64_byte_source {
                                    Some(source) => {
                                        novarocks_functions::CallArgumentUses::ToBase64Bytes {
                                            source,
                                            channels: &argument_uses,
                                        }
                                    }
                                    None => {
                                        novarocks_functions::CallArgumentUses::SelectedChannels(
                                            &argument_uses,
                                        )
                                    }
                                },
                            },
                        },
                        function_id: &function.function_id,
                        kind: function.kind,
                        selected: selection.as_ref(),
                        request: request.request(),
                        environment: &frozen.effects.environment,
                        parameters: package.parameters(),
                        decimal_overflow_policy: frozen.decimal_overflow_policy,
                        proof_scope: frozen.effects.proof_scope,
                    };
                    work.flush()?;
                    let options = if control_intrinsic {
                        PureCallPreparation::ControlIntrinsic {
                            arguments: children,
                        }
                    } else {
                        PureCallPreparation::Scalar {
                            arguments: children,
                        }
                    };
                    let token = prepare_call_created(
                        functions,
                        input,
                        Arc::clone(selection),
                        &frozen.effects,
                        options,
                        control,
                        creation,
                    )?;
                    work.flush()?;
                    let result = token.effects();
                    tokens.insert(
                        ProgramCallSite::Expression(ProgramUseRef {
                            arena: ProgramExpressionArena::Main,
                            use_id: id,
                        }),
                        token,
                    );
                    result
                }
                _ => {
                    return Err(ExpressionLoweringError::Invalid(
                        "lowered expression shape differs from source",
                    )
                    .into());
                }
            };
            effects.insert(id, scoped);
            active.remove(&id);
            stack.pop();
            work.step()?;
        }
    }
    if let Some(nodes) = aggregate_nodes {
        crate::aggregate::prepare_aggregate_calls(
            package,
            functions,
            &effects,
            nodes,
            &mut tokens,
            work,
            creation,
        )?;
        crate::table_function::prepare_table_calls(
            package,
            functions,
            &effects,
            nodes,
            &mut tokens,
            work,
            creation,
        )?;
        crate::window::prepare_window_calls(
            package,
            functions,
            &effects,
            nodes,
            &mut tokens,
            work,
            creation,
        )?;
        crate::writer_statistics::prepare_writer_calls(
            package,
            functions,
            nodes,
            &mut tokens,
            work,
            creation,
        )?;
    }
    let mut result = Vec::with_capacity(tokens.len());
    for entry in tokens {
        result.push(entry);
        work.step()?;
    }
    Ok(result)
}

// Static constants retain the actual checked source owner through binding;
// no scalar metadata enum is reconstructed from an Arrow value.
fn literal_argument(
    source: &ExprNode,
    node: &StaticExprNode,
    control: &dyn PureCompileControl,
) -> Result<Option<ConstantValue>, ExpressionLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        if let StaticExprKind::Constant(value) = node.kind()
            && !source
                .ty
                .exactly_equals_observed::<ExpressionLoweringError>(value.value_type(), || {
                    work.step().map_err(ExpressionLoweringError::Control)
                })?
        {
            return Err(ExpressionLoweringError::Invalid(
                "constant full type differs from source",
            ));
        }
        match (&source.kind, node.kind()) {
            (ExprKind::Literal(_) | ExprKind::Constant(_), StaticExprKind::Constant(value)) => {
                Ok(Some(value.clone()))
            }
            // A resolved input is not a constant, even if this particular runtime
            // batch happens to broadcast one scalar value (notably RAND seeds).
            (ExprKind::Value(_), StaticExprKind::SlotId(_)) => Ok(None),
            (ExprKind::FunctionCall { .. }, StaticExprKind::BoundCall { .. }) => Ok(None),
            (ExprKind::Conjunction { .. }, StaticExprKind::NaryAnd { .. })
            | (ExprKind::Disjunction { .. }, StaticExprKind::NaryOr { .. }) => Ok(None),
            (
                ExprKind::Unary {
                    op: novarocks_physical_plan::UnaryOperator::Not,
                    ..
                },
                StaticExprKind::Not(_),
            )
            | (ExprKind::IsNull { negated: false, .. }, StaticExprKind::IsNull(_))
            | (ExprKind::IsNull { negated: true, .. }, StaticExprKind::IsNotNull(_)) => Ok(None),
            (
                ExprKind::Unary {
                    op: novarocks_physical_plan::UnaryOperator::Minus,
                    ..
                },
                StaticExprKind::PreparedNativeNegate(_),
            ) => Ok(None),
            (
                ExprKind::Unary {
                    op: novarocks_physical_plan::UnaryOperator::BitwiseNot,
                    ..
                },
                StaticExprKind::PreparedNativeBitNot(_),
            ) => Ok(None),
            (ExprKind::Between { .. }, StaticExprKind::PreparedBetween { .. }) => Ok(None),
            (ExprKind::InList { .. }, StaticExprKind::PreparedInList { .. }) => Ok(None),
            (ExprKind::Like { .. }, StaticExprKind::PreparedLike { .. }) => Ok(None),
            (ExprKind::Case { .. }, StaticExprKind::Case { .. }) => Ok(None),
            (ExprKind::Cast { .. }, StaticExprKind::PreparedCast { .. }) => Ok(None),
            (ExprKind::Binary { op, .. }, StaticExprKind::PreparedArithmetic { operator, .. })
                if arithmetic_operator(*op) == Some(*operator) =>
            {
                Ok(None)
            }
            (
                ExprKind::Binary {
                    op: novarocks_physical_plan::BinaryOperator::EqForNull,
                    ..
                },
                StaticExprKind::PreparedNullSafeComparison { .. },
            ) => Ok(None),
            (ExprKind::Binary { op, .. }, kind)
                if kind.ordinary_comparison().is_some()
                    && comparison_operator(*op)
                        == kind.ordinary_comparison().map(|parts| parts.0) =>
            {
                Ok(None)
            }
            _ => Err(ExpressionLoweringError::Invalid(
                "unsupported call argument projection",
            )),
        }
    })();
    finish(result, &mut work)
}

fn comparison_operator(
    operator: novarocks_physical_plan::BinaryOperator,
) -> Option<novarocks_functions::ComparisonOperator> {
    operator.comparison_operator()
}

#[cfg(test)]
#[path = "literal_metadata_tests.rs"]
mod literal_metadata_tests;

fn arithmetic_operator(
    operator: novarocks_physical_plan::BinaryOperator,
) -> Option<novarocks_type_contract::ArithmeticOperator> {
    operator.arithmetic_operator()
}

#[cfg(test)]
#[path = "checked_reference_tests.rs"]
pub(crate) mod checked_reference_tests;

#[cfg(test)]
mod primary_cause_footer_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_functions::KernelFailure;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct FooterControl(AtomicUsize);
    impl PureCompileControl for FooterControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err(CompileControlError::Cancelled)
            }
        }
    }
    fn refuses() -> Vec<(
        ExpressionLoweringError,
        novarocks_failure_contract::FailureFact,
    )> {
        fn original<E: FailureProjection + Into<ExpressionLoweringError>>(
            source: E,
        ) -> (
            ExpressionLoweringError,
            novarocks_failure_contract::FailureFact,
        ) {
            let expected = source.failure_fact();
            (source.into(), expected)
        }
        let allocation = Vec::<u64>::new().try_reserve_exact(usize::MAX).unwrap_err();
        let publication = novarocks_local_program::ProgramCreationFailure::<
            std::convert::Infallible,
            std::convert::Infallible,
        >::Capacity(allocation);
        let publication_fact = publication.failure_fact();
        let geometry =
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic;
        let mut cases = vec![
            (
                ExpressionLoweringError::Publication(publication),
                publication_fact,
            ),
            (
                ExpressionLoweringError::Construction(geometry),
                geometry.failure_fact(),
            ),
            original(ConstantReferenceError::Constant(ConstantError::Limit(
                "original constant extent",
            ))),
            original(novarocks_functions::ArithmeticPrepareError::Kernel(
                KernelFailure::ResourceExhausted,
            )),
            original(novarocks_functions::CastPrepareError::Kernel(
                KernelFailure::DeadlineExceeded,
            )),
            original(novarocks_functions::ComparisonPrepareError::Kernel(
                KernelFailure::Cancelled,
            )),
            original(FunctionSpecializationFailure::Kernel(
                KernelFailure::ResourceExhausted,
            )),
        ];
        use novarocks_type_contract::owned_resources::{
            arrow_metadata::hashmap_conversion_requests, btree::BTreeResourceError,
        };
        for source in [
            hashmap_conversion_requests(usize::MAX).unwrap_err(),
            BTreeResourceError::SourceModel("original metadata source model"),
        ] {
            let expected = source.failure_fact();
            cases.push(original(source));
            let binding = novarocks_functions::FunctionBindingCreationFailure::<
                std::convert::Infallible,
            >::Resource(
                novarocks_functions::FunctionBindingConstructionResource::BTree(source)
            );
            let binding: ExpressionCreationFailure<std::convert::Infallible> = binding.into();
            cases.push((binding.into_pure(), expected));
            let arithmetic = novarocks_functions::ArithmeticCreationFailure::<
                std::convert::Infallible,
            >::Construction(
                novarocks_type_contract::ValueConstructionFailure::BTree(source)
            );
            let arithmetic: ExpressionCreationFailure<std::convert::Infallible> = arithmetic.into();
            cases.push((arithmetic.into_pure(), expected));
        }
        cases
    }
    #[test]
    fn pure_and_created_expression_refusals_keep_source_before_later_footer_control() {
        for (original, expected) in refuses() {
            assert_eq!(original.failure_fact(), expected);
            assert!(
                !matches!(original, ExpressionLoweringError::Control(_)),
                "the original domain source must survive conversion"
            );
            assert!(std::error::Error::source(&original).is_some());
            let control = FooterControl(AtomicUsize::new(0));
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)
                    .unwrap();
            work.step().unwrap();
            let result: Result<(), _> = finish(Err(original), &mut work);
            assert_eq!(result.unwrap_err().failure_fact(), expected);
            assert_eq!(
                control.0.load(Ordering::SeqCst),
                1,
                "no footer callback after an owner's refusal"
            );
        }
        for (original, expected) in refuses() {
            assert_eq!(original.failure_fact(), expected);
            let control = FooterControl(AtomicUsize::new(0));
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)
                    .unwrap();
            work.step().unwrap();
            let error: crate::FragmentCreationError<std::convert::Infallible> = original.into();
            let result: Result<(), _> = crate::creation::finish_attempt(Err(error), work);
            assert_eq!(result.unwrap_err().failure_fact(), expected);
            assert_eq!(control.0.load(Ordering::SeqCst), 1);
        }
    }
    #[test]
    fn invariant_compile_interruption_survives_each_function_and_both_footer_paths() {
        use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
        let raw = LayoutResourceError::SourceModel;
        let expected = raw.failure_fact();
        let cause = CompileControlError::from_domain(raw);
        let converted: Vec<ExpressionLoweringError> = vec![
            cause.clone().into(),
            novarocks_functions::ArithmeticPrepareError::Kernel(KernelFailure::CompileControl(
                cause.clone(),
            ))
            .into(),
            novarocks_functions::CastPrepareError::Kernel(KernelFailure::CompileControl(
                cause.clone(),
            ))
            .into(),
            novarocks_functions::ComparisonPrepareError::Kernel(KernelFailure::CompileControl(
                cause.clone(),
            ))
            .into(),
            FunctionSpecializationFailure::Kernel(KernelFailure::CompileControl(cause.clone()))
                .into(),
            FunctionSpecializationFailure::Binding(FunctionBindingError::Control(cause.clone()))
                .into(),
        ];
        for original in converted {
            assert_eq!(original.failure_fact(), expected);
            let ExpressionLoweringError::Control(actual) = &original else {
                panic!("interruption role")
            };
            assert_eq!(actual, &cause);
            let control = FooterControl(AtomicUsize::new(0));
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)
                    .unwrap();
            work.step().unwrap();
            let result: Result<(), _> = finish(Err(original), &mut work);
            let original = result.unwrap_err();
            assert_eq!(original.failure_fact(), expected);
            assert_eq!(control.0.load(Ordering::SeqCst), 1);
            let control = FooterControl(AtomicUsize::new(0));
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)
                    .unwrap();
            work.step().unwrap();
            let created: crate::FragmentCreationError<std::convert::Infallible> = original.into();
            let result: Result<(), _> = crate::creation::finish_attempt(Err(created), work);
            let crate::FragmentCreationError::Control(actual) = result.unwrap_err() else {
                panic!("interruption role")
            };
            assert_eq!(actual, cause);
            assert_eq!(actual.failure_fact(), expected);
            assert_eq!(control.0.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn ordinary_expression_validation_still_observes_pending_footer() {
        let control = FooterControl(AtomicUsize::new(0));
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        work.step().unwrap();
        let result: Result<(), _> = finish(
            Err(ExpressionLoweringError::Invalid("ordinary validation")),
            &mut work,
        );
        assert!(matches!(
            result,
            Err(ExpressionLoweringError::Control(
                CompileControlError::Cancelled
            ))
        ));
        assert_eq!(control.0.load(Ordering::SeqCst), 2);
        let control = FooterControl(AtomicUsize::new(0));
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        work.step().unwrap();
        let original: crate::FragmentCreationError<std::convert::Infallible> =
            ExpressionLoweringError::Invalid("ordinary validation").into();
        let result: Result<(), _> = crate::creation::finish_attempt(Err(original), work);
        assert!(matches!(
            result,
            Err(crate::FragmentCreationError::Control(
                CompileControlError::Cancelled
            ))
        ));
        assert_eq!(control.0.load(Ordering::SeqCst), 2);
    }
}
