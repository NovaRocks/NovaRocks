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

//! Unit-local definition lowering and installed scalar preparation.
use crate::{
    CompiledExprId, ExpressionsCompileError, ImmutableExpressions, StaticExprKind, StaticExprNode,
};
use novarocks_constant_contract::ConstantTableError as ConstantReferenceError;
use novarocks_expression_contract::{BoundExprSet, ExprId, ExprKind, ExprNode};
use novarocks_functions::{
    CallEffectInput, ConstantError, ConstantValidation, ConstantValue, FunctionBindingError,
    FunctionBindingSelection, FunctionResultType, FunctionSpecializationFailure,
    PureCallPreparation, PureCallSpecialization, PureEngineFunctionCatalog,
    ScopedExpressionEffects,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompileControlError, CompilePhase, ControlShape,
    EffectContractError, ExpressionUseId, FunctionArgumentType, FunctionKind,
    MAX_CONTROL_DEFINITIONS, MAX_CONTROL_DEPTH, OperatorFunction, PureCompileControl,
    ValueTypeError,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    sync::Arc,
};
pub(crate) mod lambda;

pub(crate) struct LoweredExpressions {
    pub arena: Arc<ImmutableExpressions>,
    pub ids: BTreeMap<ExprId, CompiledExprId>,
    pub types: novarocks_type_contract::FunctionArgumentTypes,
    pub source_positions: Vec<Option<u32>>,
}
#[derive(Debug)]
pub enum ExprCompileError {
    Construction(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    BTree(novarocks_type_contract::owned_resources::btree::BTreeResourceError),
    Publication(crate::ProgramCreationFailure<std::convert::Infallible, std::convert::Infallible>),
    Control(CompileControlError),
    Flow(novarocks_type_contract::ExpressionControlFlowError),
    Primitive(crate::primitives::ProgramPrimitiveError),
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
    UnsupportedCall(ExpressionUseId),
    Invalid(&'static str),
}
impl ExprCompileError {
    /// Construction priority that cannot be inferred from the failure class.
    /// In addition to normal resource/control precedence, enclosing compilers
    /// return these refusals before an ordinary-error footer. In particular,
    /// a SourceModel invariant retains its original construction priority.
    pub fn is_primary_construction(&self) -> bool {
        matches!(self, Self::BTree(_))
    }

    pub fn is_unsupported(&self) -> bool {
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
impl fmt::Display for ExprCompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Construction(source) => source.fmt(f),
            Self::BTree(source) => source.fmt(f),
            Self::Publication(source) => source.fmt(f),
            Self::Control(e) => e.fmt(f),
            Self::Flow(e) => e.fmt(f),
            Self::Primitive(e) => e.fmt(f),
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
impl Error for ExprCompileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Construction(source) => Some(source),
            Self::BTree(source) => Some(source),
            Self::Publication(source) => Some(source),
            Self::Control(e) => Some(e),
            Self::Flow(e) => Some(e),
            Self::Primitive(e) => Some(e),
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
    for ExprCompileError
{
    fn from(source: novarocks_type_contract::owned_resources::btree::BTreeResourceError) -> Self {
        Self::BTree(source)
    }
}
impl From<novarocks_functions::ArithmeticPrepareError> for ExprCompileError {
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
impl From<novarocks_functions::CastPrepareError> for ExprCompileError {
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
impl From<novarocks_functions::ComparisonPrepareError> for ExprCompileError {
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
impl From<CompileControlError> for ExprCompileError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<ConstantError> for ExprCompileError {
    fn from(e: ConstantError) -> Self {
        match e {
            ConstantError::Control(e) => Self::Control(e),
            other => Self::Constant(other),
        }
    }
}
impl From<ConstantReferenceError> for ExprCompileError {
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
impl From<FunctionBindingError> for ExprCompileError {
    fn from(e: FunctionBindingError) -> Self {
        match e {
            FunctionBindingError::Control(e) => Self::Control(e),
            other => Self::Binding(other),
        }
    }
}
impl From<ValueTypeError> for ExprCompileError {
    fn from(e: ValueTypeError) -> Self {
        Self::Type(e)
    }
}
impl From<ExpressionsCompileError> for ExprCompileError {
    fn from(e: ExpressionsCompileError) -> Self {
        match e {
            ExpressionsCompileError::Control(e) => Self::Control(e),
            other => Self::Expressions(other),
        }
    }
}
impl From<EffectContractError> for ExprCompileError {
    fn from(e: EffectContractError) -> Self {
        Self::Effects(e)
    }
}
impl From<FunctionSpecializationFailure> for ExprCompileError {
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
    result: Result<T, ExprCompileError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<T, ExprCompileError> {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    // The refusal already happened inside its original owner. A later
    // footer observes pending work only after ordinary validation failures.
    if result.as_ref().is_err_and(|cause| {
        matches!(cause, ExprCompileError::Control(_))
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
fn case_arity(
    operand: Option<ExprId>,
    when_then: &[(ExprId, ExprId)],
    else_expr: Option<ExprId>,
) -> Result<usize, ExprCompileError> {
    when_then
        .len()
        .checked_mul(2)
        .and_then(|count| {
            count.checked_add(usize::from(operand.is_some()) + usize::from(else_expr.is_some()))
        })
        .filter(|count| {
            !when_then.is_empty() && *count <= novarocks_type_contract::MAX_CONTROL_USE_REFERENCES
        })
        .ok_or(ExprCompileError::Invalid(
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
pub enum ExprCompileFailure<H> {
    Control(CompileControlError),
    Creator(H),
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    Owner(ExprCompileError),
}
impl<H> From<CompileControlError> for ExprCompileFailure<H> {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl<H> From<ExprCompileError> for ExprCompileFailure<H> {
    fn from(e: ExprCompileError) -> Self {
        match e {
            ExprCompileError::Control(c) => Self::Control(c),
            e => Self::Owner(e),
        }
    }
}
macro_rules! expression_creation_cause {
    ($t:ty) => {
        impl<H> From<$t> for ExprCompileFailure<H> {
            fn from(e: $t) -> Self {
                ExprCompileError::from(e).into()
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
impl<H> From<crate::ProgramCreationFailure<ExprCompileError, H>> for ExprCompileFailure<H> {
    fn from(error: crate::ProgramCreationFailure<ExprCompileError, H>) -> Self {
        use crate::ProgramCreationFailure as Failure;
        match error {
            Failure::Control(cause) => Self::Control(cause),
            Failure::Creator(cause) => Self::Creator(cause),
            Failure::Resource(cause) => Self::Resource(cause),
            Failure::Capacity(cause) => {
                Self::Owner(ExprCompileError::Publication(Failure::Capacity(cause)))
            }
            Failure::Owner(cause) => cause.into(),
        }
    }
}
impl<H> From<EffectContractError> for ExprCompileFailure<H> {
    fn from(error: EffectContractError) -> Self {
        ExprCompileError::from(error).into()
    }
}
impl<H> From<FunctionBindingError> for ExprCompileFailure<H> {
    fn from(error: FunctionBindingError) -> Self {
        ExprCompileError::from(error).into()
    }
}
impl<H> From<FunctionSpecializationFailure> for ExprCompileFailure<H> {
    fn from(error: FunctionSpecializationFailure) -> Self {
        ExprCompileError::from(error).into()
    }
}
impl<H> From<novarocks_functions::FunctionBindingCreationFailure<H>> for ExprCompileFailure<H> {
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
            F::UnconvertedResolverOwner => Self::Owner(ExprCompileError::Invalid(
                "binding construction owner is not converted",
            )),
        }
    }
}
impl<H> From<novarocks_functions::CheckedCallPreparationFailure<H>> for ExprCompileFailure<H> {
    fn from(error: novarocks_functions::CheckedCallPreparationFailure<H>) -> Self {
        use novarocks_functions::{CheckedCallPreparationFailure as F, CheckedOwnerFailure as O};
        match error {
            F::Binding(cause) => cause.into(),
            F::Preparation(O::Original(cause)) => cause.into(),
            F::Preparation(O::UnconvertedOwner) => Self::Owner(ExprCompileError::Invalid(
                "installed preparation owner is not converted",
            )),
        }
    }
}

pub(crate) type CallCreation<'a, F> = Option<(&'a F, &'a ConstantValidation)>;
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
) -> Result<(), ExprCompileFailure<H>> {
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
) -> Result<PureCallSpecialization, ExprCompileFailure<H>> {
    match creation {
        Some((factory, _)) => {
            // The definition was fully validated by this immutable compiler's
            // first pass. Expected types in this preparation get a separate
            // temporary owner; they never enlarge a published type invoice.
            let mut scope = factory.begin_scope().map_err(ExprCompileFailure::Creator)?;
            catalog
                .prepare_frozen_with_creator_in(
                    input, selected, frozen, options, &mut scope, control,
                )
                .map_err(Into::into)
        }
        None => catalog
            .prepare_frozen(input, selected, frozen, options, control)
            .map_err(Into::into),
    }
}
impl ExprCompileFailure<std::convert::Infallible> {
    pub(crate) fn into_pure(self) -> ExprCompileError {
        match self {
            Self::Control(c) => ExprCompileError::Control(c),
            Self::Creator(never) => match never {},
            Self::Resource(source) => ExprCompileError::Construction(source),
            Self::Owner(e) => e,
        }
    }
}
fn observe_type_layout<H>(
    layout: std::alloc::Layout,
    creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ExprCompileFailure<H>> {
    if let Some(c) = creation {
        work.flush()?;
        c.request_layout(layout)
            .map_err(ExprCompileFailure::Creator)?;
    }
    Ok(())
}
fn observe_type_push<H>(
    types: &Vec<FunctionArgumentType>,
    creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ExprCompileFailure<H>> {
    if creation.is_none() {
        return Ok(());
    }
    let facts =
        novarocks_type_contract::owned_resources::vec::push_growth::<FunctionArgumentType>(types)
            .map_err(|e| {
            match e {
        novarocks_type_contract::ControlResourceError::Control(c) => {
            ExprCompileFailure::Control(c)
        }
        novarocks_type_contract::ControlResourceError::SourceModel(_) => {
            ExprCompileFailure::Resource(
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
pub(crate) fn lower_core<H>(
    unit: &BoundExprSet,
    environment: crate::EvaluationEnvironment<'_>,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
    mut creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
) -> Result<LoweredExpressions, ExprCompileFailure<H>> {
    let source = unit;
    let count = unit.definitions().len();
    if count > MAX_CONTROL_DEFINITIONS {
        return Err(ExprCompileError::Invalid("expression definition limit exceeded").into());
    }
    let mut ids: BTreeMap<ExprId, CompiledExprId> = BTreeMap::new();
    let mut nodes: Vec<StaticExprNode> = Vec::new();
    nodes
        .try_reserve_exact(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    let mut types = Vec::new();
    let mut active = BTreeSet::new();
    // At most one frame per dependency depth. Child lists are borrowed and
    // walked one edge at a time, rather than cloned or enqueued wholesale.
    let mut stack = Vec::new();
    let expansions = source.control_expansions();
    let expanded = !expansions.records().is_empty();
    let mut source_positions = Vec::new();
    if expanded {
        source_positions
            .try_reserve_exact(count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
    }
    for definition in source.definitions() {
        let root = definition.id;
        if ids.contains_key(&root) {
            work.step()?;
            continue;
        }
        active.insert(root);
        stack.push((root, 0usize));
        let mut source_depth = 1usize;
        work.step()?;
        while let Some(&(id, next)) = stack.last() {
            let node = source
                .definition(id)
                .ok_or(ExprCompileError::Invalid("missing physical expression"))?;
            if node.id != id {
                return Err(
                    ExprCompileError::Invalid("physical expression key differs from ID").into(),
                );
            }
            let child = match &node.kind {
                ExprKind::Constant(_) | ExprKind::Value(_) | ExprKind::LambdaParameter { .. } => {
                    None
                }
                ExprKind::Lambda { body, .. } => (next == 0).then_some(*body),
                ExprKind::Let { value, body } => [*value, *body].get(next).copied(),
                ExprKind::FunctionCall { args, .. }
                | ExprKind::Conjunction { args }
                | ExprKind::Disjunction { args } => args.get(next).copied(),
                ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr, .. } => {
                    (next == 0).then_some(*expr)
                }

                ExprKind::Case {
                    operand,
                    when_then,
                    else_expr,
                    ..
                } => case_child(*operand, when_then, *else_expr, next),
            };
            if let Some(child) = child {
                let frame = stack
                    .last_mut()
                    .ok_or(ExprCompileError::Invalid("missing dependency frame"))?;
                frame.1 += 1;
                if !ids.contains_key(&child) {
                    if !active.insert(child) {
                        return Err(ExprCompileError::Invalid("cyclic physical expression").into());
                    }
                    source_depth += usize::from(!expansions.shares_source_position(id, child));
                    if source_depth > MAX_CONTROL_DEPTH
                        || stack.len() >= novarocks_type_contract::MAX_EXPANDED_CONTROL_DEPTH
                    {
                        return Err(ExprCompileError::Invalid(
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
                ExprKind::Value(novarocks_expression_contract::ValueAddress::Input(_)) => {
                    StaticExprKind::Input
                }
                ExprKind::Value(novarocks_expression_contract::ValueAddress::Local(binder)) => {
                    StaticExprKind::LocalValue { binder: *binder }
                }
                ExprKind::Let { value, body } => StaticExprKind::Let {
                    binder: node.id,
                    value: *ids
                        .get(value)
                        .ok_or(ExprCompileError::Invalid("Let initializer was not lowered"))?,
                    body: *ids
                        .get(body)
                        .ok_or(ExprCompileError::Invalid("Let body was not lowered"))?,
                },
                ExprKind::Lambda { body, .. } => StaticExprKind::Lambda {
                    body: *ids
                        .get(body)
                        .ok_or(ExprCompileError::Invalid("lambda body was not lowered"))?,
                },
                ExprKind::LambdaParameter { lambda, ordinal } => StaticExprKind::LambdaParameter {
                    lambda: *lambda,
                    ordinal: *ordinal,
                },
                ExprKind::Constant(reference) => StaticExprKind::Constant(
                    environment
                        .constants
                        .resolve_observed(*reference, &node.ty, work)?,
                ),
                ExprKind::Conjunction { args } | ExprKind::Disjunction { args } => {
                    let valid =
                        novarocks_expression_contract::boolean_connective_nullability_observed(
                            node,
                            |id| source.definition(id),
                            |operand, _| {
                                let local = ids.get(&operand.id).ok_or(
                                    ExprCompileError::Invalid("Boolean operand was not lowered"),
                                )?;
                                let StaticExprKind::Constant(constant) =
                                    nodes[local.index()].kind()
                                else {
                                    return Err(ExprCompileError::Invalid(
                                        "Boolean source has no original lowered constant",
                                    ));
                                };
                                constant.try_boolean().map_err(ExprCompileError::from)
                            },
                            work,
                        )?;
                    if !valid {
                        return Err(ExprCompileError::Invalid("Boolean connective result discards nullable operands without a deciding constant").into());
                    }
                    let mut local_args = Vec::with_capacity(args.len());
                    for child in args {
                        local_args.push(
                            *ids.get(child).ok_or(ExprCompileError::Invalid(
                                "Boolean operand was not lowered",
                            ))?,
                        );
                        work.step()?;
                    }
                    if matches!(node.kind, ExprKind::Conjunction { .. }) {
                        StaticExprKind::NaryAnd { args: local_args }
                    } else {
                        StaticExprKind::NaryOr { args: local_args }
                    }
                }
                ExprKind::IsNull { expr, negated } => {
                    let child = *ids.get(expr).ok_or(ExprCompileError::Invalid(
                        "null test operand was not lowered",
                    ))?;
                    if *negated {
                        StaticExprKind::IsNotNull(child)
                    } else {
                        StaticExprKind::IsNull(child)
                    }
                }
                ExprKind::Cast {
                    expr,
                    target,
                    cast_mode,
                } => {
                    let child = *ids
                        .get(expr)
                        .ok_or(ExprCompileError::Invalid("cast operand was not lowered"))?;
                    if !novarocks_type_contract::arrow_data_types_exact_with_profile_observed::<
                        ExprCompileError,
                    >(
                        target,
                        node.ty.data_type(),
                        novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
                        || work.step().map_err(ExprCompileError::Control),
                    )? {
                        return Err(
                            ExprCompileError::Invalid("cast target differs from result").into()
                        );
                    }
                    work.flush()?;
                    novarocks_functions::PreparedCastRecipe::try_new_trino(
                        &source
                            .definition(*expr)
                            .ok_or(ExprCompileError::Invalid("missing cast source"))?
                            .ty,
                        &node.ty,
                        *cast_mode,
                        control,
                    )?;
                    StaticExprKind::PreparedCast {
                        operation: novarocks_functions::CastOperation::Carrier,
                        child,
                        cast_mode: *cast_mode,
                    }
                }

                ExprKind::Case {
                    ieee_float_equality,
                    operand,
                    when_then,
                    else_expr,
                } => {
                    let count = case_arity(*operand, when_then, *else_expr)?;
                    let mut children = Vec::with_capacity(count);
                    for ordinal in 0..count {
                        let child = case_child(*operand, when_then, *else_expr, ordinal)
                            .ok_or(ExprCompileError::Invalid("missing actual CASE child"))?;
                        children.push(
                            *ids.get(&child)
                                .ok_or(ExprCompileError::Invalid("CASE child was not lowered"))?,
                        );
                        work.step()?;
                    }
                    StaticExprKind::Case {
                        ieee_float_equality: *ieee_float_equality,
                        has_case_expr: operand.is_some(),
                        has_else_expr: else_expr.is_some(),
                        children,
                    }
                }
                ExprKind::FunctionCall { args, .. } => {
                    let mut local_args = Vec::with_capacity(args.len());
                    for child in args {
                        local_args.push(*ids.get(child).ok_or(ExprCompileError::Invalid(
                            "expression dependency was not lowered",
                        ))?);
                        work.step()?;
                    }
                    StaticExprKind::BoundCall { args: local_args }
                }
            };
            work.flush()?;
            let ty = node.ty.clone_observed(|| work.step())?;
            nodes.push(StaticExprNode::new_value_typed(
                kind,
                ty.clone_observed(|| work.step())?,
            ));
            observe_type_push(&types, crate::creation::borrow_scope(&mut creation), work)?;
            let argument_type = match &node.kind {
                ExprKind::Lambda {
                    parameter_types, ..
                } => {
                    let mut owner = crate::creation::ProgramCreation {
                        creator: crate::creation::borrow_scope(&mut creation),
                    };
                    let mut parameters =
                        owner.reserve::<ExprCompileError, _>(parameter_types.len(), work)?;
                    for parameter in parameter_types {
                        parameters.push(parameter.clone_observed(|| work.step())?);
                    }
                    FunctionArgumentType::Lambda {
                        parameter_types: crate::creation::publish_argument_types(
                            parameters,
                            crate::creation::borrow_scope(&mut creation),
                            work,
                        )
                        .map_err(|e| e.map_owner(|never| match never {}))?,
                        result_type: ty,
                    }
                }
                _ => FunctionArgumentType::Value(ty),
            };
            types.push(argument_type);
            if expanded {
                source_positions.push(expansions.source_position(id).map(ExprId::get));
            }
            work.flush()?;
            ids.insert(id, CompiledExprId::new(nodes.len() - 1));
            active.remove(&id);
            stack.pop();
            if let Some(&(parent, _)) = stack.last() {
                source_depth -= usize::from(!expansions.shares_source_position(parent, id));
            }
            work.step()?;
        }
    }
    work.flush()?;
    // No legacy exception or session-timezone capability is authored here.
    // Frozen semantic parameters remain on exact prepared call contracts.
    let storage = creation
        .as_ref()
        .map(|owner| owner.storage_lifetime())
        .unwrap_or_else(novarocks_type_contract::ValueTypeStorageLifetime::pure);
    let arena = Arc::new(
        ImmutableExpressions::try_new_for_lowering(nodes, &source_positions, control)?
            .with_storage_lifetime(storage),
    );
    work.flush()?;
    let types = crate::creation::publish_argument_types(
        types,
        crate::creation::borrow_scope(&mut creation),
        work,
    )
    .map_err(|e| {
        use crate::ProgramCreationFailure as Failure;
        match e {
            Failure::Control(c) => ExprCompileFailure::Control(c),
            Failure::Creator(c) => ExprCompileFailure::Creator(c),
            Failure::Resource(c) => ExprCompileFailure::Resource(c),
            Failure::Capacity(source) => ExprCompileFailure::Owner(ExprCompileError::Publication(
                crate::ProgramCreationFailure::Capacity(source),
            )),
            Failure::Owner(never) => match never {},
        }
    })?;
    Ok(LoweredExpressions {
        arena,
        ids,
        types,
        source_positions,
    })
}

pub(crate) type PreparedUnitCalls = (
    Vec<(ExpressionUseId, PureCallSpecialization)>,
    BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    lambda::CompiledLambdas,
);
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_core<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    unit: &BoundExprSet,
    environment: crate::EvaluationEnvironment<'_>,
    lowered: &LoweredExpressions,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
    creation: CallCreation<'_, F>,
    temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    retained: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
) -> Result<PreparedUnitCalls, ExprCompileFailure<H>> {
    if lowered.ids.len() != unit.definitions().len()
        || lowered.types.len() != lowered.arena.nodes().len()
    {
        return Err(ExprCompileError::Invalid("lowered definition coverage differs").into());
    }
    let mut local_ids = BTreeSet::new();
    for definition in unit.definitions() {
        let physical = definition.id;
        let local_id = *lowered
            .ids
            .get(&physical)
            .ok_or(ExprCompileError::Invalid("missing lowered definition"))?;
        if !local_ids.insert(local_id) {
            return Err(ExprCompileError::Invalid("distinct definitions share a local ID").into());
        }
        let Some(local_type) = lowered.types.get(local_id.index()) else {
            return Err(ExprCompileError::Invalid("missing complete local value type").into());
        };
        let ty = match (&definition.kind, local_type) {
            (
                ExprKind::Lambda {
                    parameter_types, ..
                },
                FunctionArgumentType::Lambda {
                    parameter_types: actual,
                    result_type,
                },
            ) => {
                if parameter_types.len() != actual.len() {
                    return Err(ExprCompileError::Invalid("lowered lambda arity differs").into());
                }
                for (expected, found) in parameter_types.iter().zip(actual) {
                    if !expected.exactly_equals_observed(found, || {
                        work.step().map_err(ExprCompileError::from)
                    })? {
                        return Err(ExprCompileError::Invalid(
                            "lowered lambda parameter type differs",
                        )
                        .into());
                    }
                }
                result_type
            }
            (kind, FunctionArgumentType::Value(ty)) if !matches!(kind, ExprKind::Lambda { .. }) => {
                ty
            }
            _ => {
                return Err(
                    ExprCompileError::Invalid("lowered lambda and value channels differ").into(),
                );
            }
        };
        // The shared walker observes borrowed work before its operations.
        // These are cancellation observations, not fabricated completed units.
        work.flush()?;
        let same = definition
            .ty
            .exactly_equals_observed::<ExprCompileError>(ty, || {
                control
                    .checkpoint(CompilePhase::FunctionSpecialization, 0)
                    .map_err(ExprCompileError::Control)
            })?;
        work.flush()?;
        if !same {
            return Err(ExprCompileError::Invalid("lowered full type differs from source").into());
        }
        if let ExprKind::Constant(reference) = &definition.kind {
            let node = lowered
                .arena
                .node(local_id)
                .ok_or(ExprCompileError::Invalid(
                    "missing lowered constant definition",
                ))?;
            let StaticExprKind::Constant(value) = node.kind() else {
                return Err(ExprCompileError::Invalid(
                    "constant reference lost its checked source",
                )
                .into());
            };
            let source =
                environment
                    .constants
                    .resolve_observed(*reference, &definition.ty, work)?;
            work.flush()?;
            let same_type = definition.ty.exactly_equals_observed::<ExprCompileError>(
                value.value_type(),
                || {
                    control
                        .checkpoint(CompilePhase::FunctionSpecialization, 0)
                        .map_err(ExprCompileError::Control)
                },
            )?;
            work.flush()?;
            // This is exact source retention, not semantic selected-value
            // equality: a private mapping cannot substitute another backing.
            if !same_type
                || value.pool().backing_identity() != source.pool().backing_identity()
                || value.ordinal() != source.ordinal()
            {
                return Err(ConstantReferenceError::Constant(ConstantError::Invalid(
                    "lowered constant reference differs from its checked source",
                ))
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
    for definition in unit.definitions() {
        let id = definition.id;
        if let ExprKind::FunctionCall { function, args } = &definition.kind {
            let group_type = match &*function.argument_types {
                [
                    FunctionArgumentType::Value(_),
                    FunctionArgumentType::CandidateGroup { element_type },
                ] if function.function_id.as_str()
                    == OperatorFunction::InList.function_id_text() =>
                {
                    Some(element_type)
                }
                _ => None,
            };
            if group_type.is_none() && args.len() > novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            if function.kind != FunctionKind::Scalar {
                return Err(ExprCompileError::UnsupportedExpression(id).into());
            }
            let local_id = *lowered
                .ids
                .get(&id)
                .ok_or(ExprCompileError::Invalid("missing local call definition"))?;
            let Some(StaticExprKind::BoundCall { args: local_args }) =
                lowered.arena.node(local_id).map(StaticExprNode::kind)
            else {
                return Err(ExprCompileError::Invalid("lowered call shape differs").into());
            };
            let matches_arity = if group_type.is_some() {
                args.len() >= 2
            } else {
                args.len() == function.argument_types.len()
            };
            if args.len() != local_args.len() || !matches_arity {
                return Err(ExprCompileError::Invalid("static call arity differs").into());
            }
            work.flush()?;
            let result_matches = function
                .result_type
                .exactly_equals_observed::<ExprCompileError>(&definition.ty, || {
                    control
                        .checkpoint(CompilePhase::FunctionSpecialization, 0)
                        .map_err(ExprCompileError::Control)
                })?;
            work.flush()?;
            if !result_matches {
                return Err(ExprCompileError::Invalid(
                    "static call result differs from its definition",
                )
                .into());
            }
            for (ordinal, child) in args.iter().enumerate() {
                let child_id = *lowered
                    .ids
                    .get(child)
                    .ok_or(ExprCompileError::Invalid("missing static call argument"))?;
                if local_args[ordinal] != child_id {
                    return Err(ExprCompileError::Invalid("ordered static arguments differ").into());
                }
                let child_source = unit.definition(*child).ok_or(ExprCompileError::Invalid(
                    "missing physical static argument",
                ))?;
                let child_node = lowered
                    .arena
                    .node(child_id)
                    .ok_or(ExprCompileError::Invalid("missing local static argument"))?;
                let argument_type =
                    lowered
                        .types
                        .get(child_id.index())
                        .ok_or(ExprCompileError::Invalid(
                            "missing lowered static argument type",
                        ))?;
                let matches_type = if ordinal > 0
                    && let Some(element_type) = group_type
                {
                    match argument_type {
                        FunctionArgumentType::Value(actual) => actual
                            .same_physical_binding_ignoring_root_nullability_observed(
                                element_type,
                                || work.step().map_err(ExprCompileError::from),
                            )?,
                        _ => false,
                    }
                } else if matches!(argument_type, FunctionArgumentType::Lambda { .. }) {
                    argument_types_equal(argument_type, &function.argument_types[ordinal], work)?
                } else {
                    true
                };
                if !matches_type {
                    return Err(
                        ExprCompileError::Invalid("lowered static argument type differs").into(),
                    );
                }
                work.flush()?;
                // Check the independent physical/local projection. It cannot
                // supply the original binding request's constant channel.
                literal_argument(child_source, child_node, control)?;
                work.flush()?;
                work.step()?;
            }
            let source_request = unit.requests().get(&id);
            work.step()?;
            let source_request = source_request.ok_or(ExprCompileError::Invalid(
                "missing original static call request",
            ))?;
            let request = crate::original_requests::materialize_call_request_created(
                source_request,
                environment.constants,
                work,
                creation,
            )?;
            work.flush()?;
            let selection = crate::call_authorship::scalar_binding_selection(function);
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
    let flow = unit.control_flow();
    let use_depth_limit = flow.physical_use_depth().max(MAX_CONTROL_DEPTH);
    let mut effects = BTreeMap::<ExpressionUseId, ScopedExpressionEffects>::new();
    let mut tokens = BTreeMap::new();
    let mut lambdas = lambda::CompiledLambdas::default();
    let mut lexical_references = lambda::LexicalReferences::default();
    let mut temporary = crate::creation::ProgramCreation { creator: temporary };
    let mut retained = crate::creation::ProgramCreation { creator: retained };
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
                .ok_or(ExprCompileError::Invalid("missing expression occurrence"))?;
            if let Some(&child) = invocation.arguments.get(next) {
                let frame = stack
                    .last_mut()
                    .ok_or(ExprCompileError::Invalid("missing invocation frame"))?;
                frame.1 += 1;
                if !effects.contains_key(&child) {
                    if !active.insert(child) {
                        return Err(
                            ExprCompileError::Invalid("cyclic expression occurrence").into()
                        );
                    }
                    if stack.len() >= use_depth_limit {
                        return Err(ExprCompileError::Invalid(
                            "expression occurrence depth exceeded",
                        )
                        .into());
                    }
                    stack.push((child, 0));
                }
                work.step()?;
                continue;
            }
            let source = unit
                .definition(invocation.definition)
                .ok_or(ExprCompileError::Invalid("missing invocation definition"))?;
            let local_id = *lowered
                .ids
                .get(&source.id)
                .ok_or(ExprCompileError::Invalid(
                    "invocation definition was not lowered",
                ))?;
            let local = lowered
                .arena
                .node(local_id)
                .ok_or(ExprCompileError::Invalid("missing local definition"))?;
            let scoped = match (&source.kind, local.kind()) {
                (
                    ExprKind::Value(novarocks_expression_contract::ValueAddress::Input(_)),
                    StaticExprKind::Input,
                ) => {
                    if invocation.control != ControlShape::Eager || !invocation.arguments.is_empty()
                    {
                        return Err(ExprCompileError::Invalid(
                            "input occurrence has arguments or control",
                        )
                        .into());
                    }
                    ScopedExpressionEffects::pure_value(invocation.context)
                }
                (
                    ExprKind::Value(novarocks_expression_contract::ValueAddress::Local(original)),
                    StaticExprKind::LocalValue { binder },
                ) if original == binder => {
                    if invocation.control != ControlShape::Eager || !invocation.arguments.is_empty()
                    {
                        return Err(ExprCompileError::Invalid(
                            "local value occurrence has arguments or control",
                        )
                        .into());
                    }
                    ScopedExpressionEffects::pure_value(invocation.context)
                }
                (
                    ExprKind::Let { value, body },
                    StaticExprKind::Let {
                        binder,
                        value: lowered_value,
                        body: lowered_body,
                    },
                ) => {
                    let [value_use, body_use] = invocation.arguments.as_ref() else {
                        return Err(ExprCompileError::Invalid(
                            "Let must have exactly one initializer and body use",
                        )
                        .into());
                    };
                    if invocation.control != ControlShape::Let
                        || *binder != source.id
                        || flow
                            .uses()
                            .get(value_use)
                            .is_none_or(|actual| actual.definition != *value)
                        || flow
                            .uses()
                            .get(body_use)
                            .is_none_or(|actual| actual.definition != *body)
                        || lowered.ids.get(value) != Some(lowered_value)
                        || lowered.ids.get(body) != Some(lowered_body)
                    {
                        return Err(ExprCompileError::Invalid(
                            "Let occurrence differs from its original binding",
                        )
                        .into());
                    }
                    let mut scoped = ScopedExpressionEffects::pure_value(invocation.context);
                    for (ordinal, use_id) in [value_use, body_use].into_iter().enumerate() {
                        let child = *effects.get(use_id).ok_or(ExprCompileError::Invalid(
                            "Let child does not have an effect summary",
                        ))?;
                        scoped = scoped.join_control_argument(child, flow, ordinal)?;
                    }
                    scoped
                }
                (
                    ExprKind::LambdaParameter { lambda, ordinal },
                    StaticExprKind::LambdaParameter {
                        lambda: lowered_lambda,
                        ordinal: lowered_ordinal,
                    },
                ) => {
                    if lambda != lowered_lambda
                        || ordinal != lowered_ordinal
                        || invocation.control != ControlShape::Eager
                        || !invocation.arguments.is_empty()
                    {
                        return Err(ExprCompileError::Invalid(
                            "lambda parameter lost its original lexical source",
                        )
                        .into());
                    }
                    ScopedExpressionEffects::pure_value(invocation.context)
                }
                (ExprKind::Lambda { body, .. }, StaticExprKind::Lambda { body: lowered_body }) => {
                    let [body_use] = invocation.arguments.as_ref() else {
                        return Err(ExprCompileError::Invalid(
                            "lambda wrapper must have one body use",
                        )
                        .into());
                    };
                    if invocation.control != ControlShape::LambdaBody
                        || flow
                            .uses()
                            .get(body_use)
                            .is_none_or(|actual| actual.definition != *body)
                        || lowered.ids.get(body) != Some(lowered_body)
                    {
                        return Err(ExprCompileError::Invalid(
                            "lambda wrapper lost its original body",
                        )
                        .into());
                    }
                    let child_effects = *effects.get(body_use).ok_or(ExprCompileError::Invalid(
                        "lambda body does not have an effect summary",
                    ))?;
                    ScopedExpressionEffects::pure_value(invocation.context).join_control_argument(
                        child_effects,
                        flow,
                        0,
                    )?
                }
                (ExprKind::Constant(_), StaticExprKind::Constant(_)) => {
                    if invocation.control != ControlShape::Eager || !invocation.arguments.is_empty()
                    {
                        return Err(ExprCompileError::Invalid(
                            "literal occurrence has arguments or control",
                        )
                        .into());
                    }
                    ScopedExpressionEffects::pure_value(invocation.context)
                }
                (
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
                        return Err(ExprCompileError::Invalid(
                            "actual unary control or operand differs",
                        )
                        .into());
                    }
                    let child = invocation.arguments[0];
                    if flow.uses()[&child].definition != *expr {
                        return Err(ExprCompileError::Invalid(
                            "actual unary operand occurrence differs",
                        )
                        .into());
                    }
                    ScopedExpressionEffects::pure_value(invocation.context).join_control_argument(
                        *effects.get(&child).ok_or(ExprCompileError::Invalid(
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
                        cast_mode,
                    },
                    StaticExprKind::PreparedCast {
                        operation,
                        child,
                        cast_mode: local_cast_mode,
                    },
                ) => {
                    if *operation != novarocks_functions::CastOperation::Carrier
                        || cast_mode != local_cast_mode
                        || invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 1
                        || lowered.ids.get(expr) != Some(child)
                        || flow.uses()[&invocation.arguments[0]].definition != *expr
                    {
                        return Err(ExprCompileError::Invalid(
                            "cast occurrence differs from its frozen source",
                        )
                        .into());
                    }
                    if !novarocks_type_contract::arrow_data_types_exact_with_profile_observed::<
                        ExprCompileError,
                    >(
                        target,
                        source.ty.data_type(),
                        novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
                        || work.step().map_err(ExprCompileError::Control),
                    )? {
                        return Err(ExprCompileError::Invalid(
                            "cast result differs from frozen target",
                        )
                        .into());
                    }
                    work.flush()?;
                    let recipe = novarocks_functions::PreparedCastRecipe::try_new_trino(
                        &unit
                            .definition(*expr)
                            .ok_or(ExprCompileError::Invalid("missing cast operand source"))?
                            .ty,
                        &source.ty,
                        *local_cast_mode,
                        control,
                    )?;
                    recipe
                        .own_effects(invocation.context)
                        .join_control_argument(
                            *effects.get(&invocation.arguments[0]).ok_or(
                                ExprCompileError::Invalid("cast child effects were not prepared"),
                            )?,
                            flow,
                            0,
                        )?
                }

                (
                    ExprKind::Case {
                        ieee_float_equality,
                        operand,
                        when_then,
                        else_expr,
                    },
                    StaticExprKind::Case {
                        ieee_float_equality: compiled_ieee_equality,
                        has_case_expr,
                        has_else_expr,
                        children,
                    },
                ) => {
                    let count = case_arity(*operand, when_then, *else_expr)?;
                    let expected = ControlShape::Case {
                        simple: operand.is_some(),
                        arms: u32::try_from(when_then.len()).map_err(|_| {
                            ExprCompileError::Invalid("CASE arm count is not representable")
                        })?,
                        has_else: else_expr.is_some(),
                    };
                    if invocation.control != expected
                        || *has_case_expr != operand.is_some()
                        || ieee_float_equality != compiled_ieee_equality
                        || *has_else_expr != else_expr.is_some()
                        || children.len() != count
                        || invocation.arguments.len() != count
                    {
                        return Err(ExprCompileError::Invalid(
                            "actual CASE control or children differ",
                        )
                        .into());
                    }
                    let value_type = |id| {
                        unit.definition(id)
                            .map(|node| &node.ty)
                            .ok_or(ExprCompileError::Invalid("missing original CASE source"))
                    };
                    let mut combined = crate::primitives::case_own_effects(
                        operand.map(value_type).transpose()?,
                        when_then.iter().map(|(label, _)| value_type(*label)),
                        invocation.context,
                        work,
                        ExprCompileError::from,
                    )?;
                    for (ordinal, local_child) in children.iter().enumerate() {
                        let source_child = case_child(*operand, when_then, *else_expr, ordinal)
                            .ok_or(ExprCompileError::Invalid("missing actual CASE operand"))?;
                        let child_use = invocation.arguments[ordinal];
                        if lowered.ids.get(&source_child) != Some(local_child)
                            || flow.uses()[&child_use].definition != source_child
                        {
                            return Err(ExprCompileError::Invalid(
                                "ordered CASE occurrence differs",
                            )
                            .into());
                        }
                        combined = combined.join_control_argument(
                            *effects.get(&child_use).ok_or(ExprCompileError::Invalid(
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
                        return Err(ExprCompileError::Invalid(
                            "actual Boolean connective control or arity differs",
                        )
                        .into());
                    }
                    let mut combined = ScopedExpressionEffects::pure_value(invocation.context);
                    for (ordinal, &child) in args.iter().enumerate() {
                        let child_use = invocation.arguments[ordinal];
                        if flow.uses()[&child_use].definition != child {
                            return Err(ExprCompileError::Invalid(
                                "ordered Boolean operand occurrences differ",
                            )
                            .into());
                        }
                        combined = combined.join_control_argument(
                            *effects.get(&child_use).ok_or(ExprCompileError::Invalid(
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
                    let site = id;
                    if function.kind != FunctionKind::Scalar {
                        return Err(ExprCompileError::UnsupportedCall(site).into());
                    }
                    let frozen = unit
                        .claims()
                        .get(&site)
                        .ok_or(ExprCompileError::Invalid("missing frozen expression call"))?;
                    if frozen.context != invocation.context {
                        return Err(
                            ExprCompileError::Invalid("frozen occurrence context differs").into(),
                        );
                    }
                    let (type_only, control_intrinsic, body_ordinal) =
                        match (invocation.control, frozen.effects.argument_control) {
                            (ControlShape::Eager, ArgumentControl::Eager)
                            | (ControlShape::OrderedNull, ArgumentControl::OrderedNull) => {
                                (false, false, None)
                            }
                            (ControlShape::TypeOnly, ArgumentControl::TypeOnly) => {
                                (true, false, None)
                            }
                            (ControlShape::NoArguments, ArgumentControl::NoArguments) => {
                                (true, false, None)
                            }
                            (ControlShape::If, ArgumentControl::If)
                            | (ControlShape::Coalesce, ArgumentControl::Coalesce)
                            | (ControlShape::Try, ArgumentControl::Try)
                            | (ControlShape::NullIf, ArgumentControl::NullIf)
                            | (ControlShape::OrderedRegexp, ArgumentControl::OrderedRegexp)
                            | (ControlShape::CandidateGroup, ArgumentControl::CandidateGroup) => {
                                (false, true, None)
                            }
                            (
                                ControlShape::TemporalSource(shape),
                                ArgumentControl::TemporalSource(kind),
                            ) if shape.kind() == kind => (false, true, None),
                            (shape @ ControlShape::HigherOrder { body_ordinal, .. }, control)
                                if control.matches_scalar_shape(shape) =>
                            {
                                (false, false, Some(body_ordinal as usize))
                            }
                            _ => return Err(ExprCompileError::UnsupportedCall(site).into()),
                        };
                    let candidate_group = invocation.control == ControlShape::CandidateGroup;
                    let formal_count = if candidate_group { 2 } else { args.len() };
                    if args.len() != local_args.len()
                        || function.argument_types.len() != formal_count
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
                        return Err(ExprCompileError::Invalid("call argument arity differs").into());
                    }
                    let request = requests.get(&source.id).ok_or(ExprCompileError::Invalid(
                        "call definition was not statically validated",
                    ))?;
                    let selection = selected.get(&source.id).ok_or(ExprCompileError::Invalid(
                        "call selection was not statically validated",
                    ))?;
                    let mut argument_uses = if candidate_group {
                        Vec::new()
                    } else {
                        Vec::with_capacity(args.len())
                    };
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
                            let child_invocation =
                                flow.uses()
                                    .get(&child_use)
                                    .ok_or(ExprCompileError::Invalid(
                                        "missing call argument occurrence",
                                    ))?;
                            if child_invocation.definition != child {
                                return Err(ExprCompileError::Invalid(
                                    "ordered physical call arguments differ",
                                )
                                .into());
                            }
                            if Some(ordinal) != body_ordinal {
                                children = children.join_control_argument(
                                    *effects.get(&child_use).ok_or(ExprCompileError::Invalid(
                                        "argument effects were not prepared",
                                    ))?,
                                    flow,
                                    ordinal,
                                )?;
                            }
                            if let Some(source_plan) = &frozen.temporal_source {
                                let source = &source_plan.sources[ordinal];
                                if source.use_id != child_use {
                                    return Err(ExprCompileError::Invalid(
                                        "temporal source use differs from ordered child edge",
                                    )
                                    .into());
                                }
                                let source_node = unit.definition(child).ok_or(
                                    ExprCompileError::Invalid("missing temporal source definition"),
                                )?;
                                source_channels.push(novarocks_functions::TemporalSourceChannel {
                                    role: source.role,
                                    context: child_invocation.context,
                                    value_type: &source_node.ty,
                                });
                            }
                            if !candidate_group {
                                argument_uses.push(Some(child_use));
                            }
                        }
                        work.step()?;
                    }
                    let input = CallEffectInput {
                        context: frozen.context.into(),
                        argument_uses: match &frozen.temporal_source {
                            Some(source) => {
                                novarocks_functions::CallArgumentUses::TemporalSources {
                                    facts: &source.facts,
                                    channels: &source_channels,
                                }
                            }
                            None if candidate_group => {
                                let (&probe, candidates) =
                                    invocation.arguments.split_first().ok_or(
                                        ExprCompileError::Invalid("candidate group has no probe"),
                                    )?;
                                novarocks_functions::CallArgumentUses::CandidateGroup {
                                    probe,
                                    candidates,
                                }
                            }
                            None => novarocks_functions::CallArgumentUses::SelectedChannels(
                                &argument_uses,
                            ),
                        },
                        function_id: &function.function_id,
                        kind: function.kind,
                        selected: selection.as_ref(),
                        request: request.request(),
                        environment: &frozen.effects.environment,
                        parameters: environment.parameters,
                        decimal_overflow_policy: frozen.decimal_overflow_policy,
                        proof_scope: frozen.effects.proof_scope,
                    };
                    work.flush()?;
                    let body_edge = body_ordinal
                        .map(|ordinal| {
                            lambda::body_edge(|id| unit.definition(id), flow, invocation, ordinal)
                        })
                        .transpose()
                        .map_err(ExprCompileError::Invalid)?;
                    let options = if let Some(edge) = &body_edge {
                        PureCallPreparation::HigherOrder(lexical_references.preparation(
                            edge,
                            flow,
                            |id| unit.definition(id),
                            &effects,
                            children,
                            &mut retained,
                            work,
                        )?)
                    } else if control_intrinsic {
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
                    validate_prepared_call(&token, invocation, function, formal_count, work)?;
                    if let Some(edge) = body_edge {
                        let novarocks_functions::PreparedPureKernel::HigherOrder(prepared) =
                            token.prepared()
                        else {
                            return Err(ExprCompileError::Invalid(
                                "lambda requires its higher-order ABI",
                            )
                            .into());
                        };
                        let contract = prepared.body_contract();
                        if contract.context() != edge.root.context
                            || prepared.contract().body_edge_use() != edge.wrapper_use
                        {
                            return Err(ExprCompileError::Invalid(
                                "prepared lambda lost its original body use",
                            )
                            .into());
                        }
                        let (capture_count, captures) = lexical_references.captures(&edge);
                        let mut capture_sources = retained
                            .reserve::<ExprCompileError, lambda::CompiledLambdaCapture>(
                                capture_count,
                                work,
                            )?;
                        for capture in captures {
                            capture_sources.push(capture);
                            work.step()?;
                        }
                        retained.before_tree_insert::<ExprCompileError, ExpressionUseId, lambda::CompiledLambdaBody>(
                            lambdas.bodies.len(), work,
                        )?;
                        lambdas.bodies.insert(
                            id,
                            lambda::CompiledLambdaBody {
                                body_use: edge.root.context.use_id,
                                lambda_definition: edge.lambda_definition,
                                contract: Arc::clone(contract),
                                captures: capture_sources.into_boxed_slice(),
                            },
                        );
                    }
                    let result = ScopedExpressionEffects::primitive(
                        invocation.context,
                        token.effects().for_use(invocation.context)?,
                    );
                    tokens.insert(id, token);
                    result
                }
                _ => {
                    return Err(ExprCompileError::Invalid(
                        "lowered expression shape differs from source",
                    )
                    .into());
                }
            };
            lexical_references.record(source, invocation, &mut temporary, work)?;
            if let Some(scope) = lexical_references.reuse_scope(
                source,
                id,
                scoped,
                |id| unit.definition(id),
                work,
            )? {
                retained.before_tree_insert::<ExprCompileError, ExpressionUseId, lambda::LambdaReuseScope>(
                    lambdas.reuse.len(), work,
                )?;
                lambdas.reuse.insert(id, scope);
            }
            effects.insert(id, scoped);
            active.remove(&id);
            stack.pop();
            work.step()?;
        }
    }
    let mut result = Vec::with_capacity(tokens.len());
    for entry in tokens {
        result.push(entry);
        work.step()?;
    }
    Ok((result, effects, lambdas))
}

// Static constants retain the actual checked source owner through binding;
// no scalar metadata enum is reconstructed from an Arrow value.
fn literal_argument(
    source: &ExprNode,
    node: &StaticExprNode,
    control: &dyn PureCompileControl,
) -> Result<Option<ConstantValue>, ExprCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        if let StaticExprKind::Constant(value) = node.kind()
            && !source
                .ty
                .exactly_equals_observed::<ExprCompileError>(value.value_type(), || {
                    work.step().map_err(ExprCompileError::Control)
                })?
        {
            return Err(ExprCompileError::Invalid(
                "constant full type differs from source",
            ));
        }
        match (&source.kind, node.kind()) {
            (ExprKind::Constant(_), StaticExprKind::Constant(value)) => Ok(Some(value.clone())),
            // A resolved input is not a constant, even if this particular runtime
            // batch happens to broadcast one scalar value (notably RAND seeds).
            (
                ExprKind::Value(novarocks_expression_contract::ValueAddress::Input(_)),
                StaticExprKind::Input,
            )
            | (
                ExprKind::Value(novarocks_expression_contract::ValueAddress::Local(_)),
                StaticExprKind::LocalValue { .. },
            )
            | (ExprKind::Let { .. }, StaticExprKind::Let { .. })
            | (ExprKind::Lambda { .. }, StaticExprKind::Lambda { .. })
            | (ExprKind::LambdaParameter { .. }, StaticExprKind::LambdaParameter { .. }) => {
                Ok(None)
            }
            (ExprKind::FunctionCall { .. }, StaticExprKind::BoundCall { .. }) => Ok(None),
            (ExprKind::Conjunction { .. }, StaticExprKind::NaryAnd { .. })
            | (ExprKind::Disjunction { .. }, StaticExprKind::NaryOr { .. }) => Ok(None),
            (ExprKind::IsNull { negated: false, .. }, StaticExprKind::IsNull(_))
            | (ExprKind::IsNull { negated: true, .. }, StaticExprKind::IsNotNull(_)) => Ok(None),
            (ExprKind::Case { .. }, StaticExprKind::Case { .. }) => Ok(None),
            (ExprKind::Cast { .. }, StaticExprKind::PreparedCast { .. }) => Ok(None),

            _ => Err(ExprCompileError::Invalid(
                "unsupported call argument projection",
            )),
        }
    })();
    finish(result, &mut work)
}

impl From<novarocks_type_contract::ExpressionControlFlowError> for ExprCompileError {
    fn from(e: novarocks_type_contract::ExpressionControlFlowError) -> Self {
        match e {
            novarocks_type_contract::ExpressionControlFlowError::Control(c) => Self::Control(c),
            e => Self::Flow(e),
        }
    }
}
impl<H: fmt::Display> fmt::Display for ExprCompileFailure<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Creator(e) => e.fmt(f),
            Self::Resource(e) => e.fmt(f),
            Self::Owner(e) => e.fmt(f),
        }
    }
}
impl<H: Error + 'static> Error for ExprCompileFailure<H> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Creator(e) => Some(e),
            Self::Resource(e) => Some(e),
            Self::Owner(e) => Some(e),
        }
    }
}

/// Preserve the original resolved-call boundary after preparation. The unit
/// compiler creates the table itself, so foreign or duplicate attachments have
/// no public constructor, but a selected owner's token still must correspond.
fn validate_prepared_call(
    token: &PureCallSpecialization,
    invocation: &novarocks_type_contract::ExpressionInvocation<ExprId>,
    function: &novarocks_expression_contract::BoundFunction,
    arity: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ExprCompileError> {
    use novarocks_functions::{PreparedPureKernel, PurePreparationSource};
    let call = token.call_contract();
    if token.source() != PurePreparationSource::Frozen
        || call.context() != invocation.context.into()
        || !call
            .effects()
            .argument_control
            .matches_scalar_shape(invocation.control)
        || match (invocation.control, token.prepared()) {
            (ControlShape::HigherOrder { .. }, PreparedPureKernel::HigherOrder(_)) => false,
            (ControlShape::HigherOrder { .. }, _) => true,
            (ControlShape::CandidateGroup, PreparedPureKernel::Membership(_)) => false,
            (ControlShape::CandidateGroup, _) => true,
            (
                _,
                PreparedPureKernel::Scalar(_)
                | PreparedPureKernel::ScalarInvocation(_)
                | PreparedPureKernel::ControlIntrinsic(_)
                | PreparedPureKernel::OrderedRegexp(_)
                | PreparedPureKernel::NullIf(_),
            ) => false,
            _ => true,
        }
        || call.function_id() != &function.function_id
        || call.selected().overload != function.overload
        || call.logical_argument_count() != arity
        || call.selected().argument_types.len() != arity
    {
        return Err(ExprCompileError::Invalid(
            "prepared call differs from its original occurrence or lifecycle",
        ));
    }
    for (actual, expected) in call
        .selected()
        .argument_types
        .iter()
        .zip(&function.argument_types)
    {
        work.step()?;
        if !argument_types_equal(actual, expected, work)? {
            return Err(ExprCompileError::Invalid(
                "prepared call argument complete type differs",
            ));
        }
    }
    let FunctionResultType::Scalar(result) = &call.selected().result_type else {
        return Err(ExprCompileError::Invalid(
            "prepared scalar call has a relational result",
        ));
    };
    if !result.exactly_equals_observed(&function.result_type, || {
        work.step().map_err(ExprCompileError::from)
    })? {
        return Err(ExprCompileError::Invalid(
            "prepared call result complete type differs",
        ));
    }
    token.effects().for_use(invocation.context)?;
    Ok(())
}

fn argument_types_equal(
    actual: &FunctionArgumentType,
    expected: &FunctionArgumentType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ExprCompileError> {
    match (actual, expected) {
        (FunctionArgumentType::Value(a), FunctionArgumentType::Value(b)) => {
            a.exactly_equals_observed(b, || work.step().map_err(Into::into))
        }
        (
            FunctionArgumentType::CandidateGroup { element_type: a },
            FunctionArgumentType::CandidateGroup { element_type: b },
        ) => a.exactly_equals_observed(b, || work.step().map_err(Into::into)),
        (
            FunctionArgumentType::Lambda {
                parameter_types: a,
                result_type: a_result,
            },
            FunctionArgumentType::Lambda {
                parameter_types: b,
                result_type: b_result,
            },
        ) if a.len() == b.len() => {
            for (a, b) in a.iter().zip(b) {
                if !a.exactly_equals_observed(b, || work.step().map_err(ExprCompileError::from))? {
                    return Ok(false);
                }
            }
            a_result.exactly_equals_observed(b_result, || work.step().map_err(Into::into))
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
#[path = "compiler/checked_reference_tests.rs"]
mod checked_reference_tests;
#[cfg(test)]
#[path = "compiler/literal_metadata_tests.rs"]
mod literal_metadata_tests;

#[cfg(test)]
#[path = "compiler/primary_cause_footer_tests.rs"]
mod primary_cause_footer_tests;
