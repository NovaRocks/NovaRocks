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

//! Fragment binding for actual node-local and synthesized expression units.
//! Scalar definitions, flow, recipes and per-use calls retain their original
//! expression compiler; this owner supplies actual runtime input layouts.

use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

use novarocks_functions::{
    CallEffectInput, ConstantError, ConstantValidation, FunctionBindingError,
    FunctionBindingSelection, FunctionSpecializationFailure, PureCallPreparation,
    PureCallSpecialization, PureEngineFunctionCatalog,
};
use novarocks_local_program::{ProgramCallSite, ProgramRootRef, ProgramUnitId, StaticLayout};
use novarocks_physical_plan::{ConstantReferenceError, FragmentPackage, NodeId, PhysicalCallSite};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, EffectContractError, FunctionKind, PureCompileControl,
    ValueTypeError,
};

use novarocks_expression::{
    CompiledExprSet, EvaluationEnvironment, ExprCompileError, ExprCompileFailure, ExprCreation,
    ScopeColumn, ScopeLayout, ScopeLayouts,
};
use novarocks_expression_contract::{ExprNode, RootOrdinal, ValueId};
#[derive(Debug)]
pub(crate) enum ExpressionLoweringError {
    BTree(novarocks_type_contract::owned_resources::btree::BTreeResourceError),
    Control(CompileControlError),
    Constant(ConstantError),
    Reference(ConstantReferenceError),
    Binding(FunctionBindingError),
    Type(ValueTypeError),
    Unit(ExprCompileError),
    Specialization(FunctionSpecializationFailure),
    Effects(EffectContractError),
    Arithmetic(novarocks_functions::ArithmeticPrepareError),
    Cast(novarocks_functions::CastPrepareError),
    Comparison(novarocks_functions::ComparisonPrepareError),
    UnsupportedCall(PhysicalCallSite),
    Invalid(&'static str),
}
impl ExpressionLoweringError {
    /// This new model source uses the existing Owner channel, but retains the
    /// primary construction priority of the previous Resource envelope even
    /// when SourceModel's neutral class is Invariant rather than Resource.
    pub(crate) fn is_primary_construction(&self) -> bool {
        matches!(self, Self::BTree(_))
            || matches!(self, Self::Unit(error) if error.is_primary_construction())
    }

    pub(crate) fn is_unsupported(&self) -> bool {
        matches!(self, Self::Unit(e) if e.is_unsupported())
            || matches!(
                self,
                Self::UnsupportedCall(_)
                    | Self::Cast(novarocks_functions::CastPrepareError::Unsupported)
                    | Self::Arithmetic(novarocks_functions::ArithmeticPrepareError::Unsupported)
                    | Self::Comparison(novarocks_functions::ComparisonPrepareError::Unsupported)
                    | Self::Binding(FunctionBindingError::UnavailableImplementation(_))
                    | Self::Specialization(
                        FunctionSpecializationFailure::MissingPureImplementation(_)
                    )
                    | Self::Specialization(FunctionSpecializationFailure::Binding(
                        FunctionBindingError::UnavailableImplementation(_)
                    ))
            )
    }
}
impl fmt::Display for ExpressionLoweringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BTree(source) => source.fmt(f),
            Self::Control(e) => e.fmt(f),
            Self::Constant(e) => e.fmt(f),
            Self::Reference(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Unit(e) => e.fmt(f),
            Self::Specialization(e) => e.fmt(f),
            Self::Effects(e) => e.fmt(f),
            Self::Arithmetic(e) => e.fmt(f),
            Self::Cast(e) => e.fmt(f),
            Self::Comparison(e) => e.fmt(f),
            Self::UnsupportedCall(site) => write!(f, "unsupported physical call {site:?}"),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}
impl Error for ExpressionLoweringError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::BTree(source) => Some(source),
            Self::Control(e) => Some(e),
            Self::Constant(e) => Some(e),
            Self::Reference(e) => Some(e),
            Self::Binding(e) => Some(e),
            Self::Type(e) => Some(e),
            Self::Unit(e) => Some(e),
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
impl From<ExprCompileError> for ExpressionLoweringError {
    fn from(e: ExprCompileError) -> Self {
        match e {
            ExprCompileError::Control(c) => Self::Control(c),
            e => Self::Unit(e),
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
expression_creation_cause!(novarocks_constant_contract::ConstantTableError);
expression_creation_cause!(ExprCompileError);
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
        Some((factory, _)) => {
            // The definition was fully validated by this immutable compiler's
            // first pass. Expected types in this preparation get a separate
            // temporary owner; they never enlarge a published type invoice.
            let mut scope = factory
                .begin_scope()
                .map_err(ExpressionCreationFailure::Creator)?;
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

impl From<novarocks_constant_contract::ConstantTableError> for ExpressionLoweringError {
    fn from(error: novarocks_constant_contract::ConstantTableError) -> Self {
        Self::from(ConstantReferenceError::from(error))
    }
}

pub(crate) type RootMap = BTreeMap<RootOrdinal, ProgramRootRef>;

/// Assembly owns one slot for each actual unit. An unfilled slot cannot escape
/// publication; synthesized reads use the same table and compilation entry.
pub(crate) struct LoweredExpressions {
    pub units: Vec<Option<Arc<CompiledExprSet>>>,
    pub node_units: BTreeMap<NodeId, ProgramUnitId>,
    pub ids: BTreeMap<NodeId, RootMap>,
    pub union_ids: BTreeMap<NodeId, Vec<Vec<ProgramRootRef>>>,
    pub runtime_filter_key_ids: BTreeMap<NodeId, Vec<ProgramRootRef>>,
}
impl LoweredExpressions {
    pub fn new<H>(
        package: &FragmentPackage,
        mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, crate::FragmentCreationError<H>> {
        let mut result = Self {
            units: Vec::new(),
            node_units: BTreeMap::new(),
            ids: BTreeMap::new(),
            union_ids: BTreeMap::new(),
            runtime_filter_key_ids: BTreeMap::new(),
        };
        for node in package.fragment().nodes().values() {
            work.step()?;
            let mut roots = RootMap::new();
            if let Some(unit) = &node.expressions {
                let id = ProgramUnitId::new(result.units.len());
                crate::creation::before_vec_push(
                    &result.units,
                    crate::creation::borrow_scope(&mut temporary),
                    work,
                )?;
                result.units.push(None);
                crate::creation::before_tree_insert::<NodeId, ProgramUnitId, H>(
                    result.node_units.len(),
                    crate::creation::borrow_scope(&mut temporary),
                    work,
                )?;
                result.node_units.insert(node.id, id);
                for ordinal in 0..unit.roots().len() {
                    work.step()?;
                    let root = RootOrdinal::new(ordinal as u32);
                    crate::creation::before_tree_insert::<RootOrdinal, ProgramRootRef, H>(
                        roots.len(),
                        crate::creation::borrow_scope(&mut temporary),
                        work,
                    )?;
                    roots.insert(root, ProgramRootRef { unit: id, root });
                }
            }
            crate::creation::before_tree_insert::<NodeId, RootMap, H>(
                result.ids.len(),
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
            result.ids.insert(node.id, roots);
        }
        Ok(result)
    }
    pub fn roots(&self, node: NodeId) -> Result<&RootMap, ExpressionLoweringError> {
        self.ids.get(&node).ok_or(ExpressionLoweringError::Invalid(
            "node root namespace is absent",
        ))
    }
    pub fn root(
        &self,
        node: NodeId,
        root: RootOrdinal,
    ) -> Result<ProgramRootRef, ExpressionLoweringError> {
        self.roots(node)?
            .get(&root)
            .copied()
            .ok_or(ExpressionLoweringError::Invalid(
                "node expression root is absent",
            ))
    }
    pub fn unit(&self, node: NodeId) -> Result<&Arc<CompiledExprSet>, ExpressionLoweringError> {
        self.units
            .get(
                self.node_units
                    .get(&node)
                    .ok_or(ExpressionLoweringError::Invalid(
                        "node expression unit is absent",
                    ))?
                    .index(),
            )
            .and_then(Option::as_ref)
            .ok_or(ExpressionLoweringError::Invalid(
                "node expression unit has not been compiled",
            ))
    }
    pub fn finish<H>(
        self,
        temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<Arc<CompiledExprSet>>, crate::FragmentCreationError<H>> {
        let mut units = Vec::new();
        crate::creation::reserve_vec(&mut units, self.units.len(), temporary, work)?;
        for unit in self.units {
            work.step()?;
            units.push(unit.ok_or(ExpressionLoweringError::Invalid(
                "program expression unit was not compiled",
            ))?);
        }
        Ok(units)
    }
}

pub(crate) fn bound_root(
    package: &FragmentPackage,
    node: NodeId,
    root: RootOrdinal,
) -> Result<&ExprNode, ExpressionLoweringError> {
    let unit = package
        .fragment()
        .nodes()
        .get(&node)
        .and_then(|node| node.expressions.as_deref())
        .ok_or(ExpressionLoweringError::Invalid(
            "node expression unit is absent",
        ))?;
    unit.root(root)
        .and_then(|root| unit.definition(root.definition))
        .ok_or(ExpressionLoweringError::Invalid(
            "node expression root definition is absent",
        ))
}

impl<H> From<ExprCompileFailure<H>> for ExpressionCreationFailure<H> {
    fn from(error: ExprCompileFailure<H>) -> Self {
        match error {
            ExprCompileFailure::Control(c) => Self::Control(c),
            ExprCompileFailure::Creator(c) => Self::Creator(c),
            ExprCompileFailure::Resource(c) => Self::Resource(c),
            ExprCompileFailure::Owner(c) => ExpressionLoweringError::from(c).into(),
        }
    }
}

/// The complete runtime schema remains borrowed from its original local
/// layout. Canonical ValueId representatives retain their actual ordinals.
pub(crate) fn scope_layout<H>(
    values: &[ValueId],
    layout: &StaticLayout,
    declarations: &[(ValueId, novarocks_type_contract::FunctionValueType)],
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScopeLayout, crate::FragmentCreationError<H>> {
    if values.len() != layout.schema().fields().len() {
        return Err(crate::FragmentCompileError::Invalid(
            "expression scope width differs from its actual runtime layout",
        )
        .into());
    }
    let mut ordinal_types = Vec::new();
    crate::creation::reserve_vec(&mut ordinal_types, values.len(), Some(creation), work)?;
    for ordinal in 0..values.len() {
        work.step()?;
        let binding =
            layout
                .value_binding_at(ordinal)
                .ok_or(crate::FragmentCompileError::Invalid(
                    "expression runtime input lacks its complete value binding",
                ))?;
        ordinal_types.push(
            novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                binding.clone_observed(|| work.step())?,
            )?,
        );
    }
    let mut columns = Vec::new();
    crate::creation::reserve_vec(&mut columns, declarations.len(), Some(creation), work)?;
    let mut index = BTreeMap::new();
    for (ordinal, value) in values.iter().enumerate() {
        work.step()?;
        if !index.contains_key(value) {
            crate::creation::before_tree_insert::<ValueId, usize, H>(
                index.len(),
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
            index.insert(*value, ordinal);
        }
    }
    for (value, ty) in declarations {
        work.step()?;
        let ordinal = *index
            .get(value)
            .ok_or(crate::FragmentCompileError::Invalid(
                "declared expression input is absent from runtime layout",
            ))?;
        let actual = &ordinal_types[ordinal];
        if !ty.exactly_equals_observed::<crate::FragmentCreationError<H>>(actual, || {
            work.step().map_err(Into::into)
        })? {
            return Err(crate::FragmentCompileError::Invalid(
                "expression input declaration differs from its actual runtime complete type",
            )
            .into());
        }
        columns.push(ScopeColumn {
            value: *value,
            ordinal: u32::try_from(ordinal).map_err(|_| CompileControlError::ResourceExhausted)?,
            ty: actual.clone_observed(|| work.step())?,
        });
    }
    work.flush()?;
    creation.request_layout(novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::array::<ScopeColumn>(columns.len()).map_err(|_|novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)?)?).map_err(crate::FragmentCreationError::Creator)?;
    let columns = Arc::from(columns);
    work.flush()?;
    creation.request_layout(novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::array::<novarocks_type_contract::FunctionValueType>(ordinal_types.len()).map_err(|_|novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)?)?).map_err(crate::FragmentCreationError::Creator)?;
    let ordinal_types = Arc::from(ordinal_types);
    let scope = match temporary.as_mut() {
        Some(temporary) => ScopeLayout::try_new_with_creator_in(
            Arc::clone(layout.schema()),
            ordinal_types,
            columns,
            creation.storage_lifetime(),
            *temporary,
            work,
        )
        .map_err(ExpressionCreationFailure::from)?,
        None => ScopeLayout::try_new(
            Arc::clone(layout.schema()),
            ordinal_types,
            columns,
            creation.storage_lifetime(),
            work,
        )
        .map_err(ExpressionLoweringError::from)?,
    };
    Ok(scope)
}

pub(crate) fn compile_unit<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    unit: &novarocks_expression_contract::BoundExprSet,
    package: &FragmentPackage,
    scopes: &[ScopeLayout],
    combined: Option<&ScopeLayout>,
    catalog: &PureEngineFunctionCatalog,
    policy: ConstantValidation,
    factory: Option<&F>,
    mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<CompiledExprSet>, crate::FragmentCreationError<H>> {
    let environment = EvaluationEnvironment {
        constants: package.constants(),
        parameters: package.parameters(),
    };
    let scopes = ScopeLayouts {
        ports: scopes,
        all_ports: combined,
    };
    match factory {
        Some(factory) => {
            let mut retained = factory
                .begin_scope()
                .map_err(crate::FragmentCreationError::Creator)?;
            novarocks_expression::compile_expr_set(
                unit,
                environment,
                scopes,
                policy,
                catalog,
                ExprCreation {
                    temporary: crate::creation::borrow_scope(&mut temporary)
                        .expect("strong entry supplies the original temporary owner"),
                    retained: &mut retained,
                    factory,
                },
                work,
            )
            .map_err(ExpressionCreationFailure::from)
            .map_err(Into::into)
        }
        None => novarocks_expression::compile_expr_set_pure(
            unit,
            environment,
            scopes,
            policy,
            catalog,
            work,
        )
        .map_err(ExpressionLoweringError::from)
        .map_err(Into::into),
    }
}

pub(crate) fn prepare_calls_with_aggregates<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    package: &FragmentPackage,
    lowered: &LoweredExpressions,
    functions: &PureEngineFunctionCatalog,
    nodes: &BTreeMap<NodeId, novarocks_local_program::ProgramNodeId>,
    work: &mut CompileCheckpoints<'_>,
    creation: CallCreation<'_, F>,
    temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
) -> Result<Vec<(ProgramCallSite, PureCallSpecialization)>, crate::FragmentCreationError<H>> {
    let mut tokens = BTreeMap::new();
    crate::aggregate::prepare_aggregate_calls(
        package,
        functions,
        lowered,
        nodes,
        &mut tokens,
        work,
        creation,
    )?;
    crate::table_function::prepare_table_calls(
        package,
        functions,
        lowered,
        nodes,
        &mut tokens,
        work,
        creation,
    )?;
    crate::window::prepare_window_calls(
        package,
        functions,
        lowered,
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
    let mut prepared = Vec::new();
    crate::creation::reserve_vec(&mut prepared, tokens.len(), temporary, work)?;
    for token in tokens {
        work.step()?;
        prepared.push(token);
    }
    Ok(prepared)
}

#[expect(
    clippy::too_many_arguments,
    reason = "Actual node scopes, source environment and original construction ports remain separate owners"
)]
pub(crate) fn compile_node_unit<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    node: &novarocks_physical_plan::PhysicalNode,
    layouts: &[&StaticLayout],
    combined: Option<(&[ValueId], &StaticLayout)>,
    expressions: &mut LoweredExpressions,
    functions: &PureEngineFunctionCatalog,
    policy: ConstantValidation,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), crate::FragmentCreationError<H>> {
    let Some(unit) = &node.expressions else {
        return Ok(());
    };
    let mut scopes = Vec::new();
    crate::creation::reserve_vec(&mut scopes, unit.ports().len(), Some(creation), work)?;
    for (ordinal, port) in unit.ports().iter().enumerate() {
        let mut scan_values = Vec::new();
        let values: &[ValueId] = match &node.kind {
            novarocks_physical_plan::NodeKind::Scan {
                provider_outputs, ..
            } => {
                crate::creation::reserve_vec(
                    &mut scan_values,
                    provider_outputs.len(),
                    crate::creation::borrow_scope(&mut temporary),
                    work,
                )?;
                for (_, value) in provider_outputs {
                    work.step()?;
                    scan_values.push(*value);
                }
                &scan_values
            }
            _ => {
                &package
                    .fragment()
                    .nodes()
                    .get(
                        node.inputs
                            .get(ordinal)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "unit port has no actual node input",
                            ))?,
                    )
                    .ok_or(ExpressionLoweringError::Invalid(
                        "unit input node is absent",
                    ))?
                    .output
                    .columns
            }
        };
        let layout = layouts
            .get(ordinal)
            .ok_or(ExpressionLoweringError::Invalid(
                "unit port has no runtime layout",
            ))?;
        scopes.push(scope_layout(
            values,
            layout,
            &port.values,
            creation,
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?);
    }
    let all = match combined {
        Some((values, layout)) => {
            let mut declarations = Vec::new();
            for port in unit.ports() {
                crate::creation::reserve_vec(
                    &mut declarations,
                    port.values.len(),
                    crate::creation::borrow_scope(&mut temporary),
                    work,
                )?;
                for declaration in &port.values {
                    work.step()?;
                    declarations.push(declaration.clone());
                }
            }
            Some(scope_layout(
                values,
                layout,
                &declarations,
                creation,
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?)
        }
        None if scopes.len() == 1 => Some(scopes[0].clone()),
        None => None,
    };
    let compiled = compile_unit(
        unit,
        package,
        &scopes,
        all.as_ref(),
        functions,
        policy,
        factory,
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    let id = *expressions
        .node_units
        .get(&node.id)
        .ok_or(ExpressionLoweringError::Invalid(
            "node unit has no reserved program address",
        ))?;
    expressions.units[id.index()] = Some(compiled);
    Ok(())
}
impl LoweredExpressions {
    pub fn completed_units<H>(
        &self,
        temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<Arc<CompiledExprSet>>, crate::FragmentCreationError<H>> {
        let mut units = Vec::new();
        crate::creation::reserve_vec(&mut units, self.units.len(), temporary, work)?;
        for unit in &self.units {
            work.step()?;
            units.push(Arc::clone(unit.as_ref().ok_or(
                ExpressionLoweringError::Invalid("program unit was not compiled"),
            )?));
        }
        Ok(units)
    }
}

#[cfg(test)]
#[path = "checked_reference_tests.rs"]
pub(crate) mod checked_reference_tests;

#[cfg(test)]
#[path = "primary_cause_footer_tests.rs"]
mod primary_cause_footer_tests;
