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

//! Explicit function declarations and exact, carrier-neutral binding.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use sha2::{Digest, Sha256};

pub use novarocks_function_contract::FunctionBindingError;
/// Exact scoped validation failures. No resolver error is erased or recovered
/// by text/downcast; an unconverted owner never falls back to pure admission.
#[derive(Debug)]
pub enum FunctionBindingConstructionResource {
    Layout(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    Vec(novarocks_type_contract::ControlResourceError),
    HashMap(novarocks_type_contract::owned_resources::hashmap::HashMapResourceError),
    BTree(novarocks_type_contract::owned_resources::btree::BTreeResourceError),
}
#[derive(Debug)]
pub enum FunctionBindingCreationFailure<H> {
    Binding(FunctionBindingError),
    Creator(H),
    Resource(FunctionBindingConstructionResource),
    /// Stage A is not globally enabled until all admitted resolver owners join.
    UnconvertedResolverOwner,
}
impl<H> From<FunctionBindingError> for FunctionBindingCreationFailure<H> {
    fn from(error: FunctionBindingError) -> Self {
        Self::Binding(error)
    }
}
impl<H> From<CompileControlError> for FunctionBindingCreationFailure<H> {
    fn from(error: CompileControlError) -> Self {
        Self::Binding(error.into())
    }
}
impl<H> From<novarocks_type_contract::FunctionIdentityError> for FunctionBindingCreationFailure<H> {
    fn from(error: novarocks_type_contract::FunctionIdentityError) -> Self {
        Self::Binding(FunctionBindingError::InvalidBinding(
            error.to_string().into(),
        ))
    }
}
impl<H> From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for FunctionBindingCreationFailure<H>
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource(FunctionBindingConstructionResource::Layout(error))
    }
}
impl<H> From<novarocks_type_contract::ValueTypeError> for FunctionBindingCreationFailure<H> {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Binding(error.into())
    }
}
impl FunctionBindingCreationFailure<std::convert::Infallible> {
    pub(crate) fn into_pure(self) -> FunctionBindingError {
        match self {
            Self::Binding(error) => error,
            Self::Creator(never) => match never {},
            Self::Resource(_) | Self::UnconvertedResolverOwner => {
                unreachable!("pure validation delegates its actual owner")
            }
        }
    }
}

/// A staged object-safe sibling refuses explicitly until its actual owner is
/// converted. Original owner/control/kernel failures retain their exact cause.
#[derive(Debug)]
pub enum CheckedOwnerFailure<E> {
    Original(E),
    UnconvertedOwner,
}
impl<E: std::fmt::Display> std::fmt::Display for CheckedOwnerFailure<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Original(error) => error.fmt(f),
            Self::UnconvertedOwner => {
                f.write_str("checked selected preparation owner is not converted")
            }
        }
    }
}
impl<E: std::error::Error> std::error::Error for CheckedOwnerFailure<E> {}

/// A local, non-owning loan minted only after the actual binding author and
/// its original footer succeed. It proves neither effects nor preparation.
/// No constructor, memory authority, scope or query is retained here.
pub struct CheckedCallEffectInput<'a> {
    catalog: &'a EngineFunctionCatalog,
    binding: &'a FunctionBindingDefinition,
    input: crate::CallEffectInput<'a>,
    constant_policy: &'a crate::ConstantPolicy,
}
impl std::fmt::Debug for CheckedCallEffectInput<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedCallEffectInput")
            .finish_non_exhaustive()
    }
}
impl<'a> CheckedCallEffectInput<'a> {
    pub(crate) fn require_input(
        &self,
        input: crate::CallEffectInput<'_>,
        policy: &crate::ConstantPolicy,
    ) -> Result<(), FunctionBindingError> {
        if !self.input.same_borrow(input) || self.constant_policy != policy {
            return Err(invalid_binding(
                "checked selection differs from its original call input or constant policy",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_catalog(
        &self,
        catalog: &EngineFunctionCatalog,
    ) -> Result<(), FunctionBindingError> {
        if !std::ptr::eq(self.catalog, catalog) {
            return Err(invalid_binding(
                "checked selection borrows a different immutable catalogue",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_registered(
        &self,
        declaration: &FunctionBindingDeclaration,
        input: crate::CallEffectInput<'_>,
    ) -> Result<(), FunctionBindingError> {
        self.require_input(input, self.constant_policy)?;
        if !std::ptr::eq(self.binding.declaration.as_ref(), declaration) {
            return Err(invalid_binding(
                "checked selection borrows a different registered owner",
            ));
        }
        Ok(())
    }
    /// Require the exact original resolver loan, independently of its trait
    /// vtable. This does not re-resolve, allocate or retain the author.
    pub fn require_binding_author(
        &self,
        resolver: &dyn FunctionBindingResolver,
        input: crate::CallEffectInput<'_>,
    ) -> Result<(), FunctionBindingError> {
        self.require_input(input, self.constant_policy)?;
        if !std::ptr::addr_eq(self.binding.resolver.pure_author(), resolver) {
            return Err(invalid_binding(
                "checked selection borrows a different original binding author",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_fixed_author(
        &self,
        resolver: &crate::builtin::catalogue::BuiltinScalarResolver,
        input: crate::CallEffectInput<'_>,
    ) -> Result<(), FunctionBindingError> {
        self.require_input(input, self.constant_policy)?;
        if !matches!(&self.binding.resolver.0, BindingResolverOwnerKind::FixedScalar(owner)
            if std::ptr::eq(owner.as_ref(), resolver))
        {
            return Err(invalid_binding(
                "checked selection borrows a different original binding author",
            ));
        }
        Ok(())
    }
}

/// The installed catalog consumes admitted constants, never pool addresses.
pub type FunctionArgument = novarocks_function_contract::FunctionArgument<crate::ConstantValue>;
pub type FunctionBindingRequest<'a> =
    novarocks_function_contract::FunctionBindingRequest<'a, crate::ConstantValue>;

use crate::{
    AggregateOverloadDeclaration, AggregateStateFormatIdentity, EngineFunctionCatalog,
    EngineFunctionCatalogBuilder, FunctionArgumentEvaluation, FunctionArgumentType,
    FunctionCatalogError, FunctionDefinition, FunctionFailureBehavior, FunctionId, FunctionKind,
    FunctionOverloadId, FunctionValueType, FunctionVisibility, FunctionVolatility, digest_text,
};

/// Semantics declared by the implementation owner, never inferred from a name.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FunctionSemantics {
    pub volatility: FunctionVolatility,
    pub argument_evaluation: FunctionArgumentEvaluation,
    pub failure_behavior: FunctionFailureBehavior,
    pub intrinsic_row_error: novarocks_type_contract::FunctionIntrinsicRowError,
}
impl FunctionSemantics {
    /// Lossy discovery projection only. Exact controls, NULL, state,
    /// observables and environment facts remain in the full declaration.
    pub fn from_effects(effects: &novarocks_type_contract::FunctionEffectDeclaration) -> Self {
        use novarocks_type_contract::ArgumentControl;
        let argument_evaluation = match effects.argument_control {
            ArgumentControl::If
            | ArgumentControl::Coalesce
            | ArgumentControl::SimpleCase
            | ArgumentControl::SearchedCase => FunctionArgumentEvaluation::ShortCircuit,
            ArgumentControl::Eager
            | ArgumentControl::TypeOnly
            | ArgumentControl::NoArguments
            | ArgumentControl::HigherOrder { .. }
            | ArgumentControl::Aggregate
            | ArgumentControl::Window
            | ArgumentControl::Table
            | ArgumentControl::TemporalSource(_) => FunctionArgumentEvaluation::Eager,
        };
        Self {
            volatility: effects.value_stability,
            argument_evaluation,
            failure_behavior: effects.failure_behavior,
            intrinsic_row_error: effects.own_row_error,
        }
    }
}

/// The aggregate state contract remains separate from its Arrow carrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateBindingDeclaration {
    pub state_argument_contract: novarocks_type_contract::AggregateStateArgumentContract,
    pub intermediate_pattern: Box<str>,
    pub state_format: AggregateStateFormatIdentity,
}

/// One stable overload family. Patterns describe the owner's binder contract;
/// the catalog does not implement a second pattern language or rank candidates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionOverloadDeclaration {
    pub identity: FunctionOverloadId,
    pub semantics: FunctionSemantics,
    /// None is an explicit legacy-only declaration, never authority to infer
    /// missing base facts. Pure installation requires every overload's Some.
    pub effects: Option<novarocks_type_contract::FunctionEffectDeclaration>,
    pub argument_pattern: Box<str>,
    pub result_pattern: Box<str>,
    pub aggregate: Option<AggregateBindingDeclaration>,
}
impl FunctionOverloadDeclaration {
    pub fn from_effects(
        identity: FunctionOverloadId,
        argument_pattern: impl Into<Box<str>>,
        result_pattern: impl Into<Box<str>>,
        aggregate: Option<AggregateBindingDeclaration>,
        effects: novarocks_type_contract::FunctionEffectDeclaration,
    ) -> Self {
        Self {
            identity,
            semantics: FunctionSemantics::from_effects(&effects),
            effects: Some(effects),
            argument_pattern: argument_pattern.into(),
            result_pattern: result_pattern.into(),
            aggregate,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionBindingDeclaration {
    function_id: FunctionId,
    kind: FunctionKind,
    overloads: Box<[FunctionOverloadDeclaration]>,
    scalar_invocation_data: Box<[FunctionOverloadId]>,
}

impl FunctionBindingDeclaration {
    /// A complete metadata declaration, not proof of installed CPU coverage.
    /// Legacy facts are never expanded into guessed NULL/state/control facts.
    pub fn try_new_complete(
        function_id: FunctionId,
        kind: FunctionKind,
        overloads: impl IntoIterator<Item = FunctionOverloadDeclaration>,
    ) -> Result<Self, FunctionBindingError> {
        let declaration = Self::try_new(function_id, kind, overloads)?;
        declaration.validate_complete_effects()?;
        Ok(declaration)
    }

    pub fn validate_complete_effects(&self) -> Result<(), FunctionBindingError> {
        for overload in &self.overloads {
            if overload.effects.is_none() {
                return Err(FunctionBindingError::MissingEffectDeclaration(
                    overload.identity.clone(),
                ));
            }
        }
        Ok(())
    }

    pub fn effect_declaration(
        &self,
        identity: &FunctionOverloadId,
    ) -> Result<&novarocks_type_contract::FunctionEffectDeclaration, FunctionBindingError> {
        self.overload(identity)?
            .effects
            .as_ref()
            .ok_or_else(|| FunctionBindingError::MissingEffectDeclaration(identity.clone()))
    }

    pub fn try_new(
        function_id: FunctionId,
        kind: FunctionKind,
        overloads: impl IntoIterator<Item = FunctionOverloadDeclaration>,
    ) -> Result<Self, FunctionBindingError> {
        let mut overloads = overloads.into_iter().collect::<Vec<_>>();
        if overloads.is_empty() {
            return Err(invalid_binding("function has no declared overloads"));
        }
        overloads.sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
        for overload in &mut overloads {
            if let Some(effects) = &mut overload.effects {
                effects
                    .validate(kind)
                    .map_err(|_| invalid_binding("invalid complete function effect declaration"))?;
                if overload.semantics != FunctionSemantics::from_effects(effects) {
                    return Err(invalid_binding(
                        "legacy semantics differ from full declaration projection",
                    ));
                }
                // Environment dependencies are a set. Keep one canonical
                // stored source for exact-owner lookup and digest material.
                effects.environment_dependencies.sort_unstable();
            }
        }
        let mut patterns = BTreeSet::new();
        for (index, overload) in overloads.iter().enumerate() {
            if !overload
                .semantics
                .intrinsic_row_error
                .is_valid_for_kind(kind)
            {
                return Err(invalid_binding(
                    "intrinsic row-error fact differs from the function kind",
                ));
            }
            if index > 0 && overloads[index - 1].identity == overload.identity {
                return Err(FunctionBindingError::DuplicateOverload(
                    overload.identity.clone(),
                ));
            }
            validate_pattern(&overload.argument_pattern)?;
            validate_pattern(&overload.result_pattern)?;
            if !patterns.insert(overload.argument_pattern.as_ref()) {
                return Err(invalid_binding(
                    "multiple overloads declare the same argument pattern",
                ));
            }
            if (kind == FunctionKind::Aggregate) != overload.aggregate.is_some() {
                return Err(invalid_binding(
                    "aggregate state declaration differs from the function kind",
                ));
            }
            if let Some(aggregate) = &overload.aggregate {
                validate_pattern(&aggregate.intermediate_pattern)?;
            }
        }
        Ok(Self {
            function_id,
            kind,
            overloads: overloads.into_boxed_slice(),
            scalar_invocation_data: Box::new([]),
        })
    }

    /// A positive original whole-call Data fact, independent of whether this
    /// catalogue admits an installed runtime ABI. Absence proves no safety.
    pub(crate) fn with_scalar_invocation_data_overloads(
        mut self,
        identities: impl IntoIterator<Item = FunctionOverloadId>,
    ) -> Result<Self, FunctionBindingError> {
        let mut identities: Vec<_> = identities.into_iter().collect();
        identities.sort_unstable();
        for (index, identity) in identities.iter().enumerate() {
            let overload = self.overload(identity)?;
            if index > 0 && identities[index - 1] == *identity {
                return Err(FunctionBindingError::DuplicateOverload(identity.clone()));
            }
            if self.kind != FunctionKind::Scalar
                || overload.semantics.failure_behavior != FunctionFailureBehavior::Propagate
            {
                return Err(invalid_binding(
                    "scalar invocation Data requires a propagating scalar overload",
                ));
            }
        }
        self.scalar_invocation_data = identities.into_boxed_slice();
        Ok(self)
    }

    pub fn has_scalar_invocation_data(
        &self,
        identity: &FunctionOverloadId,
    ) -> Result<bool, FunctionBindingError> {
        self.overload(identity)?;
        Ok(self.scalar_invocation_data.binary_search(identity).is_ok())
    }

    pub fn function_id(&self) -> &FunctionId {
        &self.function_id
    }
    pub const fn kind(&self) -> FunctionKind {
        self.kind
    }
    /// Conservative metadata for name-based discovery. Exact calls consume
    /// only their selected overload's semantics.
    pub fn volatility(&self) -> FunctionVolatility {
        self.overloads
            .iter()
            .map(|overload| overload.semantics.volatility)
            .max()
            .expect("a declaration always contains an overload")
    }
    pub fn overloads(&self) -> &[FunctionOverloadDeclaration] {
        &self.overloads
    }

    fn overload(
        &self,
        identity: &FunctionOverloadId,
    ) -> Result<&FunctionOverloadDeclaration, FunctionBindingError> {
        self.overloads
            .binary_search_by(|candidate| candidate.identity.cmp(identity))
            .map(|index| &self.overloads[index])
            .map_err(|_| FunctionBindingError::UnknownOverload(identity.clone()))
    }
}

fn validate_pattern(pattern: &str) -> Result<(), FunctionBindingError> {
    if pattern.trim().is_empty() || pattern.len() > u16::MAX as usize {
        return Err(invalid_binding(
            "function signature pattern is empty or exceeds 65535 bytes",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum FunctionResultType {
    Scalar(FunctionValueType),
    /// Only the function's produced columns, excluding outer pass-throughs.
    Relation(novarocks_type_contract::FunctionParameterTypes),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AggregateBindingSelection {
    pub state_argument_contract: novarocks_type_contract::AggregateStateArgumentContract,
    pub intermediate_type: FunctionValueType,
    pub state_format: AggregateStateFormatIdentity,
}

/// Concrete instantiation of a declared overload, including explicit coercion
/// targets for values and lambda bodies/parameters. The caller must materialize
/// these coercions before exact validation. A value cannot coerce to a lambda,
/// and a lambda cannot gain or lose parameters through coercion.
/// No process handle or implementation pointer is part of this value.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FunctionBindingSelection {
    pub overload: FunctionOverloadId,
    pub argument_types: novarocks_type_contract::FunctionArgumentTypes,
    pub result_type: FunctionResultType,
    pub aggregate: Option<AggregateBindingSelection>,
}

/// FE resolution and BE selected-binding validation are distinct operations.
/// Implementations must validate the requested overload directly; validation
/// must not call resolution to choose a possibly different candidate.
pub trait FunctionBindingResolver: Send + Sync {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError>;

    /// Instantiate this exact declared overload through its original owner.
    /// This authors selection metadata only; it grants no effects, runtime
    /// occurrence or pure implementation. Unsupported owners refuse explicitly.
    fn select_at_overload_observed(
        &self,
        _overload: &FunctionOverloadId,
        _request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        finish_binding_work(Err(FunctionBindingError::MissingBindingDeclaration), work)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError>;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ResolvedFunctionBinding {
    /// The selected owner's positive whole-call Data declaration. This is
    /// neither a row-error fact nor permission to execute a pure ABI.
    pub declared_scalar_invocation_data: bool,
    /// Positive execution-scope fact from the actual Exact catalogue's
    /// admitted installed declaration. Original metadata binding publishes
    /// None; absence is never evidence of a kernel-only implementation.
    pub admitted_execution_abi: Option<crate::PureKernelAbi>,
    pub function_id: FunctionId,
    pub kind: FunctionKind,
    pub semantics: FunctionSemantics,
    pub logical_argument_count: usize,
    pub selected: FunctionBindingSelection,
}

/// The actual immutable binding author retained by one installed definition.
/// Pure and scoped operations use this same carrier; the original-dynamic arm
/// is an explicit staging boundary and grants no scoped-validation capability.
#[derive(Clone)]
pub struct FunctionBindingResolverOwner(BindingResolverOwnerKind);
#[derive(Clone)]
enum BindingResolverOwnerKind {
    FixedScalar(Arc<crate::builtin::catalogue::BuiltinScalarResolver>),
    StringField(Arc<crate::builtin::StringFieldOwner>),
    DynamicScalar(Arc<crate::builtin::catalogue::BuiltinDynamicScalarResolver>),
    Unnest(Arc<crate::builtin::UnnestOwner>),
    BorrowedAggregate(Arc<crate::builtin::catalogue::BuiltinAggregateResolver>),
    BorrowedConversion(Arc<crate::builtin::value_conversion::ValueConversionResolver>),
    ParametricAggregate(Arc<ParametricAggregateBindingResolver>),
    Original(Arc<dyn FunctionBindingResolver>),
}
impl FunctionBindingResolverOwner {
    /// Retain an existing original author without claiming scoped capability.
    pub fn original(resolver: Arc<dyn FunctionBindingResolver>) -> Self {
        Self(BindingResolverOwnerKind::Original(resolver))
    }
    pub(crate) fn fixed_scalar(
        resolver: Arc<crate::builtin::catalogue::BuiltinScalarResolver>,
    ) -> Self {
        Self(BindingResolverOwnerKind::FixedScalar(resolver))
    }
    pub(crate) fn string_field(resolver: Arc<crate::builtin::StringFieldOwner>) -> Self {
        Self(BindingResolverOwnerKind::StringField(resolver))
    }
    pub(crate) fn dynamic_scalar(
        resolver: Arc<crate::builtin::catalogue::BuiltinDynamicScalarResolver>,
    ) -> Self {
        Self(BindingResolverOwnerKind::DynamicScalar(resolver))
    }
    pub(crate) fn unnest(owner: Arc<crate::builtin::UnnestOwner>) -> Self {
        Self(BindingResolverOwnerKind::Unnest(owner))
    }
    pub(crate) fn borrowed_aggregate(
        owner: Arc<crate::builtin::catalogue::BuiltinAggregateResolver>,
    ) -> Self {
        Self(BindingResolverOwnerKind::BorrowedAggregate(owner))
    }
    pub(crate) fn borrowed_conversion(
        owner: Arc<crate::builtin::value_conversion::ValueConversionResolver>,
    ) -> Self {
        Self(BindingResolverOwnerKind::BorrowedConversion(owner))
    }
    pub(crate) fn parametric_aggregate(owner: Arc<ParametricAggregateBindingResolver>) -> Self {
        Self(BindingResolverOwnerKind::ParametricAggregate(owner))
    }
    fn validate_selected_with_creation<H>(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingCreationFailure<H>> {
        match creation {
            None => self
                .pure_author()
                .validate_selected(selected, request, control)
                .map_err(Into::into),
            Some(creation) => {
                match &self.0 {
                    BindingResolverOwnerKind::FixedScalar(owner) => owner
                        .validate_selected_with_creator_in(selected, request, creation, control),
                    BindingResolverOwnerKind::StringField(owner) => owner
                        .validate_selected_with_creator_in(selected, request, creation, control),
                    BindingResolverOwnerKind::DynamicScalar(owner) => owner
                        .validate_selected_with_creator_in(selected, request, creation, control),
                    BindingResolverOwnerKind::Unnest(owner) => owner
                        .validate_selected_with_creator_in(selected, request, creation, control),
                    // These exact original validators match borrowed recipes;
                    // they create no expected FVT or collection backing.
                    BindingResolverOwnerKind::BorrowedAggregate(owner) => owner
                        .validate_selected(selected, request, control)
                        .map_err(Into::into),
                    BindingResolverOwnerKind::BorrowedConversion(owner) => owner
                        .validate_selected(selected, request, control)
                        .map_err(Into::into),
                    BindingResolverOwnerKind::ParametricAggregate(owner) => owner
                        .validate_selected_with_creator_in(selected, request, creation, control),
                    BindingResolverOwnerKind::Original(_) => {
                        Err(FunctionBindingCreationFailure::UnconvertedResolverOwner)
                    }
                }
            }
        }
    }
    fn pure_author(&self) -> &(dyn FunctionBindingResolver + 'static) {
        match &self.0 {
            BindingResolverOwnerKind::FixedScalar(owner) => owner.as_ref(),
            BindingResolverOwnerKind::StringField(owner) => owner.as_ref(),
            BindingResolverOwnerKind::DynamicScalar(owner) => owner.as_ref(),
            BindingResolverOwnerKind::Unnest(owner) => owner.as_ref(),
            BindingResolverOwnerKind::BorrowedAggregate(owner) => owner.as_ref(),
            BindingResolverOwnerKind::BorrowedConversion(owner) => owner.as_ref(),
            BindingResolverOwnerKind::ParametricAggregate(owner) => owner.as_ref(),
            BindingResolverOwnerKind::Original(owner) => owner.as_ref(),
        }
    }
}
impl AsRef<dyn FunctionBindingResolver> for FunctionBindingResolverOwner {
    fn as_ref(&self) -> &(dyn FunctionBindingResolver + 'static) {
        self.pure_author()
    }
}
impl FunctionBindingResolver for FunctionBindingResolverOwner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.pure_author().resolve(request, control)
    }
    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.pure_author()
            .select_at_overload_observed(overload, request, control)
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.pure_author()
            .validate_selected(selected, request, control)
    }
}

#[derive(Clone)]
pub(crate) struct FunctionBindingDefinition {
    pub(crate) declaration: Arc<FunctionBindingDeclaration>,
    resolver: FunctionBindingResolverOwner,
    pub(crate) pure: Option<crate::pure_catalogue::PureFunctionAttachment>,
}

enum BoundValidation<'a> {
    Frozen(&'a ResolvedFunctionBinding),
    Legacy(&'a mut ResolvedFunctionBinding),
}
impl BoundValidation<'_> {
    fn binding(&self) -> &ResolvedFunctionBinding {
        match self {
            Self::Frozen(bound) => bound,
            Self::Legacy(bound) => bound,
        }
    }
}

impl FunctionBindingDefinition {
    pub(crate) fn retain_resolver_from(&mut self, original: &Self) {
        self.resolver = original.resolver.clone();
    }
}

/// Derive an aggregate's binding contract from the overloads it already
/// declares.
///
/// An aggregate overload states its identity, what it takes, what it returns
/// and what its state looks like - which is everything a binding declaration
/// holds. Deriving it means the two halves of one function cannot disagree,
/// and that registering an aggregate cannot leave it resolvable through only
/// one of them.
pub(crate) fn parametric_aggregate_binding(
    canonical_name: &str,
    volatility: crate::FunctionVolatility,
    overloads: &[crate::AggregateOverloadDeclaration],
    aggregate_resolver: Arc<dyn crate::AggregateSignatureResolver>,
) -> Result<FunctionBindingDefinition, FunctionCatalogError> {
    let invalid_identity = |error: &dyn fmt::Display| FunctionCatalogError::InvalidStableIdentity {
        subject: "parametric aggregate binding declaration",
        value: error.to_string().into(),
    };
    let function_id = FunctionId::try_new(format!("parametric.aggregate/{canonical_name}/v1"))
        .map_err(|error| invalid_identity(&error))?;
    let declared = overloads
        .iter()
        .map(|overload| {
            let state_argument_contract = aggregate_resolver
                .state_argument_contract(&overload.identity)
                .map_err(|error| invalid_identity(&error))?;
            Ok(FunctionOverloadDeclaration {
                effects: None,
                semantics: FunctionSemantics {
                    volatility,
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error:
                        novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
                },
                identity: FunctionOverloadId::try_new(overload.identity.as_str())
                    .map_err(|error| invalid_identity(&error))?,
                argument_pattern: overload.argument_pattern.clone(),
                result_pattern: overload.output_pattern.clone(),
                aggregate: Some(AggregateBindingDeclaration {
                    state_argument_contract,
                    intermediate_pattern: overload.intermediate_pattern.clone(),
                    state_format: overload.state_format.clone(),
                }),
            })
        })
        .collect::<Result<Vec<_>, FunctionCatalogError>>()?;
    let declaration =
        FunctionBindingDeclaration::try_new(function_id, crate::FunctionKind::Aggregate, declared)
            .map_err(|error| invalid_identity(&error))?;
    let declaration = Arc::new(declaration);
    let resolver = Arc::new(ParametricAggregateBindingResolver {
        aggregate_resolver,
        declaration: Arc::clone(&declaration),
    });
    Ok(FunctionBindingDefinition {
        declaration,
        resolver: FunctionBindingResolverOwner::parametric_aggregate(resolver),
        pure: None,
    })
}

/// Answers binding questions for an aggregate through the same typed contract
/// its signatures are resolved with, so the two can never disagree.
pub(crate) struct ParametricAggregateBindingResolver {
    pub(crate) aggregate_resolver: Arc<dyn crate::AggregateSignatureResolver>,
    pub(crate) declaration: Arc<FunctionBindingDeclaration>,
}

impl FunctionBindingResolver for ParametricAggregateBindingResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            let argument_types = self.argument_types(request, &mut work)?;
            work.flush()?;
            let logical = self
                .aggregate_resolver
                .resolve_aggregate(&argument_types[..request.logical_argument_count])
                .map_err(FunctionBindingError::from)?;
            let resolved = if request.logical_argument_count == argument_types.len() {
                logical
            } else {
                work.step()?;
                work.flush()?;
                self.aggregate_resolver
                    .resolve_update_signature(&logical.overload, &argument_types)
                    .map_err(FunctionBindingError::from)?
            };
            for _ in request.arguments {
                work.step()?;
            }
            self.selection(request, &resolved, &mut work)
        })();
        finish_binding_work(result, work)
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = self.select_exact_observed(overload, request, &mut work);
        finish_binding_work(result, work)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            let expected = self.select_exact_observed(&selected.overload, request, &mut work)?;
            if crate::builtin::binding_control::same_selection(&expected, selected, &mut work)? {
                Ok(())
            } else {
                Err(invalid_binding(
                    "selected aggregate overload differs from its typed contract",
                ))
            }
        })();
        finish_binding_work(result, work)
    }
}

impl fmt::Debug for FunctionBindingDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FunctionBindingDefinition")
            .field("declaration", &self.declaration)
            .finish_non_exhaustive()
    }
}

impl FunctionDefinition {
    /// Register an explicit binding contract. Legacy resolution APIs reject
    /// this definition rather than discarding its selected identity.
    /// Register a non-aggregate function from its binding declaration.
    ///
    /// An aggregate is refused here on purpose. Resolving one needs a typed
    /// signature contract that this constructor has no way to obtain, and
    /// building an aggregate without one produced a definition that named the
    /// function everywhere but could not be resolved anywhere - which is not a
    /// failure any caller can see until something tries to resolve it. Use
    /// `try_new_bound_aggregate`, which cannot be called without one.
    pub fn try_new_bound(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<dyn FunctionBindingResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        if declaration.kind == crate::FunctionKind::Aggregate {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "aggregate function without a typed signature contract",
                value: canonical_name.as_ref().into(),
            });
        }
        Self::bound(canonical_name, visibility, declaration, resolver, None)
    }

    /// Register an aggregate from its binding declaration and the contract that
    /// resolves its typed signature.
    pub fn try_new_bound_aggregate(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<dyn FunctionBindingResolver>,
        aggregate_resolver: Arc<dyn crate::AggregateSignatureResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        Self::bound(
            canonical_name,
            visibility,
            declaration,
            resolver,
            Some(aggregate_resolver),
        )
    }

    pub(crate) fn bound(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<dyn FunctionBindingResolver>,
        aggregate_resolver: Option<Arc<dyn crate::AggregateSignatureResolver>>,
    ) -> Result<Self, FunctionCatalogError> {
        Self::bound_with_owner(
            canonical_name,
            visibility,
            declaration,
            FunctionBindingResolverOwner::original(resolver),
            aggregate_resolver,
        )
    }

    pub(crate) fn bound_with_owner(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        declaration: FunctionBindingDeclaration,
        resolver: FunctionBindingResolverOwner,
        aggregate_resolver: Option<Arc<dyn crate::AggregateSignatureResolver>>,
    ) -> Result<Self, FunctionCatalogError> {
        let canonical_name = canonical_name.as_ref();
        super::validate_canonical_name(canonical_name)?;
        let aggregate_overloads = declaration
            .overloads
            .iter()
            .filter_map(|overload| {
                overload.aggregate.as_ref().map(|aggregate| {
                    AggregateOverloadDeclaration::try_new(
                        overload.identity.as_str(),
                        &overload.argument_pattern,
                        &aggregate.intermediate_pattern,
                        &overload.result_pattern,
                        aggregate.state_format.as_str(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let canonical_signatures = declaration
            .overloads
            .iter()
            .map(|overload| overload.argument_pattern.clone())
            .collect();
        Ok(Self {
            canonical_name: canonical_name.into(),
            kind: declaration.kind,
            visibility,
            volatility: declaration.volatility(),
            canonical_signatures,
            aggregate_overloads: aggregate_overloads.into_boxed_slice(),
            exact_aggregate_overloads: Box::default(),
            aggregate_resolver,
            resolver: None,
            binding: Some(FunctionBindingDefinition {
                declaration: Arc::new(declaration),
                resolver,
                pure: None,
            }),
        })
    }

    pub fn binding_declaration(&self) -> Option<&FunctionBindingDeclaration> {
        self.binding
            .as_ref()
            .map(|binding| binding.declaration.as_ref())
    }

    /// Borrow the original immutable binding author. A provider can attach its
    /// typed pure CPU to the same declaration without recreating resolution or
    /// electing another family. This loan does not certify effects or kernels.
    pub fn binding_resolver(&self) -> Option<&FunctionBindingResolverOwner> {
        self.binding.as_ref().map(|binding| &binding.resolver)
    }
}

impl EngineFunctionCatalogBuilder {
    /// Final-plan consumers require every entry to have an explicit identity.
    /// An incomplete migration cannot silently invent identities or semantics.
    pub fn seal_bound(self) -> Result<EngineFunctionCatalog, FunctionCatalogError> {
        for definition in self.definitions.values() {
            if definition.binding.is_none() {
                return Err(FunctionCatalogError::MissingBindingDeclaration {
                    name: definition.canonical_name.clone(),
                    kind: definition.kind,
                });
            }
        }
        self.seal()
    }
}

impl EngineFunctionCatalog {
    pub fn definition_by_id(&self, identity: &FunctionId) -> Option<&FunctionDefinition> {
        self.identities
            .get(identity)
            .map(|index| &self.definitions[*index])
    }

    pub fn resolve_bound_user(
        &self,
        name: &str,
        kind: FunctionKind,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            work.step()?;
            let definition = self
                .definition(name, kind)
                .ok_or(FunctionBindingError::UnknownFunction)?;
            if definition.visibility == FunctionVisibility::Hidden {
                return Err(FunctionBindingError::HiddenFunction);
            }
            resolve_definition(definition, request, &mut work)
        })();
        finish_binding_work(result, work)
    }

    pub fn resolve_bound_trusted(
        &self,
        name: &str,
        kind: FunctionKind,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            work.step()?;
            let definition = self
                .definition(name, kind)
                .ok_or(FunctionBindingError::UnknownFunction)?;
            resolve_definition(definition, request, &mut work)
        })();
        finish_binding_work(result, work)
    }

    /// Author a canonical selection at one exact installed identity/overload.
    /// The original resolver must implement this capability; there is no user
    /// name, alternate overload resolution or copied-selection fallback. The
    /// result still requires real scope/effect/kernel preparation separately.
    pub fn select_exact_overload_observed(
        &self,
        function: &FunctionId,
        kind: FunctionKind,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<FunctionBindingSelection>, FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            let definition = self.definition_by_id(function);
            work.step()?;
            let binding =
                exact_definition(definition.ok_or(FunctionBindingError::UnknownFunction)?)?;
            let same_kind = kind == binding.declaration.kind;
            work.step()?;
            if !same_kind {
                return Err(invalid_binding(
                    "exact selection kind differs from the declaration",
                ));
            }
            validate_request(kind, request, &mut work)?;
            let mut declared = false;
            for candidate in binding.declaration.overloads() {
                let same = &candidate.identity == overload;
                work.step()?;
                if same {
                    declared = true;
                    break;
                }
            }
            if !declared {
                return Err(invalid_binding(
                    "exact selection overload is not declared by this function",
                ));
            }
            work.flush()?;
            let selected =
                binding
                    .resolver
                    .select_at_overload_observed(overload, request, work.control())?;
            let same_overload = &selected.overload == overload;
            work.step()?;
            if !same_overload {
                return Err(invalid_binding(
                    "exact selection owner returned a different overload",
                ));
            }
            if let Some(expected) = request.expected_result_type {
                let matches = match &selected.result_type {
                    FunctionResultType::Scalar(actual) => actual
                        .exactly_equals_observed(expected, || {
                            work.step().map_err(FunctionBindingError::from)
                        })?,
                    FunctionResultType::Relation(_) => {
                        work.step()?;
                        false
                    }
                };
                if !matches {
                    return Err(invalid_binding(
                        "exact selection result differs from the requested result type",
                    ));
                }
            }
            validate_selected_definition(binding, &selected, request, &mut work)?;
            work.flush()?;
            let selected = Arc::new(selected);
            work.step()?;
            Ok(selected)
        })();
        finish_binding_work(result, work)
    }

    /// Validate a selected definition by its exact installed identity, even
    /// when it has no runtime occurrence. This does not resolve a user name,
    /// authenticate frozen effects, prepare an implementation, or create state.
    /// Legacy semantic metadata is deliberately outside this selected-type port.
    pub fn validate_frozen_selection(
        &self,
        function: &FunctionId,
        kind: FunctionKind,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.validate_frozen_selection_with_creation::<std::convert::Infallible>(
            function, kind, selected, request, control, None,
        )
        .map_err(FunctionBindingCreationFailure::into_pure)
    }
    /// Explicit staged port. Production callers keep the original pure entry
    /// until every admitted resolver arm supplies its scoped constructor core.
    pub fn validate_frozen_selection_with_creator_in<H>(
        &self,
        function: &FunctionId,
        kind: FunctionKind,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingCreationFailure<H>> {
        self.validate_frozen_selection_with_creation(
            function,
            kind,
            selected,
            request,
            control,
            Some(creation),
        )
    }
    fn validate_frozen_selection_with_creation<H>(
        &self,
        function: &FunctionId,
        kind: FunctionKind,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
        creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    ) -> Result<(), FunctionBindingCreationFailure<H>> {
        self.checked_frozen_selection_with_creation(
            function, kind, selected, request, control, creation,
        )
        .map(|_| ())
    }

    pub(crate) fn check_call_effect_input_with_creator_in<'a, H>(
        &'a self,
        input: crate::CallEffectInput<'a>,
        constant_policy: &'a crate::ConstantPolicy,
        creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        control: &dyn PureCompileControl,
    ) -> Result<CheckedCallEffectInput<'a>, FunctionBindingCreationFailure<H>> {
        let binding = self.checked_frozen_selection_with_creation(
            input.function_id,
            input.kind,
            input.selected,
            input.request,
            control,
            Some(creation),
        )?;
        // The original function request has no policy field. The caller loans
        // its actual already-admitted source policy; it grants no new capacity.
        Ok(CheckedCallEffectInput {
            catalog: self,
            binding,
            input,
            constant_policy,
        })
    }

    fn checked_frozen_selection_with_creation<H>(
        &self,
        function: &FunctionId,
        kind: FunctionKind,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
        creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    ) -> Result<&FunctionBindingDefinition, FunctionBindingCreationFailure<H>> {
        self.checked_frozen_selection_with_author(
            function,
            kind,
            selected,
            request,
            control,
            |binding, work| {
                validate_selected_definition_with_creation(
                    binding, selected, request, work, creation,
                )
            },
        )
    }

    fn checked_frozen_selection_with_author<H>(
        &self,
        function: &FunctionId,
        kind: FunctionKind,
        _selected: &FunctionBindingSelection,
        _request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
        validate_author: impl FnOnce(
            &FunctionBindingDefinition,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), FunctionBindingCreationFailure<H>>,
    ) -> Result<&FunctionBindingDefinition, FunctionBindingCreationFailure<H>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result: Result<&FunctionBindingDefinition, FunctionBindingCreationFailure<H>> =
            (|| {
                let definition = self
                    .definition_by_id(function)
                    .ok_or(FunctionBindingError::UnknownFunction)?;
                let binding = exact_definition(definition)?;
                if kind != binding.declaration.kind {
                    return Err(invalid_binding(
                        "frozen function kind differs from the declaration",
                    )
                    .into());
                }
                work.step()?;
                validate_author(binding, &mut work)?;
                Ok(binding)
            })();
        finish_created_binding_work(result, work)
    }

    /// Check the original catalogue/request headers before acquiring an actual
    /// validation-only scope. The same resolver author performs construction;
    /// pure callers retain their original unscoped entry and control order.
    pub fn validate_frozen_selection_with_factory_in<H, F>(
        &self,
        function: &FunctionId,
        kind: FunctionKind,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        factory: &F,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingCreationFailure<H>>
    where
        F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
    {
        self.checked_frozen_selection_with_author(
            function,
            kind,
            selected,
            request,
            control,
            |binding, work| {
                validate_selected_definition_with_author(
                    binding,
                    selected,
                    request,
                    work,
                    |author, control| {
                        let mut scope = factory
                            .begin_scope()
                            .map_err(FunctionBindingCreationFailure::Creator)?;
                        author.validate_selected_with_creation(
                            selected,
                            request,
                            Some(&mut scope),
                            control,
                        )
                    },
                )
            },
        )
        .map(|_| ())
    }

    /// Check one frozen binding and its already-coerced argument expressions.
    /// The executable implementation is selected by identity by its owner.
    pub fn validate_bound(
        &self,
        bound: &ResolvedFunctionBinding,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.validate_bound_common(BoundValidation::Frozen(bound), request, control)
    }

    /// Reconstruct only the positive declaration on a legacy codec scratch
    /// binding, at the same observed identity/semantics validation position.
    /// This never changes its admitted ABI or grants runtime admission.
    pub fn validate_legacy_bound_observed(
        &self,
        bound: &mut ResolvedFunctionBinding,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.validate_bound_common(BoundValidation::Legacy(bound), request, control)
    }

    fn validate_bound_common(
        &self,
        mut target: BoundValidation<'_>,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            work.step()?;
            let bound = target.binding();
            let definition = self
                .definition_by_id(&bound.function_id)
                .ok_or(FunctionBindingError::UnknownFunction)?;
            let binding = exact_definition(definition)?;
            let overload = binding.declaration.overload(&bound.selected.overload)?;
            if bound.kind != binding.declaration.kind || bound.semantics != overload.semantics {
                return Err(invalid_binding(
                    "frozen function kind or semantics differ from the declaration",
                ));
            }
            let declared = binding
                .declaration
                .has_scalar_invocation_data(&bound.selected.overload)?;
            match &mut target {
                BoundValidation::Frozen(bound)
                    if bound.declared_scalar_invocation_data != declared =>
                {
                    return Err(invalid_binding(
                        "frozen scalar invocation Data fact differs from the declaration",
                    ));
                }
                BoundValidation::Legacy(bound) => bound.declared_scalar_invocation_data = declared,
                BoundValidation::Frozen(_) => {}
            }
            let bound = target.binding();
            if bound.logical_argument_count != request.logical_argument_count {
                return Err(invalid_binding(
                    "frozen logical argument count differs from the expression",
                ));
            }
            validate_selected_definition(binding, &bound.selected, request, &mut work)
        })();
        finish_binding_work(result, work)
    }
}

/// The selected-type algorithm is shared with legacy validation without
/// changing that entry's separate semantics/count checks or error order.
fn validate_selected_definition(
    binding: &FunctionBindingDefinition,
    selected: &FunctionBindingSelection,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    validate_selected_definition_with_creation::<std::convert::Infallible>(
        binding, selected, request, work, None,
    )
    .map_err(FunctionBindingCreationFailure::into_pure)
}
fn validate_selected_definition_with_creation<H>(
    binding: &FunctionBindingDefinition,
    selected: &FunctionBindingSelection,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
    creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
) -> Result<(), FunctionBindingCreationFailure<H>> {
    validate_selected_definition_with_author(binding, selected, request, work, |author, control| {
        author.validate_selected_with_creation(selected, request, creation, control)
    })
}

fn validate_selected_definition_with_author<H>(
    binding: &FunctionBindingDefinition,
    selected: &FunctionBindingSelection,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
    validate_author: impl FnOnce(
        &FunctionBindingResolverOwner,
        &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingCreationFailure<H>>,
) -> Result<(), FunctionBindingCreationFailure<H>> {
    validate_selection(&binding.declaration, selected, request, work)?;
    for (argument, expected) in request.arguments.iter().zip(&selected.argument_types) {
        work.step()?;
        if !argument.matches_type_observed(expected, work)? {
            return Err(invalid_binding(
                "frozen argument types differ from the already-coerced expressions",
            )
            .into());
        }
    }
    work.flush()?;
    validate_author(&binding.resolver, work.control())
}

fn exact_definition(
    definition: &FunctionDefinition,
) -> Result<&FunctionBindingDefinition, FunctionBindingError> {
    definition
        .binding
        .as_ref()
        .ok_or(FunctionBindingError::MissingBindingDeclaration)
}

fn resolve_definition(
    definition: &FunctionDefinition,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
    let binding = exact_definition(definition)?;
    validate_request(binding.declaration.kind, request, work)?;
    work.flush()?;
    let selected = binding.resolver.resolve(request, work.control())?;
    validate_selection(&binding.declaration, &selected, request, work)?;
    Ok(ResolvedFunctionBinding {
        declared_scalar_invocation_data: binding
            .declaration
            .has_scalar_invocation_data(&selected.overload)?,
        admitted_execution_abi: None,
        function_id: binding.declaration.function_id.clone(),
        kind: binding.declaration.kind,
        semantics: binding.declaration.overload(&selected.overload)?.semantics,
        logical_argument_count: request.logical_argument_count,
        selected,
    })
}

fn validate_request(
    kind: FunctionKind,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    if request.arguments.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    if let Some(expected) = request.expected_result_type {
        validate_value_type(expected, work)?;
    }
    for argument in request.arguments {
        work.step()?;
        match argument {
            FunctionArgument::Value {
                value_type,
                constant,
            } => {
                validate_value_type(value_type, work)?;
                if let Some(constant) = constant
                    && !value_type.exactly_equals_observed(constant.value_type(), || {
                        work.step().map_err(FunctionBindingError::from)
                    })?
                {
                    return Err(invalid_binding(
                        "constant source type differs from its argument type",
                    ));
                }
            }
            FunctionArgument::Lambda {
                parameter_types,
                result_type,
            } => {
                if parameter_types.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
                    return Err(CompileControlError::ResourceExhausted.into());
                }
                for ty in parameter_types {
                    work.step()?;
                    validate_value_type(ty, work)?;
                }
                validate_value_type(result_type, work)?;
            }
        }
    }
    if request.logical_argument_count > request.arguments.len()
        || (kind != FunctionKind::Aggregate
            && request.logical_argument_count != request.arguments.len())
    {
        return Err(invalid_binding(
            "logical argument count differs from the function kind or update channels",
        ));
    }
    for argument in &request.arguments[request.logical_argument_count..] {
        work.step()?;
        if matches!(argument, FunctionArgument::Lambda { .. }) {
            return Err(invalid_binding(
                "aggregate ORDER BY update channels must be values, not lambdas",
            ));
        }
    }
    Ok(())
}

fn validate_selection(
    declaration: &FunctionBindingDeclaration,
    selected: &FunctionBindingSelection,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    validate_request(declaration.kind, request, work)?;
    if selected.argument_types.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    for argument in &selected.argument_types {
        work.step()?;
        validate_argument_type(argument, work)?;
    }
    match &selected.result_type {
        FunctionResultType::Scalar(ty) => validate_value_type(ty, work)?,
        FunctionResultType::Relation(types) => {
            if types.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            for ty in types {
                work.step()?;
                validate_value_type(ty, work)?;
            }
        }
    }
    if let Some(aggregate) = &selected.aggregate {
        validate_value_type(&aggregate.intermediate_type, work)?;
    }
    let overload = declaration.overload(&selected.overload)?;
    if selected.argument_types.len() != request.arguments.len() {
        return Err(invalid_binding(
            "selected signature argument count differs from the request",
        ));
    }
    for (argument, selected_type) in request.arguments.iter().zip(&selected.argument_types) {
        work.step()?;
        match (argument, selected_type) {
            (FunctionArgument::Value { .. }, FunctionArgumentType::Value(_)) => {}
            (
                FunctionArgument::Lambda {
                    parameter_types, ..
                },
                FunctionArgumentType::Lambda {
                    parameter_types: selected_parameters,
                    ..
                },
            ) if parameter_types.len() == selected_parameters.len() => {}
            _ => {
                return Err(invalid_binding(
                    "selected argument shape or lambda arity differs from the request",
                ));
            }
        }
    }
    if (declaration.kind == FunctionKind::Table)
        != matches!(selected.result_type, FunctionResultType::Relation(_))
    {
        return Err(invalid_binding(
            "selected result shape differs from the function kind",
        ));
    }
    match (&overload.aggregate, &selected.aggregate) {
        (None, None) => {}
        (Some(declared), Some(resolved)) => {
            if resolved.state_format != declared.state_format
                || resolved.state_argument_contract != declared.state_argument_contract
            {
                return Err(invalid_binding(
                    "aggregate state format or argument contract differs from its declaration",
                ));
            }
        }
        _ => {
            return Err(invalid_binding(
                "selected aggregate state contract differs from the declaration",
            ));
        }
    }
    Ok(())
}

fn binding_kernel_failure(error: crate::KernelFailure) -> FunctionBindingError {
    match error {
        crate::KernelFailure::CompileControl(cause) => cause.into(),
        crate::KernelFailure::Cancelled => CompileControlError::Cancelled.into(),
        crate::KernelFailure::DeadlineExceeded => CompileControlError::DeadlineExceeded.into(),
        crate::KernelFailure::ResourceExhausted => CompileControlError::ResourceExhausted.into(),
        other => invalid_binding(&other.to_string()),
    }
}

fn validate_value_type(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    crate::kernel_input::validate_type_observed(value, work).map_err(binding_kernel_failure)
}

fn validate_argument_type(
    argument: &FunctionArgumentType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    match argument {
        FunctionArgumentType::Value(value) => validate_value_type(value, work),
        FunctionArgumentType::Lambda {
            parameter_types,
            result_type,
        } => {
            if parameter_types.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            for ty in parameter_types {
                work.step()?;
                validate_value_type(ty, work)?;
            }
            validate_value_type(result_type, work)
        }
    }
}

fn finish_created_binding_work<T, H>(
    result: Result<T, FunctionBindingCreationFailure<H>>,
    work: CompileCheckpoints<'_>,
) -> Result<T, FunctionBindingCreationFailure<H>> {
    work.finish_result(result, FunctionBindingCreationFailure::from)
}
fn finish_binding_work<T>(
    result: Result<T, FunctionBindingError>,
    work: CompileCheckpoints<'_>,
) -> Result<T, FunctionBindingError> {
    work.finish_result(result, FunctionBindingError::from)
}

pub(crate) fn digest_binding_definition(
    hasher: &mut Sha256,
    binding: Option<&FunctionBindingDefinition>,
) {
    let Some(binding) = binding else {
        hasher.update([0]);
        return;
    };
    hasher.update([1]);
    let declaration = &binding.declaration;
    digest_text(hasher, declaration.function_id.as_str());
    hasher.update(
        u32::try_from(declaration.overloads.len())
            .expect("overload count fits u32")
            .to_be_bytes(),
    );
    for overload in &declaration.overloads {
        digest_text(hasher, overload.identity.as_str());
        if declaration
            .scalar_invocation_data
            .binary_search(&overload.identity)
            .is_ok()
        {
            hasher.update([0xd8, 1]);
        }
        hasher.update([match overload.semantics.volatility {
            FunctionVolatility::Immutable => 1,
            FunctionVolatility::Stable => 2,
            FunctionVolatility::Volatile => 3,
        }]);
        hasher.update([match overload.semantics.argument_evaluation {
            FunctionArgumentEvaluation::Eager => 1,
            FunctionArgumentEvaluation::ShortCircuit => 2,
        }]);
        hasher.update([match overload.semantics.failure_behavior {
            FunctionFailureBehavior::Propagate => 1,
            FunctionFailureBehavior::ReturnsNull => 2,
        }]);
        hasher.update([match overload.semantics.intrinsic_row_error {
            novarocks_type_contract::FunctionIntrinsicRowError::NoRowError => 1,
            novarocks_type_contract::FunctionIntrinsicRowError::MayRaise => 2,
            novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated => 3,
        }]);
        digest_text(hasher, &overload.argument_pattern);
        digest_text(hasher, &overload.result_pattern);
        if let Some(effects) = &overload.effects {
            hasher.update([1]);
            crate::effect_metadata::digest_effect_declaration(hasher, effects);
        } else {
            hasher.update([0]);
        }
        if let Some(aggregate) = &overload.aggregate {
            hasher.update([1]);
            digest_text(hasher, &aggregate.intermediate_pattern);
            digest_text(hasher, aggregate.state_format.as_str());
            hasher.update([match aggregate.state_argument_contract {
                novarocks_type_contract::AggregateStateArgumentContract::ExactSignature => 1,
                novarocks_type_contract::AggregateStateArgumentContract::ValueRootNullabilityIndependent => 2,
            }]);
        } else {
            hasher.update([0]);
        }
    }
    crate::pure_catalogue::digest_pure_attachment(hasher, binding.pure.as_ref());
}

pub(crate) fn invalid_binding(message: &str) -> FunctionBindingError {
    FunctionBindingError::InvalidBinding(message.into())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod effect_metadata_tests;

impl From<crate::FunctionResolutionError> for FunctionBindingError {
    fn from(error: crate::FunctionResolutionError) -> Self {
        match error {
            crate::FunctionResolutionError::Control(error) => error.into(),
            other => invalid_binding(&other.to_string()),
        }
    }
}

#[cfg(test)]
pub(crate) fn arguments_equal_for_test(
    actual: &[FunctionArgument],
    expected: &[FunctionArgument],
    control: &dyn PureCompileControl,
) -> Result<bool, FunctionBindingError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        work.step()?;
        if actual.len() != expected.len() {
            return Ok(false);
        }
        for (actual, expected) in actual.iter().zip(expected) {
            work.step()?;
            work.flush()?;
            if !actual.equals_observed(expected, CompilePhase::FunctionSpecialization, control)? {
                return Ok(false);
            }
            work.flush()?;
        }
        Ok(true)
    })();
    finish_binding_work(result, work)
}

#[cfg(test)]
#[path = "binding/parametric_exact_selection_tests.rs"]
mod parametric_exact_selection_tests;
