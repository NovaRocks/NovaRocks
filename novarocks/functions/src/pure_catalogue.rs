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

//! Atomic exact-owner registration and pure-sealed process composition.
//! Local handles are resolved here and never encoded into the plan wire.

use std::{collections::BTreeMap, fmt, sync::Arc};

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CompileCheckpoints, CompilePhase, FunctionEffectDeclaration,
    FunctionInstanceState, FunctionNullBehavior, ObservableEffects, PureCompileControl,
};
use sha2::{Digest, Sha256};

use crate::*;

/// Stable compatibility identity of actual implementation code, not an address
/// or a second logical function/overload identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PureImplementationId(Box<str>);
impl PureImplementationId {
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, FunctionCatalogError> {
        crate::validate_stable_identity("pure function implementation", value.as_ref())?;
        Ok(Self(value.as_ref().into()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Each variant names a complete typed lifecycle ABI, including aggregate
/// OVER support from the same aggregate owner. The version is independent of
/// any implementation identity and participates in native compatibility.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PureKernelAbi {
    ScalarV1,
    ScalarInvocationV1,
    HigherOrderV1,
    AggregateV1,
    AggregateWindowV1,
    WindowV1,
    TableV1,
    ControlIntrinsicV1,
}
/// Actual SQL invocation lifecycle, selected by the original call constructor.
/// This is compile-time source evidence, not a wire ABI or a name classifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PureCallLifecycle {
    Scalar,
    Aggregate,
    AggregateWindow,
    Window,
    Table,
}
impl PureCallLifecycle {
    pub const fn from_kind(kind: FunctionKind) -> Self {
        match kind {
            FunctionKind::Scalar => Self::Scalar,
            FunctionKind::Aggregate => Self::Aggregate,
            FunctionKind::Window => Self::Window,
            FunctionKind::Table => Self::Table,
        }
    }
    const fn accepts(self, abi: PureKernelAbi) -> bool {
        matches!(
            (self, abi),
            (
                Self::Scalar,
                PureKernelAbi::ScalarV1
                    | PureKernelAbi::ScalarInvocationV1
                    | PureKernelAbi::HigherOrderV1
                    | PureKernelAbi::ControlIntrinsicV1
            ) | (
                Self::Aggregate,
                PureKernelAbi::AggregateV1 | PureKernelAbi::AggregateWindowV1
            ) | (Self::AggregateWindow, PureKernelAbi::AggregateWindowV1)
                | (Self::Window, PureKernelAbi::WindowV1)
                | (Self::Table, PureKernelAbi::TableV1)
        )
    }
}

impl PureKernelAbi {
    /// Stable typed capability, independent of names or payload samples.
    pub const fn may_raise_invocation_data(self) -> bool {
        matches!(self, Self::ScalarInvocationV1)
    }
    const fn tag(self) -> u8 {
        match self {
            Self::ScalarV1 => 1,
            Self::ScalarInvocationV1 => 8,
            Self::HigherOrderV1 => 2,
            Self::AggregateV1 => 3,
            Self::AggregateWindowV1 => 4,
            Self::WindowV1 => 5,
            Self::TableV1 => 6,
            Self::ControlIntrinsicV1 => 7,
        }
    }
    fn accepts_preparation(self, options: &PureCallPreparation) -> bool {
        matches!(
            (self, options),
            (
                Self::ScalarV1 | Self::ScalarInvocationV1,
                PureCallPreparation::Scalar { .. }
            ) | (Self::HigherOrderV1, PureCallPreparation::HigherOrder(_))
                | (
                    Self::AggregateV1 | Self::AggregateWindowV1,
                    PureCallPreparation::Aggregate { .. },
                )
                | (
                    Self::AggregateWindowV1,
                    PureCallPreparation::AggregateWindow { .. }
                )
                | (Self::WindowV1, PureCallPreparation::Window { .. })
                | (Self::TableV1, PureCallPreparation::Table { .. })
                | (
                    Self::ControlIntrinsicV1,
                    PureCallPreparation::ControlIntrinsic { .. }
                )
        )
    }
    fn accepts(self, kind: FunctionKind, base: &FunctionEffectDeclaration) -> bool {
        match self {
            Self::ScalarV1 => {
                kind == FunctionKind::Scalar
                    && matches!(
                        base.argument_control,
                        ArgumentControl::Eager | ArgumentControl::TypeOnly
                    )
            }
            Self::ScalarInvocationV1 => {
                kind == FunctionKind::Scalar
                    && matches!(
                        base.argument_control,
                        ArgumentControl::Eager | ArgumentControl::NoArguments
                    )
                    && base.null_behavior == FunctionNullBehavior::CalledOnNull
            }
            Self::HigherOrderV1 => {
                kind == FunctionKind::Scalar
                    && matches!(base.argument_control, ArgumentControl::HigherOrder { .. })
            }
            Self::AggregateV1 | Self::AggregateWindowV1 => {
                kind == FunctionKind::Aggregate
                    && base.argument_control == ArgumentControl::Aggregate
            }
            Self::WindowV1 => {
                kind == FunctionKind::Window && base.argument_control == ArgumentControl::Window
            }
            Self::TableV1 => {
                kind == FunctionKind::Table && base.argument_control == ArgumentControl::Table
            }
            Self::ControlIntrinsicV1 => {
                kind == FunctionKind::Scalar
                    && matches!(
                        base.argument_control,
                        ArgumentControl::If
                            | ArgumentControl::Coalesce
                            | ArgumentControl::SimpleCase
                            | ArgumentControl::SearchedCase
                            | ArgumentControl::TemporalSource(_)
                    )
                    && (base.own_row_error == FunctionIntrinsicRowError::NoRowError
                        || (matches!(base.argument_control, ArgumentControl::TemporalSource(_))
                            && base.own_row_error == FunctionIntrinsicRowError::MayRaise))
                    && base.instance_state == FunctionInstanceState::None
                    && base.observable_effects == ObservableEffects::NONE
                    && base.environment_dependencies.is_empty()
                    && base.null_behavior == FunctionNullBehavior::ControlDefined
                    && base.value_stability == FunctionVolatility::Immutable
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PureImplementationDeclaration {
    pub overload: FunctionOverloadId,
    pub implementation: PureImplementationId,
    pub abi: PureKernelAbi,
}

/// Borrowed base metadata from one actually installed pure overload.
/// This can author invocation control topology, but does not validate a full
/// selected signature, refine effects/environment or authorize execution.
/// Fresh selected preparation against this same catalog remains mandatory.
pub struct PureOverloadDeclaration<'a> {
    attachment: &'a PureFunctionAttachment,
    implementation_index: usize,
    effects: &'a FunctionEffectDeclaration,
}
impl PureOverloadDeclaration<'_> {
    pub fn implementation(&self) -> &PureImplementationDeclaration {
        &self.attachment.implementations[self.implementation_index]
    }
    pub const fn effects(&self) -> &FunctionEffectDeclaration {
        self.effects
    }
    /// Borrow original selected argument demand from the same installed owner.
    /// Only ScalarInvocationV1 can introduce original demand-zero; metadata and
    /// other ABIs retain their exact original base. No input value is evaluated.
    pub fn selected_argument_control_observed(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
        logical_argument_count: usize,
        control: &dyn PureCompileControl,
    ) -> Result<ArgumentControl, FunctionBindingError> {
        if selected.overload != self.implementation().overload
            || logical_argument_count != selected.argument_types.len()
            || function
                != self
                    .attachment
                    .metadata_owner
                    .binding_declaration()
                    .function_id()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "selected argument demand has a foreign full signature".into(),
            ));
        }
        let actual = self
            .attachment
            .metadata_owner
            .selected_argument_control_observed(
                function,
                selected,
                logical_argument_count,
                control,
            )?;
        novarocks_type_contract::validate_selected_argument_control(
            self.effects.argument_control,
            actual,
        )
        .map_err(|_| {
            FunctionBindingError::InvalidBinding(
                "selected argument demand differs from its original declaration".into(),
            )
        })?;
        if actual != self.effects.argument_control
            && self.implementation().abi != PureKernelAbi::ScalarInvocationV1
        {
            return Err(FunctionBindingError::InvalidBinding(
                "original demand-zero requires its actual invocation lifecycle".into(),
            ));
        }
        Ok(actual)
    }
    /// Check the actual typed registration adapter, not a manifest copied from
    /// metadata. The original selected profile is borrowed without preparation.
    /// This proves installed lifecycle/static admission only; emitted-source
    /// facts, full environment refinement and preparation remain mandatory.
    pub fn admit_selected_lifecycle_observed(
        &self,
        selected: &FunctionBindingSelection,
        logical_argument_count: usize,
        lifecycle: PureCallLifecycle,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
        let implementation = self.implementation();
        if selected.overload != implementation.overload {
            return Err(FunctionBindingError::InvalidBinding(
                "lifecycle admission differs from the exact selected overload".into(),
            ));
        }
        if !self
            .attachment
            .owner
            .accepts_installed_abi(implementation.abi)
            || !lifecycle.accepts(implementation.abi)
        {
            return Err(FunctionBindingError::UnavailableImplementation(
                implementation.overload.clone(),
            ));
        }
        // Aggregate requests retain ORDER channels after the logical prefix.
        // Scalar/window/table argument lists retain exactly their logical N.
        let valid_count = match lifecycle {
            PureCallLifecycle::Aggregate | PureCallLifecycle::AggregateWindow => {
                logical_argument_count <= selected.argument_types.len()
            }
            _ => logical_argument_count == selected.argument_types.len(),
        };
        if !valid_count {
            return Err(FunctionBindingError::InvalidBinding(
                "lifecycle admission differs from the original logical argument count".into(),
            ));
        }
        self.attachment
            .metadata_owner
            .admit_selected_profile_observed(selected, logical_argument_count, control)
    }
    /// Check the actual owner's frozen semantic references without preparing
    /// a kernel, evaluating constants or refining runtime effects again.
    pub fn admit_frozen_environment_observed(
        &self,
        selected: &FunctionBindingSelection,
        environment: &[novarocks_type_contract::SemanticParameterRef],
        parameters: &novarocks_type_contract::SemanticParameters,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
        if selected.overload != self.implementation().overload {
            return Err(FunctionBindingError::InvalidBinding(
                "environment admission differs from its exact selected overload".into(),
            ));
        }
        self.attachment
            .metadata_owner
            .admit_frozen_environment_observed(selected, environment, parameters, control)
    }
    /// Borrow the exact installed metadata owner; no fresh resolution or
    /// prepared instance is constructed at this static FE admission boundary.
    pub fn admit_selected_profile_observed(
        &self,
        selected: &FunctionBindingSelection,
        logical_argument_count: usize,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
        if selected.overload != self.implementation().overload
            || selected.argument_types.len() != logical_argument_count
        {
            return Err(FunctionBindingError::InvalidBinding(
                "pure profile admission differs from its exact selected overload".into(),
            ));
        }
        self.attachment
            .metadata_owner
            .admit_selected_profile_observed(selected, logical_argument_count, control)
    }
}
impl fmt::Debug for PureOverloadDeclaration<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PureOverloadDeclaration")
            .field("implementation", self.implementation())
            .field("effects", self.effects())
            .finish()
    }
}

/// The same Arc supplies metadata, resolution, effect refinement and typed
/// preparation. Registration never accepts independently supplied operations.
/// The owner exposes immutable declarations; registration freezes one backing
/// and compares the selected base with the owner's refinement source.
pub trait PureFunctionMetadataOwner:
    FunctionBindingResolver + FunctionEffectOwner<Error = FunctionBindingError>
{
    fn binding_declaration(&self) -> &FunctionBindingDeclaration;
    fn implementation_declarations(&self) -> &[PureImplementationDeclaration];

    /// Candidate-only static profile admission from this same installed owner.
    /// The default preserves the existing presence contract. It grants no
    /// preparation, payload validation, environment fact or execution seal.
    fn admit_selected_profile_observed(
        &self,
        _selected: &FunctionBindingSelection,
        _logical_argument_count: usize,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
        Ok(())
    }
    /// An owner with semantic dependencies supplies its original checker.
    /// The empty-dependency branch grants only this exact environment shape;
    /// it is not a complete profile or runtime-effects certificate.
    fn admit_frozen_environment_observed(
        &self,
        selected: &FunctionBindingSelection,
        environment: &[novarocks_type_contract::SemanticParameterRef],
        _parameters: &novarocks_type_contract::SemanticParameters,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
        let base = self
            .binding_declaration()
            .effect_declaration(&selected.overload)?;
        if !base.environment_dependencies.is_empty() || !environment.is_empty() {
            return Err(FunctionBindingError::UnavailableImplementation(
                selected.overload.clone(),
            ));
        }
        Ok(())
    }
}

/// An independently assembled record of actually installed CPU/control
/// owners. Server must produce it from its real closed installation manifest,
/// not copy declarations from a metadata catalogue to manufacture coverage.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InstalledPureKernel {
    pub function: FunctionId,
    pub kind: FunctionKind,
    pub implementation: PureImplementationDeclaration,
    pub aggregate_state_format: Option<AggregateStateFormatIdentity>,
}

#[derive(Debug)]
pub enum PureCatalogError {
    Catalog(FunctionCatalogError),
    Binding(FunctionBindingError),
    MissingOwner(FunctionId),
    ImplementationCoverage(FunctionId),
    InvalidAbi {
        function: FunctionId,
        overload: FunctionOverloadId,
    },
    InstalledManifestMismatch,
}
impl From<FunctionCatalogError> for PureCatalogError {
    fn from(value: FunctionCatalogError) -> Self {
        Self::Catalog(value)
    }
}
impl From<FunctionBindingError> for PureCatalogError {
    fn from(value: FunctionBindingError) -> Self {
        Self::Binding(value)
    }
}
impl fmt::Display for PureCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => fmt::Display::fmt(error, formatter),
            Self::Binding(error) => fmt::Display::fmt(error, formatter),
            Self::MissingOwner(function) => write!(formatter,
                "function `{}` has no installed pure owner", function.as_str()),
            Self::ImplementationCoverage(function) => write!(formatter,
                "function `{}` has incomplete or duplicate exact implementation coverage", function.as_str()),
            Self::InvalidAbi { function, overload } => write!(formatter,
                "function `{}` overload `{}` has an incompatible installed ABI",
                function.as_str(), overload.as_str()),
            Self::InstalledManifestMismatch => formatter.write_str(
                "installed pure kernel manifest differs from exact declared implementation coverage"),
        }
    }
}
impl std::error::Error for PureCatalogError {}

/// Preparation options are typed by lifecycle, with no ignored universal
/// fields. A wrong variant fails before invoking any private prepare method.
#[derive(Clone, Debug)]
pub enum PureCallPreparation {
    Scalar {
        arguments: ScopedExpressionEffects,
    },
    HigherOrder(HigherOrderPreparationOptions),
    Aggregate {
        arguments: ScopedExpressionEffects,
        options: AggregatePreparationOptions,
    },
    AggregateWindow {
        arguments: ScopedExpressionEffects,
        options: AggregateWindowPreparationOptions,
    },
    Window {
        arguments: ScopedExpressionEffects,
        options: WindowCallOptions,
    },
    Table {
        arguments: ScopedExpressionEffects,
    },
    ControlIntrinsic {
        arguments: ScopedExpressionEffects,
    },
}

#[derive(Clone, Debug)]
pub enum PreparedPureKernel {
    Scalar(Arc<dyn PreparedScalarKernel>),
    ScalarInvocation(Arc<dyn PreparedInvocationScalarKernel>),
    HigherOrder(Arc<dyn PreparedHigherOrderKernel>),
    Aggregate(PreparedAggregateHandle),
    Window(Arc<dyn PreparedWindowKernel>),
    Table(Arc<dyn PreparedTableKernel>),
    /// Exact typed control facts for LocalProgram's intrinsic controller;
    /// this variant never pretends to be an ordinary evaluated-argument CPU.
    ControlIntrinsic(Arc<FunctionCallContract>),
}
impl PreparedPureKernel {
    /// Borrow the exact call owned by this resolved lifecycle implementation.
    /// Local compilation inspects one contract rather than reconstructing a
    /// second signature/effect model or dispatching by a SQL function name.
    pub fn call_contract(&self) -> &FunctionCallContract {
        match self {
            Self::Scalar(kernel) => kernel.contract().call(),
            Self::ScalarInvocation(kernel) => kernel.contract().call(),
            Self::HigherOrder(kernel) => kernel.contract().call(),
            Self::Aggregate(kernel) => kernel.contract().call(),
            Self::Window(kernel) => kernel.contract().call(),
            Self::Table(kernel) => kernel.contract().call(),
            Self::ControlIntrinsic(call) => call,
        }
    }
}

/// The catalogue records the actual validation route; a prepared variant or
/// function kind cannot substitute for this provenance on the frozen BE path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PurePreparationSource {
    Fresh,
    Frozen,
}

#[derive(Clone, Debug)]
pub struct PureCallSpecialization {
    prepared: PreparedPureKernel,
    effects: ScopedExpressionEffects,
    implementations: Arc<[PureImplementationDeclaration]>,
    implementation_index: usize,
    source: PurePreparationSource,
}
impl PureCallSpecialization {
    /// Preserve the original specialization/effect proof while preparing
    /// the local execution lifecycle of its actual aggregate owner.
    pub fn prepare_local_aggregate_stages(
        &mut self,
        control: &dyn PureCompileControl,
    ) -> Result<(), KernelFailure> {
        match &mut self.prepared {
            PreparedPureKernel::Aggregate(handle) => handle.prepare_local_stages(control),
            _ => Err(crate::kernel_control::invalid(
                "local stages require an aggregate specialization",
            )),
        }
    }
    /// Borrow the exact record frozen at atomic owner registration. Seven ABIs
    /// map to six prepared variants, so this is never inferred from a variant.
    pub fn implementation(&self) -> &PureImplementationDeclaration {
        &self.implementations[self.implementation_index]
    }
    pub const fn source(&self) -> PurePreparationSource {
        self.source
    }
    pub const fn prepared(&self) -> &PreparedPureKernel {
        &self.prepared
    }
    pub const fn effects(&self) -> ScopedExpressionEffects {
        self.effects
    }
    pub fn call_contract(&self) -> &FunctionCallContract {
        self.prepared.call_contract()
    }
    /// Keep both the resolved implementation and its composed, scoped effects
    /// when producing a local instruction. Consuming preparation does not
    /// instantiate mutable state or allocate runtime capabilities.
    pub fn into_parts(self) -> (PreparedPureKernel, ScopedExpressionEffects) {
        (self.prepared, self.effects)
    }
    pub fn into_prepared(self) -> PreparedPureKernel {
        self.prepared
    }
}

/// Private framework erasure operates once per preparation/batch. There is no
/// public Any/downcast/raw-op port and no runtime name resolver in a handle.
#[derive(Debug)]
struct PreparedPureCallDraft {
    prepared: PreparedPureKernel,
    effects: ScopedExpressionEffects,
}

trait InstalledPureOwner: Send + Sync {
    /// Implemented by the closed typed adapter that owns real preparation.
    /// Metadata alone cannot claim an uninstalled lifecycle.
    fn accepts_installed_abi(&self, abi: PureKernelAbi) -> bool;
    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: Option<&CallEffects>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedPureCallDraft, FunctionSpecializationFailure>;
}

#[derive(Clone)]
pub(crate) struct PureFunctionAttachment {
    implementations: Arc<[PureImplementationDeclaration]>,
    owner: Arc<dyn InstalledPureOwner>,
    metadata_owner: Arc<dyn PureFunctionMetadataOwner>,
}
impl fmt::Debug for PureFunctionAttachment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PureFunctionAttachment")
            .field("implementations", &self.implementations)
            .finish_non_exhaustive()
    }
}

/// Private single-backing adapter ensures the canonical declaration is the
/// refinement authority. The real owner must report the same selected base;
/// merely sharing an Arc does not permit two different effect sources.
struct RegisteredOwner<O> {
    declaration: Arc<FunctionBindingDeclaration>,
    owner: Arc<O>,
}
impl<O: PureFunctionMetadataOwner> FunctionBindingResolver for RegisteredOwner<O> {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.owner.resolve(request, control)
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.owner
            .select_at_overload_observed(overload, request, control)
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.owner.validate_selected(selected, request, control)
    }
}
impl<O: PureFunctionMetadataOwner> FunctionEffectOwner for RegisteredOwner<O> {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if function != self.declaration.function_id() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        self.declaration.effect_declaration(&selected.overload)
    }
    fn selected_argument_control_observed(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
        logical_argument_count: usize,
        control: &dyn PureCompileControl,
    ) -> Result<ArgumentControl, Self::Error> {
        // RegisteredOwner retains the original immutable base and delegates
        // selected demand to the SAME concrete metadata/effect owner.
        self.owner.selected_argument_control_observed(
            function,
            selected,
            logical_argument_count,
            control,
        )
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(FunctionEffectOwnerError::Control)?;
        let base = self
            .declaration(input.function_id, input.selected)
            .map_err(effect_binding_failure)?;
        if self
            .owner
            .declaration(input.function_id, input.selected)
            .map_err(effect_binding_failure)?
            != base
        {
            return Err(FunctionBindingError::InvalidBinding(
                "registered effect declaration differs from the exact implementation source".into(),
            )
            .into());
        }
        self.owner
            .validate_and_refine(input, control)
            .map_err(|error| match error {
                FunctionEffectOwnerError::Owner(error) => effect_binding_failure(error),
                control => control,
            })
    }
}

fn effect_binding_failure(
    error: FunctionBindingError,
) -> FunctionEffectOwnerError<FunctionBindingError> {
    match error {
        FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
        other => FunctionEffectOwnerError::Owner(other),
    }
}

macro_rules! forward_preparation {
    ($bound:ident, $method:ident, $contract:ty, $prepared:ty) => {
        impl<O: PureFunctionMetadataOwner + $bound> $bound for RegisteredOwner<O> {
            fn $method(
                &self,
                input: CallEffectInput<'_>,
                contract: Arc<$contract>,
                control: &dyn PureCompileControl,
            ) -> Result<Arc<$prepared>, KernelFailure> {
                self.owner.$method(input, contract, control)
            }
        }
    };
}
forward_preparation!(
    PureScalarImplementation,
    prepare_scalar,
    ScalarCallContract,
    dyn PreparedScalarKernel
);
forward_preparation!(
    PureInvocationScalarImplementation,
    prepare_invocation_scalar,
    ScalarCallContract,
    dyn PreparedInvocationScalarKernel
);
forward_preparation!(
    PureWindowImplementation,
    prepare_window,
    WindowCallContract,
    dyn PreparedWindowKernel
);
forward_preparation!(
    PureTableImplementation,
    prepare_table,
    TableCallContract,
    dyn PreparedTableKernel
);
impl<O: PureFunctionMetadataOwner + PureHigherOrderImplementation> PureHigherOrderImplementation
    for RegisteredOwner<O>
{
    fn prepare_higher_order(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<HigherOrderCallContract>,
        body: Arc<LambdaBodyContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedHigherOrderKernel>, KernelFailure> {
        self.owner
            .prepare_higher_order(input, contract, body, control)
    }
}
impl<O: PureFunctionMetadataOwner + PureAggregateImplementation> PureAggregateImplementation
    for RegisteredOwner<O>
{
    type Kernel = O::Kernel;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self::Kernel>, KernelFailure> {
        self.owner.prepare_aggregate(input, contract, control)
    }
}
impl<O: PureFunctionMetadataOwner + PureAggregateWindowImplementation>
    PureAggregateWindowImplementation for RegisteredOwner<O>
{
    fn prepare_aggregate_for_window(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self::Kernel>, KernelFailure> {
        self.owner
            .prepare_aggregate_for_window(input, contract, control)
    }
    fn prepare_aggregate_window(
        &self,
        aggregate: Arc<Self::Kernel>,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
        self.owner
            .prepare_aggregate_window(aggregate, contract, control)
    }
}

struct ScalarOwner<O>(Arc<RegisteredOwner<O>>);
struct InvocationScalarOwner<O>(Arc<RegisteredOwner<O>>);
struct HigherOrderOwner<O>(Arc<RegisteredOwner<O>>);
struct ScalarHigherOrderOwner<O>(Arc<RegisteredOwner<O>>);
struct AggregateOwner<O>(Arc<RegisteredOwner<O>>);
struct AggregateWindowOwner<O>(Arc<RegisteredOwner<O>>);
struct WindowOwner<O>(Arc<RegisteredOwner<O>>);
struct TableOwner<O>(Arc<RegisteredOwner<O>>);
struct ControlOwner<O>(Arc<RegisteredOwner<O>>);

fn wrong_options() -> FunctionSpecializationFailure {
    FunctionSpecializationFailure::InvalidInput(
        "prepare options differ from the installed exact lifecycle ABI",
    )
}

impl<O: PureFunctionMetadataOwner + PureScalarImplementation> InstalledPureOwner
    for ScalarOwner<O>
{
    fn accepts_installed_abi(&self, abi: PureKernelAbi) -> bool {
        matches!(abi, PureKernelAbi::ScalarV1)
    }
    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: Option<&CallEffects>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedPureCallDraft, FunctionSpecializationFailure> {
        let PureCallPreparation::Scalar { arguments } = options else {
            return Err(wrong_options());
        };
        let value = match frozen {
            Some(frozen) => specialize_frozen_scalar(
                self.0.as_ref(),
                input,
                selected,
                frozen,
                arguments,
                control,
            ),
            None => specialize_scalar(self.0.as_ref(), input, selected, arguments, control),
        }?;
        Ok(PreparedPureCallDraft {
            effects: value.effects(),
            prepared: PreparedPureKernel::Scalar(value.into_prepared()),
        })
    }
}
impl<O: PureFunctionMetadataOwner + PureInvocationScalarImplementation> InstalledPureOwner
    for InvocationScalarOwner<O>
{
    fn accepts_installed_abi(&self, abi: PureKernelAbi) -> bool {
        matches!(abi, PureKernelAbi::ScalarInvocationV1)
    }
    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: Option<&CallEffects>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedPureCallDraft, FunctionSpecializationFailure> {
        let PureCallPreparation::Scalar { arguments } = options else {
            return Err(wrong_options());
        };
        let value = match frozen {
            Some(frozen) => specialize_frozen_invocation_scalar(
                self.0.as_ref(),
                input,
                selected,
                frozen,
                arguments,
                control,
            ),
            None => {
                specialize_invocation_scalar(self.0.as_ref(), input, selected, arguments, control)
            }
        }?;
        Ok(PreparedPureCallDraft {
            effects: value.effects(),
            prepared: PreparedPureKernel::ScalarInvocation(value.into_prepared()),
        })
    }
}
impl<O: PureFunctionMetadataOwner + PureHigherOrderImplementation> InstalledPureOwner
    for HigherOrderOwner<O>
{
    fn accepts_installed_abi(&self, abi: PureKernelAbi) -> bool {
        matches!(abi, PureKernelAbi::HigherOrderV1)
    }
    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: Option<&CallEffects>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedPureCallDraft, FunctionSpecializationFailure> {
        let PureCallPreparation::HigherOrder(options) = options else {
            return Err(wrong_options());
        };
        let value = match frozen {
            Some(frozen) => specialize_frozen_higher_order(
                self.0.as_ref(),
                input,
                selected,
                frozen,
                options,
                control,
            ),
            None => specialize_higher_order(self.0.as_ref(), input, selected, options, control),
        }?;
        Ok(PreparedPureCallDraft {
            effects: value.effects(),
            prepared: PreparedPureKernel::HigherOrder(value.into_prepared()),
        })
    }
}
impl<O: PureFunctionMetadataOwner + PureScalarImplementation + PureHigherOrderImplementation>
    InstalledPureOwner for ScalarHigherOrderOwner<O>
{
    fn accepts_installed_abi(&self, abi: PureKernelAbi) -> bool {
        matches!(abi, PureKernelAbi::ScalarV1 | PureKernelAbi::HigherOrderV1)
    }
    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: Option<&CallEffects>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedPureCallDraft, FunctionSpecializationFailure> {
        match options {
            options @ PureCallPreparation::Scalar { .. } => {
                ScalarOwner(self.0.clone()).prepare(input, selected, frozen, options, control)
            }
            options @ PureCallPreparation::HigherOrder(_) => {
                HigherOrderOwner(self.0.clone()).prepare(input, selected, frozen, options, control)
            }
            _ => Err(wrong_options()),
        }
    }
}
impl<O: PureFunctionMetadataOwner + PureAggregateImplementation> InstalledPureOwner
    for AggregateOwner<O>
{
    fn accepts_installed_abi(&self, abi: PureKernelAbi) -> bool {
        matches!(abi, PureKernelAbi::AggregateV1)
    }
    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: Option<&CallEffects>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedPureCallDraft, FunctionSpecializationFailure> {
        let PureCallPreparation::Aggregate { arguments, options } = options else {
            return Err(wrong_options());
        };
        prepare_aggregate_handle(
            self.0.as_ref(),
            input,
            selected,
            frozen,
            arguments,
            options,
            control,
        )
    }
}
fn prepare_aggregate_handle<O: PureAggregateImplementation>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    arguments: ScopedExpressionEffects,
    options: AggregatePreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<PreparedPureCallDraft, FunctionSpecializationFailure> {
    let value = match frozen {
        Some(frozen) => {
            specialize_frozen_aggregate(owner, input, selected, frozen, arguments, options, control)
        }
        None => specialize_aggregate(owner, input, selected, arguments, options, control),
    }?;
    let effects = value.effects();
    let handle = PreparedAggregateHandle::from_typed(value.into_prepared(), control)
        .map_err(FunctionSpecializationFailure::Kernel)?;
    Ok(PreparedPureCallDraft {
        effects,
        prepared: PreparedPureKernel::Aggregate(handle),
    })
}
impl<O: PureFunctionMetadataOwner + PureAggregateWindowImplementation> InstalledPureOwner
    for AggregateWindowOwner<O>
{
    fn accepts_installed_abi(&self, abi: PureKernelAbi) -> bool {
        matches!(
            abi,
            PureKernelAbi::AggregateV1 | PureKernelAbi::AggregateWindowV1
        )
    }
    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: Option<&CallEffects>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedPureCallDraft, FunctionSpecializationFailure> {
        match options {
            PureCallPreparation::Aggregate { arguments, options } => prepare_aggregate_handle(
                self.0.as_ref(),
                input,
                selected,
                frozen,
                arguments,
                options,
                control,
            ),
            PureCallPreparation::AggregateWindow { arguments, options } => {
                let value = match frozen {
                    Some(frozen) => specialize_frozen_aggregate_window(
                        self.0.as_ref(),
                        input,
                        selected,
                        frozen,
                        arguments,
                        options,
                        control,
                    ),
                    None => specialize_aggregate_window(
                        self.0.as_ref(),
                        input,
                        selected,
                        arguments,
                        options,
                        control,
                    ),
                }?;
                Ok(PreparedPureCallDraft {
                    effects: value.effects(),
                    prepared: PreparedPureKernel::Window(value.into_prepared()),
                })
            }
            _ => Err(wrong_options()),
        }
    }
}
impl<O: PureFunctionMetadataOwner + PureWindowImplementation> InstalledPureOwner
    for WindowOwner<O>
{
    fn accepts_installed_abi(&self, abi: PureKernelAbi) -> bool {
        matches!(abi, PureKernelAbi::WindowV1)
    }
    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: Option<&CallEffects>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedPureCallDraft, FunctionSpecializationFailure> {
        let PureCallPreparation::Window { arguments, options } = options else {
            return Err(wrong_options());
        };
        let value = match frozen {
            Some(frozen) => specialize_frozen_window(
                self.0.as_ref(),
                input,
                selected,
                frozen,
                arguments,
                options,
                control,
            ),
            None => specialize_window(
                self.0.as_ref(),
                input,
                selected,
                arguments,
                options,
                control,
            ),
        }?;
        Ok(PreparedPureCallDraft {
            effects: value.effects(),
            prepared: PreparedPureKernel::Window(value.into_prepared()),
        })
    }
}
impl<O: PureFunctionMetadataOwner + PureTableImplementation> InstalledPureOwner for TableOwner<O> {
    fn accepts_installed_abi(&self, abi: PureKernelAbi) -> bool {
        matches!(abi, PureKernelAbi::TableV1)
    }
    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: Option<&CallEffects>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedPureCallDraft, FunctionSpecializationFailure> {
        let PureCallPreparation::Table { arguments } = options else {
            return Err(wrong_options());
        };
        let value = match frozen {
            Some(frozen) => specialize_frozen_table(
                self.0.as_ref(),
                input,
                selected,
                frozen,
                arguments,
                control,
            ),
            None => specialize_table(self.0.as_ref(), input, selected, arguments, control),
        }?;
        Ok(PreparedPureCallDraft {
            effects: value.effects(),
            prepared: PreparedPureKernel::Table(value.into_prepared()),
        })
    }
}
impl<O: PureFunctionMetadataOwner> InstalledPureOwner for ControlOwner<O> {
    fn accepts_installed_abi(&self, abi: PureKernelAbi) -> bool {
        matches!(abi, PureKernelAbi::ControlIntrinsicV1)
    }
    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: Option<&CallEffects>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedPureCallDraft, FunctionSpecializationFailure> {
        let PureCallPreparation::ControlIntrinsic { arguments } = options else {
            return Err(wrong_options());
        };
        let (receipt, effects) = crate::specialization::refine_once_for_specialization(
            self.0.as_ref(),
            input,
            frozen,
            arguments,
            control,
        )?;
        let call = Arc::new(
            FunctionCallContract::from_refined(input, &receipt, selected, control)
                .map_err(FunctionSpecializationFailure::Kernel)?,
        );
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(FunctionSpecializationFailure::Control)?;
        Ok(PreparedPureCallDraft {
            effects,
            prepared: PreparedPureKernel::ControlIntrinsic(call),
        })
    }
}

type RegisteredParts<O> = (
    Arc<RegisteredOwner<O>>,
    Arc<[PureImplementationDeclaration]>,
);

fn register_owner<O: PureFunctionMetadataOwner + 'static>(
    owner: Arc<O>,
    allowed: &[PureKernelAbi],
) -> Result<RegisteredParts<O>, PureCatalogError> {
    let declaration = owner.binding_declaration();
    declaration.validate_complete_effects()?;
    let mut implementations = owner.implementation_declarations().to_vec();
    implementations.sort_unstable();
    if implementations.len() != declaration.overloads().len()
        || implementations
            .windows(2)
            .any(|rows| rows[0].overload == rows[1].overload)
    {
        return Err(PureCatalogError::ImplementationCoverage(
            declaration.function_id().clone(),
        ));
    }
    for (overload, implementation) in declaration.overloads().iter().zip(&implementations) {
        if overload.identity != implementation.overload {
            return Err(PureCatalogError::ImplementationCoverage(
                declaration.function_id().clone(),
            ));
        }
        if !allowed.contains(&implementation.abi)
            || (declaration.has_scalar_invocation_data(&overload.identity)?
                && implementation.abi != PureKernelAbi::ScalarInvocationV1)
            || !implementation.abi.accepts(
                declaration.kind(),
                declaration.effect_declaration(&overload.identity)?,
            )
        {
            return Err(PureCatalogError::InvalidAbi {
                function: declaration.function_id().clone(),
                overload: overload.identity.clone(),
            });
        }
    }
    let declaration = Arc::new(declaration.clone());
    Ok((
        Arc::new(RegisteredOwner { declaration, owner }),
        implementations.into(),
    ))
}

fn attach_definition<O: PureFunctionMetadataOwner + 'static>(
    name: impl AsRef<str>,
    visibility: FunctionVisibility,
    registered: Arc<RegisteredOwner<O>>,
    implementations: Arc<[PureImplementationDeclaration]>,
    owner: Arc<dyn InstalledPureOwner>,
    aggregate_resolver: Option<Arc<dyn AggregateSignatureResolver>>,
) -> Result<FunctionDefinition, PureCatalogError> {
    let mut definition = FunctionDefinition::bound(
        name,
        visibility,
        registered.declaration.as_ref().clone(),
        registered.clone(),
        aggregate_resolver,
    )?;
    let binding = definition
        .binding
        .as_mut()
        .expect("bound constructor returns a binding");
    binding.declaration = registered.declaration.clone();
    binding.pure = Some(PureFunctionAttachment {
        implementations,
        owner,
        metadata_owner: registered.owner.clone(),
    });
    Ok(definition)
}

macro_rules! ordinary_registration {
    ($method:ident, $bound:ident, $adapter:ident, $abi:ident) => {
        impl FunctionDefinition {
            pub fn $method<O: PureFunctionMetadataOwner + $bound + 'static>(
                name: impl AsRef<str>,
                visibility: FunctionVisibility,
                owner: Arc<O>,
            ) -> Result<Self, PureCatalogError> {
                let (registered, implementations) = register_owner(owner, &[PureKernelAbi::$abi])?;
                attach_definition(
                    name,
                    visibility,
                    registered.clone(),
                    implementations,
                    Arc::new($adapter(registered)),
                    None,
                )
            }
        }
    };
}
ordinary_registration!(
    try_new_pure_scalar,
    PureScalarImplementation,
    ScalarOwner,
    ScalarV1
);
ordinary_registration!(
    try_new_pure_invocation_scalar,
    PureInvocationScalarImplementation,
    InvocationScalarOwner,
    ScalarInvocationV1
);
ordinary_registration!(
    try_new_pure_higher_order,
    PureHigherOrderImplementation,
    HigherOrderOwner,
    HigherOrderV1
);
ordinary_registration!(
    try_new_pure_window,
    PureWindowImplementation,
    WindowOwner,
    WindowV1
);
ordinary_registration!(
    try_new_pure_table,
    PureTableImplementation,
    TableOwner,
    TableV1
);
impl FunctionDefinition {
    pub fn try_new_pure_scalar_higher_order<
        O: PureFunctionMetadataOwner
            + PureScalarImplementation
            + PureHigherOrderImplementation
            + 'static,
    >(
        name: impl AsRef<str>,
        visibility: FunctionVisibility,
        owner: Arc<O>,
    ) -> Result<Self, PureCatalogError> {
        let (registered, implementations) = register_owner(
            owner,
            &[PureKernelAbi::ScalarV1, PureKernelAbi::HigherOrderV1],
        )?;
        attach_definition(
            name,
            visibility,
            registered.clone(),
            implementations,
            Arc::new(ScalarHigherOrderOwner(registered)),
            None,
        )
    }
    pub fn try_new_pure_control<O: PureFunctionMetadataOwner + 'static>(
        name: impl AsRef<str>,
        visibility: FunctionVisibility,
        owner: Arc<O>,
    ) -> Result<Self, PureCatalogError> {
        let (registered, implementations) =
            register_owner(owner, &[PureKernelAbi::ControlIntrinsicV1])?;
        attach_definition(
            name,
            visibility,
            registered.clone(),
            implementations,
            Arc::new(ControlOwner(registered)),
            None,
        )
    }
    /// The retained legacy signature port is supplied by the same owner. Pure
    /// compilation only uses exact bindings/typed prepare and never this port.
    pub fn try_new_pure_aggregate<
        O: PureFunctionMetadataOwner
            + PureAggregateImplementation
            + AggregateSignatureResolver
            + 'static,
    >(
        name: impl AsRef<str>,
        visibility: FunctionVisibility,
        owner: Arc<O>,
    ) -> Result<Self, PureCatalogError> {
        let legacy = owner.clone();
        let (registered, implementations) = register_owner(owner, &[PureKernelAbi::AggregateV1])?;
        attach_definition(
            name,
            visibility,
            registered.clone(),
            implementations,
            Arc::new(AggregateOwner(registered)),
            Some(legacy),
        )
    }
    pub fn try_new_pure_aggregate_window<
        O: PureFunctionMetadataOwner
            + PureAggregateWindowImplementation
            + AggregateSignatureResolver
            + 'static,
    >(
        name: impl AsRef<str>,
        visibility: FunctionVisibility,
        owner: Arc<O>,
    ) -> Result<Self, PureCatalogError> {
        let legacy = owner.clone();
        let (registered, implementations) = register_owner(
            owner,
            &[PureKernelAbi::AggregateV1, PureKernelAbi::AggregateWindowV1],
        )?;
        attach_definition(
            name,
            visibility,
            registered.clone(),
            implementations,
            Arc::new(AggregateWindowOwner(registered)),
            Some(legacy),
        )
    }
}

/// This type can only be created by complete metadata/typed-owner/independent
/// installed-manifest closure. A metadata-only EngineFunctionCatalog cannot
/// become this type through a public constructor or unchecked conversion.
#[derive(Clone, Debug)]
pub struct PureEngineFunctionCatalog {
    catalog: EngineFunctionCatalog,
}
impl EngineFunctionCatalogBuilder {
    pub fn seal_pure(
        self,
        installed: impl IntoIterator<Item = InstalledPureKernel>,
    ) -> Result<PureEngineFunctionCatalog, PureCatalogError> {
        let mut expected = BTreeMap::new();
        for definition in self.definitions() {
            let binding = definition.binding.as_ref().ok_or_else(|| {
                PureCatalogError::Catalog(FunctionCatalogError::MissingBindingDeclaration {
                    name: definition.canonical_name.clone(),
                    kind: definition.kind(),
                })
            })?;
            binding.declaration.validate_complete_effects()?;
            let attachment = binding.pure.as_ref().ok_or_else(|| {
                PureCatalogError::MissingOwner(binding.declaration.function_id().clone())
            })?;
            for (overload, implementation) in binding
                .declaration
                .overloads()
                .iter()
                .zip(attachment.implementations.iter())
            {
                let base = binding
                    .declaration
                    .effect_declaration(&implementation.overload)?;
                if !implementation.abi.accepts(binding.declaration.kind(), base)
                    || (binding
                        .declaration
                        .has_scalar_invocation_data(&overload.identity)?
                        && implementation.abi != PureKernelAbi::ScalarInvocationV1)
                {
                    return Err(PureCatalogError::InvalidAbi {
                        function: binding.declaration.function_id().clone(),
                        overload: implementation.overload.clone(),
                    });
                }
                let record = InstalledPureKernel {
                    function: binding.declaration.function_id().clone(),
                    kind: binding.declaration.kind(),
                    implementation: implementation.clone(),
                    aggregate_state_format: overload
                        .aggregate
                        .as_ref()
                        .map(|aggregate| aggregate.state_format.clone()),
                };
                expected.insert(
                    (
                        record.function.clone(),
                        record.implementation.overload.clone(),
                    ),
                    record,
                );
            }
        }
        let mut actual = BTreeMap::new();
        for record in installed {
            if actual
                .insert(
                    (
                        record.function.clone(),
                        record.implementation.overload.clone(),
                    ),
                    record,
                )
                .is_some()
            {
                return Err(PureCatalogError::InstalledManifestMismatch);
            }
        }
        if expected != actual {
            return Err(PureCatalogError::InstalledManifestMismatch);
        }
        Ok(PureEngineFunctionCatalog {
            catalog: self.seal_bound()?,
        })
    }
}
impl PureEngineFunctionCatalog {
    pub fn metadata(&self) -> &EngineFunctionCatalog {
        &self.catalog
    }
    pub fn digest(&self) -> [u8; 32] {
        self.catalog.digest()
    }
    pub fn prepare_fresh(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
        self.prepare(input, selected, None, options, control)
    }
    /// BE always supplies frozen facts. There is no public optional/fresh
    /// fallback on the frozen path and no SQL name parameter to re-resolve.
    pub fn prepare_frozen(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: &CallEffects,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
        self.prepare(input, selected, Some(frozen), options, control)
    }
    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: Option<&CallEffects>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
        prepare_selected(&self.catalog, input, selected, frozen, options, control)
    }
}

impl EngineFunctionCatalog {
    /// Look up one registered base by exact identities without resolving a
    /// SQL name or constructing a selected signature. A metadata-only entry
    /// has no capability here. This loan uses the same attachment and effect
    /// declaration consulted by actual selected preparation below.
    pub fn pure_overload_declaration_observed<'a>(
        &'a self,
        function_id: &FunctionId,
        kind: FunctionKind,
        overload: &FunctionOverloadId,
        control: &dyn PureCompileControl,
    ) -> Result<PureOverloadDeclaration<'a>, FunctionSpecializationFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            let binding = lookup_selected_binding(self, function_id, &mut work)?;
            let exact_kind = binding.declaration.kind() == kind;
            work.step()?;
            if !exact_kind {
                return Err(FunctionSpecializationFailure::InvalidInput(
                    "pure preparation has a different exact kind or selected owner",
                ));
            }
            lookup_installed_overload(binding, overload, &mut work)
        })();
        if matches!(&result, Err(FunctionSpecializationFailure::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    /// Prepare one exact selected implementation from this catalogue's
    /// original installed attachment. This grants no whole-catalogue seal and
    /// never resolves a SQL name or manufactures missing implementation facts.
    pub fn prepare_fresh_selected(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
        prepare_selected(self, input, selected, None, options, control)
    }
}

fn lookup_selected_binding<'a>(
    catalog: &'a EngineFunctionCatalog,
    function_id: &FunctionId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionBindingDefinition, FunctionSpecializationFailure> {
    let definition = catalog.definition_by_id(function_id);
    work.step()?;
    let definition = definition.ok_or(FunctionSpecializationFailure::Binding(
        FunctionBindingError::UnknownFunction,
    ))?;
    let binding = definition.binding.as_ref();
    work.step()?;
    binding.ok_or(FunctionSpecializationFailure::Binding(
        FunctionBindingError::MissingBindingDeclaration,
    ))
}

fn lookup_installed_overload<'a>(
    binding: &'a FunctionBindingDefinition,
    overload: &FunctionOverloadId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PureOverloadDeclaration<'a>, FunctionSpecializationFailure> {
    let attachment = binding.pure.as_ref();
    work.step()?;
    let attachment = attachment.ok_or_else(|| {
        FunctionSpecializationFailure::MissingPureImplementation(overload.clone())
    })?;
    let effects = binding.declaration.effect_declaration(overload);
    work.step()?;
    let effects = effects?;
    let index = attachment
        .implementations
        .binary_search_by(|record| record.overload.cmp(overload));
    work.step()?;
    let implementation_index = index.map_err(|_| {
        FunctionSpecializationFailure::Binding(FunctionBindingError::UnknownOverload(
            overload.clone(),
        ))
    })?;
    Ok(PureOverloadDeclaration {
        attachment,
        implementation_index,
        effects,
    })
}

fn prepare_selected(
    catalog: &EngineFunctionCatalog,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    options: PureCallPreparation,
    control: &dyn PureCompileControl,
) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        let binding = lookup_selected_binding(catalog, input.function_id, &mut work)?;
        let exact_owner = binding.declaration.kind() == input.kind
            && std::ptr::eq(input.selected, selected.as_ref());
        work.step()?;
        if !exact_owner {
            return Err(FunctionSpecializationFailure::InvalidInput(
                "pure preparation has a different exact kind or selected owner",
            ));
        }
        // Use the same complete selected-signature author as frozen metadata
        // consumers. Owner refinement below still validates its own facts.
        work.flush()?;
        catalog.validate_frozen_selection(
            input.function_id,
            input.kind,
            selected.as_ref(),
            input.request,
            control,
        )?;
        work.flush()?;
        let declaration = lookup_installed_overload(binding, &selected.overload, &mut work)?;
        let attachment = declaration.attachment;
        let index = declaration.implementation_index;
        let accepts = attachment.implementations[index]
            .abi
            .accepts_preparation(&options);
        work.step()?;
        if !accepts {
            return Err(wrong_options());
        }
        work.flush()?;
        let draft = attachment
            .owner
            .prepare(input, selected, frozen, options, control)?;
        work.flush()?;
        Ok(PureCallSpecialization {
            prepared: draft.prepared,
            effects: draft.effects,
            implementations: attachment.implementations.clone(),
            implementation_index: index,
            source: if frozen.is_some() {
                PurePreparationSource::Frozen
            } else {
                PurePreparationSource::Fresh
            },
        })
    })();
    // Preparation may forward an original compile refusal through the kernel
    // boundary. Both typed representations remain primary without a callback.
    if matches!(
        result,
        Err(FunctionSpecializationFailure::Control(_))
            | Err(FunctionSpecializationFailure::Kernel(
                KernelFailure::Cancelled
                    | KernelFailure::DeadlineExceeded
                    | KernelFailure::ResourceExhausted
            ))
    ) {
        return result;
    }
    work.finish()?;
    result
}

pub(crate) fn digest_pure_attachment(
    hasher: &mut Sha256,
    attachment: Option<&PureFunctionAttachment>,
) {
    let Some(attachment) = attachment else {
        hasher.update([0]);
        return;
    };
    hasher.update([1]);
    hasher.update((attachment.implementations.len() as u64).to_be_bytes());
    for implementation in attachment.implementations.iter() {
        digest_text(hasher, implementation.overload.as_str());
        digest_text(hasher, implementation.implementation.as_str());
        hasher.update([implementation.abi.tag()]);
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod typed_tests;

#[cfg(test)]
#[path = "pure_catalogue/selected_fresh_tests.rs"]
mod selected_fresh_tests;

#[cfg(test)]
#[path = "pure_overload_declaration_tests.rs"]
mod pure_overload_declaration_tests;
