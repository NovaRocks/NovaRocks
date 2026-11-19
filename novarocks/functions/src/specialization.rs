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

//! Shared single-owner refinement handoff to dedicated lifecycle preparations.

use crate::{
    CallEffectInput, CallEffectRefinementError, FunctionBindingError, FunctionBindingSelection,
    FunctionEffectOwner, FunctionId, KernelFailure, RefinedCallEffects, ScopedExpressionEffects,
};
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CompileControlError, CompilePhase, PureCompileControl,
};
use std::fmt;

#[derive(Debug)]
pub enum FunctionSpecializationFailure {
    Binding(FunctionBindingError),
    Effects(novarocks_type_contract::EffectContractError),
    Control(CompileControlError),
    Kernel(KernelFailure),
    /// The selected overload has no pure implementation installed in this
    /// catalog; it names the overload so the missing owner is identifiable.
    MissingPureImplementation(crate::FunctionOverloadId),
    InvalidInput(&'static str),
}
impl From<KernelFailure> for FunctionSpecializationFailure {
    fn from(error: KernelFailure) -> Self {
        match error {
            KernelFailure::CompileControl(cause) => Self::Control(cause),
            other => Self::Kernel(other),
        }
    }
}
impl FunctionSpecializationFailure {
    pub(crate) fn is_interruption(&self) -> bool {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        matches!(
            self,
            Self::Control(_) | Self::Binding(FunctionBindingError::Control(_))
        ) || matches!(self, Self::Kernel(cause) if cause.is_interruption())
            || matches!(
                self.failure_fact().class(),
                FailureClass::Control | FailureClass::Resource
            )
    }
}
impl fmt::Display for FunctionSpecializationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binding(e) => fmt::Display::fmt(e, f),
            Self::Effects(e) => fmt::Display::fmt(e, f),
            Self::Control(e) => fmt::Display::fmt(e, f),
            Self::Kernel(e) => fmt::Display::fmt(e, f),
            Self::MissingPureImplementation(overload) => write!(
                f,
                "selected overload `{}` has no installed pure implementation",
                overload.as_str()
            ),
            Self::InvalidInput(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for FunctionSpecializationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Binding(cause) => Some(cause),
            Self::Effects(cause) => Some(cause),
            Self::Control(cause) => Some(cause),
            Self::Kernel(cause) => Some(cause),
            Self::MissingPureImplementation(_) | Self::InvalidInput(_) => None,
        }
    }
}

pub(crate) fn refine_once_for_specialization<
    'a,
    O: FunctionEffectOwner<Error = FunctionBindingError> + ?Sized,
    S: ScopedArgumentEffects,
>(
    owner: &O,
    input: CallEffectInput<'a>,
    frozen: Option<&CallEffects>,
    arguments: S,
    control: &dyn PureCompileControl,
) -> Result<(RefinedCallEffects<'a>, S), FunctionSpecializationFailure> {
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    let receipt = match frozen {
        Some(frozen) => crate::validate_frozen_call_effects(owner, input, frozen, control),
        None => crate::refine_call_effects(owner, input, control),
    }
    .map_err(|error| match error {
        CallEffectRefinementError::Owner(error) => FunctionSpecializationFailure::from(error),
        CallEffectRefinementError::Control(error) => FunctionSpecializationFailure::Control(error),
        CallEffectRefinementError::Contract(error) => FunctionSpecializationFailure::Effects(error),
        CallEffectRefinementError::InvalidInput(error) => {
            FunctionSpecializationFailure::InvalidInput(error)
        }
    })?;
    let effects = arguments
        .compose_for_call(&receipt, input)
        .map_err(FunctionSpecializationFailure::Effects)?;
    Ok((receipt, effects))
}

impl From<CompileControlError> for FunctionSpecializationFailure {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

impl From<FunctionBindingError> for FunctionSpecializationFailure {
    fn from(error: FunctionBindingError) -> Self {
        match error {
            FunctionBindingError::Control(error) => Self::Control(error),
            other => Self::Binding(other),
        }
    }
}

pub(crate) struct CheckedEffectAdapter<'a, O: ?Sized> {
    pub(crate) owner: &'a O,
    pub(crate) checked: &'a crate::CheckedCallEffectInput<'a>,
}
impl<O: FunctionEffectOwner<Error = FunctionBindingError> + ?Sized> FunctionEffectOwner
    for CheckedEffectAdapter<'_, O>
{
    type Error = crate::CheckedOwnerFailure<FunctionBindingError>;
    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&novarocks_type_contract::FunctionEffectDeclaration, Self::Error> {
        self.owner
            .declaration(function, selected)
            .map_err(crate::CheckedOwnerFailure::Original)
    }
    fn selected_argument_control_observed(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
        logical_argument_count: usize,
        control: &dyn PureCompileControl,
    ) -> Result<ArgumentControl, Self::Error> {
        self.owner
            .selected_argument_control_observed(function, selected, logical_argument_count, control)
            .map_err(crate::CheckedOwnerFailure::Original)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, crate::FunctionEffectOwnerError<Self::Error>> {
        match self
            .owner
            .validate_and_refine_checked(input, self.checked, control)
        {
            Ok(effects) => Ok(effects),
            Err(crate::CheckedOwnerFailure::Original(
                crate::FunctionEffectOwnerError::Control(error),
            )) => Err(crate::FunctionEffectOwnerError::Control(error)),
            Err(crate::CheckedOwnerFailure::Original(crate::FunctionEffectOwnerError::Owner(
                error,
            ))) => Err(crate::FunctionEffectOwnerError::Owner(
                crate::CheckedOwnerFailure::Original(error),
            )),
            Err(crate::CheckedOwnerFailure::UnconvertedOwner) => {
                Err(crate::FunctionEffectOwnerError::Owner(
                    crate::CheckedOwnerFailure::UnconvertedOwner,
                ))
            }
        }
    }
}
pub(crate) fn checked_refinement_failure(
    error: crate::CallEffectRefinementError<crate::CheckedOwnerFailure<FunctionBindingError>>,
) -> crate::CheckedOwnerFailure<FunctionSpecializationFailure> {
    use crate::{CallEffectRefinementError as E, CheckedOwnerFailure as C};
    match error {
        E::Owner(C::UnconvertedOwner) => C::UnconvertedOwner,
        E::Owner(C::Original(error)) => C::Original(error.into()),
        E::Control(error) => C::Original(FunctionSpecializationFailure::Control(error)),
        E::Contract(error) => C::Original(FunctionSpecializationFailure::Effects(error)),
        E::InvalidInput(error) => C::Original(FunctionSpecializationFailure::InvalidInput(error)),
    }
}

pub(crate) fn refine_once_checked<
    'a,
    O: FunctionEffectOwner<Error = FunctionBindingError> + ?Sized,
    S: ScopedArgumentEffects,
>(
    owner: &O,
    checked: &crate::CheckedCallEffectInput<'_>,
    input: CallEffectInput<'a>,
    frozen: Option<&CallEffects>,
    arguments: S,
    control: &dyn PureCompileControl,
) -> Result<(RefinedCallEffects<'a>, S), crate::CheckedOwnerFailure<FunctionSpecializationFailure>>
{
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    let adapter = CheckedEffectAdapter { owner, checked };
    let receipt = match frozen {
        Some(frozen) => crate::validate_frozen_call_effects(&adapter, input, frozen, control),
        None => crate::refine_call_effects(&adapter, input, control),
    }
    .map_err(checked_refinement_failure)?;
    let effects = arguments
        .compose_for_call(&receipt, input)
        .map_err(FunctionSpecializationFailure::Effects)?;
    Ok((receipt, effects))
}

/// Private static dispatch preserves each dedicated lifecycle's summary type.
pub(crate) trait ScopedArgumentEffects: Copy {
    fn compose_for_call(
        self,
        receipt: &RefinedCallEffects<'_>,
        input: CallEffectInput<'_>,
    ) -> Result<Self, novarocks_type_contract::EffectContractError>;
}
impl ScopedArgumentEffects for ScopedExpressionEffects {
    fn compose_for_call(
        self,
        receipt: &RefinedCallEffects<'_>,
        input: CallEffectInput<'_>,
    ) -> Result<Self, novarocks_type_contract::EffectContractError> {
        receipt.compose_for_use(input, self)
    }
}
impl ScopedArgumentEffects for crate::ScopedRelationalEffects {
    fn compose_for_call(
        self,
        receipt: &RefinedCallEffects<'_>,
        input: CallEffectInput<'_>,
    ) -> Result<Self, novarocks_type_contract::EffectContractError> {
        receipt.compose_for_relation(input, self)
    }
}
