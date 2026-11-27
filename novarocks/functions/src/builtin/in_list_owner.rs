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

use super::binding_control;
use crate::{
    CallArgumentUses, CallEffectInput, FunctionArgument, FunctionBindingDeclaration,
    FunctionBindingError, FunctionBindingRequest, FunctionBindingResolver,
    FunctionBindingResolverOwner, FunctionBindingSelection, FunctionCatalogError,
    FunctionDefinition, FunctionEffectOwner, FunctionEffectOwnerError, FunctionId, FunctionKind,
    FunctionOverloadDeclaration, FunctionOverloadId, FunctionResultType, FunctionValueType,
    FunctionVisibility, PureFunctionMetadataOwner, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CompileCheckpoints, FunctionArgumentType, FunctionArgumentTypes,
    FunctionEffectDeclaration, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility, ObservableEffects,
    OperatorFunction, PureCompileControl, ValueLogicalType,
};
use std::sync::Arc;

pub(crate) struct InListOwner {
    declaration: FunctionBindingDeclaration,
    overload: FunctionOverloadId,
    implementations: [PureImplementationDeclaration; 1],
}

pub(super) fn definition() -> Result<FunctionDefinition, FunctionCatalogError> {
    let id = OperatorFunction::InList.function_id();
    let name = OperatorFunction::InList.catalog_name();
    let overload =
        FunctionOverloadId::try_new(OperatorFunction::IN_LIST_OVERLOAD).map_err(|error| {
            FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin IN overload",
                value: error.to_string().into(),
            }
        })?;
    let declaration = FunctionBindingDeclaration::try_new_complete(
        id,
        FunctionKind::Scalar,
        [FunctionOverloadDeclaration::from_effects(
            overload.clone(),
            "(comparable T, ordered candidates of T)",
            "boolean",
            None,
            FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::MayRaise,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::ControlDefined,
                argument_control: ArgumentControl::CandidateGroup,
                instance_state: FunctionInstanceState::None,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::default(),
            },
        )],
    )
    .map_err(catalog_error)?;
    let implementation = PureImplementationId::try_new("builtin.scalar/__operator_in/selected-v1")?;
    let owner = Arc::new(InListOwner {
        declaration,
        implementations: [PureImplementationDeclaration {
            overload: overload.clone(),
            implementation,
            abi: PureKernelAbi::ControlIntrinsicV1,
        }],
        overload,
    });
    FunctionDefinition::try_new_pure_control(name, FunctionVisibility::Hidden, owner)
        .map_err(catalog_error)
}

fn catalog_error(error: impl std::fmt::Display) -> FunctionCatalogError {
    FunctionCatalogError::InvalidStableIdentity {
        subject: "builtin IN candidate group",
        value: error.to_string().into(),
    }
}

struct Profile<'a> {
    probe: &'a FunctionValueType,
    element_type: &'a FunctionValueType,
    result_nullable: bool,
    own_row_error: FunctionIntrinsicRowError,
}

fn profile<'a>(
    request: FunctionBindingRequest<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Profile<'a>, FunctionBindingError> {
    let [
        FunctionArgument::Value {
            value_type: probe, ..
        },
        FunctionArgument::CandidateGroup {
            element_type,
            members,
        },
    ] = request.arguments
    else {
        return Err(FunctionBindingError::NoMatchingOverload);
    };
    if request.logical_argument_count != 2 || members.is_empty() {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    binding_control::request_types(request, work)?;
    if !probe
        .same_physical_binding_ignoring_root_nullability_observed(element_type, || work.step())?
    {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    let mut candidate_nullable = false;
    for member in members {
        work.step()?;
        if !member
            .value_type
            .same_physical_binding_ignoring_root_nullability_observed(element_type, || {
                work.step()
            })?
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        if let Some(constant) = &member.constant
            && !binding_control::exact_type(&member.value_type, constant.value_type(), work)?
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        candidate_nullable |= member.value_type.nullable();
    }
    if candidate_nullable != element_type.nullable() {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    work.flush()?;
    let (result_nullable, comparison) =
        crate::native_inlist::candidate_group_profile(probe, element_type, work.control())
            .map_err(|error| match error.control_error() {
                Some(error) => FunctionBindingError::Control(error),
                None => FunctionBindingError::NoMatchingOverload,
            })?;
    if let Some(expected) = request.expected_result_type
        && !boolean_result(expected, result_nullable)
    {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    Ok(Profile {
        probe,
        element_type,
        result_nullable,
        own_row_error: if comparison.may_raise_row_error {
            FunctionIntrinsicRowError::MayRaise
        } else {
            FunctionIntrinsicRowError::NoRowError
        },
    })
}

fn boolean_result(result: &FunctionValueType, nullable: bool) -> bool {
    result.data_type() == &DataType::Boolean
        && result.logical_type() == ValueLogicalType::Physical
        && result.nullable() == nullable
}

impl InListOwner {
    fn checked_profile<'a>(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'a>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Profile<'a>, FunctionBindingError> {
        let [
            FunctionArgumentType::Value(probe),
            FunctionArgumentType::CandidateGroup { element_type },
        ] = selected.argument_types.as_ref()
        else {
            return Err(FunctionBindingError::NoMatchingOverload);
        };
        let FunctionResultType::Scalar(result) = &selected.result_type else {
            return Err(FunctionBindingError::NoMatchingOverload);
        };
        if selected.overload != self.overload || selected.aggregate.is_some() {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        let profile = profile(request, work)?;
        if !binding_control::exact_type(profile.probe, probe, work)?
            || !binding_control::exact_type(profile.element_type, element_type, work)?
            || !boolean_result(result, profile.result_nullable)
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        Ok(profile)
    }

    fn refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionBindingError> {
        binding_control::scope(control, |work| {
            self.declaration(input.function_id, input.selected)?;
            if input.kind != FunctionKind::Scalar
                || !input.environment.is_empty()
                || !matches!(input.argument_uses, CallArgumentUses::CandidateGroup { .. })
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let profile = self.checked_profile(input.selected, input.request, work)?;
            Ok(CallEffects {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: profile.own_row_error,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::ControlDefined,
                argument_control: ArgumentControl::CandidateGroup,
                instance_state: FunctionInstanceState::None,
                observable_effects: ObservableEffects::NONE,
                environment: Box::default(),
                proof_scope: input.proof_scope,
            })
        })
    }
}

impl FunctionBindingResolver for InListOwner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.select_at_overload_observed(&self.overload, request, control)
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            if overload != &self.overload {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let profile = profile(request, work)?;
            Ok(FunctionBindingSelection {
                overload: self.overload.clone(),
                argument_types: FunctionArgumentTypes::from_pure_boxed(Box::new([
                    FunctionArgumentType::Value(profile.probe.clone()),
                    FunctionArgumentType::CandidateGroup {
                        element_type: profile.element_type.clone(),
                    },
                ])),
                result_type: FunctionResultType::Scalar(FunctionValueType::new(
                    DataType::Boolean,
                    profile.result_nullable,
                )),
                aggregate: None,
            })
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            self.checked_profile(selected, request, work)?;
            Ok(())
        })
    }
}

impl PureFunctionMetadataOwner for InListOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> FunctionBindingResolverOwner {
        FunctionBindingResolverOwner::borrowed_membership(self)
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}

impl FunctionEffectOwner for InListOwner {
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

    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.refine(input, control).map_err(effect_failure)
    }

    fn validate_and_refine_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, crate::CheckedOwnerFailure<FunctionEffectOwnerError<Self::Error>>>
    {
        checked
            .require_binding_author(self, input)
            .map_err(effect_failure)
            .map_err(crate::CheckedOwnerFailure::Original)?;
        self.validate_and_refine(input, control)
            .map_err(crate::CheckedOwnerFailure::Original)
    }
}

fn effect_failure(error: FunctionBindingError) -> FunctionEffectOwnerError<FunctionBindingError> {
    match error {
        FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
        error => FunctionEffectOwnerError::Owner(error),
    }
}
