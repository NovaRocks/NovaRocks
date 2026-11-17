// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact emitted-source TIME owners prepare control contracts, never ordinary kernels.

use super::{binding_control, catalogue::BuiltinScalarResolver};
use crate::{
    CallEffectInput, FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingSelection, FunctionCatalogError, FunctionDefinition,
    FunctionEffectOwner, FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionVisibility, FunctionVolatility,
    PureFunctionMetadataOwner, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
};
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileControlError, FunctionEffectDeclaration,
    FunctionInstanceState, FunctionNullBehavior, ObservableEffects, PureCompileControl,
    TemporalSourceKind, TemporalSourceRole, TemporalSourceShape,
};
use std::sync::Arc;

/// Only registration interprets the installed names. Prepared contracts retain
/// the exact owner declaration, including its closed argument-control protocol.
pub(super) fn operation(name: &str) -> Option<TemporalSourceKind> {
    match name {
        "time_to_sec" => Some(TemporalSourceKind::TimeToSec),
        "time_format" => Some(TemporalSourceKind::TimeFormat),
        _ => None,
    }
}

pub(super) fn effects(operation: TemporalSourceKind) -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: match operation {
            TemporalSourceKind::TimeToSec => FunctionIntrinsicRowError::MayRaise,
            TemporalSourceKind::TimeFormat => FunctionIntrinsicRowError::NoRowError,
        },
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::ControlDefined,
        argument_control: ArgumentControl::TemporalSource(operation),
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

pub(super) fn definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(TemporalOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_control(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin TIME source pure control owner",
            value: error.to_string().into(),
        },
    )
}

struct TemporalOwner {
    operation: TemporalSourceKind,
    declaration: FunctionBindingDeclaration,
    resolver: Arc<BuiltinScalarResolver>,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl TemporalOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let operation =
            operation(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
                subject: "uninstalled builtin control operation",
                value: name.into(),
            })?;
        let expected = effects(operation);
        if declaration.function_id().as_str() != format!("builtin.scalar/{name}/v1")
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 3
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin TIME source control declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let implementation =
            PureImplementationId::try_new(format!("builtin.scalar/{name}/selected-v1"))?;
        let implementations = declaration
            .overloads()
            .iter()
            .map(|overload| PureImplementationDeclaration {
                overload: overload.identity.clone(),
                implementation: implementation.clone(),
                abi: PureKernelAbi::ControlIntrinsicV1,
            })
            .collect();
        Ok(Self {
            operation,
            declaration,
            resolver: Arc::new(resolver),
            implementations,
        })
    }

    fn call_effects(&self, scope: CallProofScope) -> CallEffects {
        let declaration = effects(self.operation);
        CallEffects {
            value_stability: declaration.value_stability,
            own_row_error: declaration.own_row_error,
            failure_behavior: declaration.failure_behavior,
            null_behavior: declaration.null_behavior,
            argument_control: declaration.argument_control,
            instance_state: declaration.instance_state,
            observable_effects: declaration.observable_effects,
            environment: Box::new([]),
            proof_scope: scope,
        }
    }
}
impl FunctionBindingResolver for TemporalOwner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver.resolve(request, control)
    }
    fn select_at_overload_observed(
        &self,
        overload: &crate::FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver
            .select_at_overload_observed(overload, request, control)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.resolver.validate_selected(selected, request, control)
    }
}
impl PureFunctionMetadataOwner for TemporalOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> crate::FunctionBindingResolverOwner {
        crate::FunctionBindingResolverOwner::fixed_scalar(Arc::clone(&self.resolver))
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }
    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
    fn admit_selected_profile_observed(
        &self,
        selected: &FunctionBindingSelection,
        logical_argument_count: usize,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.resolver
            .check_instantiated_type_profile_observed(selected, logical_argument_count, control)
            .map_err(|error| match error {
                FunctionBindingError::Control(cause) => FunctionBindingError::Control(cause),
                _ => FunctionBindingError::UnavailableImplementation(selected.overload.clone()),
            })
    }
}
impl FunctionEffectOwner for TemporalOwner {
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
        self.refine_selected_core(input, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }
    fn validate_and_refine_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, crate::CheckedOwnerFailure<FunctionEffectOwnerError<Self::Error>>>
    {
        self.refine_selected_core(input, control, || {
            checked.require_fixed_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

impl TemporalOwner {
    fn refine_selected_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        binding_control::scope(control, |work| {
            work.step()?;
            if input.function_id != self.declaration.function_id()
                || input.kind != FunctionKind::Scalar
            {
                return Err(FunctionBindingError::UnknownFunction);
            }
            if !input.environment.is_empty()
                || (input.proof_scope != CallProofScope::Unconditional
                    && input.proof_scope != CallProofScope::Region(input.context.region))
            {
                return Err(FunctionBindingError::InvalidBinding(
                    "TIME source requires no environment and its exact proof scope".into(),
                ));
            }
            if input.request.arguments.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            let crate::CallArgumentUses::TemporalSources { facts, channels } = input.argument_uses
            else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            facts
                .validate()
                .map_err(|_| FunctionBindingError::NoMatchingOverload)?;
            let arity = match self.operation {
                TemporalSourceKind::TimeToSec => 1,
                TemporalSourceKind::TimeFormat => 2,
            };
            if facts.shape().kind() != self.operation
                || input.request.logical_argument_count != arity
                || input.request.arguments.len() != arity
                || input.selected.argument_types.len() != arity
                || channels.len() != facts.shape().source_count()
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let first = match &input.selected.argument_types[0] {
                crate::FunctionArgumentType::Value(ty) => ty,
                _ => return Err(FunctionBindingError::NoMatchingOverload),
            };
            if first.logical_type() != novarocks_type_contract::ValueLogicalType::Physical
                || !matches!(
                    first.data_type(),
                    arrow_schema::DataType::Utf8
                        | arrow_schema::DataType::Date32
                        | arrow_schema::DataType::Timestamp(
                            arrow_schema::TimeUnit::Microsecond,
                            None
                        )
                )
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            for (ordinal, channel) in channels.iter().enumerate() {
                work.step()?;
                binding_control::value_type(channel.value_type, work)?;
                if Some(channel.role) != facts.shape().roles()[ordinal] {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
                let accepted = match channel.role {
                    TemporalSourceRole::Normal => {
                        binding_control::exact_type(first, channel.value_type, work)?
                    }
                    // Original raw override admission is Arrow Utf8, including
                    // an explicitly declared JSON/Variant UTF8 source domain.
                    TemporalSourceRole::RawOverride => {
                        *channel.value_type.data_type() == arrow_schema::DataType::Utf8
                    }
                    TemporalSourceRole::Format => match &input.selected.argument_types[1] {
                        crate::FunctionArgumentType::Value(expected) => {
                            binding_control::exact_type(expected, channel.value_type, work)?
                        }
                        _ => false,
                    },
                    TemporalSourceRole::OriginalSeconds => {
                        channel.value_type.logical_type()
                            == novarocks_type_contract::ValueLogicalType::Physical
                            && *channel.value_type.data_type() == arrow_schema::DataType::Int64
                    }
                    TemporalSourceRole::ImmediateCastSource => {
                        matches!(facts.shape(), TemporalSourceShape::SecondsCastString)
                            == (*channel.value_type.data_type() == arrow_schema::DataType::Utf8)
                    }
                    // The original deepest-source branch uses the sole Arrow cast;
                    // admission checks its declared carrier, never guessed source text.
                    TemporalSourceRole::DeepestCastSource => {
                        work.flush()?;
                        let valid = arrow_cast::can_cast_types(
                            channel.value_type.data_type(),
                            &arrow_schema::DataType::Utf8,
                        );
                        work.flush()?;
                        valid
                    }
                };
                if !accepted {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
            }
            binding_control::request_types(input.request, work)?;
            work.flush()?;
            validate()?;
            work.step()?;
            let mut effects = self.call_effects(input.proof_scope);
            if facts.shape() != TemporalSourceShape::SecondsCastOther {
                effects.own_row_error = FunctionIntrinsicRowError::NoRowError;
            }
            Ok(effects)
        })
        .map_err(|error| match error {
            FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
            other => FunctionEffectOwnerError::Owner(other),
        })
    }
}
