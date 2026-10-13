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

//! Immutable owner for the original Mann-Whitney aggregate and shared kernel.
//! DISTINCT, function ORDER BY and aggregate OVER retain separate obligations.

use super::{
    aggregate_mann_whitney_selected::{MannKernel, validate_contract},
    catalogue::BuiltinAggregateResolver,
};
use crate::kernel_control::{compile_failure, invalid};
use crate::*;
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};
use std::sync::Arc;

pub(super) fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Aggregate,
        instance_state: FunctionInstanceState::AggregateInstance,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}
pub(super) fn definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: Arc<BuiltinAggregateResolver>,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(MannOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_aggregate(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin Mann-Whitney aggregate pure owner",
            value: error.to_string().into(),
        },
    )
}
struct MannOwner {
    resolver: Arc<BuiltinAggregateResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl MannOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<BuiltinAggregateResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        let expected = effects();
        let valid = (name == "mann_whitney_u_test")
            && declaration.function_id().as_str() == format!("builtin.aggregate/{name}/v1")
            && declaration.kind() == FunctionKind::Aggregate
            && declaration.overloads().len() == 1
            && declaration.overloads().iter().all(|overload| {
                overload.identity.as_str() == format!("builtin.aggregate/{name}/derived-v1")
                    && overload.effects.as_ref() == Some(&expected)
                    && overload.aggregate.as_ref().is_some_and(|state| {
                        state.state_format.as_str() == format!("novarocks/{name}/state-v1")
                    })
            });
        if !valid {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin Mann-Whitney aggregate pure declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let implementation =
            PureImplementationId::try_new(format!("builtin.aggregate/{name}/selected-v1"))?;
        let implementations = declaration
            .overloads()
            .iter()
            .map(|overload| PureImplementationDeclaration {
                overload: overload.identity.clone(),
                implementation: implementation.clone(),
                abi: PureKernelAbi::AggregateV1,
            })
            .collect();
        Ok(Self {
            resolver,
            declaration,
            implementations,
        })
    }
    fn call_effects(&self, proof_scope: CallProofScope) -> CallEffects {
        let base = effects();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope,
        }
    }
}

impl FunctionBindingResolver for MannOwner {
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
impl AggregateSignatureResolver for MannOwner {
    fn validate_value_arguments(
        &self,
        types: &[FunctionValueType],
    ) -> Result<(), FunctionResolutionError> {
        self.resolver.validate_value_arguments(types)
    }
    fn resolve_aggregate(
        &self,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.resolver.resolve_aggregate(types)
    }
    fn resolve_update_signature(
        &self,
        id: &AggregateOverloadIdentity,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.resolver.resolve_update_signature(id, types)
    }
    fn produces_null(&self) -> bool {
        self.resolver.produces_null()
    }
    fn supports_ordered_update_channels(&self) -> bool {
        self.resolver.supports_ordered_update_channels()
    }

    fn state_argument_contract(
        &self,
        selected_overload: &crate::AggregateOverloadIdentity,
    ) -> Result<novarocks_type_contract::AggregateStateArgumentContract, FunctionResolutionError>
    {
        self.resolver.state_argument_contract(selected_overload)
    }
}
impl PureFunctionMetadataOwner for MannOwner {
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
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        self.declaration.effect_declaration(&selected.overload)?;
        let result = (|| {
            let original_channels = (2..=4).contains(&logical_argument_count)
                && selected.argument_types.len() == logical_argument_count
                && selected
                    .argument_types
                    .iter()
                    .all(|argument| matches!(argument, FunctionArgumentType::Value(_)));
            work.step()?;
            if !original_channels {
                return Err(FunctionBindingError::UnavailableImplementation(
                    selected.overload.clone(),
                ));
            }
            let state = selected.aggregate.as_ref().ok_or_else(|| {
                FunctionBindingError::UnavailableImplementation(selected.overload.clone())
            })?;
            let FunctionResultType::Scalar(result) = &selected.result_type else {
                return Err(FunctionBindingError::UnavailableImplementation(
                    selected.overload.clone(),
                ));
            };
            let exact = FunctionValueType::new(DataType::Utf8, true)
                .exactly_equals_observed::<FunctionBindingError>(result, || {
                    work.step().map_err(Into::into)
                })?
                && FunctionValueType::new(DataType::Binary, true)
                    .exactly_equals_observed::<FunctionBindingError>(
                        &state.intermediate_type,
                        || work.step().map_err(Into::into),
                    )?
                && state.state_format.as_str() == "novarocks/mann_whitney_u_test/state-v1";
            if exact {
                Ok(())
            } else {
                Err(FunctionBindingError::UnavailableImplementation(
                    selected.overload.clone(),
                ))
            }
        })();
        if matches!(&result, Err(FunctionBindingError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
}
impl FunctionEffectOwner for MannOwner {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        id: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if id != self.declaration.function_id() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        self.declaration.effect_declaration(&selected.overload)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        let result = (|| {
            let exact = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Aggregate;
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !exact {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            let scope = input.environment.is_empty()
                && (input.proof_scope == CallProofScope::Unconditional
                    || input.proof_scope == CallProofScope::Domain(input.context.domain));
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !scope {
                return Err(FunctionBindingError::InvalidBinding(
                    "Mann-Whitney aggregate requires its original scope and no environment".into(),
                )
                .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            self.validate_selected(input.selected, input.request, control)
                .map_err(|error| match error {
                    FunctionBindingError::Control(error) => {
                        FunctionEffectOwnerError::Control(error)
                    }
                    other => FunctionEffectOwnerError::Owner(other),
                })?;
            Ok(self.call_effects(input.proof_scope))
        })();
        if matches!(&result, Err(FunctionEffectOwnerError::Control(_))) {
            return result;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        result
    }
}
impl PureAggregateImplementation for MannOwner {
    type Kernel = MannKernel;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<MannKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result = (|| {
            let call = contract.call();
            let exact = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Aggregate
                && call.function_id() == input.function_id
                && std::ptr::eq(call.selected(), input.selected)
                && call.context() == input.context
                && call.decimal_overflow_policy() == input.decimal_overflow_policy
                && call.effects() == &self.call_effects(input.proof_scope)
                && call.logical_argument_count() == input.request.logical_argument_count;
            work.step().map_err(compile_failure)?;
            if !exact {
                return Err(invalid(
                    "Mann-Whitney aggregate preparation differs from its exact checked call",
                ));
            }
            let supported = (2..=4).contains(&input.request.logical_argument_count)
                && input.request.arguments.len() == input.request.logical_argument_count
                && input.selected.argument_types.len() == input.request.arguments.len()
                && !contract.distinct()
                && contract.order_keys().is_empty();
            work.step().map_err(compile_failure)?;
            if !supported {
                return Err(invalid(
                    "Mann-Whitney aggregate requires its original unmodified channels",
                ));
            }
            work.flush().map_err(compile_failure)?;
            self.validate_selected(input.selected, input.request, control)
                .map_err(|error| match error {
                    FunctionBindingError::Control(error) => compile_failure(error),
                    _ => invalid("Mann-Whitney aggregate preparation has a stale selected binding"),
                })?;
            for (actual, selected) in input
                .request
                .arguments
                .iter()
                .zip(&input.selected.argument_types)
            {
                let (
                    FunctionArgument::Value {
                        value_type: actual, ..
                    },
                    FunctionArgumentType::Value(selected),
                ) = (actual, selected)
                else {
                    work.step().map_err(compile_failure)?;
                    return Err(invalid(
                        "Mann-Whitney aggregate requires canonical value arguments",
                    ));
                };
                if !actual.exactly_equals_observed::<KernelFailure>(selected, || {
                    work.step().map_err(compile_failure)
                })? {
                    return Err(invalid(
                        "Mann-Whitney aggregate request differs from its exact selected source",
                    ));
                }
            }
            validate_contract(&contract, &mut work)?;
            work.flush().map_err(compile_failure)?;
            let prepared = Arc::new(MannKernel::try_new(contract, control)?);
            work.flush().map_err(compile_failure)?;
            Ok(prepared)
        })();
        if matches!(
            &result,
            Err(KernelFailure::Cancelled
                | KernelFailure::DeadlineExceeded
                | KernelFailure::ResourceExhausted)
        ) {
            return result;
        }
        work.finish().map_err(compile_failure)?;
        result
    }
}

#[cfg(test)]
mod tests;
