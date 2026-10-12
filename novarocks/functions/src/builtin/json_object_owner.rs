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

//! Exact binding and lifecycle for the shared original JSON_OBJECT core.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::catalogue::BuiltinScalarResolver;
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingSelection, FunctionCatalogError, FunctionDefinition,
    FunctionEffectOwner, FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionVisibility, FunctionVolatility,
    InvocationScalarKernelInstance, KernelEvaluationControl, KernelFailure,
    PreparedInvocationScalarKernel, PureFunctionMetadataOwner, PureImplementationDeclaration,
    PureImplementationId, PureInvocationScalarImplementation, PureKernelAbi, ScalarCallContract,
    ScalarCallInput, SelectedValues,
};

/// Original Data belongs to the whole invocation, independently of throw policy.
pub(super) fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

pub(super) fn definition(
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let identities = declaration
        .overloads()
        .iter()
        .map(|overload| overload.identity.clone())
        .collect::<Vec<_>>();
    let declaration = declaration
        .with_scalar_invocation_data_overloads(identities)
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin json_object whole-call declaration",
            value: error.to_string().into(),
        })?;
    let owner = Arc::new(JsonObjectOwner::new(declaration, resolver)?);
    FunctionDefinition::try_new_pure_invocation_scalar(
        "json_object",
        FunctionVisibility::Public,
        owner,
    )
    .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
        subject: "builtin json_object pure owner",
        value: error.to_string().into(),
    })
}

struct JsonObjectOwner {
    resolver: BuiltinScalarResolver,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl JsonObjectOwner {
    fn new(
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let function = "builtin.scalar/json_object/v1";
        let expected = effects();
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 1
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some()
                    || overload.effects.as_ref() != Some(&expected)
                    || declaration.has_scalar_invocation_data(&overload.identity) != Ok(true)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin json_object pure declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let implementation =
            PureImplementationId::try_new("builtin.scalar/json_object/selected-v1")?;
        let implementations = declaration
            .overloads()
            .iter()
            .map(|overload| PureImplementationDeclaration {
                overload: overload.identity.clone(),
                implementation: implementation.clone(),
                abi: PureKernelAbi::ScalarInvocationV1,
            })
            .collect();
        Ok(Self {
            resolver,
            declaration,
            implementations,
        })
    }

    fn call_effects(&self, scope: CallProofScope) -> CallEffects {
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
            proof_scope: scope,
        }
    }
}

impl FunctionBindingResolver for JsonObjectOwner {
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

impl PureFunctionMetadataOwner for JsonObjectOwner {
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
        work.step()?;
        self.declaration.effect_declaration(&selected.overload)?;
        let supported =
            super::json_object_selected::original_native_profile(selected, logical_argument_count);
        work.finish()?;
        if supported {
            Ok(())
        } else {
            Err(FunctionBindingError::UnavailableImplementation(
                selected.overload.clone(),
            ))
        }
    }
}

impl FunctionEffectOwner for JsonObjectOwner {
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
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        work.step().map_err(FunctionEffectOwnerError::Control)?;
        if input.function_id != self.declaration.function_id() || input.kind != FunctionKind::Scalar
        {
            return Err(FunctionBindingError::UnknownFunction.into());
        }
        if !input.environment.is_empty()
            || !matches!(input.proof_scope, CallProofScope::Unconditional)
                && input.proof_scope != CallProofScope::Domain(input.context.domain)
        {
            return Err(FunctionBindingError::InvalidBinding(
                "json_object has no environment dependencies and requires an exact proof scope"
                    .into(),
            )
            .into());
        }
        work.flush().map_err(FunctionEffectOwnerError::Control)?;
        self.validate_selected(input.selected, input.request, control)
            .map_err(|error| match error {
                FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
                other => FunctionEffectOwnerError::Owner(other),
            })?;
        self.admit_selected_profile_observed(
            input.selected,
            input.request.logical_argument_count,
            control,
        )
        .map_err(|error| match error {
            FunctionBindingError::Control(cause) => FunctionEffectOwnerError::Control(cause),
            other => FunctionEffectOwnerError::Owner(other),
        })?;
        let result = self.call_effects(input.proof_scope);
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        Ok(result)
    }
}

impl PureInvocationScalarImplementation for JsonObjectOwner {
    fn prepare_invocation_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedInvocationScalarKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        work.step().map_err(compile_failure)?;
        if input.function_id != self.declaration.function_id()
            || input.kind != FunctionKind::Scalar
            || contract.function_id() != input.function_id
            || !std::ptr::eq(contract.selected(), input.selected)
            || contract.context() != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || contract.effects() != &self.call_effects(input.proof_scope)
            || !input.environment.is_empty()
        {
            return Err(invalid(
                "json_object preparation differs from its exact checked call",
            ));
        }
        work.flush().map_err(compile_failure)?;
        self.validate_selected(input.selected, input.request, control)
            .map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("json_object preparation has a stale selected binding"),
            })?;
        super::json_object_selected::validate_profile(&contract, || {
            work.step().map_err(compile_failure)
        })?;
        work.finish().map_err(compile_failure)?;
        // The prepared object retains the same canonical contract. Its body is
        // a static pure implementation and needs no live resolver or authority.
        Ok(Arc::new(PreparedJsonObject { contract }))
    }
}

#[derive(Debug)]
struct PreparedJsonObject {
    contract: Arc<ScalarCallContract>,
}
impl PreparedInvocationScalarKernel for PreparedJsonObject {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<JsonObjectInstance>()
            + crate::aggregate_host_allocator::HostAggregateAllocator::metadata_allocation_bytes()
    }
    fn instance_inline_allocation_bytes(&self) -> usize {
        std::mem::size_of::<JsonObjectInstance>()
    }
    fn create_instance_with_allocator(
        &self,
        host: Arc<dyn crate::AggregateStateAllocator>,
    ) -> Result<Box<dyn InvocationScalarKernelInstance>, KernelFailure> {
        if host.opaque_allocation_host().is_none() {
            return Err(invalid(
                "json_object requires its actual scalar allocation host",
            ));
        }
        let allocator =
            crate::aggregate_host_allocator::HostAggregateAllocator::try_new(Arc::clone(&host))?;
        Ok(Box::new(JsonObjectInstance {
            contract: Arc::clone(&self.contract),
            allocator,
            host,
            failed: false,
        }))
    }
}
struct JsonObjectInstance {
    contract: Arc<ScalarCallContract>,
    allocator: crate::aggregate_host_allocator::HostAggregateAllocator,
    host: Arc<dyn crate::AggregateStateAllocator>,
    failed: bool,
}
impl InvocationScalarKernelInstance for JsonObjectInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, crate::ScalarInvocationFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = super::json_object_selected::evaluate(
            input,
            &self.contract,
            control,
            &self.allocator,
            &self.host,
        );
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.allocator.metadata_bytes()
    }
}

#[cfg(test)]
#[path = "json_object_owner_tests.rs"]
mod tests;
