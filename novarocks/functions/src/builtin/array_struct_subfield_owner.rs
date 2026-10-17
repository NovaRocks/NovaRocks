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

//! Public whole-invocation owner for the one original ARRAY projection core.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::catalogue::BuiltinDynamicScalarResolver;
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    AggregateStateAllocator, CallEffectInput, FunctionBindingDeclaration, FunctionBindingError,
    FunctionBindingRequest, FunctionBindingResolver, FunctionBindingSelection,
    FunctionCatalogError, FunctionDefinition, FunctionEffectOwner, FunctionEffectOwnerError,
    FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError, FunctionKind,
    FunctionVisibility, FunctionVolatility, InvocationScalarKernelInstance,
    KernelEvaluationControl, KernelFailure, PreparedInvocationScalarKernel,
    PureFunctionMetadataOwner, PureImplementationDeclaration, PureImplementationId,
    PureInvocationScalarImplementation, PureKernelAbi, ScalarCallContract, ScalarCallInput,
    ScalarInvocationFailure, SelectedValues,
};

/// Original normalization precedes root NULL masking; full raw diagnostics are retained.
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
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinDynamicScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let identities = declaration
        .overloads()
        .iter()
        .map(|overload| overload.identity.clone())
        .collect::<Vec<_>>();
    let declaration = declaration
        .with_scalar_invocation_data_overloads(identities)
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin __array_struct_subfield whole-call declaration",
            value: error.to_string().into(),
        })?;
    let owner = Arc::new(ArrayStructSubfieldOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_invocation_scalar(name, FunctionVisibility::Public, owner)
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin __array_struct_subfield pure owner",
            value: error.to_string().into(),
        })
}

pub(super) struct ArrayStructSubfieldOwner {
    resolver: Arc<BuiltinDynamicScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl ArrayStructSubfieldOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinDynamicScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let function = format!("builtin.scalar/{name}/v1");
        let expected = effects();
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 1
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin __array_struct_subfield pure declaration",
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
                abi: PureKernelAbi::ScalarInvocationV1,
            })
            .collect();
        Ok(Self {
            resolver: Arc::new(resolver),
            declaration,
            implementations,
        })
    }

    fn argument_control(&self, count: usize) -> ArgumentControl {
        if crate::invocation_arity::ARRAY_STRUCT_SUBFIELD_ARITY
            .failure(count)
            .is_some()
        {
            ArgumentControl::NoArguments
        } else {
            ArgumentControl::Eager
        }
    }
    fn call_effects(&self, scope: CallProofScope, count: usize) -> CallEffects {
        let base = effects();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: self.argument_control(count),
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope: scope,
        }
    }
}

impl FunctionBindingResolver for ArrayStructSubfieldOwner {
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

impl PureFunctionMetadataOwner for ArrayStructSubfieldOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> crate::FunctionBindingResolverOwner {
        crate::FunctionBindingResolverOwner::dynamic_scalar(Arc::clone(&self.resolver))
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}

impl FunctionEffectOwner for ArrayStructSubfieldOwner {
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
        _control: &dyn PureCompileControl,
    ) -> Result<ArgumentControl, Self::Error> {
        self.declaration(function, selected)?;
        if logical_argument_count != selected.argument_types.len() {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        Ok(self.argument_control(logical_argument_count))
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
            checked.require_binding_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

impl PureInvocationScalarImplementation for ArrayStructSubfieldOwner {
    fn prepare_invocation_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedInvocationScalarKernel>, KernelFailure> {
        self.prepare_selected_core(input, contract, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }
    fn prepare_invocation_scalar_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedInvocationScalarKernel>, crate::CheckedOwnerFailure<KernelFailure>>
    {
        self.prepare_selected_core(input, contract, control, || {
            checked.require_binding_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

#[derive(Debug)]
struct PreparedArrayStructSubfield {
    contract: Arc<ScalarCallContract>,
}
impl PreparedInvocationScalarKernel for PreparedArrayStructSubfield {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }
    fn input_domain_policy(&self) -> crate::InvocationInputDomainPolicy {
        crate::InvocationInputDomainPolicy::OriginalActualDomain
    }
    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<ArrayStructSubfieldInstance>()
            + crate::aggregate_host_allocator::HostAggregateAllocator::metadata_allocation_bytes()
    }
    fn instance_inline_allocation_bytes(&self) -> usize {
        std::mem::size_of::<ArrayStructSubfieldInstance>()
    }
    fn create_instance_with_allocator(
        &self,
        host: Arc<dyn AggregateStateAllocator>,
    ) -> Result<Box<dyn InvocationScalarKernelInstance>, KernelFailure> {
        if host.opaque_allocation_host().is_none() {
            return Err(invalid(
                "__array_struct_subfield requires its actual scalar allocation host",
            ));
        }
        let allocator =
            crate::aggregate_host_allocator::HostAggregateAllocator::try_new(host.clone())?;
        Ok(Box::new(ArrayStructSubfieldInstance {
            contract: self.contract.clone(),
            host,
            allocator,
            failed: false,
        }))
    }
}
struct ArrayStructSubfieldInstance {
    contract: Arc<ScalarCallContract>,
    host: Arc<dyn AggregateStateAllocator>,
    allocator: crate::aggregate_host_allocator::HostAggregateAllocator,
    failed: bool,
}
impl InvocationScalarKernelInstance for ArrayStructSubfieldInstance {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.allocator.metadata_bytes()
    }
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let _ = (input, control);
        self.failed = true;
        Err(invalid("__array_struct_subfield requires an explicit actual invocation scope").into())
    }
    fn evaluate_scoped<'a>(
        &mut self,
        input: crate::ScopedScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = super::array_struct_subfield_selected::evaluate_scoped(
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
}

impl ArrayStructSubfieldOwner {
    fn refine_selected_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        let result = (|| {
            let exact_header = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Scalar;
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !exact_header {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            let exact_scope = input.environment.is_empty()
                && (matches!(input.proof_scope, CallProofScope::Unconditional)
                    || input.proof_scope == CallProofScope::Domain(input.context.domain));
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !exact_scope {
                return Err(FunctionBindingError::InvalidBinding(
                    "__array_struct_subfield has no environment dependencies and requires an exact proof scope"
                        .into(),
                )
                .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
                other => FunctionEffectOwnerError::Owner(other),
            })?;
            Ok(self.call_effects(input.proof_scope, input.request.logical_argument_count))
        })();
        if matches!(&result, Err(FunctionEffectOwnerError::Control(_))) {
            return result;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        result
    }
    fn prepare_selected_core(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<dyn PreparedInvocationScalarKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result: Result<Arc<dyn PreparedInvocationScalarKernel>, KernelFailure> = (|| {
            let exact_call = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Scalar
                && contract.function_id() == input.function_id
                && std::ptr::eq(contract.selected(), input.selected)
                && contract.context() == input.context
                && contract.decimal_overflow_policy() == input.decimal_overflow_policy
                && contract.effects()
                    == &self.call_effects(input.proof_scope, input.request.logical_argument_count)
                && input.environment.is_empty();
            work.step().map_err(compile_failure)?;
            if !exact_call {
                return Err(invalid(
                    "__array_struct_subfield preparation differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("__array_struct_subfield preparation has a stale selected binding"),
            })?;
            work.flush().map_err(compile_failure)?;
            // The prepared object retains the same canonical contract. Its body is
            // a static pure implementation and needs no live resolver or authority.
            let prepared = Arc::new(PreparedArrayStructSubfield { contract })
                as Arc<dyn PreparedInvocationScalarKernel>;
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
