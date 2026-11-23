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

//! Exact installation of Trino's binary and UTF-8 overloads.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::super::catalogue::BuiltinScalarResolver;
use super::trino::{self, TrinoByteOp};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    AggregateStateAllocator, CallEffectInput, FunctionArgumentType, FunctionBindingDeclaration,
    FunctionBindingError, FunctionBindingRequest, FunctionBindingResolver,
    FunctionBindingSelection, FunctionCatalogError, FunctionDefinition, FunctionEffectOwner,
    FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError,
    FunctionKind, FunctionVisibility, FunctionVolatility, KernelEvaluationControl, KernelFailure,
    PreparedScalarKernel, PureFunctionMetadataOwner, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, PureScalarImplementation, ScalarCallContract,
    ScalarCallInput, ScalarKernelInstance, SelectedValues,
};
use novarocks_type_contract::{CharacterKind, ValueKind, ValueLogicalType};

pub(in crate::builtin) fn effects(
    operation: TrinoByteOp,
    signature: &str,
) -> FunctionEffectDeclaration {
    let may_raise = operation.may_raise()
        && (operation != TrinoByteOp::FromUtf8 || signature.starts_with("(binary,"));
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: if may_raise {
            FunctionIntrinsicRowError::MayRaise
        } else {
            FunctionIntrinsicRowError::NoRowError
        },
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::Strict,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

pub(in crate::builtin) fn definition(
    operation: TrinoByteOp,
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(TrinoByteOwner::new(operation, name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin Trino byte owner",
            value: error.to_string().into(),
        },
    )
}

struct TrinoByteOwner {
    operation: TrinoByteOp,
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}

impl TrinoByteOwner {
    fn new(
        operation: TrinoByteOp,
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let function = format!("builtin.scalar/{name}/v1");
        if TrinoByteOp::from_name(name) != Some(operation)
            || declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some()
                    || overload.effects.as_ref()
                        != Some(&effects(operation, &overload.argument_pattern))
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin Trino byte declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let implementation =
            PureImplementationId::try_new(format!("builtin.scalar/{name}/trino-bytes-v1"))?;
        let implementations = declaration
            .overloads()
            .iter()
            .map(|overload| PureImplementationDeclaration {
                overload: overload.identity.clone(),
                implementation: implementation.clone(),
                abi: PureKernelAbi::ScalarV1,
            })
            .collect();
        Ok(Self {
            operation,
            resolver: Arc::new(resolver),
            declaration,
            implementations,
        })
    }

    fn call_effects(
        &self,
        input: CallEffectInput<'_>,
    ) -> Result<CallEffects, FunctionBindingError> {
        let base = self
            .declaration
            .effect_declaration(&input.selected.overload)?;
        Ok(CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope: input.proof_scope,
        })
    }
}

impl FunctionBindingResolver for TrinoByteOwner {
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

impl PureFunctionMetadataOwner for TrinoByteOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> crate::FunctionBindingResolverOwner {
        crate::FunctionBindingResolverOwner::fixed_scalar(Arc::clone(&self.resolver))
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}

impl FunctionEffectOwner for TrinoByteOwner {
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

impl PureScalarImplementation for TrinoByteOwner {
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        self.prepare_selected_core(input, contract, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }
    fn prepare_scalar_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, crate::CheckedOwnerFailure<KernelFailure>> {
        self.prepare_selected_core(input, contract, control, || {
            checked.require_fixed_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

#[derive(Debug)]
struct PreparedTrinoBytes {
    contract: Arc<ScalarCallContract>,
    operation: TrinoByteOp,
    char_width: Option<usize>,
}

impl PreparedScalarKernel for PreparedTrinoBytes {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        size_of::<TrinoByteInstance>() + HostAggregateAllocator::metadata_allocation_bytes()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        self.create_instance_with_allocator(None)
    }

    fn create_instance_with_allocator(
        &self,
        host: Option<Arc<dyn AggregateStateAllocator>>,
    ) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        let allocator = host
            .as_ref()
            .map(|host| {
                if host.opaque_allocation_host().is_none() {
                    return Err(invalid(
                        "Trino byte evaluation requires the original opaque allocation host",
                    ));
                }
                HostAggregateAllocator::try_new(Arc::clone(host))
            })
            .transpose()?;
        Ok(Box::new(TrinoByteInstance {
            operation: self.operation,
            char_width: self.char_width,
            allocator,
            host,
        }))
    }
}

struct TrinoByteInstance {
    operation: TrinoByteOp,
    char_width: Option<usize>,
    allocator: Option<HostAggregateAllocator>,
    host: Option<Arc<dyn AggregateStateAllocator>>,
}
impl ScalarKernelInstance for TrinoByteInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        trino::evaluate(
            self.operation,
            input,
            control,
            self.allocator.as_ref(),
            self.host.as_ref(),
            self.char_width,
        )
    }

    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + self
                .allocator
                .as_ref()
                .map_or(0, HostAggregateAllocator::metadata_bytes)
    }
}

impl TrinoByteOwner {
    fn refine_selected_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        let result = (|| {
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if input.function_id != self.declaration.function_id()
                || input.kind != FunctionKind::Scalar
            {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            if !input.environment.is_empty()
                || !matches!(input.proof_scope, CallProofScope::Unconditional)
                    && input.proof_scope != input.context.proof_scope()
            {
                return Err(FunctionBindingError::InvalidBinding(
                    "Trino byte has no environment dependencies and requires an exact proof scope"
                        .into(),
                )
                .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
                other => FunctionEffectOwnerError::Owner(other),
            })?;
            self.call_effects(input)
                .map_err(FunctionEffectOwnerError::Owner)
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
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result: Result<Arc<dyn PreparedScalarKernel>, KernelFailure> = (|| {
            work.step().map_err(compile_failure)?;
            if input.function_id != self.declaration.function_id()
                || input.kind != FunctionKind::Scalar
                || contract.function_id() != input.function_id
                || !std::ptr::eq(contract.selected(), input.selected)
                || novarocks_type_contract::CallEffectContext::from(contract.context())
                    != input.context
                || contract.decimal_overflow_policy() != input.decimal_overflow_policy
                || contract.effects()
                    != &self
                        .call_effects(input)
                        .map_err(|_| invalid("Trino byte overload effects are absent"))?
                || !input.environment.is_empty()
            {
                return Err(invalid(
                    "Trino byte preparation differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("Trino byte preparation has a stale selected binding"),
            })?;
            // The prepared object retains the same canonical contract. Its body is
            // a static pure implementation and needs no live resolver or authority.
            let arguments = contract.selected().argument_types.as_ref();
            let accepted = match arguments {
                [FunctionArgumentType::Value(source)] => {
                    source.logical_type() == ValueLogicalType::Physical
                        && self.operation.accepts(&[source.data_type()])
                }
                [
                    FunctionArgumentType::Value(source),
                    FunctionArgumentType::Value(option),
                ] => {
                    source.logical_type() == ValueLogicalType::Physical
                        && option.logical_type() == ValueLogicalType::Physical
                        && self
                            .operation
                            .accepts(&[source.data_type(), option.data_type()])
                }
                _ => false,
            };
            if !accepted
                || contract.result_type().logical_type() != ValueLogicalType::Physical
                || contract.result_type().data_type() != &self.operation.result_type()
            {
                return Err(invalid(
                    "Trino byte contract differs from its installed profile",
                ));
            }
            let char_width = match (self.operation, arguments) {
                (TrinoByteOp::ToUtf8, [FunctionArgumentType::Value(source)]) => {
                    match source.value_type().kind() {
                        ValueKind::BoundedCharacter(character)
                            if character.kind() == CharacterKind::Char =>
                        {
                            Some(character.length() as usize)
                        }
                        _ => None,
                    }
                }
                _ => None,
            };
            Ok(Arc::new(PreparedTrinoBytes {
                contract,
                operation: self.operation,
                char_width,
            }) as Arc<dyn PreparedScalarKernel>)
        })();
        if result.as_ref().is_err_and(KernelFailure::is_interruption) {
            return result;
        }
        work.finish().map_err(compile_failure)?;
        result
    }
}
