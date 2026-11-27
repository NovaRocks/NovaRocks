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

//! Exact selected kernels for SQL JSON parsing, construction and serialization.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::{catalogue::BuiltinScalarResolver, json_sql::JsonFunction, json_sql_selected};
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingSelection, FunctionCatalogError, FunctionDefinition,
    FunctionEffectOwner, FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionVisibility, FunctionVolatility,
    KernelEvaluationControl, KernelFailure, PreparedScalarKernel, PureFunctionMetadataOwner,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, PureScalarImplementation,
    ScalarCallContract, ScalarCallInput, ScalarKernelInstance, SelectedValues,
};

pub(super) fn effects(operation: JsonFunction) -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: if operation.may_raise() {
            FunctionIntrinsicRowError::MayRaise
        } else {
            FunctionIntrinsicRowError::NoRowError
        },
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: if operation.constructs() {
            FunctionNullBehavior::CalledOnNull
        } else {
            FunctionNullBehavior::Strict
        },
        argument_control: ArgumentControl::OrderedNull,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

pub(super) fn definition(
    operation: JsonFunction,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(ParseJsonOwner::new(operation, declaration, resolver)?);
    let visibility = if operation.public() {
        FunctionVisibility::Public
    } else {
        FunctionVisibility::Hidden
    };
    FunctionDefinition::try_new_pure_scalar(operation.name(), visibility, owner).map_err(|error| {
        FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin parse_json pure owner",
            value: error.to_string().into(),
        }
    })
}

struct ParseJsonOwner {
    operation: JsonFunction,
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl ParseJsonOwner {
    fn new(
        operation: JsonFunction,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let expected = effects(operation);
        if declaration.function_id() != &operation.function_id()
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 1
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin parse_json pure declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let implementation = PureImplementationId::try_new(format!(
            "builtin.scalar/{}/native-json-v1",
            operation.name()
        ))?;
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

    fn call_effects(&self, scope: CallProofScope) -> CallEffects {
        let base = effects(self.operation);
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

impl FunctionBindingResolver for ParseJsonOwner {
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

impl PureFunctionMetadataOwner for ParseJsonOwner {
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
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        work.step()?;
        self.declaration.effect_declaration(&selected.overload)?;
        if selected.aggregate.is_some() || selected.argument_types.len() != logical_argument_count {
            return Err(FunctionBindingError::UnavailableImplementation(
                selected.overload.clone(),
            ));
        }
        self.operation.check_arguments(
            selected
                .argument_types
                .iter()
                .map(|argument| match argument {
                    crate::FunctionArgumentType::Value(value) => Some(value),
                    _ => None,
                }),
            &mut work,
        )?;
        let crate::FunctionResultType::Scalar(result) = &selected.result_type else {
            return Err(FunctionBindingError::UnavailableImplementation(
                selected.overload.clone(),
            ));
        };
        let result_matches = if self.operation.returns_json() {
            super::json_sql::is_json(result)
        } else {
            result.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
                && result.data_type()
                    == &if self.operation == JsonFunction::Format {
                        arrow_schema::DataType::Utf8
                    } else {
                        arrow_schema::DataType::Binary
                    }
        };
        work.finish()?;
        if !result_matches {
            return Err(FunctionBindingError::UnavailableImplementation(
                selected.overload.clone(),
            ));
        }
        Ok(())
    }
}

impl FunctionEffectOwner for ParseJsonOwner {
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

impl PureScalarImplementation for ParseJsonOwner {
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
struct PreparedParseJson {
    operation: JsonFunction,
    contract: Arc<ScalarCallContract>,
}
impl PreparedScalarKernel for PreparedParseJson {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<ParseJsonInstance>()
            + crate::aggregate_host_allocator::HostAggregateAllocator::metadata_allocation_bytes()
    }
    fn instance_inline_allocation_bytes(&self) -> usize {
        std::mem::size_of::<ParseJsonInstance>()
    }
    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Err(invalid("JSON requires its actual scalar allocation host"))
    }
    fn create_instance_with_allocator(
        &self,
        allocator: Option<Arc<dyn crate::AggregateStateAllocator>>,
    ) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        let host =
            allocator.ok_or_else(|| invalid("JSON requires its actual scalar allocation host"))?;
        if host.opaque_allocation_host().is_none() {
            return Err(invalid("JSON requires its actual scalar allocation host"));
        }
        let allocator =
            crate::aggregate_host_allocator::HostAggregateAllocator::try_new(Arc::clone(&host))?;
        Ok(Box::new(ParseJsonInstance {
            operation: self.operation,
            allocator,
            host,
            failed: false,
        }))
    }
}
struct ParseJsonInstance {
    operation: JsonFunction,
    allocator: crate::aggregate_host_allocator::HostAggregateAllocator,
    host: Arc<dyn crate::AggregateStateAllocator>,
    failed: bool,
}
impl ScalarKernelInstance for ParseJsonInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = json_sql_selected::evaluate(
            self.operation,
            input,
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
#[path = "parse_json_owner_tests.rs"]
mod tests;

impl ParseJsonOwner {
    fn refine_selected_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        work.step().map_err(FunctionEffectOwnerError::Control)?;
        if input.function_id != self.declaration.function_id() || input.kind != FunctionKind::Scalar
        {
            return Err(FunctionBindingError::UnknownFunction.into());
        }
        if !input.environment.is_empty()
            || !matches!(input.proof_scope, CallProofScope::Unconditional)
                && input.proof_scope != input.context.proof_scope()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "JSON has no environment dependencies and requires an exact proof scope".into(),
            )
            .into());
        }
        work.flush().map_err(FunctionEffectOwnerError::Control)?;
        validate().map_err(|error| match error {
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
    fn prepare_selected_core(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        work.step().map_err(compile_failure)?;
        if input.function_id != self.declaration.function_id()
            || input.kind != FunctionKind::Scalar
            || contract.function_id() != input.function_id
            || !std::ptr::eq(contract.selected(), input.selected)
            || novarocks_type_contract::CallEffectContext::from(contract.context()) != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || contract.effects() != &self.call_effects(input.proof_scope)
            || !input.environment.is_empty()
        {
            return Err(invalid(
                "JSON preparation differs from its exact checked call",
            ));
        }
        work.flush().map_err(compile_failure)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => compile_failure(error),
            _ => invalid("JSON preparation has a stale selected binding"),
        })?;
        self.admit_selected_profile_observed(
            input.selected,
            input.request.logical_argument_count,
            control,
        )
        .map_err(|error| match error {
            FunctionBindingError::Control(error) => compile_failure(error),
            _ => invalid("JSON preparation has an unsupported selected profile"),
        })?;
        work.finish().map_err(compile_failure)?;
        // The prepared object retains the same canonical contract. Its body is
        // a static pure implementation and needs no live resolver or authority.
        Ok(Arc::new(PreparedParseJson {
            operation: self.operation,
            contract,
        }))
    }
}
