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

//! Private original ARRAY diagnostic transport factory for cross-crate probes.
//! It is not a public ARRAY capability and cannot fund successful computation.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::catalogue::BuiltinDynamicScalarResolver;
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    AggregateStateAllocator, CallEffectInput, EvaluatedArgument, EvaluationCheckpoints,
    FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingSelection, FunctionCatalogError, FunctionDefinition,
    FunctionEffectOwner, FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionVisibility, FunctionVolatility,
    InvocationScalarKernelInstance, KernelEvaluationControl, KernelFailure,
    PreparedInvocationScalarKernel, PureFunctionMetadataOwner, PureImplementationDeclaration,
    PureImplementationId, PureInvocationScalarImplementation, PureKernelAbi, ScalarCallContract,
    ScalarCallInput, ScalarInvocationFailure, SelectedValues,
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
    let owner = Arc::new(ArrayProjectionDiagnosticProbeOwner::new(
        name,
        declaration,
        resolver,
    )?);
    FunctionDefinition::try_new_pure_invocation_scalar(name, FunctionVisibility::Public, owner)
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin __array_struct_subfield pure owner",
            value: error.to_string().into(),
        })
}

pub(super) struct ArrayProjectionDiagnosticProbeOwner {
    resolver: Arc<BuiltinDynamicScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl ArrayProjectionDiagnosticProbeOwner {
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

impl FunctionBindingResolver for ArrayProjectionDiagnosticProbeOwner {
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

impl PureFunctionMetadataOwner for ArrayProjectionDiagnosticProbeOwner {
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

impl FunctionEffectOwner for ArrayProjectionDiagnosticProbeOwner {
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

impl PureInvocationScalarImplementation for ArrayProjectionDiagnosticProbeOwner {
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
struct PreparedArrayProjectionDiagnostic {
    contract: Arc<ScalarCallContract>,
}
impl PreparedInvocationScalarKernel for PreparedArrayProjectionDiagnostic {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }
    fn instance_retained_upper_bound(&self) -> usize {
        crate::aggregate_host_allocator::HostAggregateAllocator::metadata_allocation_bytes()
    }
    fn instance_inline_allocation_bytes(&self) -> usize {
        std::mem::size_of::<ArrayProjectionDiagnosticInstance>()
    }
    fn create_instance_with_allocator(
        &self,
        host: Arc<dyn AggregateStateAllocator>,
    ) -> Result<Box<dyn InvocationScalarKernelInstance>, KernelFailure> {
        let allocator =
            crate::aggregate_host_allocator::HostAggregateAllocator::try_new(host.clone())?;
        Ok(Box::new(ArrayProjectionDiagnosticInstance {
            contract: self.contract.clone(),
            host,
            allocator,
        }))
    }
}
struct ArrayProjectionDiagnosticInstance {
    contract: Arc<ScalarCallContract>,
    host: Arc<dyn AggregateStateAllocator>,
    allocator: crate::aggregate_host_allocator::HostAggregateAllocator,
}
impl InvocationScalarKernelInstance for ArrayProjectionDiagnosticInstance {
    fn retained_bytes(&self) -> usize {
        self.allocator.metadata_bytes()
    }
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
        use super::array_struct_subfield_core::ProjectionPort;
        let mut work = EvaluationCheckpoints::new(control);
        if let Some(failure) = crate::invocation_arity::ARRAY_STRUCT_SUBFIELD_ARITY
            .failure(self.contract.call().logical_argument_count())
        {
            let mut diagnostic =
                super::array_scalar_diagnostic_source::HostProjectionDiagnostics::try_new(
                    &self.allocator,
                    self.host.clone(),
                    self.contract.clone(),
                    input.selection(),
                    &mut work,
                )?;
            // Original FUNCTION_REGISTRY maps both public spellings to this
            // canonical FunctionKind metadata. No child/value exists here.
            return Err(
                failure.with_original_message("array_struct_subfield", |args| {
                    diagnostic.value_error(args)
                }),
            );
        }
        let [source, names] = input.arguments() else {
            return Err(
                invalid("ARRAY diagnostic probe requires its actual two value channels").into(),
            );
        };
        // ConstantValue carries a checked ordinal. Project that ACTUAL value;
        // another pool row must never become the first field-name input.
        // Empty activation projects zero visible rows, exactly as original
        // arena literal evaluation on an actual empty invocation Chunk.
        let charge = crate::opaque_memory::OpaqueRetainedCharge::try_new(self.host.clone())?;
        let mut source_scope = None;
        let mut names_scope = None;
        fn operand(
            arg: EvaluatedArgument<'_>,
            rows: usize,
            charge: &crate::opaque_memory::OpaqueRetainedCharge,
            scope: &mut Option<crate::opaque_memory::OpaqueReservation>,
            work: &mut EvaluationCheckpoints<'_>,
        ) -> Result<arrow_array::ArrayRef, KernelFailure> {
            let (offset, len) = match arg {
                EvaluatedArgument::Column(array) => return Ok(array.clone()),
                EvaluatedArgument::Scalar(_) => (0, usize::from(rows != 0)),
                EvaluatedArgument::ConstantView(value) => {
                    (value.original().ordinal() as usize, usize::from(rows != 0))
                }
                EvaluatedArgument::Constant(value) => {
                    (value.ordinal() as usize, usize::from(rows != 0))
                }
                EvaluatedArgument::SelectedColumn(_) => {
                    return Err(invalid(
                        "ARRAY diagnostic probe requires the original full invocation source loan, not a compact field-name column",
                    ));
                }
            };
            let original = arg.array();
            let metadata =
                crate::array_backing_geometry::copy_metadata_bytes(original.as_ref(), work)?;
            work.flush()?;
            *scope = Some(charge.reserve_operation(metadata)?);
            work.flush()?;
            let selected = original.slice(offset, len);
            work.flush()?;
            Ok(selected)
        }
        let source = operand(
            *source,
            input.selection().batch_rows(),
            &charge,
            &mut source_scope,
            &mut work,
        )?;
        let names = operand(
            *names,
            input.selection().batch_rows(),
            &charge,
            &mut names_scope,
            &mut work,
        )?;
        let result = {
            let mut diagnostic =
                super::array_scalar_diagnostic_source::HostProjectionDiagnostics::try_new(
                    &self.allocator,
                    self.host.clone(),
                    self.contract.clone(),
                    input.selection(),
                    &mut work,
                )?;
            super::array_struct_subfield_core::project_with_port(
                &source,
                &names,
                Some(self.contract.result_type().data_type()),
                &mut diagnostic,
            )
        };
        // Diagnostic failures are final. No success callback/formatter/retry.
        match result {
            Err(error)=>Err(error),
            Ok(_)=>Err(invalid("ARRAY diagnostic transport probe cannot publish successful computation without the complete original operation host").into()),
        }
        // Actual operand ArrayRefs drop before their metadata leases.
    }
}

impl ArrayProjectionDiagnosticProbeOwner {
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
                    || input.proof_scope == CallProofScope::Region(input.context.region));
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
            let prepared = Arc::new(PreparedArrayProjectionDiagnostic { contract })
                as Arc<dyn PreparedInvocationScalarKernel>;
            work.flush().map_err(compile_failure)?;
            Ok(prepared)
        })();
        if result.as_ref().is_err_and(KernelFailure::is_interruption) {
            return result;
        }
        work.finish().map_err(compile_failure)?;
        result
    }
}
