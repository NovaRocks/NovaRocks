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

//! One real immutable owner for the installed LISTAGG aggregate identity.
//! Raw ordered state interpretation and aggregate OVER retain separate obligations.

use super::{
    aggregate_concat::ListaggKernel,
    aggregate_window_adapter::{TrackedWindowOwner, admit_frame_modifiers},
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
    let owner = Arc::new(ConcatOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_aggregate_window(name, FunctionVisibility::Public, owner)
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin LISTAGG aggregate pure owner",
            value: error.to_string().into(),
        })
}
struct ConcatOwner {
    resolver: Arc<BuiltinAggregateResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl ConcatOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<BuiltinAggregateResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        let expected = effects();
        let valid = name == "listagg"
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
                subject: "builtin LISTAGG aggregate pure declaration",
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
                abi: PureKernelAbi::AggregateWindowV1,
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

impl FunctionBindingResolver for ConcatOwner {
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
impl AggregateSignatureResolver for ConcatOwner {
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
impl PureFunctionMetadataOwner for ConcatOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> crate::FunctionBindingResolverOwner {
        crate::FunctionBindingResolverOwner::borrowed_aggregate(Arc::clone(&self.resolver))
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }
    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
    fn admit_selected_lifecycle_profile_observed(
        &self,
        selected: &FunctionBindingSelection,
        logical_argument_count: usize,
        lifecycle: PureCallLifecycle,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.admit_selected_profile_observed(selected, logical_argument_count, control)?;
        let PureCallLifecycle::AggregateWindow { distinct, ordered } = lifecycle else {
            return Ok(());
        };
        self.declaration.effect_declaration(&selected.overload)?;
        if logical_argument_count != 5 {
            return Err(FunctionBindingError::InvalidBinding(
                "LISTAGG window requires five exact value channels".into(),
            ));
        }
        admit_frame_modifiers(selected, logical_argument_count, distinct, ordered, control)
    }
}
impl FunctionEffectOwner for ConcatOwner {
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
        self.refine_core(input, control, || {
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
        self.refine_core(input, control, || {
            checked.require_binding_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}
impl PureAggregateImplementation for ConcatOwner {
    type Kernel = ListaggKernel;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<ListaggKernel>, KernelFailure> {
        self.prepare_core(input, contract, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }
    fn prepare_aggregate_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self::Kernel>, crate::CheckedOwnerFailure<KernelFailure>> {
        self.prepare_core(input, contract, control, || {
            checked.require_binding_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

impl PureAggregateWindowImplementation for ConcatOwner {
    fn prepare_aggregate_window(
        &self,
        aggregate: Arc<ListaggKernel>,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
        TrackedWindowOwner::Listagg(aggregate).prepare(contract, control)
    }
}

impl ConcatOwner {
    fn refine_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
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
                    || input.proof_scope == input.context.proof_scope());
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !scope {
                return Err(FunctionBindingError::InvalidBinding(
                    "LISTAGG aggregate requires its original scope and exact frozen environment"
                        .into(),
                )
                .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
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
    fn prepare_core(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<ListaggKernel>, KernelFailure> {
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
                    "LISTAGG aggregate preparation differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("LISTAGG aggregate preparation has a stale selected binding"),
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
                        "LISTAGG aggregate requires canonical value arguments",
                    ));
                };
                if !actual.exactly_equals_observed::<KernelFailure>(selected, || {
                    work.step().map_err(compile_failure)
                })? {
                    return Err(invalid(
                        "LISTAGG aggregate request differs from its exact selected source",
                    ));
                }
            }
            if contract.phase() != AggregateKernelPhase::Single
                || input.request.logical_argument_count != 5
            {
                return Err(invalid(
                    "LISTAGG requires its five logical arguments in Single",
                ));
            }
            let (ascending, nulls_first): (Vec<_>, Vec<_>) = contract
                .order_keys()
                .iter()
                .map(|key| (key.ascending, key.nulls_first))
                .unzip();
            work.flush().map_err(compile_failure)?;
            let prepared = Arc::new(ListaggKernel {
                ascending: ascending.into_boxed_slice(),
                nulls_first: nulls_first.into_boxed_slice(),
                contract,
            });
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
