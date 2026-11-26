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

//! One real immutable owner for the installed ARRAY family aggregate identity.
//! Producer DISTINCT/ORDER facts survive all phases and public ARRAY OVER.

use super::{
    aggregate_array::{ArrayKernel, Operation, supported, validate_contract},
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
    let owner = Arc::new(ArrayOwner::new(name, declaration, resolver)?);
    let definition = if matches!(owner.operation, Operation::Array) {
        FunctionDefinition::try_new_pure_aggregate_window(name, FunctionVisibility::Public, owner)
    } else {
        FunctionDefinition::try_new_pure_aggregate(name, FunctionVisibility::Public, owner)
    };
    definition.map_err(|error| FunctionCatalogError::InvalidStableIdentity {
        subject: "builtin ARRAY family aggregate pure owner",
        value: error.to_string().into(),
    })
}
struct ArrayOwner {
    resolver: Arc<BuiltinAggregateResolver>,
    declaration: FunctionBindingDeclaration,
    operation: Operation,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl ArrayOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<BuiltinAggregateResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        let expected = effects();
        let valid = matches!(
            name,
            "array_agg" | "array_agg_distinct" | "array_unique_agg"
        ) && declaration.function_id().as_str()
            == format!("builtin.aggregate/{name}/v1")
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
                subject: "builtin ARRAY family aggregate pure declaration",
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
                abi: if name == "array_agg" {
                    PureKernelAbi::AggregateWindowV1
                } else {
                    PureKernelAbi::AggregateV1
                },
            })
            .collect();
        Ok(Self {
            resolver,
            declaration,
            operation: match name {
                "array_agg" => Operation::Array,
                "array_agg_distinct" => Operation::Distinct,
                "array_unique_agg" => Operation::Unique,
                _ => unreachable!("validated ARRAY identity"),
            },
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

impl FunctionBindingResolver for ArrayOwner {
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
impl AggregateSignatureResolver for ArrayOwner {
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
impl PureFunctionMetadataOwner for ArrayOwner {
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
        if !matches!(self.operation, Operation::Array) {
            return Err(FunctionBindingError::UnavailableImplementation(
                selected.overload.clone(),
            ));
        }
        self.declaration.effect_declaration(&selected.overload)?;
        admit_frame_modifiers(selected, logical_argument_count, distinct, ordered, control)?;
        let (Some(FunctionArgumentType::Value(argument)), FunctionResultType::Scalar(result)) =
            (selected.argument_types.first(), &selected.result_type)
        else {
            return Err(FunctionBindingError::InvalidBinding(
                "ARRAY window requires its exact value and List result".into(),
            ));
        };
        if logical_argument_count != 1
            || !crate::control_values::supports_indexed_result(argument.data_type())
            || !crate::control_values::supports_indexed_result(result.data_type())
        {
            return Err(FunctionBindingError::UnavailableImplementation(
                selected.overload.clone(),
            ));
        }
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        for (ordinal, argument) in selected.argument_types.iter().enumerate() {
            let FunctionArgumentType::Value(argument) = argument else {
                return Err(FunctionBindingError::InvalidBinding(
                    "ARRAY window requires exact Value arguments".into(),
                ));
            };
            let json_payload = (ordinal == 0 && !distinct).then_some(argument.value_type());
            if !supported(argument.data_type(), json_payload, &mut || work.step())? {
                return Err(FunctionBindingError::UnavailableImplementation(
                    selected.overload.clone(),
                ));
            }
        }
        work.finish()?;
        Ok(())
    }
}
impl FunctionEffectOwner for ArrayOwner {
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
impl PureAggregateImplementation for ArrayOwner {
    type Kernel = ArrayKernel;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<ArrayKernel>, KernelFailure> {
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

impl PureAggregateWindowImplementation for ArrayOwner {
    fn prepare_aggregate_window(
        &self,
        aggregate: Arc<ArrayKernel>,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
        TrackedWindowOwner::Array(aggregate).prepare(contract, control)
    }
}

impl ArrayOwner {
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
                    "ARRAY family aggregate requires its original scope and no environment".into(),
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
    ) -> Result<Arc<ArrayKernel>, KernelFailure> {
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
                    "ARRAY family aggregate preparation differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("ARRAY family aggregate preparation has a stale selected binding"),
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
                        "ARRAY family aggregate requires canonical value arguments",
                    ));
                };
                if !actual.exactly_equals_observed::<KernelFailure>(selected, || {
                    work.step().map_err(compile_failure)
                })? {
                    return Err(invalid(
                        "ARRAY family aggregate request differs from its exact selected source",
                    ));
                }
            }
            validate_contract(self.operation, &contract, &mut work)?;
            work.flush().map_err(compile_failure)?;
            let prepared = Arc::new(ArrayKernel {
                ascending: contract
                    .state_interpretation()
                    .expect("validated ARRAY receipt")
                    .order_keys
                    .iter()
                    .map(|key| key.ascending)
                    .collect(),
                nulls_first: contract
                    .state_interpretation()
                    .expect("validated ARRAY receipt")
                    .order_keys
                    .iter()
                    .map(|key| key.nulls_first)
                    .collect(),
                contract,
                operation: self.operation,
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
