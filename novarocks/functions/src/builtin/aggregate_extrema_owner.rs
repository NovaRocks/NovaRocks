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

//! Exact MIN/MAX fixed-width and UTF-8 owners. MIN/MAX OVER folds each frame
//! through the same fixed-width kernel; a UTF-8 state retains heap and has no
//! inline window state, so its OVER form is refused by name.

use super::aggregate_extrema_dispatch::{ExtremaState, PreparedExtrema};
use super::aggregate_extrema_utf8::Utf8ExtremaKernel;
use super::aggregate_window_adapter::InlineAggregateWindowKernel;
use super::{
    aggregate_extrema::{ExtremaKernel, ExtremaOperation, supported_type},
    catalogue::BuiltinAggregateResolver,
};
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::*;
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};
use std::sync::Arc;

pub(super) fn operation(name: &str) -> Option<ExtremaOperation> {
    match name {
        "min" => Some(ExtremaOperation::Min),
        "max" => Some(ExtremaOperation::Max),
        _ => None,
    }
}

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
    let ordinary = Arc::new(super::aggregate_n_owner::NOwner::for_extrema_overload(
        name,
        declaration.clone(),
        resolver.clone(),
    )?);
    let owner = Arc::new(ExtremaOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_aggregate_overloads(
        name,
        FunctionVisibility::Public,
        owner,
        ordinary,
    )
    .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
        subject: "builtin MIN/MAX pure owner",
        value: error.to_string().into(),
    })
}
struct ExtremaOwner {
    resolver: Arc<BuiltinAggregateResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
    operation: ExtremaOperation,
}
impl ExtremaOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<BuiltinAggregateResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        let expected = effects();
        let operation =
            operation(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
                subject: "uninstalled aggregate extrema",
                value: name.into(),
            })?;
        let valid = declaration.function_id().as_str() == format!("builtin.aggregate/{name}/v1")
            && declaration.kind() == FunctionKind::Aggregate
            && declaration.overloads().len() == 2
            && declaration.overloads().iter().any(|overload| {
                overload.identity.as_str() == format!("builtin.aggregate/{name}/derived-v1")
                    && overload.effects.as_ref() == Some(&expected)
                    && overload.aggregate.as_ref().is_some_and(|state| {
                        state.state_format.as_str() == format!("novarocks/{name}/state-v1")
                    })
            });
        if !valid {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin MIN/MAX pure declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let implementation =
            PureImplementationId::try_new(format!("builtin.aggregate/{name}/selected-v1"))?;
        let top_n_id = format!("builtin.aggregate/{name}/top-n-v1");
        let top_n_implementation =
            PureImplementationId::try_new(format!("builtin.aggregate/{name}/top-n-selected-v1"))?;
        let implementations = declaration
            .overloads()
            .iter()
            .map(|overload| {
                let top_n = overload.identity.as_str() == top_n_id;
                PureImplementationDeclaration {
                    overload: overload.identity.clone(),
                    implementation: if top_n {
                        top_n_implementation.clone()
                    } else {
                        implementation.clone()
                    },
                    abi: if top_n {
                        PureKernelAbi::AggregateV1
                    } else {
                        PureKernelAbi::AggregateWindowV1
                    },
                }
            })
            .collect();
        Ok(Self {
            resolver,
            declaration,
            implementations,
            operation,
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
fn has_inline_window_state(selected: &FunctionBindingSelection) -> bool {
    !matches!(selected.argument_types.first(),
        Some(FunctionArgumentType::Value(source)) if source.data_type() == &DataType::Utf8)
}

impl InlineAggregateWindowKernel for PreparedExtrema {
    fn copy_state(&self, state: &ExtremaState) -> Result<ExtremaState, KernelFailure> {
        match (self, state) {
            (PreparedExtrema::Fixed(_), ExtremaState::Fixed(value)) => {
                Ok(ExtremaState::Fixed(*value))
            }
            _ => Err(internal("MIN/MAX OVER state has no inline window copy")),
        }
    }
}

impl PureAggregateWindowImplementation for ExtremaOwner {
    fn prepare_aggregate_window(
        &self,
        aggregate: Arc<PreparedExtrema>,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
        if !has_inline_window_state(contract.call().selected()) {
            control
                .checkpoint(CompilePhase::FunctionSpecialization, 0)
                .map_err(compile_failure)?;
            return Err(invalid(
                "MIN/MAX OVER a UTF-8 value has no inline window state",
            ));
        }
        super::aggregate_window_adapter::prepare(aggregate, contract, control)
    }
}

impl FunctionBindingResolver for ExtremaOwner {
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
impl AggregateSignatureResolver for ExtremaOwner {
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
impl PureFunctionMetadataOwner for ExtremaOwner {
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
        if lifecycle != PureCallLifecycle::AggregateWindow {
            return Ok(());
        }
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        work.step()?;
        self.declaration.effect_declaration(&selected.overload)?;
        if logical_argument_count != 1 || selected.argument_types.len() != 1 {
            return Err(FunctionBindingError::InvalidBinding(
                "MIN/MAX window admission requires one exact value channel".into(),
            ));
        }
        let supported = has_inline_window_state(selected);
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
impl FunctionEffectOwner for ExtremaOwner {
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
impl PureAggregateImplementation for ExtremaOwner {
    type Kernel = PreparedExtrema;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<PreparedExtrema>, KernelFailure> {
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

impl ExtremaOwner {
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
                    "MIN/MAX requires its original scope and no environment".into(),
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
    ) -> Result<Arc<PreparedExtrema>, KernelFailure> {
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
                    "MIN/MAX preparation differs from its exact checked call",
                ));
            }
            // DISTINCT is idempotent for these exact extrema algorithms. The
            // original merge contract still forbids repeating DISTINCT there.
            let supported = contract.order_keys().is_empty()
                && input.request.logical_argument_count == 1
                && input.request.logical_argument_count == input.request.arguments.len();
            work.step().map_err(compile_failure)?;
            if !supported {
                return Err(invalid(
                    "MIN/MAX lifecycle requires one value and no function ORDER BY",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("MIN/MAX preparation has a stale selected binding"),
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
                    return Err(invalid("MIN/MAX requires canonical value arguments"));
                };
                if !actual.exactly_equals_observed::<KernelFailure>(selected, || {
                    work.step().map_err(compile_failure)
                })? {
                    return Err(invalid(
                        "MIN/MAX request differs from its exact selected source",
                    ));
                }
            }
            let Some(FunctionArgumentType::Value(source)) = input.selected.argument_types.first()
            else {
                return Err(invalid("MIN/MAX requires its selected value channel"));
            };
            let temporal = source.value_type().temporal();
            let supported = temporal.is_some()
                || ((supported_type(source.data_type()) || source.data_type() == &DataType::Utf8)
                    && match source.logical_type() {
                        ValueLogicalType::LargeInt => {
                            source.data_type() == &DataType::FixedSizeBinary(16)
                        }
                        ValueLogicalType::Physical => {
                            source.data_type() != &DataType::FixedSizeBinary(16)
                        }
                        _ => false,
                    });
            work.step().map_err(compile_failure)?;
            if !supported {
                return Err(invalid(
                    "MIN/MAX pure owner does not implement this input carrier",
                ));
            }
            let final_type = contract.final_type();
            let intermediate = contract.intermediate_type();
            let state_format = match self.operation {
                ExtremaOperation::Min => "novarocks/min/state-v1",
                ExtremaOperation::Max => "novarocks/max/state-v1",
            };
            let exact = contract.state_format().as_str() == state_format
                && final_type.nullable()
                && intermediate.nullable()
                && source.logical_type() == final_type.logical_type()
                && source.logical_type() == intermediate.logical_type()
                && temporal == final_type.value_type().temporal()
                && temporal == intermediate.value_type().temporal();
            work.step().map_err(compile_failure)?;
            if !exact
                || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
                    source.data_type(),
                    final_type.data_type(),
                    || work.step().map_err(compile_failure),
                )?
                || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
                    source.data_type(),
                    intermediate.data_type(),
                    || work.step().map_err(compile_failure),
                )?
            {
                return Err(invalid(
                    "MIN/MAX result and state differ from their complete source domain",
                ));
            }
            work.flush().map_err(compile_failure)?;
            let prepared = if source.data_type() == &DataType::Utf8 {
                PreparedExtrema::Utf8(Utf8ExtremaKernel {
                    contract,
                    operation: self.operation,
                })
            } else {
                PreparedExtrema::Fixed(ExtremaKernel {
                    contract,
                    operation: self.operation,
                })
            };
            let prepared = Arc::new(prepared);
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
