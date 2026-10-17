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

//! One immutable owner binds, refines and prepares all exact conversions.

use std::sync::Arc;

use novarocks_type_contract::{
    CallEffects, CallProofScope, CompileCheckpoints, CompilePhase, DecimalOverflowPolicy,
    FunctionEffectDeclaration, FunctionIntrinsicRowError, PureCompileControl,
};

use super::value_conversion::{self, ValueConversionResolver};
use super::value_conversion_kernel::ConversionRecipe;
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingSelection, FunctionCatalogError, FunctionDefinition,
    FunctionEffectOwner, FunctionEffectOwnerError, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionVisibility, KernelEvaluationControl, KernelFailure, PreparedScalarKernel,
    PureFunctionMetadataOwner, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    PureScalarImplementation, ScalarCallContract, ScalarCallInput, ScalarKernelInstance,
    SelectedValues,
};

fn implementation_pairs() -> [(&'static str, &'static str); 9] {
    [
        (
            value_conversion::JSON_TEXT,
            "builtin.scalar/value_domain_conversion/json_text_same_structure/selected-v1",
        ),
        (
            value_conversion::OPAQUE_BINARY,
            "builtin.scalar/value_domain_conversion/opaque_to_binary_same_structure/selected-v1",
        ),
        (
            value_conversion::SIGNED_LARGEINT,
            "builtin.scalar/value_domain_conversion/signed_to_largeint/selected-v1",
        ),
        (
            value_conversion::LARGEINT_SIGNED,
            "builtin.scalar/value_domain_conversion/largeint_to_signed_null_overflow/selected-v1",
        ),
        (
            value_conversion::LARGEINT_DECIMAL,
            "builtin.scalar/value_domain_conversion/largeint_to_decimal_exact/selected-v1",
        ),
        (
            value_conversion::LARGEINT_FLOAT,
            "builtin.scalar/value_domain_conversion/largeint_to_float_round/selected-v1",
        ),
        (
            value_conversion::LARGEINT_TEXT,
            "builtin.scalar/value_domain_conversion/largeint_to_utf8_text/selected-v1",
        ),
        (
            value_conversion::UTF8_LARGEINT,
            "builtin.scalar/value_domain_conversion/utf8_to_largeint_null_invalid/selected-v1",
        ),
        (
            value_conversion::NULL_LIFT,
            "builtin.scalar/value_domain_conversion/null_to_typed_nullable/selected-v1",
        ),
    ]
}

pub(super) fn definition(
    declaration: FunctionBindingDeclaration,
    resolver: ValueConversionResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(ValueConversionOwner::new(declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(
        value_conversion::VALUE_CONVERSION_NAME,
        FunctionVisibility::Hidden,
        owner,
    )
    .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
        subject: "internal value-domain conversion pure owner",
        value: error.to_string().into(),
    })
}

struct ValueConversionOwner {
    resolver: Arc<ValueConversionResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl ValueConversionOwner {
    fn new(
        declaration: FunctionBindingDeclaration,
        resolver: ValueConversionResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let pairs = implementation_pairs();
        if declaration.function_id().as_str() != value_conversion::VALUE_CONVERSION_FUNCTION_ID
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != pairs.len()
            || declaration.overloads().iter().any(|overload| {
                !pairs
                    .iter()
                    .any(|(id, _)| overload.identity.as_str() == *id)
                    || overload.aggregate.is_some()
                    || overload.effects.as_ref()
                        != Some(&value_conversion::overload_effects(
                            overload.identity.as_str(),
                        ))
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "internal value-domain conversion pure declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let implementations = declaration
            .overloads()
            .iter()
            .map(|overload| {
                let (_, implementation) = pairs
                    .iter()
                    .find(|(id, _)| overload.identity.as_str() == *id)
                    .ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
                        subject: "internal value-domain conversion overload",
                        value: overload.identity.as_str().into(),
                    })?;
                Ok(PureImplementationDeclaration {
                    overload: overload.identity.clone(),
                    implementation: PureImplementationId::try_new(*implementation)?,
                    abi: PureKernelAbi::ScalarV1,
                })
            })
            .collect::<Result<_, FunctionCatalogError>>()?;
        Ok(Self {
            resolver: Arc::new(resolver),
            declaration,
            implementations,
        })
    }

    fn call_effects(&self, input: CallEffectInput<'_>) -> CallEffects {
        let mut base = value_conversion::overload_effects(input.selected.overload.as_str());
        if input.selected.overload.as_str() == value_conversion::LARGEINT_DECIMAL {
            let can_overflow = match &input.selected.result_type {
                crate::FunctionResultType::Scalar(target) => {
                    value_conversion::largeint_decimal::DecimalRecipe::try_new(target.data_type())
                        .is_none_or(|recipe| recipe.can_overflow())
                }
                _ => true,
            };
            base.own_row_error = if can_overflow
                && input.decimal_overflow_policy == DecimalOverflowPolicy::ReportError
            {
                FunctionIntrinsicRowError::MayRaise
            } else {
                FunctionIntrinsicRowError::NoRowError
            };
        }
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::default(),
            proof_scope: input.proof_scope,
        }
    }
}

impl FunctionBindingResolver for ValueConversionOwner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver.resolve(request, control)
    }
    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
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

impl PureFunctionMetadataOwner for ValueConversionOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> crate::FunctionBindingResolverOwner {
        crate::FunctionBindingResolverOwner::borrowed_conversion(Arc::clone(&self.resolver))
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }
    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}

impl FunctionEffectOwner for ValueConversionOwner {
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
            checked.require_binding_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

impl PureScalarImplementation for ValueConversionOwner {
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
            checked.require_binding_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

#[derive(Debug)]
struct PreparedConversion {
    contract: Arc<ScalarCallContract>,
    recipe: Arc<ConversionRecipe>,
    // Actual immutable recipe inline/graph backing, excluding the canonical
    // selected-contract backing and Arc allocation header. Formal host Account,
    // allocation grant and final-free integration remains a separate obligation.
    _recipe_retained_bytes: usize,
}
impl PreparedScalarKernel for PreparedConversion {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }
    fn instance_retained_upper_bound(&self) -> usize {
        // Shared immutable preparation backing belongs to its preparation owner,
        // as for ScalarKernelInstanceHandle. This is the boxed instance handle,
        // not a MEM charge, allocation authorization or recipe backing invoice.
        std::mem::size_of::<ConversionInstance>()
    }
    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(ConversionInstance {
            recipe: Arc::clone(&self.recipe),
        }))
    }
}

struct ConversionInstance {
    recipe: Arc<ConversionRecipe>,
}
impl ScalarKernelInstance for ConversionInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        self.recipe.evaluate(input, control)
    }
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
pub(super) fn prepared_for_test(
    source: &crate::FunctionValueType,
    target: &crate::FunctionValueType,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    prepared_for_test_with_policy(
        source,
        target,
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    )
}

#[cfg(test)]
pub(super) fn prepared_for_test_with_policy(
    source: &crate::FunctionValueType,
    target: &crate::FunctionValueType,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    tests::prepare(source, target, policy)
}

#[cfg(test)]
#[path = "value_conversion_owner_tests.rs"]
mod tests;

impl ValueConversionOwner {
    fn refine_selected_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        let result = (|| {
            let identity = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Scalar;
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !identity {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            let scope = matches!(input.proof_scope, CallProofScope::Unconditional)
                || input.proof_scope == CallProofScope::Domain(input.context.domain);
            let environment = input.environment.is_empty();
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !scope || !environment {
                return Err(FunctionBindingError::InvalidBinding(
                    "value conversion requires its exact proof scope and no environment".into(),
                )
                .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
                other => FunctionEffectOwnerError::Owner(other),
            })?;
            Ok(self.call_effects(input))
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
        let result = (|| {
            let exact = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Scalar
                && contract.function_id() == input.function_id
                && std::ptr::eq(contract.selected(), input.selected)
                && contract.context() == input.context
                && contract.decimal_overflow_policy() == input.decimal_overflow_policy
                && contract.effects() == &self.call_effects(input)
                && input.environment.is_empty();
            work.step().map_err(compile_failure)?;
            if !exact {
                return Err(invalid(
                    "value conversion differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("value conversion preparation has a stale selected binding"),
            })?;
            work.flush().map_err(compile_failure)?;
            let recipe = ConversionRecipe::try_new(&contract, control)?;
            let recipe_retained_bytes = recipe.retained_bytes();
            // Recipe data is immutable and shared. No instance compiles it again
            // or acquires a runtime capability; preparation retains no control.
            let prepared: Arc<dyn PreparedScalarKernel> = Arc::new(PreparedConversion {
                contract,
                recipe: Arc::new(recipe),
                _recipe_retained_bytes: recipe_retained_bytes,
            });
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
