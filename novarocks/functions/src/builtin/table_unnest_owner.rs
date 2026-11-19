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

//! Installed UNNEST binding, effects and selected table lifecycle owner.
use super::catalogue::{
    BUILTIN_UNNEST_FUNCTION_ID, BUILTIN_UNNEST_OVERLOAD_ID, BuiltinUnnestResolver,
};
use crate::kernel_control::{compile_failure, invalid};
use crate::*;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};
use std::sync::Arc;

pub(super) fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Table,
        instance_state: FunctionInstanceState::TableInstance,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::default(),
    }
}
pub(super) fn definition(
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinUnnestResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let expected = effects();
    if declaration.function_id().as_str() != BUILTIN_UNNEST_FUNCTION_ID
        || declaration.kind() != FunctionKind::Table
        || declaration.overloads().len() != 1
        || declaration.overloads()[0].identity.as_str() != BUILTIN_UNNEST_OVERLOAD_ID
        || declaration.overloads()[0].effects.as_ref() != Some(&expected)
        || declaration.overloads()[0].aggregate.is_some()
    {
        return Err(FunctionCatalogError::InvalidStableIdentity {
            subject: "UNNEST declaration",
            value: declaration.function_id().as_str().into(),
        });
    }
    let implementations = Box::from([PureImplementationDeclaration {
        overload: declaration.overloads()[0].identity.clone(),
        implementation: PureImplementationId::try_new("builtin.table/unnest/selected-v1")?,
        abi: PureKernelAbi::TableV1,
    }]);
    FunctionDefinition::try_new_pure_table(
        "unnest",
        FunctionVisibility::Public,
        Arc::new(Owner {
            declaration,
            resolver,
            implementations,
        }),
    )
    .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
        subject: "UNNEST pure owner",
        value: error.to_string().into(),
    })
}
pub(crate) struct Owner {
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinUnnestResolver,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl Owner {
    pub(crate) fn validate_selected_with_creator_in<H>(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        control: &dyn PureCompileControl,
    ) -> Result<(), crate::FunctionBindingCreationFailure<H>> {
        self.resolver.validate_selected_with_creator_in(
            selected,
            request,
            &self.declaration.overloads()[0].identity,
            creation,
            control,
        )
    }
}
impl FunctionBindingResolver for Owner {
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
impl PureFunctionMetadataOwner for Owner {
    fn binding_resolver_owner(self: Arc<Self>) -> crate::FunctionBindingResolverOwner {
        crate::FunctionBindingResolverOwner::unnest(self)
    }
    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }
    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}
impl FunctionEffectOwner for Owner {
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
            checked.require_binding_author(self, input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}
impl PureTableImplementation for Owner {
    fn prepare_table(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<TableCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedTableKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result = (|| {
            let call = contract.call();
            let same = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Table
                && std::ptr::eq(call.selected(), input.selected)
                && call.context() == input.context
                && call.decimal_overflow_policy() == input.decimal_overflow_policy
                && call.effects().argument_control == ArgumentControl::Table;
            work.step().map_err(compile_failure)?;
            if !same {
                return Err(invalid(
                    "UNNEST preparation differs from the exact selected call",
                ));
            }
            super::table_unnest::prepare(contract, &mut work)
        })();
        if result.as_ref().is_err_and(KernelFailure::is_interruption) {
            return result;
        }
        work.finish().map_err(compile_failure)?;
        result
    }
    fn prepare_table_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        contract: Arc<TableCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedTableKernel>, crate::CheckedOwnerFailure<KernelFailure>> {
        checked.require_binding_author(self, input).map_err(|_| {
            crate::CheckedOwnerFailure::Original(invalid(
                "UNNEST preparation borrows a different checked author",
            ))
        })?;
        // The original table preparation validates its exact immutable call;
        // it never reconstructs an expected selected type.
        self.prepare_table(input, contract, control)
            .map_err(crate::CheckedOwnerFailure::Original)
    }
}

#[cfg(test)]
#[path = "table_unnest_created_tests.rs"]
mod created_tests;

impl Owner {
    fn refine_selected_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        let result = (|| {
            let header = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Table;
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !header {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            let scope = input.environment.is_empty()
                && (input.proof_scope == CallProofScope::Unconditional
                    || input.proof_scope == input.context.proof_scope());
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !scope {
                return Err(FunctionBindingError::InvalidBinding(
                    "UNNEST requires its exact environment-free scope".into(),
                )
                .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(cause) => FunctionEffectOwnerError::Control(cause),
                error => FunctionEffectOwnerError::Owner(error),
            })?;
            let base = effects();
            Ok(CallEffects {
                value_stability: base.value_stability,
                own_row_error: base.own_row_error,
                failure_behavior: base.failure_behavior,
                null_behavior: base.null_behavior,
                argument_control: base.argument_control,
                instance_state: base.instance_state,
                observable_effects: base.observable_effects,
                environment: Box::default(),
                proof_scope: input.proof_scope,
            })
        })();
        if matches!(&result, Err(FunctionEffectOwnerError::Control(_))) {
            return result;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        result
    }
}
