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

//! Each installed FIRST/LAST Window function shares its binding, effects and CPU preparation owner.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::catalogue::BuiltinScalarResolver;
use super::window_value::{PreparedValue, ValueOperation};
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, FunctionArgument, FunctionArgumentType, FunctionBindingDeclaration,
    FunctionBindingError, FunctionBindingRequest, FunctionBindingResolver,
    FunctionBindingSelection, FunctionCatalogError, FunctionDefinition, FunctionEffectOwner,
    FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError,
    FunctionKind, FunctionResultType, FunctionVisibility, FunctionVolatility, KernelFailure,
    PreparedWindowKernel, PureFunctionMetadataOwner, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, PureWindowImplementation, WindowCallContract,
    WindowCallOptions,
};

/// Only the two actual installed FIRST/LAST Window declarations have an owner.
pub(super) fn operation(name: &str) -> Option<ValueOperation> {
    match name {
        "first_value" => Some(ValueOperation::First),
        "last_value" => Some(ValueOperation::Last),
        _ => None,
    }
}

/// Complete partition input is required; no scalar row-error or NULL mask applies.
pub(super) fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Window,
        instance_state: FunctionInstanceState::WindowPartition,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

pub(super) fn definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(WindowValueOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_window(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin window_value pure owner",
            value: error.to_string().into(),
        },
    )
}

pub(super) struct WindowValueOwner {
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
    operation: ValueOperation,
}
impl WindowValueOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let operation =
            operation(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
                subject: "uninstalled window_value",
                value: name.into(),
            })?;
        let function = format!("builtin.window/{name}/v1");
        let expected = effects();
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Window
            || declaration.overloads().len() != 1
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin window_value pure declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let implementation =
            PureImplementationId::try_new(format!("builtin.window/{name}/selected-v1"))?;
        let implementations = declaration
            .overloads()
            .iter()
            .map(|overload| PureImplementationDeclaration {
                overload: overload.identity.clone(),
                implementation: implementation.clone(),
                abi: PureKernelAbi::WindowV1,
            })
            .collect();
        Ok(Self {
            resolver: Arc::new(resolver),
            declaration,
            implementations,
            operation,
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

impl FunctionBindingResolver for WindowValueOwner {
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

impl PureFunctionMetadataOwner for WindowValueOwner {
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

impl FunctionEffectOwner for WindowValueOwner {
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

impl PureWindowImplementation for WindowValueOwner {
    fn prepare_window(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
        self.prepare_core(input, contract, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }
    fn prepare_window_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, crate::CheckedOwnerFailure<KernelFailure>> {
        self.prepare_core(input, contract, control, || {
            checked.require_binding_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

fn canonical_call(
    input: CallEffectInput<'_>,
    options: WindowCallOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let one = input.request.arguments.len() == 1
        && input.request.logical_argument_count == 1
        && input.selected.argument_types.len() == 1;
    work.step().map_err(compile_failure)?;
    if !one {
        return Err(invalid(
            "FIRST/LAST preparation requires exactly one canonical value argument",
        ));
    }
    let (
        FunctionArgument::Value {
            value_type: actual, ..
        },
        FunctionArgumentType::Value(source),
    ) = (
        &input.request.arguments[0],
        &input.selected.argument_types[0],
    )
    else {
        return Err(invalid(
            "FIRST/LAST preparation requires its canonical value channel",
        ));
    };
    if !actual
        .exactly_equals_observed::<KernelFailure>(source, || work.step().map_err(compile_failure))?
    {
        return Err(invalid(
            "FIRST/LAST request differs from its complete selected source type",
        ));
    }
    let FunctionResultType::Scalar(result) = &input.selected.result_type else {
        return Err(invalid("FIRST/LAST preparation requires a scalar result"));
    };
    let exact = result.nullable()
        && result.same_value_domain_observed::<KernelFailure>(source, || {
            work.step().map_err(compile_failure)
        })?;
    work.step().map_err(compile_failure)?;
    if !exact
        || !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            source.data_type(),
            result.data_type(),
            || work.step().map_err(compile_failure),
        )?
    {
        return Err(invalid(
            "FIRST/LAST result differs from its complete nullable source domain",
        ));
    }
    let supported = options.frame().is_none_or(|frame| {
        frame.exclusion == novarocks_type_contract::WindowFrameExclusion::NoOthers
    });
    work.step().map_err(compile_failure)?;
    if !supported {
        return Err(invalid(
            "FIRST/LAST requires the existing contiguous NO OTHERS geometry ABI",
        ));
    }
    // SQL's original frame/peer owner already normalizes and authors membership.
    // Preserve options verbatim; this owner does not derive defaults or ordering.
    Ok(())
}

impl WindowValueOwner {
    fn refine_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        let result = (|| {
            let exact_header = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Window;
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !exact_header {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            let exact_scope = input.environment.is_empty()
                && (matches!(input.proof_scope, CallProofScope::Unconditional)
                    || input.proof_scope == input.context.proof_scope());
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !exact_scope {
                return Err(FunctionBindingError::InvalidBinding(
                    "window_value has no environment dependencies and requires an exact proof scope"
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
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result: Result<Arc<dyn PreparedWindowKernel>, KernelFailure> = (|| {
            let exact_call = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Window
                && contract.call().function_id() == input.function_id
                && std::ptr::eq(contract.call().selected(), input.selected)
                && contract.call().context() == input.context
                && contract.call().decimal_overflow_policy() == input.decimal_overflow_policy
                && contract.call().effects() == &self.call_effects(input.proof_scope)
                && input.environment.is_empty();
            work.step().map_err(compile_failure)?;
            if !exact_call {
                return Err(invalid(
                    "window_value preparation differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("window_value preparation has a stale selected binding"),
            })?;
            canonical_call(input, *contract.options(), &mut work)?;
            work.flush().map_err(compile_failure)?;
            // The prepared object retains the same canonical contract. Its body is
            // a static pure implementation and needs no live resolver or authority.
            let prepared = Arc::new(PreparedValue {
                contract,
                operation: self.operation,
            }) as Arc<dyn PreparedWindowKernel>;
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
