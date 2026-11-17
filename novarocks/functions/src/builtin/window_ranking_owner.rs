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

//! Each installed ranking Window function shares its binding, effects and CPU preparation owner.

use arrow_schema::DataType;
use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl, ValueLogicalType,
};

use super::catalogue::BuiltinScalarResolver;
use super::window_ranking::{PreparedRanking, RankingOperation};
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingSelection, FunctionCatalogError, FunctionDefinition,
    FunctionEffectOwner, FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionResultType, FunctionVisibility,
    FunctionVolatility, KernelFailure, PreparedWindowKernel, PureFunctionMetadataOwner,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, PureWindowImplementation,
    WindowCallContract, WindowCallOptions,
};

/// Only the five actual installed ranking Window declarations have an owner.
pub(super) fn operation(name: &str) -> Option<RankingOperation> {
    match name {
        "row_number" => Some(RankingOperation::RowNumber),
        "rank" => Some(RankingOperation::Rank),
        "dense_rank" => Some(RankingOperation::DenseRank),
        "cume_dist" => Some(RankingOperation::CumeDist),
        "percent_rank" => Some(RankingOperation::PercentRank),
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
    let owner = Arc::new(WindowRankingOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_window(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin window_ranking pure owner",
            value: error.to_string().into(),
        },
    )
}

pub(super) struct WindowRankingOwner {
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
    operation: RankingOperation,
}
impl WindowRankingOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let operation =
            operation(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
                subject: "uninstalled window_ranking",
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
                subject: "builtin window_ranking pure declaration",
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

impl FunctionBindingResolver for WindowRankingOwner {
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

impl PureFunctionMetadataOwner for WindowRankingOwner {
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

impl FunctionEffectOwner for WindowRankingOwner {
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

impl PureWindowImplementation for WindowRankingOwner {
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
    operation: RankingOperation,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let empty = input.request.arguments.is_empty()
        && input.request.logical_argument_count == 0
        && input.selected.argument_types.is_empty();
    work.step().map_err(compile_failure)?;
    if !empty {
        return Err(invalid(
            "ranking preparation requires its canonical zero-argument signature",
        ));
    }
    let carrier = match operation {
        RankingOperation::RowNumber | RankingOperation::Rank | RankingOperation::DenseRank => {
            DataType::Int64
        }
        RankingOperation::CumeDist | RankingOperation::PercentRank => DataType::Float64,
    };
    let exact = matches!(&input.selected.result_type, FunctionResultType::Scalar(ty)
        if *ty.data_type() == carrier && ty.logical_type() == ValueLogicalType::Physical && ty.nullable());
    work.step().map_err(compile_failure)?;
    if !exact {
        return Err(invalid(
            "ranking preparation differs from its canonical full result type",
        ));
    }
    let supported = options.frame().is_none_or(|frame| {
        frame.exclusion == novarocks_type_contract::WindowFrameExclusion::NoOthers
    });
    work.step().map_err(compile_failure)?;
    if !supported {
        return Err(invalid(
            "ranking requires the existing contiguous NO OTHERS geometry ABI",
        ));
    }
    // Legacy ranking ignores frame membership and NULL treatment. Preserve the
    // exact options; this is not a default frame or an input comparer.
    Ok(())
}

impl WindowRankingOwner {
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
                    || input.proof_scope == CallProofScope::Region(input.context.region));
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !exact_scope {
                return Err(FunctionBindingError::InvalidBinding(
                    "window_ranking has no environment dependencies and requires an exact proof scope"
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
                    "window_ranking preparation differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("window_ranking preparation has a stale selected binding"),
            })?;
            canonical_call(input, *contract.options(), self.operation, &mut work)?;
            work.flush().map_err(compile_failure)?;
            // The prepared object retains the same canonical contract. Its body is
            // a static pure implementation and needs no live resolver or authority.
            let prepared = Arc::new(PreparedRanking {
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
