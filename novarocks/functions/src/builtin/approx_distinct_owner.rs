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

use super::approx_distinct_kernel::{ApproxDistinctKernel, ApproxProfile};
use super::binding_control;
use crate::kernel_control::{compile_failure, invalid};
use crate::*;
use arrow_schema::DataType;
use novarocks_type_contract::{
    AggregateStateArgumentContract, ArgumentControl, CallEffects, CallProofScope,
    CompileCheckpoints, CompilePhase, FunctionEffectDeclaration, FunctionInstanceState,
    FunctionNullBehavior, ObservableEffects, PureCompileControl, ValueLogicalType,
};
use std::sync::Arc;

const NAME: &str = "approx_distinct";

#[derive(Clone, Copy)]
struct Overload {
    name: &'static str,
    signature: &'static str,
    state: &'static str,
}

const UNKNOWN: Overload = Overload {
    name: "unknown-v1",
    signature: "(unknown)->i64",
    state: "unknown-v1",
};
const BOOLEAN: Overload = Overload {
    name: "boolean-v1",
    signature: "(bool)->i64",
    state: "boolean-set-v1",
};
const HASH: Overload = Overload {
    name: "default-hll-v1",
    signature: "(any)->i64",
    state: "hll-xxhash64-p11-v1",
};
const UNKNOWN_ERROR: Overload = Overload {
    name: "unknown-error-v1",
    signature: "(unknown,f64)->i64",
    state: "unknown-error-v1",
};
const BOOLEAN_ERROR: Overload = Overload {
    name: "boolean-error-v1",
    signature: "(bool,f64)->i64",
    state: "boolean-error-v1",
};
const HASH_ERROR: Overload = Overload {
    name: "explicit-error-hll-v1",
    signature: "(any,f64)->i64",
    state: "explicit-error-hll-v1",
};

impl Overload {
    fn identity(self) -> Result<FunctionOverloadId, FunctionBindingError> {
        FunctionOverloadId::try_new(format!("builtin.aggregate/{NAME}/{}", self.name))
            .map_err(binding_error)
    }

    fn state_format(self) -> Result<AggregateStateFormatIdentity, FunctionBindingError> {
        AggregateStateFormatIdentity::try_new(format!("novarocks/{NAME}/{}", self.state))
            .map_err(binding_error)
    }

    fn declaration(self) -> Result<FunctionOverloadDeclaration, FunctionBindingError> {
        Ok(FunctionOverloadDeclaration {
            identity: self.identity()?,
            argument_pattern: self.signature.into(),
            result_pattern: "i64".into(),
            semantics: FunctionSemantics {
                volatility: FunctionVolatility::Immutable,
                argument_evaluation: FunctionArgumentEvaluation::Eager,
                failure_behavior: FunctionFailureBehavior::Propagate,
                intrinsic_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
            },
            effects: Some(effects()),
            aggregate: Some(AggregateBindingDeclaration {
                intermediate_pattern: "binary".into(),
                state_format: self.state_format()?,
                state_argument_contract: AggregateStateArgumentContract::ExactSignature,
            }),
        })
    }
}

fn effects() -> FunctionEffectDeclaration {
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

fn matches_scalar(value: &FunctionValueType, data_type: DataType) -> bool {
    value.data_type() == &data_type
        && !value.nullable()
        && value.logical_type() == ValueLogicalType::Physical
}

fn binding_error(error: impl std::fmt::Display) -> FunctionBindingError {
    FunctionBindingError::InvalidBinding(error.to_string().into())
}

fn catalogue_error(error: impl std::fmt::Display) -> FunctionCatalogError {
    FunctionCatalogError::InvalidStableIdentity {
        subject: "builtin approx_distinct aggregate owner",
        value: error.to_string().into(),
    }
}

pub(super) fn definition() -> Result<FunctionDefinition, FunctionCatalogError> {
    let function_id =
        FunctionId::try_new(format!("builtin.aggregate/{NAME}/v1")).map_err(catalogue_error)?;
    let overloads = [
        UNKNOWN,
        BOOLEAN,
        HASH,
        UNKNOWN_ERROR,
        BOOLEAN_ERROR,
        HASH_ERROR,
    ]
    .map(Overload::declaration)
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .map_err(catalogue_error)?;
    let declaration =
        FunctionBindingDeclaration::try_new(function_id, FunctionKind::Aggregate, overloads)
            .map_err(catalogue_error)?;
    let declared_only = [UNKNOWN_ERROR, BOOLEAN_ERROR, HASH_ERROR]
        .map(Overload::identity)
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(catalogue_error)?;
    let implementations = [UNKNOWN, BOOLEAN, HASH]
        .into_iter()
        .map(|overload| {
            Ok(PureImplementationDeclaration {
                overload: overload.identity().map_err(catalogue_error)?,
                implementation: PureImplementationId::try_new(format!(
                    "builtin.aggregate/{NAME}/selected-{}",
                    overload.name
                ))?,
                abi: PureKernelAbi::AggregateV1,
            })
        })
        .collect::<Result<_, FunctionCatalogError>>()?;
    FunctionDefinition::try_new_pure_aggregate_with_declared_only(
        NAME,
        FunctionVisibility::Public,
        Arc::new(ApproxDistinctOwner {
            declaration,
            implementations,
        }),
        &declared_only,
    )
    .map_err(catalogue_error)
}

pub(crate) struct ApproxDistinctOwner {
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}

impl ApproxDistinctOwner {
    fn decision(
        &self,
        request: FunctionBindingRequest<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Overload, FunctionBindingError> {
        let Some(FunctionArgument::Value { value_type, .. }) = request.arguments.first() else {
            return Err(FunctionBindingError::NoMatchingOverload);
        };
        for argument in request.arguments {
            work.step()?;
            if !matches!(argument, FunctionArgument::Value { .. }) {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
        }
        let value = value_type.data_type();
        match request.logical_argument_count {
            1 => Ok(match value {
                DataType::Null => UNKNOWN,
                DataType::Boolean => BOOLEAN,
                _ => HASH,
            }),
            2 if matches!(
                request.arguments.get(1),
                Some(FunctionArgument::Value { value_type, .. }) if value_type.data_type() == &DataType::Float64
                    && value_type.logical_type() == ValueLogicalType::Physical
            ) =>
            {
                Ok(match value {
                    DataType::Null => UNKNOWN_ERROR,
                    DataType::Boolean => BOOLEAN_ERROR,
                    _ => HASH_ERROR,
                })
            }
            _ => Err(FunctionBindingError::NoMatchingOverload),
        }
    }

    fn materialize(
        &self,
        request: FunctionBindingRequest<'_>,
        required: Option<&FunctionOverloadId>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let overload = self.decision(request, work)?;
        let identity = overload.identity()?;
        if let Some(required) = required
            && required != &identity
        {
            return Err(FunctionBindingError::UnknownOverload(required.clone()));
        }
        Ok(FunctionBindingSelection {
            overload: identity,
            argument_types: binding_control::argument_types(request, work)?,
            result_type: FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, false)),
            aggregate: Some(AggregateBindingSelection {
                intermediate_type: FunctionValueType::new(DataType::Binary, false),
                state_format: overload.state_format()?,
                state_argument_contract: AggregateStateArgumentContract::ExactSignature,
            }),
        })
    }

    fn signature(
        &self,
        overload: Overload,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let overload_id = overload
            .identity()
            .and_then(|id| AggregateOverloadIdentity::try_new(id.as_str()).map_err(binding_error))
            .map_err(|error| FunctionResolutionError::BadSignature(error.to_string()))?;
        Ok(ResolvedAggregateSignature {
            overload: overload_id,
            argument_types: types.to_vec(),
            intermediate_type: DataType::Binary,
            output_type: DataType::Int64,
            state_format: overload
                .state_format()
                .map_err(|error| FunctionResolutionError::BadSignature(error.to_string()))?,
        })
    }
}

impl FunctionBindingResolver for ApproxDistinctOwner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| self.materialize(request, None, work))
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            self.materialize(request, Some(overload), work)
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            let overload = self.decision(request, work)?;
            let Some(aggregate) = &selected.aggregate else {
                return Err(binding_error(
                    "approx_distinct requires its state declaration",
                ));
            };
            if selected
                .overload
                .as_str()
                .strip_prefix("builtin.aggregate/approx_distinct/")
                != Some(overload.name)
                || aggregate
                    .state_format
                    .as_str()
                    .strip_prefix("novarocks/approx_distinct/")
                    != Some(overload.state)
                || aggregate.state_argument_contract
                    != AggregateStateArgumentContract::ExactSignature
                || !matches_scalar(&aggregate.intermediate_type, DataType::Binary)
                || !matches!(&selected.result_type, FunctionResultType::Scalar(result) if matches_scalar(result, DataType::Int64))
                || request.arguments.len() != selected.argument_types.len()
            {
                return Err(binding_error(
                    "approx_distinct selected state or result differs from the binding",
                ));
            }
            for (argument, original) in selected.argument_types.iter().zip(request.arguments) {
                work.step()?;
                let (
                    FunctionArgumentType::Value(argument),
                    FunctionArgument::Value {
                        value_type: original,
                        ..
                    },
                ) = (argument, original)
                else {
                    return Err(binding_error(
                        "approx_distinct requires its original value channels",
                    ));
                };
                if !argument.exactly_equals_checked_observed(original, || {
                    work.step().map_err(FunctionBindingError::from)
                })? {
                    return Err(binding_error(
                        "approx_distinct argument differs from its selected source",
                    ));
                }
            }
            Ok(())
        })
    }
}

impl AggregateSignatureResolver for ApproxDistinctOwner {
    fn resolve_aggregate(
        &self,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let overload = match types {
            [DataType::Null] => UNKNOWN,
            [DataType::Boolean] => BOOLEAN,
            [_] => HASH,
            [DataType::Null, DataType::Float64] => UNKNOWN_ERROR,
            [DataType::Boolean, DataType::Float64] => BOOLEAN_ERROR,
            [_, DataType::Float64] => HASH_ERROR,
            _ => {
                return Err(FunctionResolutionError::NoMatchingSignature {
                    candidates: 6,
                    binding_enforced: true,
                });
            }
        };
        self.signature(overload, types)
    }

    fn resolve_update_signature(
        &self,
        overload: &AggregateOverloadIdentity,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let selected = match (overload.as_str(), types) {
            ("builtin.aggregate/approx_distinct/unknown-v1", [DataType::Null]) => UNKNOWN,
            ("builtin.aggregate/approx_distinct/boolean-v1", [DataType::Boolean]) => BOOLEAN,
            ("builtin.aggregate/approx_distinct/default-hll-v1", [other])
                if !matches!(other, DataType::Null | DataType::Boolean) =>
            {
                HASH
            }
            (
                "builtin.aggregate/approx_distinct/unknown-error-v1",
                [DataType::Null, DataType::Float64],
            ) => UNKNOWN_ERROR,
            (
                "builtin.aggregate/approx_distinct/boolean-error-v1",
                [DataType::Boolean, DataType::Float64],
            ) => BOOLEAN_ERROR,
            (
                "builtin.aggregate/approx_distinct/explicit-error-hll-v1",
                [other, DataType::Float64],
            ) if !matches!(other, DataType::Null | DataType::Boolean) => HASH_ERROR,
            _ => {
                return Err(FunctionResolutionError::BadSignature(
                    "approx_distinct arguments differ from the exact overload".into(),
                ));
            }
        };
        self.signature(selected, types)
    }

    fn produces_null(&self) -> bool {
        false
    }
}

impl PureFunctionMetadataOwner for ApproxDistinctOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> FunctionBindingResolverOwner {
        FunctionBindingResolverOwner::borrowed_approx_distinct(self)
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
        control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
        if logical_argument_count == 1
            && let [FunctionArgumentType::Value(value)] = selected.argument_types.as_ref()
            && ApproxProfile::prepare(value).is_some()
        {
            Ok(())
        } else {
            Err(FunctionBindingError::UnavailableImplementation(
                selected.overload.clone(),
            ))
        }
    }
}

impl ApproxDistinctOwner {
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
impl FunctionEffectOwner for ApproxDistinctOwner {
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
            checked.require_binding_author(self, input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}
impl PureAggregateImplementation for ApproxDistinctOwner {
    type Kernel = ApproxDistinctKernel;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<ApproxDistinctKernel>, KernelFailure> {
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
            checked.require_binding_author(self, input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

impl ApproxDistinctOwner {
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
                    "approx_distinct requires its original scope and no environment".into(),
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
        work.finish_result(result, FunctionEffectOwnerError::Control)
    }
    fn prepare_core(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<ApproxDistinctKernel>, KernelFailure> {
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
                    "approx_distinct preparation differs from its exact checked call",
                ));
            }
            let supported = contract.order_keys().is_empty()
                && input.request.logical_argument_count == 1
                && input.request.logical_argument_count == input.request.arguments.len();
            work.step().map_err(compile_failure)?;
            if !supported {
                return Err(invalid(
                    "approx_distinct lifecycle does not implement function ORDER BY",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("approx_distinct preparation has a stale selected binding"),
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
                        "approx_distinct requires canonical value arguments",
                    ));
                };
                if !actual.exactly_equals_observed::<KernelFailure>(selected, || {
                    work.step().map_err(compile_failure)
                })? {
                    return Err(invalid(
                        "approx_distinct request differs from its exact selected source",
                    ));
                }
            }
            let Some(FunctionArgumentType::Value(value)) = input.selected.argument_types.first()
            else {
                return Err(invalid(
                    "approx_distinct requires its selected value channel",
                ));
            };
            let profile = ApproxProfile::prepare(value)
                .ok_or_else(|| invalid("approx_distinct requires its installed input profile"))?;
            work.flush().map_err(compile_failure)?;
            let prepared = Arc::new(ApproxDistinctKernel { contract, profile });
            work.flush().map_err(compile_failure)?;
            Ok(prepared)
        })();
        work.finish_result(result, compile_failure)
    }
}
