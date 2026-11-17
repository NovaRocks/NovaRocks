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

//! The exact from_unixtime binding, effects and CPU preparation owner.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::catalogue::BuiltinScalarResolver;
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

/// The four actual declared overloads retain their original signatures.
pub(super) fn operation(name: &str) -> Option<()> {
    (name == "from_unixtime").then_some(())
}

/// The operation is pure only with its exact frozen original timezone input.
pub(super) fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::MayRaise,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::Strict,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([
            novarocks_type_contract::SemanticParameterKey::TimeZone,
        ]),
    }
}

pub(super) fn definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(FromUnixtimeOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin from_unixtime original epoch formatting pure owner",
            value: error.to_string().into(),
        },
    )
}

pub(super) struct FromUnixtimeOwner {
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl FromUnixtimeOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        operation(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
            subject: "uninstalled from_unixtime original epoch formatting",
            value: name.into(),
        })?;
        let function = format!("builtin.scalar/{name}/v1");
        let expected = effects();
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 4
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin from_unixtime original epoch formatting pure declaration",
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
                abi: PureKernelAbi::ScalarV1,
            })
            .collect();
        Ok(Self {
            resolver: Arc::new(resolver),
            declaration,
            implementations,
        })
    }

    fn call_effects(
        &self,
        scope: CallProofScope,
        environment: &[novarocks_type_contract::SemanticParameterRef],
    ) -> CallEffects {
        let base = effects();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: environment.into(),
            proof_scope: scope,
        }
    }
}

impl FunctionBindingResolver for FromUnixtimeOwner {
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

impl PureFunctionMetadataOwner for FromUnixtimeOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> crate::FunctionBindingResolverOwner {
        crate::FunctionBindingResolverOwner::fixed_scalar(Arc::clone(&self.resolver))
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
    fn admit_frozen_environment_observed(
        &self,
        selected: &FunctionBindingSelection,
        environment: &[novarocks_type_contract::SemanticParameterRef],
        parameters: &novarocks_type_contract::SemanticParameters,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
        valid_environment_references(environment, parameters)
            .map(|_| ())
            .map_err(|_| FunctionBindingError::UnavailableImplementation(selected.overload.clone()))
    }
}

impl FunctionEffectOwner for FromUnixtimeOwner {
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

impl PureScalarImplementation for FromUnixtimeOwner {
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
struct PreparedFromUnixtime {
    contract: Arc<ScalarCallContract>,
    zone: super::calendar_unixtime::TimeZoneSpec,
    format: Option<crate::ConstantValue>,
}
impl PreparedScalarKernel for PreparedFromUnixtime {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<FromUnixtimeInstance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(FromUnixtimeInstance {
            zone: self.zone,
            format: self.format.clone(),
        }))
    }
}

struct FromUnixtimeInstance {
    zone: super::calendar_unixtime::TimeZoneSpec,
    format: Option<crate::ConstantValue>,
}
impl ScalarKernelInstance for FromUnixtimeInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        super::calendar_unixtime::evaluate_selected(input, self.zone, self.format.as_ref(), control)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

fn valid_environment(
    input: CallEffectInput<'_>,
) -> Result<super::calendar_unixtime::TimeZoneSpec, FunctionBindingError> {
    valid_environment_references(input.environment, input.parameters)
}
fn valid_environment_references(
    environment: &[novarocks_type_contract::SemanticParameterRef],
    parameters: &novarocks_type_contract::SemanticParameters,
) -> Result<super::calendar_unixtime::TimeZoneSpec, FunctionBindingError> {
    use novarocks_type_contract::{SemanticParameterKey, SemanticParameterValue};
    let [reference] = environment else {
        return Err(FunctionBindingError::InvalidBinding(
            "from_unixtime requires exactly its original frozen TimeZone reference".into(),
        ));
    };
    if reference.expected_key != SemanticParameterKey::TimeZone {
        return Err(FunctionBindingError::InvalidBinding(
            "from_unixtime semantic reference is not TimeZone".into(),
        ));
    }
    let value = parameters
        .require(*reference)
        .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?;
    let SemanticParameterValue::TimeZone(zone) = value else {
        return Err(FunctionBindingError::InvalidBinding(
            "from_unixtime semantic reference has the wrong value kind".into(),
        ));
    };
    match super::calendar_unixtime::parse_tz(zone){Some(zone @ (super::calendar_unixtime::TimeZoneSpec::Fixed(_)|super::calendar_unixtime::TimeZoneSpec::Named(_)))=>Ok(zone),_=>Err(FunctionBindingError::InvalidBinding("from_unixtime original ProcessLocal fallback has no frozen rule author; no default is allowed".into()))}
}
fn prepare_profile(
    input: CallEffectInput<'_>,
    contract: &ScalarCallContract,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<crate::ConstantValue>, KernelFailure> {
    use crate::{FunctionArgument, FunctionArgumentType, ValueLogicalType};
    use arrow_array::{Array, StringArray};
    use arrow_schema::DataType;
    let ty = contract.result_type();
    if ty.logical_type() != ValueLogicalType::Physical
        || ty.data_type() != &DataType::Utf8
        || !ty.nullable()
    {
        return Err(invalid(
            "from_unixtime requires its original nullable Utf8 result",
        ));
    }
    let types = &contract.selected().argument_types;
    if matches!(types.first(),Some(FunctionArgumentType::Value(source)) if matches!(source.data_type(),DataType::Date32|DataType::Timestamp(_, _)))
    {
        return Err(invalid(
            "from_unixtime rejects its declared Date32/Timestamp epoch carrier: original integer reader returns from_unixtime expects int",
        ));
    }
    match (types.as_ref(), input.request.arguments) {
        ([FunctionArgumentType::Value(source)], [_])
            if source.logical_type() == ValueLogicalType::Physical
                && source.data_type() == &DataType::Int64 =>
        {
            Ok(None)
        }
        (
            [
                FunctionArgumentType::Value(source),
                FunctionArgumentType::Value(format),
            ],
            [
                _,
                FunctionArgument::Value {
                    constant: Some(constant),
                    ..
                },
            ],
        ) if source.logical_type() == ValueLogicalType::Physical
            && source.data_type() == &DataType::Utf8
            && format.logical_type() == ValueLogicalType::Physical
            && format.data_type() == &DataType::Utf8 =>
        {
            let array = constant
                .pool()
                .array()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid("from_unixtime format constant has the wrong carrier"))?;
            let row = constant.ordinal() as usize;
            if !array.is_null(row) {
                work.flush().map_err(compile_failure)?;
                let normalized =
                    super::calendar_unixtime::normalize_from_unixtime_format(array.value(row));
                work.flush().map_err(compile_failure)?;
                if let Some(normalized) = normalized {
                    work.flush().map_err(compile_failure)?;
                    let malformed = chrono::format::StrftimeItems::new(&normalized)
                        .any(|item| matches!(item, chrono::format::Item::Error));
                    work.flush().map_err(compile_failure)?;
                    if malformed {
                        return Err(invalid(
                            "from_unixtime refuses the original malformed-format panic shape pending a separate v1 bug ruling",
                        ));
                    }
                }
            }
            work.step().map_err(compile_failure)?;
            Ok(Some(constant.clone()))
        }
        ([_, _], [_, FunctionArgument::Value { constant: None, .. }]) => Err(invalid(
            "from_unixtime requires an exact constant format proof; dynamic original panic shapes are unproven",
        )),
        _ => Err(invalid(
            "from_unixtime declared Date32/Timestamp argument is rejected by the original integer reader; no carrier repair is allowed",
        )),
    }
}
#[cfg(test)]
pub(super) fn prepared_for_test(
    sources: &[crate::FunctionValueType],
    constants: &[Option<crate::ConstantValue>],
    zone: Option<&str>,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    use crate::{FunctionArgument, ScopedExpressionEffects, specialize_scalar};
    use novarocks_type_contract::{
        EvaluationDemand, EvaluationRegionId, ExpressionEffectContext, ExpressionUseId,
        SemanticParameterId, SemanticParameterKey, SemanticParameterRef, SemanticParameterValue,
        SemanticParameters,
    };
    let (_, signatures) = super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(name, _)| name == "from_unixtime")
        .unwrap();
    let (declaration, resolver) = super::catalogue::scalar_definition_parts(
        "from_unixtime",
        &signatures,
        FunctionKind::Scalar,
    )
    .unwrap();
    let owner = FromUnixtimeOwner::new("from_unixtime", declaration, resolver).unwrap();
    if sources.len() != constants.len() {
        return Err(crate::FunctionSpecializationFailure::InvalidInput(
            "unixtime fixture sources/constant counts differ",
        ));
    }
    let args = sources
        .iter()
        .zip(constants)
        .map(|(ty, constant)| FunctionArgument::Value {
            value_type: ty.clone(),
            constant: constant.clone(),
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &args,
        logical_argument_count: args.len(),
    };
    let selected = Arc::new(owner.resolve(request, control)?);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        region: EvaluationRegionId::new(7),
        demand: EvaluationDemand::Value,
    };
    let refs = zone
        .map(|_| {
            vec![SemanticParameterRef {
                id: SemanticParameterId::new(17),
                expected_key: SemanticParameterKey::TimeZone,
            }]
        })
        .unwrap_or_default();
    let parameters = SemanticParameters::try_new(
        zone.map(|zone| {
            vec![(
                SemanticParameterId::new(17),
                SemanticParameterValue::TimeZone(zone.into()),
            )]
        })
        .unwrap_or_default(),
    )
    .unwrap();
    let uses = (0..args.len())
        .map(|i| Some(ExpressionUseId::new(42 + i as u32)))
        .collect::<Vec<_>>();
    let input = CallEffectInput {
        context,
        argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected: &selected,
        request,
        environment: &refs,
        parameters: &parameters,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        proof_scope: CallProofScope::Unconditional,
    };
    specialize_scalar(
        &owner,
        input,
        selected.clone(),
        ScopedExpressionEffects::pure_value(context),
        control,
    )
    .map(|s| s.into_prepared())
}

impl FromUnixtimeOwner {
    fn refine_selected_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        let result = (|| {
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if input.function_id != self.declaration.function_id()
                || input.kind != FunctionKind::Scalar
            {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            if valid_environment(input).is_err()
                || !matches!(input.proof_scope, CallProofScope::Unconditional)
                    && input.proof_scope != CallProofScope::Region(input.context.region)
            {
                return Err(FunctionBindingError::InvalidBinding(
                "from_unixtime requires its exact original frozen non-Local TimeZone author and proof scope".into(),
            )
            .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
                other => FunctionEffectOwnerError::Owner(other),
            })?;
            Ok(self.call_effects(input.proof_scope, input.environment))
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
        let result: Result<Arc<dyn PreparedScalarKernel>, KernelFailure> = (|| {
            work.step().map_err(compile_failure)?;
            if input.function_id != self.declaration.function_id()
                || input.kind != FunctionKind::Scalar
                || contract.function_id() != input.function_id
                || !std::ptr::eq(contract.selected(), input.selected)
                || contract.context() != input.context
                || contract.decimal_overflow_policy() != input.decimal_overflow_policy
                || contract.effects() != &self.call_effects(input.proof_scope, input.environment)
                || valid_environment(input).is_err()
            {
                return Err(invalid(
                    "from_unixtime original epoch formatting preparation differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate()
                .map_err(|error| match error {
                    FunctionBindingError::Control(error) => compile_failure(error),
                    _ => invalid("from_unixtime original epoch formatting preparation has a stale selected binding"),
                })?;
            // The prepared object retains the same canonical contract. Its body is
            // a static pure implementation and needs no live resolver or authority.
            let zone = valid_environment(input).map_err(|error| invalid(&error.to_string()))?;
            let format = prepare_profile(input, &contract, &mut work)?;
            Ok(Arc::new(PreparedFromUnixtime {
                contract,
                zone,
                format,
            }) as Arc<dyn PreparedScalarKernel>)
        })();
        if result.as_ref().is_err_and(KernelFailure::is_interruption) {
            return result;
        }
        work.finish().map_err(compile_failure)?;
        result
    }
}
