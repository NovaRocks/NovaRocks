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

//! Each installed extended-calendar function shares its binding, effects and CPU preparation owner.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::calendar_extended::CalendarExtendedOp;
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

/// Installed extended calendar operations share the original pure value algorithms.
pub(super) fn operation(name: &str) -> Option<CalendarExtendedOp> {
    match name {
        "date_trunc" => Some(CalendarExtendedOp::Trunc),
        "convert_tz" => Some(CalendarExtendedOp::ConvertTz),
        "unix_timestamp" => Some(CalendarExtendedOp::UnixTimestamp),
        "to_datetime_ntz" => Some(CalendarExtendedOp::EpochNtz),
        "timestampdiff" => Some(CalendarExtendedOp::TimestampDiff),
        "date_add" | "adddate" | "days_add" => Some(CalendarExtendedOp::DaysShift(1)),
        "date_sub" | "subdate" | "days_sub" => Some(CalendarExtendedOp::DaysShift(-1)),
        "weeks_add" => Some(CalendarExtendedOp::DaysShift(7)),
        "weeks_sub" => Some(CalendarExtendedOp::DaysShift(-7)),
        "add_months" | "months_add" => Some(CalendarExtendedOp::MonthsShift(1)),
        "months_sub" => Some(CalendarExtendedOp::MonthsShift(-1)),
        "years_add" => Some(CalendarExtendedOp::MonthsShift(12)),
        "years_sub" => Some(CalendarExtendedOp::MonthsShift(-12)),
        "seconds_add" => Some(CalendarExtendedOp::DurationShift(
            super::calendar_duration::CalendarDurationShift::SecondsAdd,
        )),
        "seconds_sub" => Some(CalendarExtendedOp::DurationShift(
            super::calendar_duration::CalendarDurationShift::SecondsSub,
        )),
        "minutes_add" => Some(CalendarExtendedOp::DurationShift(
            super::calendar_duration::CalendarDurationShift::MinutesAdd,
        )),
        "minutes_sub" => Some(CalendarExtendedOp::DurationShift(
            super::calendar_duration::CalendarDurationShift::MinutesSub,
        )),
        "hours_add" => Some(CalendarExtendedOp::DurationShift(
            super::calendar_duration::CalendarDurationShift::HoursAdd,
        )),
        "hours_sub" => Some(CalendarExtendedOp::DurationShift(
            super::calendar_duration::CalendarDurationShift::HoursSub,
        )),
        "microseconds_add" => Some(CalendarExtendedOp::DurationShift(
            super::calendar_duration::CalendarDurationShift::MicrosecondsAdd,
        )),
        "microseconds_sub" => Some(CalendarExtendedOp::DurationShift(
            super::calendar_duration::CalendarDurationShift::MicrosecondsSub,
        )),

        "date_format" => Some(CalendarExtendedOp::DateFormat),
        "str_to_date" => Some(CalendarExtendedOp::Parse(
            super::calendar_extended_parse::CalendarParseOp::StrToDate,
        )),
        "last_day" => Some(CalendarExtendedOp::Parse(
            super::calendar_extended_parse::CalendarParseOp::LastDay,
        )),
        "next_day" => Some(CalendarExtendedOp::Parse(
            super::calendar_extended_parse::CalendarParseOp::NextDay,
        )),
        "previous_day" => Some(CalendarExtendedOp::Parse(
            super::calendar_extended_parse::CalendarParseOp::PreviousDay,
        )),
        "from_days" => Some(CalendarExtendedOp::Parse(
            super::calendar_extended_parse::CalendarParseOp::FromDays,
        )),
        "weeks_diff" => Some(CalendarExtendedOp::WeeksDiff),
        "hours_diff" => Some(CalendarExtendedOp::HoursDiff),
        "minutes_diff" => Some(CalendarExtendedOp::MinutesDiff),
        "seconds_diff" => Some(CalendarExtendedOp::SecondsDiff),
        "timestamp" => Some(CalendarExtendedOp::Timestamp),
        _ => None,
    }
}

/// Truncation checks its unit even when the date is NULL, preserving required errors.
/// The immutable operation discriminator is not mutable instance state.
pub(super) fn effects(operation: CalendarExtendedOp) -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: if matches!(
            operation,
            CalendarExtendedOp::Trunc
                | CalendarExtendedOp::TimestampDiff
                | CalendarExtendedOp::Parse(
                    super::calendar_extended_parse::CalendarParseOp::LastDay
                        | super::calendar_extended_parse::CalendarParseOp::NextDay
                        | super::calendar_extended_parse::CalendarParseOp::PreviousDay
                )
        ) {
            FunctionIntrinsicRowError::MayRaise
        } else {
            FunctionIntrinsicRowError::NoRowError
        },
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: if matches!(
            operation,
            CalendarExtendedOp::Trunc
                | CalendarExtendedOp::DaysShift(_)
                | CalendarExtendedOp::MonthsShift(_)
                | CalendarExtendedOp::DurationShift(_)
        ) {
            FunctionNullBehavior::CalledOnNull
        } else {
            FunctionNullBehavior::Strict
        },
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

pub(super) fn definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(CalendarExtendedOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin extended calendar pure owner",
            value: error.to_string().into(),
        },
    )
}

pub(super) struct CalendarExtendedOwner {
    operation: CalendarExtendedOp,
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl CalendarExtendedOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let operation =
            operation(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
                subject: "uninstalled extended calendar",
                value: name.into(),
            })?;
        let function = format!("builtin.scalar/{name}/v1");
        let expected = effects(operation);
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().is_empty()
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin extended calendar pure declaration",
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
            operation,
            resolver: Arc::new(resolver),
            declaration,
            implementations,
        })
    }

    fn call_effects(&self, scope: CallProofScope) -> CallEffects {
        let base = effects(self.operation);
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

impl FunctionBindingResolver for CalendarExtendedOwner {
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

impl PureFunctionMetadataOwner for CalendarExtendedOwner {
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

impl FunctionEffectOwner for CalendarExtendedOwner {
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

impl PureScalarImplementation for CalendarExtendedOwner {
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
struct PreparedCalendarExtended {
    operation: CalendarExtendedOp,
    contract: Arc<ScalarCallContract>,
}
impl PreparedScalarKernel for PreparedCalendarExtended {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        // The immutable discriminator is the instance's only owned state.
        // This representational bound is not a formal Account/grant receipt.
        std::mem::size_of::<CalendarExtendedInstance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(CalendarExtendedInstance {
            operation: self.operation,
        }))
    }
}

struct CalendarExtendedInstance {
    operation: CalendarExtendedOp,
}
impl ScalarKernelInstance for CalendarExtendedInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        super::calendar_extended::evaluate_calendar_extended(self.operation, input, control)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
#[cfg(test)]
pub(super) fn owner_for_test(name: &str) -> CalendarExtendedOwner {
    let (_, signatures) = super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(candidate, _)| candidate == name)
        .unwrap();
    let (declaration, resolver) =
        super::catalogue::scalar_definition_parts(name, &signatures, FunctionKind::Scalar).unwrap();
    CalendarExtendedOwner::new(name, declaration, resolver).unwrap()
}
#[cfg(test)]
pub(super) fn prepared_for_test_with_policy(
    name: &str,
    sources: &[crate::FunctionValueType],
    policy: novarocks_type_contract::DecimalOverflowPolicy,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    prepared_for_test_with_control(name, sources, policy, crate::binding_test_control())
}
#[cfg(test)]
pub(super) fn prepared_for_test_with_control(
    name: &str,
    sources: &[crate::FunctionValueType],
    policy: novarocks_type_contract::DecimalOverflowPolicy,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    use crate::{FunctionArgument, ScopedExpressionEffects, specialize_scalar};
    use novarocks_type_contract::{
        EvaluationDemand, EvaluationRegionId, ExpressionEffectContext, ExpressionUseId,
        SemanticParameters,
    };
    let owner = owner_for_test(name);
    let arguments: Vec<_> = sources
        .iter()
        .cloned()
        .map(|value_type| FunctionArgument::Value {
            value_type,
            constant: None,
        })
        .collect();
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: arguments.len(),
    };
    let selected = Arc::new(owner.resolve(request, control)?);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        region: EvaluationRegionId::new(7),
        demand: EvaluationDemand::Value,
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses: Vec<_> = (0..arguments.len())
        .map(|i| Some(ExpressionUseId::new(42 + i as u32)))
        .collect();
    let input = CallEffectInput {
        context,
        argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected: &selected,
        request,
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: policy,
        proof_scope: CallProofScope::Unconditional,
    };
    specialize_scalar(
        &owner,
        input,
        selected.clone(),
        ScopedExpressionEffects::pure_value(context),
        control,
    )
    .map(|specialization| specialization.into_prepared())
}

impl CalendarExtendedOwner {
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
            if !input.environment.is_empty()
                || !matches!(input.proof_scope, CallProofScope::Unconditional)
                    && input.proof_scope != CallProofScope::Region(input.context.region)
            {
                return Err(FunctionBindingError::InvalidBinding(
                    "extended calendar has no environment dependencies and requires an exact proof scope"
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
                || contract.effects() != &self.call_effects(input.proof_scope)
                || !input.environment.is_empty()
            {
                return Err(invalid(
                    "extended calendar preparation differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("extended calendar preparation has a stale selected binding"),
            })?;
            if self.operation == CalendarExtendedOp::UnixTimestamp
                && contract.selected().argument_types.is_empty()
            {
                return Err(invalid(&format!(
                    "{} rejects the zero-argument clock form: an explicit statement-time contract is required",
                    self.declaration.function_id().as_str()
                )));
            }
            if matches!(self.operation, CalendarExtendedOp::MonthsShift(_))
                && *contract.result_type().data_type() == arrow_schema::DataType::Date32
            {
                return Err(invalid(&format!(
                    "{} rejects declared Date32 result: legacy month shift returns Timestamp(Microsecond, None)",
                    self.declaration.function_id().as_str()
                )));
            }
            if matches!(self.operation, CalendarExtendedOp::DurationShift(_))
                && *contract.result_type().data_type() == arrow_schema::DataType::Date32
            {
                return Err(invalid(&format!(
                    "{} rejects declared Date32 result: legacy duration shift returns Timestamp(Microsecond, None)",
                    self.declaration.function_id().as_str()
                )));
            }
            // The prepared object retains the same canonical contract. Its body is
            // a static pure implementation and needs no live resolver or authority.
            Ok(Arc::new(PreparedCalendarExtended {
                contract,
                operation: self.operation,
            }) as Arc<dyn PreparedScalarKernel>)
        })();
        if result.as_ref().is_err_and(KernelFailure::is_interruption) {
            return result;
        }
        work.finish().map_err(compile_failure)?;
        result
    }
}
