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

use std::sync::Arc;

use arrow_array::{Array, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CompilePhase, FunctionArgumentTypes, FunctionEffectDeclaration,
    FunctionInstanceState, FunctionNullBehavior, ObservableEffects, PICOS_PER_DAY,
    PICOS_PER_SECOND, PureCompileControl, SemanticParameterKey as Key, SemanticParameterRef,
    SemanticParameterValue, SemanticParameters, StatementStartUtc, TemporalMicros, TemporalType,
    TemporalValue, TimeZoneKey, ValueLogicalType,
};
use novarocks_value_arrays::decode::PreparedTemporalArray;

use crate::kernel_control::invalid;
use crate::kernel_input::{EvaluationCheckpoints, validate_argument_observed};
use crate::temporal_cast::Type;
use crate::temporal_value::{round_time, round_timestamp};
use crate::temporal_zone_rules::{TemporalZoneSource, offset_seconds, utc_to_local};
use crate::{
    CallEffectInput, CheckedCallEffectInput, CheckedOwnerFailure, FunctionArgument,
    FunctionArgumentType, FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingResolverOwner, FunctionBindingSelection,
    FunctionCatalogError, FunctionDefinition, FunctionEffectOwner, FunctionEffectOwnerError,
    FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError, FunctionKind,
    FunctionOverloadDeclaration, FunctionOverloadId, FunctionResultType, FunctionValueType,
    FunctionVisibility, FunctionVolatility, KernelEvaluationControl, KernelFailure,
    PreparedScalarKernel, PureFunctionMetadataOwner, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, PureScalarImplementation, RowDataError,
    ScalarCallContract, ScalarCallInput, ScalarKernelInstance, SelectedValues,
};

use super::{binding_control, temporal_output::TemporalOutputBuilder};

pub const FUNCTION_NAME: &str = "__temporal_statement_syntax";
pub const FUNCTION_ID: &str = "builtin.scalar/temporal_statement_syntax/v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemporalSyntax {
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    LocalTime,
    LocalTimestamp,
    AtTimestampName,
    AtTimestampInterval,
    AtTimestampLocal,
    AtTimeName,
    AtTimeInterval,
    AtTimeLocal,
}

const SYNTAX: [TemporalSyntax; 11] = {
    use TemporalSyntax::*;
    [
        CurrentDate,
        CurrentTime,
        CurrentTimestamp,
        LocalTime,
        LocalTimestamp,
        AtTimestampName,
        AtTimestampInterval,
        AtTimestampLocal,
        AtTimeName,
        AtTimeInterval,
        AtTimeLocal,
    ]
};

impl TemporalSyntax {
    pub fn identity(self) -> &'static str {
        use TemporalSyntax::*;
        match self {
            CurrentDate => "builtin.scalar/temporal_statement_syntax/current_date/v1",
            CurrentTime => "builtin.scalar/temporal_statement_syntax/current_time/v1",
            CurrentTimestamp => "builtin.scalar/temporal_statement_syntax/current_timestamp/v1",
            LocalTime => "builtin.scalar/temporal_statement_syntax/local_time/v1",
            LocalTimestamp => "builtin.scalar/temporal_statement_syntax/local_timestamp/v1",
            AtTimestampName => "builtin.scalar/temporal_statement_syntax/at_timestamp_name/v1",
            AtTimestampInterval => {
                "builtin.scalar/temporal_statement_syntax/at_timestamp_interval/v1"
            }
            AtTimestampLocal => "builtin.scalar/temporal_statement_syntax/at_timestamp_local/v1",
            AtTimeName => "builtin.scalar/temporal_statement_syntax/at_time_name/v1",
            AtTimeInterval => "builtin.scalar/temporal_statement_syntax/at_time_interval/v1",
            AtTimeLocal => "builtin.scalar/temporal_statement_syntax/at_time_local/v1",
        }
    }

    pub fn dependencies(self) -> &'static [Key] {
        use TemporalSyntax::*;
        match self {
            AtTimestampName | AtTimestampInterval | AtTimeInterval => &[],
            AtTimestampLocal => &[Key::TimeZone],
            AtTimeName => &[Key::StatementStartUtc],
            _ => &[Key::StatementStartUtc, Key::TimeZone],
        }
    }

    fn argument_pattern(self) -> &'static str {
        use TemporalSyntax::*;
        match self {
            CurrentDate => "() with explicit DATE result",
            CurrentTime => "() with explicit TIME WITH TIME ZONE result",
            CurrentTimestamp => "() with explicit TIMESTAMP WITH TIME ZONE result",
            LocalTime => "() with explicit TIME result",
            LocalTimestamp => "() with explicit TIMESTAMP result",
            AtTimestampName => "(TIMESTAMP WITH TIME ZONE, VARCHAR)",
            AtTimestampInterval => "(TIMESTAMP WITH TIME ZONE, day-time INTERVAL)",
            AtTimestampLocal => "(TIMESTAMP WITH TIME ZONE) with effective session zone",
            AtTimeName => "(TIME WITH TIME ZONE, VARCHAR)",
            AtTimeInterval => "(TIME WITH TIME ZONE, day-time INTERVAL)",
            AtTimeLocal => "(TIME WITH TIME ZONE) with effective session zone",
        }
    }

    fn argument_count(self) -> usize {
        use TemporalSyntax::*;
        match self {
            CurrentDate | CurrentTime | CurrentTimestamp | LocalTime | LocalTimestamp => 0,
            AtTimestampLocal | AtTimeLocal => 1,
            _ => 2,
        }
    }
    fn effects(self) -> FunctionEffectDeclaration {
        FunctionEffectDeclaration {
            value_stability: FunctionVolatility::Immutable,
            own_row_error: FunctionIntrinsicRowError::MayRaise,
            failure_behavior: FunctionFailureBehavior::Propagate,
            null_behavior: FunctionNullBehavior::Strict,
            // Even the clock forms have an eager signature with zero actual
            // arguments. NoArguments belongs to the invocation ABI that can
            // suppress the runtime channels of a different logical call.
            argument_control: ArgumentControl::Eager,
            instance_state: FunctionInstanceState::None,
            observable_effects: ObservableEffects::NONE,
            environment_dependencies: self.dependencies().into(),
        }
    }

    fn from_overload(overload: &FunctionOverloadId) -> Result<Self, FunctionBindingError> {
        SYNTAX
            .into_iter()
            .find(|operation| operation.identity() == overload.as_str())
            .ok_or_else(|| FunctionBindingError::UnknownOverload(overload.clone()))
    }

    fn accepts(self, arguments: &[FunctionArgument], result: &FunctionValueType) -> bool {
        use TemporalSyntax::*;
        let ty = result.value_type().temporal();
        if arguments.len() != self.argument_count() {
            return false;
        }
        if arguments.is_empty() {
            return !result.nullable()
                && match self {
                    CurrentDate => ty.is_none() && result.data_type() == &DataType::Date32,
                    CurrentTime => matches!(ty, Some(TemporalType::TimeWithTimeZone(_))),
                    CurrentTimestamp => matches!(ty, Some(TemporalType::TimestampWithTimeZone(_))),
                    LocalTime => matches!(ty, Some(TemporalType::Time(_))),
                    LocalTimestamp => matches!(ty, Some(TemporalType::Timestamp(_))),
                    _ => false,
                };
        }
        let projected = match &arguments[0] {
            FunctionArgument::Value { value_type, .. } => value_type.value_type().temporal(),
            _ => return false,
        };
        let category_correct = match self {
            AtTimeName | AtTimeInterval | AtTimeLocal => {
                matches!(ty, Some(TemporalType::TimeWithTimeZone(_)))
            }
            _ => matches!(ty, Some(TemporalType::TimestampWithTimeZone(_))),
        };
        let expected_nullable = arguments.iter().any(|argument| match argument {
            FunctionArgument::Value { value_type, .. } => value_type.nullable(),
            _ => true,
        });
        if projected != ty || !category_correct || result.nullable() != expected_nullable {
            return false;
        }
        match arguments.get(1) {
            None => true,
            Some(FunctionArgument::Value { value_type, .. }) => match self {
                AtTimestampName | AtTimeName => {
                    value_type.logical_type() == ValueLogicalType::Physical
                        && value_type.data_type() == &DataType::Utf8
                }
                _ => {
                    matches!(value_type.value_type().temporal(), Some(TemporalType::Interval(qualifier)) if !qualifier.is_year_month())
                }
            },
            _ => false,
        }
    }

    fn capture(
        self,
        references: &[SemanticParameterRef],
        parameters: &SemanticParameters,
    ) -> Option<FrozenFacts> {
        if self.dependencies().len() != references.len() {
            return None;
        }
        let mut facts = FrozenFacts::default();
        for &key in self.dependencies() {
            let reference = references
                .iter()
                .find(|reference| reference.expected_key == key)?;
            match parameters.require(*reference).ok()? {
                SemanticParameterValue::TimeZone(zone) => facts.zone = Some(*zone),
                SemanticParameterValue::StatementStartUtc(start) => facts.start = Some(*start),
                _ => return None,
            }
        }
        Some(facts)
    }
}

pub(crate) fn definition() -> Result<FunctionDefinition, FunctionCatalogError> {
    fn invalid_catalog(error: impl std::fmt::Display) -> FunctionCatalogError {
        FunctionCatalogError::InvalidStableIdentity {
            subject: "temporal statement syntax owner",
            value: error.to_string().into(),
        }
    }
    let mut overloads = Vec::new();
    let mut implementations = Vec::new();
    for syntax in SYNTAX {
        let overload = FunctionOverloadId::try_new(syntax.identity()).map_err(invalid_catalog)?;
        overloads.push(FunctionOverloadDeclaration::from_effects(
            overload.clone(),
            syntax.argument_pattern(),
            "exact temporal result",
            None,
            syntax.effects(),
        ));
        implementations.push(PureImplementationDeclaration {
            overload,
            implementation: PureImplementationId::try_new(syntax.identity())?,
            abi: PureKernelAbi::ScalarV1,
        });
    }
    let declaration = FunctionBindingDeclaration::try_new_complete(
        FunctionId::try_new(FUNCTION_ID).map_err(invalid_catalog)?,
        FunctionKind::Scalar,
        overloads,
    )
    .map_err(invalid_catalog)?;
    FunctionDefinition::try_new_pure_scalar(
        FUNCTION_NAME,
        FunctionVisibility::Hidden,
        Arc::new(ContextOwner {
            declaration,
            implementations: implementations.into_boxed_slice(),
            resolver: Arc::new(TemporalContextResolver),
        }),
    )
    .map_err(invalid_catalog)
}

pub(crate) struct TemporalContextResolver;

impl FunctionBindingResolver for TemporalContextResolver {
    fn resolve(
        &self,
        _: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
        Err(FunctionBindingError::NoMatchingOverload)
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            binding_control::request_types(request, work)?;
            let syntax = TemporalSyntax::from_overload(overload)?;
            let result = request
                .expected_result_type
                .ok_or(FunctionBindingError::NoMatchingOverload)?;
            if !syntax.accepts(request.arguments, result)
                || request.logical_argument_count != request.arguments.len()
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let argument_types = request
                .arguments
                .iter()
                .map(|argument| match argument {
                    FunctionArgument::Value { value_type, .. } => {
                        FunctionArgumentType::Value(value_type.clone())
                    }
                    _ => unreachable!("temporal syntax accepts only eager scalar values"),
                })
                .collect::<Vec<_>>();
            Ok(FunctionBindingSelection {
                overload: overload.clone(),
                argument_types: FunctionArgumentTypes::from_pure_boxed(
                    argument_types.into_boxed_slice(),
                ),
                result_type: FunctionResultType::Scalar(result.clone()),
                aggregate: None,
            })
        })
    }

    fn validate_selected(
        &self,
        selection: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            binding_control::request_types(request, work)?;
            let syntax = TemporalSyntax::from_overload(&selection.overload)?;
            let FunctionResultType::Scalar(result) = &selection.result_type else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            let requested = request
                .expected_result_type
                .ok_or(FunctionBindingError::NoMatchingOverload)?;
            if selection.aggregate.is_some()
                || !syntax.accepts(request.arguments, result)
                || request.logical_argument_count != request.arguments.len()
                || selection.argument_types.len() != request.arguments.len()
                || !binding_control::exact_type(result, requested, work)?
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            for (selected, argument) in selection.argument_types.iter().zip(request.arguments) {
                let (
                    FunctionArgumentType::Value(selected),
                    FunctionArgument::Value { value_type, .. },
                ) = (selected, argument)
                else {
                    return Err(FunctionBindingError::NoMatchingOverload);
                };
                if !binding_control::exact_type(selected, value_type, work)? {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
            }
            Ok(())
        })
    }
}

struct ContextOwner {
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
    resolver: Arc<TemporalContextResolver>,
}

impl FunctionBindingResolver for ContextOwner {
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

impl PureFunctionMetadataOwner for ContextOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> FunctionBindingResolverOwner {
        FunctionBindingResolverOwner::borrowed_temporal_context(Arc::clone(&self.resolver))
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
        environment: &[SemanticParameterRef],
        parameters: &SemanticParameters,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
        let syntax = TemporalSyntax::from_overload(&selected.overload)?;
        syntax
            .capture(environment, parameters)
            .map(|_| ())
            .ok_or_else(|| {
                FunctionBindingError::UnavailableImplementation(selected.overload.clone())
            })
    }
}

impl ContextOwner {
    fn selected_effects(
        &self,
        input: CallEffectInput<'_>,
    ) -> Result<CallEffects, FunctionBindingError> {
        if input.function_id != self.declaration.function_id() || input.kind != FunctionKind::Scalar
        {
            return Err(FunctionBindingError::UnknownFunction);
        }
        let syntax = TemporalSyntax::from_overload(&input.selected.overload)?;
        syntax
            .capture(input.environment, input.parameters)
            .ok_or_else(|| {
                FunctionBindingError::UnavailableImplementation(input.selected.overload.clone())
            })?;
        let base = syntax.effects();
        Ok(CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: input.environment.into(),
            proof_scope: input.proof_scope,
        })
    }

    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        let effects = self
            .selected_effects(input)
            .map_err(|_| invalid("temporal syntax has invalid selected effects"))?;
        if input.function_id != contract.function_id()
            || !std::ptr::eq(input.selected, contract.selected())
            || novarocks_type_contract::CallEffectContext::from(contract.context()) != input.context
            || input.decimal_overflow_policy != contract.decimal_overflow_policy()
            || &effects != contract.effects()
        {
            return Err(invalid(
                "temporal syntax differs from its checked scalar contract",
            ));
        }
        let syntax = TemporalSyntax::from_overload(&input.selected.overload)
            .map_err(|_| invalid("unknown temporal syntax overload"))?;
        let facts = syntax
            .capture(input.environment, input.parameters)
            .ok_or_else(|| invalid("temporal syntax has missing environment"))?;
        let recipe = ContextRecipe { syntax, facts };
        Ok(Arc::new(Prepared { contract, recipe }))
    }
}

impl FunctionEffectOwner for ContextOwner {
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
        self.resolver
            .validate_selected(input.selected, input.request, control)?;
        self.selected_effects(input).map_err(Into::into)
    }

    fn validate_and_refine_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &CheckedCallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, CheckedOwnerFailure<FunctionEffectOwnerError<Self::Error>>> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(|cause| {
                CheckedOwnerFailure::Original(FunctionEffectOwnerError::Control(cause))
            })?;
        checked
            .require_binding_author(self.resolver.as_ref(), input)
            .map_err(|cause| CheckedOwnerFailure::Original(cause.into()))?;
        self.selected_effects(input)
            .map_err(|cause| CheckedOwnerFailure::Original(cause.into()))
    }
}

impl PureScalarImplementation for ContextOwner {
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        self.resolver
            .validate_selected(input.selected, input.request, control)
            .map_err(binding_failure)?;
        self.prepare(input, contract)
    }
    fn prepare_scalar_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &CheckedCallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, CheckedOwnerFailure<KernelFailure>> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(|cause| {
                CheckedOwnerFailure::Original(crate::kernel_control::compile_failure(cause))
            })?;
        checked
            .require_binding_author(self.resolver.as_ref(), input)
            .map_err(|cause| CheckedOwnerFailure::Original(binding_failure(cause)))?;
        self.prepare(input, contract)
            .map_err(CheckedOwnerFailure::Original)
    }
}

fn binding_failure(error: FunctionBindingError) -> KernelFailure {
    match error {
        FunctionBindingError::Control(cause) => crate::kernel_control::compile_failure(cause),
        _ => invalid("temporal syntax has a stale binding"),
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct FrozenFacts {
    start: Option<StatementStartUtc>,
    zone: Option<TimeZoneKey>,
}

impl FrozenFacts {
    fn instant(self) -> Result<TemporalMicros, RowFailure> {
        let start = self.start.ok_or(RowFailure::Range)?;
        TemporalMicros::from_picos(
            i128::from(start.seconds()) * i128::from(PICOS_PER_SECOND)
                + i128::from(start.nanos()) * 1_000,
        )
        .ok_or(RowFailure::Range)
    }
}

#[derive(Clone, Copy, Debug)]
struct ContextRecipe {
    syntax: TemporalSyntax,
    facts: FrozenFacts,
}

#[derive(Debug)]
struct Prepared {
    contract: Arc<ScalarCallContract>,
    recipe: ContextRecipe,
}
impl PreparedScalarKernel for Prepared {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }
    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<ContextRecipe>()
    }
    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(self.recipe))
    }
}

impl ScalarKernelInstance for ContextRecipe {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        let mut work = EvaluationCheckpoints::new(control);
        let result = self.evaluate_rows(input, control, &mut work);
        work.finish_result(result)
    }
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl ContextRecipe {
    fn evaluate_rows<'a>(
        self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        let target = input.contract().result_type();
        let declared = target.value_type().temporal();
        let selection = input.selection();
        let bound = input.contract().value_argument_types();
        if !input.arguments().is_empty() {
            for (argument, ty) in input.arguments().iter().zip(bound) {
                work.flush()?;
                validate_argument_observed(*argument, selection, ty, control)?;
            }
        }
        let source = input
            .arguments()
            .first()
            .map(|argument| {
                let ty = input
                    .contract()
                    .value_argument_types()
                    .next()
                    .ok_or_else(|| invalid("time zone operand has no selected type"))?;
                let declaration = ty
                    .value_type()
                    .temporal()
                    .ok_or_else(|| invalid("time zone operand has no temporal declaration"))?;
                PreparedTemporalArray::bind(declaration, argument.array().as_ref(), ty.nullable())
                    .map_err(|_| invalid("invalid frozen temporal operand"))
            })
            .transpose()?;
        let zone_argument = input
            .arguments()
            .get(1)
            .map(|argument| {
                let ty = input
                    .contract()
                    .value_argument_types()
                    .nth(1)
                    .ok_or_else(|| invalid("time zone argument has no selected type"))?;
                match ty.value_type().temporal() {
                    Some(declaration) => PreparedTemporalArray::bind(
                        declaration,
                        argument.array().as_ref(),
                        ty.nullable(),
                    )
                    .map(ZoneArgument::Interval)
                    .map_err(|_| invalid("invalid frozen time zone interval")),
                    None => argument
                        .array()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(ZoneArgument::Text)
                        .ok_or_else(|| invalid("invalid frozen time zone text")),
                }
            })
            .transpose()?;
        let current = source.is_none().then(|| self.current(declared));
        work.flush()?;
        let mut output = TemporalOutputBuilder::new(
            declared.map_or(Type::Date, Type::Temporal),
            selection.len(),
        )?;
        let mut errors = Vec::new();
        for (ordinal, logical) in selection.iter().enumerate() {
            work.step()?;
            let result = if let Some(current) = current {
                current
            } else {
                let row = input.arguments()[0].value_row(ordinal, logical);
                let value = source
                    .as_ref()
                    .expect("validated temporal operand")
                    .value(row)
                    .map_err(|_| invalid("invalid reachable temporal operand"))?;
                let Some(value) = value else {
                    output.null();
                    continue;
                };
                let zone = match &zone_argument {
                    None => Ok(self.facts.zone),
                    Some(argument) => {
                        let row = input.arguments()[1].value_row(ordinal, logical);
                        argument.zone(row, work)?
                    }
                };
                match zone {
                    Ok(Some(zone)) => self.at_zone(value, zone),
                    Ok(None) => {
                        output.null();
                        continue;
                    }
                    Err(cause) => Err(cause),
                }
            };
            match result {
                Ok(ResultValue::Date(value)) => output.date(value)?,
                Ok(ResultValue::Temporal(value)) => {
                    let ty = declared
                        .ok_or_else(|| invalid("temporal syntax result type is missing"))?;
                    if let Ok(value) = value.validate(ty) {
                        output.temporal(ty, value)?;
                    } else {
                        append_error(&mut output, &mut errors, ordinal, RowFailure::Range, work)?;
                    }
                }
                Err(cause) => append_error(&mut output, &mut errors, ordinal, cause, work)?,
            }
        }
        work.flush()?;
        let array = output.finish();
        SelectedValues::try_new_observed(
            selection,
            target.data_type(),
            array,
            errors.into_boxed_slice(),
            || work.step(),
        )
    }

    fn current(self, target: Option<TemporalType>) -> Result<ResultValue, RowFailure> {
        use TemporalSyntax::*;
        let instant = self.facts.instant()?;
        let zone = self.facts.zone.ok_or(RowFailure::Zone)?;
        let local =
            || utc_to_local(zone, TemporalZoneSource::Jdk, instant).map_err(|_| RowFailure::Range);
        if self.syntax == CurrentDate {
            let days = local()?.micros().div_euclid(86_400 * 1_000_000);
            return i32::try_from(days)
                .map(ResultValue::Date)
                .map_err(|_| RowFailure::Range);
        }
        let precision = target.ok_or(RowFailure::Range)?.precision();
        let rounded_clock = || {
            let picos = local()?.as_picos().rem_euclid(i128::from(PICOS_PER_DAY)) as i64;
            round_time(picos, precision).map_err(|_| RowFailure::Range)
        };
        let value = match self.syntax {
            CurrentTime => TemporalValue::TimeWithTimeZone {
                picos: rounded_clock()?,
                offset_minutes: offset_minutes(zone, instant)?,
            },
            CurrentTimestamp => TemporalValue::TimestampWithTimeZone {
                instant: round_timestamp(instant, precision).map_err(|_| RowFailure::Range)?,
                zone,
            },
            LocalTime => TemporalValue::Time(rounded_clock()?),
            LocalTimestamp => TemporalValue::Timestamp(
                round_timestamp(local()?, precision).map_err(|_| RowFailure::Range)?,
            ),
            _ => return Err(RowFailure::Range),
        };
        Ok(ResultValue::Temporal(value))
    }

    fn at_zone(self, value: TemporalValue, zone: TimeZoneKey) -> Result<ResultValue, RowFailure> {
        let value = match value {
            TemporalValue::TimestampWithTimeZone { instant, .. } => {
                TemporalValue::TimestampWithTimeZone { instant, zone }
            }
            TemporalValue::TimeWithTimeZone {
                picos,
                offset_minutes: old,
            } => {
                let offset_minutes = match zone.fixed_offset_minutes() {
                    Some(minutes) => minutes,
                    None => offset_minutes(zone, self.facts.instant()?)?,
                };
                let correction = i64::from(offset_minutes - old) * 60 * PICOS_PER_SECOND;
                TemporalValue::TimeWithTimeZone {
                    picos: (picos + correction).rem_euclid(PICOS_PER_DAY),
                    offset_minutes,
                }
            }
            _ => return Err(RowFailure::Range),
        };
        Ok(ResultValue::Temporal(value))
    }
}

fn offset_minutes(zone: TimeZoneKey, instant: TemporalMicros) -> Result<i16, RowFailure> {
    let seconds = offset_seconds(zone, TemporalZoneSource::Jdk, instant.floor_millis())
        .map_err(|_| RowFailure::Range)?;
    i16::try_from(seconds / 60).map_err(|_| RowFailure::Zone)
}

#[derive(Clone, Copy, Debug)]
enum ResultValue {
    Date(i32),
    Temporal(TemporalValue),
}
enum ZoneArgument<'a> {
    Text(&'a StringArray),
    Interval(PreparedTemporalArray<'a>),
}
impl ZoneArgument<'_> {
    fn zone(
        &self,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Result<Option<TimeZoneKey>, RowFailure>, KernelFailure> {
        match self {
            Self::Text(text) => {
                if text.is_null(row) {
                    return Ok(Ok(None));
                }
                let text = text.value(row);
                for _ in text.as_bytes().chunks(32) {
                    work.step()?;
                }
                Ok(TimeZoneKey::parse(text).map(Some).ok_or(RowFailure::Zone))
            }
            Self::Interval(value) => {
                let value = value
                    .value(row)
                    .map_err(|_| invalid("invalid reachable time zone interval"))?;
                Ok(match value {
                    None => Ok(None),
                    Some(TemporalValue::IntervalDayTime(value)) => {
                        let picos = value.as_picos();
                        let minute = i128::from(60 * PICOS_PER_SECOND);
                        if picos % minute != 0 {
                            Err(RowFailure::Minute)
                        } else {
                            i16::try_from(picos / minute)
                                .ok()
                                .and_then(TimeZoneKey::from_offset_minutes)
                                .map(Some)
                                .ok_or(RowFailure::Zone)
                        }
                    }
                    _ => return Err(invalid("time zone interval is from a different family")),
                })
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum RowFailure {
    Zone,
    Minute,
    Range,
}
impl std::fmt::Display for RowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Zone => "Invalid time zone",
            Self::Minute => "Time zone offset must contain whole minutes",
            Self::Range => "Temporal value is outside the supported range",
        })
    }
}
impl std::error::Error for RowFailure {}
impl novarocks_failure_contract::FailureProjection for RowFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        use novarocks_function_contract::kernel_failure::{
            INVALID_FUNCTION_ARGUMENT, NUMERIC_OUT_OF_RANGE,
        };
        FailureFact::new(
            FailureClass::Data,
            match self {
                Self::Zone | Self::Minute => INVALID_FUNCTION_ARGUMENT,
                Self::Range => NUMERIC_OUT_OF_RANGE,
            },
        )
    }
}

fn append_error(
    output: &mut TemporalOutputBuilder,
    errors: &mut Vec<RowDataError>,
    ordinal: usize,
    cause: RowFailure,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    work.flush()?;
    errors
        .try_reserve(1)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    errors.push(RowDataError::from_domain(ordinal, cause)?);
    output.null();
    work.flush()
}
