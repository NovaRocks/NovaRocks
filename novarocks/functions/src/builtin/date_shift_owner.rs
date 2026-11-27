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

use arrow_array::{Array, Date32Array, Int64Array, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffectContext, CallEffects, CallProofScope, CharacterKind, CharacterType,
    CompilePhase, FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior,
    ObservableEffects, PureCompileControl, TemporalType, ValueKind, ValueLayout, ValueTypeRef,
    ValueTypeStorageLifetime, trino_can_coerce_scalar, trino_common_scalar_kind,
};
use novarocks_value_arrays::decode::PreparedTemporalArray;

use super::{
    binding_control,
    date_field::{Field, IntervalExtraction},
    date_pattern,
    date_shift::{ShiftError, Unit, add, difference, last_day, truncate},
    temporal_output::TemporalOutputBuilder,
};
use crate::{
    CallEffectInput, CheckedCallEffectInput, CheckedOwnerFailure, ConstantError, FunctionArgument,
    FunctionArgumentType, FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingResolverOwner, FunctionBindingSelection,
    FunctionCatalogError, FunctionDefinition, FunctionEffectOwner, FunctionEffectOwnerError,
    FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError, FunctionKind,
    FunctionOverloadDeclaration, FunctionOverloadId, FunctionResultType, FunctionValueType,
    FunctionVisibility, FunctionVolatility, KernelEvaluationControl, KernelFailure,
    PreparedScalarKernel, PureFunctionMetadataOwner, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, PureScalarImplementation, RowDataError,
    ScalarCallContract, ScalarCallInput, ScalarKernelInstance, SelectedValues,
    kernel_control::{compile_failure, invalid},
    kernel_input::{EvaluationCheckpoints, validate_argument_observed},
    temporal_arithmetic::Value,
    temporal_cast::{self, Type},
};

const FAMILIES: [&str; 5] = [
    "date",
    "time",
    "time_with_time_zone",
    "timestamp",
    "timestamp_with_time_zone",
];
const INTERVAL_YEAR_MONTH: &str = "interval_year_month";
const INTERVAL_DAY_TIME: &str = "interval_day_time";
const INVALID_SELECTION: &str = "Calendar call differs from its selected argument or result type";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CalendarFunction {
    Add,
    Difference,
    Truncate,
    LastDay,
    DateProjection,
    Format,
    Parse,
    Field(&'static str, Field),
}

pub(super) const FUNCTIONS: &[CalendarFunction] = &[
    CalendarFunction::Field("year", Field::Year),
    CalendarFunction::Field("quarter", Field::Quarter),
    CalendarFunction::Field("month", Field::Month),
    CalendarFunction::Field("day", Field::Day),
    CalendarFunction::Field("day_of_month", Field::Day),
    CalendarFunction::Field("day_of_week", Field::DayOfWeek),
    CalendarFunction::Field("dow", Field::DayOfWeek),
    CalendarFunction::Field("day_of_year", Field::DayOfYear),
    CalendarFunction::Field("doy", Field::DayOfYear),
    CalendarFunction::Field("week", Field::Week),
    CalendarFunction::Field("week_of_year", Field::Week),
    CalendarFunction::Field("hour", Field::Hour),
    CalendarFunction::Field("minute", Field::Minute),
    CalendarFunction::Field("second", Field::Second),
    CalendarFunction::Field("__extract_year_of_week", Field::YearOfWeek),
    CalendarFunction::Field("__extract_timezone_hour", Field::TimeZoneHour),
    CalendarFunction::Field("__extract_timezone_minute", Field::TimeZoneMinute),
    CalendarFunction::Add,
    CalendarFunction::Difference,
    CalendarFunction::Truncate,
    CalendarFunction::LastDay,
    CalendarFunction::DateProjection,
    CalendarFunction::Format,
    CalendarFunction::Parse,
];

impl CalendarFunction {
    fn name(self) -> &'static str {
        match self {
            Self::Add => "date_add",
            Self::Difference => "date_diff",
            Self::Truncate => "date_trunc",
            Self::LastDay => "last_day_of_month",
            Self::DateProjection => "date",
            Self::Format => "date_format",
            Self::Parse => "date_parse",
            Self::Field(name, _) => name,
        }
    }

    fn overloads(self) -> impl Iterator<Item = &'static str> {
        let families: &'static [&'static str] = match self {
            Self::Field(_, Field::Hour | Field::Minute | Field::Second) => &FAMILIES[1..],
            Self::Field(_, Field::TimeZoneHour | Field::TimeZoneMinute) => {
                &["time_with_time_zone", "timestamp_with_time_zone"]
            }
            Self::LastDay | Self::Field(_, _) => &["date", "timestamp", "timestamp_with_time_zone"],
            Self::DateProjection => &["varchar", "timestamp", "timestamp_with_time_zone"],
            Self::Format => &["timestamp", "timestamp_with_time_zone"],
            Self::Parse => &["varchar"],
            _ => &FAMILIES,
        };
        families.iter().copied().chain(self.interval_family())
    }

    fn interval_family(self) -> Option<&'static str> {
        match self {
            Self::Field(_, Field::Year | Field::Month) => Some(INTERVAL_YEAR_MONTH),
            Self::Field(_, Field::Day | Field::Hour | Field::Minute | Field::Second) => {
                Some(INTERVAL_DAY_TIME)
            }
            _ => None,
        }
    }

    fn family(self, value: &ValueKind) -> Result<&'static str, FunctionBindingError> {
        match value {
            ValueKind::Temporal(TemporalType::Interval(qualifier)) => {
                Ok(if qualifier.is_year_month() {
                    INTERVAL_YEAR_MONTH
                } else {
                    INTERVAL_DAY_TIME
                })
            }
            ValueKind::String | ValueKind::BoundedCharacter(_)
                if matches!(self, Self::DateProjection | Self::Parse) =>
            {
                Ok("varchar")
            }
            _ => family(value),
        }
    }

    fn temporal_argument_index(self) -> usize {
        match self {
            Self::Add => 2,
            Self::Difference | Self::Truncate => 1,
            Self::LastDay
            | Self::DateProjection
            | Self::Format
            | Self::Parse
            | Self::Field(_, _) => 0,
        }
    }

    fn uses_unit(self) -> bool {
        matches!(self, Self::Add | Self::Difference | Self::Truncate)
    }
}

// Design: ADR-0256 (docs/adr/ADR-0256-temporal-calls-freeze-overloads-and-selected-row-effects.md)
pub(crate) struct DateShiftResolver {
    operation: CalendarFunction,
    overloads: Vec<FunctionOverloadId>,
}

fn family(kind: &ValueKind) -> Result<&'static str, FunctionBindingError> {
    match kind {
        ValueKind::Scalar(DataType::Date32) => Ok(FAMILIES[0]),
        ValueKind::Temporal(TemporalType::Time(_)) => Ok(FAMILIES[1]),
        ValueKind::Temporal(TemporalType::TimeWithTimeZone(_)) => Ok(FAMILIES[2]),
        ValueKind::Temporal(TemporalType::Timestamp(_)) => Ok(FAMILIES[3]),
        ValueKind::Temporal(TemporalType::TimestampWithTimeZone(_)) => Ok(FAMILIES[4]),
        _ => Err(FunctionBindingError::NoMatchingOverload),
    }
}

pub(super) fn temporal_argument(
    original: &FunctionValueType,
    kind: &ValueKind,
) -> Result<FunctionValueType, FunctionBindingError> {
    if original.value_type().kind() == kind {
        return Ok(original.clone());
    }
    if *kind == ValueKind::Scalar(DataType::Date32) {
        return Ok(FunctionValueType::new(
            DataType::Date32,
            original.nullable(),
        ));
    }
    let ValueKind::Temporal(temporal) = kind else {
        return Err(FunctionBindingError::NoMatchingOverload);
    };
    let storage = ValueTypeStorageLifetime::pure();
    let value = ValueTypeRef::try_new(kind.clone(), original.nullable(), storage.clone())
        .map_err(binding_error)?;
    let layout = ValueLayout::try_new(&temporal.carrier(), storage).map_err(binding_error)?;
    FunctionValueType::try_from_value(value, layout).map_err(binding_error)
}

fn is_varchar(ty: &FunctionValueType) -> bool {
    ty.data_type() == &DataType::Utf8
        && match ty.value_type().kind() {
            ValueKind::String => true,
            ValueKind::BoundedCharacter(character) => character.kind() == CharacterKind::Varchar,
            _ => false,
        }
}

pub(super) fn varchar_argument(
    ty: &FunctionValueType,
) -> Result<FunctionValueType, FunctionBindingError> {
    if is_varchar(ty) {
        return Ok(ty.clone());
    }
    let target = match ty.value_type().kind() {
        ValueKind::BoundedCharacter(character) if character.kind() == CharacterKind::Char => {
            let target = CharacterType::new(CharacterKind::Varchar, character.length())
                .ok_or(FunctionBindingError::NoMatchingOverload)?;
            ValueKind::BoundedCharacter(target)
        }
        ValueKind::String | ValueKind::Scalar(DataType::Null) => ValueKind::String,
        _ => return Err(FunctionBindingError::NoMatchingOverload),
    };
    let storage = ValueTypeStorageLifetime::pure();
    let value =
        ValueTypeRef::try_new(target, ty.nullable(), storage.clone()).map_err(binding_error)?;
    let layout = ValueLayout::try_new(&DataType::Utf8, storage).map_err(binding_error)?;
    FunctionValueType::try_from_value(value, layout).map_err(binding_error)
}

fn binding_error(error: impl fmt::Display) -> FunctionBindingError {
    FunctionBindingError::InvalidBinding(error.to_string().into())
}

use std::fmt;

impl DateShiftResolver {
    fn selection(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        if request.logical_argument_count != request.arguments.len() {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        let original = request
            .arguments
            .iter()
            .map(|arg| match arg {
                FunctionArgument::Value { value_type, .. } => Ok(value_type),
                _ => Err(FunctionBindingError::NoMatchingOverload),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let nullable = original.iter().any(|arg| arg.nullable());
        let (stamp, arguments, result) = match (self.operation, original.as_slice()) {
            (CalendarFunction::Add, [unit, offset, stamp]) => {
                if !trino_can_coerce_scalar(
                    offset.value_type().kind(),
                    &ValueKind::Scalar(DataType::Int64),
                ) {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
                if stamp.data_type() == &DataType::Null {
                    return Err(FunctionBindingError::AmbiguousOverload);
                }
                let result = stamp.with_nullable(nullable).map_err(binding_error)?;
                (
                    (*stamp).clone(),
                    vec![
                        varchar_argument(unit)?,
                        FunctionValueType::new(DataType::Int64, offset.nullable()),
                        (*stamp).clone(),
                    ],
                    result,
                )
            }
            (CalendarFunction::Truncate, [unit, stamp]) => {
                if stamp.data_type() == &DataType::Null {
                    return Err(FunctionBindingError::AmbiguousOverload);
                }
                (
                    (*stamp).clone(),
                    vec![varchar_argument(unit)?, (*stamp).clone()],
                    stamp.with_nullable(nullable).map_err(binding_error)?,
                )
            }
            (CalendarFunction::Difference, [unit, first, second]) => {
                let common =
                    trino_common_scalar_kind(first.value_type().kind(), second.value_type().kind())
                        .ok_or(FunctionBindingError::NoMatchingOverload)?;
                let common = if common == ValueKind::Scalar(DataType::Null) {
                    // All signatures return BIGINT and strict NULL precludes
                    // evaluation. DATE supplies the exact common input profile.
                    ValueKind::Scalar(DataType::Date32)
                } else {
                    common
                };
                let first = temporal_argument(first, &common)?;
                let second = temporal_argument(second, &common)?;
                (
                    first.clone(),
                    vec![varchar_argument(unit)?, first, second],
                    FunctionValueType::new(DataType::Int64, nullable),
                )
            }
            (CalendarFunction::Field(_, field), [stamp]) => {
                let stamp = if stamp.data_type() == &DataType::Null {
                    let default_kind = match field {
                        Field::Hour | Field::Minute | Field::Second => {
                            ValueKind::Temporal(TemporalType::Time(
                                novarocks_type_contract::TemporalPrecision::WHOLE_SECONDS,
                            ))
                        }
                        _ => ValueKind::Scalar(DataType::Date32),
                    };
                    temporal_argument(stamp, &default_kind)?
                } else {
                    (*stamp).clone()
                };
                (
                    stamp.clone(),
                    vec![stamp],
                    FunctionValueType::new(DataType::Int64, nullable),
                )
            }
            (CalendarFunction::Format, [source, pattern]) => {
                let source = match source.value_type().kind() {
                    ValueKind::Scalar(DataType::Date32 | DataType::Null) => temporal_argument(
                        source,
                        &ValueKind::Temporal(TemporalType::Timestamp(
                            novarocks_type_contract::TemporalPrecision::WHOLE_SECONDS,
                        )),
                    )?,
                    ValueKind::Temporal(
                        TemporalType::Timestamp(_) | TemporalType::TimestampWithTimeZone(_),
                    ) => (*source).clone(),
                    _ => return Err(FunctionBindingError::NoMatchingOverload),
                };
                (
                    source.clone(),
                    vec![source, varchar_argument(pattern)?],
                    FunctionValueType::new(DataType::Utf8, nullable),
                )
            }
            (CalendarFunction::Parse, [source, pattern]) => {
                let source = varchar_argument(source)?;
                let target = temporal_argument(
                    &FunctionValueType::new(DataType::Null, nullable),
                    &ValueKind::Temporal(TemporalType::Timestamp(
                        novarocks_type_contract::TemporalPrecision::DEFAULT,
                    )),
                )?;
                (
                    source.clone(),
                    vec![source, varchar_argument(pattern)?],
                    target,
                )
            }
            (CalendarFunction::DateProjection, [source]) => {
                let source = match source.value_type().kind() {
                    ValueKind::Scalar(DataType::Date32) => temporal_argument(
                        source,
                        &ValueKind::Temporal(TemporalType::Timestamp(
                            novarocks_type_contract::TemporalPrecision::WHOLE_SECONDS,
                        )),
                    )?,
                    ValueKind::Temporal(
                        TemporalType::Timestamp(_) | TemporalType::TimestampWithTimeZone(_),
                    ) => (*source).clone(),
                    _ => varchar_argument(source)?,
                };
                (
                    source.clone(),
                    vec![source],
                    FunctionValueType::new(DataType::Date32, nullable),
                )
            }
            (CalendarFunction::LastDay, [stamp]) => {
                let stamp = if stamp.data_type() == &DataType::Null {
                    temporal_argument(stamp, &ValueKind::Scalar(DataType::Date32))?
                } else {
                    (*stamp).clone()
                };
                (
                    stamp.clone(),
                    vec![stamp],
                    FunctionValueType::new(DataType::Date32, nullable),
                )
            }
            _ => return Err(FunctionBindingError::NoMatchingOverload),
        };
        let family = self.operation.family(stamp.value_type().kind())?;
        let index = self
            .operation
            .overloads()
            .position(|candidate| candidate == family)
            .ok_or(FunctionBindingError::NoMatchingOverload)?;
        Ok(FunctionBindingSelection {
            overload: self.overloads[index].clone(),
            argument_types: arguments
                .into_iter()
                .map(FunctionArgumentType::Value)
                .collect::<Vec<_>>()
                .into(),
            result_type: FunctionResultType::Scalar(result),
            aggregate: None,
        })
    }

    fn select(
        &self,
        expected: Option<&FunctionOverloadId>,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            binding_control::request_types(request, work)?;
            let selection = self.selection(request)?;
            if expected.is_some_and(|expected| *expected != selection.overload) {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            Ok(selection)
        })
    }
}

impl FunctionBindingResolver for DateShiftResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.select(None, request, control)
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.select(Some(overload), request, control)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            binding_control::request_types(request, work)?;
            let exact = self.selection(request)?;
            if selected != &exact
                || !request
                    .arguments
                    .iter()
                    .zip(&exact.argument_types)
                    .all(|(arg, declared)| matches!(
                        (arg, declared),
                        (FunctionArgument::Value { value_type, .. }, FunctionArgumentType::Value(ty))
                            if value_type == ty
                    ))
            {
                return Err(FunctionBindingError::InvalidBinding(INVALID_SELECTION.into()));
            }
            Ok(())
        })
    }
}

pub(super) fn definition(
    operation: CalendarFunction,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let name = operation.name();
    let implementation =
        PureImplementationId::try_new(format!("builtin.scalar/{name}/selected-v1"))?;
    let mut overloads = Vec::new();
    let mut declarations = Vec::new();
    let mut implementations = Vec::new();
    let public = !name.starts_with("__extract_");
    let mut display = Vec::new();
    for family in operation.overloads() {
        let id = FunctionOverloadId::try_new(format!("builtin.scalar/{name}/{family}/v1"))
            .map_err(|error| owner_error("calendar overload", &error))?;
        let temporal = match family {
            "date" => "date",
            "varchar" => "varchar(x)",
            "time" => "time(p)",
            "time_with_time_zone" => "time(p) with time zone",
            "timestamp" => "timestamp(p)",
            INTERVAL_YEAR_MONTH => "interval year-month(Q)",
            INTERVAL_DAY_TIME => "interval day-time(Q)",
            _ => "timestamp(p) with time zone",
        };
        let (arguments, result) = match operation {
            CalendarFunction::Add => (format!("varchar(x),bigint,{temporal}"), temporal),
            CalendarFunction::Difference => (format!("varchar(x),{temporal},{temporal}"), "bigint"),
            CalendarFunction::Truncate => (format!("varchar(x),{temporal}"), temporal),
            CalendarFunction::LastDay | CalendarFunction::DateProjection => {
                (temporal.to_owned(), "date")
            }
            CalendarFunction::Field(_, _) => (temporal.to_owned(), "bigint"),
            CalendarFunction::Format => (format!("{temporal},varchar(x)"), "varchar"),
            CalendarFunction::Parse => ("varchar(x),varchar(y)".to_owned(), "timestamp(3)"),
        };
        if public {
            let public_arguments = match family {
                INTERVAL_YEAR_MONTH => "interval year to month".to_owned(),
                INTERVAL_DAY_TIME => "interval day to second".to_owned(),
                _ => arguments.replace(',', ", "),
            };
            display.push((
                id.clone(),
                vec![crate::FunctionPublicSignature::new(
                    public_arguments,
                    result,
                    "",
                )],
            ));
        }
        declarations.push(FunctionOverloadDeclaration::from_effects(
            id.clone(),
            arguments,
            result,
            None,
            effects(),
        ));
        implementations.push(PureImplementationDeclaration {
            overload: id.clone(),
            implementation: implementation.clone(),
            abi: PureKernelAbi::ScalarV1,
        });
        overloads.push(id);
    }
    let function = FunctionId::try_new(format!("builtin.scalar/{name}/v1"))
        .map_err(|error| owner_error("calendar function", &error))?;
    let declaration =
        FunctionBindingDeclaration::try_new_complete(function, FunctionKind::Scalar, declarations)
            .map_err(|error| owner_error("calendar declaration", &error))?;
    let declaration = if public {
        declaration
            .with_public_signatures(display)
            .map_err(super::catalogue::public_signature_error)?
    } else {
        declaration
    };
    FunctionDefinition::try_new_pure_scalar(
        name,
        if public {
            FunctionVisibility::Public
        } else {
            FunctionVisibility::Hidden
        },
        Arc::new(Owner {
            resolver: Arc::new(DateShiftResolver {
                operation,
                overloads,
            }),
            declaration,
            implementations: implementations.into_boxed_slice(),
        }),
    )
    .map_err(|error| owner_error("calendar implementation", &error))
}

fn owner_error(subject: &'static str, error: &dyn fmt::Display) -> FunctionCatalogError {
    FunctionCatalogError::InvalidStableIdentity {
        subject,
        value: error.to_string().into(),
    }
}

fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::MayRaise,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::Strict,
        argument_control: ArgumentControl::OrderedNull,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

struct Owner {
    resolver: Arc<DateShiftResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
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

impl PureFunctionMetadataOwner for Owner {
    fn binding_resolver_owner(self: Arc<Self>) -> FunctionBindingResolverOwner {
        FunctionBindingResolverOwner::borrowed_date_shift(Arc::clone(&self.resolver))
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}

impl Owner {
    fn refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionBindingError> {
        binding_control::scope(control, |work| {
            if input.function_id != self.declaration.function_id()
                || input.kind != FunctionKind::Scalar
                || !input.environment.is_empty()
                || input.proof_scope != CallProofScope::Unconditional
                    && input.proof_scope != input.context.proof_scope()
            {
                return Err(FunctionBindingError::InvalidBinding(
                    "Calendar call requires its admitted proof and empty environment".into(),
                ));
            }
            work.flush()?;
            validate()?;
            let base = self
                .declaration
                .effect_declaration(&input.selected.overload)?;
            Ok(CallEffects {
                value_stability: base.value_stability,
                own_row_error: base.own_row_error,
                failure_behavior: base.failure_behavior,
                null_behavior: base.null_behavior.clone(),
                argument_control: base.argument_control,
                instance_state: base.instance_state,
                observable_effects: base.observable_effects,
                environment: Box::new([]),
                proof_scope: input.proof_scope,
            })
        })
    }

    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        let effects = self
            .refine(input, control, validate)
            .map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("Calendar preparation rejected the frozen binding"),
            })?;
        if contract.function_id() != input.function_id
            || !std::ptr::eq(contract.selected(), input.selected)
            || CallEffectContext::from(contract.context()) != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || contract.effects() != &effects
        {
            return Err(invalid(
                "Calendar preparation differs from its selected call",
            ));
        }
        let operation = self.resolver.operation;
        let interval = match (operation, input.selected.argument_types.as_ref()) {
            (CalendarFunction::Field(_, field), [FunctionArgumentType::Value(argument)]) => {
                match argument.value_type().temporal() {
                    Some(TemporalType::Interval(qualifier)) => {
                        Some(IntervalExtraction::new(field, qualifier).map_err(|_| {
                            invalid("Calendar field differs from its frozen interval")
                        })?)
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        Ok(Arc::new(Prepared {
            operation,
            unit: if operation.uses_unit() {
                PreparedUnit::new(input.request, control)?
            } else {
                PreparedUnit::Dynamic
            },
            interval,
            contract,
        }))
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
        self.refine(input, control, || {
            self.resolver
                .validate_selected(input.selected, input.request, control)
        })
        .map_err(effect_error)
    }

    fn validate_and_refine_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &CheckedCallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, CheckedOwnerFailure<FunctionEffectOwnerError<Self::Error>>> {
        self.refine(input, control, || {
            checked.require_binding_author(self.resolver.as_ref(), input)
        })
        .map_err(effect_error)
        .map_err(CheckedOwnerFailure::Original)
    }
}

fn effect_error(error: FunctionBindingError) -> FunctionEffectOwnerError<FunctionBindingError> {
    match error {
        FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
        other => FunctionEffectOwnerError::Owner(other),
    }
}

impl PureScalarImplementation for Owner {
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        self.prepare(input, contract, control, || {
            self.resolver
                .validate_selected(input.selected, input.request, control)
        })
    }

    fn prepare_scalar_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &CheckedCallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, CheckedOwnerFailure<KernelFailure>> {
        self.prepare(input, contract, control, || {
            checked.require_binding_author(self.resolver.as_ref(), input)
        })
        .map_err(CheckedOwnerFailure::Original)
    }
}

#[derive(Debug)]
struct Prepared {
    operation: CalendarFunction,
    unit: PreparedUnit,
    interval: Option<IntervalExtraction>,
    contract: Arc<ScalarCallContract>,
}

impl PreparedScalarKernel for Prepared {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<Instance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(Instance {
            operation: self.operation,
            unit: self.unit,
            interval: self.interval,
        }))
    }
}

#[derive(Clone, Copy, Debug)]
enum PreparedUnit {
    Dynamic,
    Known(Unit),
    Invalid,
}

impl PreparedUnit {
    fn new(
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        let Some(FunctionArgument::Value {
            constant: Some(value),
            ..
        }) = request.arguments.first()
        else {
            return Ok(Self::Dynamic);
        };
        let text = value
            .try_utf8_borrowed_observed(CompilePhase::FunctionSpecialization, control)
            .map_err(|error| match error {
                ConstantError::Control(error) => compile_failure(error),
                _ => invalid("Calendar unit constant differs from its frozen binding"),
            })?;
        Ok(match text {
            None => Self::Dynamic,
            Some(text) => Unit::parse(text).map_or(Self::Invalid, Self::Known),
        })
    }

    fn parse(self, text: &str) -> Result<Unit, ShiftError> {
        match self {
            Self::Dynamic => Unit::parse(text).ok_or(ShiftError::InvalidUnit),
            Self::Known(unit) => Ok(unit),
            Self::Invalid => Err(ShiftError::InvalidUnit),
        }
    }
}

struct Instance {
    operation: CalendarFunction,
    unit: PreparedUnit,
    interval: Option<IntervalExtraction>,
}

enum Source<'a> {
    Date(&'a Date32Array),
    Text(&'a StringArray),
    Temporal(PreparedTemporalArray<'a>),
}

impl<'a> Source<'a> {
    fn bind(ty: &FunctionValueType, array: &'a dyn Array) -> Result<Self, KernelFailure> {
        let bad = || invalid("Calendar operand differs from its frozen temporal type");
        if let Some(temporal) = ty.value_type().temporal() {
            let source =
                PreparedTemporalArray::bind(temporal, array, ty.nullable()).map_err(|_| bad())?;
            Ok(Self::Temporal(source))
        } else if is_varchar(ty) {
            Ok(Self::Text(array.as_any().downcast_ref().ok_or_else(bad)?))
        } else {
            Ok(Self::Date(array.as_any().downcast_ref().ok_or_else(bad)?))
        }
    }

    fn at(&self, row: usize) -> Result<Option<Value>, KernelFailure> {
        match self {
            Self::Date(array) => Ok(array.is_valid(row).then(|| Value::Date(array.value(row)))),
            Self::Text(_) => Err(invalid("Calendar call expected a date or temporal value")),
            Self::Temporal(array) => array
                .value(row)
                .map(|value| value.map(Value::Temporal))
                .map_err(|_| invalid("Calendar call rejected a frozen temporal operand")),
        }
    }
}

fn project_date(
    source: &Source<'_>,
    temporal: Option<TemporalType>,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<Result<Value, ShiftError>>, KernelFailure> {
    use crate::datetime_value::{DateParseObservation, parse_trino_date32_observed};
    if let Source::Text(text) = source {
        if text.is_null(row) {
            return Ok(None);
        }
        let days = parse_trino_date32_observed(text.value(row), |event| match event {
            DateParseObservation::Step => work.step(),
            DateParseObservation::OpaqueBoundary => work.flush(),
        })?;
        return Ok(Some(days.map(Value::Date).ok_or(ShiftError::Cast)));
    }
    let Some(Value::Temporal(value)) = source.at(row)? else {
        return Ok(None);
    };
    let temporal = temporal.ok_or_else(|| invalid("Date projection expected a timestamp"))?;
    let pair = temporal_cast::Pair {
        source: Type::Temporal(temporal),
        target: Type::Date,
        dependencies: temporal_cast::Dependencies::None,
    };
    Ok(Some(
        match pair.convert(
            temporal_cast::Input::Temporal(value),
            temporal_cast::Facts::default(),
        ) {
            Ok(temporal_cast::Output::Date(date)) => Ok(Value::Date(date)),
            Err(_) => Err(ShiftError::Cast),
            _ => return Err(invalid("Date projection returned a foreign value")),
        },
    ))
}

impl ScalarKernelInstance for Instance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        control.checkpoint(0)?;
        let arguments = input.arguments();
        let declared = &input.contract().selected().argument_types;
        let selection = input.selection();
        if arguments.len() != declared.len() {
            return Err(invalid(
                "Calendar argument count differs from its frozen contract",
            ));
        }
        for (argument, ty) in arguments.iter().zip(declared) {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("Calendar call expected a value argument"));
            };
            validate_argument_observed(*argument, selection, ty, control)?;
        }
        let temporal_index = self.operation.temporal_argument_index();
        let stamp_arg = arguments[temporal_index];
        let FunctionArgumentType::Value(stamp_type) = &declared[temporal_index] else {
            return Err(invalid("Calendar call expected a temporal argument"));
        };
        let stamp = Source::bind(stamp_type, stamp_arg.array().as_ref())?;
        let temporal = stamp_type.value_type().temporal();
        let text = if self.operation.uses_unit() {
            let arg = arguments[0];
            let text = arg
                .array()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid("Calendar unit is not UTF8"))?;
            Some((arg, text))
        } else {
            None
        };
        let offset = if self.operation == CalendarFunction::Add {
            let arg = arguments[1];
            let offset = arg
                .array()
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| invalid("Date addition offset is not BIGINT"))?;
            Some((arg, offset))
        } else {
            None
        };
        let second = if self.operation == CalendarFunction::Difference {
            let arg = arguments[2];
            let FunctionArgumentType::Value(ty) = &declared[2] else {
                return Err(invalid(
                    "Date difference expected a second temporal argument",
                ));
            };
            Some((arg, Source::bind(ty, arg.array().as_ref())?))
        } else {
            None
        };
        let pattern = if matches!(
            self.operation,
            CalendarFunction::Format | CalendarFunction::Parse
        ) {
            let arg = arguments[1];
            let text = arg
                .array()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid("Date pattern is not UTF8"))?;
            Some((arg, text))
        } else {
            None
        };
        let result = input.contract().result_type();
        let output_type = match self.operation {
            CalendarFunction::Difference | CalendarFunction::Field(_, _) => Type::Bigint,
            CalendarFunction::LastDay | CalendarFunction::DateProjection => Type::Date,
            CalendarFunction::Format => Type::Text,
            CalendarFunction::Parse => Type::Temporal(
                result
                    .value_type()
                    .temporal()
                    .ok_or_else(|| invalid("date_parse requires a frozen timestamp result"))?,
            ),
            _ => temporal.map_or(Type::Date, Type::Temporal),
        };
        let mut work = EvaluationCheckpoints::new(control);
        let mut output = TemporalOutputBuilder::new(output_type, selection.len())?;
        let mut errors = Vec::new();
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let source_row = stamp_arg.value_row(ordinal, row);
            if let Some((arg, patterns)) = pattern {
                let pattern_row = arg.value_row(ordinal, row);
                if patterns.is_null(pattern_row) {
                    output.null();
                    continue;
                }
                let pattern = patterns.value(pattern_row);
                let computed = if let Source::Text(text) = &stamp {
                    if text.is_null(source_row) {
                        output.null();
                        continue;
                    }
                    date_pattern::parse(text.value(source_row), pattern, &mut work)
                        .map(PatternResult::Parsed)
                } else {
                    let Some(Value::Temporal(value)) = stamp.at(source_row)? else {
                        output.null();
                        continue;
                    };
                    date_pattern::format(value, pattern, &mut work).map(PatternResult::Formatted)
                };
                match computed {
                    Ok(PatternResult::Formatted(text)) => output.text(&text)?,
                    Ok(PatternResult::Parsed(value)) => output.temporal(
                        output_type
                            .temporal()
                            .ok_or_else(|| invalid("date_parse has no timestamp output"))?,
                        value,
                    )?,
                    Err(date_pattern::Failure::Kernel(error)) => return Err(error),
                    Err(date_pattern::Failure::Value) => append_error(
                        &mut output,
                        &mut errors,
                        ordinal,
                        ShiftError::InvalidPattern,
                        &mut work,
                    )?,
                }
                continue;
            }
            let value = if self.operation == CalendarFunction::DateProjection {
                let Some(value) = project_date(&stamp, temporal, source_row, &mut work)? else {
                    output.null();
                    continue;
                };
                value
            } else {
                let Some(stamp_value) = stamp.at(source_row)? else {
                    output.null();
                    continue;
                };
                let unit = if let Some((arg, text)) = text {
                    let i = arg.value_row(ordinal, row);
                    if text.is_null(i) {
                        output.null();
                        continue;
                    }
                    Some(text.value(i))
                } else {
                    None
                };
                match (unit, &offset, &second) {
                    (Some(unit), Some((arg, offset)), None) => {
                        let i = arg.value_row(ordinal, row);
                        if offset.is_null(i) {
                            output.null();
                            continue;
                        }
                        self.unit
                            .parse(unit)
                            .and_then(|unit| add(unit, offset.value(i), temporal, stamp_value))
                    }
                    (Some(unit), None, Some((arg, second))) => {
                        let Some(second_value) = second.at(arg.value_row(ordinal, row))? else {
                            output.null();
                            continue;
                        };
                        self.unit
                            .parse(unit)
                            .and_then(|unit| difference(unit, stamp_value, second_value))
                            .map(Value::Bigint)
                    }
                    (Some(unit), None, None) => self
                        .unit
                        .parse(unit)
                        .and_then(|unit| truncate(unit, temporal, stamp_value)),
                    (None, None, None) => match self.operation {
                        CalendarFunction::LastDay => last_day(stamp_value).map(Value::Date),
                        CalendarFunction::Field(_, field) => self
                            .interval
                            .map_or_else(
                                || field.evaluate(stamp_value),
                                |interval| interval.evaluate(stamp_value),
                            )
                            .map(Value::Bigint),
                        _ => return Err(invalid("Calendar call expected its unit argument")),
                    },
                    _ => return Err(invalid("Unexpected calendar call operands")),
                }
            };
            match value {
                Ok(Value::Date(days)) => output.date(days)?,
                Ok(Value::Bigint(value)) => output.bigint(value)?,
                Ok(Value::Temporal(value)) => output.temporal(
                    temporal.ok_or_else(|| invalid("Calendar call expected DATE output"))?,
                    value,
                )?,
                Ok(_) => return Err(invalid("Calendar call returned a foreign value")),
                Err(cause) => append_error(&mut output, &mut errors, ordinal, cause, &mut work)?,
            }
        }
        work.flush()?;
        let selected = SelectedValues::try_new_observed(
            selection,
            result.data_type(),
            output.finish(),
            errors.into_boxed_slice(),
            || work.step(),
        );
        work.finish_result(selected)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

enum PatternResult {
    Formatted(String),
    Parsed(novarocks_type_contract::TemporalValue),
}

fn append_error(
    output: &mut TemporalOutputBuilder,
    errors: &mut Vec<RowDataError>,
    ordinal: usize,
    cause: ShiftError,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    work.flush()?;
    errors
        .try_reserve(1)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    errors.push(RowDataError::from_domain(ordinal, cause)?);
    output.null();
    Ok(())
}
