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

use arrow_array::{Array, Float64Array, Int64Array, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CompilePhase, FunctionArgumentTypes, FunctionEffectDeclaration,
    FunctionInstanceState, FunctionNullBehavior, ObservableEffects, PICOS_PER_DAY,
    PICOS_PER_SECOND, PureCompileControl, SemanticParameterKey as Key, SemanticParameterRef,
    SemanticParameterValue, SemanticParameters, StatementStartUtc, TemporalMicros,
    TemporalPrecision, TemporalType, TemporalValue, TimeZoneKey, ValueKind, ValueLogicalType,
    trino_can_coerce_scalar,
};
use novarocks_value_arrays::decode::PreparedTemporalArray;

use crate::kernel_control::invalid;
use crate::kernel_input::{EvaluationCheckpoints, validate_argument_observed};
use crate::temporal_cast::Type;
use crate::temporal_value::{round_time, round_timestamp};
use crate::temporal_zone_rules::{
    TemporalZoneSource, joda_local_to_utc, offset_seconds, utc_to_local,
};
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

use super::{
    binding_control,
    date_shift_owner::{temporal_argument, varchar_argument},
    temporal_output::TemporalOutputBuilder,
};

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
    Now,
    QuotedCurrentDate,
    AtTimezoneName,
    AtTimezoneInterval,
    WithTimezone,
    FromUnixSession,
    FromUnixName,
    FromUnixOffset,
    ToUnix,
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

const PUBLIC: &[(&str, &[TemporalSyntax])] = {
    use TemporalSyntax::*;
    &[
        ("now", &[Now]),
        ("current_date", &[QuotedCurrentDate]),
        ("at_timezone", &[AtTimezoneName, AtTimezoneInterval]),
        ("with_timezone", &[WithTimezone]),
        (
            "from_unixtime",
            &[FromUnixSession, FromUnixName, FromUnixOffset],
        ),
        ("to_unixtime", &[ToUnix]),
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
            Now => "builtin.scalar/now/clock-v1",
            QuotedCurrentDate => "builtin.scalar/current_date/clock-v1",
            AtTimezoneName => "builtin.scalar/at_timezone/timestamp_zone_name/v1",
            AtTimezoneInterval => "builtin.scalar/at_timezone/timestamp_offset/v1",
            WithTimezone => "builtin.scalar/with_timezone/timestamp_zone_name/v1",
            FromUnixSession => "builtin.scalar/from_unixtime/session_zone/v1",
            FromUnixName => "builtin.scalar/from_unixtime/zone_name/v1",
            FromUnixOffset => "builtin.scalar/from_unixtime/zone_offset/v1",
            ToUnix => "builtin.scalar/to_unixtime/timestamp_with_time_zone/v1",
        }
    }

    pub fn dependencies(self) -> &'static [Key] {
        use TemporalSyntax::*;
        match self {
            AtTimestampName | AtTimestampInterval | AtTimeInterval | AtTimezoneName
            | AtTimezoneInterval | WithTimezone | FromUnixName | FromUnixOffset | ToUnix => &[],
            AtTimestampLocal | FromUnixSession => &[Key::TimeZone],
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
            Now | QuotedCurrentDate => "()",
            AtTimezoneName => "timestamp(p) with time zone,varchar(x)",
            AtTimezoneInterval => "timestamp(p) with time zone,interval day-time(Q)",
            WithTimezone => "timestamp(p),varchar(x)",
            FromUnixSession => "double",
            FromUnixName => "double,varchar(x)",
            FromUnixOffset => "double,bigint,bigint",
            ToUnix => "timestamp(p) with time zone",
        }
    }

    fn argument_count(self) -> usize {
        use TemporalSyntax::*;
        match self {
            CurrentDate | CurrentTime | CurrentTimestamp | LocalTime | LocalTimestamp | Now
            | QuotedCurrentDate => 0,
            AtTimestampLocal | AtTimeLocal | FromUnixSession | ToUnix => 1,
            FromUnixOffset => 3,
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
            .chain(
                PUBLIC
                    .iter()
                    .flat_map(|(_, operations)| operations.iter().copied()),
            )
            .find(|operation| operation.identity() == overload.as_str())
            .ok_or_else(|| FunctionBindingError::UnknownOverload(overload.clone()))
    }

    fn public_result_pattern(self) -> &'static str {
        use TemporalSyntax::*;
        match self {
            QuotedCurrentDate => "date",
            Now | FromUnixSession | FromUnixName | FromUnixOffset => "timestamp(3) with time zone",
            ToUnix => "double",
            _ => "timestamp(p) with time zone",
        }
    }

    fn public_selection(
        self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        use TemporalSyntax::*;
        if request.arguments.len() != self.argument_count()
            || request.logical_argument_count != request.arguments.len()
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        let arguments = request
            .arguments
            .iter()
            .map(|arg| match arg {
                FunctionArgument::Value { value_type, .. } => Ok(value_type),
                _ => Err(FunctionBindingError::NoMatchingOverload),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let nullable = arguments.iter().any(|arg| arg.nullable());
        let zoned = |source: &FunctionValueType, interpret_local: bool| {
            use TemporalType as T;
            let precision = match source.value_type().kind() {
                ValueKind::Temporal(T::Timestamp(p)) => *p,
                ValueKind::Temporal(T::TimestampWithTimeZone(p)) if !interpret_local => *p,
                ValueKind::Scalar(DataType::Date32 | DataType::Null) => {
                    TemporalPrecision::WHOLE_SECONDS
                }
                _ => return Err(FunctionBindingError::NoMatchingOverload),
            };
            let argument_type = if interpret_local {
                T::Timestamp(precision)
            } else {
                T::TimestampWithTimeZone(precision)
            };
            let target_type = T::TimestampWithTimeZone(precision);
            let argument = temporal_argument(source, &ValueKind::Temporal(argument_type))?;
            let result = temporal_argument(source, &ValueKind::Temporal(target_type))?
                .with_nullable(nullable)
                .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?;
            Ok((argument, result))
        };
        let default_zoned = || {
            let null = FunctionValueType::new(DataType::Null, nullable);
            temporal_argument(
                &null,
                &ValueKind::Temporal(TemporalType::TimestampWithTimeZone(
                    TemporalPrecision::DEFAULT,
                )),
            )
        };
        let coerce_numeric = |ty: &FunctionValueType, target: DataType| {
            if !trino_can_coerce_scalar(ty.value_type().kind(), &ValueKind::Scalar(target.clone()))
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            Ok(FunctionValueType::new(target, ty.nullable()))
        };
        let (arguments, result) = match self {
            Now => (vec![], default_zoned()?),
            QuotedCurrentDate => (vec![], FunctionValueType::new(DataType::Date32, false)),
            AtTimezoneName | AtTimezoneInterval | WithTimezone => {
                let (arg, result) = zoned(arguments[0], self == WithTimezone)?;
                let zone = arguments[1];
                let zone = if self == AtTimezoneInterval {
                    if !matches!(zone.value_type().temporal(), Some(TemporalType::Interval(q)) if !q.is_year_month())
                    {
                        return Err(FunctionBindingError::NoMatchingOverload);
                    }
                    zone.clone()
                } else {
                    varchar_argument(zone)?
                };
                (vec![arg, zone], result)
            }
            ToUnix => {
                let (arg, _) = zoned(arguments[0], false)?;
                (
                    vec![arg],
                    FunctionValueType::new(DataType::Float64, nullable),
                )
            }
            FromUnixSession | FromUnixName | FromUnixOffset => {
                let mut selected = vec![coerce_numeric(arguments[0], DataType::Float64)?];
                match self {
                    FromUnixName => selected.push(varchar_argument(arguments[1])?),
                    FromUnixOffset => {
                        selected.push(coerce_numeric(arguments[1], DataType::Int64)?);
                        selected.push(coerce_numeric(arguments[2], DataType::Int64)?);
                    }
                    _ => {}
                }
                (selected, default_zoned()?)
            }
            _ => return Err(FunctionBindingError::NoMatchingOverload),
        };
        Ok(FunctionBindingSelection {
            overload: FunctionOverloadId::try_new(self.identity())
                .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?,
            argument_types: FunctionArgumentTypes::from_pure_boxed(
                arguments
                    .into_iter()
                    .map(FunctionArgumentType::Value)
                    .collect(),
            ),
            result_type: FunctionResultType::Scalar(result),
            aggregate: None,
        })
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

pub(crate) fn register(
    builder: &mut crate::EngineFunctionCatalogBuilder,
) -> Result<(), FunctionCatalogError> {
    builder.register(definition(FUNCTION_NAME, FUNCTION_ID, &SYNTAX, false)?)?;
    for &(name, operations) in PUBLIC {
        builder.register(definition(
            name,
            &format!("builtin.scalar/{name}/v1"),
            operations,
            true,
        )?)?;
    }
    Ok(())
}

fn definition(
    name: &str,
    function: &str,
    operations: &'static [TemporalSyntax],
    public: bool,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    fn invalid_catalog(error: impl std::fmt::Display) -> FunctionCatalogError {
        FunctionCatalogError::InvalidStableIdentity {
            subject: "temporal statement syntax owner",
            value: error.to_string().into(),
        }
    }
    let mut overloads = Vec::new();
    let mut implementations = Vec::new();
    for &syntax in operations {
        let overload = FunctionOverloadId::try_new(syntax.identity()).map_err(invalid_catalog)?;
        overloads.push(FunctionOverloadDeclaration::from_effects(
            overload.clone(),
            syntax.argument_pattern(),
            if public {
                syntax.public_result_pattern()
            } else {
                "exact temporal result"
            },
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
        FunctionId::try_new(function).map_err(invalid_catalog)?,
        FunctionKind::Scalar,
        overloads,
    )
    .map_err(invalid_catalog)?;
    FunctionDefinition::try_new_pure_scalar(
        name,
        if public {
            FunctionVisibility::Public
        } else {
            FunctionVisibility::Hidden
        },
        Arc::new(ContextOwner {
            declaration,
            implementations: implementations.into_boxed_slice(),
            resolver: Arc::new(TemporalContextResolver { operations, public }),
        }),
    )
    .map_err(invalid_catalog)
}

// Design: ADR-0256 (docs/adr/ADR-0256-temporal-calls-freeze-overloads-and-selected-row-effects.md)
pub(crate) struct TemporalContextResolver {
    operations: &'static [TemporalSyntax],
    public: bool,
}

impl FunctionBindingResolver for TemporalContextResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            if !self.public {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            binding_control::request_types(request, work)?;
            let mut selected = None;
            for &syntax in self.operations {
                match syntax.public_selection(request) {
                    Ok(_) if selected.is_some() => {
                        return Err(FunctionBindingError::AmbiguousOverload);
                    }
                    Ok(candidate) => selected = Some(candidate),
                    Err(FunctionBindingError::NoMatchingOverload) => {}
                    Err(error) => return Err(error),
                }
            }
            selected.ok_or(FunctionBindingError::NoMatchingOverload)
        })
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
            if !self.operations.contains(&syntax) {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            if self.public {
                return syntax.public_selection(request);
            }
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
            if !self.operations.contains(&syntax) {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            if self.public {
                if selection != &syntax.public_selection(request)?
                    || !selection.argument_types.iter().zip(request.arguments).all(|(selected, argument)| {
                        matches!(
                            (selected, argument),
                            (FunctionArgumentType::Value(selected), FunctionArgument::Value { value_type, .. })
                                if selected == value_type
                        )
                    })
                {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
                return Ok(());
            }
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
        if matches!(
            self.syntax,
            TemporalSyntax::FromUnixSession
                | TemporalSyntax::FromUnixName
                | TemporalSyntax::FromUnixOffset
        ) {
            return self.evaluate_from_epoch(input, work);
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
        let mut output = if self.syntax == TemporalSyntax::ToUnix {
            TemporalOutputBuilder::double(selection.len())?
        } else {
            TemporalOutputBuilder::new(
                declared.map_or(Type::Date, Type::Temporal),
                selection.len(),
            )?
        };
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
                if self.syntax == TemporalSyntax::ToUnix {
                    let TemporalValue::TimestampWithTimeZone { instant, .. } = value else {
                        return Err(invalid("to_unixtime requires a frozen zoned timestamp"));
                    };
                    let precision = input
                        .contract()
                        .value_argument_types()
                        .next()
                        .and_then(|ty| ty.value_type().temporal())
                        .ok_or_else(|| invalid("to_unixtime source precision is missing"))?
                        .precision();
                    Ok(ResultValue::Double(epoch_seconds(instant, precision)))
                } else {
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
                }
            };
            match result {
                Ok(ResultValue::Date(value)) => output.date(value)?,
                Ok(ResultValue::Double(value)) => output.append_double(value)?,
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

    fn evaluate_from_epoch<'a>(
        self,
        input: ScalarCallInput<'_, 'a>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        let [seconds_arg, zone_args @ ..] = input.arguments() else {
            return Err(invalid("from_unixtime requires an epoch value"));
        };
        let seconds = seconds_arg
            .array()
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("from_unixtime epoch is not DOUBLE"))?;
        let zones = match zone_args {
            [] => EpochZone::Session(
                self.facts
                    .zone
                    .ok_or_else(|| invalid("from_unixtime session zone is missing"))?,
            ),
            [arg] => EpochZone::Name(
                arg.array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("from_unixtime zone name is not VARCHAR"))?,
            ),
            [hours, minutes] => {
                let hours = hours
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| invalid("from_unixtime zone hours are not BIGINT"))?;
                let minutes = minutes
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| invalid("from_unixtime zone minutes are not BIGINT"))?;
                EpochZone::Offset(hours, minutes)
            }
            _ => return Err(invalid("from_unixtime got a foreign selected profile")),
        };
        let target = input.contract().result_type();
        let temporal = target
            .value_type()
            .temporal()
            .ok_or_else(|| invalid("from_unixtime selected result is not temporal"))?;
        let selection = input.selection();
        let mut output = TemporalOutputBuilder::new(Type::Temporal(temporal), selection.len())?;
        let mut errors = Vec::new();
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let sec_row = seconds_arg.value_row(ordinal, row);
            if seconds.is_null(sec_row) {
                output.null();
                continue;
            }
            let zone = match zones {
                EpochZone::Session(zone) => Ok(zone),
                EpochZone::Name(text) => {
                    let i = zone_args[0].value_row(ordinal, row);
                    if text.is_null(i) {
                        output.null();
                        continue;
                    }
                    let name = text.value(i);
                    for _ in name.as_bytes().chunks(32) {
                        work.step()?;
                    }
                    TimeZoneKey::parse(name).ok_or(RowFailure::UnknownEpochZone)
                }
                EpochZone::Offset(hours, minutes) => {
                    let hour_row = zone_args[0].value_row(ordinal, row);
                    let minute_row = zone_args[1].value_row(ordinal, row);
                    if hours.is_null(hour_row) || minutes.is_null(minute_row) {
                        output.null();
                        continue;
                    }
                    let offset = hours
                        .value(hour_row)
                        .wrapping_mul(60)
                        .wrapping_add(minutes.value(minute_row));
                    i16::try_from(offset)
                        .ok()
                        .and_then(TimeZoneKey::from_offset_minutes)
                        .ok_or(RowFailure::Zone)
                }
            };
            match zone.and_then(|zone| zoned_epoch(seconds.value(sec_row), zone)) {
                Ok(value) => output.temporal(temporal, value)?,
                Err(cause) => append_error(&mut output, &mut errors, ordinal, cause, work)?,
            }
        }
        work.flush()?;
        SelectedValues::try_new_observed(
            selection,
            target.data_type(),
            output.finish(),
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
        if matches!(self.syntax, CurrentDate | QuotedCurrentDate) {
            let local = utc_to_local(zone, TemporalZoneSource::Joda, instant)
                .map_err(|_| RowFailure::Range)?;
            let days = local.micros().div_euclid(86_400 * 1_000_000);
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
            CurrentTimestamp | Now => TemporalValue::TimestampWithTimeZone {
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
            TemporalValue::Timestamp(local) if self.syntax == TemporalSyntax::WithTimezone => {
                let instant =
                    joda_local_to_utc(zone, local, None).map_err(|_| RowFailure::Range)?;
                TemporalValue::TimestampWithTimeZone { instant, zone }
            }
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
    Double(f64),
    Temporal(TemporalValue),
}
enum EpochZone<'a> {
    Session(TimeZoneKey),
    Name(&'a StringArray),
    Offset(&'a Int64Array, &'a Int64Array),
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
    EpochRange,
    UnknownEpochZone,
}
impl std::fmt::Display for RowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Zone => "Invalid time zone",
            Self::Minute => "Time zone offset must contain whole minutes",
            Self::Range => "Temporal value is outside the supported range",
            Self::EpochRange => "Epoch value cannot be represented as a timestamp with time zone",
            Self::UnknownEpochZone => "Time zone is not supported",
        })
    }
}
impl std::error::Error for RowFailure {}
impl novarocks_failure_contract::FailureProjection for RowFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        use novarocks_function_contract::kernel_failure::{
            INVALID_FUNCTION_ARGUMENT, NUMERIC_OUT_OF_RANGE, UNKNOWN_EPOCH_TIME_ZONE,
        };
        FailureFact::new(
            FailureClass::Data,
            match self {
                Self::Zone | Self::Minute | Self::EpochRange => INVALID_FUNCTION_ARGUMENT,
                Self::Range => NUMERIC_OUT_OF_RANGE,
                Self::UnknownEpochZone => UNKNOWN_EPOCH_TIME_ZONE,
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

fn epoch_millis(seconds: f64) -> i64 {
    if seconds.is_nan() {
        return 0;
    }
    let millis = seconds * 1_000.0;
    if millis.abs() >= (1_u64 << 52) as f64 {
        return millis as i64;
    }
    let floor = millis.floor();
    let fraction = millis - floor;
    let magnitude = millis.abs();
    let ulp = magnitude.next_up() - magnitude;
    if (fraction - 0.5).abs() > 2.0 * ulp {
        return floor as i64 + i64::from(fraction >= 0.5);
    }
    // Compare with the canonical double nearest the decimal tie. The product
    // alone can move an adjacent double onto the same half-millisecond.
    let lower = floor as i64;
    let midpoint = (lower * 2 + 1) as f64 / 2_000.0;
    lower + i64::from(seconds > midpoint || seconds == midpoint && seconds > 0.0)
}

fn zoned_epoch(seconds: f64, zone: TimeZoneKey) -> Result<TemporalValue, RowFailure> {
    use novarocks_type_contract::{MAX_ZONED_TIMESTAMP_MILLIS, MIN_ZONED_TIMESTAMP_MILLIS};

    let millis = epoch_millis(seconds);
    if !(MIN_ZONED_TIMESTAMP_MILLIS..=MAX_ZONED_TIMESTAMP_MILLIS).contains(&millis) {
        return Err(RowFailure::EpochRange);
    }
    Ok(TemporalValue::TimestampWithTimeZone {
        instant: TemporalMicros::whole_micros(millis * 1_000),
        zone,
    })
}

fn epoch_seconds(instant: TemporalMicros, precision: TemporalPrecision) -> f64 {
    if precision <= TemporalPrecision::DEFAULT {
        return instant.floor_millis() as f64 / 1_000.0;
    }
    let divisor = i128::from(novarocks_type_contract::PICOS_PER_SECOND);
    let stamp = instant.as_picos();
    let seconds = stamp.div_euclid(divisor) as f64;
    let remainder = stamp.rem_euclid(divisor) as f64;
    let divisor = divisor as f64;
    let fraction = remainder / divisor;
    // Carry the lost parts of both operations, including cancellation for
    // a negative instant close to the epoch.
    let fraction_error = (-fraction).mul_add(divisor, remainder) / divisor;
    let sum = seconds + fraction;
    let sum_error = fraction - (sum - seconds);
    sum + (sum_error + fraction_error)
}
