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

//! Hidden arithmetic operators select complete temporal argument and result types.

use std::sync::Arc;

use arrow_array::{Array, Date32Array, Float64Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffectContext, CallEffects, CallProofScope, FunctionEffectDeclaration,
    FunctionInstanceState, FunctionNullBehavior, IntervalField, IntervalQualifier,
    ObservableEffects, OperatorFunction, PureCompileControl, TemporalPrecision, TemporalType,
    ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};
use novarocks_value_arrays::decode::PreparedTemporalArray;

use super::{binding_control, temporal_output::TemporalOutputBuilder};
use crate::{
    CallEffectInput, CheckedCallEffectInput, CheckedOwnerFailure, EvaluatedArgument,
    FunctionArgument, FunctionArgumentType, FunctionBindingDeclaration, FunctionBindingError,
    FunctionBindingRequest, FunctionBindingResolver, FunctionBindingResolverOwner,
    FunctionBindingSelection, FunctionCatalogError, FunctionDefinition, FunctionEffectOwner,
    FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError,
    FunctionKind, FunctionOverloadDeclaration, FunctionOverloadId, FunctionResultType,
    FunctionValueType, FunctionVisibility, FunctionVolatility, KernelEvaluationControl,
    KernelFailure, PreparedScalarKernel, PureFunctionMetadataOwner, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, PureScalarImplementation, RowDataError,
    ScalarCallContract, ScalarCallInput, ScalarKernelInstance, SelectedValues,
    kernel_control::{compile_failure, invalid},
    kernel_input::EvaluationCheckpoints,
    temporal_arithmetic::{Value, evaluate},
    temporal_cast::Type,
};

pub(super) const OPERATORS: [OperatorFunction; 5] = [
    OperatorFunction::TemporalAdd,
    OperatorFunction::TemporalSubtract,
    OperatorFunction::TemporalMultiply,
    OperatorFunction::TemporalDivide,
    OperatorFunction::TemporalNegation,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Family {
    Date,
    Time,
    ZonedTime,
    Timestamp,
    ZonedTimestamp,
    YearMonth,
    DayTime,
    Bigint,
    Double,
}

impl Family {
    const ALL: [Self; 9] = [
        Self::Date,
        Self::Time,
        Self::ZonedTime,
        Self::Timestamp,
        Self::ZonedTimestamp,
        Self::YearMonth,
        Self::DayTime,
        Self::Bigint,
        Self::Double,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Date => "date",
            Self::Time => "time",
            Self::ZonedTime => "time_with_time_zone",
            Self::Timestamp => "timestamp",
            Self::ZonedTimestamp => "timestamp_with_time_zone",
            Self::YearMonth => "interval_year_month",
            Self::DayTime => "interval_day_time",
            Self::Bigint => "bigint",
            Self::Double => "double",
        }
    }

    fn of(ty: &FunctionValueType) -> Option<Self> {
        if let Some(temporal) = ty.value_type().temporal() {
            return Some(match temporal {
                TemporalType::Time(_) => Self::Time,
                TemporalType::TimeWithTimeZone(_) => Self::ZonedTime,
                TemporalType::Timestamp(_) => Self::Timestamp,
                TemporalType::TimestampWithTimeZone(_) => Self::ZonedTimestamp,
                TemporalType::Interval(q) if q.is_year_month() => Self::YearMonth,
                TemporalType::Interval(_) => Self::DayTime,
            });
        }
        if ty.logical_type() != ValueLogicalType::Physical {
            return None;
        }
        Some(match ty.data_type() {
            DataType::Date32 => Self::Date,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Self::Bigint,
            DataType::Float32 | DataType::Float64 => Self::Double,
            _ => return None,
        })
    }

    const fn interval(self) -> bool {
        matches!(self, Self::YearMonth | Self::DayTime)
    }

    const fn datetime(self) -> bool {
        matches!(
            self,
            Self::Time | Self::ZonedTime | Self::Timestamp | Self::ZonedTimestamp
        )
    }

    const fn shift_accepts(self, right: Self) -> bool {
        use Family::*;
        matches!(
            (self, right),
            (Date | Timestamp | ZonedTimestamp, YearMonth | DayTime) | (Time | ZonedTime, DayTime)
        )
    }

    fn argument(self, source: &FunctionValueType) -> FunctionValueType {
        match self {
            Self::Bigint => FunctionValueType::new(DataType::Int64, source.nullable()),
            Self::Double => FunctionValueType::new(DataType::Float64, source.nullable()),
            _ => source.clone(),
        }
    }

    fn matches_argument(self, bound: &FunctionValueType, original: &FunctionValueType) -> bool {
        match self {
            Self::Bigint => bound.data_type() == &DataType::Int64 && bound == original,
            Self::Double => bound.data_type() == &DataType::Float64 && bound == original,
            _ => bound == original,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Signature(Family, Option<Family>);

impl Signature {
    fn accepts(self, operator: OperatorFunction) -> bool {
        use Family::*;
        use OperatorFunction::*;
        let Self(left, right) = self;
        match (operator, right) {
            (TemporalNegation, None) => left.interval(),
            (TemporalAdd, Some(right)) => {
                left.shift_accepts(right)
                    || right.shift_accepts(left)
                    || left == right && left.interval()
            }
            (TemporalSubtract, Some(right)) => {
                left.shift_accepts(right) || left == right && (left.interval() || left.datetime())
            }
            (TemporalMultiply, Some(right)) => {
                left.interval() && matches!(right, Bigint | Double)
                    || right.interval() && matches!(left, Bigint | Double)
            }
            (TemporalDivide, Some(Double)) => left.interval(),
            _ => false,
        }
    }

    fn row_error(self, operator: OperatorFunction) -> FunctionIntrinsicRowError {
        use Family::*;
        use OperatorFunction::*;
        match (operator, self.0, self.1) {
            (TemporalAdd | TemporalSubtract, Time | ZonedTime, Some(DayTime))
            | (TemporalAdd, DayTime, Some(Time | ZonedTime))
            | (TemporalSubtract, Time, Some(Time))
            | (TemporalSubtract, ZonedTime, Some(ZonedTime)) => {
                FunctionIntrinsicRowError::NoRowError
            }
            _ => FunctionIntrinsicRowError::MayRaise,
        }
    }

    fn result(
        self,
        operator: OperatorFunction,
        left: &FunctionValueType,
        right: Option<&FunctionValueType>,
    ) -> Result<Option<TemporalType>, FunctionBindingError> {
        use Family::*;
        use TemporalPrecision as P;
        use TemporalType as T;
        let left = left.value_type().temporal();
        let right = right.and_then(|value| value.value_type().temporal());
        let Some(second) = self.1 else {
            return Ok(Some(derived_interval(qualifier(left)?)?));
        };
        let first = self.0;
        if first == Date || second == Date {
            return Ok(None);
        }
        if first == second && first.datetime() && operator == OperatorFunction::TemporalSubtract {
            let precision = left
                .zip(right)
                .map(|(lhs, rhs)| lhs.precision().max(rhs.precision()).max(P::MICROSECONDS))
                .ok_or(FunctionBindingError::NoMatchingOverload)?;
            return Ok(Some(new_interval(
                IntervalField::Day,
                IntervalField::Second,
                precision,
            )?));
        }
        if first.interval() && second.interval() {
            let a = qualifier(left)?;
            let b = qualifier(right)?;
            return Ok(Some(new_interval(
                a.start().min(b.start()),
                a.end().max(b.end()),
                a.fractional_precision().max(b.fractional_precision()),
            )?));
        }
        if let Some(time) = left
            .filter(|_| first.datetime())
            .or_else(|| right.filter(|_| second.datetime()))
        {
            let delta = qualifier(if first.interval() { left } else { right })?;
            if delta.is_year_month() {
                return Ok(Some(time));
            }
            let precision = time
                .precision()
                .max(delta.fractional_precision())
                .max(P::DEFAULT);
            return Ok(Some(match time {
                T::Time(_) => T::Time(precision),
                T::TimeWithTimeZone(_) => T::TimeWithTimeZone(precision),
                T::Timestamp(_) => T::Timestamp(precision),
                T::TimestampWithTimeZone(_) => T::TimestampWithTimeZone(precision),
                _ => return Err(FunctionBindingError::NoMatchingOverload),
            }));
        }
        Ok(Some(derived_interval(qualifier(if first.interval() {
            left
        } else {
            right
        })?)?))
    }
}

fn qualifier(temporal: Option<TemporalType>) -> Result<IntervalQualifier, FunctionBindingError> {
    match temporal {
        Some(TemporalType::Interval(value)) => Ok(value),
        _ => Err(FunctionBindingError::NoMatchingOverload),
    }
}

fn derived_interval(q: IntervalQualifier) -> Result<TemporalType, FunctionBindingError> {
    new_interval(q.start(), q.end(), q.fractional_precision())
}

fn new_interval(
    start: IntervalField,
    end: IntervalField,
    precision: TemporalPrecision,
) -> Result<TemporalType, FunctionBindingError> {
    IntervalQualifier::new(start, end, start.max_leading_precision(), precision)
        .map(TemporalType::Interval)
        .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))
}

fn result_type(
    result: Option<TemporalType>,
    nullable: bool,
) -> Result<FunctionValueType, FunctionBindingError> {
    let Some(temporal) = result else {
        return Ok(FunctionValueType::new(DataType::Date32, nullable));
    };
    let invalid = |error: &dyn std::fmt::Display| {
        FunctionBindingError::InvalidBinding(error.to_string().into())
    };
    let storage = ValueTypeStorageLifetime::pure();
    let semantic = ValueTypeRef::try_new(ValueKind::Temporal(temporal), nullable, storage.clone())
        .map_err(|error| invalid(&error))?;
    let layout =
        ValueLayout::try_new(&temporal.carrier(), storage).map_err(|error| invalid(&error))?;
    FunctionValueType::try_from_value(semantic, layout).map_err(|error| invalid(&error))
}

fn matches_result(
    actual: &FunctionValueType,
    result: Option<TemporalType>,
    nullable: bool,
) -> bool {
    actual.nullable() == nullable
        && match result {
            Some(ty) => {
                actual.value_type().temporal() == Some(ty) && actual.data_type() == &ty.carrier()
            }
            None => {
                actual.data_type() == &DataType::Date32
                    && actual.logical_type() == ValueLogicalType::Physical
            }
        }
}

pub(crate) struct TemporalArithmeticResolver {
    operator: OperatorFunction,
    overloads: Vec<(Signature, FunctionOverloadId)>,
}

impl TemporalArithmeticResolver {
    fn operands(
        request: FunctionBindingRequest<'_>,
    ) -> Result<(&FunctionValueType, Option<&FunctionValueType>), FunctionBindingError> {
        let (left, right) = match request.arguments {
            [
                FunctionArgument::Value {
                    value_type: left, ..
                },
            ] => (left, None),
            [
                FunctionArgument::Value {
                    value_type: left, ..
                },
                FunctionArgument::Value {
                    value_type: right, ..
                },
            ] => (left, Some(right)),
            _ => return Err(FunctionBindingError::NoMatchingOverload),
        };
        if request.logical_argument_count != request.arguments.len() {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        Ok((left, right))
    }

    fn signature(
        &self,
        left: &FunctionValueType,
        right: Option<&FunctionValueType>,
    ) -> Result<Signature, FunctionBindingError> {
        let lhs = Family::of(left).ok_or(FunctionBindingError::NoMatchingOverload)?;
        let rhs = right
            .map(|ty| Family::of(ty).ok_or(FunctionBindingError::NoMatchingOverload))
            .transpose()?;
        // An integer division factor uses the reference DOUBLE overload.
        let rhs = match (self.operator, rhs) {
            (OperatorFunction::TemporalDivide, Some(Family::Bigint)) => Some(Family::Double),
            _ => rhs,
        };
        let signature = Signature(lhs, rhs);
        if !signature.accepts(self.operator) {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        Ok(signature)
    }

    fn select(
        &self,
        expected: Option<&FunctionOverloadId>,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            binding_control::request_types(request, work)?;
            let (left, right) = Self::operands(request)?;
            let coerced = match (self.operator, right) {
                (OperatorFunction::TemporalSubtract, Some(right)) => {
                    datetime_subtract_inputs(left, right)?
                }
                _ => None,
            };
            let (left, right) = match coerced.as_ref() {
                Some([left, right]) => (left, Some(right)),
                None => (left, right),
            };
            let signature = self.signature(left, right)?;
            let mut choice = None;
            for (actual, overload) in &self.overloads {
                work.step()?;
                if signature == *actual && expected.is_none_or(|id| id == overload) {
                    choice = Some(overload);
                    break;
                }
            }
            let overload = choice.ok_or(FunctionBindingError::NoMatchingOverload)?;
            let nullable = left.nullable() || right.is_some_and(FunctionValueType::nullable);
            let result = signature.result(self.operator, left, right)?;
            let mut arguments = vec![FunctionArgumentType::Value(signature.0.argument(left))];
            if let Some((family, operand)) = signature.1.zip(right) {
                arguments.push(FunctionArgumentType::Value(family.argument(operand)));
            }
            Ok(FunctionBindingSelection {
                overload: overload.clone(),
                argument_types: arguments.into(),
                result_type: FunctionResultType::Scalar(result_type(result, nullable)?),
                aggregate: None,
            })
        })
    }
}

impl FunctionBindingResolver for TemporalArithmeticResolver {
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
            let (left, right) = Self::operands(request)?;
            let signature = self.signature(left, right)?;
            let nullable = left.nullable() || right.is_some_and(FunctionValueType::nullable);
            let result = signature.result(self.operator, left, right)?;
            let arguments_match = match (selected.argument_types.as_ref(), right, signature.1) {
                ([FunctionArgumentType::Value(lhs)], None, None) => {
                    signature.0.matches_argument(lhs, left)
                }
                (
                    [
                        FunctionArgumentType::Value(lhs),
                        FunctionArgumentType::Value(rhs),
                    ],
                    Some(right),
                    Some(family),
                ) => signature.0.matches_argument(lhs, left) && family.matches_argument(rhs, right),
                _ => false,
            };
            let result_matches = matches!(&selected.result_type, FunctionResultType::Scalar(actual)
                if matches_result(actual, result, nullable));
            if selected.aggregate.is_none() && arguments_match && result_matches {
                for (choice, overload) in &self.overloads {
                    work.step()?;
                    if signature == *choice && selected.overload == *overload {
                        return Ok(());
                    }
                }
            }
            Err(FunctionBindingError::InvalidBinding(
                "Temporal arithmetic differs from its exact operands, result or overload".into(),
            ))
        })
    }
}

fn datetime_subtract_inputs(
    left: &FunctionValueType,
    right: &FunctionValueType,
) -> Result<Option<[FunctionValueType; 2]>, FunctionBindingError> {
    let datetime = |value: &FunctionValueType| match Family::of(value) {
        Some(Family::Date) => Some(TemporalType::Timestamp(TemporalPrecision::WHOLE_SECONDS)),
        Some(family) if family.datetime() => value.value_type().temporal(),
        _ => None,
    };
    let Some((lhs, rhs)) = datetime(left).zip(datetime(right)) else {
        return Ok(None);
    };
    let target = lhs
        .common_type(rhs)
        .ok_or(FunctionBindingError::NoMatchingOverload)?;
    let convert = |value: &FunctionValueType| {
        if value.value_type().temporal() == Some(target) {
            Ok(value.clone())
        } else {
            result_type(Some(target), value.nullable())
        }
    };
    if left.value_type().temporal() == Some(target) && right.value_type().temporal() == Some(target)
    {
        return Ok(None);
    }
    Ok(Some([convert(left)?, convert(right)?]))
}

pub(super) fn definition(
    operator: OperatorFunction,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let name = operator.catalog_name();
    let mut overloads = Vec::new();
    let mut declarations = Vec::new();
    let mut implementations = Vec::new();
    let implementation =
        PureImplementationId::try_new(format!("builtin.scalar/{name}/selected-v1"))?;
    for left in Family::ALL {
        for right in std::iter::once(None).chain(Family::ALL.into_iter().map(Some)) {
            let signature = Signature(left, right);
            if !signature.accepts(operator) {
                continue;
            }
            let args = match right {
                Some(right) => format!("{},{}", left.name(), right.name()),
                None => left.name().to_owned(),
            };
            let id = FunctionOverloadId::try_new(format!(
                "builtin.scalar/{name}/{}/v1",
                args.replace(',', "_"),
            ))
            .map_err(|error| owner_error("temporal arithmetic overload", &error))?;
            declarations.push(FunctionOverloadDeclaration::from_effects(
                id.clone(),
                args,
                "temporal arithmetic result",
                None,
                effects(signature.row_error(operator)),
            ));
            implementations.push(PureImplementationDeclaration {
                overload: id.clone(),
                implementation: implementation.clone(),
                abi: PureKernelAbi::ScalarV1,
            });
            overloads.push((signature, id));
        }
    }
    let declaration = FunctionBindingDeclaration::try_new_complete(
        operator.function_id(),
        FunctionKind::Scalar,
        declarations,
    )
    .map_err(|error| owner_error("temporal arithmetic declaration", &error))?;
    let owner = Arc::new(Owner {
        resolver: Arc::new(TemporalArithmeticResolver {
            operator,
            overloads,
        }),
        declaration,
        implementations: implementations.into_boxed_slice(),
    });
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Hidden, owner)
        .map_err(|error| owner_error("temporal arithmetic implementation", &error))
}

fn owner_error(subject: &'static str, error: &dyn std::fmt::Display) -> FunctionCatalogError {
    FunctionCatalogError::InvalidStableIdentity {
        subject,
        value: error.to_string().into(),
    }
}

fn effects(own_row_error: FunctionIntrinsicRowError) -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::Strict,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

struct Owner {
    resolver: Arc<TemporalArithmeticResolver>,
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
        FunctionBindingResolverOwner::borrowed_temporal_arithmetic(Arc::clone(&self.resolver))
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
                    "Temporal arithmetic requires its admitted proof and empty environment".into(),
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
                null_behavior: base.null_behavior,
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
                _ => invalid("Temporal arithmetic preparation rejected the frozen binding"),
            })?;
        if contract.function_id() != input.function_id
            || !std::ptr::eq(contract.selected(), input.selected)
            || CallEffectContext::from(contract.context()) != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || contract.effects() != &effects
        {
            return Err(invalid(
                "Temporal arithmetic preparation differs from its selected call",
            ));
        }
        Ok(Arc::new(Prepared {
            operator: self.resolver.operator,
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
    operator: OperatorFunction,
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
        Ok(Box::new(Instance(self.operator)))
    }
}

struct Instance(OperatorFunction);

struct Input<'a> {
    argument: EvaluatedArgument<'a>,
    rows: Source<'a>,
}

enum Source<'a> {
    Date(&'a Date32Array),
    Bigint(&'a Int64Array),
    Double(&'a Float64Array),
    Temporal(PreparedTemporalArray<'a>),
}

impl<'a> Input<'a> {
    fn bind(
        argument: EvaluatedArgument<'a>,
        ty: &FunctionValueType,
    ) -> Result<Self, KernelFailure> {
        let array = argument.array();
        let bad = || invalid("Temporal arithmetic argument differs from its frozen type");
        let rows = if let Some(temporal) = ty.value_type().temporal() {
            Source::Temporal(
                PreparedTemporalArray::bind(temporal, array.as_ref(), ty.nullable())
                    .map_err(|_| bad())?,
            )
        } else {
            match ty.data_type() {
                DataType::Date32 => Source::Date(array.as_any().downcast_ref().ok_or_else(bad)?),
                DataType::Int64 => Source::Bigint(array.as_any().downcast_ref().ok_or_else(bad)?),
                DataType::Float64 => Source::Double(array.as_any().downcast_ref().ok_or_else(bad)?),
                _ => return Err(bad()),
            }
        };
        Ok(Self { argument, rows })
    }

    fn at(&self, ordinal: usize, logical_row: usize) -> Result<Option<Value>, KernelFailure> {
        let row = self.argument.value_row(ordinal, logical_row);
        Ok(match &self.rows {
            Source::Date(array) => array.is_valid(row).then(|| Value::Date(array.value(row))),
            Source::Bigint(array) => array.is_valid(row).then(|| Value::Bigint(array.value(row))),
            Source::Double(array) => array.is_valid(row).then(|| Value::Double(array.value(row))),
            Source::Temporal(array) => array
                .value(row)
                .map_err(|_| invalid("Temporal arithmetic rejected a frozen operand"))?
                .map(Value::Temporal),
        })
    }
}

impl ScalarKernelInstance for Instance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        control.checkpoint(0)?;
        let (left, right) = match (
            input.arguments(),
            input.contract().selected().argument_types.as_ref(),
        ) {
            ([left], [FunctionArgumentType::Value(lt)]) => (Input::bind(*left, lt)?, None),
            (
                [left, right],
                [
                    FunctionArgumentType::Value(lt),
                    FunctionArgumentType::Value(rt),
                ],
            ) => (Input::bind(*left, lt)?, Some(Input::bind(*right, rt)?)),
            _ => {
                return Err(invalid(
                    "Temporal arithmetic argument arity differs from its selection",
                ));
            }
        };
        let selection = input.selection();
        let result = input.contract().result_type();
        let temporal = result.value_type().temporal();
        let mut work = EvaluationCheckpoints::new(control);
        let mut output = TemporalOutputBuilder::new(
            temporal.map(Type::Temporal).unwrap_or(Type::Date),
            selection.len(),
        )?;
        let mut errors = Vec::new();
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let Some(lhs) = left.at(ordinal, row)? else {
                output.null();
                continue;
            };
            let rhs = match &right {
                Some(input) => match input.at(ordinal, row)? {
                    Some(rhs) => Some(rhs),
                    None => {
                        output.null();
                        continue;
                    }
                },
                None => None,
            };
            match evaluate(self.0, temporal, lhs, rhs) {
                Ok(Value::Date(day)) => output.date(day)?,
                Ok(Value::Temporal(value)) => output.temporal(
                    temporal.ok_or_else(|| invalid("Expected DATE output"))?,
                    value,
                )?,
                Ok(_) => return Err(invalid("Temporal arithmetic returned a foreign result")),
                Err(cause) => {
                    work.flush()?;
                    errors
                        .try_reserve(1)
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    errors.push(RowDataError::from_domain(ordinal, cause)?);
                    output.null();
                }
            }
        }
        work.flush()?;
        let output = output.finish();
        let selected = SelectedValues::try_new_observed(
            selection,
            result.data_type(),
            output,
            errors.into_boxed_slice(),
            || work.step(),
        );
        work.finish_result(selected)
    }
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
