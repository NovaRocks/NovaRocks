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
    ValueTypeStorageLifetime, trino_can_coerce_scalar,
};
use novarocks_value_arrays::decode::PreparedTemporalArray;

use super::{
    binding_control,
    date_shift::{ShiftError, Unit, add},
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
    temporal_cast::Type,
};

const NAME: &str = "date_add";
const FAMILIES: [&str; 5] = [
    "date",
    "time",
    "time_with_time_zone",
    "timestamp",
    "timestamp_with_time_zone",
];
const INVALID_SELECTION: &str = "date_add differs from its selected argument or result type";

pub(crate) struct DateShiftResolver {
    overloads: Vec<FunctionOverloadId>,
}

fn family_index(stamp: &FunctionValueType) -> Result<usize, FunctionBindingError> {
    match stamp.value_type().kind() {
        ValueKind::Scalar(DataType::Date32) => Ok(0),
        ValueKind::Temporal(TemporalType::Time(_)) => Ok(1),
        ValueKind::Temporal(TemporalType::TimeWithTimeZone(_)) => Ok(2),
        ValueKind::Temporal(TemporalType::Timestamp(_)) => Ok(3),
        ValueKind::Temporal(TemporalType::TimestampWithTimeZone(_)) => Ok(4),
        _ => Err(FunctionBindingError::NoMatchingOverload),
    }
}

fn is_varchar(ty: &FunctionValueType) -> bool {
    ty.data_type() == &DataType::Utf8
        && match ty.value_type().kind() {
            ValueKind::String => true,
            ValueKind::BoundedCharacter(character) => character.kind() == CharacterKind::Varchar,
            _ => false,
        }
}

fn varchar_argument(ty: &FunctionValueType) -> Result<FunctionValueType, FunctionBindingError> {
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

fn operands(
    request: FunctionBindingRequest<'_>,
) -> Result<[&FunctionValueType; 3], FunctionBindingError> {
    match request.arguments {
        [
            FunctionArgument::Value {
                value_type: unit, ..
            },
            FunctionArgument::Value {
                value_type: offset, ..
            },
            FunctionArgument::Value {
                value_type: stamp, ..
            },
        ] if request.logical_argument_count == 3 => Ok([unit, offset, stamp]),
        _ => Err(FunctionBindingError::NoMatchingOverload),
    }
}

impl DateShiftResolver {
    fn select(
        &self,
        expected: Option<&FunctionOverloadId>,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            binding_control::request_types(request, work)?;
            let arguments @ [unit, offset, stamp] = operands(request)?;
            let overload = &self.overloads[family_index(stamp)?];
            if expected.is_some_and(|expected| expected != overload)
                || !trino_can_coerce_scalar(
                    offset.value_type().kind(),
                    &ValueKind::Scalar(DataType::Int64),
                )
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let nullable = arguments.iter().any(|arg| arg.nullable());
            let result = stamp.with_nullable(nullable).map_err(binding_error)?;
            let unit = varchar_argument(unit)?;
            let offset = FunctionValueType::new(DataType::Int64, offset.nullable());
            Ok(FunctionBindingSelection {
                overload: overload.clone(),
                argument_types: [unit, offset, stamp.clone()]
                    .map(FunctionArgumentType::Value)
                    .into(),
                result_type: FunctionResultType::Scalar(result),
                aggregate: None,
            })
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
            let arguments @ [unit, offset, stamp] = operands(request)?;
            let index = family_index(stamp)?;
            let [
                FunctionArgumentType::Value(declared_unit),
                FunctionArgumentType::Value(declared_offset),
                FunctionArgumentType::Value(declared_stamp),
            ] = selected.argument_types.as_ref()
            else {
                return Err(FunctionBindingError::InvalidBinding(
                    INVALID_SELECTION.into(),
                ));
            };
            let FunctionResultType::Scalar(result) = &selected.result_type else {
                return Err(FunctionBindingError::InvalidBinding(
                    INVALID_SELECTION.into(),
                ));
            };
            if !is_varchar(unit)
                || offset.value_type().kind() != &ValueKind::Scalar(DataType::Int64)
                || unit != declared_unit
                || offset != declared_offset
                || stamp != declared_stamp
                || selected.aggregate.is_some()
                || selected.overload != self.overloads[index]
                || result.nullable() != arguments.iter().any(|arg| arg.nullable())
                || !stamp
                    .same_physical_binding_ignoring_root_nullability_observed(result, || {
                        work.step()
                    })?
            {
                return Err(FunctionBindingError::InvalidBinding(
                    INVALID_SELECTION.into(),
                ));
            }
            Ok(())
        })
    }
}

pub(super) fn definition() -> Result<FunctionDefinition, FunctionCatalogError> {
    let implementation = PureImplementationId::try_new("builtin.scalar/date_add/selected-v1")?;
    let mut overloads = Vec::new();
    let mut declarations = Vec::new();
    let mut implementations = Vec::new();
    for family in FAMILIES {
        let id = FunctionOverloadId::try_new(format!("builtin.scalar/date_add/{family}/v1"))
            .map_err(|error| owner_error("date_add overload", &error))?;
        let temporal = if family == "date" {
            "date".to_owned()
        } else {
            format!("{family}(p)")
        };
        declarations.push(FunctionOverloadDeclaration::from_effects(
            id.clone(),
            format!("varchar(x),bigint,{temporal}"),
            temporal,
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
    let function = FunctionId::try_new("builtin.scalar/date_add/v1")
        .map_err(|error| owner_error("date_add function", &error))?;
    let declaration =
        FunctionBindingDeclaration::try_new_complete(function, FunctionKind::Scalar, declarations)
            .map_err(|error| owner_error("date_add declaration", &error))?;
    FunctionDefinition::try_new_pure_scalar(
        NAME,
        FunctionVisibility::Public,
        Arc::new(Owner {
            resolver: Arc::new(DateShiftResolver { overloads }),
            declaration,
            implementations: implementations.into_boxed_slice(),
        }),
    )
    .map_err(|error| owner_error("date_add implementation", &error))
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
        argument_control: ArgumentControl::Eager,
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
                    "date_add requires its admitted proof and empty environment".into(),
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
                _ => invalid("date_add preparation rejected the frozen binding"),
            })?;
        if contract.function_id() != input.function_id
            || !std::ptr::eq(contract.selected(), input.selected)
            || CallEffectContext::from(contract.context()) != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || contract.effects() != &effects
        {
            return Err(invalid(
                "date_add preparation differs from its selected call",
            ));
        }
        Ok(Arc::new(Prepared {
            unit: PreparedUnit::new(input.request, control)?,
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
    unit: PreparedUnit,
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
        Ok(Box::new(Instance(self.unit)))
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
                _ => invalid("date_add unit constant differs from its frozen binding"),
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

struct Instance(PreparedUnit);

enum Source<'a> {
    Date(&'a Date32Array),
    Temporal(PreparedTemporalArray<'a>),
}

impl<'a> Source<'a> {
    fn bind(ty: &FunctionValueType, array: &'a dyn Array) -> Result<Self, KernelFailure> {
        let bad = || invalid("date_add operand differs from its frozen temporal type");
        if let Some(temporal) = ty.value_type().temporal() {
            let source =
                PreparedTemporalArray::bind(temporal, array, ty.nullable()).map_err(|_| bad())?;
            Ok(Self::Temporal(source))
        } else {
            Ok(Self::Date(array.as_any().downcast_ref().ok_or_else(bad)?))
        }
    }

    fn at(&self, row: usize) -> Result<Option<Value>, KernelFailure> {
        match self {
            Self::Date(array) => Ok(array.is_valid(row).then(|| Value::Date(array.value(row)))),
            Self::Temporal(array) => array
                .value(row)
                .map(|value| value.map(Value::Temporal))
                .map_err(|_| invalid("date_add rejected a frozen temporal operand")),
        }
    }
}

impl ScalarKernelInstance for Instance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        control.checkpoint(0)?;
        let [unit_arg, offset_arg, stamp_arg] = input.arguments() else {
            return Err(invalid(
                "date_add argument count differs from its selection",
            ));
        };
        let [
            FunctionArgumentType::Value(unit_type),
            FunctionArgumentType::Value(offset_type),
            FunctionArgumentType::Value(stamp_type),
        ] = input.contract().selected().argument_types.as_ref()
        else {
            return Err(invalid(
                "date_add argument count differs from its frozen contract",
            ));
        };
        let selection = input.selection();
        for (argument, ty) in [
            (unit_arg, unit_type),
            (offset_arg, offset_type),
            (stamp_arg, stamp_type),
        ] {
            validate_argument_observed(*argument, selection, ty, control)?;
        }
        let result = input.contract().result_type();
        let temporal = result.value_type().temporal();
        let text = unit_arg
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("date_add unit is not UTF8"))?;
        let offset = offset_arg
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("date_add offset is not BIGINT"))?;
        let stamp = Source::bind(stamp_type, stamp_arg.array().as_ref())?;
        let mut work = EvaluationCheckpoints::new(control);
        let mut output = TemporalOutputBuilder::new(
            temporal.map(Type::Temporal).unwrap_or(Type::Date),
            selection.len(),
        )?;
        let mut errors = Vec::new();
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let text_row = unit_arg.value_row(ordinal, row);
            let offset_row = offset_arg.value_row(ordinal, row);
            if text.is_null(text_row) || offset.is_null(offset_row) {
                output.null();
                continue;
            }
            let Some(stamp_value) = stamp.at(stamp_arg.value_row(ordinal, row))? else {
                output.null();
                continue;
            };
            match self
                .0
                .parse(text.value(text_row))
                .and_then(|unit| add(unit, offset.value(offset_row), temporal, stamp_value))
            {
                Ok(Value::Date(days)) => output.date(days)?,
                Ok(Value::Temporal(value)) => output.temporal(
                    temporal.ok_or_else(|| invalid("date_add expected DATE output"))?,
                    value,
                )?,
                Ok(_) => return Err(invalid("date_add returned a foreign value")),
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
