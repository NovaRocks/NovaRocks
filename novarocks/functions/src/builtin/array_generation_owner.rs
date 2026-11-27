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

//! Binding and hosted evaluation for Trino repeat and scalar sequence.

use std::sync::Arc;

use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffectContext, CallEffects, CallProofScope, FunctionEffectDeclaration,
    FunctionInstanceState, FunctionNullBehavior, IntervalField, IntervalQualifier,
    ObservableEffects, PureCompileControl, TemporalPrecision, TemporalType, ValueKind,
    trino_can_coerce_scalar, trino_common_scalar_kind,
};

use super::{
    array_generation::{self, Operation},
    binding_control,
    catalogue::{BuiltinScalarResolver, public_signature_error},
    date_shift_owner::temporal_argument,
    scalar_construction::{ScalarConstruction, pure_value},
    signature::declared_list_exact_with_construction,
};
use crate::{
    AggregateStateAllocator, CallEffectInput, CheckedCallEffectInput, CheckedOwnerFailure,
    FunctionArgument, FunctionArgumentType, FunctionBindingDeclaration, FunctionBindingError,
    FunctionBindingRequest, FunctionBindingResolver, FunctionBindingResolverOwner,
    FunctionBindingSelection, FunctionCatalogError, FunctionDefinition, FunctionEffectOwner,
    FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError,
    FunctionKind, FunctionOverloadDeclaration, FunctionOverloadId, FunctionPublicSignature,
    FunctionResultType, FunctionValueType, FunctionVisibility, FunctionVolatility,
    KernelEvaluationControl, KernelFailure, PreparedScalarKernel, PureFunctionMetadataOwner,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, PureScalarImplementation,
    ScalarCallContract, ScalarCallInput, ScalarKernelInstance, SelectedValues,
    aggregate_host_allocator::HostAggregateAllocator,
    kernel_control::{compile_failure, invalid},
};

const BIGINT: ValueKind = ValueKind::Scalar(DataType::Int64);
const DATE: ValueKind = ValueKind::Scalar(DataType::Date32);
const UNKNOWN: ValueKind = ValueKind::Scalar(DataType::Null);

#[derive(Clone, Copy)]
enum SequenceOverload {
    Bigint = 0,
    BigintStep,
    Date,
    DateDayTime,
    DateYearMonth,
    TimestampDayTime,
    TimestampYearMonth,
}

impl SequenceOverload {
    const ALL: [Self; 7] = [
        Self::Bigint,
        Self::BigintStep,
        Self::Date,
        Self::DateDayTime,
        Self::DateYearMonth,
        Self::TimestampDayTime,
        Self::TimestampYearMonth,
    ];

    const fn identity(self) -> &'static str {
        match self {
            Self::Bigint => "builtin.scalar/sequence/(i64,i64)->list<i64>;strict;legacy",
            Self::BigintStep => "builtin.scalar/sequence/(i64,i64,i64)->list<i64>;strict;legacy",
            Self::Date => "builtin.scalar/sequence/(date,date)->list<date>;strict;legacy",
            Self::DateDayTime => "builtin.scalar/sequence/date-day-time/v1",
            Self::DateYearMonth => "builtin.scalar/sequence/date-year-month/v1",
            Self::TimestampDayTime => "builtin.scalar/sequence/timestamp-day-time/v1",
            Self::TimestampYearMonth => "builtin.scalar/sequence/timestamp-year-month/v1",
        }
    }

    const fn signature(self) -> (&'static str, &'static str) {
        match self {
            Self::Bigint => ("bigint, bigint", "array(bigint)"),
            Self::BigintStep => ("bigint, bigint, bigint", "array(bigint)"),
            Self::Date => ("date, date", "array(date)"),
            Self::DateDayTime => ("date, date, interval day(q) to second(r)", "array(date)"),
            Self::DateYearMonth => ("date, date, interval year(q) to month", "array(date)"),
            Self::TimestampDayTime => (
                "timestamp(p), timestamp(p), interval day(q) to second(r)",
                "array(timestamp(max(p,r)))",
            ),
            Self::TimestampYearMonth => (
                "timestamp(p), timestamp(p), interval year(q) to month",
                "array(timestamp(p))",
            ),
        }
    }
}

struct SequenceShape {
    overload: SequenceOverload,
    endpoint: ValueKind,
    step: Option<ValueKind>,
    element: ValueKind,
}

fn value(argument: &FunctionArgument) -> Result<&FunctionValueType, FunctionBindingError> {
    match argument {
        FunctionArgument::Value { value_type, .. } => Ok(value_type),
        _ => Err(FunctionBindingError::NoMatchingOverload),
    }
}

fn generation_argument(
    original: &FunctionValueType,
    target: &ValueKind,
) -> Result<FunctionValueType, FunctionBindingError> {
    if target == &BIGINT && original.value_type().kind() != target {
        return Ok(FunctionValueType::new(DataType::Int64, original.nullable()));
    }
    temporal_argument(original, target)
}

impl SequenceShape {
    fn infer(request: FunctionBindingRequest<'_>) -> Result<Self, FunctionBindingError> {
        if request.arguments.len() != request.logical_argument_count {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        let (start, stop, step) = match request.arguments {
            [start, stop] => (value(start)?, value(stop)?, None),
            [start, stop, step] => (value(start)?, value(stop)?, Some(value(step)?)),
            _ => return Err(FunctionBindingError::NoMatchingOverload),
        };
        let start_kind = start.value_type().kind();
        let stop_kind = stop.value_type().kind();
        let bigint_endpoints = trino_can_coerce_scalar(start_kind, &BIGINT)
            && trino_can_coerce_scalar(stop_kind, &BIGINT);
        let endpoint = trino_common_scalar_kind(start_kind, stop_kind)
            .ok_or(FunctionBindingError::NoMatchingOverload)?;
        let step_kind = step.map(|step| step.value_type().kind());
        if step_kind.is_none() && endpoint == UNKNOWN {
            return Err(FunctionBindingError::AmbiguousOverload);
        }
        if bigint_endpoints && step_kind.is_none_or(|kind| trino_can_coerce_scalar(kind, &BIGINT)) {
            if endpoint == UNKNOWN && step_kind == Some(&UNKNOWN) {
                return Err(FunctionBindingError::AmbiguousOverload);
            }
            return Ok(Self {
                overload: if step.is_some() {
                    SequenceOverload::BigintStep
                } else {
                    SequenceOverload::Bigint
                },
                endpoint: BIGINT,
                step: step.map(|_| BIGINT),
                element: BIGINT,
            });
        }
        let endpoint = if endpoint == UNKNOWN { DATE } else { endpoint };
        if step.is_none() && endpoint == DATE {
            return Ok(Self {
                overload: SequenceOverload::Date,
                endpoint: DATE,
                step: None,
                element: DATE,
            });
        }
        let interval = match step_kind {
            Some(ValueKind::Temporal(TemporalType::Interval(qualifier))) => *qualifier,
            Some(kind) if *kind == UNKNOWN && endpoint == DATE => {
                // Both interval families return DATE and a strict NULL step
                // cannot reach either body. Choose the canonical month form.
                IntervalQualifier::new(
                    IntervalField::Year,
                    IntervalField::Month,
                    IntervalField::Year.max_leading_precision(),
                    TemporalPrecision::WHOLE_SECONDS,
                )
                .map_err(|_| FunctionBindingError::NoMatchingOverload)?
            }
            Some(kind)
                if *kind == UNKNOWN
                    && matches!(&endpoint, ValueKind::Temporal(TemporalType::Timestamp(_))) =>
            {
                return Err(FunctionBindingError::AmbiguousOverload);
            }
            _ => return Err(FunctionBindingError::NoMatchingOverload),
        };
        let (overload, element) = match (&endpoint, interval.is_year_month()) {
            (kind, true) if *kind == DATE => (SequenceOverload::DateYearMonth, DATE),
            (kind, false) if *kind == DATE => (SequenceOverload::DateDayTime, DATE),
            (ValueKind::Temporal(TemporalType::Timestamp(precision)), true) => (
                SequenceOverload::TimestampYearMonth,
                ValueKind::Temporal(TemporalType::Timestamp(*precision)),
            ),
            (ValueKind::Temporal(TemporalType::Timestamp(precision)), false) => (
                SequenceOverload::TimestampDayTime,
                ValueKind::Temporal(TemporalType::Timestamp(
                    (*precision).max(interval.fractional_precision()),
                )),
            ),
            _ => return Err(FunctionBindingError::NoMatchingOverload),
        };
        Ok(Self {
            overload,
            endpoint,
            step: Some(ValueKind::Temporal(TemporalType::Interval(interval))),
            element,
        })
    }
}

pub(crate) struct SequenceResolver {
    overloads: Vec<FunctionOverloadId>,
}

impl SequenceResolver {
    fn select(
        &self,
        expected: Option<&FunctionOverloadId>,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            binding_control::request_types(request, work)?;
            let shape = SequenceShape::infer(request)?;
            let overload = &self.overloads[shape.overload as usize];
            if expected.is_some_and(|expected| expected != overload) {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let mut nullable = false;
            let mut arguments = Vec::with_capacity(request.arguments.len());
            for (index, argument) in request.arguments.iter().enumerate() {
                work.step()?;
                let original = value(argument)?;
                let target = if index == 2 {
                    shape
                        .step
                        .as_ref()
                        .ok_or(FunctionBindingError::NoMatchingOverload)?
                } else {
                    &shape.endpoint
                };
                nullable |= original.nullable();
                arguments.push(FunctionArgumentType::Value(generation_argument(
                    original, target,
                )?));
            }
            let exemplar = FunctionValueType::new(DataType::Date32, false);
            let element = generation_argument(&exemplar, &shape.element)?;
            let storage = novarocks_type_contract::ValueTypeStorageLifetime::pure();
            let mut construction =
                ScalarConstruction::<std::convert::Infallible>::new(storage, None);
            let result =
                declared_list_exact_with_construction(&element, nullable, &mut construction)
                    .map_err(pure_value)?;
            Ok(FunctionBindingSelection {
                overload: overload.clone(),
                argument_types: arguments.into(),
                result_type: FunctionResultType::Scalar(result),
                aggregate: None,
            })
        })
    }
}

impl FunctionBindingResolver for SequenceResolver {
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
        let stale = || {
            FunctionBindingError::InvalidBinding(
                "sequence differs from its frozen argument or result type".into(),
            )
        };
        binding_control::scope(control, |work| {
            binding_control::request_types(request, work)?;
            let shape = SequenceShape::infer(request)?;
            if selected.overload != self.overloads[shape.overload as usize]
                || selected.aggregate.is_some()
                || selected.argument_types.len() != request.arguments.len()
            {
                return Err(stale());
            }
            let mut nullable = false;
            for (index, (original, selected)) in request
                .arguments
                .iter()
                .zip(selected.argument_types.iter())
                .enumerate()
            {
                let original = value(original)?;
                nullable |= original.nullable();
                let FunctionArgumentType::Value(selected) = selected else {
                    return Err(stale());
                };
                let expected = if index == 2 {
                    shape.step.as_ref()
                } else {
                    Some(&shape.endpoint)
                };
                work.step()?;
                if expected != Some(selected.value_type().kind())
                    || !binding_control::exact_type(original, selected, work)?
                {
                    return Err(stale());
                }
            }
            let FunctionResultType::Scalar(result) = &selected.result_type else {
                return Err(stale());
            };
            let (ValueKind::List(element), DataType::List(field)) =
                (result.value_type().kind(), result.data_type())
            else {
                return Err(stale());
            };
            if element.kind() != &shape.element
                || nullable != result.nullable()
                || element.nullable()
                || field.is_nullable()
                || field.name() != "item"
            {
                return Err(stale());
            }
            Ok(())
        })
    }
}

pub(super) fn effects(name: &str) -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::MayRaise,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: if name == "repeat" {
            FunctionNullBehavior::StrictPositions(Box::new([1]))
        } else {
            FunctionNullBehavior::Strict
        },
        argument_control: ArgumentControl::OrderedNull,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

fn catalog_error(subject: &'static str, error: &dyn std::fmt::Display) -> FunctionCatalogError {
    FunctionCatalogError::InvalidStableIdentity {
        subject,
        value: error.to_string().into(),
    }
}

pub(super) fn repeat_definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    if name != "repeat" || declaration.overloads().len() != 1 {
        return Err(catalog_error("builtin repeat declaration", &name));
    }
    definition(
        name,
        Resolver::Repeat(Arc::new(resolver)),
        Operation::Repeat,
        declaration,
    )
}

pub(super) fn sequence_definition() -> Result<FunctionDefinition, FunctionCatalogError> {
    let mut signatures = Vec::new();
    let mut declarations = Vec::new();
    let mut overloads = Vec::new();
    for shape in SequenceOverload::ALL {
        let id = FunctionOverloadId::try_new(shape.identity())
            .map_err(|error| catalog_error("builtin sequence overload", &error))?;
        let (arguments, result) = shape.signature();
        signatures.push((
            id.clone(),
            vec![FunctionPublicSignature::new(arguments, result, "")],
        ));
        declarations.push(FunctionOverloadDeclaration::from_effects(
            id.clone(),
            arguments,
            result,
            None,
            effects("sequence"),
        ));
        overloads.push(id);
    }
    let function = FunctionId::try_new("builtin.scalar/sequence/v1")
        .map_err(|error| catalog_error("builtin sequence function", &error))?;
    let declaration =
        FunctionBindingDeclaration::try_new_complete(function, FunctionKind::Scalar, declarations)
            .map_err(|error| catalog_error("builtin sequence declaration", &error))?
            .with_public_signatures(signatures)
            .map_err(public_signature_error)?;
    definition(
        "sequence",
        Resolver::Sequence(Arc::new(SequenceResolver { overloads })),
        Operation::Sequence,
        declaration,
    )
}

fn definition(
    name: &str,
    resolver: Resolver,
    operation: Operation,
    declaration: FunctionBindingDeclaration,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let effect = effects(name);
    if declaration.function_id().as_str() != format!("builtin.scalar/{name}/v1")
        || declaration.kind() != FunctionKind::Scalar
        || declaration.overloads().iter().any(|overload| {
            overload.aggregate.is_some() || overload.effects.as_ref() != Some(&effect)
        })
    {
        return Err(catalog_error("builtin array generation declaration", &name));
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
    let owner = Arc::new(Owner {
        resolver,
        operation,
        declaration,
        implementations,
    });
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Public, owner)
        .map_err(|error| catalog_error("builtin array generator implementation", &error))
}

enum Resolver {
    Repeat(Arc<BuiltinScalarResolver>),
    Sequence(Arc<SequenceResolver>),
}

impl Resolver {
    fn author(&self) -> &dyn FunctionBindingResolver {
        match self {
            Self::Repeat(resolver) => resolver.as_ref(),
            Self::Sequence(resolver) => resolver.as_ref(),
        }
    }
}

struct Owner {
    resolver: Resolver,
    operation: Operation,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}

impl FunctionBindingResolver for Owner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver.author().resolve(request, control)
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver
            .author()
            .select_at_overload_observed(overload, request, control)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.resolver
            .author()
            .validate_selected(selected, request, control)
    }
}

impl PureFunctionMetadataOwner for Owner {
    fn binding_resolver_owner(self: Arc<Self>) -> FunctionBindingResolverOwner {
        match &self.resolver {
            Resolver::Repeat(resolver) => {
                FunctionBindingResolverOwner::fixed_scalar(Arc::clone(resolver))
            }
            Resolver::Sequence(resolver) => {
                FunctionBindingResolverOwner::borrowed_array_sequence(Arc::clone(resolver))
            }
        }
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
                    "array generation requires its admitted proof and empty environment".into(),
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
                _ => invalid("array generation preparation rejected the frozen binding"),
            })?;
        if contract.function_id() != input.function_id
            || !std::ptr::eq(contract.selected(), input.selected)
            || CallEffectContext::from(contract.context()) != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || contract.effects() != &effects
        {
            return Err(invalid(
                "array generation preparation differs from its selected call",
            ));
        }
        Ok(Arc::new(Prepared {
            operation: self.operation,
            contract,
        }))
    }
}

fn effect_error(error: FunctionBindingError) -> FunctionEffectOwnerError<FunctionBindingError> {
    match error {
        FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
        other => FunctionEffectOwnerError::Owner(other),
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
            self.validate_selected(input.selected, input.request, control)
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
            checked.require_binding_author(self.resolver.author(), input)
        })
        .map_err(effect_error)
        .map_err(CheckedOwnerFailure::Original)
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
            self.validate_selected(input.selected, input.request, control)
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
            checked.require_binding_author(self.resolver.author(), input)
        })
        .map_err(CheckedOwnerFailure::Original)
    }
}

#[derive(Debug)]
struct Prepared {
    operation: Operation,
    contract: Arc<ScalarCallContract>,
}

impl PreparedScalarKernel for Prepared {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        size_of::<Instance>() + HostAggregateAllocator::metadata_allocation_bytes()
    }

    fn instance_inline_allocation_bytes(&self) -> usize {
        size_of::<Instance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Err(invalid(
            "array generation requires its scalar allocation host",
        ))
    }

    fn create_instance_with_allocator(
        &self,
        host: Option<Arc<dyn AggregateStateAllocator>>,
    ) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        let host = host
            .filter(|host| host.opaque_allocation_host().is_some())
            .ok_or_else(|| invalid("array generation requires its scalar allocation host"))?;
        let allocator = HostAggregateAllocator::try_new(Arc::clone(&host))?;
        Ok(Box::new(Instance {
            operation: self.operation,
            host,
            allocator,
        }))
    }
}

struct Instance {
    operation: Operation,
    host: Arc<dyn AggregateStateAllocator>,
    allocator: HostAggregateAllocator,
}

impl ScalarKernelInstance for Instance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        array_generation::evaluate(self.operation, input, control, &self.host, &self.allocator)
    }

    fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.allocator.metadata_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CallArgumentUses, EvaluatedArgument, PreparedPureKernel, PureCallPreparation,
        ScalarEvaluationInstance, ScopedExpressionEffects, Selection, binding_test_control,
        builtin::{
            array_invocation_test_support::{Control, Host},
            catalogue::build_builtin_engine_function_catalog,
        },
    };
    use arrow_array::{Array, ArrayRef, Int64Array, ListArray};
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_type_contract::{
        DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext,
        ExpressionUseId, SemanticParameters,
    };

    fn argument(value_type: FunctionValueType) -> FunctionArgument {
        FunctionArgument::Value {
            value_type,
            constant: None,
        }
    }

    fn prepared(name: &str, arguments: &[FunctionArgument]) -> Arc<dyn PreparedScalarKernel> {
        let control = binding_test_control();
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let request = FunctionBindingRequest {
            arguments,
            logical_argument_count: arguments.len(),
            expected_result_type: None,
        };
        let bound = catalog
            .resolve_bound_user(name, FunctionKind::Scalar, request, control)
            .unwrap();
        let selected = Arc::new(bound.selected);
        let uses: Vec<_> = (1..=arguments.len() as u32)
            .map(|id| Some(ExpressionUseId::new(id)))
            .collect();
        let context = ExpressionEffectContext {
            use_id: ExpressionUseId::new(0),
            region: EvaluationRegionId::new(0),
            demand: EvaluationDemand::Value,
        };
        let parameters = SemanticParameters::try_new([]).unwrap();
        let prepared = catalog
            .prepare_fresh_selected(
                CallEffectInput {
                    context: context.into(),
                    argument_uses: CallArgumentUses::SelectedChannels(&uses),
                    function_id: &bound.function_id,
                    kind: FunctionKind::Scalar,
                    selected: &selected,
                    request,
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                    proof_scope: CallProofScope::Unconditional,
                },
                Arc::clone(&selected),
                PureCallPreparation::Scalar {
                    arguments: ScopedExpressionEffects::pure_value(context),
                },
                control,
            )
            .unwrap()
            .into_prepared();
        let PreparedPureKernel::Scalar(prepared) = prepared else {
            panic!("expected scalar generator");
        };
        prepared
    }

    fn instance(
        prepared: Arc<dyn PreparedScalarKernel>,
        host: &Arc<Host>,
    ) -> ScalarEvaluationInstance {
        ScalarEvaluationInstance::instantiate_with_allocator(
            prepared,
            Some(Arc::clone(host) as Arc<dyn AggregateStateAllocator>),
        )
        .unwrap()
    }

    #[test]
    fn array_generation_repeat_observes_selected_rows_and_list_extent() {
        let host = Host::with_limit(usize::MAX);
        let types = [
            argument(FunctionValueType::new(DataType::Int64, true)),
            argument(FunctionValueType::new(DataType::Int64, true)),
        ];
        let prepared = prepared("repeat", &types);
        let value: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        let counts: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(-1),
            Some(2),
            None,
            Some(100_001),
        ]));
        let inputs = [
            EvaluatedArgument::Scalar(&value),
            EvaluatedArgument::Column(&counts),
        ];
        {
            let mut engine = instance(Arc::clone(&prepared), &host);
            let selection = Selection::try_sparse(4, &[1, 2, 3]).unwrap();
            let (_, result, errors) = engine
                .evaluate(selection, &inputs, &Control::default())
                .unwrap()
                .into_parts();
            let arrays = result.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(arrays.value(0).null_count(), 2);
            assert_eq!(arrays.value(0).len(), 2);
            assert!(arrays.is_null(1));
            assert!(arrays.is_null(2));
            assert_eq!(errors.len(), 1);
            assert_eq!(errors[0].selected_ordinal(), 2);
            assert_eq!(
                errors[0].reason(),
                crate::DataFailureReason::Domain(
                    novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT
                )
            );
        }
        host.released();
        {
            let count: ArrayRef = Arc::new(Int64Array::from(vec![100_000]));
            let inputs = [
                EvaluatedArgument::Scalar(&value),
                EvaluatedArgument::Scalar(&count),
            ];
            let mut engine = instance(prepared, &host);
            let rows = i32::MAX as usize / 100_000 + 1;
            let cause = engine
                .evaluate(Selection::all(rows), &inputs, &Control::default())
                .unwrap_err();
            assert_eq!(cause.failure_fact().class(), FailureClass::Representation);
        }
        host.released();
    }

    #[test]
    fn array_generation_sequence_uses_exact_temporal_child_and_host_refusal() {
        use super::super::temporal_output::TemporalOutputBuilder;
        use crate::temporal_cast::Type;
        use novarocks_type_contract::{TemporalMicros, TemporalValue};
        use novarocks_value_arrays::decode::PreparedTemporalArray;

        let host = Host::with_limit(usize::MAX);
        let p = TemporalPrecision::WHOLE_SECONDS;
        let r = TemporalPrecision::PICOSECONDS;
        let stamp = TemporalType::Timestamp(p);
        let qualifier =
            IntervalQualifier::new(IntervalField::Second, IntervalField::Second, 2, r).unwrap();
        let interval = TemporalType::Interval(qualifier);
        let value_type = |ty| {
            generation_argument(
                &FunctionValueType::new(DataType::Date32, false),
                &ValueKind::Temporal(ty),
            )
            .unwrap()
        };
        let types = [
            argument(value_type(stamp)),
            argument(value_type(stamp)),
            argument(value_type(interval)),
        ];
        let prepared = prepared("sequence", &types);
        let result = prepared.contract().result_type();
        let ValueKind::List(child) = result.value_type().kind() else {
            panic!("expected List")
        };
        assert_eq!(child.temporal(), Some(TemporalType::Timestamp(r)));

        let stamp_at = |seconds: i64| {
            let mut output = TemporalOutputBuilder::new(Type::Temporal(stamp), 1).unwrap();
            output
                .temporal(
                    stamp,
                    TemporalValue::Timestamp(TemporalMicros::whole_micros(seconds * 1_000_000)),
                )
                .unwrap();
            output.finish()
        };
        let start = stamp_at(-1);
        let stop = stamp_at(1);
        let mut values = TemporalOutputBuilder::new(Type::Temporal(interval), 1).unwrap();
        values
            .temporal(
                interval,
                TemporalValue::IntervalDayTime(TemporalMicros::new(999_999, 999_999).unwrap()),
            )
            .unwrap();
        let step = values.finish();
        let inputs = [
            EvaluatedArgument::Scalar(&start),
            EvaluatedArgument::Scalar(&stop),
            EvaluatedArgument::Column(&step),
        ];
        {
            let mut engine = instance(Arc::clone(&prepared), &host);
            let (_, result, errors) = engine
                .evaluate(Selection::all(1), &inputs, &Control::default())
                .unwrap()
                .into_parts();
            assert!(errors.is_empty());
            let arrays = result.as_any().downcast_ref::<ListArray>().unwrap();
            let values = arrays.value(0);
            let view =
                PreparedTemporalArray::bind(TemporalType::Timestamp(r), values.as_ref(), false)
                    .unwrap();
            assert_eq!(values.len(), 3);
            assert_eq!(
                view.value(1).unwrap(),
                Some(TemporalValue::Timestamp(
                    TemporalMicros::new(-1, 999_999).unwrap()
                ))
            );
        }
        host.released();

        {
            let mut engine = instance(prepared, &host);
            *host.refusal.lock().unwrap() =
                Some((host.attempts(), KernelFailure::ResourceExhausted));
            let cause = engine
                .evaluate(Selection::all(1), &inputs, &Control::default())
                .unwrap_err();
            assert_eq!(cause, KernelFailure::ResourceExhausted);
        }
        host.released();
    }
}
