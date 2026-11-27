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

//! Actual window signature, ordered channels and explicit frame options.

use std::sync::Arc;

use novarocks_functions::{
    AggregateKernelPhase, AggregateOrderKey, AggregatePreparationOptions,
    AggregateWindowPreparationOptions, ConstantValidation, FunctionArgument,
    FunctionBindingRequest, FunctionBindingSelection, FunctionResultType,
    FunctionSpecializationFailure, KernelFailure, MAX_CALL_EFFECT_ARGUMENTS, PureCallPreparation,
    ScopedRelationalEffects, WindowCallOptions,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionKind,
    FunctionValueType, ValueTypeError,
};
use {
    novarocks_expression_contract::{BoundFunction, RootOrdinal},
    novarocks_physical_plan::{
        AggregatePhase, ConstantPools, ConstantReference, NullOrdering, PhysicalNode,
        SortDirection, WindowBound, WindowCallSpec, window_offset_observed,
    },
};

use super::{
    expression_occurrences::ExpressionOccurrenceError,
    lowered_draft::{CanonicalCallOperationalRequest, CheckedWindowLogicalSourceEntry},
    physical_call_arguments::{
        PhysicalArgumentError, argument_types_exact_observed, author_physical_argument_observed,
    },
    physical_scalar_requests::author_scalar_result_selection_observed,
};

#[derive(Debug)]
pub(crate) enum PhysicalWindowRequestError {
    Control(CompileControlError),
    Argument(PhysicalArgumentError),
    Function(ExpressionOccurrenceError),
    InvalidSource(&'static str),
    MissingArgument(RootOrdinal),
    NonconstantFrameOffset(ConstantReference),
    TooManyArguments,
}
impl From<CompileControlError> for PhysicalWindowRequestError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<PhysicalArgumentError> for PhysicalWindowRequestError {
    fn from(error: PhysicalArgumentError) -> Self {
        match error {
            PhysicalArgumentError::Control(cause) => Self::Control(cause),
            other => Self::Argument(other),
        }
    }
}
impl From<KernelFailure> for PhysicalWindowRequestError {
    fn from(error: KernelFailure) -> Self {
        match ExpressionOccurrenceError::function(FunctionSpecializationFailure::Kernel(error)) {
            ExpressionOccurrenceError::Control(cause) => Self::Control(cause),
            other => Self::Function(other),
        }
    }
}

impl From<ValueTypeError> for PhysicalWindowRequestError {
    fn from(error: ValueTypeError) -> Self {
        Self::from(PhysicalArgumentError::from(error))
    }
}

#[derive(Debug)]
enum WindowRequestArguments<'a> {
    OwnedPhysical {
        arguments: Vec<FunctionArgument>,
        logical_count: usize,
        result_constraint: &'a FunctionValueType,
    },
    BorrowedCanonical {
        request: &'a Arc<CanonicalCallOperationalRequest>,
        decimal_policy: DecimalOverflowPolicy,
        constant_policy: ConstantValidation,
    },
}

#[derive(Debug)]
pub(crate) struct AuthoredPhysicalWindowRequest<'a> {
    source: &'a WindowCallSpec,
    function: &'a BoundFunction,
    selected: Arc<FunctionBindingSelection>,
    arguments: WindowRequestArguments<'a>,
    window: WindowCallOptions,
    aggregate: Option<AggregatePreparationOptions>,
}
impl AuthoredPhysicalWindowRequest<'_> {
    pub const fn source(&self) -> &WindowCallSpec {
        self.source
    }
    pub const fn function(&self) -> &BoundFunction {
        self.function
    }
    pub const fn selected(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub fn request(&self) -> FunctionBindingRequest<'_> {
        match &self.arguments {
            WindowRequestArguments::OwnedPhysical {
                arguments,
                logical_count,
                result_constraint,
            } => FunctionBindingRequest {
                arguments,
                logical_argument_count: *logical_count,
                expected_result_type: Some(result_constraint),
            },
            WindowRequestArguments::BorrowedCanonical { request, .. } => request.request(),
        }
    }
    pub fn captured_decimal_overflow_policy(&self) -> Option<DecimalOverflowPolicy> {
        match &self.arguments {
            WindowRequestArguments::OwnedPhysical { .. } => None,
            WindowRequestArguments::BorrowedCanonical { decimal_policy, .. } => {
                Some(*decimal_policy)
            }
        }
    }
    pub fn captured_constant_policy(&self) -> Option<ConstantValidation> {
        match &self.arguments {
            WindowRequestArguments::OwnedPhysical { .. } => None,
            WindowRequestArguments::BorrowedCanonical {
                constant_policy, ..
            } => Some(*constant_policy),
        }
    }
    pub fn preparation(&self, arguments: ScopedRelationalEffects) -> PureCallPreparation {
        match &self.aggregate {
            Some(aggregate) => PureCallPreparation::AggregateWindow {
                arguments,
                options: AggregateWindowPreparationOptions {
                    aggregate: aggregate.clone(),
                    window: self.window,
                },
            },
            None => PureCallPreparation::Window {
                arguments,
                options: self.window,
            },
        }
    }
}

/// Static channels are args then function ORDER BY. Frame offsets remain
/// independent checked constants, never runtime uses or logical arguments.
/// Exact offset facts reuse the sole physical constant consumer; None frame
/// stays None. The caller owns original source/result/lexical admission, entry,
/// footer and opaque request/selection/options coexistence, not a host grant.
pub(crate) fn author_physical_window_request_observed<'a>(
    source: &'a WindowCallSpec,
    node: &PhysicalNode,
    pools: &ConstantPools,
    literal_policy: ConstantValidation,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalWindowRequest<'a>, PhysicalWindowRequestError> {
    work.step()?;
    let WindowCallSpec {
        binding: function,
        distinct,
        arguments: args,
        order_by: function_order_by,
        frame,
        ignore_nulls,
        aggregate_binding,
        ..
    } = source;
    let count = args.len().checked_add(function_order_by.len());
    work.step()?;
    let count = count
        .filter(|&count| count <= MAX_CALL_EFFECT_ARGUMENTS)
        .ok_or(PhysicalWindowRequestError::TooManyArguments)?;
    let exact_count = count == function.argument_types.len();
    work.step()?;
    if !exact_count {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window channels differ from their selected signature",
        ));
    }
    let aggregate = aggregate_options_observed(
        function,
        *distinct,
        args.len(),
        function_order_by,
        aggregate_binding.as_deref(),
        work,
    )?;
    work.flush()?;
    let mut arguments = Vec::new();
    arguments
        .try_reserve_exact(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    for id in args
        .iter()
        .copied()
        .chain(function_order_by.iter().map(|key| key.expr))
    {
        let argument = super::unit_source::UnitDefinitions::from_node(node).root_definition(id);
        work.step()?;
        let argument = argument.ok_or(PhysicalWindowRequestError::MissingArgument(id))?;
        arguments.push(author_physical_argument_observed(
            argument,
            pools,
            literal_policy,
            CompilePhase::FunctionSpecialization,
            work,
        )?);
        work.step()?;
    }
    let selected =
        author_scalar_result_selection_observed(function, aggregate_binding.as_deref(), work)?;
    let window = window_options_observed(frame.as_ref(), *ignore_nulls, pools, work)?;
    Ok(AuthoredPhysicalWindowRequest {
        source,
        function,
        selected,
        arguments: WindowRequestArguments::OwnedPhysical {
            arguments,
            logical_count: args.len(),
            result_constraint: &function.result_type,
        },
        window,
        aggregate,
    })
}

/// Borrow the original emitter's operational channels and the same selected
/// Arc. Only frame offsets consult the original physical constant consumer;
/// logical arguments are neither projected again nor admitted as literals.
/// The caller owns immutable source/catalog/pool admission and the footer.
pub(crate) fn author_physical_window_request_from_journal_observed<'source>(
    entry: &CheckedWindowLogicalSourceEntry<'source>,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalWindowRequest<'source>, PhysicalWindowRequestError> {
    author_window_request_from_canonical_observed(
        entry.source(),
        entry.captured(),
        entry.canonical_operational(),
        || entry.fragment().values().get(&entry.source().output),
        pools,
        work,
    )
}

fn author_window_request_from_canonical_observed<'source>(
    source: &'source WindowCallSpec,
    captured: &'source crate::binding::CapturedLogicalCallArguments,
    canonical: &'source Arc<CanonicalCallOperationalRequest>,
    output: impl FnOnce() -> Option<&'source novarocks_physical_plan::ValueDef>,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalWindowRequest<'source>, PhysicalWindowRequestError> {
    work.step()?;
    let belongs = canonical.belongs_to(captured);
    work.step()?;
    if !belongs {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window canonical request has another original capture",
        ));
    }
    let WindowCallSpec {
        binding: function,
        distinct,
        arguments: args,
        order_by: function_order_by,
        frame,
        ignore_nulls,
        aggregate_binding,
        ..
    } = source;
    let count = args.len().checked_add(function_order_by.len());
    work.step()?;
    let count = count
        .filter(|&count| count <= MAX_CALL_EFFECT_ARGUMENTS)
        .ok_or(PhysicalWindowRequestError::TooManyArguments)?;
    let request = canonical.request();
    let selected = canonical.selected();
    let complete_count = request.logical_argument_count == args.len()
        && request.arguments.len() == count
        && selected.argument_types.len() == count;
    work.step()?;
    if !complete_count {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window operational channel counts differ",
        ));
    }
    work.flush()?;
    let original = captured.binding().resolved();
    let identity = original.function_id == function.function_id
        && original.kind == function.kind
        && original.selected.overload == function.overload
        && selected.overload == function.overload;
    work.step()?;
    work.flush()?;
    if !identity {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window canonical binding has another identity",
        ));
    }
    signature_observed(function, selected, work)?;
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window canonical result is not scalar",
        ));
    };
    let output = output().ok_or(PhysicalWindowRequestError::InvalidSource(
        "window output value is absent",
    ))?;
    work.step()?;
    if !output.ty.exactly_equals_observed(result, || {
        work.step().map_err(PhysicalWindowRequestError::from)
    })? {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window source result differs from canonical result",
        ));
    }
    if let Some(binding) = aggregate_binding.as_deref() {
        signature_observed(&binding.function, selected, work)?;
        work.step()?;
        if binding.function.function_id != function.function_id
            || binding.function.kind != function.kind
        {
            return Err(PhysicalWindowRequestError::InvalidSource(
                "window aggregate binding identity differs",
            ));
        }
        let matching = match (&selected.aggregate, &binding.merge_state) {
            (None, None) => true,
            (Some(state), Some(merge)) => {
                state.state_argument_contract == merge.state_argument_contract
                    && state.state_format == merge.state_format
                    && state
                        .intermediate_type
                        .exactly_equals_observed(&merge.intermediate_type, || {
                            work.step().map_err(PhysicalWindowRequestError::from)
                        })?
            }
            _ => false,
        };
        if !matching {
            return Err(PhysicalWindowRequestError::InvalidSource(
                "window aggregate canonical state metadata differs",
            ));
        }
    } else if selected.aggregate.is_some() {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window aggregate canonical header differs",
        ));
    }
    let aggregate = aggregate_options_observed(
        function,
        *distinct,
        args.len(),
        function_order_by,
        aggregate_binding.as_deref(),
        work,
    )?;
    let window = window_options_observed(frame.as_ref(), *ignore_nulls, pools, work)?;
    work.flush()?;
    let selected = Arc::clone(selected);
    work.step()?;
    work.flush()?;
    Ok(AuthoredPhysicalWindowRequest {
        source,
        function,
        selected,
        arguments: WindowRequestArguments::BorrowedCanonical {
            request: canonical,
            decimal_policy: captured.binding().decimal_overflow_policy(),
            constant_policy: captured.constant_policy(),
        },
        window,
        aggregate,
    })
}

/// Test-only access to the same request core from an actual raw emission.
/// The view's original draft validator ran before this component invocation;
/// neither this adapter nor its output is a Frozen-call receipt.
#[cfg(test)]
fn author_window_request_from_emission_for_test<'source>(
    emission: &super::contract_lowering::original_emission_test_observer::OriginalWindowEmission<
        'source,
    >,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalWindowRequest<'source>, PhysicalWindowRequestError> {
    author_window_request_from_canonical_observed(
        emission.source(),
        emission.captured(),
        emission.canonical(),
        || emission.builder().value(emission.source().output),
        emission.constants(),
        work,
    )
}

fn signature_observed(
    function: &BoundFunction,
    selected: &FunctionBindingSelection,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), PhysicalWindowRequestError> {
    work.flush()?;
    let header = function.overload == selected.overload
        && function.argument_types.len() == selected.argument_types.len();
    work.step()?;
    work.flush()?;
    if !header {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window canonical selected signature differs",
        ));
    }
    for (left, right) in function
        .argument_types
        .iter()
        .zip(selected.argument_types.iter())
    {
        if !argument_types_exact_observed::<PhysicalWindowRequestError>(left, right, work)? {
            return Err(PhysicalWindowRequestError::InvalidSource(
                "window canonical selected argument differs",
            ));
        }
    }
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window canonical result is not scalar",
        ));
    };
    if !function.result_type.exactly_equals_observed(result, || {
        work.step().map_err(PhysicalWindowRequestError::from)
    })? {
        return Err(PhysicalWindowRequestError::InvalidSource(
            "window canonical selected result differs",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "physical_window_journal_tests.rs"]
mod journal_tests;

fn aggregate_options_observed(
    function: &BoundFunction,
    distinct: bool,
    logical_count: usize,
    function_order_by: &[novarocks_physical_plan::SortExpr],
    aggregate_binding: Option<&novarocks_physical_plan::AggregateBinding>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<AggregatePreparationOptions>, PhysicalWindowRequestError> {
    let aggregate = match (function.kind, aggregate_binding) {
        (FunctionKind::Window, None) if !distinct && function_order_by.is_empty() => None,
        (FunctionKind::Aggregate, Some(binding)) => {
            let exact = binding.phase == AggregatePhase::Single
                && binding.logical_argument_count as usize == logical_count;
            work.step()?;
            if !exact {
                return Err(PhysicalWindowRequestError::InvalidSource(
                    "aggregate OVER requires its exact Single logical argument count",
                ));
            }
            work.flush()?;
            let mut keys = Vec::new();
            keys.try_reserve_exact(function_order_by.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            for key in function_order_by {
                keys.push(AggregateOrderKey {
                    ascending: key.direction == SortDirection::Ascending,
                    nulls_first: key.null_ordering == NullOrdering::First,
                });
                work.step()?;
            }
            work.flush()?;
            Some(AggregatePreparationOptions {
                state_interpretation: binding
                    .state_interpretation()
                    .map(|facts| facts.clone_observed(work).map(Arc::new))
                    .transpose()?,
                phase: AggregateKernelPhase::Single,
                distinct,
                order_keys: keys.into(),
                state_input_type: None,
            })
        }
        _ => {
            return Err(PhysicalWindowRequestError::InvalidSource(
                "window source kind and lifecycle binding differ",
            ));
        }
    };
    Ok(aggregate)
}

fn window_options_observed(
    frame: Option<&novarocks_physical_plan::WindowFrame>,
    ignore_nulls: bool,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
) -> Result<WindowCallOptions, PhysicalWindowRequestError> {
    let frame = frame
        .map(|frame| {
            Ok::<_, PhysicalWindowRequestError>(novarocks_type_contract::WindowFrame {
                units: frame.units,
                start: bound(frame.start, pools, work)?,
                end: bound(frame.end, pools, work)?,
                exclusion: frame.exclusion,
            })
        })
        .transpose()?;
    work.flush()?;
    let window = WindowCallOptions::try_new(frame, ignore_nulls, work.control())?;
    work.flush()?;
    Ok(window)
}

fn bound(
    bound: WindowBound,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
) -> Result<novarocks_type_contract::WindowBound<u64>, PhysicalWindowRequestError> {
    use novarocks_type_contract::WindowBound as Bound;
    work.step()?;
    Ok(match bound {
        Bound::UnboundedPreceding => Bound::UnboundedPreceding,
        Bound::CurrentRow => Bound::CurrentRow,
        Bound::UnboundedFollowing => Bound::UnboundedFollowing,
        Bound::Preceding(id) | Bound::Following(id) => {
            let offset =
                window_offset_observed(pools, id, work).map_err(PhysicalArgumentError::from)?;
            work.step()?;
            let offset = offset.ok_or(PhysicalWindowRequestError::NonconstantFrameOffset(id))?;
            if matches!(bound, Bound::Preceding(_)) {
                Bound::Preceding(offset)
            } else {
                Bound::Following(offset)
            }
        }
    })
}
