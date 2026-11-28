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

//! Lower one admitted physical Window into the local Analytic owner and
//! prepare each of its calls from the exact frozen window facts.
//!
//! Every call becomes a `Prepared` local call whose kernel is the one this
//! compiler attaches at `ProgramCallSite::Window`. It is prepared through the
//! installed pure window owner (or the aggregate owner's window adapter for an
//! aggregate OVER) from the frozen binding, the original request, the frozen
//! call effects and the actual effects of its argument roots. No
//! implementation is selected by name: the display name copied into an
//! aggregate binding is a diagnostic tag only.
//!
//! Calls retain their real relation contexts. Arguments reference the node's
//! expression unit roots; frame offsets borrow actual constant references and
//! never enter the unit's evaluation flow.
//!
//! Frames belong to each call. A call without a frame receives the SQL
//! default, derived only by `effective_frame` and frozen explicitly into the
//! program; the prepared options keep the frozen absence of a frame.

use crate::{
    assert_rows::reserve_vec, expressions::ExpressionLoweringError, lowering::FragmentCompileError,
};
use novarocks_expression_contract::{BoundFunction, RootOrdinal};
use novarocks_functions::{
    AggregateBindingSelection, AggregateKernelPhase, AggregateOrderKey, AggregateOverloadIdentity,
    AggregatePreparationOptions, AggregateWindowPreparationOptions, CallEffectInput,
    EngineFunctionCatalog, FunctionBindingSelection, FunctionResultType,
    FunctionSpecializationFailure, PureCallLifecycle, PureCallPreparation, PureCallSpecialization,
    PureEngineFunctionCatalog, PureKernelAbi, ResolvedAggregateSignature, ScopedRelationalEffects,
    WindowCallOptions,
};
use novarocks_local_program::{
    AnalyticOutputColumn, ProgramCallSite, ProgramNodeId, ProgramNodeKind, StaticAggregateOrder,
    StaticLayout, StaticWindowFunction, WindowBoundary, WindowFrame as ProgramWindowFrame,
    WindowFunctionKind, WindowType,
};
use novarocks_physical_plan::{
    AggregatePhase, FragmentPackage, FunctionArgumentType, NodeId, NodeKind, NullOrdering,
    PhysicalCallSite, PhysicalNode, SortDirection,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, ExpressionEffects,
    FunctionKind, PureCompileControl, WindowBound, WindowFrame, WindowFrameExclusion,
    WindowFrameUnits,
};
use novarocks_types::SlotId;
use std::{collections::BTreeMap, sync::Arc};

/// Admit one physical Window occurrence before any lowering work. Each call's
/// installed kernel is found through the pure catalog by its frozen function
/// identity and overload, never by a SQL name; a call shape no installed
/// kernel prepares is refused here by its feature.
pub(crate) fn admit_window(
    _package: &FragmentPackage,
    node: &PhysicalNode,
    functions: &PureEngineFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    let NodeKind::Window(spec) = &node.kind else {
        return Err(FragmentCompileError::Invalid(
            "window admission for another node family",
        ));
    };
    if node.inputs.len() != 1 || spec.calls.is_empty() {
        return Err(FragmentCompileError::Invalid(
            "window input or call list differs",
        ));
    }
    let unsupported = |feature| FragmentCompileError::Unsupported {
        node: Some(node.id),
        feature,
    };
    for item in spec.calls.iter() {
        let function = &item.binding;
        let frame = &item.frame;
        let aggregate_binding = &item.aggregate_binding;
        if let Some(frame) = frame {
            if frame.units == WindowFrameUnits::Groups {
                return Err(unsupported("GROUPS window frame"));
            }
            if frame.exclusion != WindowFrameExclusion::NoOthers {
                return Err(unsupported("window frame exclusion other than NO OTHERS"));
            }
            if frame.units == WindowFrameUnits::Range && has_offset(frame) {
                return Err(unsupported("RANGE window frame offset"));
            }
        }
        let (abi, missing) = match (function.kind, aggregate_binding.as_deref()) {
            (FunctionKind::Window, None) if !item.distinct && item.order_by.is_empty() => (
                PureKernelAbi::WindowV1,
                "window function without an installed pure window kernel",
            ),
            (FunctionKind::Aggregate, Some(binding)) if binding.phase == AggregatePhase::Single => {
                (
                    PureKernelAbi::AggregateWindowV1,
                    "aggregate OVER without an installed pure window kernel",
                )
            }
            _ => {
                return Err(FragmentCompileError::Invalid(
                    "window call binds no exact window or single-phase aggregate",
                ));
            }
        };
        for argument in function.argument_types.iter() {
            let lambda = matches!(argument, FunctionArgumentType::Lambda { .. });
            work.step()?;
            if lambda {
                return Err(unsupported("window call with a lambda argument"));
            }
        }
        work.flush()?;
        let owner = window_function_owner(function, functions.metadata(), work.control())?;
        work.flush()?;
        if owner.implementation().abi != abi {
            return Err(unsupported(missing));
        }
        if let Some(binding) = aggregate_binding.as_deref()
            && (item.distinct || !item.order_by.is_empty())
        {
            let selected = window_selection(function, Some(binding));
            let lifecycle = PureCallLifecycle::AggregateWindow {
                distinct: item.distinct,
                ordered: !item.order_by.is_empty(),
            };
            match owner.admit_selected_lifecycle_observed(
                &selected,
                item.arguments.len(),
                lifecycle,
                work.control(),
            ) {
                Ok(()) => {}
                Err(novarocks_functions::FunctionBindingError::UnavailableImplementation(_)) => {
                    return Err(unsupported(if item.distinct {
                        "DISTINCT window call"
                    } else {
                        "window function ORDER BY"
                    }));
                }
                Err(error) => return Err(ExpressionLoweringError::from(error).into()),
            }
        }
        work.step()?;
    }
    Ok(())
}

/// Preserve metadata lookup failures separately from a missing ABI capability.
/// The production caller supplies metadata from its sealed pure catalog.
#[cfg(test)]
pub(super) fn window_function_abi(
    function: &BoundFunction,
    functions: &EngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<PureKernelAbi, FragmentCompileError> {
    window_function_owner(function, functions, control).map(|owner| owner.implementation().abi)
}

fn window_function_owner<'a>(
    function: &BoundFunction,
    functions: &'a EngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<novarocks_functions::PureOverloadDeclaration<'a>, FragmentCompileError> {
    match functions.pure_overload_declaration_observed(
        &function.function_id,
        function.kind,
        &function.overload,
        control,
    ) {
        Ok(declaration) => Ok(declaration),
        Err(FunctionSpecializationFailure::Control(cause)) => Err(cause.into()),
        Err(error) => Err(ExpressionLoweringError::Specialization(error).into()),
    }
}

fn has_offset<O>(frame: &WindowFrame<O>) -> bool {
    [&frame.start, &frame.end]
        .iter()
        .any(|bound| matches!(bound, WindowBound::Preceding(_) | WindowBound::Following(_)))
}

/// One call's frozen frame with its checked constant offsets, in the prepared
/// options' vocabulary. `None` keeps the frozen absence of a frame.
fn frozen_frame(
    package: &FragmentPackage,
    frame: Option<&novarocks_physical_plan::WindowFrame>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<WindowFrame<u64>>, ExpressionLoweringError> {
    let Some(frame) = frame else {
        return Ok(None);
    };
    let mut bound =
        |bound: &WindowBound<novarocks_physical_plan::ConstantReference>| -> Result<WindowBound<u64>, ExpressionLoweringError> {
            let mut offset = |id: &novarocks_physical_plan::ConstantReference| {
                novarocks_physical_plan::window_offset_observed(
                    package.constants(),
                    *id,
                    work,
                )?
                .ok_or(ExpressionLoweringError::Invalid(
                    "window frame offset is not a checked constant",
                ))
            };
            Ok(match bound {
                WindowBound::UnboundedPreceding => WindowBound::UnboundedPreceding,
                WindowBound::Preceding(id) => WindowBound::Preceding(offset(id)?),
                WindowBound::CurrentRow => WindowBound::CurrentRow,
                WindowBound::Following(id) => WindowBound::Following(offset(id)?),
                WindowBound::UnboundedFollowing => WindowBound::UnboundedFollowing,
            })
        };
    Ok(Some(WindowFrame {
        units: frame.units,
        start: bound(&frame.start)?,
        end: bound(&frame.end)?,
        exclusion: frame.exclusion,
    }))
}

/// The one host derivation of a call's effective frame. An explicit frame is
/// carried exactly. Without one, SQL's default frame is RANGE BETWEEN
/// UNBOUNDED PRECEDING AND CURRENT ROW, whose CURRENT ROW extends over the
/// row's ORDER BY peers; without ORDER BY every row of a partition is its
/// peer, so the frame is the whole partition.
pub(crate) fn effective_frame(
    frozen: Option<&WindowFrame<u64>>,
    ordered: bool,
) -> Result<ProgramWindowFrame, FragmentCompileError> {
    let Some(frame) = frozen else {
        return Ok(if ordered {
            ProgramWindowFrame {
                start: None,
                end: Some(WindowBoundary::CurrentRow),
                window_type: WindowType::Range,
            }
        } else {
            ProgramWindowFrame {
                start: None,
                end: None,
                window_type: WindowType::Rows,
            }
        });
    };
    let window_type = match frame.units {
        WindowFrameUnits::Rows => WindowType::Rows,
        WindowFrameUnits::Range => WindowType::Range,
        WindowFrameUnits::Groups => {
            return Err(FragmentCompileError::Unsupported {
                node: None,
                feature: "GROUPS window frame",
            });
        }
    };
    let offset = |offset: u64| {
        i64::try_from(offset).map_err(|_| FragmentCompileError::Unsupported {
            node: None,
            feature: "window frame offset beyond the signed 64-bit range",
        })
    };
    let bounded = |bound: WindowBound<u64>| -> Result<WindowBoundary, FragmentCompileError> {
        Ok(match bound {
            WindowBound::CurrentRow => WindowBoundary::CurrentRow,
            WindowBound::Preceding(value) => WindowBoundary::Preceding(offset(value)?),
            WindowBound::Following(value) => WindowBoundary::Following(offset(value)?),
            WindowBound::UnboundedPreceding | WindowBound::UnboundedFollowing => {
                return Err(FragmentCompileError::Invalid(
                    "window frame bound is unbounded on its wrong side",
                ));
            }
        })
    };
    Ok(ProgramWindowFrame {
        start: match frame.start {
            WindowBound::UnboundedPreceding => None,
            bound => Some(bounded(bound)?),
        },
        end: match frame.end {
            WindowBound::UnboundedFollowing => None,
            bound => Some(bounded(bound)?),
        },
        window_type,
    })
}

/// Lower one admitted Window whose output is its whole input followed by one
/// value per call, in call order. Pass-through columns keep their input
/// channels; `slots` is the planned output, input slots first.
pub(crate) fn lower_window_with_creation<H, P: crate::project_metadata::ProjectMetadataScope>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    input: ProgramNodeId,
    input_layout: &StaticLayout,
    expressions: &crate::expressions::RootMap,
    slots: &[SlotId],
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    metadata: &mut crate::project_metadata::ProjectMetadataMode<'_, P>,
) -> Result<(ProgramNodeKind, StaticLayout), crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core_with_creation(
        package,
        node,
        input,
        input_layout,
        expressions,
        slots,
        &mut work,
        creation,
        metadata,
    );
    crate::creation::finish_attempt(result, work)
}

fn lower_core_with_creation<H, P: crate::project_metadata::ProjectMetadataScope>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    input: ProgramNodeId,
    input_layout: &StaticLayout,
    expressions: &crate::expressions::RootMap,
    slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    metadata: &mut crate::project_metadata::ProjectMetadataMode<'_, P>,
) -> Result<(ProgramNodeKind, StaticLayout), crate::FragmentCreationError<H>> {
    let NodeKind::Window(spec) = &node.kind else {
        return Err((FragmentCompileError::Invalid("Window kind differs")).into());
    };
    let width = input_layout.slots().len();
    let total = width
        .checked_add(spec.calls.len())
        .ok_or(CompileControlError::ResourceExhausted)?;
    if node.inputs.len() != 1 || node.output.columns.len() != total || slots.len() != total {
        return Err((FragmentCompileError::Invalid(
            "window input, output or channel width differs",
        ))
        .into());
    }
    let lowered = |id: &RootOrdinal, missing: &'static str| {
        expressions
            .get(id)
            .copied()
            .ok_or(FragmentCompileError::Invalid(missing))
    };
    let mut partition_exprs = Vec::new();
    reserve_vec(&mut partition_exprs, spec.partition_by.len(), work)?;
    for key in spec.partition_by.iter() {
        partition_exprs.push(lowered(&key.expr, "missing lowered window partition key")?);
        work.step()?;
    }
    let mut order_by_exprs = Vec::new();
    reserve_vec(&mut order_by_exprs, spec.order_by.len(), work)?;
    for key in spec.order_by.iter() {
        order_by_exprs.push(lowered(&key.expr, "missing lowered window order key")?);
        work.step()?;
    }
    let ordered = !spec.order_by.is_empty();
    let values = package.fragment().values();
    let mut functions = Vec::new();
    reserve_vec(&mut functions, spec.calls.len(), work)?;
    for (ordinal, item) in spec.calls.iter().enumerate() {
        let args = &item.arguments;
        let frame = &item.frame;
        let ignore_nulls = item.ignore_nulls;
        let aggregate_binding = &item.aggregate_binding;
        if node.output.columns.get(width + ordinal) != Some(&item.output) {
            return Err((FragmentCompileError::Invalid(
                "window output is not its input followed by its call outputs",
            ))
            .into());
        }
        let output = values
            .get(&item.output)
            .ok_or(FragmentCompileError::Invalid("missing window output value"))?;
        work.flush()?;
        let same = item
            .binding
            .result_type
            .exactly_equals_observed::<crate::FragmentCreationError<H>>(&output.ty, || {
                work.step().map_err(Into::into)
            })?;
        if !same {
            return Err((FragmentCompileError::Invalid(
                "window output type differs from its call",
            ))
            .into());
        }
        let mut inputs = Vec::new();
        reserve_vec(&mut inputs, args.len(), work)?;
        for argument in args.iter() {
            inputs.push(lowered(argument, "missing lowered window argument")?);
            work.step()?;
        }
        let mut function_order_by = Vec::new();
        reserve_vec(&mut function_order_by, item.order_by.len(), work)?;
        for key in &item.order_by {
            function_order_by.push(lowered(&key.expr, "missing lowered function sort key")?);
            work.step()?;
        }
        let aggregate_order = if aggregate_binding.is_some() {
            let mut is_asc_order = Vec::new();
            let mut nulls_first = Vec::new();
            reserve_vec(&mut is_asc_order, item.order_by.len(), work)?;
            reserve_vec(&mut nulls_first, item.order_by.len(), work)?;
            for key in &item.order_by {
                is_asc_order.push(key.direction == SortDirection::Ascending);
                nulls_first.push(key.null_ordering == NullOrdering::First);
                work.step()?;
            }
            Some(StaticAggregateOrder {
                is_asc_order,
                nulls_first,
                is_distinct: item.distinct,
            })
        } else {
            None
        };
        let frozen = frozen_frame(package, frame.as_ref(), work)?;
        let state_interpretation = aggregate_binding
            .as_ref()
            .and_then(|binding| binding.merge_state.as_ref())
            .and_then(|state| state.state_interpretation.as_ref())
            .map(|facts| facts.clone_observed(work))
            .transpose()?;
        let frozen_aggregate = aggregate_binding
            .as_deref()
            .map(|binding| crate::aggregate::freeze_binding_with_creation(binding, work, creation))
            .transpose()?;
        let aggregate_binding = match aggregate_binding.as_deref() {
            Some(binding) if binding.merge_state.is_some() => {
                Some(aggregate_signature(binding, work)?)
            }
            _ => None,
        };
        work.flush()?;
        functions.push(StaticWindowFunction {
            frozen_aggregate,
            kind: WindowFunctionKind::Prepared { aggregate_order },
            args: inputs,
            function_order_by,
            state_interpretation,
            return_type: item.binding.result_type.data_type().clone(),
            aggregate_binding,
            frame: Some(effective_frame(frozen.as_ref(), ordered)?),
            ignore_nulls,
        });
        work.step()?;
    }
    let mut output_columns = Vec::new();
    reserve_vec(&mut output_columns, total, work)?;
    for &slot in input_layout.slots() {
        output_columns.push(AnalyticOutputColumn::InputSlotId(slot));
        work.step()?;
    }
    for call in 0..spec.calls.len() {
        output_columns.push(AnalyticOutputColumn::Window(call));
        work.step()?;
    }
    let result = package
        .result()
        .filter(|result| result.output.columns == node.output.columns);
    let requests = if matches!(
        metadata,
        crate::project_metadata::ProjectMetadataMode::Funded(_)
    ) {
        Some(crate::project_metadata::window_output_requests(
            node.id,
            total,
            node.output
                .columns
                .iter()
                .enumerate()
                .map(|(ordinal, value)| {
                    let ty = &values
                        .get(value)
                        .ok_or(FragmentCompileError::Invalid("missing window output type"))?
                        .ty;
                    let label =
                        match result {
                            Some(result) => {
                                let field = result.fields.get(ordinal).ok_or(
                                    FragmentCompileError::Invalid("missing window result label"),
                                )?;
                                Some(field.alias.as_deref().unwrap_or(&field.name))
                            }
                            None => None,
                        };
                    Ok((ty, label))
                }),
            work,
        )?)
    } else {
        None
    };
    let mut build = || -> Result<crate::project_metadata::ProjectMetadataOutput, crate::FragmentCreationError<H>> {
        let mut typed_layout =
            crate::typed_layout::TypedLayoutBuilder::new_with_creation(total, work, creation)?;
        for (ordinal, value) in node.output.columns.iter().enumerate() {
            let ty = &values
                .get(value)
                .ok_or(FragmentCompileError::Invalid("missing window output type"))?
                .ty;
            // Full result labels are authoritative only when this window's entire
            // ordered output is the result port's.
            let name = match result {
                Some(result) => {
                    let field = result
                        .fields
                        .get(ordinal)
                        .ok_or(FragmentCompileError::Invalid("missing window result label"))?;
                    crate::typed_layout::copy_name_with_creation(
                        field.alias.as_deref().unwrap_or(&field.name),
                        work,
                        creation,
                    )?
                }
                None => crate::typed_layout::local_name_with_creation(
                    "local_window",
                    node.id.get(),
                    ordinal,
                    work,
                    creation,
                )?,
            };
            work.flush()?;
            typed_layout.push_with_creation(ty, name, work, creation)?;
            work.step()?;
        }
        work.flush()?;
        let layout = typed_layout.finish_with_creation(slots, work, creation)?;
        Ok(crate::project_metadata::ProjectMetadataOutput {
            layout,
            exprs: Vec::new(),
            expr_slot_ids: Vec::new(),
        })
    };
    let layout = match requests {
        Some(requests) => metadata.materialize(&requests, build)?.layout,
        None => build()?.layout,
    };
    work.flush()?;
    Ok((
        ProgramNodeKind::Analytic {
            input,
            partition_exprs,
            order_by_exprs,
            functions,
            output_columns,
        },
        layout,
    ))
}

/// The mandatory legacy carrier of an aggregate OVER binding. Its display
/// name is a diagnostic tag; the frozen preparation owns the call.
fn aggregate_signature(
    binding: &novarocks_physical_plan::AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Arc<str>, ResolvedAggregateSignature), FragmentCompileError> {
    let state = binding
        .merge_state
        .as_ref()
        .ok_or(FragmentCompileError::Invalid(
            "aggregate window has no merge signature",
        ))?;
    let function = &binding.function;
    let mut argument_types = Vec::new();
    reserve_vec(&mut argument_types, function.argument_types.len(), work)?;
    for argument in function.argument_types.iter() {
        let FunctionArgumentType::Value(value) = argument else {
            return Err(FragmentCompileError::Invalid(
                "aggregate window channel is not a value",
            ));
        };
        work.flush()?;
        argument_types.push(value.data_type().clone());
        work.step()?;
    }
    let overload =
        AggregateOverloadIdentity::try_new(function.overload.as_str()).map_err(|error| {
            FragmentCompileError::Owner {
                phase: "aggregate window overload identity",
                error: novarocks_failure_contract::SharedFailureCause::new(error),
            }
        })?;
    work.flush()?;
    let name: Arc<str> = Arc::from(function.function_id.as_str());
    work.flush()?;
    Ok((
        name,
        ResolvedAggregateSignature {
            overload,
            argument_types,
            intermediate_type: state.intermediate_type.data_type().clone(),
            output_type: function.result_type.data_type().clone(),
            state_format: state.state_format.clone(),
        },
    ))
}

/// The occurrences a compiled Window retires and the argument roots it owns.
pub(crate) fn prepare_window_calls<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    functions: &PureEngineFunctionCatalog,
    effects: &crate::expressions::LoweredExpressions,
    nodes: &BTreeMap<NodeId, ProgramNodeId>,
    tokens: &mut BTreeMap<ProgramCallSite, PureCallSpecialization>,
    work: &mut CompileCheckpoints<'_>,
    creation: crate::expressions::CallCreation<'_, F>,
) -> Result<(), crate::expressions::ExpressionCreationFailure<H>> {
    let mut prepared = 0usize;
    for node in package.fragment().nodes().values() {
        work.step()?;
        let NodeKind::Window(spec) = &node.kind else {
            continue;
        };
        let local = *nodes.get(&node.id).ok_or(ExpressionLoweringError::Invalid(
            "window node has no local node",
        ))?;
        for (ordinal, call) in spec.calls.iter().enumerate() {
            let ordinal = u32::try_from(ordinal)
                .map_err(|_| ExpressionLoweringError::Invalid("window call ordinal exhausted"))?;
            let token = prepare_call(
                package, functions, effects, node, ordinal, call, work, creation,
            )?;
            if tokens
                .insert(
                    ProgramCallSite::Window {
                        node: local,
                        call: ordinal,
                    },
                    token,
                )
                .is_some()
            {
                return Err(
                    ExpressionLoweringError::Invalid("window call site is prepared twice").into(),
                );
            }
            prepared += 1;
            work.step()?;
        }
    }
    let mut frozen = 0usize;
    for site in package.calls().entries().keys() {
        if matches!(site, PhysicalCallSite::Window { .. }) {
            frozen += 1;
        }
        work.step()?;
    }
    if frozen != prepared {
        return Err(ExpressionLoweringError::Invalid(
            "frozen window calls differ from the actual window call roots",
        )
        .into());
    }
    Ok(())
}

fn window_selection(
    function: &BoundFunction,
    binding: Option<&novarocks_physical_plan::AggregateBinding>,
) -> FunctionBindingSelection {
    FunctionBindingSelection {
        overload: function.overload.clone(),
        argument_types: function.argument_types.clone(),
        result_type: FunctionResultType::Scalar(function.result_type.clone()),
        aggregate: binding
            .and_then(|binding| binding.merge_state.as_ref())
            .map(|binding| AggregateBindingSelection {
                state_argument_contract: binding.state_argument_contract,
                intermediate_type: binding.intermediate_type.clone(),
                state_format: binding.state_format.clone(),
            }),
    }
}

fn prepare_call<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    functions: &PureEngineFunctionCatalog,
    effects: &crate::expressions::LoweredExpressions,
    node: &PhysicalNode,
    ordinal: u32,
    call: &novarocks_physical_plan::WindowCallSpec,
    work: &mut CompileCheckpoints<'_>,
    creation: crate::expressions::CallCreation<'_, F>,
) -> Result<PureCallSpecialization, crate::expressions::ExpressionCreationFailure<H>> {
    let function = &call.binding;
    let distinct = call.distinct;
    let args = &call.arguments;
    let function_order_by = &call.order_by;
    let frame = &call.frame;
    let ignore_nulls = call.ignore_nulls;
    let aggregate_binding = &call.aggregate_binding;
    let site = PhysicalCallSite::Window {
        node: node.id,
        call: ordinal,
    };
    let frozen = package
        .calls()
        .entries()
        .get(&site)
        .ok_or(ExpressionLoweringError::Invalid(
            "missing frozen window call",
        ))?;
    let source_request = package.fragment().call_requests().get(site);
    work.step()?;
    let source_request = source_request.ok_or(ExpressionLoweringError::Invalid(
        "missing original window call request",
    ))?;
    let request = crate::original_requests::materialize_call_request_created(
        source_request,
        package.constants(),
        work,
        creation,
    )?;
    let argument_count = args
        .len()
        .checked_add(function_order_by.len())
        .ok_or(CompileControlError::ResourceExhausted)?;
    if request.request().logical_argument_count != args.len()
        || request.request().arguments.len() != argument_count
    {
        return Err(ExpressionLoweringError::Invalid(
            "window request differs from the logical and function sort channels",
        )
        .into());
    }
    work.flush()?;
    // The selection is the frozen binding itself, never a re-resolution.
    let selection = Arc::new(window_selection(function, aggregate_binding.as_deref()));
    work.flush()?;
    let lifecycle = match function.kind {
        FunctionKind::Window => PureCallLifecycle::Window,
        FunctionKind::Aggregate => PureCallLifecycle::AggregateWindow {
            distinct,
            ordered: !function_order_by.is_empty(),
        },
        _ => return Err(ExpressionLoweringError::UnsupportedCall(site).into()),
    };
    if !distinct && function_order_by.is_empty() {
        let owner = functions.metadata().pure_overload_declaration_observed(
            &function.function_id,
            function.kind,
            &function.overload,
            work.control(),
        )?;
        owner.admit_selected_lifecycle_observed(
            &selection,
            args.len(),
            lifecycle,
            work.control(),
        )?;
    }
    work.flush()?;
    crate::expressions::validate_call_created(
        functions.metadata(),
        &function.function_id,
        function.kind,
        selection.as_ref(),
        request.request(),
        work.control(),
        creation,
    )?;
    work.flush()?;
    let mut argument_uses = Vec::new();
    reserve_vec(&mut argument_uses, argument_count, work)?;
    let mut children = ExpressionEffects::PURE_VALUE;
    if argument_count != 0 {
        let unit = node
            .expressions
            .as_ref()
            .ok_or(ExpressionLoweringError::Invalid(
                "window node has no expression unit",
            ))?;
        let compiled = effects.unit(node.id)?;
        for argument in args
            .iter()
            .copied()
            .chain(function_order_by.iter().map(|key| key.expr))
        {
            let root = unit.root(argument).ok_or(ExpressionLoweringError::Invalid(
                "window argument root is absent",
            ))?;
            let child = unit
                .root_use(argument)
                .ok_or(ExpressionLoweringError::Invalid(
                    "window argument root has no actual use",
                ))?;
            let child_invocation =
                unit.control_flow()
                    .uses()
                    .get(&child)
                    .ok_or(ExpressionLoweringError::Invalid(
                        "missing window argument occurrence",
                    ))?;
            let region = unit
                .control_flow()
                .regions()
                .get(&child_invocation.context.region);
            work.step()?;
            if child_invocation.definition != root.definition
                || child_invocation.context.demand != EvaluationDemand::Value
                || !region.is_some_and(|region| region.parent.is_none() && region.guard.is_none())
            {
                return Err(ExpressionLoweringError::Invalid(
                    "window argument use differs from its actual call argument",
                )
                .into());
            }
            let summary =
                compiled
                    .effects()
                    .get(&child)
                    .ok_or(ExpressionLoweringError::Invalid(
                        "window argument effects were not prepared",
                    ))?;
            children = children.join(summary.for_use(child_invocation.context)?);
            argument_uses.push(Some(child));
            work.step()?;
        }
    }
    let window = WindowCallOptions::try_new(
        frozen_frame(package, frame.as_ref(), work)?,
        ignore_nulls,
        work.control(),
    )
    .map_err(FunctionSpecializationFailure::Kernel)?;
    work.flush()?;
    let arguments = ScopedRelationalEffects::primitive(frozen.context, children);
    let options = match (function.kind, aggregate_binding.as_deref()) {
        (FunctionKind::Window, None) if !distinct && function_order_by.is_empty() => {
            PureCallPreparation::Window {
                arguments,
                options: window,
            }
        }
        (FunctionKind::Aggregate, Some(binding)) if binding.phase == AggregatePhase::Single => {
            let state_interpretation = binding
                .state_interpretation()
                .map(|facts| facts.clone_observed(work).map(Arc::new))
                .transpose()?;
            let mut order_keys = Vec::new();
            reserve_vec(&mut order_keys, function_order_by.len(), work)?;
            for key in function_order_by {
                order_keys.push(AggregateOrderKey {
                    ascending: key.direction == SortDirection::Ascending,
                    nulls_first: key.null_ordering == NullOrdering::First,
                });
                work.step()?;
            }
            PureCallPreparation::AggregateWindow {
                arguments,
                options: AggregateWindowPreparationOptions {
                    aggregate: AggregatePreparationOptions {
                        state_interpretation,
                        phase: AggregateKernelPhase::Single,
                        distinct,
                        order_keys: order_keys.into(),
                        state_input_type: None,
                    },
                    window,
                },
            }
        }
        _ => return Err(ExpressionLoweringError::UnsupportedCall(site).into()),
    };
    let input = CallEffectInput {
        context: frozen.context.into(),
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
        function_id: &function.function_id,
        kind: function.kind,
        selected: selection.as_ref(),
        request: request.request(),
        environment: &frozen.effects.environment,
        parameters: package.parameters(),
        decimal_overflow_policy: frozen.decimal_overflow_policy,
        proof_scope: frozen.effects.proof_scope,
    };
    let token = crate::expressions::prepare_call_created(
        functions,
        input,
        Arc::clone(&selection),
        &frozen.effects,
        options,
        work.control(),
        creation,
    )?;
    work.flush()?;
    Ok(token)
}
