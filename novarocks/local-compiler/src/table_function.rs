// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Lower one admitted physical TableFunction into a subordinate argument
//! Project and the local TableFunction owner, and prepare its exact frozen
//! relational call.
//!
//! Shape: the subordinate Project reads the one outer input. Its columns are
//! every distinct pass-through value, in first output order, as compiler
//! authored slot reads (the same fresh-occurrence mechanism a Union
//! normalizer uses), followed by one column per call argument whose root is
//! the physical `TableFunctionArgument` root moved onto that Project. The
//! TableFunction reads its parameters and outer pass-through values from the
//! Project's columns and publishes the physical output under fresh slots.
//!
//! The call is prepared through the installed pure TableV1 owner from the
//! frozen binding, the original request, the frozen call effects and the
//! actual effects of its argument roots. There is no name dispatch: the local
//! `function_name` is a diagnostic tag only. A table function that has no
//! installed pure TableV1 kernel, or that has no exactly one outer input, is
//! an explicit refusal.

use crate::{
    assert_rows::reserve_vec,
    channels::{ChannelLoweringError, NodeChannels, ResolvedInput, UnionChannelBranch},
    expressions::ExpressionLoweringError,
    lowering::FragmentCompileError,
};
use arrow_schema::DataType;
use novarocks_functions::{
    CallEffectInput, EngineFunctionCatalog, FunctionBindingSelection, FunctionResultType,
    PureCallPreparation, PureCallSpecialization, PureEngineFunctionCatalog, PureKernelAbi,
    ScopedRelationalEffects,
};
use novarocks_local_program::{
    DiagnosticSourceNodeId, LocalOperatorId, LocalOperatorOrigin, LocalOperatorProvenance,
    MetricAggregation, OperatorMetricAggregation, ProgramCallSite, ProgramChannelLayoutRole,
    ProgramChannelSite, ProgramNode, ProgramNodeId, ProgramNodeKind, ProgramRootRef, StaticLayout,
    TableFunctionOutputSlot,
};
use novarocks_physical_plan::{
    BoundTableFunction, Fragment, FragmentPackage, FunctionArgumentType, NodeId, NodeKind,
    PhysicalCallSite, PhysicalNode, TableFunctionOutput, ValueId,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, ExpressionEffects,
    FunctionKind, FunctionValueType, PureCompileControl,
};
use novarocks_types::SlotId;
use std::{collections::BTreeMap, sync::Arc};

/// Admit one physical TableFunction occurrence before any lowering work. The
/// installed kernel is found through the pure catalog by the frozen function
/// identity and overload, never by a SQL name.
pub(crate) fn admit_table_function(
    node: &PhysicalNode,
    functions: &PureEngineFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    let NodeKind::TableFunction {
        function, outputs, ..
    } = &node.kind
    else {
        return Err(FragmentCompileError::Invalid(
            "table function admission for another node family",
        ));
    };
    match node.inputs.len() {
        0 => {
            return Err(FragmentCompileError::Unsupported {
                node: Some(node.id),
                feature: "standalone table function without an outer input",
            });
        }
        1 => {}
        _ => {
            return Err(FragmentCompileError::Unsupported {
                node: Some(node.id),
                feature: "table function over more than one input",
            });
        }
    }
    if outputs.len() != node.output.columns.len() {
        return Err(FragmentCompileError::Invalid(
            "table function output mapping width differs from its output",
        ));
    }
    for argument in function.argument_types.iter() {
        let lambda = matches!(argument, FunctionArgumentType::Lambda { .. });
        work.step()?;
        if lambda {
            return Err(FragmentCompileError::Unsupported {
                node: Some(node.id),
                feature: "table function with a lambda argument",
            });
        }
    }
    work.flush()?;
    let abi = table_function_abi(function, functions.metadata(), work.control());
    work.flush()?;
    if abi? != PureKernelAbi::TableV1 {
        return Err(FragmentCompileError::Unsupported {
            node: Some(node.id),
            feature: "table function without an installed pure TableV1 kernel",
        });
    }
    Ok(())
}

/// Preserve the exact metadata lookup cause before capability classification.
/// The production caller supplies metadata from its sealed pure catalog.
pub(super) fn table_function_abi(
    function: &BoundTableFunction,
    functions: &EngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<PureKernelAbi, FragmentCompileError> {
    match functions.pure_overload_declaration_observed(
        &function.function_id,
        FunctionKind::Table,
        &function.overload,
        control,
    ) {
        Ok(declaration) => Ok(declaration.implementation().abi),
        Err(novarocks_functions::FunctionSpecializationFailure::Control(cause)) => {
            Err(cause.into())
        }
        Err(error) => Err(ExpressionLoweringError::Specialization(error).into()),
    }
}

/// Additional resource upper bound of one TableFunction beyond its own node
/// and output channels: its subordinate Project node, that Project's output
/// channels and the relation's result channels, and one derived definition per
/// pass-through output.
pub(crate) fn resource_bound(
    node: &PhysicalNode,
) -> Result<Option<(usize, usize, usize)>, FragmentCompileError> {
    let NodeKind::TableFunction {
        function,
        arguments,
        outputs,
        ..
    } = &node.kind
    else {
        return Ok(None);
    };
    let pass_through = outputs
        .iter()
        .filter(|output| matches!(output, TableFunctionOutput::PassThrough(_)))
        .count();
    let channels = pass_through
        .checked_add(arguments.len())
        .and_then(|width| width.checked_add(function.result_types.len()))
        .ok_or(CompileControlError::ResourceExhausted)?;
    Ok(Some((1, channels, pass_through)))
}

/// One TableFunction's planned channels.
pub(crate) struct PlannedTableFunction {
    /// The subordinate argument Project, directly before the table function.
    pub project: ProgramNodeId,
    pub node: ProgramNodeId,
    /// Distinct pass-through values in first output order, with their exact
    /// outer input channel.
    pub pass_through: Vec<(ValueId, ResolvedInput)>,
    /// The Project's output: one slot per pass-through value, then one per
    /// call argument.
    pub project_slots: Arc<[SlotId]>,
    /// One slot per produced relation column, projected or not.
    pub results: Arc<[SlotId]>,
}

pub(crate) struct PlannedTableFunctionChannels {
    pub planned: PlannedTableFunction,
    /// The Project's pass-through reads, authored like a Union normalizer's.
    pub selection: UnionChannelBranch,
    pub slots: Arc<[SlotId]>,
    pub port: BTreeMap<ValueId, usize>,
}

/// Plan the subordinate Project and the table function over the one outer
/// input. Every published output occurrence is a fresh channel; a repeated
/// output value is one produced value, so its first ordinal represents it.
pub(crate) fn plan_table_function_channels(
    fragment: &Fragment,
    node: &PhysicalNode,
    local: ProgramNodeId,
    child: &NodeChannels,
    child_port: &BTreeMap<ValueId, usize>,
    next_slot: &mut u64,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PlannedTableFunctionChannels, ChannelLoweringError> {
    let NodeKind::TableFunction {
        function,
        arguments,
        outputs,
        ..
    } = &node.kind
    else {
        return Err(ChannelLoweringError::Invalid(
            "table function planning for another node family",
        ));
    };
    if node.inputs.len() != 1 || outputs.len() != node.output.columns.len() {
        return Err(ChannelLoweringError::Invalid(
            "table function occurrence shape differs",
        ));
    }
    let project = ProgramNodeId::new(local.index().checked_sub(1).ok_or(
        ChannelLoweringError::Invalid("table function has no argument Project position"),
    )?);
    let mut pass_through: Vec<(ValueId, ResolvedInput)> = Vec::new();
    let mut sources = Vec::new();
    reserve_vec(&mut pass_through, outputs.len(), work)?;
    reserve_vec(&mut sources, outputs.len(), work)?;
    let mut port = BTreeMap::new();
    for (ordinal, (output, &value)) in outputs.iter().zip(node.output.columns.iter()).enumerate() {
        let same = output.value() == value;
        work.step()?;
        if !same {
            return Err(ChannelLoweringError::Invalid(
                "table function output mapping differs from its output",
            ));
        }
        if !fragment.values().contains_key(&value) {
            return Err(ChannelLoweringError::Invalid(
                "missing table function output value",
            ));
        }
        port.entry(value).or_insert(ordinal);
        if let TableFunctionOutput::PassThrough(value) = output
            && !pass_through.iter().any(|(seen, _)| seen == value)
        {
            let input = crate::channels::resolve_input(*value, child, child_port)?;
            let ty = fragment
                .values()
                .get(value)
                .ok_or(ChannelLoweringError::Invalid(
                    "missing table function pass-through value",
                ))?
                .ty
                .clone();
            pass_through.push((*value, input));
            sources.push(crate::channels::UnionChannelSource { input, ty });
        }
        work.step()?;
    }
    let width = pass_through
        .len()
        .checked_add(arguments.len())
        .ok_or(CompileControlError::ResourceExhausted)?;
    let project_slots = fresh_slots(width, next_slot, work)?;
    let results = fresh_slots(function.result_types.len(), next_slot, work)?;
    let slots = fresh_slots(node.output.columns.len(), next_slot, work)?;
    Ok(PlannedTableFunctionChannels {
        planned: PlannedTableFunction {
            project,
            node: local,
            pass_through,
            project_slots,
            results,
        },
        selection: UnionChannelBranch {
            normalizer: project,
            input: child.local,
            sources,
        },
        slots,
        port,
    })
}

fn fresh_slots(
    count: usize,
    next_slot: &mut u64,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<[SlotId]>, ChannelLoweringError> {
    let mut slots = Vec::new();
    reserve_vec(&mut slots, count, work)?;
    for _ in 0..count {
        let slot = u32::try_from(*next_slot)
            .map_err(|_| ChannelLoweringError::Invalid("slot identity exhausted"))?;
        *next_slot = next_slot
            .checked_add(1)
            .ok_or(ChannelLoweringError::Invalid("slot identity exhausted"))?;
        slots.push(SlotId::new(slot));
        work.step()?;
    }
    work.flush()?;
    let slots: Arc<[SlotId]> = Arc::from(slots);
    work.flush()?;
    Ok(slots)
}

pub(crate) struct LoweredTableFunction {
    pub nodes: Vec<ProgramNode>,
    pub channels: Vec<(ProgramChannelSite, FunctionValueType)>,
    pub operators: Vec<LocalOperatorProvenance>,
}

/// Lower one planned table function into its argument Project and the local
/// TableFunction. `pass_through_definitions` are the Project's synthesized
/// slot-read definitions, in planned pass-through order.
pub(crate) fn lower_table_function_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    planned: &PlannedTableFunction,
    published: &[SlotId],
    child: ProgramNodeId,
    expressions: &crate::expressions::RootMap,
    pass_through_definitions: &[ProgramRootRef],
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<LoweredTableFunction, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core_with_creation(
        package,
        node,
        planned,
        published,
        child,
        expressions,
        pass_through_definitions,
        &mut work,
        creation,
    );
    crate::creation::finish_attempt(result, work)
}

fn lower_core_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    planned: &PlannedTableFunction,
    published: &[SlotId],
    child: ProgramNodeId,
    expressions: &crate::expressions::RootMap,
    pass_through_definitions: &[ProgramRootRef],
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<LoweredTableFunction, crate::FragmentCreationError<H>> {
    let fragment = package.fragment();
    let NodeKind::TableFunction {
        function,
        arguments,
        outputs,
        left_outer,
    } = &node.kind
    else {
        return Err((FragmentCompileError::Invalid(
            "table function lowering for another node family",
        ))
        .into());
    };
    let pass = planned.pass_through.len();
    if pass_through_definitions.len() != pass
        || planned.project_slots.len() != pass + arguments.len()
        || planned.results.len() != function.result_types.len()
        || published.len() != outputs.len()
        || published.len() != node.output.columns.len()
        || function.argument_types.len() != arguments.len()
    {
        return Err((FragmentCompileError::Invalid(
            "table function planned width differs from its occurrence",
        ))
        .into());
    }
    // The argument Project: pass-through reads, then the argument roots.
    let mut exprs = Vec::new();
    let mut project_types = Vec::new();
    reserve_vec(&mut exprs, planned.project_slots.len(), work)?;
    reserve_vec(&mut project_types, planned.project_slots.len(), work)?;
    for ((value, _input), &definition) in planned.pass_through.iter().zip(pass_through_definitions)
    {
        let ty = fragment
            .values()
            .get(value)
            .ok_or(FragmentCompileError::Invalid(
                "missing table function pass-through value",
            ))?
            .ty
            .clone();
        exprs.push(definition);
        project_types.push(ty);
        work.step()?;
    }
    let mut param_types = Vec::new();
    reserve_vec(&mut param_types, arguments.len(), work)?;
    for (argument, bound) in arguments.iter().zip(function.argument_types.iter()) {
        let definition = crate::expressions::bound_root(package, node.id, *argument)?;
        let FunctionArgumentType::Value(bound) = bound else {
            return Err((FragmentCompileError::Unsupported {
                node: Some(node.id),
                feature: "table function with a lambda argument",
            })
            .into());
        };
        exprs.push(
            *expressions
                .get(argument)
                .ok_or(FragmentCompileError::Invalid(
                    "missing lowered table function argument",
                ))?,
        );
        project_types.push(definition.ty.clone());
        param_types.push(bound.data_type().clone());
        work.step()?;
    }
    let project_layout = layout_with_creation(
        package,
        node,
        planned.project,
        &project_types,
        &planned.project_slots,
        false,
        work,
        creation,
    )?;
    let mut channels = Vec::new();
    let channel_count = project_types
        .len()
        .checked_add(outputs.len())
        .and_then(|count| count.checked_add(function.result_types.len()))
        .ok_or(CompileControlError::ResourceExhausted)?;
    reserve_vec(&mut channels, channel_count, work)?;
    for (ordinal, ty) in project_types.into_iter().enumerate() {
        channels.push((
            ProgramChannelSite::Layout {
                node: planned.project,
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
            },
            ty,
        ));
        work.step()?;
    }
    // The table function: each output occurrence is an outer pass-through
    // channel of the Project or one produced relation column.
    let mut output_types = Vec::new();
    let mut output_slot_sources = Vec::new();
    reserve_vec(&mut output_types, outputs.len(), work)?;
    reserve_vec(&mut output_slot_sources, outputs.len(), work)?;
    for output in outputs.iter() {
        let source = match output {
            TableFunctionOutput::PassThrough(value) => {
                let ordinal = planned
                    .pass_through
                    .iter()
                    .position(|(seen, _)| seen == value)
                    .ok_or(FragmentCompileError::Invalid(
                        "table function pass-through value was not planned",
                    ))?;
                TableFunctionOutputSlot::Outer {
                    slot: planned.project_slots[ordinal],
                }
            }
            TableFunctionOutput::FunctionResult { result_ordinal, .. } => {
                let index = usize::try_from(*result_ordinal)
                    .ok()
                    .filter(|index| *index < function.result_types.len())
                    .ok_or(FragmentCompileError::Invalid(
                        "table function result ordinal is outside its relation",
                    ))?;
                TableFunctionOutputSlot::Result { index }
            }
        };
        output_types.push(
            fragment
                .values()
                .get(&output.value())
                .ok_or(FragmentCompileError::Invalid(
                    "missing table function output value",
                ))?
                .ty
                .clone(),
        );
        output_slot_sources.push(source);
        work.step()?;
    }
    let output_layout = layout_with_creation(
        package,
        node,
        planned.node,
        &output_types,
        published,
        true,
        work,
        creation,
    )?;
    for (ordinal, ty) in output_types.into_iter().enumerate() {
        channels.push((
            ProgramChannelSite::Layout {
                node: planned.node,
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
            },
            ty,
        ));
        work.step()?;
    }
    // Every produced relation column has its exact bound type, including an
    // unprojected one; a LEFT OUTER output widens only its own channel.
    let mut ret_types: Vec<DataType> = Vec::new();
    reserve_vec(&mut ret_types, function.result_types.len(), work)?;
    for (ordinal, ty) in function.result_types.iter().enumerate() {
        ret_types.push(ty.data_type().clone());
        channels.push((
            ProgramChannelSite::TableResult {
                node: planned.node,
                result: u32::try_from(ordinal)
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
            },
            ty.clone(),
        ));
        work.step()?;
    }
    let source = DiagnosticSourceNodeId::new(node.id.get());
    let owner = operator_id(planned.node)?;
    let metrics = OperatorMetricAggregation {
        cpu_time: MetricAggregation::Sum,
        wall_time: MetricAggregation::Maximum,
        peak_retained_bytes: MetricAggregation::Maximum,
    };
    let mut nodes = Vec::new();
    reserve_vec(&mut nodes, 2, work)?;
    let mut operators = Vec::new();
    reserve_vec(&mut operators, 2, work)?;
    nodes.push(ProgramNode::new_local(
        planned.project,
        vec![source],
        ProgramNodeKind::Project {
            input: child,
            is_subordinate: true,
            validate_final_result_input: false,
            exprs,
            expr_slot_ids: planned.project_slots.to_vec(),
            expr_slot_schemas: None,
            output_indices: None,
        },
        project_layout,
    ));
    operators.push(LocalOperatorProvenance {
        id: operator_id(planned.project)?,
        lowered_nodes: Box::from([planned.project]),
        sources: Box::from([source]),
        origin: LocalOperatorOrigin::Split { piece: 0 },
        cost_owner: owner,
        metrics,
    });
    nodes.push(ProgramNode::new_local(
        planned.node,
        vec![source],
        ProgramNodeKind::TableFunction {
            input: planned.project,
            // A diagnostic tag only; execution resolves the prepared call.
            function_name: Arc::from(function.function_id.as_str()),
            param_slots: planned.project_slots[pass..].to_vec(),
            outer_slots: planned.project_slots[..pass].to_vec(),
            fn_result_slots: planned.results.to_vec(),
            fn_result_required: true,
            is_left_join: *left_outer,
            param_types,
            ret_types,
            output_slot_sources,
        },
        output_layout,
    ));
    operators.push(LocalOperatorProvenance {
        id: owner,
        lowered_nodes: Box::from([planned.node]),
        sources: Box::from([source]),
        origin: LocalOperatorOrigin::Split { piece: 1 },
        cost_owner: owner,
        metrics,
    });
    Ok(LoweredTableFunction {
        nodes,
        channels,
        operators,
    })
}

fn operator_id(node: ProgramNodeId) -> Result<LocalOperatorId, FragmentCompileError> {
    Ok(LocalOperatorId::new(
        u32::try_from(node.index()).map_err(|_| CompileControlError::ResourceExhausted)?,
    ))
}

/// A layout named with the result labels only when `labels` is set and the
/// entire ordered physical output is the result port's.
fn layout_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    types: &[FunctionValueType],
    slots: &[SlotId],
    labels: bool,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<StaticLayout, crate::FragmentCreationError<H>> {
    let result = package
        .result()
        .filter(|result| labels && result.output.columns == node.output.columns);
    let mut typed_layout =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(types.len(), work, creation)?;
    for (ordinal, ty) in types.iter().enumerate() {
        let name = match result {
            Some(result) => {
                let field = result
                    .fields
                    .get(ordinal)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing table function result label",
                    ))?;
                crate::typed_layout::copy_name_with_creation(
                    field.alias.as_deref().unwrap_or(&field.name),
                    work,
                    creation,
                )?
            }
            None => crate::typed_layout::local_name_with_creation(
                "local",
                local.index(),
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
    work.flush()?;
    Ok(layout)
}

/// A `TableFunctionArgument` root belongs to the argument Project, after the
/// pass-through reads.
/// Prepare every frozen table call of the fragment through its exact
/// installed owner. `effects` holds the prepared effects of every expression
/// occurrence, so each argument root contributes its actual effects; `nodes`
/// names the local node each physical node lowers to.
pub(crate) fn prepare_table_calls<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
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
        if !matches!(node.kind, NodeKind::TableFunction { .. }) {
            continue;
        }
        let local = *nodes.get(&node.id).ok_or(ExpressionLoweringError::Invalid(
            "table function node has no local node",
        ))?;
        let token = prepare_call(package, functions, effects, node, work, creation)?;
        if tokens
            .insert(ProgramCallSite::Table { node: local }, token)
            .is_some()
        {
            return Err(
                ExpressionLoweringError::Invalid("table call site is prepared twice").into(),
            );
        }
        prepared += 1;
        work.step()?;
    }
    // Every frozen table claim names one actual table function.
    let mut frozen = 0usize;
    for site in package.calls().entries().keys() {
        if matches!(site, PhysicalCallSite::Table { .. }) {
            frozen += 1;
        }
        work.step()?;
    }
    if frozen != prepared {
        return Err(ExpressionLoweringError::Invalid(
            "frozen table calls differ from the actual table functions",
        )
        .into());
    }
    Ok(())
}

fn prepare_call<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    functions: &PureEngineFunctionCatalog,
    effects: &crate::expressions::LoweredExpressions,
    node: &PhysicalNode,
    work: &mut CompileCheckpoints<'_>,
    creation: crate::expressions::CallCreation<'_, F>,
) -> Result<PureCallSpecialization, crate::expressions::ExpressionCreationFailure<H>> {
    let NodeKind::TableFunction {
        function,
        arguments,
        ..
    } = &node.kind
    else {
        return Err(ExpressionLoweringError::Invalid(
            "table call preparation for another node family",
        )
        .into());
    };
    let site = PhysicalCallSite::Table { node: node.id };
    let frozen = package
        .calls()
        .entries()
        .get(&site)
        .ok_or(ExpressionLoweringError::Invalid(
            "missing frozen table call",
        ))?;
    let source_request = package.fragment().call_requests().get(site);
    work.step()?;
    let source_request = source_request.ok_or(ExpressionLoweringError::Invalid(
        "missing original table call request",
    ))?;
    let request = crate::original_requests::materialize_call_request_created(
        source_request,
        package.constants(),
        work,
        creation,
    )?;
    work.flush()?;
    // The selection is the frozen binding itself, never a re-resolution.
    let selection = Arc::new(FunctionBindingSelection {
        overload: function.overload.clone(),
        argument_types: function.argument_types.clone(),
        result_type: FunctionResultType::Relation(function.result_types.clone()),
        aggregate: None,
    });
    work.flush()?;
    crate::expressions::validate_call_created(
        functions.metadata(),
        &function.function_id,
        FunctionKind::Table,
        selection.as_ref(),
        request.request(),
        work.control(),
        creation,
    )?;
    work.flush()?;
    // Each argument root is an independent unguarded Value occurrence whose
    // definition is the call's ordered argument.
    let mut argument_uses = Vec::new();
    reserve_vec(&mut argument_uses, arguments.len(), work)?;
    let mut children = ExpressionEffects::PURE_VALUE;
    for &definition in arguments.iter() {
        let unit = node
            .expressions
            .as_ref()
            .ok_or(ExpressionLoweringError::Invalid(
                "table node has no expression unit",
            ))?;
        let compiled = effects.unit(node.id)?;
        let flow = unit.control_flow();

        let use_id = unit
            .root_use(definition)
            .ok_or(ExpressionLoweringError::Invalid(
                "table argument root has no actual use",
            ))?;
        let root = unit
            .root(definition)
            .ok_or(ExpressionLoweringError::Invalid(
                "table argument root is absent",
            ))?;
        let invocation = flow
            .uses()
            .get(&use_id)
            .ok_or(ExpressionLoweringError::Invalid(
                "table argument root has no invocation",
            ))?;
        let domain = flow.regions().get(&invocation.context.region);
        work.step()?;
        let exact = invocation.definition == root.definition
            && invocation.context.use_id == use_id
            && invocation.context.demand == EvaluationDemand::Value
            && domain.is_some_and(|domain| domain.parent.is_none() && domain.guard.is_none());
        if !exact {
            return Err(ExpressionLoweringError::Invalid(
                "table argument root differs from its actual call argument",
            )
            .into());
        }
        let summary = compiled
            .effects()
            .get(&use_id)
            .ok_or(ExpressionLoweringError::Invalid(
                "table argument effects were not prepared",
            ))?;
        children = children.join(summary.for_use(invocation.context)?);
        argument_uses.push(Some(use_id));
        work.step()?;
    }
    work.flush()?;
    let input = CallEffectInput {
        context: frozen.context.into(),
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
        function_id: &function.function_id,
        kind: FunctionKind::Table,
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
        PureCallPreparation::Table {
            arguments: ScopedRelationalEffects::primitive(frozen.context, children),
        },
        work.control(),
        creation,
    )?;
    work.flush()?;
    Ok(token)
}
