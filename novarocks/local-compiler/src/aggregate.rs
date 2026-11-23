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

//! Lower one admitted physical Aggregate into the local aggregate owner and
//! prepare each of its calls from the exact frozen relational facts.
//!
//! Every call is prepared through its installed pure owner with the frozen
//! binding, the original request, the frozen call effects and the actual
//! effects of its argument roots. No implementation is selected by name: the
//! display name copied into the local call is a diagnostic tag only. The
//! physical grouping guarantee travels as a compiled program fact, because a
//! node's finalization does not imply it.

use crate::{
    assert_rows::reserve_vec, expressions::ExpressionLoweringError, lowering::FragmentCompileError,
};
use novarocks_functions::{
    AggregateBindingSelection, AggregateKernelPhase, AggregateOverloadIdentity,
    AggregatePreparationOptions, CallEffectInput, FunctionBindingSelection, FunctionResultType,
    PureCallPreparation, PureCallSpecialization, PureEngineFunctionCatalog,
    ResolvedAggregateSignature, ScopedRelationalEffects,
};
use novarocks_local_program::{
    CompiledAggregate, CompiledAggregateGrouping, ProgramCallSite, ProgramNodeId, ProgramNodeKind,
    StaticAggregateCall, StaticAggregateOrder, StaticLayout,
};
use novarocks_physical_plan::{
    AggregateCall, AggregateGrouping, AggregatePhase, FragmentPackage, FunctionArgumentType,
    NodeId, NodeKind, PhysicalCallSite, PhysicalNode,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, ExpressionEffects, FunctionKind,
    PureCompileControl,
};
use novarocks_types::SlotId;
use std::{collections::BTreeMap, sync::Arc};

/// The local kernel phase of one physical call phase. Sequence identity stays
/// a checked physical fact; the local owner needs only the lifecycle.
pub(crate) const fn kernel_phase(phase: AggregatePhase) -> AggregateKernelPhase {
    match phase {
        AggregatePhase::Single => AggregateKernelPhase::Single,
        AggregatePhase::Partial { .. } => AggregateKernelPhase::Partial,
        AggregatePhase::Intermediate { .. } => AggregateKernelPhase::Intermediate,
        AggregatePhase::Final { .. } => AggregateKernelPhase::Final,
    }
}

pub(crate) struct LoweredAggregate {
    pub kind: ProgramNodeKind,
    pub layout: StaticLayout,
    pub fact: CompiledAggregate,
}

/// Lower one Aggregate whose output is its group values followed by one value
/// per call, in call order. Function ORDER channels follow logical arguments
/// and retain their original state interpretation across merge phases.
pub(crate) fn lower_aggregate_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    input: ProgramNodeId,
    expressions: &crate::expressions::RootMap,
    slots: &[SlotId],
    topn_filters: novarocks_local_program::AggregateTopNFilters,
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<LoweredAggregate, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core_with_creation(
        package,
        node,
        input,
        expressions,
        slots,
        topn_filters,
        &mut work,
        creation,
    );
    crate::creation::finish_attempt(result, work)
}

fn lower_core_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    input: ProgramNodeId,
    expressions: &crate::expressions::RootMap,
    slots: &[SlotId],
    topn_filters: novarocks_local_program::AggregateTopNFilters,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<LoweredAggregate, crate::FragmentCreationError<H>> {
    let NodeKind::Aggregate {
        group_by,
        calls,
        grouping,
    } = &node.kind
    else {
        return Err((FragmentCompileError::Invalid("Aggregate kind differs")).into());
    };
    let width = group_by
        .len()
        .checked_add(calls.len())
        .ok_or(FragmentCompileError::Invalid("aggregate width exhausted"))?;
    if node.inputs.len() != 1 || node.output.columns.len() != width || slots.len() != width {
        return Err((FragmentCompileError::Invalid(
            "aggregate input, output or channel width differs",
        ))
        .into());
    }
    // The output is exactly the group values followed by the call outputs.
    let expected = group_by
        .iter()
        .map(|(_, value)| *value)
        .chain(calls.iter().map(|call| call.output));
    for (actual, expected) in node.output.columns.iter().zip(expected) {
        work.step()?;
        if *actual != expected {
            return Err((FragmentCompileError::Invalid(
                "aggregate output is not its group values followed by its call outputs",
            ))
            .into());
        }
    }
    let values = package.fragment().values();

    let mut keys = Vec::new();
    reserve_vec(&mut keys, group_by.len(), work)?;
    for (expression, value) in group_by.iter() {
        let definition = crate::expressions::bound_root(package, node.id, *expression)?;
        let output = values
            .get(value)
            .ok_or(FragmentCompileError::Invalid("missing group value"))?;
        work.flush()?;
        let same = definition
            .ty
            .exactly_equals_observed::<crate::FragmentCreationError<H>>(&output.ty, || {
                work.step().map_err(Into::into)
            })?;
        if !same {
            return Err((FragmentCompileError::Invalid(
                "group value type differs from its definition",
            ))
            .into());
        }
        keys.push(
            *expressions
                .get(expression)
                .ok_or(FragmentCompileError::Invalid(
                    "missing lowered group definition",
                ))?,
        );
        work.step()?;
    }
    let mut functions = Vec::new();
    reserve_vec(&mut functions, calls.len(), work)?;
    let mut finalizing = None;
    let mut merging = true;
    for call in calls.iter() {
        let phase = call.binding.phase;
        if finalizing.replace(phase.produces_final_result()) == Some(!phase.produces_final_result())
        {
            return Err((FragmentCompileError::Invalid(
                "aggregate mixes finalizing and non-finalizing calls",
            ))
            .into());
        }
        merging &= !phase.consumes_logical_arguments();
        functions.push(lower_call(call, expressions, work, creation)?);
        work.step()?;
    }
    // A finalizing call requires a complete grouping. A node without calls
    // finalizes exactly when its groups are complete.
    let need_finalize = finalizing.unwrap_or(*grouping == AggregateGrouping::Complete);
    if need_finalize && *grouping != AggregateGrouping::Complete {
        return Err(
            (FragmentCompileError::Invalid("finalizing aggregate has a partial grouping")).into(),
        );
    }
    let layout = lower_output_layout_with_creation(
        package,
        node,
        slots,
        width,
        "local_aggregate",
        work,
        creation,
    )?;
    Ok(LoweredAggregate {
        kind: ProgramNodeKind::Aggregate {
            input,
            group_by: keys,
            functions,
            need_finalize,
            input_is_intermediate: !calls.is_empty() && merging,
            topn_filters,
            // Streaming pre-aggregation remains outside this installed route.
            streaming_preaggregation_mode: None,
        },
        layout,
        fact: CompiledAggregate {
            grouping: match grouping {
                AggregateGrouping::Partial => CompiledAggregateGrouping::Partial,
                AggregateGrouping::Complete => CompiledAggregateGrouping::Complete,
            },
        },
    })
}

/// Materialize this relation's actual output ValueDefs, not its child's fields.
pub(crate) fn lower_output_layout_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    slots: &[SlotId],
    width: usize,
    family: &str,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<StaticLayout, crate::FragmentCreationError<H>> {
    let values = package.fragment().values();
    let result = package
        .result()
        .filter(|result| result.output.columns == node.output.columns);
    let mut typed_layout =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(width, work, creation)?;
    for (ordinal, value) in node.output.columns.iter().enumerate() {
        let ty = &values
            .get(value)
            .ok_or(FragmentCompileError::Invalid(
                "missing aggregate output type",
            ))?
            .ty;
        // Full result labels are authoritative only when this aggregate's
        // entire ordered output is the result port's, whichever node
        // publishes it.
        let name = match result {
            Some(result) => {
                let field = result
                    .fields
                    .get(ordinal)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing aggregate result label",
                    ))?;
                crate::typed_layout::copy_name_with_creation(
                    field.alias.as_deref().unwrap_or(&field.name),
                    work,
                    creation,
                )?
            }
            None => crate::typed_layout::local_name_with_creation(
                family,
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
    work.flush()?;
    Ok(layout)
}

/// Copy the full selection from its physical author's immutable declaration.
/// No carrier/root-domain projection can author this retained selection.
pub(crate) fn freeze_binding_with_creation<H>(
    binding: &novarocks_physical_plan::AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<Arc<novarocks_local_program::FrozenAggregateSelection>, crate::FragmentCreationError<H>>
{
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    let function = &binding.function;
    if function.kind != FunctionKind::Aggregate {
        return Err(FragmentCompileError::Invalid(
            "frozen aggregate selection is not an aggregate",
        )
        .into());
    }
    // These entities are immutable guarded owners. Validate channel roles
    // before sharing them; no label or argument backing is reconstructed.
    let function_id = function.function_id.clone_observed(|| work.step())?;
    let overload = function.overload.clone_observed(|| work.step())?;
    for argument in &function.argument_types {
        work.step()?;
        if !matches!(argument, FunctionArgumentType::Value(_)) {
            return Err(FragmentCompileError::Invalid("aggregate channel is not a value").into());
        }
    }
    let argument_types = function.argument_types.clone_observed(|| work.step())?;
    let result_type =
        FunctionResultType::Scalar(function.result_type.clone_observed(|| work.step())?);
    let intermediate_type = binding.intermediate_type.clone_observed(|| work.step())?;
    let state_format = binding.state_format.clone_observed(|| work.step())?;
    crate::creation::request(
        arc_layout(std::alloc::Layout::new::<FunctionBindingSelection>())?,
        creation,
        work,
    )?;
    let selected = Arc::new(FunctionBindingSelection {
        overload,
        argument_types,
        result_type,
        aggregate: Some(AggregateBindingSelection {
            state_argument_contract: binding.state_argument_contract,
            intermediate_type,
            state_format,
        }),
    });
    let frozen = novarocks_local_program::FrozenAggregateSelection::try_new(
        function_id,
        selected,
        binding.logical_argument_count as usize,
        creation.storage_lifetime(),
    )
    .map_err(|error| FragmentCompileError::Owner {
        phase: "frozen aggregate selection",
        error: novarocks_failure_contract::SharedFailureCause::new(error),
    })?;
    crate::creation::request(
        novarocks_local_program::FrozenAggregateSelection::storage_allocation_layout()?,
        creation,
        work,
    )?;
    Ok(Arc::new(frozen))
}

pub(crate) fn lower_call<H>(
    call: &AggregateCall,
    expressions: &crate::expressions::RootMap,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<StaticAggregateCall, crate::FragmentCreationError<H>> {
    let binding = &call.binding;
    let function = &binding.function;
    if function.kind != FunctionKind::Aggregate {
        return Err(
            FragmentCompileError::Invalid("aggregate call binds a non-aggregate function").into(),
        );
    }
    let mut inputs = Vec::new();
    let input_count = call
        .arguments
        .len()
        .checked_add(call.order_by.len())
        .ok_or(FragmentCompileError::Invalid(
            "aggregate channel count exhausted",
        ))?;
    reserve_vec(&mut inputs, input_count, work)?;
    for argument in call
        .arguments
        .iter()
        .chain(call.order_by.iter().map(|key| &key.expr))
    {
        inputs.push(
            *expressions
                .get(argument)
                .ok_or(FragmentCompileError::Invalid(
                    "missing lowered aggregate argument",
                ))?,
        );
        work.step()?;
    }
    let mut argument_types = Vec::new();
    reserve_vec(&mut argument_types, function.argument_types.len(), work)?;
    for argument in function.argument_types.iter() {
        let FunctionArgumentType::Value(value) = argument else {
            return Err(FragmentCompileError::Invalid("aggregate channel is not a value").into());
        };
        work.flush()?;
        argument_types.push(value.data_type().clone());
        work.step()?;
    }
    let overload =
        AggregateOverloadIdentity::try_new(function.overload.as_str()).map_err(|error| {
            FragmentCompileError::Owner {
                phase: "aggregate overload identity",
                error: novarocks_failure_contract::SharedFailureCause::new(error),
            }
        })?;
    work.flush()?;
    let name: Arc<str> = Arc::from(function.function_id.as_str());
    work.flush()?;
    let mut ascending = Vec::new();
    let mut nulls_first = Vec::new();
    reserve_vec(&mut ascending, call.order_by.len(), work)?;
    reserve_vec(&mut nulls_first, call.order_by.len(), work)?;
    for key in &call.order_by {
        ascending.push(key.direction == novarocks_physical_plan::SortDirection::Ascending);
        nulls_first.push(key.null_ordering == novarocks_physical_plan::NullOrdering::First);
        work.step()?;
    }
    let frozen = freeze_binding_with_creation(binding, work, creation)?;
    let filter_root = call
        .filter_root
        .map(|root| {
            expressions
                .get(&root)
                .copied()
                .ok_or(FragmentCompileError::Invalid(
                    "missing lowered aggregate filter input",
                ))
        })
        .transpose()?;
    Ok(StaticAggregateCall {
        filter_root,
        frozen: Some(frozen),
        state_interpretation: binding
            .state_interpretation
            .as_ref()
            .map(|value| value.clone_observed(work))
            .transpose()?,
        // A diagnostic tag only; the frozen preparation owns the call.
        name,
        inputs,
        input_is_intermediate: !binding.phase.consumes_logical_arguments(),
        types: None,
        order: StaticAggregateOrder {
            is_asc_order: ascending,
            nulls_first,
            is_distinct: call.distinct,
            group_concat_max_len: None,
        },
        resolved: ResolvedAggregateSignature {
            overload,
            argument_types,
            intermediate_type: binding.intermediate_type.data_type().clone(),
            output_type: function.result_type.data_type().clone(),
            state_format: binding.state_format.clone(),
        },
    })
}

/// Prepare every frozen Aggregate call of the fragment through its exact
/// installed owner. `effects` holds the prepared effects of every expression
/// occurrence, so each argument root contributes its actual effects; `nodes`
/// names the local node each physical aggregate lowers to.
pub(crate) fn prepare_aggregate_calls<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    package: &FragmentPackage,
    functions: &PureEngineFunctionCatalog,
    effects: &crate::expressions::LoweredExpressions,
    nodes: &BTreeMap<NodeId, ProgramNodeId>,
    tokens: &mut BTreeMap<ProgramCallSite, PureCallSpecialization>,
    work: &mut CompileCheckpoints<'_>,
    creation: crate::expressions::CallCreation<'_, F>,
) -> Result<(), crate::expressions::ExpressionCreationFailure<H>> {
    let mut prepared = [0usize; 2];
    for node in package.fragment().nodes().values() {
        work.step()?;
        let (calls, family) = match &node.kind {
            NodeKind::Aggregate { calls, .. } => (calls, AggregateCallFamily::Aggregate),
            NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::GroupedStates { calls, .. },
                ..
            } => (calls, AggregateCallFamily::TopNState),
            _ => continue,
        };
        let local = *nodes.get(&node.id).ok_or(ExpressionLoweringError::Invalid(
            "aggregate node has no local node",
        ))?;
        for (ordinal, call) in calls.iter().enumerate() {
            let ordinal = u32::try_from(ordinal).map_err(|_| {
                ExpressionLoweringError::Invalid("aggregate call ordinal exhausted")
            })?;
            let token = prepare_call(
                package, functions, effects, node, family, ordinal, call, work, creation,
            )?;
            let site = match family {
                AggregateCallFamily::Aggregate => ProgramCallSite::Aggregate {
                    node: local,
                    call: ordinal,
                },
                AggregateCallFamily::TopNState => ProgramCallSite::TopNState {
                    node: local,
                    call: ordinal,
                },
            };
            if tokens.insert(site, token).is_some() {
                return Err(ExpressionLoweringError::Invalid(
                    "aggregate call site is prepared twice",
                )
                .into());
            }
            prepared[family.index()] += 1;
            work.step()?;
        }
    }
    let mut frozen = [0usize; 2];
    for site in package.calls().entries().keys() {
        match site {
            PhysicalCallSite::Aggregate { .. } => frozen[0] += 1,
            PhysicalCallSite::TopNState { .. } => frozen[1] += 1,
            _ => {}
        }
        work.step()?;
    }
    if frozen != prepared {
        return Err(ExpressionLoweringError::Invalid(
            "frozen aggregate calls differ from the actual aggregate calls",
        )
        .into());
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum AggregateCallFamily {
    Aggregate,
    TopNState,
}
impl AggregateCallFamily {
    const fn index(self) -> usize {
        match self {
            Self::Aggregate => 0,
            Self::TopNState => 1,
        }
    }
}

fn prepare_call<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    functions: &PureEngineFunctionCatalog,
    effects: &crate::expressions::LoweredExpressions,
    node: &PhysicalNode,
    family: AggregateCallFamily,
    ordinal: u32,
    call: &AggregateCall,
    work: &mut CompileCheckpoints<'_>,
    creation: crate::expressions::CallCreation<'_, F>,
) -> Result<PureCallSpecialization, crate::expressions::ExpressionCreationFailure<H>> {
    let site = match family {
        AggregateCallFamily::Aggregate => PhysicalCallSite::Aggregate {
            node: node.id,
            call: ordinal,
        },
        AggregateCallFamily::TopNState => PhysicalCallSite::TopNState {
            node: node.id,
            call: ordinal,
        },
    };
    if matches!(family, AggregateCallFamily::TopNState)
        && (!matches!(call.binding.phase, AggregatePhase::Intermediate { .. })
            || call.arguments.len() != 1
            || call.distinct
            || !call.order_by.is_empty())
    {
        return Err(ExpressionLoweringError::Invalid(
            "TopNState requires one exact Intermediate state channel",
        )
        .into());
    }
    let binding = &call.binding;
    let function = &binding.function;
    if function.kind != FunctionKind::Aggregate {
        return Err(ExpressionLoweringError::Invalid(
            "aggregate call binds a non-aggregate function",
        )
        .into());
    }
    let frozen = package
        .calls()
        .entries()
        .get(&site)
        .ok_or(ExpressionLoweringError::Invalid(
            "missing frozen aggregate call",
        ))?;
    let source_request = package.fragment().call_requests().get(site);
    work.step()?;
    let source_request = source_request.ok_or(ExpressionLoweringError::Invalid(
        "missing original aggregate call request",
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
        result_type: FunctionResultType::Scalar(function.result_type.clone()),
        aggregate: Some(AggregateBindingSelection {
            state_argument_contract: binding.state_argument_contract,
            intermediate_type: binding.intermediate_type.clone(),
            state_format: binding.state_format.clone(),
        }),
    });
    work.flush()?;
    crate::expressions::validate_call_created(
        functions.metadata(),
        &function.function_id,
        FunctionKind::Aggregate,
        selection.as_ref(),
        request.request(),
        work.control(),
        creation,
    )?;
    work.flush()?;
    let phase = kernel_phase(binding.phase);
    // Each argument root is an independent unguarded Value occurrence whose
    // definition is the call's ordered argument.
    let mut argument_uses = Vec::new();
    let channel_count = call
        .arguments
        .len()
        .checked_add(call.order_by.len())
        .ok_or(ExpressionLoweringError::Invalid(
            "aggregate channel count exhausted",
        ))?;
    reserve_vec(&mut argument_uses, channel_count, work)?;
    let mut contexts = Vec::new();
    reserve_vec(&mut contexts, channel_count, work)?;
    let mut children = ExpressionEffects::PURE_VALUE;
    for &definition in call
        .arguments
        .iter()
        .chain(call.order_by.iter().map(|key| &key.expr))
    {
        let unit = node
            .expressions
            .as_ref()
            .ok_or(ExpressionLoweringError::Invalid(
                "aggregate node has no expression unit",
            ))?;
        let compiled = effects.unit(node.id)?;
        let flow = unit.control_flow();
        let use_id = unit
            .root_use(definition)
            .ok_or(ExpressionLoweringError::Invalid(
                "aggregate argument root has no actual use",
            ))?;
        let root = unit
            .root(definition)
            .ok_or(ExpressionLoweringError::Invalid(
                "aggregate argument root is absent",
            ))?;
        let invocation = flow
            .uses()
            .get(&use_id)
            .ok_or(ExpressionLoweringError::Invalid(
                "aggregate argument root has no invocation",
            ))?;
        let domain = flow.regions().get(&invocation.context.region);
        work.step()?;
        let exact = invocation.definition == root.definition
            && invocation.context.use_id == use_id
            && invocation.context.demand == EvaluationDemand::Value
            && domain.is_some_and(|domain| domain.parent.is_none() && domain.guard.is_none());
        if !exact {
            return Err(ExpressionLoweringError::Invalid(
                "aggregate argument root differs from its actual call argument",
            )
            .into());
        }
        let summary = compiled
            .effects()
            .get(&use_id)
            .ok_or(ExpressionLoweringError::Invalid(
                "aggregate argument effects were not prepared",
            ))?;
        children = children.join(summary.for_use(invocation.context)?);
        argument_uses.push(Some(use_id));
        contexts.push(invocation.context);
        work.step()?;
    }
    let original = binding
        .state_interpretation
        .as_ref()
        .map(|value| value.clone_observed(work).map(Arc::new))
        .transpose()?;
    let mut order_keys = Vec::new();
    reserve_vec(&mut order_keys, call.order_by.len(), work)?;
    for key in &call.order_by {
        order_keys.push(novarocks_functions::AggregateOrderKey {
            ascending: key.direction == novarocks_physical_plan::SortDirection::Ascending,
            nulls_first: key.null_ordering == novarocks_physical_plan::NullOrdering::First,
        });
        work.step()?;
    }
    let (argument_uses_shape, options) = if phase.consumes_logical_arguments() {
        (
            novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
            AggregatePreparationOptions {
                state_interpretation: original.clone(),
                phase,
                distinct: call.distinct,
                order_keys: order_keys.into(),
                state_input_type: None,
            },
        )
    } else {
        // A merging phase reads exactly one state root of its own type.
        let ([state], [context]) = (call.arguments.as_ref(), contexts.as_slice()) else {
            return Err(ExpressionLoweringError::Invalid(
                "merging aggregate phase requires exactly one state argument",
            )
            .into());
        };
        if call.distinct {
            return Err(ExpressionLoweringError::Invalid(
                "merging aggregate phase repeats DISTINCT",
            )
            .into());
        }
        let state_type = &crate::expressions::bound_root(package, node.id, *state)?.ty;
        work.flush()?;
        (
            novarocks_functions::CallArgumentUses::AggregateMerge {
                phase,
                state_context: (*context).into(),
                state_input_type: state_type,
            },
            AggregatePreparationOptions {
                state_interpretation: original,
                phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: Some(state_type.clone()),
            },
        )
    };
    work.flush()?;
    let input = CallEffectInput {
        context: frozen.context.into(),
        argument_uses: argument_uses_shape,
        function_id: &function.function_id,
        kind: FunctionKind::Aggregate,
        selected: selection.as_ref(),
        request: request.request(),
        environment: &frozen.effects.environment,
        parameters: package.parameters(),
        decimal_overflow_policy: frozen.decimal_overflow_policy,
        proof_scope: frozen.effects.proof_scope,
    };
    let mut token = crate::expressions::prepare_call_created(
        functions,
        input,
        Arc::clone(&selection),
        &frozen.effects,
        PureCallPreparation::Aggregate {
            arguments: ScopedRelationalEffects::primitive(frozen.context, children),
            options,
        },
        work.control(),
        creation,
    )?;
    work.flush()?;
    if matches!(&node.kind, NodeKind::Aggregate { calls, .. }
        if !calls.is_empty()
            && calls.iter().all(|call| call.binding.phase == AggregatePhase::Single))
    {
        token
            .prepare_local_aggregate_stages(work.control())
            .map_err(novarocks_functions::FunctionSpecializationFailure::Kernel)?;
    }
    work.flush()?;
    Ok(token)
}
