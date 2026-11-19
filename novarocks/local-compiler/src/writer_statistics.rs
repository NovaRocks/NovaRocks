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

//! Admit, lower and prepare writer statistics (collect-on-write aggregates).
//!
//! A writer's partial calls read its projected provider input and write the
//! auxiliary channels of its multiplex relation. A finish's final calls merge
//! those channels per write target into internal final channels, and its
//! grouped Unpivot expands each target's final values into Root artifact rows
//! through that target's own mappings.
//!
//! Every call is prepared through its installed pure owner from the frozen
//! binding, the original request and the frozen call effects; the display
//! name copied into a local call is a diagnostic tag only.
//!
//! The frontend freezes the writer's actual materialized Value context. Its
//! root domain belongs to the original flow, while its use is not an expression
//! invocation. Both native carriers borrow that receipt through one preparation
//! body; this compiler never reconstructs a substitute context.

use crate::{
    aggregate::kernel_phase, assert_rows::reserve_vec, expressions::ExpressionLoweringError,
    lowering::FragmentCompileError,
};
use novarocks_functions::{
    AggregateBindingSelection, AggregateKernelPhase, AggregateOverloadIdentity, ConstantError,
    FunctionBindingSelection, FunctionResultType, PureCallSpecialization,
    PureEngineFunctionCatalog, ResolvedAggregateSignature,
};
use novarocks_local_program::{
    ProgramCallSite, ProgramChannelSite, ProgramNodeId, UnpivotConstant, WriterFinalAggregateCall,
    WriterFinalAggregatePlan, WriterGroupedUnpivotMapping, WriterGroupedUnpivotPlan,
    WriterPartialAggregateCall,
};
use novarocks_physical_plan::{
    AggregateBinding, AggregatePhase, ConstantReferenceError, FragmentPackage,
    FunctionArgumentType, NodeId, NodeKind, PhysicalCallSite, PhysicalNode,
    UnpivotConstant as SourceConstant, ValueId, WriterAggregateCall, WriterFinishSpec,
    WriterRelationFieldRole, WriterRelationSchema, WriterTarget,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionKind, FunctionValueType,
};
use novarocks_types::SlotId;
use std::{collections::BTreeMap, sync::Arc};

fn unsupported(node: &PhysicalNode, feature: &'static str) -> FragmentCompileError {
    FragmentCompileError::Unsupported {
        node: Some(node.id),
        feature,
    }
}

/// The ordinal of the first relation field carrying `value` in `role`.
fn relation_ordinal(
    schema: &WriterRelationSchema,
    value: ValueId,
    role: WriterRelationFieldRole,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<usize>, FragmentCompileError> {
    for (ordinal, field) in schema.fields.iter().enumerate() {
        let found = field.value == value && field.role == role;
        work.step()?;
        if found {
            return Ok(Some(ordinal));
        }
    }
    Ok(None)
}

/// Every writer call has one logical value channel.
fn admit_single_channel(
    node: &PhysicalNode,
    call: &WriterAggregateCall,
) -> Result<(), FragmentCompileError> {
    let function = &call.binding.function;
    if function.kind != FunctionKind::Aggregate {
        return Err(FragmentCompileError::Invalid(
            "writer aggregate binds a non-aggregate function",
        ));
    }
    if call.binding.logical_argument_count != 1
        || !matches!(
            function.argument_types.as_ref(),
            [FunctionArgumentType::Value(_)]
        )
    {
        return Err(unsupported(
            node,
            "writer aggregate over other than one logical value argument",
        ));
    }
    Ok(())
}

fn projected_position(
    target: &WriterTarget,
    value: ValueId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<usize>, FragmentCompileError> {
    let mut found = None;
    for (ordinal, field) in target.target_fields.iter().enumerate() {
        work.step()?;
        if field.projected == value && found.replace(ordinal).is_some() {
            return Err(FragmentCompileError::Invalid(
                "writer partial aggregate input has duplicate projected owners",
            ));
        }
    }
    Ok(found)
}

/// Admit a writer's partial calls: each runs its Partial phase over one value
/// of the writer's own projected input and writes one auxiliary channel of its
/// multiplex relation.
pub(crate) fn admit_partial_calls(
    node: &PhysicalNode,
    target: &WriterTarget,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    for call in target.partial_aggregates.iter() {
        work.step()?;
        if !matches!(call.binding.phase, AggregatePhase::Partial { .. }) {
            return Err(unsupported(
                node,
                "writer partial aggregate outside its Partial phase",
            ));
        }
        admit_single_channel(node, call)?;
        if projected_position(target, call.input, work)?.is_none() {
            return Err(unsupported(
                node,
                "writer partial aggregate over a value outside its target input",
            ));
        }
        let output = relation_ordinal(
            &target.output_schema,
            call.output,
            WriterRelationFieldRole::Auxiliary,
            work,
        )?;
        if output.is_none() {
            return Err(FragmentCompileError::Invalid(
                "writer partial aggregate output is not an auxiliary writer relation field",
            ));
        }
    }
    Ok(())
}

/// Admit a finish's final calls and grouped Unpivot: each call runs its Final
/// phase over one auxiliary channel of the writer relation; the grouped
/// Unpivot groups by the writer relation's target ordinal, expands only into
/// the Root relation's target-ordinal and auxiliary fields, and maps only the
/// finish's own targets onto its own final values.
pub(crate) fn admit_finish_statistics(
    node: &PhysicalNode,
    spec: &WriterFinishSpec,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    for call in spec.final_aggregates.iter() {
        work.step()?;
        if !matches!(call.binding.phase, AggregatePhase::Final { .. }) {
            return Err(unsupported(
                node,
                "writer final aggregate outside its Final phase",
            ));
        }
        admit_single_channel(node, call)?;
        let input = relation_ordinal(
            &spec.input_schema,
            call.input,
            WriterRelationFieldRole::Auxiliary,
            work,
        )?;
        if input.is_none() {
            return Err(unsupported(
                node,
                "writer final aggregate over a value outside its auxiliary writer channels",
            ));
        }
    }
    let Some(unpivot) = &spec.grouped_unpivot else {
        return if spec.final_aggregates.is_empty() {
            Ok(())
        } else {
            Err(FragmentCompileError::Invalid(
                "writer final aggregates have no grouped Unpivot",
            ))
        };
    };
    if spec.final_aggregates.is_empty() || unpivot.mappings.is_empty() {
        return Err(FragmentCompileError::Invalid(
            "writer grouped Unpivot has no final aggregate or mapping",
        ));
    }
    let grouping = relation_ordinal(
        &spec.input_schema,
        unpivot.grouping_input,
        WriterRelationFieldRole::TargetOrdinal,
        work,
    )?;
    let passthrough = relation_ordinal(
        &spec.output_schema,
        unpivot.passthrough_output,
        WriterRelationFieldRole::TargetOrdinal,
        work,
    )?;
    if grouping.is_none() || passthrough.is_none() {
        return Err(FragmentCompileError::Invalid(
            "writer grouped Unpivot does not group and pass through the target ordinal",
        ));
    }
    for output in
        std::iter::once(unpivot.value_output).chain(unpivot.literal_outputs.iter().copied())
    {
        let ordinal = relation_ordinal(
            &spec.output_schema,
            output,
            WriterRelationFieldRole::Auxiliary,
            work,
        )?;
        if ordinal.is_none() {
            return Err(FragmentCompileError::Invalid(
                "writer grouped Unpivot expands into a non-auxiliary Root field",
            ));
        }
    }
    for mapping in unpivot.mappings.iter() {
        work.step()?;
        let expected = spec
            .expected_target_ordinals
            .contains(&mapping.write_target_ordinal)
            && unpivot
                .statistics_target_ordinals
                .contains(&mapping.write_target_ordinal);
        if !expected {
            return Err(unsupported(
                node,
                "writer grouped Unpivot mapping for a target outside the finish's statistics targets",
            ));
        }
        let mut mapped = false;
        for call in spec.final_aggregates.iter() {
            mapped |= call.output == mapping.input;
            work.step()?;
        }
        if !mapped {
            return Err(FragmentCompileError::Invalid(
                "writer grouped Unpivot mapping reads a value outside its final aggregates",
            ));
        }
        if mapping.constants.len() != unpivot.literal_outputs.len() {
            return Err(FragmentCompileError::Invalid(
                "writer grouped Unpivot constant width differs from its literal outputs",
            ));
        }
    }
    Ok(())
}

/// The local signature of one frozen writer binding. The binding itself is
/// the call's identity; this copy only names the carriers the local owners
/// check against the prepared contract.
fn resolved_signature(
    binding: &AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Arc<str>, ResolvedAggregateSignature), FragmentCompileError> {
    let function = &binding.function;
    let mut argument_types = Vec::new();
    reserve_vec(&mut argument_types, function.argument_types.len(), work)?;
    for argument in function.argument_types.iter() {
        let FunctionArgumentType::Value(value) = argument else {
            return Err(FragmentCompileError::Invalid(
                "writer aggregate channel is not a value",
            ));
        };
        work.flush()?;
        argument_types.push(value.data_type().clone());
        work.step()?;
    }
    let overload =
        AggregateOverloadIdentity::try_new(function.overload.as_str()).map_err(|error| {
            FragmentCompileError::Owner {
                phase: "writer aggregate overload identity",
                error: novarocks_failure_contract::SharedFailureCause::new(error),
            }
        })?;
    work.flush()?;
    // A diagnostic tag only; the frozen preparation owns the call.
    let name: Arc<str> = Arc::from(function.function_id.as_str());
    work.flush()?;
    Ok((
        name,
        ResolvedAggregateSignature {
            overload,
            argument_types,
            intermediate_type: binding.intermediate_type.data_type().clone(),
            output_type: function.result_type.data_type().clone(),
            state_format: binding.state_format.clone(),
        },
    ))
}

/// Lower one writer's partial calls. A call reads the projected field of the
/// exact projected field position and writes the multiplex
/// slot of its auxiliary output field.
pub(crate) fn lower_partial_calls<H>(
    target: &WriterTarget,
    projection_slots: &[SlotId],
    output_slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<Vec<WriterPartialAggregateCall>, crate::FragmentCreationError<H>> {
    if projection_slots.len() != target.input.len()
        || output_slots.len() != target.output_schema.fields.len()
    {
        return Err((FragmentCompileError::Invalid(
            "writer statistics channels differ from the writer relations",
        ))
        .into());
    }
    let mut calls = Vec::new();
    reserve_vec(&mut calls, target.partial_aggregates.len(), work)?;
    for call in target.partial_aggregates.iter() {
        let input = projected_position(target, call.input, work)?.ok_or(
            FragmentCompileError::Invalid("writer partial aggregate input is not projected"),
        )?;
        let output = relation_ordinal(
            &target.output_schema,
            call.output,
            WriterRelationFieldRole::Auxiliary,
            work,
        )?
        .ok_or(FragmentCompileError::Invalid(
            "writer partial aggregate output has no multiplex channel",
        ))?;
        let (function_name, resolved) = resolved_signature(&call.binding, work)?;
        calls.push(WriterPartialAggregateCall {
            frozen: Some(crate::aggregate::freeze_binding_with_creation(
                &call.binding,
                work,
                creation,
            )?),
            input_slot_id: projection_slots[input],
            function_name,
            resolved,
            intermediate_slot_id: output_slots[output],
        });
        work.step()?;
    }
    Ok(calls)
}

/// The channels of one finish's statistics: its receiver relation, its Root
/// relation and the fresh internal slots of the grouped Unpivot's grouping
/// output followed by one final output per call.
pub(crate) struct FinishStatisticsChannels<'a> {
    pub node: ProgramNodeId,
    pub physical: NodeId,
    pub input_slots: &'a [SlotId],
    pub root_slots: &'a [SlotId],
    pub statistics_slots: &'a [SlotId],
    pub expressions: &'a crate::expressions::RootMap,
}

pub(crate) struct LoweredFinishStatistics {
    pub plan: WriterFinalAggregatePlan,
    /// One `WriterFinalOutput` channel per final call, typed by the result
    /// its Final phase produces.
    pub channels: Vec<(ProgramChannelSite, FunctionValueType)>,
}

/// The number of fresh internal slots a finish's statistics own.
pub(crate) fn finish_statistics_slots(spec: &WriterFinishSpec) -> usize {
    if spec.grouped_unpivot.is_none() {
        0
    } else {
        spec.final_aggregates.len().saturating_add(1)
    }
}

/// Lower one admitted finish's final calls and grouped Unpivot.
pub(crate) fn lower_finish_statistics<H>(
    package: &FragmentPackage,
    spec: &WriterFinishSpec,
    channels: FinishStatisticsChannels<'_>,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<LoweredFinishStatistics, crate::FragmentCreationError<H>> {
    let Some(unpivot) = &spec.grouped_unpivot else {
        return Ok(LoweredFinishStatistics {
            plan: WriterFinalAggregatePlan {
                prepared: None, // CompiledPackage uses its actual state-template owner.
                calls: Vec::new(),
                unpivot: None,
            },
            channels: Vec::new(),
        });
    };
    if channels.input_slots.len() != spec.input_schema.fields.len()
        || channels.root_slots.len() != spec.output_schema.fields.len()
        || channels.statistics_slots.len() != finish_statistics_slots(spec)
    {
        return Err((FragmentCompileError::Invalid(
            "writer statistics channels differ from the finish relations",
        ))
        .into());
    }
    let (grouping_output_slot_id, final_slots) =
        channels
            .statistics_slots
            .split_first()
            .ok_or(FragmentCompileError::Invalid(
                "writer grouped Unpivot has no grouping output channel",
            ))?;
    let mut calls = Vec::new();
    let mut lowered_channels = Vec::new();
    reserve_vec(&mut calls, spec.final_aggregates.len(), work)?;
    reserve_vec(&mut lowered_channels, spec.final_aggregates.len(), work)?;
    for (ordinal, (call, final_output_slot_id)) in
        spec.final_aggregates.iter().zip(final_slots).enumerate()
    {
        let input = relation_ordinal(
            &spec.input_schema,
            call.input,
            WriterRelationFieldRole::Auxiliary,
            work,
        )?
        .ok_or(FragmentCompileError::Invalid(
            "writer final aggregate input has no multiplex channel",
        ))?;
        let (function_name, resolved) = resolved_signature(&call.binding, work)?;
        calls.push(WriterFinalAggregateCall {
            frozen: Some(crate::aggregate::freeze_binding_with_creation(
                &call.binding,
                work,
                creation,
            )?),
            function_name,
            resolved,
            intermediate_input_slot_id: channels.input_slots[input],
            final_output_slot_id: *final_output_slot_id,
        });
        work.flush()?;
        lowered_channels.push((
            ProgramChannelSite::WriterFinalOutput {
                node: channels.node,
                call: u32::try_from(ordinal).map_err(|_| CompileControlError::ResourceExhausted)?,
            },
            call.binding.function.result_type.clone(),
        ));
        work.step()?;
    }
    let root = |value: ValueId, role, work: &mut CompileCheckpoints<'_>| {
        relation_ordinal(&spec.output_schema, value, role, work)?
            .map(|ordinal| channels.root_slots[ordinal])
            .ok_or(FragmentCompileError::Invalid(
                "writer grouped Unpivot output has no Root channel",
            ))
    };
    let grouping_input_slot_id = relation_ordinal(
        &spec.input_schema,
        unpivot.grouping_input,
        WriterRelationFieldRole::TargetOrdinal,
        work,
    )?
    .map(|ordinal| channels.input_slots[ordinal])
    .ok_or(FragmentCompileError::Invalid(
        "writer grouped Unpivot grouping input has no multiplex channel",
    ))?;
    let passthrough_output_slot_id = root(
        unpivot.passthrough_output,
        WriterRelationFieldRole::TargetOrdinal,
        work,
    )?;
    let value_output_slot_id = root(
        unpivot.value_output,
        WriterRelationFieldRole::Auxiliary,
        work,
    )?;
    let mut literal_output_slot_ids = Vec::new();
    reserve_vec(
        &mut literal_output_slot_ids,
        unpivot.literal_outputs.len(),
        work,
    )?;
    for output in unpivot.literal_outputs.iter() {
        literal_output_slot_ids.push(root(*output, WriterRelationFieldRole::Auxiliary, work)?);
    }
    let fragment = package.fragment();
    let value_type =
        |value: ValueId| {
            fragment.values().get(&value).map(|value| &value.ty).ok_or(
                FragmentCompileError::Invalid("missing writer grouped Unpivot value"),
            )
        };
    let value_output = value_type(unpivot.value_output)?;
    let mut mappings = Vec::new();
    reserve_vec(&mut mappings, unpivot.mappings.len(), work)?;
    for mapping in unpivot.mappings.iter() {
        let call = spec
            .final_aggregates
            .iter()
            .position(|call| call.output == mapping.input)
            .ok_or(FragmentCompileError::Invalid(
                "writer grouped Unpivot mapping reads a value outside its final aggregates",
            ))?;
        work.step()?;
        // The final value may only widen into the Root value field.
        let source = &spec.final_aggregates[call].binding.function.result_type;
        work.flush()?;
        let same_domain = source
            .same_value_domain_observed::<FragmentCompileError>(value_output, || {
                work.step().map_err(Into::into)
            })?;
        if !same_domain || (source.nullable() && !value_output.nullable()) {
            return Err((FragmentCompileError::Invalid(
                "writer grouped Unpivot final value differs from its Root value field",
            ))
            .into());
        }
        if mapping.constants.len() != unpivot.literal_outputs.len() {
            return Err((FragmentCompileError::Invalid(
                "writer grouped Unpivot constant width differs from its literal outputs",
            ))
            .into());
        }
        let mut constants = Vec::new();
        reserve_vec(&mut constants, mapping.constants.len(), work)?;
        for (constant, output) in mapping.constants.iter().zip(unpivot.literal_outputs.iter()) {
            constants.push(lower_constant(
                package,
                channels.physical,
                constant,
                value_type(*output)?,
                channels.expressions,
                work,
            )?);
            work.step()?;
        }
        mappings.push(WriterGroupedUnpivotMapping {
            grouping_key: mapping.write_target_ordinal.get(),
            input_value_slot_id: *final_slots.get(call).ok_or(FragmentCompileError::Invalid(
                "writer grouped Unpivot mapping has no final channel",
            ))?,
            constants,
        });
        work.step()?;
    }
    let max_output_rows = usize::try_from(unpivot.max_output_rows).map_err(|_| {
        FragmentCompileError::Invalid("writer grouped Unpivot row bound exceeds host range")
    })?;
    let max_output_bytes = usize::try_from(unpivot.max_output_bytes).map_err(|_| {
        FragmentCompileError::Invalid("writer grouped Unpivot byte bound exceeds host range")
    })?;
    if max_output_rows == 0 || max_output_bytes == 0 {
        return Err(
            (FragmentCompileError::Invalid("writer grouped Unpivot bounds are zero")).into(),
        );
    }
    Ok(LoweredFinishStatistics {
        plan: WriterFinalAggregatePlan {
            prepared: None, // CompiledPackage uses its actual state-template owner.
            calls,
            unpivot: Some(WriterGroupedUnpivotPlan {
                grouping_input_slot_id,
                grouping_output_slot_id: *grouping_output_slot_id,
                passthrough_output_slot_id,
                value_output_slot_id,
                literal_output_slot_ids,
                mappings,
                max_output_rows,
                max_output_bytes,
            }),
        },
        channels: lowered_channels,
    })
}

fn constant_error(error: ConstantReferenceError) -> FragmentCompileError {
    match error {
        ConstantReferenceError::Control(cause) => FragmentCompileError::Control(cause),
        error => FragmentCompileError::Owner {
            phase: "writer grouped Unpivot constant",
            error: novarocks_failure_contract::SharedFailureCause::new(error),
        },
    }
}

fn collection_error(error: ConstantError) -> FragmentCompileError {
    constant_error(ConstantReferenceError::from(error))
}

/// A special collection constant is a nonnull source of exactly its output
/// field's value domain.
fn collection_domain(
    source: &FunctionValueType,
    output: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    work.step()?;
    if source.nullable() {
        return Err(FragmentCompileError::Invalid(
            "writer grouped Unpivot collection constant must be nonnull",
        ));
    }
    if !source.same_value_domain_observed::<FragmentCompileError>(output, || {
        work.step().map_err(Into::into)
    })? {
        return Err(FragmentCompileError::Invalid(
            "writer grouped Unpivot collection constant differs from its Root field",
        ));
    }
    Ok(())
}

/// Lower one mapping constant onto its literal output field: a scalar
/// constant is the root definition its `FinishUnpivotConstant` root
/// evaluates; a collection constant is copied from the package's checked
/// constant pools.
fn lower_constant(
    package: &FragmentPackage,
    node: NodeId,
    constant: &SourceConstant,
    output: &FunctionValueType,
    expressions: &crate::expressions::RootMap,
    work: &mut CompileCheckpoints<'_>,
) -> Result<UnpivotConstant, FragmentCompileError> {
    match constant {
        SourceConstant::Scalar(id) => {
            let definition = crate::expressions::bound_root(package, node, *id)?;
            work.step()?;
            if !matches!(
                definition.kind,
                novarocks_expression_contract::ExprKind::Constant(_)
            ) {
                return Err(FragmentCompileError::Invalid(
                    "writer grouped Unpivot scalar source is not a static constant",
                ));
            }
            if definition.ty.nullable() && !output.nullable() {
                return Err(FragmentCompileError::Invalid(
                    "writer grouped Unpivot constant narrows its Root field",
                ));
            }
            work.flush()?;
            let same = definition
                .ty
                .same_physical_binding_ignoring_root_nullability_observed(output, || {
                    work.step().map_err(FragmentCompileError::from)
                })?;
            if !same {
                return Err(FragmentCompileError::Invalid(
                    "writer grouped Unpivot constant differs from its Root field",
                ));
            }
            Ok(UnpivotConstant::Scalar {
                expr_id: *expressions.get(id).ok_or(FragmentCompileError::Invalid(
                    "missing lowered writer grouped Unpivot constant",
                ))?,
                nullable: definition.ty.nullable(),
            })
        }
        SourceConstant::Int32List(reference) => {
            let value = package
                .constants()
                .resolve_source_observed(*reference, work)
                .map_err(|error| constant_error(error.into()))?;
            collection_domain(value.value_type(), output, work)?;
            work.flush()?;
            let selected = value
                .int32_list_observed(CompilePhase::LowerProgram, work.control())
                .map_err(collection_error)?;
            work.flush()?;
            let selected = selected.ok_or(FragmentCompileError::Invalid(
                "writer grouped Unpivot Int32List constant is NULL",
            ))?;
            let mut copied = Vec::new();
            reserve_vec(&mut copied, selected.len(), work)?;
            for index in 0..selected.len() {
                let item = selected
                    .item_observed(index, work)
                    .map_err(collection_error)?
                    .ok_or(FragmentCompileError::Invalid(
                        "writer grouped Unpivot Int32List item is NULL",
                    ))?;
                copied.push(item);
                work.step()?;
            }
            Ok(UnpivotConstant::Int32List(copied))
        }
        SourceConstant::Utf8Map(reference) => {
            let value = package
                .constants()
                .resolve_source_observed(*reference, work)
                .map_err(|error| constant_error(error.into()))?;
            collection_domain(value.value_type(), output, work)?;
            work.flush()?;
            let selected = value
                .utf8_map_observed(CompilePhase::LowerProgram, work.control())
                .map_err(collection_error)?;
            work.flush()?;
            let selected = selected.ok_or(FragmentCompileError::Invalid(
                "writer grouped Unpivot Utf8Map constant is NULL",
            ))?;
            let mut copied = Vec::new();
            reserve_vec(&mut copied, selected.len(), work)?;
            for index in 0..selected.len() {
                let (key, value) = selected
                    .item_observed(index, work)
                    .map_err(collection_error)?;
                let key = key.ok_or(FragmentCompileError::Invalid(
                    "writer grouped Unpivot Utf8Map key is NULL",
                ))?;
                let value = value.ok_or(FragmentCompileError::Invalid(
                    "writer grouped Unpivot Utf8Map value is NULL",
                ))?;
                work.flush()?;
                let entry: (Arc<str>, Arc<str>) = (Arc::from(key), Arc::from(value));
                work.flush()?;
                copied.push(entry);
                work.step()?;
            }
            Ok(UnpivotConstant::Utf8Map(copied))
        }
    }
}

/// Prepare every frozen writer Partial and Final call of the fragment through
/// its exact installed owner. `nodes` names the local node each physical
/// writer-family node lowers to.
pub(crate) fn prepare_writer_calls<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    functions: &PureEngineFunctionCatalog,
    nodes: &BTreeMap<NodeId, ProgramNodeId>,
    tokens: &mut BTreeMap<ProgramCallSite, PureCallSpecialization>,
    work: &mut CompileCheckpoints<'_>,
    creation: crate::expressions::CallCreation<'_, F>,
) -> Result<(), crate::expressions::ExpressionCreationFailure<H>> {
    let mut prepared = 0usize;
    for node in package.fragment().nodes().values() {
        work.step()?;
        let (calls, phase): (&[WriterAggregateCall], _) = match &node.kind {
            NodeKind::TableWriter { target } => {
                (&target.partial_aggregates, AggregateKernelPhase::Partial)
            }
            NodeKind::TableFinish(spec) => (&spec.final_aggregates, AggregateKernelPhase::Final),
            _ => continue,
        };
        if calls.is_empty() {
            continue;
        }
        let local = *nodes.get(&node.id).ok_or(ExpressionLoweringError::Invalid(
            "writer-family node has no local node",
        ))?;
        for (ordinal, call) in calls.iter().enumerate() {
            let ordinal = u32::try_from(ordinal).map_err(|_| {
                ExpressionLoweringError::Invalid("writer aggregate call ordinal exhausted")
            })?;
            let (site, local_site) = match phase {
                AggregateKernelPhase::Partial => (
                    PhysicalCallSite::WriterPartial {
                        node: node.id,
                        call: ordinal,
                    },
                    ProgramCallSite::WriterPartial {
                        node: local,
                        call: ordinal,
                    },
                ),
                _ => (
                    PhysicalCallSite::WriterFinal {
                        node: node.id,
                        call: ordinal,
                    },
                    ProgramCallSite::WriterFinal {
                        node: local,
                        call: ordinal,
                    },
                ),
            };
            let token = prepare_call(package, functions, site, phase, call, work, creation)?;
            if tokens.insert(local_site, token).is_some() {
                return Err(ExpressionLoweringError::Invalid(
                    "writer aggregate call site is prepared twice",
                )
                .into());
            }
            prepared += 1;
            work.step()?;
        }
    }
    // Every frozen writer claim names one actual call.
    let mut frozen = 0usize;
    for site in package.calls().entries().keys() {
        if matches!(
            site,
            PhysicalCallSite::WriterPartial { .. } | PhysicalCallSite::WriterFinal { .. }
        ) {
            frozen += 1;
        }
        work.step()?;
    }
    if frozen != prepared {
        return Err(ExpressionLoweringError::Invalid(
            "frozen writer aggregate calls differ from the actual writer aggregate calls",
        )
        .into());
    }
    Ok(())
}

fn prepare_call<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    package: &FragmentPackage,
    functions: &PureEngineFunctionCatalog,
    site: PhysicalCallSite,
    phase: AggregateKernelPhase,
    call: &WriterAggregateCall,
    work: &mut CompileCheckpoints<'_>,
    creation: crate::expressions::CallCreation<'_, F>,
) -> Result<PureCallSpecialization, crate::expressions::ExpressionCreationFailure<H>> {
    let binding = &call.binding;
    let function = &binding.function;
    if function.kind != FunctionKind::Aggregate || kernel_phase(binding.phase) != phase {
        return Err(ExpressionLoweringError::UnsupportedCall(site).into());
    }
    let frozen = package
        .calls()
        .entries()
        .get(&site)
        .ok_or(ExpressionLoweringError::Invalid(
            "missing frozen writer aggregate call",
        ))?;
    let source_request = package.fragment().call_requests().get(site);
    work.step()?;
    let source_request = source_request.ok_or(ExpressionLoweringError::Invalid(
        "missing original writer aggregate call request",
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
    let input_type = &package
        .fragment()
        .values()
        .get(&call.input)
        .ok_or(ExpressionLoweringError::Invalid(
            "missing writer aggregate input value",
        ))?
        .ty;
    let input_context =
        frozen
            .materialized_writer_input
            .ok_or(ExpressionLoweringError::Invalid(
                "frozen writer lacks its original materialized Value context",
            ))?;
    let source = novarocks_functions::FrozenWriterAggregatePreparation {
        function_id: &function.function_id,
        selected: &selection,
        original_request: request.request(),
        call_context: frozen.context,
        materialized_input_context: input_context,
        frozen_effects: &frozen.effects,
        decimal_overflow_policy: frozen.decimal_overflow_policy,
        parameters: package.parameters(),
        actual_input_type: input_type,
        phase,
    };
    let token = match creation {
        Some((factory, policy)) => {
            let mut scope = factory
                .begin_scope()
                .map_err(crate::expressions::ExpressionCreationFailure::Creator)?;
            novarocks_functions::prepare_frozen_writer_aggregate_with_creator_in(
                functions.metadata(),
                source,
                policy,
                &mut scope,
                work.control(),
            )?
        }
        None => novarocks_functions::prepare_frozen_writer_aggregate(
            functions.metadata(),
            source,
            work.control(),
        )?,
    };
    work.flush()?;
    Ok(token)
}
