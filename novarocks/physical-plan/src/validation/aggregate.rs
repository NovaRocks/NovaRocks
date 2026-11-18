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

use super::*;

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AggregatePhase, Distribution, Fragment, FragmentId, NodeId, NodeKind, PhysicalPlan,
    RootOrdinal, ValueId,
};

pub(crate) fn validate_aggregate_sequences(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    type CallRef = (FragmentId, NodeId, crate::AggregateCallId);

    #[derive(Default)]
    struct SequenceMembers {
        partials: BTreeSet<CallRef>,
        intermediates: BTreeSet<CallRef>,
        finals: Vec<CallRef>,
    }

    let mut sequences: BTreeMap<crate::AggregateSequenceId, SequenceMembers> = BTreeMap::new();
    let mut calls_by_ref = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            let Some((_, calls)) = node.kind.aggregate_contract() else {
                continue;
            };
            for call in calls {
                let call_ref = (fragment.id(), node.id, call.id);
                calls_by_ref.insert(call_ref, (fragment, node, call));
                let Some(sequence) = call.binding.phase.sequence() else {
                    continue;
                };
                let members = sequences.entry(sequence).or_default();
                match call.binding.phase {
                    AggregatePhase::Single => unreachable!("single phase has no sequence"),
                    AggregatePhase::Partial { .. } => {
                        members.partials.insert(call_ref);
                    }
                    AggregatePhase::Intermediate { .. } => {
                        members.intermediates.insert(call_ref);
                    }
                    AggregatePhase::Final { .. } => members.finals.push(call_ref),
                }
            }
        }
    }

    let mut trace_budget = SemanticTraceWorkBudget::new(errors.limits());
    let mut trace_indexes = SemanticTraceIndexes::default();
    for (sequence, members) in sequences {
        let path = format!("aggregate_sequences[{}]", sequence.get());
        if members.finals.len() != 1 {
            errors.push(ValidationError::new(
                &path,
                "aggregate sequence must have exactly one final call",
            ));
            continue;
        }
        if members.partials.is_empty() {
            errors.push(ValidationError::new(
                &path,
                "aggregate sequence final has no partial producer",
            ));
            continue;
        }
        let final_ref = members.finals[0];
        let Some(&(final_fragment, final_node, final_call)) = calls_by_ref.get(&final_ref) else {
            continue;
        };
        let NodeKind::Aggregate { group_by, .. } = &final_node.kind else {
            continue;
        };
        let Some(values) =
            aggregate_state_inputs(final_fragment, final_node.id, group_by, final_call)
        else {
            errors.push(ValidationError::new(
                &path,
                "aggregate final inputs are not direct grouping/state values",
            ));
            continue;
        };
        let Some(input) = final_node.inputs.first().copied() else {
            continue;
        };
        let partial_shape = members.partials.iter().find_map(|call_ref| {
            calls_by_ref.get(call_ref).map(|(_, _, call)| {
                (
                    call.distinct,
                    call.order_by
                        .iter()
                        .map(|item| (item.direction, item.null_ordering))
                        .collect::<Vec<_>>(),
                )
            })
        });
        let Some(partial_shape) = partial_shape else {
            continue;
        };
        let mut matched_partials = BTreeSet::new();
        let mut matched_intermediates = BTreeSet::new();
        let valid = trace_aggregate_sequence_inputs(
            plan,
            sequence,
            (final_fragment.id(), input),
            values,
            &final_call.binding,
            &partial_shape,
            &mut matched_partials,
            &mut matched_intermediates,
            &mut trace_budget,
            &mut trace_indexes,
        );
        if !valid
            || matched_partials != members.partials
            || matched_intermediates != members.intermediates
        {
            errors.push(ValidationError::new(
                &path,
                "aggregate state paths do not reduce exactly into their matching final",
            ));
        }
    }
}

pub(crate) fn aggregate_state_inputs(
    fragment: &Fragment,
    node: NodeId,
    group_by: &[(RootOrdinal, ValueId)],
    call: &crate::AggregateCall,
) -> Option<Vec<ValueId>> {
    let mut values = group_by
        .iter()
        .map(|(expression, _)| crate::expression_value(fragment, node, *expression))
        .collect::<Option<Vec<_>>>()?;
    if call.arguments.len() != 1 || !call.order_by.is_empty() {
        return None;
    }
    values.push(crate::expression_value(fragment, node, call.arguments[0])?);
    Some(values)
}

pub(crate) fn aggregate_outputs(
    group_by: &[(RootOrdinal, ValueId)],
    call: &crate::AggregateCall,
) -> Vec<ValueId> {
    group_by
        .iter()
        .map(|(_, output)| *output)
        .chain(std::iter::once(call.output))
        .collect()
}

/// Whether the groups a producing phase emits reduce into the groups the
/// phase above it reads.
///
/// Both lists are an aggregate's group keys followed by its one state. The
/// state has to be the same value, and every key the reader still groups by
/// has to be one the producer already grouped by -- its groups may be finer,
/// never coarser. A phase that groups by more is what a DISTINCT chain is
/// made of: the dedup below groups by the distinct column too, and the
/// rollup above combines those groups once the column has done its work.
fn aggregate_outputs_reduce_into(produced: &[ValueId], expected: &[ValueId]) -> bool {
    let (Some((produced_state, produced_keys)), Some((expected_state, expected_keys))) =
        (produced.split_last(), expected.split_last())
    else {
        return false;
    };
    produced_state == expected_state && expected_keys.iter().all(|key| produced_keys.contains(key))
}

// This comparison consumes the original owner's state argument contract;
// it is separate from each phase's exact local function signature validation.
#[derive(Debug)]
enum StateSignatureComparisonError {
    WorkExhausted,
    InvalidType,
    Control(novarocks_type_contract::CompileControlError),
}
impl From<novarocks_type_contract::ValueTypeError> for StateSignatureComparisonError {
    fn from(_: novarocks_type_contract::ValueTypeError) -> Self {
        Self::InvalidType
    }
}

fn state_value_types_match(
    left: &crate::ValueType,
    right: &crate::ValueType,
    ignore_root_nullability: bool,
    charge: &mut impl FnMut(usize) -> Result<(), StateSignatureComparisonError>,
) -> Result<bool, StateSignatureComparisonError> {
    if ignore_root_nullability {
        if !left.same_value_domain_observed(right, || charge(1))? {
            return Ok(false);
        }
        novarocks_type_contract::arrow_data_types_exact_observed::<StateSignatureComparisonError>(
            left.data_type(),
            right.data_type(),
            || charge(1),
        )
    } else {
        left.exactly_equals_observed(right, || charge(1))
    }
}

fn aggregate_bindings_match_with(
    expected: &crate::AggregateBinding,
    actual: &crate::AggregateBinding,
    charge: &mut impl FnMut(usize) -> Result<(), StateSignatureComparisonError>,
) -> Result<bool, StateSignatureComparisonError> {
    use novarocks_type_contract::{AggregateStateArgumentContract, FunctionArgumentType};
    let left = &expected.function;
    let right = &actual.function;
    let identity_work = [
        left.function_id.as_str().len(),
        right.function_id.as_str().len(),
        left.overload.as_str().len(),
        right.overload.as_str().len(),
        expected.state_format.as_str().len(),
        actual.state_format.as_str().len(),
    ]
    .into_iter()
    .try_fold(8usize, usize::checked_add);
    charge(identity_work.ok_or(StateSignatureComparisonError::WorkExhausted)?)?;
    if left.function_id != right.function_id
        || left.overload != right.overload
        || left.kind != right.kind
        || left.argument_types.len() != right.argument_types.len()
        || expected.logical_argument_count != actual.logical_argument_count
        || expected.state_argument_contract != actual.state_argument_contract
        || expected.state_format != actual.state_format
        || expected.phase.sequence() != actual.phase.sequence()
        || usize::try_from(expected.logical_argument_count)
            .map_or(true, |count| count > left.argument_types.len())
    {
        return Ok(false);
    }
    let same_state = match (&expected.state_interpretation, &actual.state_interpretation) {
        (None, None) => true,
        (Some(a), Some(b)) => a.matches_observed(b, || charge(1))?,
        _ => false,
    };
    if !same_state {
        return Ok(false);
    }
    for (ordinal, (a, b)) in left
        .argument_types
        .iter()
        .zip(&right.argument_types)
        .enumerate()
    {
        let ignore_root = expected.state_argument_contract
            == AggregateStateArgumentContract::ValueRootNullabilityIndependent
            && ordinal < expected.logical_argument_count as usize;
        let matches = match (a, b) {
            (FunctionArgumentType::Value(a), FunctionArgumentType::Value(b)) => {
                state_value_types_match(a, b, ignore_root, charge)?
            }
            (
                FunctionArgumentType::Lambda {
                    parameter_types: ap,
                    result_type: ar,
                },
                FunctionArgumentType::Lambda {
                    parameter_types: bp,
                    result_type: br,
                },
            ) => {
                charge(1)?;
                if ap.len() != bp.len() {
                    return Ok(false);
                }
                for (a, b) in ap.iter().zip(bp) {
                    if !state_value_types_match(a, b, false, charge)? {
                        return Ok(false);
                    }
                }
                state_value_types_match(ar, br, false, charge)?
            }
            _ => false,
        };
        if !matches {
            return Ok(false);
        }
    }
    Ok(
        state_value_types_match(&left.result_type, &right.result_type, false, charge)?
            && state_value_types_match(
                &expected.intermediate_type,
                &actual.intermediate_type,
                false,
                charge,
            )?,
    )
}

pub(crate) fn aggregate_bindings_match(
    expected: &crate::AggregateBinding,
    actual: &crate::AggregateBinding,
    budget: &mut SemanticTraceWorkBudget,
) -> bool {
    aggregate_bindings_match_with(expected, actual, &mut |units| {
        if budget.charge(units) {
            Ok(())
        } else {
            Err(StateSignatureComparisonError::WorkExhausted)
        }
    })
    .unwrap_or(false)
}

/// Borrow the original cross-phase state comparison with the caller's exact
/// control meter. Installed declarations and source provenance remain separate
/// mandatory gates; a matching signature alone supplies neither proof.
pub fn aggregate_bindings_match_observed(
    expected: &crate::AggregateBinding,
    actual: &crate::AggregateBinding,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<bool, novarocks_type_contract::CompileControlError> {
    work.flush()?;
    let result = aggregate_bindings_match_with(expected, actual, &mut |units| {
        for _ in 0..units {
            work.step()
                .map_err(StateSignatureComparisonError::Control)?;
        }
        Ok(())
    });
    match result {
        Ok(matches) => {
            work.flush()?;
            Ok(matches)
        }
        Err(StateSignatureComparisonError::Control(cause)) => Err(cause),
        Err(StateSignatureComparisonError::WorkExhausted) => {
            Err(novarocks_type_contract::CompileControlError::ResourceExhausted)
        }
        Err(StateSignatureComparisonError::InvalidType) => {
            work.flush()?;
            Ok(false)
        }
    }
}

/// The RF author and validator borrow this exact direct-parent proof. It
/// proves state/key correspondence, not runtime installation or source custody.
fn aggregate_topn_grouped_parent_matches_with(
    fragment: &Fragment,
    aggregate: &crate::PhysicalNode,
    topn: &crate::PhysicalNode,
    charge: &mut impl FnMut(usize) -> Result<(), StateSignatureComparisonError>,
) -> Result<bool, StateSignatureComparisonError> {
    charge(1)?;
    let NodeKind::Aggregate {
        group_by: producer_keys,
        calls: producer_calls,
        grouping: crate::AggregateGrouping::Partial,
    } = &aggregate.kind
    else {
        return Ok(false);
    };
    let NodeKind::TopN {
        order_by,
        limit,
        offset,
        phase: crate::TopNPhase::Partial { .. },
        reduction:
            crate::TopNReduction::GroupedStates {
                group_by,
                calls,
                comparator,
            },
    } = &topn.kind
    else {
        return Ok(false);
    };
    if aggregate.inputs.len() != 1
        || topn.inputs.as_ref() != [aggregate.id]
        || producer_keys.len() != 1
        || group_by.len() != 1
        || order_by.len() != 1
        || producer_calls.is_empty()
        || calls.len() != producer_calls.len()
        || *limit == 0
        || *offset != 0
        || *comparator != crate::OrderedComparisonAlgorithm::NativeScalarOrderV1
        || aggregate.output_properties.row_multiplicity != crate::RowMultiplicity::SingleCopy
        || aggregate.output.columns.len().checked_sub(1) != Some(producer_calls.len())
        || topn.output.columns.len().checked_sub(1) != Some(calls.len())
    {
        return Ok(false);
    }
    let (producer_expression, key) = producer_keys[0];
    let Some(expression) = fragment.root_expression(aggregate.id, producer_expression) else {
        return Ok(false);
    };
    let crate::ExprKind::Value(input) = expression.kind else {
        return Ok(false);
    };
    let Some(input_definition) = fragment.values().get(&input) else {
        return Ok(false);
    };
    let Some(key_definition) = fragment.values().get(&key) else {
        return Ok(false);
    };
    if group_by[0].1 != key
        || crate::expression_value(fragment, topn.id, group_by[0].0) != Some(key)
        || crate::expression_value(fragment, topn.id, order_by[0].expr) != Some(key)
        || aggregate.output.columns[0] != key
        || topn.output.columns[0] != key
        || !state_value_types_match(&expression.ty, &input_definition.ty, false, charge)?
        || !state_value_types_match(&expression.ty, &key_definition.ty, false, charge)?
        || !comparator.supports_value_type(&key_definition.ty)
    {
        return Ok(false);
    }
    for expression_id in [group_by[0].0, order_by[0].expr] {
        let Some(expression) = fragment.root_expression(topn.id, expression_id) else {
            return Ok(false);
        };
        if !state_value_types_match(&expression.ty, &key_definition.ty, false, charge)? {
            return Ok(false);
        }
    }
    for (ordinal, (producer, consumer)) in producer_calls.iter().zip(calls).enumerate() {
        charge(1)?;
        let AggregatePhase::Partial { sequence } = producer.binding.phase else {
            return Ok(false);
        };
        if consumer.binding.phase != (AggregatePhase::Intermediate { sequence })
            || consumer.distinct
            || !consumer.order_by.is_empty()
            || consumer.arguments.len() != 1
            || crate::expression_value(fragment, topn.id, consumer.arguments[0])
                != Some(producer.output)
            || producer.output == consumer.output
            || aggregate.output.columns[ordinal + 1] != producer.output
            || topn.output.columns[ordinal + 1] != consumer.output
            || !aggregate_bindings_match_with(&producer.binding, &consumer.binding, charge)?
        {
            return Ok(false);
        }
        for call in [producer, consumer] {
            let Some(definition) = fragment.values().get(&call.output) else {
                return Ok(false);
            };
            if definition.origin
                != (crate::ValueOrigin::AggregateState {
                    call: call.id,
                    phase: call.binding.phase,
                })
                || !state_value_types_match(
                    &definition.ty,
                    &call.binding.intermediate_type,
                    false,
                    charge,
                )?
            {
                return Ok(false);
            }
        }
        let Some(argument) = fragment.root_expression(topn.id, consumer.arguments[0]) else {
            return Ok(false);
        };
        if !state_value_types_match(
            &argument.ty,
            &producer.binding.intermediate_type,
            false,
            charge,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn aggregate_topn_grouped_parent_matches(
    fragment: &Fragment,
    aggregate: &crate::PhysicalNode,
    topn: &crate::PhysicalNode,
    budget: &mut SemanticTraceWorkBudget,
) -> bool {
    aggregate_topn_grouped_parent_matches_with(fragment, aggregate, topn, &mut |units| {
        if budget.charge(units) {
            Ok(())
        } else {
            Err(StateSignatureComparisonError::WorkExhausted)
        }
    })
    .unwrap_or(false)
}

/// Use the original state/type comparison and the SQL author's control. The
/// rest of plan validation and authenticated call sources remain mandatory.
pub fn aggregate_topn_grouped_parent_matches_observed(
    fragment: &Fragment,
    aggregate: &crate::PhysicalNode,
    topn: &crate::PhysicalNode,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<bool, novarocks_type_contract::CompileControlError> {
    work.flush()?;
    let result =
        aggregate_topn_grouped_parent_matches_with(fragment, aggregate, topn, &mut |units| {
            for _ in 0..units {
                work.step()
                    .map_err(StateSignatureComparisonError::Control)?;
            }
            Ok(())
        });
    match result {
        Ok(matches) => {
            work.flush()?;
            Ok(matches)
        }
        Err(StateSignatureComparisonError::Control(cause)) => Err(cause),
        Err(StateSignatureComparisonError::WorkExhausted) => {
            Err(novarocks_type_contract::CompileControlError::ResourceExhausted)
        }
        Err(StateSignatureComparisonError::InvalidType) => {
            work.flush()?;
            Ok(false)
        }
    }
}

pub(crate) fn trace_aggregate_sequence_inputs(
    plan: &PhysicalPlan,
    sequence: crate::AggregateSequenceId,
    start: (FragmentId, NodeId),
    initial_values: Vec<ValueId>,
    expected_binding: &crate::AggregateBinding,
    partial_shape: &(bool, Vec<(crate::SortDirection, crate::NullOrdering)>),
    matched_partials: &mut BTreeSet<(FragmentId, NodeId, crate::AggregateCallId)>,
    matched_intermediates: &mut BTreeSet<(FragmentId, NodeId, crate::AggregateCallId)>,
    trace_budget: &mut SemanticTraceWorkBudget,
    trace_indexes: &mut SemanticTraceIndexes,
) -> bool {
    let mut pending = vec![(start, initial_values)];
    let mut visited = BTreeSet::new();
    while let Some((node_ref, expected_values)) = pending.pop() {
        if !trace_budget.charge(expected_values.len().saturating_add(1))
            || !visited.insert((node_ref.0, node_ref.1, expected_values.clone()))
        {
            return false;
        }
        let Some(fragment) = plan.fragments().get(&node_ref.0) else {
            return false;
        };
        let Some(node) = fragment.nodes().get(&node_ref.1) else {
            return false;
        };
        match &node.kind {
            NodeKind::Aggregate {
                group_by, calls, ..
            }
            | NodeKind::TopN {
                reduction:
                    crate::TopNReduction::GroupedStates {
                        group_by, calls, ..
                    },
                ..
            } => {
                if matches!(
                    node.kind,
                    NodeKind::TopN {
                        reduction: crate::TopNReduction::GroupedStates { .. },
                        ..
                    }
                ) && partial_shape.0
                {
                    return false;
                }
                let Some(call) = trace_indexes.aggregate_sequence_call(
                    fragment.id(),
                    node.id,
                    calls,
                    sequence,
                    trace_budget,
                ) else {
                    return false;
                };
                if !aggregate_bindings_match(expected_binding, &call.binding, trace_budget)
                    || !aggregate_outputs_reduce_into(
                        &aggregate_outputs(group_by, call),
                        &expected_values,
                    )
                {
                    return false;
                }
                let call_ref = (fragment.id(), node.id, call.id);
                match call.binding.phase {
                    AggregatePhase::Partial { .. } => {
                        let shape = (
                            call.distinct,
                            call.order_by
                                .iter()
                                .map(|item| (item.direction, item.null_ordering))
                                .collect::<Vec<_>>(),
                        );
                        if shape != *partial_shape || !matched_partials.insert(call_ref) {
                            return false;
                        }
                    }
                    AggregatePhase::Intermediate { .. } => {
                        if !matched_intermediates.insert(call_ref) {
                            return false;
                        }
                        let Some(values) =
                            aggregate_state_inputs(fragment, node.id, group_by, call)
                        else {
                            return false;
                        };
                        let Some(input) = node.inputs.first().copied() else {
                            return false;
                        };
                        pending.push(((fragment.id(), input), values));
                    }
                    AggregatePhase::Single | AggregatePhase::Final { .. } => return false,
                }
            }
            NodeKind::ExchangeSource { edge, .. } => {
                let Some(edge) = plan.edges().get(edge) else {
                    return false;
                };
                if edge.kind != crate::EdgeKind::Stream
                    || edge.destination.fragment != node_ref.0
                    || edge.destination.node != node_ref.1
                {
                    return false;
                }
                let Some(values) = trace_indexes.map_edge_values(
                    edge.id,
                    &edge.destination.receive_mapping,
                    &expected_values,
                    false,
                    trace_budget,
                ) else {
                    return false;
                };
                let Some(source) = plan.fragments().get(&edge.source.fragment) else {
                    return false;
                };
                pending.push(((source.id(), source.root()), values));
            }
            NodeKind::Project { expressions } => {
                let Some(input) = node.inputs.first().copied() else {
                    return false;
                };
                let Some(child) = fragment.nodes().get(&input) else {
                    return false;
                };
                let Some(values) = trace_indexes.map_project_values(
                    fragment,
                    node,
                    child,
                    expressions,
                    &expected_values,
                    trace_budget,
                ) else {
                    return false;
                };
                pending.push(((fragment.id(), input), values));
            }
            // Only Rows preserves state identity. The separate TopN sequence
            // proves Complete input whenever this row budget ranks groups.
            // GroupedStates above instead follows its exact Intermediate call.
            NodeKind::TopN {
                phase: crate::TopNPhase::Partial { .. },
                reduction: crate::TopNReduction::Rows,
                ..
            } => {
                let Some(input) = node.inputs.first().copied() else {
                    return false;
                };
                pending.push(((fragment.id(), input), expected_values));
            }
            NodeKind::SetOp {
                kind: crate::SetOperationKind::UnionAll,
                input_mappings,
            } => {
                if node.inputs.len() != input_mappings.len() || node.inputs.is_empty() {
                    return false;
                }
                for (input_ordinal, (input, mapping)) in
                    node.inputs.iter().zip(input_mappings).enumerate()
                {
                    let Some(mapped) = trace_indexes.map_union_values(
                        fragment.id(),
                        node.id,
                        input_ordinal,
                        &node.output.columns,
                        mapping,
                        &expected_values,
                        false,
                        trace_budget,
                    ) else {
                        return false;
                    };
                    pending.push(((fragment.id(), *input), mapped));
                }
            }
            _ => return false,
        }
    }
    true
}

pub(crate) fn validate_topn_reductions(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    type NodeRef = (FragmentId, NodeId);

    let mut partials: BTreeMap<crate::TopNSequenceId, BTreeSet<NodeRef>> = BTreeMap::new();
    let mut finals: BTreeMap<crate::TopNSequenceId, Vec<NodeRef>> = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::TopN { phase, .. } = &node.kind {
                match *phase {
                    crate::TopNPhase::Single => {}
                    crate::TopNPhase::Partial { sequence } => {
                        partials
                            .entry(sequence)
                            .or_default()
                            .insert((fragment.id(), node.id));
                    }
                    crate::TopNPhase::Final { sequence } => {
                        finals
                            .entry(sequence)
                            .or_default()
                            .push((fragment.id(), node.id));
                    }
                }
            }
        }
    }

    let sequences = partials
        .keys()
        .chain(finals.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut trace_budget = SemanticTraceWorkBudget::new(errors.limits());
    let mut trace_indexes = SemanticTraceIndexes::default();
    for sequence in sequences {
        let sequence_path = format!("topn_sequences[{}]", sequence.get());
        let expected_partials = partials.get(&sequence).cloned().unwrap_or_default();
        let sequence_finals = finals.get(&sequence).map(Vec::as_slice).unwrap_or_default();
        if sequence_finals.len() != 1 {
            errors.push(ValidationError::new(
                &sequence_path,
                "TopN sequence must have exactly one final node",
            ));
            continue;
        }
        if expected_partials.is_empty() {
            errors.push(ValidationError::new(
                &sequence_path,
                "TopN sequence final has no partial producer",
            ));
            continue;
        }
        let (final_fragment_id, final_node_id) = sequence_finals[0];
        let Some(final_fragment) = plan.fragments().get(&final_fragment_id) else {
            continue;
        };
        let Some(final_node) = final_fragment.nodes().get(&final_node_id) else {
            continue;
        };
        let NodeKind::TopN {
            order_by,
            limit,
            offset,
            phase: crate::TopNPhase::Final { .. },
            ..
        } = &final_node.kind
        else {
            continue;
        };
        let Some(required_partial_limit) = limit.checked_add(*offset) else {
            continue;
        };
        let Some(expected_ordering) = derive_ordering(final_fragment, final_node.id, &[], order_by)
        else {
            continue;
        };
        let Some(input) = final_node.inputs.first().copied() else {
            continue;
        };
        let mut matched = BTreeSet::new();
        let all_paths_match = trace_topn_reduction_inputs(
            plan,
            sequence,
            (final_fragment_id, input),
            expected_ordering,
            required_partial_limit,
            &mut matched,
            &mut trace_budget,
            &mut trace_indexes,
        );
        if !all_paths_match || matched != expected_partials {
            errors.push(ValidationError::new(
                &sequence_path,
                "TopN partial paths do not reduce exactly into their matching final",
            ));
        }
    }
}

/// What one path of a TopN reduction trace has established so far.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct Traced {
    /// A partial carrying this sequence has already been matched on this path.
    reduced: bool,
    /// The order being pruned by is an aggregate's own grouping.
    by_grouping: bool,
    /// A downstream grouped merge or Complete aggregate covers duplicates.
    duplicate_states_covered: bool,
    /// A grouped reduction must reach and check its full producing grouping.
    key_contract_pending: bool,
    /// A grouped sequence's ordinary row budget needs Complete input.
    row_unique_pending: bool,
    comparator: Option<crate::OrderedComparisonAlgorithm>,
}

pub(crate) fn trace_topn_reduction_inputs(
    plan: &PhysicalPlan,
    sequence: crate::TopNSequenceId,
    start: (FragmentId, NodeId),
    initial_ordering: Vec<crate::OrderingKey>,
    required_partial_limit: u64,
    matched: &mut BTreeSet<(FragmentId, NodeId)>,
    trace_budget: &mut SemanticTraceWorkBudget,
    trace_indexes: &mut SemanticTraceIndexes,
) -> bool {
    // A matched member does not discharge its input obligations. Grouped
    // reductions must reach the full producing grouping; ordinary row budgets
    // in a grouping sequence require Complete before encountering another
    // grouped reduction. Exact aliases and cuts preserve these obligations.
    // Only a fully discharged ordinary raw-row path may stop below a partial.
    let mut pending = vec![(start, initial_ordering, Traced::default())];
    let mut visited = BTreeSet::new();
    while let Some((node_ref, expected_ordering, traced)) = pending.pop() {
        if !trace_budget.charge(expected_ordering.len().saturating_add(1))
            || !visited.insert((node_ref.0, node_ref.1, expected_ordering.clone(), traced))
        {
            return false;
        }
        macro_rules! dead_end {
            () => {{
                if traced.key_contract_pending {
                    return false;
                }
                if traced.reduced {
                    continue;
                } else {
                    return false;
                }
            }};
        }
        let Some(fragment) = plan.fragments().get(&node_ref.0) else {
            dead_end!();
        };
        let Some(node) = fragment.nodes().get(&node_ref.1) else {
            dead_end!();
        };
        match &node.kind {
            NodeKind::TopN {
                limit,
                offset,
                phase:
                    crate::TopNPhase::Partial {
                        sequence: partial_sequence,
                    },
                reduction,
                ..
            } => {
                let grouped_comparator =
                    if let crate::TopNReduction::GroupedStates { comparator, .. } = reduction {
                        Some(*comparator)
                    } else {
                        None
                    };
                if grouped_comparator.is_some() && traced.row_unique_pending {
                    return false;
                }
                if traced
                    .comparator
                    .zip(grouped_comparator)
                    .is_some_and(|(a, b)| a != b)
                {
                    return false;
                }
                if *partial_sequence != sequence
                    || *offset != 0
                    || *limit != required_partial_limit
                    || node.output_properties.ordering.as_ref() != expected_ordering.as_slice()
                {
                    dead_end!();
                }
                if !matched.insert(node_ref) {
                    dead_end!();
                }
                // A reduction can be staged more than once -- one partial
                // prunes before an aggregate and another after it -- so the
                // walk carries on into what this one read.
                if let Some(input) = node.inputs.first().copied() {
                    pending.push((
                        (fragment.id(), input),
                        expected_ordering,
                        Traced {
                            reduced: true,
                            duplicate_states_covered: matches!(
                                reduction,
                                crate::TopNReduction::GroupedStates { .. }
                            ),
                            row_unique_pending: matches!(reduction, crate::TopNReduction::Rows)
                                && traced.by_grouping,
                            comparator: grouped_comparator.or(traced.comparator),
                            key_contract_pending: traced.by_grouping
                                || matches!(reduction, crate::TopNReduction::GroupedStates { .. }),
                            ..traced
                        },
                    ));
                }
            }
            NodeKind::ExchangeSource { edge, .. } => {
                let Some(edge) = plan.edges().get(edge) else {
                    dead_end!();
                };
                // A gather carries every row, so a reduction crosses it
                // whatever it is pruning by. Any other stream carries a subset
                // to each instance, which only preserves a reduction that
                // prunes by an aggregate's own grouping: each key keeps its top
                // rows wherever it appears, and a key the final publishes has
                // fewer than the limit ahead of it globally, so fewer than the
                // limit ahead of it anywhere.
                let gathered = edge.partitioning.source == Distribution::Singleton
                    && edge.partitioning.destination == Distribution::Singleton;
                if edge.kind != crate::EdgeKind::Stream
                    || edge.destination.fragment != node_ref.0
                    || edge.destination.node != node_ref.1
                    || !(gathered || traced.by_grouping)
                {
                    dead_end!();
                }
                let Some(mapped_values) = trace_indexes.map_edge_values(
                    edge.id,
                    &edge.destination.receive_mapping,
                    &expected_ordering
                        .iter()
                        .map(|key| key.value)
                        .collect::<Vec<_>>(),
                    true,
                    trace_budget,
                ) else {
                    dead_end!();
                };
                let mapped = expected_ordering
                    .iter()
                    .zip(mapped_values)
                    .map(|(key, value)| crate::OrderingKey {
                        value,
                        direction: key.direction,
                        null_ordering: key.null_ordering,
                    })
                    .collect();
                let Some(source) = plan.fragments().get(&edge.source.fragment) else {
                    dead_end!();
                };
                pending.push(((source.id(), source.root()), mapped, traced));
            }
            NodeKind::Project { expressions } => {
                let Some(input) = node.inputs.first().copied() else {
                    dead_end!();
                };
                let Some(child) = fragment.nodes().get(&input) else {
                    dead_end!();
                };
                let expected_values = expected_ordering
                    .iter()
                    .map(|key| key.value)
                    .collect::<Vec<_>>();
                let Some(values) = trace_indexes.map_project_values(
                    fragment,
                    node,
                    child,
                    expressions,
                    &expected_values,
                    trace_budget,
                ) else {
                    dead_end!();
                };
                let ordering = expected_ordering
                    .iter()
                    .zip(values)
                    .map(|(key, value)| crate::OrderingKey {
                        value,
                        direction: key.direction,
                        null_ordering: key.null_ordering,
                    })
                    .collect();
                pending.push(((fragment.id(), input), ordering, traced));
            }
            // A Partial may repeat a group at any local DOP. Only a key
            // budget consumer can retain/merge all such contributions. A
            // complete grouping can also make an ordinary row budget sound.
            NodeKind::Aggregate {
                group_by, grouping, ..
            } => {
                if traced.row_unique_pending && *grouping != crate::AggregateGrouping::Complete {
                    return false;
                }
                if *grouping == crate::AggregateGrouping::Partial
                    && !traced.duplicate_states_covered
                {
                    return false;
                }
                if group_by.len() != expected_ordering.len() {
                    if traced.key_contract_pending {
                        return false;
                    }
                    dead_end!();
                }
                let Some(input) = node.inputs.first().copied() else {
                    dead_end!();
                };
                let mut mapped = Vec::with_capacity(expected_ordering.len());
                for key in &expected_ordering {
                    let Some((expression, _)) =
                        group_by.iter().find(|(_, output)| *output == key.value)
                    else {
                        if traced.key_contract_pending {
                            return false;
                        }
                        dead_end!();
                    };
                    let Some(source) =
                        fragment
                            .root_expression(node.id, *expression)
                            .and_then(|expression| match expression.kind {
                                crate::ExprKind::Value(source) => Some(source),
                                _ => None,
                            })
                    else {
                        if traced.key_contract_pending {
                            return false;
                        }
                        dead_end!();
                    };
                    mapped.push(crate::OrderingKey {
                        value: source,
                        direction: key.direction,
                        null_ordering: key.null_ordering,
                    });
                }
                if !trace_budget.charge(mapped.len().saturating_add(1)) {
                    dead_end!();
                }
                pending.push((
                    (fragment.id(), input),
                    mapped,
                    Traced {
                        by_grouping: true,
                        duplicate_states_covered: traced.duplicate_states_covered
                            || *grouping == crate::AggregateGrouping::Complete,
                        key_contract_pending: false,
                        row_unique_pending: false,
                        ..traced
                    },
                ));
            }
            NodeKind::SetOp {
                kind: crate::SetOperationKind::UnionAll,
                input_mappings,
            } => {
                // Branch-local uniqueness does not prove disjoint union keys.
                if traced.row_unique_pending {
                    return false;
                }
                if node.inputs.len() != input_mappings.len() || node.inputs.is_empty() {
                    dead_end!();
                }
                let expected_values = expected_ordering
                    .iter()
                    .map(|key| key.value)
                    .collect::<Vec<_>>();
                for (input_ordinal, (input, mapping)) in
                    node.inputs.iter().zip(input_mappings).enumerate()
                {
                    let mapped_values = trace_indexes.map_union_values(
                        fragment.id(),
                        node.id,
                        input_ordinal,
                        &node.output.columns,
                        mapping,
                        &expected_values,
                        true,
                        trace_budget,
                    );
                    let Some(mapped_values) = mapped_values else {
                        dead_end!();
                    };
                    let mapped = expected_ordering
                        .iter()
                        .zip(mapped_values)
                        .map(|(key, value)| crate::OrderingKey {
                            value,
                            direction: key.direction,
                            null_ordering: key.null_ordering,
                        })
                        .collect();
                    pending.push(((fragment.id(), *input), mapped, traced));
                }
            }
            _ => dead_end!(),
        }
    }
    true
}

#[cfg(test)]
#[path = "aggregate_state_argument_tests.rs"]
mod aggregate_state_argument_tests;
