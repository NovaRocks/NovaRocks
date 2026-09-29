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

use arrow_schema::DataType;
use novarocks_connector_contract::{
    ConnectorCodecCategory, ConnectorEncodedPayload, ConnectorReadRelationKind,
    ConnectorReadWorkSource, MAX_CONNECTOR_WRITE_TARGETS,
};

use crate::{
    AggregatePhase, Distribution, ExprId, ExprKind, Fragment, FunctionKind, NodeId, NodeKind,
    PhysicalNode, ProviderColumnReference, ProviderReadReference, Relation, RowMultiplicity,
    ValueDef, ValueId, ValueOrigin,
};

pub(crate) fn writer_result_proof_matches_finish(
    proof: &crate::WriterResultCut,
    finish: &crate::WriterRelationSchema,
    expected_values: &[ValueId],
) -> bool {
    proof.schema_revision == finish.revision
        && proof.fields.len() == finish.fields.len()
        && proof
            .fields
            .iter()
            .zip(&finish.fields)
            .zip(expected_values)
            .all(|((proof, finish), expected)| {
                proof.destination == *expected
                    && proof.name == finish.name
                    && proof.ty == finish.ty
                    && proof.role == finish.role
            })
}

pub(crate) fn writer_schema_matches_finish_values(
    writer: &crate::WriterRelationSchema,
    finish: &crate::WriterRelationSchema,
    expected_values: &[ValueId],
) -> bool {
    writer_schema_shapes_match(writer, finish)
        && writer.fields.len() == expected_values.len()
        && writer
            .fields
            .iter()
            .zip(expected_values)
            .all(|(field, expected)| field.value == *expected)
}

pub(crate) fn change_event_source(
    fragment: &Fragment,
    effect: ValueId,
) -> Option<&[crate::ChangeEventSpec]> {
    let value = fragment.values().get(&effect)?;
    if value.ty.data_type != DataType::Int8 || value.ty.nullable {
        return None;
    }
    let ValueOrigin::NodeOutput {
        node,
        output_ordinal,
    } = value.origin
    else {
        return None;
    };
    let source = fragment.nodes().get(&node)?;
    let ordinal = usize::try_from(output_ordinal).ok()?;
    if source.output.columns.get(ordinal) != Some(&effect) {
        return None;
    }
    match &source.kind {
        NodeKind::ChangeEventExpand {
            events,
            effect_output,
        } if *effect_output == effect => Some(events),
        _ => None,
    }
}

pub(crate) fn validate_value(
    fragment: &Fragment,
    value: &ValueDef,
    aggregate_calls: &BTreeMap<crate::AggregateCallId, AggregatePhase>,
    errors: &mut ValidationContext,
) {
    let path = format!(
        "fragments[{}].values[{}]",
        fragment.id().get(),
        value.id.get()
    );
    match &value.origin {
        ValueOrigin::ProviderField { scan_node, field } => {
            match fragment.nodes().get(scan_node) {
                Some(node) if matches!(node.kind, NodeKind::Scan { .. }) => {}
                Some(_) => errors.push(ValidationError::new(
                    &path,
                    "provider field origin does not reference a scan node",
                )),
                None => errors.push(ValidationError::new(
                    &path,
                    format!("scan node {} is not defined", scan_node.get()),
                )),
            }
            validate_column_reference(None, field, &path, errors);
        }
        ValueOrigin::Expr { node, expr } => {
            require_node(fragment, *node, &path, errors);
            if let Some(expression) = fragment.expressions().get(*expr) {
                // The value names what the expression produces, and may admit
                // null where the expression does not: an exact value standing
                // where null is admitted is sound. The reverse is not.
                if expression.ty.data_type != value.ty.data_type
                    || (expression.ty.nullable && !value.ty.nullable)
                {
                    errors.push(ValidationError::new(
                        &path,
                        "expression origin type differs from value type, or the value stops admitting null the expression admits",
                    ));
                }
            } else {
                errors.push(ValidationError::new(
                    &path,
                    format!("expression {} is not defined", expr.get()),
                ));
            }
        }
        ValueOrigin::NullExtended { node, of } => {
            match fragment.nodes().get(node) {
                Some(owner)
                    if matches!(
                        owner.kind,
                        NodeKind::HashJoin { .. }
                            | NodeKind::NestLoopJoin { .. }
                            | NodeKind::Repeat { .. }
                    ) => {}
                Some(_) => errors.push(ValidationError::new(
                    &path,
                    "NULL-extended value owner cannot produce NULL extensions",
                )),
                None => require_node(fragment, *node, &path, errors),
            }
            if let Some(source) = fragment.values().get(of) {
                if source.ty.data_type != value.ty.data_type || !value.ty.nullable {
                    errors.push(ValidationError::new(
                        &path,
                        "null-extended value must preserve the data type and be nullable",
                    ));
                }
            } else {
                errors.push(ValidationError::new(
                    &path,
                    format!("source value {} is not defined", of.get()),
                ));
            }
        }
        ValueOrigin::AggregateState { call, phase } => match aggregate_calls.get(call) {
            Some(actual)
                if actual == phase
                    && matches!(
                        phase,
                        AggregatePhase::Partial { .. } | AggregatePhase::Intermediate { .. }
                    ) => {}
            Some(_) => errors.push(ValidationError::new(
                &path,
                "aggregate state origin differs from the call phase",
            )),
            None => errors.push(ValidationError::new(
                &path,
                format!("aggregate call {} is not defined", call.get()),
            )),
        },
        ValueOrigin::AggregateResult { call } => match aggregate_calls.get(call) {
            Some(AggregatePhase::Single | AggregatePhase::Final { .. }) => {}
            Some(_) => errors.push(ValidationError::new(
                &path,
                "aggregate result origin references an intermediate-state call",
            )),
            None => errors.push(ValidationError::new(
                &path,
                format!("aggregate call {} is not defined", call.get()),
            )),
        },
        ValueOrigin::NodeOutput {
            node,
            output_ordinal,
        } => match fragment.nodes().get(node) {
            Some(owner)
                if usize::try_from(*output_ordinal)
                    .ok()
                    .and_then(|ordinal| owner.output.columns.get(ordinal))
                    == Some(&value.id) => {}
            Some(_) => errors.push(ValidationError::new(
                &path,
                "node-output origin does not match the declared output ordinal",
            )),
            None => require_node(fragment, *node, &path, errors),
        },
        ValueOrigin::ExchangeImport { .. } | ValueOrigin::CteImport { .. } => {
            // Cross-fragment definitions are checked with the complete graph.
        }
        ValueOrigin::WriterDerived { writer_node, .. } => match fragment.nodes().get(writer_node) {
            Some(node)
                if matches!(
                    node.kind,
                    NodeKind::TableWriter { .. } | NodeKind::TableFinish(_)
                ) => {}
            Some(_) => errors.push(ValidationError::new(
                &path,
                "writer-derived value does not reference a writer node",
            )),
            None => errors.push(ValidationError::new(
                &path,
                format!("writer node {} is not defined", writer_node.get()),
            )),
        },
    }
}

pub(crate) fn validate_node(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    errors: &mut ValidationContext,
) {
    let path = format!(
        "fragments[{}].nodes[{}]",
        fragment.id().get(),
        node.id.get()
    );
    if node.output.node != node.id {
        errors.push(ValidationError::new(
            &path,
            "output port belongs to a different node",
        ));
    }
    for input in &node.inputs {
        require_node(fragment, *input, &path, errors);
    }
    if node.inputs.len() != node.required_inputs.len() {
        errors.push(ValidationError::new(
            &path,
            "required input properties must have one entry per input",
        ));
    }
    for (ordinal, properties) in node.required_inputs.iter().enumerate() {
        validate_distribution(
            fragment,
            &properties.distribution,
            "node.required_input",
            errors,
        );
        for key in &properties.ordering {
            require_value(fragment, key.value, &path, errors);
        }
        if let Some(input) = node
            .inputs
            .get(ordinal)
            .and_then(|input| fragment.nodes().get(input))
        {
            if let Some(input_values) = indexes.output(input.id) {
                validate_property_keys_on_port(
                    fragment,
                    properties,
                    input_values,
                    &format!("{path}.required_inputs[{ordinal}]"),
                    errors,
                );
            }
            if !properties_satisfy(&input.output_properties, properties) {
                errors.push(ValidationError::new(
                    &path,
                    format!("input {ordinal} does not provide its required physical properties"),
                ));
            }
        }
    }
    validate_distribution(
        fragment,
        &node.output_properties.distribution,
        "node.output",
        errors,
    );
    for key in &node.output_properties.ordering {
        require_value(fragment, key.value, &path, errors);
    }
    for value in &node.output.columns {
        require_value(fragment, *value, &path, errors);
    }
    if let Some(output_values) = indexes.output(node.id) {
        validate_property_keys_on_port(
            fragment,
            &node.output_properties,
            output_values,
            &format!("{path}.output_properties"),
            errors,
        );
    }
    validate_node_output_properties(fragment, node, &path, errors);
    let mut expressions = Vec::new();
    node.kind.expression_references(&mut expressions);
    for expression in expressions {
        match fragment.expressions().get(expression) {
            Some(expression) if expression.owner != node.id => errors.push(ValidationError::new(
                &path,
                "node references an expression owned by another physical node",
            )),
            Some(expression) if expression.lambda_scope.is_some() => errors.push(
                ValidationError::new(&path, "node expression root is inside a lambda scope"),
            ),
            Some(_) => {}
            None => errors.push(ValidationError::new(
                &path,
                format!("expression {} is not defined", expression.get()),
            )),
        }
    }
    validate_node_arity(node, &path, errors);
    validate_node_semantics(fragment, node, indexes, &path, errors);
    validate_node_output_closure(fragment, node, indexes, &path, errors);
}

pub(crate) fn validate_node_output_closure(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    path: &str,
    errors: &mut ValidationContext,
) {
    let input_columns = node
        .inputs
        .first()
        .and_then(|input| fragment.nodes().get(input))
        .map(|input| input.output.columns.as_ref())
        .unwrap_or_default();
    let input_values = indexes
        .visible_input(node.id)
        .expect("every fragment node has one indexed visible-input port");
    let aggregate_call_ids = match &node.kind {
        NodeKind::Aggregate { calls, .. } => {
            calls.iter().map(|call| call.id).collect::<BTreeSet<_>>()
        }
        _ => BTreeSet::new(),
    };
    let repeat_grouping_values = match &node.kind {
        NodeKind::Repeat {
            grouping_values, ..
        } => grouping_values.iter().copied().collect::<BTreeSet<_>>(),
        _ => BTreeSet::new(),
    };
    if matches!(
        node.kind,
        NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. }
    ) {
        validate_join_output_closure(fragment, node, indexes, path, errors);
        return;
    }
    let exact = match &node.kind {
        NodeKind::Scan {
            provider_outputs,
            derived_values,
            ..
        } => {
            validate_scan_output_coverage(node, provider_outputs, derived_values, path, errors);
            None
        }
        NodeKind::Filter { .. }
        | NodeKind::Sort { .. }
        | NodeKind::TopN { .. }
        | NodeKind::Limit { .. }
        | NodeKind::AssertOneRow(_) => Some(input_columns.to_vec()),
        NodeKind::Project { expressions } => {
            Some(expressions.iter().map(|(_, value)| *value).collect())
        }
        NodeKind::Aggregate {
            group_by, calls, ..
        } => Some(
            group_by
                .iter()
                .map(|(_, value)| *value)
                .chain(calls.iter().map(|call| call.output))
                .collect(),
        ),
        NodeKind::Window(spec) => Some(
            input_columns
                .iter()
                .copied()
                .chain(spec.expressions.iter().map(|expression| expression.output))
                .collect(),
        ),
        NodeKind::Repeat {
            grouping_values,
            grouping_outputs,
            ..
        } => {
            let replacements = grouping_values.iter().copied().collect::<BTreeMap<_, _>>();
            Some(
                input_columns
                    .iter()
                    .map(|value| replacements.get(value).copied().unwrap_or(*value))
                    .chain(grouping_outputs.iter().map(|output| output.output))
                    .collect(),
            )
        }
        NodeKind::Unpivot { spec } => Some(
            spec.passthrough
                .iter()
                .map(|(_, output)| *output)
                .chain(std::iter::once(spec.value_output))
                .chain(spec.literal_outputs.iter().copied())
                .collect(),
        ),
        NodeKind::TableFunction { outputs, .. } => {
            Some(outputs.iter().map(|output| output.value()).collect())
        }
        NodeKind::ExchangeSource { .. } => None,
        NodeKind::TableWriter { target } => Some(
            target
                .output_schema
                .fields
                .iter()
                .map(|field| field.value)
                .collect(),
        ),
        NodeKind::TableFinish(spec) => Some(
            spec.output_schema
                .fields
                .iter()
                .map(|field| field.value)
                .collect(),
        ),
        NodeKind::HashJoin { .. }
        | NodeKind::NestLoopJoin { .. }
        | NodeKind::SetOp { .. }
        | NodeKind::Values { .. }
        | NodeKind::GenerateSeries { .. }
        | NodeKind::ChangeEventExpand { .. } => None,
    };
    if let Some(exact) = exact
        && exact.as_slice() != node.output.columns.as_ref()
    {
        errors.push(ValidationError::new(
            path,
            "node output port differs from its exact produced/pass-through sequence",
        ));
    }
    for (ordinal, value) in node.output.columns.iter().enumerate() {
        if input_values.contains(value) {
            continue;
        }
        let owned = fragment.values().get(value).is_some_and(|definition| {
            value_origin_allowed(
                node,
                definition,
                ordinal,
                input_values,
                &aggregate_call_ids,
                &repeat_grouping_values,
            )
        });
        if !owned {
            errors.push(ValidationError::new(
                path,
                format!("output value {} is not produced by this node", value.get()),
            ));
        }
    }
}

pub(crate) fn validate_scan_output_coverage(
    node: &PhysicalNode,
    provider_outputs: &[(ProviderColumnReference, ValueId)],
    derived_values: &[ValueId],
    path: &str,
    errors: &mut ValidationContext,
) {
    let expected = ValuePortIndex::new(
        &provider_outputs
            .iter()
            .map(|(_, value)| *value)
            .chain(derived_values.iter().copied())
            .collect::<Vec<_>>(),
    );
    let actual = ValuePortIndex::new(&node.output.columns);
    if actual.occurrences != expected.occurrences {
        errors.push(ValidationError::new(
            path,
            "scan output port does not exactly cover its provider and derived value occurrences",
        ));
    }
}

pub(crate) fn validate_join_output_closure(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    path: &str,
    errors: &mut ValidationContext,
) {
    let Some(left) = node.inputs.first().and_then(|id| fragment.nodes().get(id)) else {
        return;
    };
    let Some(right) = node.inputs.get(1).and_then(|id| fragment.nodes().get(id)) else {
        return;
    };
    let (kind, declared_nulls) = match &node.kind {
        NodeKind::HashJoin {
            kind,
            null_extended,
            ..
        }
        | NodeKind::NestLoopJoin {
            kind,
            null_extended,
            ..
        } => (*kind, null_extended.as_ref()),
        _ => return,
    };
    let left_values = indexes
        .output(left.id)
        .expect("every fragment node has one indexed output port");
    let right_values = indexes
        .output(right.id)
        .expect("every fragment node has one indexed output port");
    let declared = declared_nulls.iter().copied().collect::<BTreeSet<_>>();
    if declared.len() != declared_nulls.len() {
        errors.push(ValidationError::new(
            path,
            "join NULL-extended value identities are duplicated",
        ));
    }
    let output_nulls = node
        .output
        .columns
        .iter()
        .filter_map(|value| {
            fragment.values().get(value).and_then(|definition| {
                matches!(
                    definition.origin,
                    ValueOrigin::NullExtended { node: owner, .. } if owner == node.id
                )
                .then_some(*value)
            })
        })
        .collect::<BTreeSet<_>>();
    if output_nulls != declared {
        errors.push(ValidationError::new(
            path,
            "join NULL-extended declarations differ from its output values",
        ));
    }
    for value in &node.output.columns {
        let allowed_original = match kind {
            crate::JoinKind::Cross | crate::JoinKind::Inner => {
                left_values.contains(value) || right_values.contains(value)
            }
            crate::JoinKind::LeftOuter
            | crate::JoinKind::LeftSemi
            | crate::JoinKind::LeftAnti
            | crate::JoinKind::NullAwareLeftAnti => left_values.contains(value),
            crate::JoinKind::RightOuter
            | crate::JoinKind::RightSemi
            | crate::JoinKind::RightAnti => right_values.contains(value),
            crate::JoinKind::FullOuter => false,
        };
        if allowed_original {
            continue;
        }
        let valid_null_extension = declared.contains(value)
            && fragment.values().get(value).is_some_and(|definition| {
                matches!(
                    definition.origin,
                    ValueOrigin::NullExtended { node: owner, of }
                        if owner == node.id
                            && match kind {
                                crate::JoinKind::LeftOuter => right_values.contains(&of),
                                crate::JoinKind::RightOuter => left_values.contains(&of),
                                crate::JoinKind::FullOuter => {
                                    left_values.contains(&of) || right_values.contains(&of)
                                }
                                crate::JoinKind::Cross
                                | crate::JoinKind::Inner
                                | crate::JoinKind::LeftSemi
                                | crate::JoinKind::LeftAnti
                                | crate::JoinKind::NullAwareLeftAnti
                                | crate::JoinKind::RightSemi
                                | crate::JoinKind::RightAnti => false,
                            }
                )
            });
        if !valid_null_extension {
            errors.push(ValidationError::new(
                path,
                format!(
                    "join output value {} is not valid for its join kind",
                    value.get()
                ),
            ));
        }
    }
}

pub(crate) fn value_origin_allowed(
    node: &PhysicalNode,
    definition: &ValueDef,
    ordinal: usize,
    input_values: &VisibleInputIndex,
    aggregate_call_ids: &BTreeSet<crate::AggregateCallId>,
    repeat_grouping_values: &BTreeSet<(ValueId, ValueId)>,
) -> bool {
    match (&node.kind, &definition.origin) {
        (NodeKind::Scan { .. }, ValueOrigin::ProviderField { scan_node, .. }) => {
            *scan_node == node.id
        }
        (
            NodeKind::Scan { .. } | NodeKind::Project { .. } | NodeKind::Window { .. },
            ValueOrigin::Expr { node: owner, .. },
        ) => *owner == node.id,
        (
            NodeKind::Aggregate { .. },
            ValueOrigin::AggregateState { call, .. } | ValueOrigin::AggregateResult { call },
        ) => aggregate_call_ids.contains(call),
        (
            NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. },
            ValueOrigin::NullExtended { node: owner, of },
        ) => *owner == node.id && input_values.contains(of),
        (
            NodeKind::ExchangeSource {
                edge: node_edge, ..
            },
            ValueOrigin::ExchangeImport { edge, .. } | ValueOrigin::CteImport { edge, .. },
        ) => node_edge == edge,
        (
            NodeKind::TableWriter { .. } | NodeKind::TableFinish(_),
            ValueOrigin::WriterDerived { writer_node, .. },
        ) => *writer_node == node.id,
        (NodeKind::Repeat { .. }, ValueOrigin::NullExtended { node: owner, of }) => {
            *owner == node.id && repeat_grouping_values.contains(&(*of, definition.id))
        }
        (
            NodeKind::SetOp { .. },
            ValueOrigin::NodeOutput {
                node: owner,
                output_ordinal,
            },
        ) => {
            *owner == node.id
                && usize::try_from(*output_ordinal)
                    .ok()
                    .and_then(|ordinal| node.output.columns.get(ordinal))
                    == Some(&definition.id)
        }
        (
            NodeKind::Values { .. }
            | NodeKind::Repeat { .. }
            | NodeKind::Unpivot { .. }
            | NodeKind::GenerateSeries { .. }
            | NodeKind::TableFunction { .. }
            | NodeKind::ChangeEventExpand { .. },
            ValueOrigin::NodeOutput {
                node: owner,
                output_ordinal,
            },
        ) => *owner == node.id && usize::try_from(*output_ordinal).ok() == Some(ordinal),
        _ => false,
    }
}

pub(crate) fn validate_node_arity(node: &PhysicalNode, path: &str, errors: &mut ValidationContext) {
    let valid = match &node.kind {
        NodeKind::Scan { .. }
        | NodeKind::Values { .. }
        | NodeKind::GenerateSeries { .. }
        | NodeKind::ExchangeSource { .. } => node.inputs.is_empty(),
        NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. } => node.inputs.len() == 2,
        NodeKind::SetOp { .. } => node.inputs.len() >= 2,
        NodeKind::TableFunction { .. } => node.inputs.len() <= 1,
        _ => node.inputs.len() == 1,
    };
    if !valid {
        errors.push(ValidationError::new(path, "node has invalid input arity"));
    }
}

pub(crate) fn validate_node_semantics(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    path: &str,
    errors: &mut ValidationContext,
) {
    match &node.kind {
        NodeKind::Scan {
            relation,
            read_budget,
            provider_outputs,
            residuals,
            derived_values,
            ..
        } => {
            validate_relation(fragment, relation, path, errors);
            if read_budget.max_batch_rows == 0
                || read_budget.max_batch_rows > MAX_SCAN_BATCH_ROWS
                || read_budget.max_batch_bytes == 0
                || read_budget.max_batch_bytes > MAX_SCAN_BATCH_BYTES
            {
                errors.push(ValidationError::new(
                    path,
                    "scan read budget must be non-zero and within the supported bounds",
                ));
            }
            if provider_outputs.len() != relation.schema().len() {
                errors.push(ValidationError::new(
                    path,
                    "provider output mapping does not cover the exact relation schema",
                ));
            }
            for ((column, value), field) in provider_outputs.iter().zip(relation.schema()) {
                if column != &field.column {
                    errors.push(ValidationError::new(
                        path,
                        "provider output column differs from the relation schema ordinal",
                    ));
                }
                match fragment.values().get(value) {
                    Some(definition)
                        if definition.ty == field.ty
                            && matches!(
                                &definition.origin,
                                ValueOrigin::ProviderField { scan_node, field: origin }
                                    if *scan_node == node.id && origin == column
                            ) => {}
                    Some(_) => errors.push(ValidationError::new(
                        path,
                        "provider output value has inconsistent type or origin",
                    )),
                    None => require_value(fragment, *value, path, errors),
                }
            }
            for value in derived_values {
                match fragment.values().get(value) {
                    Some(definition) if matches!(definition.origin, ValueOrigin::Expr { node: owner, .. } if owner == node.id) =>
                        {}
                    Some(_) => errors.push(ValidationError::new(
                        path,
                        "derived scan value does not have this scan as its expression owner",
                    )),
                    None => require_value(fragment, *value, path, errors),
                }
            }
            for residual in residuals {
                require_boolean_expression(fragment, *residual, path, errors);
            }
            validate_scan_predicate_contract(fragment, node.id, relation, residuals, path, errors);
        }
        NodeKind::Filter { predicates } => {
            if predicates.is_empty() {
                errors.push(ValidationError::new(
                    path,
                    "filter requires at least one predicate",
                ));
            }
            for predicate in predicates {
                require_boolean_expression(fragment, *predicate, path, errors);
            }
            require_passthrough_output(fragment, node, path, errors);
        }
        NodeKind::Project { expressions } => {
            let input = node.inputs.first().and_then(|id| fragment.nodes().get(id));
            let input_values = input.and_then(|input| indexes.output(input.id));
            for (expression, value) in expressions {
                match (
                    fragment.expressions().get(*expression),
                    fragment.values().get(value),
                ) {
                    (Some(expression_node), Some(value_def))
                        if expression_node.ty == value_def.ty
                            && (matches!(
                                value_def.origin,
                                ValueOrigin::Expr { node: owner, expr: source }
                                    if owner == node.id && source == expression_node.id
                            ) || matches!(
                                expression_node.kind,
                                ExprKind::Value(source)
                                    if source == *value
                                        && input_values
                                            .as_ref()
                                            .is_some_and(|input| input.contains(&source))
                            )) => {}
                    (Some(_), Some(_)) => errors.push(ValidationError::new(
                        path,
                        "project output has inconsistent expression, type or owner",
                    )),
                    _ => {}
                }
            }
        }
        NodeKind::Aggregate {
            group_by,
            calls,
            grouping,
        } => {
            // A call that finalizes has read every row of its group, so a
            // node carrying one states its groups are complete.  The reverse
            // does not follow: a node can finish its groups and still hand on
            // state, which is what the phase between a dedup and the rollup
            // that reads it does.  Whether a node that claims complete groups
            // really has them is decided by its input's distribution, not by
            // its calls.
            if calls.iter().any(|call| {
                matches!(
                    call.binding.phase,
                    crate::AggregatePhase::Single | crate::AggregatePhase::Final { .. }
                )
            }) && *grouping != crate::AggregateGrouping::Complete
            {
                errors.push(ValidationError::new(
                    path,
                    "aggregate finalizes a call on groups it does not state are complete",
                ));
            }
            for (expression_id, output) in group_by {
                match (
                    fragment.expressions().get(*expression_id),
                    fragment.values().get(output),
                ) {
                    (Some(expression_node), Some(value))
                        if expression_node.ty == value.ty
                            && (matches!(expression_node.kind, ExprKind::Value(source) if source == *output)
                                || matches!(
                                    value.origin,
                                    ValueOrigin::Expr { node: owner, expr }
                                        if owner == node.id && expr == *expression_id
                                )) => {}
                    (Some(_), Some(_)) => errors.push(ValidationError::new(
                        path,
                        "aggregate grouping output has inconsistent expression, type or origin",
                    )),
                    _ => {}
                }
            }
            let mut ids = BTreeSet::new();
            // A node emits one row per group, and every call on it either
            // finishes its value there or hands on a state -- the engine
            // finalizes a node, not a call. Which side of that a call is on
            // is the only phase fact the calls must share: `count(distinct x),
            // sum(y)` finishing together reads values for one and a state for
            // the other, and the dedup below it starts one state while
            // merging the other.
            let finalizes = calls
                .first()
                .map(|call| call.binding.phase.produces_final_result());
            for call in calls {
                if !ids.insert(call.id) {
                    errors.push(ValidationError::new(
                        path,
                        "aggregate call identity is duplicated",
                    ));
                }
                if call.binding.function.kind != FunctionKind::Aggregate {
                    errors.push(ValidationError::new(
                        path,
                        "aggregate node has non-aggregate binding",
                    ));
                }
                if finalizes
                    .is_some_and(|expected| expected != call.binding.phase.produces_final_result())
                {
                    errors.push(ValidationError::new(
                        path,
                        "aggregate node finishes some calls and hands others on",
                    ));
                }
                validate_aggregate_value_inputs(
                    fragment,
                    &call.binding,
                    &call.arguments,
                    &call.order_by,
                    path,
                    errors,
                );
                if !call.binding.phase.consumes_logical_arguments() && call.distinct {
                    errors.push(ValidationError::new(
                        path,
                        "state-consuming aggregate phase cannot apply DISTINCT again",
                    ));
                }
                if let Some(output) = fragment.values().get(&call.output) {
                    let expected = match call.binding.phase {
                        AggregatePhase::Single | AggregatePhase::Final { .. } => {
                            &call.binding.function.result_type
                        }
                        AggregatePhase::Partial { .. } | AggregatePhase::Intermediate { .. } => {
                            &call.binding.intermediate_type
                        }
                    };
                    if &output.ty != expected {
                        errors.push(ValidationError::new(
                            path,
                            "aggregate output type differs from phase output",
                        ));
                    }
                    let expected_origin = match call.binding.phase {
                        AggregatePhase::Single | AggregatePhase::Final { .. } => matches!(
                            output.origin,
                            ValueOrigin::AggregateResult { call: id } if id == call.id
                        ),
                        AggregatePhase::Partial { .. } | AggregatePhase::Intermediate { .. } => {
                            matches!(
                                output.origin,
                                ValueOrigin::AggregateState { call: id, phase }
                                    if id == call.id && phase == call.binding.phase
                            )
                        }
                    };
                    if !expected_origin {
                        errors.push(ValidationError::new(
                            path,
                            "aggregate output origin differs from the call phase",
                        ));
                    }
                }
            }
        }
        NodeKind::HashJoin {
            kind,
            keys,
            build_side,
            distribution,
            residual,
            null_extended,
            ..
        } => {
            if *kind == crate::JoinKind::Cross {
                errors.push(ValidationError::new(
                    path,
                    "cross join cannot use the hash-join node contract",
                ));
            }
            let broadcast_build_is_safe = match kind {
                crate::JoinKind::Inner => true,
                crate::JoinKind::LeftOuter
                | crate::JoinKind::LeftSemi
                | crate::JoinKind::LeftAnti
                | crate::JoinKind::NullAwareLeftAnti => *build_side == crate::JoinSide::Right,
                crate::JoinKind::RightOuter
                | crate::JoinKind::RightSemi
                | crate::JoinKind::RightAnti => *build_side == crate::JoinSide::Left,
                crate::JoinKind::FullOuter | crate::JoinKind::Cross => false,
            };
            if *distribution == crate::JoinDistribution::BroadcastBuild && !broadcast_build_is_safe
            {
                errors.push(ValidationError::new(
                    path,
                    "broadcast-build join cannot publish build-side rows from replicated workers",
                ));
            }
            if keys.is_empty() {
                errors.push(ValidationError::new(
                    path,
                    "hash join must have at least one key",
                ));
            }
            for key in keys {
                if let (Some(left), Some(right)) = (
                    fragment.expressions().get(key.left),
                    fragment.expressions().get(key.right),
                ) && left.ty.data_type != right.ty.data_type
                {
                    errors.push(ValidationError::new(
                        path,
                        "hash join key pair does not have one exact execution data type",
                    ));
                }
            }
            if let Some(residual) = residual {
                require_boolean_expression(fragment, *residual, path, errors);
            }
            if let (Some(left), Some(right)) = (
                node.inputs.first().and_then(|id| fragment.nodes().get(id)),
                node.inputs.get(1).and_then(|id| fragment.nodes().get(id)),
            ) {
                validate_expression_values_on_port(
                    fragment,
                    &keys.iter().map(|key| key.left).collect::<Vec<_>>(),
                    indexes
                        .output(left.id)
                        .expect("every fragment node has one indexed output port"),
                    path,
                    errors,
                );
                validate_expression_values_on_port(
                    fragment,
                    &keys.iter().map(|key| key.right).collect::<Vec<_>>(),
                    indexes
                        .output(right.id)
                        .expect("every fragment node has one indexed output port"),
                    path,
                    errors,
                );
            }
            validate_null_extended(fragment, node.id, null_extended, path, errors);
            if node.required_inputs.len() == 2 {
                let left_keys = keys
                    .iter()
                    .filter_map(|key| crate::expression_value(fragment.expressions(), key.left))
                    .collect::<Vec<_>>();
                let right_keys = keys
                    .iter()
                    .filter_map(|key| crate::expression_value(fragment.expressions(), key.right))
                    .collect::<Vec<_>>();
                let compatible = match (
                    distribution,
                    &node.required_inputs[0],
                    &node.required_inputs[1],
                ) {
                    (
                        crate::JoinDistribution::Partitioned,
                        crate::PhysicalProperties {
                            distribution:
                                Distribution::Hash {
                                    keys: left,
                                    scheme: left_scheme,
                                },
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ..
                        },
                        crate::PhysicalProperties {
                            distribution:
                                Distribution::Hash {
                                    keys: right,
                                    scheme: right_scheme,
                                },
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ..
                        },
                    ) => {
                        left_scheme == right_scheme
                            && left.as_ref() == left_keys
                            && right.as_ref() == right_keys
                    }
                    (
                        crate::JoinDistribution::Colocated,
                        crate::PhysicalProperties {
                            distribution:
                                Distribution::BucketShuffle {
                                    keys: left,
                                    scheme: left_scheme,
                                },
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ..
                        },
                        crate::PhysicalProperties {
                            distribution:
                                Distribution::BucketShuffle {
                                    keys: right,
                                    scheme: right_scheme,
                                },
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ..
                        },
                    ) => {
                        left_scheme == right_scheme
                            && left.as_ref() == left_keys
                            && right.as_ref() == right_keys
                    }
                    (crate::JoinDistribution::BroadcastBuild, left, right) => match build_side {
                        crate::JoinSide::Left => {
                            left.distribution == Distribution::Broadcast
                                && left.row_multiplicity == RowMultiplicity::Replicated
                                && right.row_multiplicity == RowMultiplicity::SingleCopy
                        }
                        crate::JoinSide::Right => {
                            right.distribution == Distribution::Broadcast
                                && right.row_multiplicity == RowMultiplicity::Replicated
                                && left.row_multiplicity == RowMultiplicity::SingleCopy
                        }
                    },
                    (
                        crate::JoinDistribution::Singleton,
                        crate::PhysicalProperties {
                            distribution: Distribution::Singleton,
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ..
                        },
                        crate::PhysicalProperties {
                            distribution: Distribution::Singleton,
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ..
                        },
                    ) => true,
                    _ => false,
                };
                if !compatible {
                    errors.push(ValidationError::new(
                        path,
                        "hash join lacks the exact partition-space proof required by its distribution mode",
                    ));
                }
            }
        }
        NodeKind::NestLoopJoin {
            kind,
            distribution,
            predicate,
            null_extended,
        } => {
            if *kind == crate::JoinKind::Cross && (predicate.is_some() || !null_extended.is_empty())
            {
                errors.push(ValidationError::new(
                    path,
                    "cross join cannot carry a predicate or NULL-extension outputs",
                ));
            }
            if let Some(predicate) = predicate {
                require_boolean_expression(fragment, *predicate, path, errors);
            }
            validate_null_extended(fragment, node.id, null_extended, path, errors);
            if nest_loop_join_output_distribution(fragment, node, *kind, *distribution, *predicate)
                .is_none()
            {
                errors.push(ValidationError::new(
                    path,
                    "nested-loop join lacks a complete singleton or right-broadcast placement proof",
                ));
            }
        }
        NodeKind::Limit {
            limit: None,
            offset: 0,
        } => errors.push(ValidationError::new(
            path,
            "limit node has neither a limit nor an offset",
        )),
        NodeKind::Sort { order_by, mode } => {
            require_passthrough_output(fragment, node, path, errors);
            validate_ordering_expressions(fragment, node, indexes, order_by, path, errors);
            // A sort orders by its partition keys and then within them, so it
            // has keys as long as one of the two does: a window with
            // `PARTITION BY` and no `ORDER BY` still needs its partitions
            // grouped.
            let partition_keys = match mode {
                crate::SortMode::Global => 0,
                crate::SortMode::Analytic { partition_by }
                | crate::SortMode::PartitionTopN { partition_by, .. } => partition_by.len(),
            };
            if order_by.is_empty() && partition_keys == 0 {
                errors.push(ValidationError::new(path, "sort order is empty"));
            }
            match mode {
                crate::SortMode::Global => {}
                crate::SortMode::Analytic { partition_by } => {
                    validate_partition_ordering(
                        fragment,
                        node,
                        indexes,
                        partition_by,
                        path,
                        errors,
                    );
                }
                crate::SortMode::PartitionTopN {
                    partition_by,
                    limit,
                    ..
                } => {
                    validate_partition_ordering(
                        fragment,
                        node,
                        indexes,
                        partition_by,
                        path,
                        errors,
                    );
                    if *limit == 0 {
                        errors.push(ValidationError::new(
                            path,
                            "partition TopN sort requires a non-zero limit",
                        ));
                    }
                }
            }
        }
        NodeKind::TopN {
            order_by,
            limit,
            offset,
            phase,
        } => {
            require_passthrough_output(fragment, node, path, errors);
            validate_ordering_expressions(fragment, node, indexes, order_by, path, errors);
            if order_by.is_empty() {
                errors.push(ValidationError::new(path, "TopN order is empty"));
            }
            if limit.checked_add(*offset).is_none() {
                errors.push(ValidationError::new(
                    path,
                    "TopN limit and offset overflow the row-count domain",
                ));
            }
            if matches!(phase, crate::TopNPhase::Partial { .. }) && *offset != 0 {
                errors.push(ValidationError::new(
                    path,
                    "partial TopN cannot apply an offset before global completion",
                ));
            }
        }
        NodeKind::Limit { .. } => {
            require_passthrough_output(fragment, node, path, errors);
            let singleton = node
                .required_inputs
                .first()
                .is_some_and(|required| required.distribution == Distribution::Singleton)
                && node
                    .inputs
                    .first()
                    .and_then(|input| fragment.nodes().get(input))
                    .is_some_and(|input| {
                        input.output_properties.distribution == Distribution::Singleton
                    });
            if !singleton {
                errors.push(ValidationError::new(
                    path,
                    "global limit requires a singleton input",
                ));
            }
        }
        NodeKind::SetOp {
            kind,
            input_mappings,
        } => {
            if input_mappings.len() != node.inputs.len()
                || input_mappings
                    .iter()
                    .any(|mapping| mapping.len() != node.output.columns.len())
            {
                errors.push(ValidationError::new(
                    path,
                    "set operation mappings must match every input and output ordinal",
                ));
            }
            for (input_ordinal, mapping) in input_mappings.iter().enumerate() {
                let Some(input_values) = node
                    .inputs
                    .get(input_ordinal)
                    .and_then(|input| indexes.output(*input))
                else {
                    continue;
                };
                for (output_ordinal, input_value) in mapping.iter().enumerate() {
                    if !input_values.contains(input_value) {
                        errors.push(ValidationError::new(
                            path,
                            "set operation mapping references a value outside its input port",
                        ));
                    }
                    if let Some(output_value) = node.output.columns.get(output_ordinal)
                        && let (Some(input_value), Some(output_value)) = (
                            fragment.values().get(input_value),
                            fragment.values().get(output_value),
                        )
                        && (input_value.ty.data_type != output_value.ty.data_type
                            || (input_value.ty.nullable && !output_value.ty.nullable))
                    {
                        // A set operation's column admits null when any branch
                        // it reads does, so a branch that never writes null
                        // still belongs in it; one that admits null the column
                        // does not is the mismatch.
                        errors.push(ValidationError::new(
                            path,
                            "set operation input type differs from its output ordinal",
                        ));
                    }
                }
            }
            if *kind != crate::SetOperationKind::UnionAll
                && set_operation_output_distribution(fragment, node, *kind).is_none()
            {
                errors.push(ValidationError::new(
                    path,
                    "INTERSECT and EXCEPT require singleton inputs or one exact shared partition space over every comparison ordinal",
                ));
            }
        }
        NodeKind::Values { rows } => {
            if rows
                .iter()
                .any(|row| row.len() != node.output.columns.len())
            {
                errors.push(ValidationError::new(
                    path,
                    "VALUES row width differs from output width",
                ));
            }
            for row in rows {
                for (ordinal, expression) in row.iter().enumerate() {
                    if let Some(output) = node.output.columns.get(ordinal)
                        && let (Some(expression), Some(output)) = (
                            fragment.expressions().get(*expression),
                            fragment.values().get(output),
                        )
                        && expression.ty != output.ty
                    {
                        errors.push(ValidationError::new(
                            path,
                            "VALUES expression type differs from its output ordinal",
                        ));
                    }
                }
            }
        }
        NodeKind::ExchangeSource { edge, imports } => {
            for (source, destination) in imports {
                match fragment.values().get(destination) {
                    Some(value)
                        if matches!(
                            value.origin,
                            ValueOrigin::ExchangeImport {
                                edge: value_edge,
                                source_value
                            } if value_edge == *edge && source_value == *source
                        ) || matches!(
                            value.origin,
                            ValueOrigin::CteImport {
                                edge: value_edge,
                                producer_value,
                                ..
                            } if value_edge == *edge && producer_value == *source
                        ) => {}
                    Some(_) => errors.push(ValidationError::new(
                        path,
                        "exchange import mapping differs from destination value origin",
                    )),
                    None => require_value(fragment, *destination, path, errors),
                }
            }
            if !imports
                .iter()
                .map(|(_, destination)| destination)
                .eq(node.output.columns.iter())
            {
                errors.push(ValidationError::new(
                    path,
                    "exchange output occurrences differ from its exact import sequence",
                ));
            }
        }
        NodeKind::TableWriter { target } => {
            let child = node
                .inputs
                .first()
                .and_then(|child| fragment.nodes().get(child));
            let child_output = child
                .map(|child| child.output.columns.as_ref())
                .unwrap_or_default();
            let child_values = indexes
                .visible_input(node.id)
                .expect("every fragment node has one indexed visible-input port");
            if target.input.as_ref() != child_output {
                errors.push(ValidationError::new(
                    path,
                    "table writer input differs from its exact child output",
                ));
            }
            for value in &target.input {
                require_value(fragment, *value, path, errors);
            }
            validate_distribution(fragment, &target.required_distribution, "writer", errors);
            if target.required_distribution == Distribution::Broadcast
                || child.is_none_or(|child| {
                    child.output_properties.row_multiplicity != RowMultiplicity::SingleCopy
                })
                || node
                    .required_inputs
                    .first()
                    .is_none_or(|required| required.row_multiplicity != RowMultiplicity::SingleCopy)
            {
                errors.push(ValidationError::new(
                    path,
                    "table writer requires a non-replicated owned input distribution",
                ));
            }
            if node
                .required_inputs
                .first()
                .is_none_or(|required| required.distribution != target.required_distribution)
            {
                errors.push(ValidationError::new(
                    path,
                    "table writer has two different input distribution contracts",
                ));
            }
            validate_encoded_payload(
                None,
                &target.handle,
                &[ConnectorCodecCategory::WriteHandle],
                path,
                errors,
            );
            validate_writer_schema(
                fragment,
                &target.output_schema,
                WriterRelationContract::Multiplex,
                Some(node.id),
                path,
                errors,
            );
            let mut target_tokens = BTreeSet::new();
            for field in &target.target_fields {
                if !target_tokens.insert(&field.token) {
                    errors.push(ValidationError::new(
                        path,
                        "table writer repeats a target field token",
                    ));
                }
                match fragment.values().get(&field.input) {
                    // A field states the type its target accepts, and the
                    // value it reads must have that data type. Either side
                    // may admit more nulls; whether this row can be written
                    // is the target's answer when the row reaches it.
                    Some(value) if value.ty.data_type != field.ty.data_type => {
                        errors.push(ValidationError::new(
                            path,
                            format!(
                                "writer target field states {:?} but reads {:?}",
                                field.ty, value.ty
                            ),
                        ));
                    }
                    Some(_) => {}
                    None => require_value(fragment, field.input, path, errors),
                }
                if !child_values.contains(&field.input) {
                    errors.push(ValidationError::new(
                        path,
                        "table writer target field is absent from its exact child output",
                    ));
                }
            }
            validate_writer_aggregates(fragment, &target.partial_aggregates, path, errors);
            validate_writer_aggregate_ports(
                fragment,
                &target.partial_aggregates,
                node.id,
                child_values,
                path,
                errors,
            );
        }
        NodeKind::TableFinish(spec) => {
            if spec.expected_target_ordinals.is_empty()
                || spec
                    .expected_target_ordinals
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1])
            {
                errors.push(ValidationError::new(
                    path,
                    "table finish target ordinals must be non-empty and strictly increasing",
                ));
            }
            if spec.expected_target_ordinals.len() > MAX_CONNECTOR_WRITE_TARGETS {
                errors.push(ValidationError::resource_limit(
                    path,
                    "table finish target count exceeds the connector contract bound",
                ));
            }
            validate_writer_schema(
                fragment,
                &spec.input_schema,
                WriterRelationContract::Multiplex,
                None,
                path,
                errors,
            );
            validate_writer_schema(
                fragment,
                &spec.output_schema,
                WriterRelationContract::RootResult,
                Some(node.id),
                path,
                errors,
            );
            validate_writer_aggregates(fragment, &spec.final_aggregates, path, errors);
            let child_output = node
                .inputs
                .first()
                .and_then(|child| fragment.nodes().get(child))
                .map(|child| child.output.columns.as_ref())
                .unwrap_or_default();
            let finish_input_is_single_copy = node
                .inputs
                .first()
                .and_then(|child| fragment.nodes().get(child))
                .is_some_and(|child| {
                    child.output_properties.distribution == Distribution::Singleton
                        && child.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                })
                && node.required_inputs.first().is_some_and(|required| {
                    required.distribution == Distribution::Singleton
                        && required.row_multiplicity == RowMultiplicity::SingleCopy
                });
            if !finish_input_is_single_copy {
                errors.push(ValidationError::new(
                    path,
                    "table finish requires singleton writer results with single-copy ownership",
                ));
            }
            let input_schema_values = spec
                .input_schema
                .fields
                .iter()
                .map(|field| field.value)
                .collect::<Vec<_>>();
            if input_schema_values.as_slice() != child_output {
                errors.push(ValidationError::new(
                    path,
                    "table finish input schema differs from its exact child output",
                ));
            }
            let child_values = indexes
                .visible_input(node.id)
                .expect("every fragment node has one indexed visible-input port");
            if spec
                .final_aggregates
                .iter()
                .any(|call| !child_values.contains(&call.input))
            {
                errors.push(ValidationError::new(
                    path,
                    "table finish aggregate input is absent from its exact child output",
                ));
            }
            validate_writer_aggregate_ports(
                fragment,
                &spec.final_aggregates,
                node.id,
                child_values,
                path,
                errors,
            );
            if spec.final_aggregates.is_empty() != spec.grouped_unpivot.is_none() {
                errors.push(ValidationError::new(
                    path,
                    "table finish final aggregates and grouped Unpivot must be both absent or both present",
                ));
            }
            if let Some(unpivot) = &spec.grouped_unpivot {
                validate_writer_grouped_unpivot(fragment, node.id, spec, unpivot, path, errors);
            }
        }
        NodeKind::TableFunction {
            function,
            arguments,
            outputs,
            left_outer,
        } => {
            if !function
                .intrinsic_row_error
                .is_valid_for_kind(crate::FunctionKind::Table)
            {
                errors.push(ValidationError::new(
                    path,
                    "bound table function has a non-row intrinsic fact",
                ));
            }
            if function.result_types.is_empty() {
                errors.push(ValidationError::new(
                    path,
                    "table function has an empty relation result schema",
                ));
            }
            validate_function_arguments(
                fragment,
                &function.function_id,
                &function.argument_types,
                arguments,
                path,
                errors,
            );
            if *left_outer && node.inputs.len() != 1 {
                errors.push(ValidationError::new(
                    path,
                    "left-outer table function requires one outer input",
                ));
            }
            let input_values = indexes
                .visible_input(node.id)
                .expect("every fragment node has one indexed visible-input port");
            let mut seen_results = BTreeSet::new();
            for (output_ordinal, output) in outputs.iter().enumerate() {
                match *output {
                    crate::TableFunctionOutput::PassThrough(value) => {
                        if !input_values.contains(&value) {
                            errors.push(ValidationError::new(
                                path,
                                "table function pass-through value is absent from its outer input",
                            ));
                        }
                    }
                    crate::TableFunctionOutput::FunctionResult {
                        result_ordinal,
                        value,
                    } => {
                        let Ok(result_ordinal) = usize::try_from(result_ordinal) else {
                            errors.push(ValidationError::new(
                                path,
                                "table function result ordinal is outside the host range",
                            ));
                            continue;
                        };
                        if !seen_results.insert(result_ordinal) {
                            errors.push(ValidationError::new(
                                path,
                                "table function result ordinal is duplicated",
                            ));
                        }
                        let Some(expected) = function.result_types.get(result_ordinal) else {
                            errors.push(ValidationError::new(
                                path,
                                "table function result ordinal is outside its bound schema",
                            ));
                            continue;
                        };
                        if let Some(definition) = fragment.values().get(&value) {
                            let mut expected = expected.clone();
                            expected.nullable |= *left_outer;
                            if definition.ty != expected {
                                errors.push(ValidationError::new(
                                    path,
                                    "table function result type differs from its bound schema",
                                ));
                            }
                            if !matches!(
                                definition.origin,
                                ValueOrigin::NodeOutput { node: owner, output_ordinal: origin_ordinal }
                                    if owner == node.id
                                        && usize::try_from(origin_ordinal).ok() == Some(output_ordinal)
                            ) {
                                errors.push(ValidationError::new(
                                    path,
                                    "table function result value is not owned by its output occurrence",
                                ));
                            }
                        }
                    }
                }
            }
            if seen_results.len() != function.result_types.len()
                || !(0..function.result_types.len()).all(|ordinal| seen_results.contains(&ordinal))
            {
                errors.push(ValidationError::new(
                    path,
                    "table function outputs do not cover its bound relation result schema exactly",
                ));
            }
            if outputs.len() != node.output.columns.len() {
                errors.push(ValidationError::new(
                    path,
                    "table function output mapping differs from its output port",
                ));
            } else {
                for (mapped, value) in outputs.iter().zip(&node.output.columns) {
                    if mapped.value() != *value {
                        errors.push(ValidationError::new(
                            path,
                            "table function output mapping differs from its output port",
                        ));
                    }
                }
            }
            for value in &node.output.columns {
                if fragment.values().get(value).is_none() {
                    errors.push(ValidationError::new(
                        path,
                        "table function output value is not defined",
                    ));
                }
            }
        }
        NodeKind::AssertOneRow(spec) => {
            require_passthrough_output(fragment, node, path, errors);
            match spec {
                crate::RowCountAssertionSpec::Global { subject, .. } => {
                    if subject.is_empty() {
                        errors.push(ValidationError::new(
                            path,
                            "global row-count assertion subject is empty",
                        ));
                    }
                }
                crate::RowCountAssertionSpec::PerKeyAtMostOne {
                    keys,
                    labels,
                    message,
                } => {
                    if keys.is_empty() || keys.len() != labels.len() {
                        errors.push(ValidationError::new(
                            path,
                            "keyed row-count assertion requires matching non-empty keys and labels",
                        ));
                    }
                    if labels.iter().any(|label| label.is_empty()) || message.is_empty() {
                        errors.push(ValidationError::new(
                            path,
                            "keyed row-count assertion labels and message must be non-empty",
                        ));
                    }
                    let input_values = indexes
                        .visible_input(node.id)
                        .expect("every fragment node has one indexed visible-input port");
                    for value in keys {
                        require_value(fragment, *value, path, errors);
                        if !input_values.contains(value) {
                            errors.push(ValidationError::new(
                                path,
                                "keyed row-count assertion key is absent from its exact child port",
                            ));
                        }
                    }
                }
            }
        }
        NodeKind::Window(spec) => {
            if !spec.partition_by.is_empty() {
                validate_partition_ordering(
                    fragment,
                    node,
                    indexes,
                    &spec.partition_by,
                    path,
                    errors,
                );
            }
            validate_ordering_expressions(fragment, node, indexes, &spec.order_by, path, errors);
            if spec.expressions.is_empty() {
                errors.push(ValidationError::new(
                    path,
                    "window node has no function calls",
                ));
            }
            for item in &spec.expressions {
                if let Some(expression) = fragment.expressions().get(item.expression)
                    && let ExprKind::WindowCall {
                        frame: Some(frame), ..
                    } = &expression.kind
                    && frame_has_offset(frame)
                {
                    if frame.units == crate::WindowFrameUnits::Range && spec.order_by.len() != 1 {
                        errors.push(ValidationError::new(
                            path,
                            "RANGE window frame with an offset requires exactly one order key",
                        ));
                    }
                    if frame.units == crate::WindowFrameUnits::Groups && spec.order_by.is_empty() {
                        errors.push(ValidationError::new(
                            path,
                            "GROUPS window frame with an offset requires an order key",
                        ));
                    }
                }
                match (
                    fragment.expressions().get(item.expression),
                    fragment.values().get(&item.output),
                ) {
                    (Some(expression), Some(output))
                        if expression.ty == output.ty
                            && expression.owner == node.id
                            && expression.lambda_scope.is_none()
                            && matches!(expression.kind, ExprKind::WindowCall { .. })
                            && matches!(
                                output.origin,
                                ValueOrigin::Expr { node: owner, expr }
                                    if owner == node.id && expr == item.expression
                            ) => {}
                    (Some(_), Some(_)) => errors.push(ValidationError::new(
                        path,
                        "window output has inconsistent expression, type or origin",
                    )),
                    _ => {}
                }
            }
        }
        NodeKind::Repeat {
            rollup_keys,
            grouping_sets,
            grouping_values,
            grouping_outputs,
        } => {
            let input = node.inputs.first().and_then(|id| fragment.nodes().get(id));
            let input_values = indexes
                .visible_input(node.id)
                .expect("every fragment node has one indexed visible-input port");
            for value in rollup_keys
                .iter()
                .chain(grouping_sets.iter().flatten())
                .chain(
                    grouping_outputs
                        .iter()
                        .flat_map(|output| output.arguments.iter()),
                )
            {
                if input.is_some() && !input_values.contains(value) {
                    errors.push(ValidationError::new(
                        path,
                        "repeat grouping value is absent from its input port",
                    ));
                }
            }
            // The keys are the domain every set is read against, so each one
            // stands exactly once and no set names a key outside it.
            let keys = rollup_keys.iter().copied().collect::<BTreeSet<_>>();
            if keys.len() != rollup_keys.len()
                || grouping_sets
                    .iter()
                    .flatten()
                    .any(|value| !keys.contains(value))
            {
                errors.push(ValidationError::new(
                    path,
                    "repeat grouping set names a key outside the rollup keys",
                ));
            }
            let nullable_inputs = keys
                .iter()
                .copied()
                .filter(|value| {
                    grouping_sets
                        .iter()
                        .any(|grouping_set| !grouping_set.contains(value))
                })
                .collect::<BTreeSet<_>>();
            let mappings = grouping_values
                .iter()
                .map(|(input, output)| (*input, *output))
                .collect::<BTreeMap<_, _>>();
            if mappings.len() != grouping_values.len()
                || mappings.keys().copied().collect::<BTreeSet<_>>() != nullable_inputs
            {
                errors.push(ValidationError::new(
                    path,
                    "repeat nullable grouping mappings do not cover the exact nullable inputs",
                ));
            }
            for (input, output) in grouping_values {
                match fragment.values().get(output) {
                    Some(definition)
                        if matches!(
                            definition.origin,
                            ValueOrigin::NullExtended { node: owner, of }
                                if owner == node.id && of == *input
                        ) => {}
                    Some(_) => errors.push(ValidationError::new(
                        path,
                        "repeat grouping output is not the declared NULL extension",
                    )),
                    None => require_value(fragment, *output, path, errors),
                }
            }
        }
        NodeKind::GenerateSeries { start, stop, step } => {
            if node.output.columns.len() != 1 {
                errors.push(ValidationError::new(
                    path,
                    "generate-series requires exactly one output value",
                ));
            }
            let output_type = node
                .output
                .columns
                .first()
                .and_then(|value| fragment.values().get(value))
                .map(|value| &value.ty);
            for argument in [Some(*start), Some(*stop), *step].into_iter().flatten() {
                if let (Some(output_type), Some(argument)) =
                    (output_type, fragment.expressions().get(argument))
                    && &argument.ty != output_type
                {
                    errors.push(ValidationError::new(
                        path,
                        "generate-series argument type differs from its output",
                    ));
                }
            }
        }
        NodeKind::ChangeEventExpand {
            events,
            effect_output,
        } => {
            require_value(fragment, *effect_output, path, errors);
            let effect_is_exact = fragment.values().get(effect_output).is_some_and(|value| {
                value.ty.data_type == DataType::Int8
                    && !value.ty.nullable
                    && matches!(
                        value.origin,
                        ValueOrigin::NodeOutput {
                            node: owner,
                            output_ordinal,
                        } if owner == node.id
                            && usize::try_from(output_ordinal)
                                .ok()
                                .and_then(|ordinal| node.output.columns.get(ordinal))
                                == Some(effect_output)
                    )
            });
            if !effect_is_exact {
                errors.push(ValidationError::new(
                    path,
                    "change-event effect output must be an exact non-null Int8 node output",
                ));
            }
            if events.is_empty() {
                errors.push(ValidationError::new(
                    path,
                    "change-event expansion has no events",
                ));
            }
            let output_values = indexes
                .output(node.id)
                .expect("every fragment node has one indexed output port");
            for event in events {
                if let Some(predicate) = event.predicate {
                    require_boolean_expression(fragment, predicate, path, errors);
                }
                let mut assigned_outputs = BTreeMap::new();
                for (output, expression) in &event.assignments {
                    require_value(fragment, *output, path, errors);
                    if *output == *effect_output {
                        errors.push(ValidationError::new(
                            path,
                            "change-event assignment targets the generated effect output",
                        ));
                    }
                    if !output_values.contains(output) {
                        errors.push(ValidationError::new(
                            path,
                            "change-event assignment output is absent from the node output port",
                        ));
                    }
                    if assigned_outputs
                        .insert(*output, expression.is_some())
                        .is_some()
                    {
                        errors.push(ValidationError::new(
                            path,
                            "change-event event contains a duplicate assignment output",
                        ));
                    }
                    if let Some(expression) = expression
                        && let (Some(output), Some(expression)) = (
                            fragment.values().get(output),
                            fragment.expressions().get(*expression),
                        )
                        && (output.ty.data_type != expression.ty.data_type
                            || (expression.ty.nullable && !output.ty.nullable))
                    {
                        errors.push(ValidationError::new(
                            path,
                            format!(
                                "change-event assignment type differs from its output: output {:?}, expression {:?}",
                                output.ty, expression.ty
                            ),
                        ));
                    }
                }
                for output in node
                    .output
                    .columns
                    .iter()
                    .copied()
                    .filter(|output| output != effect_output)
                {
                    let assigned_non_null = assigned_outputs.get(&output).copied().unwrap_or(false);
                    if !assigned_non_null
                        && fragment
                            .values()
                            .get(&output)
                            .is_some_and(|value| !value.ty.nullable)
                    {
                        errors.push(ValidationError::new(
                            path,
                            "change-event event leaves a non-null output without an expression",
                        ));
                    }
                }
            }
        }
        NodeKind::Unpivot { spec } => validate_unpivot(fragment, node, indexes, spec, path, errors),
    }
}

pub(crate) fn validate_aggregate_value_inputs(
    fragment: &Fragment,
    binding: &crate::AggregateBinding,
    args: &[ExprId],
    order_by: &[crate::SortExpr],
    path: &str,
    errors: &mut ValidationContext,
) {
    validate_aggregate_arguments(fragment, binding, args, order_by, path, errors);
}

/// An ordering key reads the rows the node receives.
///
/// A key written as a column names one of them directly, and that value has
/// to be on the child's port. A key written as an expression -- `ORDER BY
/// coalesce(a, b)`, or the merged column a FULL OUTER `USING` produces -- is
/// evaluated here over the same rows, and the values it reaches are checked
/// with every other expression this node owns. What such a node cannot do is
/// claim an ordering, because there is no value to name it by; that is
/// decided where its properties are.
pub(crate) fn validate_ordering_expressions(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    ordering: &[crate::SortExpr],
    path: &str,
    errors: &mut ValidationContext,
) {
    let input_values = node.inputs.first().and_then(|input| indexes.output(*input));
    for key in ordering {
        let Some(value) = crate::expression_value(fragment.expressions(), key.expr) else {
            continue;
        };
        if !input_values.is_some_and(|input| input.contains(&value)) {
            errors.push(ValidationError::new(
                path,
                "physical ordering key must be a direct value from the exact child port",
            ));
        }
    }
}

pub(crate) fn validate_partition_ordering(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    partition_by: &[crate::SortExpr],
    path: &str,
    errors: &mut ValidationContext,
) {
    if partition_by.is_empty() {
        errors.push(ValidationError::new(
            path,
            "partitioned ordering requires at least one partition key",
        ));
    }
    validate_ordering_expressions(fragment, node, indexes, partition_by, path, errors);
}

#[derive(Clone, Copy)]
pub(crate) enum WriterRelationContract {
    Multiplex,
    RootResult,
}

pub(crate) fn validate_writer_schema(
    fragment: &Fragment,
    schema: &crate::WriterRelationSchema,
    contract: WriterRelationContract,
    owner: Option<NodeId>,
    path: &str,
    errors: &mut ValidationContext,
) {
    let expected_revision = match contract {
        WriterRelationContract::Multiplex => crate::WRITER_MULTIPLEX_SCHEMA_REVISION,
        WriterRelationContract::RootResult => crate::ROOT_WRITE_RESULT_SCHEMA_REVISION,
    };
    let valid_width = match contract {
        WriterRelationContract::Multiplex => schema.fields.len() >= 4,
        WriterRelationContract::RootResult => schema.fields.len() == 8,
    };
    if schema.revision != expected_revision || !valid_width {
        errors.push(ValidationError::new(
            path,
            "writer relation schema has an unsupported revision or width",
        ));
    }
    let mut names = BTreeSet::new();
    let mut values = BTreeSet::new();
    for (ordinal, field) in schema.fields.iter().enumerate() {
        if field.name.is_empty()
            || !names.insert(field.name.as_ref())
            || !values.insert(field.value)
        {
            errors.push(ValidationError::new(
                path,
                "writer relation schema contains an empty/duplicate field or value",
            ));
        }
        match fragment.values().get(&field.value) {
            Some(value) if value.ty != field.ty => errors.push(ValidationError::new(
                path,
                "writer relation field type differs from its value",
            )),
            Some(_) => {}
            None => require_value(fragment, field.value, path, errors),
        }
        if !writer_relation_field_matches(contract, ordinal, field, schema.fields.len()) {
            errors.push(ValidationError::new(
                path,
                "writer relation field differs from its closed role contract",
            ));
        }
        if let Some(owner) = owner {
            let expected = match (field.role, contract) {
                (crate::WriterRelationFieldRole::Kind, _) => crate::WriterDerivedKind::RelationKind,
                (crate::WriterRelationFieldRole::TargetOrdinal, _) => {
                    crate::WriterDerivedKind::WriteTargetOrdinal
                }
                (crate::WriterRelationFieldRole::RowCount, _) => {
                    crate::WriterDerivedKind::AffectedRows
                }
                (crate::WriterRelationFieldRole::CommitFragment, _) => {
                    crate::WriterDerivedKind::CommitFragment
                }
                (crate::WriterRelationFieldRole::Auxiliary, _) => {
                    crate::WriterDerivedKind::RelationAuxiliary
                }
            };
            if !fragment.values().get(&field.value).is_some_and(|value| {
                matches!(
                    value.origin,
                    ValueOrigin::WriterDerived { writer_node, kind }
                        if writer_node == owner && kind == expected
                )
            }) {
                errors.push(ValidationError::new(
                    path,
                    "writer relation field origin differs from its role and owner",
                ));
            }
        }
    }
}

pub(crate) fn writer_relation_field_matches(
    contract: WriterRelationContract,
    actual_ordinal: usize,
    field: &crate::WriterRelationField,
    field_count: usize,
) -> bool {
    let ordinal = match contract {
        WriterRelationContract::Multiplex => match field.role {
            crate::WriterRelationFieldRole::Kind => 0,
            crate::WriterRelationFieldRole::TargetOrdinal => 1,
            crate::WriterRelationFieldRole::RowCount => 2,
            crate::WriterRelationFieldRole::CommitFragment => 3,
            crate::WriterRelationFieldRole::Auxiliary => {
                return actual_ordinal >= 4
                    && field_count > 4
                    && field.ty.nullable
                    && field.ty.data_type != DataType::Null;
            }
        },
        WriterRelationContract::RootResult => match field.role {
            crate::WriterRelationFieldRole::Kind => 0,
            crate::WriterRelationFieldRole::TargetOrdinal => 1,
            crate::WriterRelationFieldRole::RowCount => 2,
            crate::WriterRelationFieldRole::CommitFragment => 3,
            crate::WriterRelationFieldRole::Auxiliary => match field.name.as_ref() {
                "input_fields" => 4,
                "blob_type" => 5,
                "body" => 6,
                "properties" => 7,
                _ => return false,
            },
        },
    };
    if actual_ordinal != ordinal {
        return false;
    }
    match (contract, ordinal) {
        (_, 0) => {
            field.name.as_ref() == "kind"
                && field.role == crate::WriterRelationFieldRole::Kind
                && field.ty == crate::ValueType::new(DataType::Int8, false)
        }
        (WriterRelationContract::Multiplex, 1) => {
            field.name.as_ref() == "write_target_ordinal"
                && field.role == crate::WriterRelationFieldRole::TargetOrdinal
                && field.ty == crate::ValueType::new(DataType::Int32, false)
        }
        (WriterRelationContract::RootResult, 1) => {
            field.name.as_ref() == "write_target_ordinal"
                && field.role == crate::WriterRelationFieldRole::TargetOrdinal
                && field.ty == crate::ValueType::new(DataType::Int32, true)
        }
        (_, 2) => {
            field.name.as_ref() == "row_count"
                && field.role == crate::WriterRelationFieldRole::RowCount
                && field.ty == crate::ValueType::new(DataType::Int64, true)
        }
        (_, 3) => {
            field.name.as_ref() == "commit_fragment"
                && field.role == crate::WriterRelationFieldRole::CommitFragment
                && field.ty == crate::ValueType::new(DataType::Binary, true)
        }
        (WriterRelationContract::RootResult, 4) => {
            matches!(
                &field.ty.data_type,
                DataType::List(item)
                    if item.name() == "item"
                        && item.data_type() == &DataType::Int32
                        && !item.is_nullable()
                        && item.metadata().is_empty()
            ) && field.ty.nullable
        }
        (WriterRelationContract::RootResult, 5) => {
            field.ty == crate::ValueType::new(DataType::Utf8, true)
        }
        (WriterRelationContract::RootResult, 6) => {
            field.ty == crate::ValueType::new(DataType::Binary, true)
        }
        (WriterRelationContract::RootResult, 7) => {
            matches!(
                &field.ty.data_type,
                DataType::Map(entries, false)
                    if entries.name() == "entries"
                        && !entries.is_nullable()
                        && entries.metadata().is_empty()
                        && matches!(entries.data_type(), DataType::Struct(fields)
                            if fields.len() == 2
                                && fields[0].name() == "key"
                                && fields[0].data_type() == &DataType::Utf8
                                && !fields[0].is_nullable()
                                && fields[0].metadata().is_empty()
                                && fields[1].name() == "value"
                                && fields[1].data_type() == &DataType::Utf8
                                && !fields[1].is_nullable()
                                && fields[1].metadata().is_empty())
            ) && field.ty.nullable
        }
        _ => false,
    }
}

pub(crate) fn validate_writer_aggregates(
    fragment: &Fragment,
    calls: &[crate::WriterAggregateCall],
    path: &str,
    errors: &mut ValidationContext,
) {
    for call in calls {
        if call.binding.function.kind != FunctionKind::Aggregate {
            errors.push(ValidationError::new(
                path,
                "writer aggregate has non-aggregate binding",
            ));
        }
        if call.binding.logical_argument_count != 1
            || call.binding.function.argument_types.len() != 1
        {
            errors.push(ValidationError::new(
                path,
                "writer aggregate carrier supports exactly one logical argument",
            ));
        }
        require_value(fragment, call.input, path, errors);
        require_value(fragment, call.output, path, errors);
        let expected_input = match call.binding.phase {
            AggregatePhase::Single | AggregatePhase::Partial { .. } => {
                match call.binding.function.argument_types.first() {
                    Some(crate::FunctionArgumentType::Value(value)) => Some(value),
                    Some(crate::FunctionArgumentType::Lambda { .. }) | None => {
                        errors.push(ValidationError::new(
                            path,
                            "writer aggregate requires one scalar value argument",
                        ));
                        None
                    }
                }
            }
            AggregatePhase::Intermediate { .. } | AggregatePhase::Final { .. } => {
                Some(&call.binding.intermediate_type)
            }
        };
        // Everything a writer aggregate reads is a column of the write
        // relation: the row being written, or the state a merging phase was
        // handed. Either may admit a null the declaration does not -- the row
        // because the statement's value may be null where the target is not,
        // the state because a row which is not an aggregate row carries
        // nothing in it. What the aggregate is, is its type; whether a given
        // row may be written is the target's own answer.
        if let (Some(expected), Some(actual)) = (expected_input, fragment.values().get(&call.input))
            && expected.data_type != actual.ty.data_type
        {
            errors.push(ValidationError::new(
                path,
                format!(
                    "writer aggregate reads {:?} where its phase accepts {:?}",
                    actual.ty, expected
                ),
            ));
        }
        let expected_output = match call.binding.phase {
            AggregatePhase::Single | AggregatePhase::Final { .. } => {
                &call.binding.function.result_type
            }
            AggregatePhase::Partial { .. } | AggregatePhase::Intermediate { .. } => {
                &call.binding.intermediate_type
            }
        };
        // A writer aggregate's output is a column of the write relation, and
        // that relation admits null there because a row which is not an
        // aggregate row carries nothing in it. So the column may be wider than
        // the phase produces. It may not be narrower: a column that claimed
        // non-null while the phase can produce a null would be read as a value
        // that was never written.
        if let Some(actual) = fragment.values().get(&call.output)
            && (actual.ty.data_type != expected_output.data_type
                || (expected_output.nullable && !actual.ty.nullable))
        {
            errors.push(ValidationError::new(
                path,
                "writer aggregate output type differs from its phase contract",
            ));
        }
    }
}

pub(crate) fn validate_writer_aggregate_ports(
    fragment: &Fragment,
    calls: &[crate::WriterAggregateCall],
    owner: NodeId,
    child_values: &VisibleInputIndex,
    path: &str,
    errors: &mut ValidationContext,
) {
    let mut outputs = BTreeSet::new();
    for call in calls {
        if !child_values.contains(&call.input) {
            errors.push(ValidationError::new(
                path,
                "writer aggregate input is absent from its exact child output",
            ));
        }
        if !outputs.insert(call.output) {
            errors.push(ValidationError::new(
                path,
                "writer aggregate output is duplicated",
            ));
        }
        if !fragment.values().get(&call.output).is_some_and(|value| {
            matches!(
                value.origin,
                ValueOrigin::WriterDerived { writer_node, .. } if writer_node == owner
            )
        }) {
            errors.push(ValidationError::new(
                path,
                "writer aggregate output is not owned by this writer node",
            ));
        }
    }
}

pub(crate) fn validate_unpivot(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    spec: &crate::UnpivotSpec,
    path: &str,
    errors: &mut ValidationContext,
) {
    if spec.mappings.is_empty() {
        errors.push(ValidationError::new(path, "unpivot requires mappings"));
    }
    if !validate_unpivot_resource_limits(
        fragment,
        spec.mappings.len(),
        spec.mappings
            .iter()
            .map(|mapping| mapping.constants.as_ref()),
        spec.max_output_rows,
        spec.max_output_bytes,
        path,
        errors,
    ) {
        return;
    }
    let child_values = indexes
        .visible_input(node.id)
        .expect("every fragment node has one indexed visible-input port");
    let mut output_roles = BTreeSet::new();
    for (input, output) in &spec.passthrough {
        require_value(fragment, *input, path, errors);
        require_value(fragment, *output, path, errors);
        if !child_values.contains(input) {
            errors.push(ValidationError::new(
                path,
                "unpivot passthrough input is absent from its exact child output",
            ));
        }
        if !output_roles.insert(*output) {
            errors.push(ValidationError::new(
                path,
                "unpivot output roles contain a duplicate value",
            ));
        }
        if let (Some(input), Some(output)) =
            (fragment.values().get(input), fragment.values().get(output))
            && input.ty != output.ty
        {
            errors.push(ValidationError::new(
                path,
                "unpivot passthrough changes the value type",
            ));
        }
    }
    require_value(fragment, spec.value_output, path, errors);
    if !output_roles.insert(spec.value_output) {
        errors.push(ValidationError::new(
            path,
            "unpivot output roles contain a duplicate value",
        ));
    }
    for value in &spec.literal_outputs {
        require_value(fragment, *value, path, errors);
        if !output_roles.insert(*value) {
            errors.push(ValidationError::new(
                path,
                "unpivot output roles contain a duplicate value",
            ));
        }
    }
    if output_roles.len() != node.output.columns.len()
        || !node
            .output
            .columns
            .iter()
            .all(|output| output_roles.contains(output))
    {
        errors.push(ValidationError::new(
            path,
            "unpivot output roles do not exactly cover the node output port",
        ));
    }
    let mut value_nullable = false;
    let mut literal_nullable = vec![false; spec.literal_outputs.len()];
    for mapping in &spec.mappings {
        require_value(fragment, mapping.input, path, errors);
        if !child_values.contains(&mapping.input) {
            errors.push(ValidationError::new(
                path,
                "unpivot mapping input is absent from its exact child output",
            ));
        }
        if let (Some(input), Some(output)) = (
            fragment.values().get(&mapping.input),
            fragment.values().get(&spec.value_output),
        ) {
            if input.ty.data_type != output.ty.data_type {
                errors.push(ValidationError::new(
                    path,
                    "unpivot mapping input type differs from its value output",
                ));
            }
            value_nullable |= input.ty.nullable;
        }
        if mapping.constants.len() != spec.literal_outputs.len() {
            errors.push(ValidationError::new(
                path,
                "unpivot constant width differs from literal output width",
            ));
        }
        for (index, (constant, output)) in mapping
            .constants
            .iter()
            .zip(&spec.literal_outputs)
            .enumerate()
        {
            if let Some(nullable) =
                validate_unpivot_constant(fragment, constant, *output, "unpivot", path, errors)
            {
                literal_nullable[index] |= nullable;
            }
        }
    }
    if fragment
        .values()
        .get(&spec.value_output)
        .is_some_and(|output| output.ty.nullable != value_nullable)
    {
        errors.push(ValidationError::new(
            path,
            "unpivot value output nullability differs from its mapping inputs",
        ));
    }
    for (output, nullable) in spec.literal_outputs.iter().zip(literal_nullable) {
        if fragment
            .values()
            .get(output)
            .is_some_and(|output| output.ty.nullable != nullable)
        {
            errors.push(ValidationError::new(
                path,
                "unpivot literal output nullability differs from its constants",
            ));
        }
    }
}

pub(crate) fn validate_writer_grouped_unpivot(
    fragment: &Fragment,
    owner: NodeId,
    finish: &crate::WriterFinishSpec,
    spec: &crate::WriterGroupedUnpivotSpec,
    path: &str,
    errors: &mut ValidationContext,
) {
    if finish.final_aggregates.is_empty() || spec.mappings.is_empty() {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot requires final aggregates and mappings",
        ));
    }
    if !validate_unpivot_resource_limits(
        fragment,
        spec.mappings.len(),
        spec.mappings
            .iter()
            .map(|mapping| mapping.constants.as_ref()),
        spec.max_output_rows,
        spec.max_output_bytes,
        path,
        errors,
    ) {
        return;
    }

    let input_values = finish
        .input_schema
        .fields
        .iter()
        .map(|field| field.value)
        .collect::<BTreeSet<_>>();
    let output_fields = finish
        .output_schema
        .fields
        .iter()
        .map(|field| (field.value, field.role))
        .collect::<BTreeMap<_, _>>();
    let aggregate_outputs = finish
        .final_aggregates
        .iter()
        .map(|call| call.output)
        .collect::<BTreeSet<_>>();
    let write_targets = finish
        .expected_target_ordinals
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let statistics_targets = spec
        .statistics_target_ordinals
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if spec.statistics_target_ordinals.is_empty()
        || statistics_targets.len() != spec.statistics_target_ordinals.len()
        || spec
            .statistics_target_ordinals
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot statistics target ordinals must be non-empty and strictly increasing",
        ));
    }
    if !statistics_targets.is_subset(&write_targets) {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot statistics targets are not write targets",
        ));
    }

    if !input_values.contains(&spec.grouping_input) {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot grouping input is absent from the finish input schema",
        ));
    }
    if finish
        .input_schema
        .fields
        .iter()
        .find(|field| field.value == spec.grouping_input)
        .is_none_or(|field| field.role != crate::WriterRelationFieldRole::TargetOrdinal)
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot grouping input is not the finish target ordinal field",
        ));
    }
    for (label, value) in [
        ("grouping input", spec.grouping_input),
        ("grouping output", spec.grouping_output),
    ] {
        match fragment.values().get(&value) {
            Some(definition) if definition.ty == crate::ValueType::new(DataType::Int32, false) => {}
            Some(_) => errors.push(ValidationError::new(
                path,
                format!("writer grouped Unpivot {label} must be non-null Int32"),
            )),
            None => require_value(fragment, value, path, errors),
        }
    }
    match fragment.values().get(&spec.passthrough_output) {
        Some(definition) if definition.ty.data_type == DataType::Int32 => {}
        Some(_) => errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot passthrough output must be Int32",
        )),
        None => require_value(fragment, spec.passthrough_output, path, errors),
    }
    if !fragment
        .values()
        .get(&spec.grouping_output)
        .is_some_and(|value| {
            matches!(
                value.origin,
                ValueOrigin::WriterDerived {
                    writer_node,
                    kind: crate::WriterDerivedKind::GroupingKey,
                } if writer_node == owner
            )
        })
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot grouping output is not owned by this finish node",
        ));
    }
    if output_fields.get(&spec.passthrough_output)
        != Some(&crate::WriterRelationFieldRole::TargetOrdinal)
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot passthrough output is not the finish target ordinal field",
        ));
    }
    if !fragment
        .values()
        .get(&spec.passthrough_output)
        .is_some_and(|value| {
            matches!(
                value.origin,
                ValueOrigin::WriterDerived {
                    writer_node,
                    kind: crate::WriterDerivedKind::WriteTargetOrdinal,
                } if writer_node == owner
            )
        })
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot passthrough output is not this finish node's target ordinal",
        ));
    }
    if output_fields.get(&spec.value_output) != Some(&crate::WriterRelationFieldRole::Auxiliary)
        || spec.literal_outputs.iter().any(|value| {
            output_fields.get(value) != Some(&crate::WriterRelationFieldRole::Auxiliary)
        })
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot value and literal outputs are not auxiliary finish fields",
        ));
    }
    let unpivot_auxiliary_outputs = std::iter::once(spec.value_output)
        .chain(spec.literal_outputs.iter().copied())
        .collect::<BTreeSet<_>>();
    let schema_auxiliary_outputs = finish
        .output_schema
        .fields
        .iter()
        .filter_map(|field| {
            (field.role == crate::WriterRelationFieldRole::Auxiliary).then_some(field.value)
        })
        .collect::<BTreeSet<_>>();
    if unpivot_auxiliary_outputs.len() != spec.literal_outputs.len() + 1
        || unpivot_auxiliary_outputs != schema_auxiliary_outputs
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot roles do not exactly cover distinct auxiliary finish fields",
        ));
    }

    let mut mapped_targets = BTreeSet::new();
    let mut mapped_inputs = BTreeSet::new();
    let mut mapping_keys = BTreeSet::new();
    let mut previous_target = None;
    for mapping in &spec.mappings {
        if previous_target.is_some_and(|previous| previous > mapping.write_target_ordinal) {
            errors.push(ValidationError::new(
                path,
                "writer grouped Unpivot mappings are not ordered by target ordinal",
            ));
        }
        previous_target = Some(mapping.write_target_ordinal);
        if !statistics_targets.contains(&mapping.write_target_ordinal) {
            errors.push(ValidationError::new(
                path,
                "writer grouped Unpivot mapping names an unexpected write target",
            ));
        }
        mapped_targets.insert(mapping.write_target_ordinal);
        if !mapping_keys.insert((mapping.write_target_ordinal, mapping.input)) {
            errors.push(ValidationError::new(
                path,
                "writer grouped Unpivot repeats a target and aggregate output mapping",
            ));
        }
        if !aggregate_outputs.contains(&mapping.input) {
            errors.push(ValidationError::new(
                path,
                "writer grouped Unpivot mapping input is not a final aggregate output",
            ));
        }
        if let (Some(input), Some(output)) = (
            fragment.values().get(&mapping.input),
            fragment.values().get(&spec.value_output),
        ) && input.ty.data_type != output.ty.data_type
        {
            errors.push(ValidationError::new(
                path,
                "writer grouped Unpivot aggregate type differs from its value output",
            ));
        }
        mapped_inputs.insert(mapping.input);
        if mapping.constants.len() != spec.literal_outputs.len() {
            errors.push(ValidationError::new(
                path,
                "writer grouped Unpivot constant width differs from its literal output width",
            ));
        }
        for (constant, output) in mapping.constants.iter().zip(&spec.literal_outputs) {
            validate_unpivot_constant(
                fragment,
                constant,
                *output,
                "writer grouped Unpivot",
                path,
                errors,
            );
        }
    }
    if mapped_targets != statistics_targets {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot mappings do not cover every statistics target",
        ));
    }
    if mapped_inputs != aggregate_outputs {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot mappings do not cover every final aggregate output",
        ));
    }
}

pub(crate) fn validate_unpivot_resource_limits<'a>(
    fragment: &Fragment,
    mapping_count: usize,
    constants: impl IntoIterator<Item = &'a [crate::UnpivotConstant]>,
    max_output_rows: u64,
    max_output_bytes: u64,
    path: &str,
    errors: &mut ValidationContext,
) -> bool {
    if max_output_rows == 0
        || max_output_bytes == 0
        || max_output_rows > MAX_UNPIVOT_OUTPUT_ROWS
        || max_output_bytes > MAX_UNPIVOT_OUTPUT_BYTES
    {
        errors.push(ValidationError::new(
            path,
            "unpivot row/byte bounds are zero or exceed the contract maximum",
        ));
        return false;
    }
    if mapping_count > errors.limits().unpivot_mappings {
        errors.push(ValidationError::resource_limit(
            path,
            "unpivot mapping count exceeds the contract maximum",
        ));
        return false;
    }

    let mut constant_count = 0_usize;
    let mut collection_items = 0_usize;
    let mut literal_bytes = 0_usize;
    for constants in constants {
        constant_count = match constant_count.checked_add(constants.len()) {
            Some(count) if count <= errors.limits().unpivot_constants => count,
            _ => {
                errors.push(ValidationError::resource_limit(
                    path,
                    "unpivot constant count exceeds the contract maximum",
                ));
                return false;
            }
        };
        for constant in constants {
            let (items, bytes) = match constant {
                crate::UnpivotConstant::Scalar(expression) => {
                    (0, unpivot_scalar_literal_bytes(fragment, *expression))
                }
                crate::UnpivotConstant::Int32List(values) => (
                    values.len(),
                    values.len().saturating_mul(std::mem::size_of::<i32>()),
                ),
                crate::UnpivotConstant::Utf8Map(entries) => (
                    entries.len(),
                    entries.iter().fold(0_usize, |total, (key, value)| {
                        total.saturating_add(key.len()).saturating_add(value.len())
                    }),
                ),
            };
            collection_items = collection_items.saturating_add(items);
            literal_bytes = literal_bytes.saturating_add(bytes);
            if collection_items > errors.limits().unpivot_collection_items
                || literal_bytes > MAX_UNPIVOT_LITERAL_BYTES
            {
                errors.push(ValidationError::resource_limit(
                    path,
                    "unpivot literal collections exceed the contract budget",
                ));
                return false;
            }
        }
    }
    true
}

pub(crate) fn unpivot_scalar_literal_bytes(fragment: &Fragment, expression: ExprId) -> usize {
    let Some(ExprKind::Literal(literal)) = fragment
        .expressions()
        .get(expression)
        .map(|expression| &expression.kind)
    else {
        return 0;
    };
    match literal {
        crate::LiteralValue::Utf8(value) => value.len(),
        crate::LiteralValue::Binary(value) => value.len(),
        crate::LiteralValue::Null => 0,
        crate::LiteralValue::Boolean(_) => std::mem::size_of::<bool>(),
        crate::LiteralValue::Int64(_)
        | crate::LiteralValue::UInt64(_)
        | crate::LiteralValue::Float64Bits(_)
        | crate::LiteralValue::Time64(_)
        | crate::LiteralValue::Timestamp(_) => std::mem::size_of::<u64>(),
        crate::LiteralValue::Date32(_) => std::mem::size_of::<u32>(),
        crate::LiteralValue::LargeInt(_)
        | crate::LiteralValue::Decimal128(_)
        | crate::LiteralValue::IntervalMonthDayNano(_) => std::mem::size_of::<u128>(),
        crate::LiteralValue::Decimal256(value) => value.len(),
    }
}

pub(crate) fn validate_unpivot_constant(
    fragment: &Fragment,
    constant: &crate::UnpivotConstant,
    output: ValueId,
    context: &str,
    path: &str,
    errors: &mut ValidationContext,
) -> Option<bool> {
    let output_type = fragment.values().get(&output).map(|value| &value.ty)?;
    let (matches, nullable) = match constant {
        crate::UnpivotConstant::Scalar(expression) => match fragment.expressions().get(*expression)
        {
            Some(expression) => (
                matches!(expression.kind, ExprKind::Literal(_))
                    && expression.ty.data_type == output_type.data_type,
                expression.ty.nullable,
            ),
            None => {
                errors.push(ValidationError::new(
                    path,
                    format!("{context} scalar constant expression is not defined"),
                ));
                return None;
            }
        },
        crate::UnpivotConstant::Int32List(_) => (
            matches!(
                &output_type.data_type,
                DataType::List(field)
                    if field.name() == "item"
                        && field.data_type() == &DataType::Int32
                        && !field.is_nullable()
                        && field.metadata().is_empty()
            ),
            false,
        ),
        crate::UnpivotConstant::Utf8Map(_) => (
            matches!(
                &output_type.data_type,
                DataType::Map(entries, false)
                    if entries.name() == "entries"
                        && !entries.is_nullable()
                        && entries.metadata().is_empty()
                        && matches!(entries.data_type(), DataType::Struct(fields)
                            if fields.len() == 2
                                && fields[0].name() == "key"
                                && fields[0].data_type() == &DataType::Utf8
                                && !fields[0].is_nullable()
                                && fields[0].metadata().is_empty()
                                && fields[1].name() == "value"
                                && fields[1].data_type() == &DataType::Utf8
                                && !fields[1].is_nullable()
                                && fields[1].metadata().is_empty())
            ),
            false,
        ),
    };
    if !matches {
        errors.push(ValidationError::new(
            path,
            format!("{context} constant type differs from its literal output"),
        ));
    }
    if let crate::UnpivotConstant::Utf8Map(entries) = constant
        && (entries.iter().any(|(key, _)| key.is_empty())
            || entries
                .windows(2)
                .any(|pair| pair[0].0.as_ref() >= pair[1].0.as_ref()))
    {
        errors.push(ValidationError::new(
            path,
            format!("{context} map keys must be non-empty and strictly increasing"),
        ));
    }
    Some(nullable)
}

pub(crate) fn validate_relation(
    fragment: &Fragment,
    relation: &Relation,
    path: &str,
    errors: &mut ValidationContext,
) {
    validate_read_reference(relation.read(), path, errors);
    if relation.work_source() == ConnectorReadWorkSource::WholeRelation
        && relation.read().relation.kind() != ConnectorReadRelationKind::SystemTable
    {
        errors.push(ValidationError::new(
            path,
            "whole-relation work is valid only for a system-table relation",
        ));
    }
    if relation.work_source() == ConnectorReadWorkSource::WholeRelation
        && (!matches!(
            relation.provided_properties().distribution,
            Distribution::Singleton
        ) || fragment.dop_domain().min != 1
            || fragment.dop_domain().max != 1)
    {
        errors.push(ValidationError::new(
            path,
            "whole-relation work requires singleton distribution and exactly one driver",
        ));
    }
    let relation_source = relation.source_binding();
    if relation_source.selection_digest == [0; 32] {
        errors.push(ValidationError::new(
            path,
            "relation selection digest is zero",
        ));
    }
    if relation.schema().is_empty() {
        errors.push(ValidationError::new(path, "relation schema is empty"));
    }
    for field in relation.schema() {
        validate_column_reference(Some(relation.read()), &field.column, path, errors);
    }
    for guarantee in relation.predicate_guarantees() {
        require_boolean_expression(fragment, guarantee.predicate, path, errors);
    }
    let mut artifact_ids = BTreeSet::new();
    for requirement in relation.artifact_inputs() {
        if !artifact_ids.insert(requirement.artifact) {
            errors.push(ValidationError::new(
                path,
                "relation has duplicate artifact input requirements",
            ));
        }
        if requirement.format.revision == 0 || requirement.schema.is_empty() {
            errors.push(ValidationError::new(
                path,
                "artifact input requirement has an invalid format or empty schema",
            ));
        }
        validate_read_reference(&requirement.source.source, path, errors);
        validate_coverage(&requirement.required_coverage, path, errors);
        if requirement.source != relation_source
            || requirement.required_coverage.selection_digest != requirement.source.selection_digest
        {
            errors.push(ValidationError::new(
                path,
                "artifact input requirement is not bound to the relation's exact source selection",
            ));
        }
    }
    validate_distribution(
        fragment,
        &relation.provided_properties().distribution,
        "relation.provided_properties",
        errors,
    );
    for key in &relation.provided_properties().ordering {
        require_value(fragment, key.value, path, errors);
    }
    if let Relation::Metadata(metadata) = relation
        && (metadata.coverage_evidence.is_empty()
            || metadata.coverage_evidence.len() > MAX_METADATA_COVERAGE_EVIDENCE_BYTES)
    {
        errors.push(ValidationError::new(
            path,
            "metadata relation coverage evidence must be bounded and non-empty",
        ));
    }
}

pub(crate) fn validate_scan_predicate_contract(
    fragment: &Fragment,
    scan: NodeId,
    relation: &Relation,
    residuals: &[ExprId],
    path: &str,
    errors: &mut ValidationContext,
) {
    let mut guarantees = BTreeMap::new();
    for guarantee in relation.predicate_guarantees() {
        match guarantees.entry(guarantee.predicate) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(guarantee.kind);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                let message = if *entry.get() == guarantee.kind {
                    "relation contains a duplicate guarantee for one predicate"
                } else {
                    "relation contains conflicting guarantees for one predicate"
                };
                errors.push(ValidationError::new(path, message));
            }
        }
        if fragment
            .expressions()
            .get(guarantee.predicate)
            .is_some_and(|expression| expression.owner != scan)
        {
            errors.push(ValidationError::new(
                path,
                "relation predicate guarantee is not owned by its scan",
            ));
        }
        if !fragment_expressions_are_replica_deterministic(
            fragment,
            std::iter::once(guarantee.predicate),
            true,
        ) {
            errors.push(ValidationError::new(
                path,
                "relation predicate guarantee must be replica deterministic",
            ));
        }
    }

    let mut residual_set = BTreeSet::new();
    for residual in residuals {
        if !residual_set.insert(*residual) {
            errors.push(ValidationError::new(
                path,
                "scan contains a duplicate residual predicate",
            ));
        }
        if fragment
            .expressions()
            .get(*residual)
            .is_some_and(|expression| expression.owner != scan)
        {
            errors.push(ValidationError::new(
                path,
                "scan residual predicate is not owned by its scan",
            ));
        }
    }

    for (predicate, kind) in guarantees {
        if kind == crate::PredicateGuaranteeKind::PruningOnly && !residual_set.contains(&predicate)
        {
            errors.push(ValidationError::new(
                path,
                "pruning-only relation predicate must be evaluated by the scan residual",
            ));
        }
    }
}

pub(crate) fn validate_read_reference(
    reference: &ProviderReadReference,
    path: &str,
    errors: &mut ValidationContext,
) {
    let descriptor = reference.binding.descriptor();
    let catalog = reference.binding.catalog_handle();
    if &descriptor.instance_id != catalog.catalog_name() {
        errors.push(ValidationError::new(
            path,
            "provider instance and catalog handle identify different bindings",
        ));
    }
    validate_encoded_payload(
        Some(reference),
        reference.relation.table(),
        &[ConnectorCodecCategory::ReadTable],
        path,
        errors,
    );
    validate_encoded_payload(
        Some(reference),
        reference.relation.view(),
        &[ConnectorCodecCategory::ReadView],
        path,
        errors,
    );
}

pub(crate) fn validate_column_reference(
    relation: Option<&ProviderReadReference>,
    column: &ProviderColumnReference,
    path: &str,
    errors: &mut ValidationContext,
) {
    validate_encoded_payload(
        relation,
        &column.column_payload,
        &[ConnectorCodecCategory::ReadColumn],
        path,
        errors,
    );
}

pub(crate) fn validate_encoded_payload(
    relation: Option<&ProviderReadReference>,
    payload: &ConnectorEncodedPayload,
    allowed_categories: &[ConnectorCodecCategory],
    path: &str,
    errors: &mut ValidationContext,
) {
    if payload.payload().is_empty() || payload.payload().len() > MAX_PROVIDER_PRIVATE_PAYLOAD_BYTES
    {
        errors.push(ValidationError::new(
            path,
            "provider-private payload must be bounded and non-empty",
        ));
    }
    if !allowed_categories.contains(&payload.header().category()) {
        errors.push(ValidationError::new(
            path,
            "provider-private payload has the wrong category",
        ));
    }
    if let Some(relation) = relation
        && (payload.header().provider_id() != &relation.binding.descriptor().provider_id
            || payload.header().catalog() != relation.binding.catalog_handle())
    {
        errors.push(ValidationError::new(
            path,
            "provider-private payload header differs from the exact relation binding",
        ));
    }
}

pub(crate) fn validate_null_extended(
    fragment: &Fragment,
    owner: NodeId,
    values: &[ValueId],
    path: &str,
    errors: &mut ValidationContext,
) {
    for value in values {
        match fragment.values().get(value) {
            Some(value) if matches!(value.origin, ValueOrigin::NullExtended { node, .. } if node == owner) =>
                {}
            Some(_) => errors.push(ValidationError::new(
                path,
                "join null-extension list contains a value with another origin",
            )),
            None => require_value(fragment, *value, path, errors),
        }
    }
}

pub(crate) fn require_passthrough_output(
    fragment: &Fragment,
    node: &PhysicalNode,
    path: &str,
    errors: &mut ValidationContext,
) {
    if let Some(input) = node.inputs.first().and_then(|id| fragment.nodes().get(id))
        && input.output.columns != node.output.columns
    {
        errors.push(ValidationError::new(
            path,
            "pass-through node changed value identities or output order",
        ));
    }
}

pub(crate) fn require_boolean_expression(
    fragment: &Fragment,
    expression: ExprId,
    path: &str,
    errors: &mut ValidationContext,
) {
    match fragment.expressions().get(expression) {
        Some(expression) if expression.ty.data_type == DataType::Boolean => {}
        Some(_) => errors.push(ValidationError::new(
            path,
            "predicate expression is not Boolean",
        )),
        None => errors.push(ValidationError::new(
            path,
            format!("expression {} is not defined", expression.get()),
        )),
    }
}

pub(crate) fn writer_schema_shapes_match(
    writer: &crate::WriterRelationSchema,
    finish: &crate::WriterRelationSchema,
) -> bool {
    writer.revision == finish.revision
        && writer.fields.len() == finish.fields.len()
        && writer
            .fields
            .iter()
            .zip(&finish.fields)
            .all(|(writer, finish)| {
                writer.name == finish.name && writer.ty == finish.ty && writer.role == finish.role
            })
}
