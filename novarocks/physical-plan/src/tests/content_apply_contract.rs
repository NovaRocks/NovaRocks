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
use novarocks_connector_contract::{ConnectorRowMutationEffect, ConnectorWriteFieldToken};

#[derive(Clone, Copy)]
enum Shape {
    Valid,
    DifferentContentDomain,
    EmptyRoute,
    ForeignRouteOrdinal,
    MissingTargetPosition,
    MissingSummaryField,
    RelationalSummaryOutput,
    WrongAuthor,
}

fn receive(
    builder: &mut FragmentBuilder,
    edge: EdgeId,
    types: &[ValueType],
) -> (NodeId, Vec<ValueId>) {
    let id = builder.reserve_node_id().unwrap();
    let imports = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            let source_value = ValueId::new(ordinal as u32);
            let value = builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::ExchangeImport { edge, source_value },
                )
                .unwrap();
            (source_value, value)
        })
        .collect::<Box<[_]>>();
    let values = imports.iter().map(|&(_, value)| value).collect::<Vec<_>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            id,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: hash_properties(values[0]),
            output: OutputPort {
                node: id,
                columns: values.clone().into_boxed_slice(),
            },
            kind: NodeKind::ExchangeSource { edge, imports },
            expressions: None,
        })
        .unwrap();
    (id, values)
}

fn output(builder: &mut FragmentBuilder, node: NodeId, ordinal: u32, ty: ValueType) -> ValueId {
    builder
        .add_value(
            ty,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: ordinal,
            },
        )
        .unwrap()
}

fn summary(builder: &mut FragmentBuilder, kind: SummaryKind) -> SummaryColumns {
    let values = kind
        .fields()
        .iter()
        .map(|&field| {
            let ty = match field.wire_type() {
                SummaryWireType::Int64 => DataType::Int64,
                SummaryWireType::FixedSizeBinary(width) => {
                    DataType::FixedSizeBinary(i32::from(width))
                }
            };
            builder
                .add_value(ValueType::new(ty, false), ValueOrigin::Summary { field })
                .unwrap()
        })
        .collect::<Vec<_>>();
    match kind {
        SummaryKind::WriteCounts => SummaryColumns::write_counts(values[0], values[1], values[2]),
        SummaryKind::DeleteVerification => {
            SummaryColumns::delete_verification(values[0], values[1], values[2], values[3])
        }
    }
}

fn build(shape: Shape) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(1));
    let content_ty = ty(DataType::Int64, false);
    let weight_ty = ty(DataType::Int8, false);
    let (weighted, left) = receive(
        &mut builder,
        EdgeId::new(5),
        &[content_ty.clone(), weight_ty.clone()],
    );
    let metadata = [
        DataType::Utf8,
        DataType::Int64,
        DataType::Int32,
        DataType::Struct(Default::default()),
    ];
    let right_content_ty = if matches!(shape, Shape::DifferentContentDomain) {
        ty(DataType::Int32, false)
    } else {
        content_ty.clone()
    };
    let target_types = std::iter::once(right_content_ty)
        .chain(metadata.iter().cloned().map(|carrier| ty(carrier, false)))
        .collect::<Vec<_>>();
    let (target, right) = receive(&mut builder, EdgeId::new(6), &target_types);
    let apply = builder.reserve_node_id().unwrap();
    let mut columns = Vec::new();
    columns.push(output(&mut builder, apply, 0, content_ty.clone()));
    for (ordinal, carrier) in metadata.into_iter().enumerate() {
        columns.push(output(
            &mut builder,
            apply,
            ordinal as u32 + 1,
            ty(carrier, true),
        ));
    }
    columns.push(output(&mut builder, apply, 5, weight_ty.clone()));
    let data = columns[0];
    let verification = summary(&mut builder, SummaryKind::DeleteVerification);
    columns.extend(verification.values().iter().copied());
    if matches!(shape, Shape::MissingSummaryField) {
        columns.pop();
    }
    let route_keys = match shape {
        Shape::EmptyRoute => Box::from([]),
        Shape::ForeignRouteOrdinal => Box::from([1]),
        _ => Box::from([0]),
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            id: apply,
            inputs: Box::from([weighted, target]),
            required_inputs: Box::from([hash_properties(left[0]), hash_properties(right[0])]),
            output_properties: unconstrained(),
            output: OutputPort {
                node: apply,
                columns: columns.into_boxed_slice(),
            },
            kind: NodeKind::ContentApply(ContentApplySpec {
                weighted_content: Box::from([left[0]]),
                target_content: Box::from([right[0]]),
                route_keys,
                weight: left[1],
                target: ContentApplyTarget {
                    file: right[1],
                    position: if matches!(shape, Shape::MissingTargetPosition) {
                        left[0]
                    } else {
                        right[2]
                    },
                    spec_id: right[3],
                    partition: right[4],
                },
            }),
            expressions: None,
        })
        .unwrap();
    let root = builder.reserve_node_id().unwrap();
    let effect = output(&mut builder, root, 0, weight_ty);
    let writer_data = output(&mut builder, root, 1, content_ty.clone());
    let mut root_output = vec![effect, writer_data];
    if matches!(shape, Shape::RelationalSummaryOutput) {
        root_output.push(verification.values()[0]);
    }
    let definition = builder
        .define(root, content_ty, ExprKind::Value(data))
        .unwrap();
    let expr = builder.root_definition(
        root,
        definition,
        novarocks_type_contract::EvaluationDemand::Value,
        RootScope::Port(0),
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            id: root,
            inputs: Box::from([apply]),
            required_inputs: Box::from([unconstrained()]),
            output_properties: unconstrained(),
            output: OutputPort {
                node: root,
                columns: root_output.into_boxed_slice(),
            },
            kind: NodeKind::ChangeEventExpand {
                effect_output: effect,
                events: Box::from([ChangeEventSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Insert,
                    assignments: Box::from([ChangeAssignment::exact(writer_data, Some(expr))]),
                }]),
            },
            expressions: None,
        })
        .unwrap();
    let write_counts = summary(&mut builder, SummaryKind::WriteCounts);
    builder.set_summaries(Box::from([
        SummaryOutput {
            edge: EdgeId::new(8),
            columns: verification,
            author: SummaryAuthor::Node(if matches!(shape, Shape::WrongAuthor) {
                root
            } else {
                apply
            }),
        },
        SummaryOutput {
            edge: EdgeId::new(9),
            columns: write_counts,
            author: SummaryAuthor::RouterSink,
        },
    ]));
    builder.finish_fixture_definition(
        root,
        FragmentSink::Router {
            effect,
            routes: Box::from([ChangeStreamRoute {
                route_id: write_route_id(4),
                write_target_ordinal: write_target_ordinal(0),
                accepted_effects: Box::from([ConnectorRowMutationEffect::Insert]),
                input_mapping: Box::from([(
                    ConnectorWriteFieldToken::from_bytes([7; 32]),
                    writer_data,
                )]),
                requirements: WriteRouteRequirements {
                    colocation: WriteColocation::None,
                    layout: WriteLayoutPreference::None,
                },
                edge: EdgeId::new(7),
            }]),
        },
        dop(),
        StagePlacementPolicy::Singleton,
    )
}

#[test]
fn content_apply_publishes_data_and_verification_on_distinct_ports() {
    let fragment = build(Shape::Valid).unwrap();
    let root = &fragment.nodes()[&fragment.root()];
    let apply = &fragment.nodes()[&root.inputs[0]];
    assert_eq!(apply.data_output_columns().len(), 6);
    let summary = fragment
        .summaries()
        .iter()
        .find(|output| output.kind() == SummaryKind::DeleteVerification)
        .unwrap();
    assert_eq!(summary.author, SummaryAuthor::Node(apply.id));
    assert_eq!(&apply.output.columns[6..], summary.columns.values());
    let port = &root.expressions.as_ref().unwrap().ports()[0];
    assert!(
        port.values
            .iter()
            .map(|(value, _)| *value)
            .eq(apply.data_output_columns().iter().copied())
    );
    let input_consumption = &crate::wait_graph::input_consumption;
    use crate::wait_graph::InputConsumption;
    assert_eq!(
        input_consumption(&apply.kind, 0),
        InputConsumption::DrainedBeforeOutput
    );
    assert_eq!(
        input_consumption(&apply.kind, 1),
        InputConsumption::Pipelined
    );
}

/// The wait graph only consumes the local operator topology. Turn the two
/// receivers into leaves so this assertion needs no unrelated sender graph.
pub(crate) fn content_apply_wait_fixture() -> Fragment {
    let fragment = build(Shape::Valid).unwrap();
    let mut parts = fragment.into_parts();
    for node in parts.nodes.values_mut() {
        if matches!(node.kind, NodeKind::ExchangeSource { .. }) {
            node.kind = NodeKind::Values {
                rows: Box::default(),
            };
        }
    }
    Fragment::from(parts)
}

#[test]
fn content_apply_requires_the_same_partition_space_for_both_ports() {
    let fragment = build(Shape::Valid).unwrap();
    let apply = fragment.nodes()[&fragment.root()].inputs[0];
    let target = fragment.nodes()[&apply].inputs[1];
    let mut parts = fragment.into_parts();
    let Distribution::Hash { scheme, .. } = &mut parts
        .nodes
        .get_mut(&target)
        .unwrap()
        .output_properties
        .distribution
    else {
        panic!("target fixture is hash partitioned");
    };
    *scheme = hash_scheme(99);
    let fragment = Fragment::from(parts);
    let mut errors = crate::validation::ValidationContext::new();
    crate::validation::validate_node_output_properties(
        &fragment,
        &fragment.nodes()[&apply],
        "content apply",
        &mut errors,
    );
    assert!(errors.into_vec().iter().any(|error| {
        error
            .message()
            .contains("shared native exchange hash partition space")
    }));
}

#[test]
fn content_apply_rejects_inexact_content_position_routes_and_summary_authors() {
    for shape in [
        Shape::DifferentContentDomain,
        Shape::EmptyRoute,
        Shape::ForeignRouteOrdinal,
        Shape::MissingTargetPosition,
        Shape::MissingSummaryField,
        Shape::RelationalSummaryOutput,
        Shape::WrongAuthor,
    ] {
        assert!(build(shape).is_err());
    }
}
