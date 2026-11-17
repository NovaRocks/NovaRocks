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
#[cfg(test)]
use crate::tests::unit_fixture::DefinitionFixture;

fn append_hash_exchange(
    builder: &mut FragmentBuilder,
    edge: EdgeId,
    scheme: &HashPartitionScheme,
) -> (NodeId, ValueId, PhysicalProperties) {
    let node = builder.reserve_node_id().unwrap();
    let source = ValueId::new(edge.get() + 10_000);
    let value = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport {
                edge,
                source_value: source,
            },
        )
        .unwrap();
    let properties = PhysicalProperties {
        distribution: Distribution::Hash {
            keys: Box::from([value]),
            scheme: scheme.clone(),
        },
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties.clone(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source, value)]),
            },
        })
        .unwrap();
    (node, value, properties)
}

fn finish_duplicate_comparison_intersect(
    distinct_outputs: bool,
) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(701));
    let scheme = hash_scheme(71);
    let (left, left_value, left_properties) =
        append_hash_exchange(&mut builder, EdgeId::new(701), &scheme);
    let (right, right_value, right_properties) =
        append_hash_exchange(&mut builder, EdgeId::new(702), &scheme);
    let set_op = builder.reserve_node_id().unwrap();
    let output = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node: set_op,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let second_output = if distinct_outputs {
        builder
            .add_value(
                ty(DataType::Int64, false),
                ValueOrigin::NodeOutput {
                    node: set_op,
                    output_ordinal: 1,
                },
            )
            .unwrap()
    } else {
        output
    };
    let output_keys: Box<[ValueId]> = if distinct_outputs {
        Box::from([output, second_output])
    } else {
        Box::from([output])
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: set_op,
            inputs: Box::from([left, right]),
            required_inputs: Box::from([left_properties, right_properties]),
            output_properties: PhysicalProperties {
                distribution: Distribution::Hash {
                    keys: output_keys,
                    scheme,
                },
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: set_op,
                columns: Box::from([output, second_output]),
            },
            kind: NodeKind::SetOp {
                kind: SetOperationKind::Intersect,
                input_mappings: Box::from([
                    Box::from([left_value, left_value]),
                    Box::from([right_value, right_value]),
                ]),
            },
        })
        .unwrap();

    builder.finish_fixture_definition(
        set_op,
        FragmentSink::Noop,
        dop(),
        crate::StagePlacementPolicy::Singleton,
    )
}

#[test]
fn intersect_preserves_duplicate_comparison_occurrences_in_one_hash_space() {
    finish_duplicate_comparison_intersect(false)
        .expect("duplicate equality occurrences share one unique hash key");
}

#[test]
fn intersect_cannot_publish_two_hash_keys_for_one_input_comparison_value() {
    let errors = finish_duplicate_comparison_intersect(true)
        .expect_err("one input hash key cannot prove two output hash keys");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("set operation output properties differ")
    }));
}
