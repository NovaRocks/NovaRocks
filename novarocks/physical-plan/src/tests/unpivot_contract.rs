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
use crate::tests::constant_fixture::ConstantFixture;
#[cfg(test)]
use crate::tests::unit_fixture::DefinitionFixture;

#[derive(Clone, Copy)]
enum UnpivotShape {
    Valid,
    Permuted,
    MissingOutput,
    ExtraOutput,
    DuplicateOutput,
    DuplicateReusedInput,
    WrongOutputOrdinal,
    MappingOutsideChild,
    MappingTypeDrift,
    ConstantTypeDrift,
}

fn finish_unpivot(shape: UnpivotShape) -> Result<(Fragment, ConstantPools), ValidationErrors> {
    let mut constants = ConstantFixture::new();
    let mut builder = FragmentBuilder::new(FragmentId::new(812));
    let source = builder.reserve_node_id().unwrap();
    let number_expr = builder
        .define(
            source,
            ty(DataType::Int64, false),
            constants.i64(7, &ty(DataType::Int64, false)),
        )
        .unwrap();
    let text_expr = builder
        .define(
            source,
            ty(DataType::Utf8, false),
            constants.utf8("source", &ty(DataType::Utf8, false)),
        )
        .unwrap();
    let number = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let text = builder
        .add_value(
            ty(DataType::Utf8, false),
            ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 1,
            },
        )
        .unwrap();
    let value_field_root_0 = builder.root_definition(
        source,
        number_expr,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    let value_field_root_1 = builder.root_definition(
        source,
        text_expr,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::AllPorts,
    );
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node: source,
                columns: Box::from([number, text]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([value_field_root_0, value_field_root_1])]),
            },
        })
        .unwrap();

    let unpivot = builder.reserve_node_id().unwrap();
    let constant = match shape {
        UnpivotShape::ConstantTypeDrift => builder
            .define(
                unpivot,
                ty(DataType::Int64, false),
                constants.i64(9, &ty(DataType::Int64, false)),
            )
            .unwrap(),
        _ => builder
            .define(
                unpivot,
                ty(DataType::Utf8, false),
                constants.utf8("label", &ty(DataType::Utf8, false)),
            )
            .unwrap(),
    };
    let constant = builder.root_definition(
        unpivot,
        constant,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::Port(0),
    );
    let value_output = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node: unpivot,
                output_ordinal: if matches!(shape, UnpivotShape::DuplicateReusedInput) {
                    2
                } else {
                    1
                },
            },
        )
        .unwrap();
    let literal_output = if matches!(shape, UnpivotShape::DuplicateReusedInput) {
        text
    } else {
        builder
            .add_value(
                ty(DataType::Utf8, false),
                ValueOrigin::NodeOutput {
                    node: unpivot,
                    output_ordinal: if matches!(shape, UnpivotShape::Permuted) {
                        0
                    } else {
                        2
                    },
                },
            )
            .unwrap()
    };

    let mapping_input = match shape {
        UnpivotShape::MappingOutsideChild => value_output,
        UnpivotShape::MappingTypeDrift => text,
        _ => number,
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: unpivot,
            inputs: Box::from([source]),
            required_inputs: Box::from([unconstrained()]),
            output_properties: singleton(),
            output: OutputPort {
                node: unpivot,
                columns: match shape {
                    UnpivotShape::Permuted | UnpivotShape::WrongOutputOrdinal => {
                        Box::from([literal_output, value_output, text])
                    }
                    UnpivotShape::MissingOutput => Box::from([text, value_output]),
                    UnpivotShape::ExtraOutput => {
                        Box::from([text, value_output, literal_output, number])
                    }
                    UnpivotShape::DuplicateOutput => Box::from([text, value_output, value_output]),
                    UnpivotShape::DuplicateReusedInput => Box::from([number, number, value_output]),
                    _ => Box::from([text, value_output, literal_output]),
                },
            },
            kind: NodeKind::Unpivot {
                spec: UnpivotSpec {
                    passthrough: if matches!(shape, UnpivotShape::DuplicateReusedInput) {
                        Box::from([(number, number)])
                    } else {
                        Box::from([(text, text)])
                    },
                    value_output,
                    literal_outputs: if matches!(shape, UnpivotShape::DuplicateReusedInput) {
                        Box::from([text])
                    } else {
                        Box::from([literal_output])
                    },
                    mappings: Box::from([UnpivotValueMapping {
                        input: mapping_input,
                        constants: Box::from([UnpivotConstant::Scalar(constant)]),
                    }]),
                    max_output_rows: 1024,
                    max_output_bytes: 1024 * 1024,
                },
            },
        })
        .unwrap();
    let fragment = builder.finish_fixture_definition(
        unpivot,
        FragmentSink::Noop,
        dop(),
        crate::StagePlacementPolicy::Singleton,
    )?;
    Ok((fragment, constants.into_pools()))
}

#[test]
fn ordinary_unpivot_accepts_one_closed_input_and_output_shape() {
    finish_unpivot(UnpivotShape::Valid).expect("the exact Unpivot contract must validate");
}

#[test]
fn ordinary_unpivot_rejects_a_mapping_input_outside_its_child_port() {
    let errors = finish_unpivot(UnpivotShape::MappingOutsideChild)
        .expect_err("an Unpivot mapping must consume its exact child output");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("mapping input is absent from its exact child output")
    }));
}

#[test]
fn ordinary_unpivot_rejects_mapping_type_drift() {
    let errors = finish_unpivot(UnpivotShape::MappingTypeDrift)
        .expect_err("all mapping inputs must share the value output type");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("mapping input type differs from its value output")
    }));
}

#[test]
fn ordinary_unpivot_rejects_literal_constant_type_drift() {
    let errors = finish_unpivot(UnpivotShape::ConstantTypeDrift)
        .expect_err("a constant must match its literal output type");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("constant type differs from its literal output")
    }));
}

#[test]
fn ordinary_unpivot_preserves_the_explicit_port_order_of_distinct_roles() {
    let (fragment, _constants) = finish_unpivot(UnpivotShape::Permuted)
        .expect("Unpivot's explicit port may order its exact roles independently");
    let root = fragment.nodes().get(&fragment.root()).unwrap();
    let NodeKind::Unpivot { spec } = &root.kind else {
        panic!("expected Unpivot")
    };
    assert_eq!(
        root.output.columns.as_ref(),
        &[
            spec.literal_outputs[0],
            spec.value_output,
            spec.passthrough[0].1
        ]
    );
}

#[test]
fn ordinary_unpivot_port_permutation_does_not_allow_missing_extra_or_duplicate_roles() {
    for shape in [
        UnpivotShape::MissingOutput,
        UnpivotShape::ExtraOutput,
        UnpivotShape::DuplicateOutput,
    ] {
        finish_unpivot(shape).expect_err("the port must cover each output role exactly once");
    }
}

#[test]
fn ordinary_unpivot_port_order_still_binds_produced_value_origin_ordinals() {
    let errors = finish_unpivot(UnpivotShape::WrongOutputOrdinal)
        .expect_err("permuting a port cannot preserve stale value-origin ordinals");
    assert!(
        errors
            .errors()
            .iter()
            .any(|error| error.message().contains("is not produced by this node"))
    );
}

#[test]
fn ordinary_unpivot_rejects_duplicate_reused_input_even_when_all_origins_are_valid() {
    let errors = finish_unpivot(UnpivotShape::DuplicateReusedInput)
        .expect_err("a repeated passthrough cannot substitute for a missing child-backed literal");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("output roles do not exactly cover the node output port")
    }));
}
