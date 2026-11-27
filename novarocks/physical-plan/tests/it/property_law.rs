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

use arrow_schema::DataType;
use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_physical_plan::*;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, PureCompileControl,
};
use std::{collections::BTreeMap, sync::Arc};

struct Control;

impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn value(builder: &mut FragmentBuilder, node: NodeId, ordinal: u32) -> ValueId {
    builder
        .add_value(
            ValueType::new(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: ordinal,
            },
        )
        .unwrap()
}

fn ordered_input() -> (FragmentBuilder, NodeId, ValueId, ValueId) {
    let mut builder = FragmentBuilder::new(FragmentId::new(1));
    let rows = builder.reserve_node_id().unwrap();
    let key = value(&mut builder, rows, 0);
    let measure = value(&mut builder, rows, 1);
    builder
        .add_values(rows, Box::default(), Box::new([key, measure]))
        .unwrap();
    let sort = builder.reserve_node_id().unwrap();
    let unit = builder.unit(sort).unwrap();
    let read = unit
        .add(
            None,
            ValueType::new(DataType::Int64, false),
            ExprKind::input(key),
        )
        .unwrap();
    let root = unit
        .root(read, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    builder
        .add_sort(
            sort,
            rows,
            Box::new([SortExpr {
                expr: root,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            }]),
            SortMode::Global,
        )
        .unwrap();
    let ports = builder.node_ports(sort).unwrap();
    let unit = builder.take_unit_builder(sort).unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let unit = unit
        .seal(ports, &mut work)
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    builder.install_unit(sort, Arc::new(unit)).unwrap();
    work.finish().unwrap();
    assert_eq!(
        builder.node_output_properties(sort).unwrap().ordering[0].value,
        key
    );
    (builder, sort, key, measure)
}

fn assert_output_order(builder: FragmentBuilder, node: NodeId, expected: &[OrderingKey]) {
    assert_eq!(
        builder
            .node_output_properties(node)
            .unwrap()
            .ordering
            .as_ref(),
        expected,
    );
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    validate_fragment_output_properties_observed(
        &fragment,
        PlanLimits::FROZEN,
        1 << 20,
        PropertyProofProjectionLimits {
            max_request_bytes: 1 << 20,
            max_coexisting_bytes: 8 << 20,
            max_projection_work: 1 << 20,
        },
        &Control,
    )
    .unwrap();
    let declared = &fragment.nodes()[&node].output_properties;
    assert_eq!(declared.ordering.as_ref(), expected);
    assert_eq!(declared.distribution, Distribution::Singleton);
    assert_eq!(declared.row_multiplicity, RowMultiplicity::SingleCopy);
}

#[test]
fn global_row_assertion_preserves_order() {
    let (mut builder, input, key, _) = ordered_input();
    let node = builder.reserve_node_id().unwrap();
    builder
        .add_assert_one_row(
            node,
            input,
            RowCountAssertionSpec::Global {
                subject: "scalar subquery".into(),
                desired_rows: 1,
                comparison: RowCountAssertion::Le,
            },
        )
        .unwrap();
    assert_output_order(
        builder,
        node,
        &[OrderingKey {
            value: key,
            direction: SortDirection::Ascending,
            null_ordering: NullOrdering::Last,
        }],
    );
}

#[test]
fn per_key_row_assertion_drops_input_ordering() {
    let (mut builder, input, key, _) = ordered_input();
    let node = builder.reserve_node_id().unwrap();
    builder
        .add_assert_one_row(
            node,
            input,
            RowCountAssertionSpec::PerKeyAtMostOne {
                declared_violation: None,
                applicability: AssertionApplicability::AllRows,
                keys: Box::new([key]),
                labels: Box::new(["key".into()]),
                message: "duplicate key".into(),
            },
        )
        .unwrap();
    assert_output_order(builder, node, &[]);
}

#[test]
fn repeat_drops_ordering_of_a_passthrough_key() {
    let (mut builder, input, key, measure) = ordered_input();
    let node = builder.reserve_node_id().unwrap();
    builder
        .add_repeat(
            node,
            input,
            Box::default(),
            Box::new([Box::default(), Box::default()]),
            Box::default(),
            Box::default(),
            Box::new([key, measure]),
        )
        .unwrap();
    assert_output_order(builder, node, &[]);
}

#[test]
fn unpivot_drops_ordering_of_a_passthrough_key() {
    let (mut builder, input, key, measure) = ordered_input();
    let node = builder.reserve_node_id().unwrap();
    let output = value(&mut builder, node, 1);
    builder
        .add_row_rewriting(
            node,
            input,
            Some(&BTreeMap::from([(key, key)])),
            Box::new([key, output]),
            NodeKind::Unpivot {
                spec: UnpivotSpec {
                    passthrough: Box::new([(key, key)]),
                    value_output: output,
                    literal_outputs: Box::default(),
                    mappings: Box::new([UnpivotValueMapping {
                        input: measure,
                        constants: Box::default(),
                    }]),
                    max_output_rows: 16,
                    max_output_bytes: 1024,
                },
            },
        )
        .unwrap();
    assert_output_order(builder, node, &[]);
}
