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

#[test]
fn exchange_source_rejects_reordered_output_occurrences() {
    let mut builder = FragmentBuilder::new(FragmentId::new(711));
    let node = builder.reserve_node_id().unwrap();
    let edge = EdgeId::new(711);
    let first_source = ValueId::new(10_711);
    let second_source = ValueId::new(20_711);
    let first = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport {
                edge,
                source_value: first_source,
            },
        )
        .unwrap();
    let second = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport {
                edge,
                source_value: second_source,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node,
                columns: Box::from([second, first]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(first_source, first), (second_source, second)]),
            },
        })
        .unwrap();

    let errors = builder
        .finish_fixture_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .expect_err("exchange output positions must preserve the import sequence");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("exchange output occurrences differ from its exact import sequence")
    }));
}
