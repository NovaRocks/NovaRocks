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

//! Mixed independent source: root promotion must never retag an OWN CV.
use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    FunctionArgumentType, NodeKind, PhysicalCallSite, StaticFunctionArgument,
};
use novarocks_sql::compiler::{
    SqlCompileControl, ordinary_extrema_constant_state_source_for_test,
    ordinary_state_observe_for_test,
};
fn check(maximum: bool) {
    let control = SqlCompileControl::unbounded();
    let owner = ordinary_extrema_constant_state_source_for_test(maximum, &control).unwrap();
    let observed = ordinary_state_observe_for_test(&owner, &control).unwrap();
    assert_eq!(
        (observed.partials, observed.intermediates, observed.finals),
        (2, 1, 1)
    );
    assert_eq!(observed.final_consumer_constant, Some(3));
    assert_eq!(
        observed.final_producer_constants,
        vec![Some(3), None, Some(2)]
    );
    assert_eq!(observed.final_independent_lineages, 3);
    // The actual nonconstant contribution retains Int64 after the outer cast.
    // Constants retain their OWN complete nonnullable Int64 receipt in both modes.
    let mut nullable_producers = 0;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for (ordinal, call) in calls.iter().enumerate() {
                let request = fragment
                    .call_requests()
                    .get(PhysicalCallSite::Aggregate {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    })
                    .unwrap();
                let StaticFunctionArgument::Value {
                    value_type,
                    constant,
                } = &request.arguments[0]
                else {
                    panic!("actual MIN/MAX Value source")
                };
                assert_eq!(value_type.data_type().clone(), DataType::Int64);
                if constant.is_some() {
                    assert!(!value_type.nullable(), "OWN CV retains exact original FVT");
                } else {
                    assert!(call.binding.phase.consumes_logical_arguments());
                    assert!(
                        value_type.nullable(),
                        "both modes retain successful Int8 NEGATE NULL through the Int64 cast"
                    );
                    assert_eq!(call.arguments.len(), 1);
                    assert_eq!(
                        {
                            let unit = fragment.unit(node.id).unwrap();
                            &unit
                                .definition(unit.root(call.arguments[0]).unwrap().definition)
                                .unwrap()
                                .ty
                        },
                        value_type,
                        "the request retains the completed producer's full FVT"
                    );
                    let FunctionArgumentType::Value(bound) =
                        &call.binding.function.argument_types[0]
                    else {
                        panic!("actual MIN/MAX selected Value")
                    };
                    assert_eq!(bound, value_type, "selected producer uses its own request");
                    nullable_producers += 1;
                }
            }
        }
    }
    assert_eq!(nullable_producers, 1);
}

#[test]
fn numeric_unary_exact_state_min_own_constant_with_nullable_producer() {
    check(false);
}

#[test]
fn numeric_unary_exact_state_max_own_constant_with_nullable_producer() {
    check(true);
}
