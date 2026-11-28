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

//! Exact typed original ORDER BY input on a single-stage aggregate.
use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    AggregatePhase, FunctionArgumentType, NodeKind, PhysicalCallSite, StaticFunctionArgument,
};
use novarocks_sql::compiler::{SqlCompileControl, ordered_array_source_for_test};

#[test]
fn numeric_unary_exact_typed_single_stage_order() {
    let control = SqlCompileControl::unbounded();
    let source = ordered_array_source_for_test(&control).unwrap();
    let (fragment, node, calls) = source
        .plan()
        .fragments()
        .values()
        .find_map(|fragment| {
            fragment.nodes().values().find_map(|node| match &node.kind {
                NodeKind::Aggregate { calls, .. } => Some((fragment, node, calls)),
                _ => None,
            })
        })
        .expect("single-stage aggregate");
    let [call] = calls.as_ref() else {
        panic!("expected one aggregate call");
    };
    assert_eq!(call.binding.phase, AggregatePhase::Single);
    assert!(call.binding.merge_state.is_none());
    let request = fragment
        .call_requests()
        .get(PhysicalCallSite::Aggregate {
            node: node.id,
            call: 0,
        })
        .unwrap();
    assert_eq!(request.logical_argument_count, 1);
    assert_eq!(request.arguments.len(), 2);
    let StaticFunctionArgument::Value {
        constant: None,
        value_type,
    } = &request.arguments[1]
    else {
        panic!("actual nonconstant ORDER BY channel");
    };
    assert_eq!(value_type.data_type(), &DataType::Int8);
    assert!(value_type.nullable());
    let FunctionArgumentType::Value(bound) = &call.binding.function.argument_types[1] else {
        panic!("bound ORDER BY type");
    };
    assert_eq!(bound, value_type);
    assert_eq!(call.order_by.len(), 1);
    let unit = fragment.unit(node.id).unwrap();
    let root = unit.root(call.order_by[0].expr).unwrap();
    assert_eq!(&unit.definition(root.definition).unwrap().ty, value_type);
}
