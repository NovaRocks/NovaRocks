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

//! Actual corpus nullable Int32 source and permanent complete-package contract.
use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    FunctionArgumentType, NodeKind, PhysicalCallSite, StaticFunctionArgument,
};
fn source() -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    super::hll_hash_actual_sql_source_tests::hll_sql_source(
        "select bitmap_union_int(c2) from fixture.t1",
        &[("c1", DataType::Int32), ("c2", DataType::Int32)],
    )
}
#[test]
fn bitmap_union_int_actual_original_required_sql_full_signature_source() {
    let source = source();
    let mut count = 0;
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for (ordinal, call) in calls.iter().enumerate() {
                if call.binding.function.function_id.as_str()
                    != "builtin.aggregate/bitmap_union_int/v1"
                {
                    continue;
                }
                let definition = PhysicalCallSite::Aggregate {
                    node: node.id,
                    call: u32::try_from(ordinal).unwrap(),
                };
                let request = fragment.call_requests().get(definition).unwrap();
                assert_eq!(request.logical_argument_count, 1);
                assert_eq!(request.arguments.len(), 1);
                assert_eq!(call.binding.function.argument_types.len(), 1);
                let StaticFunctionArgument::Value { value_type, .. } = &request.arguments[0] else {
                    panic!("original scalar argument")
                };
                let FunctionArgumentType::Value(selected) =
                    &call.binding.function.argument_types[0]
                else {
                    panic!("original selected argument")
                };
                assert_eq!(selected, value_type);
                assert_eq!(value_type.data_type().clone(), DataType::Int32);
                assert!(value_type.nullable());
                assert_eq!(
                    call.binding.intermediate_type.data_type().clone(),
                    DataType::Binary
                );
                assert_eq!(
                    call.binding.function.result_type.data_type().clone(),
                    DataType::Int64
                );
                assert!(call.binding.function.result_type.nullable());
                eprintln!(
                    "BITMAP_UNION_INT actual source fragment={:?} definition={definition:?} binding={:?} request={request:?}",
                    fragment.id(),
                    call.binding
                );
                count += 1;
            }
        }
    }
    assert!(count > 0, "actual aggregate root must remain present");
}
#[test]
fn bitmap_union_int_actual_original_required_sql_complete_compilation() {
    let source = source();
    let results = super::filter_conjunction_actual_sql_compiler_tests::compiler_results(
        &source,
        &super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue(),
    );
    assert!(!results.is_empty());
    for (fragment, program) in results {
        program.unwrap_or_else(|error| {
            panic!("complete actual bitmap_union_int source fragment {fragment:?}: {error:?}")
        });
    }
}
