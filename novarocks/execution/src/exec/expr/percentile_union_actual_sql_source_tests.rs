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

//! Borrow the already published complete required SQL sources; no second plan author.
use super::*;
use novarocks_physical_plan::{NodeKind, PhysicalCallSite};
#[test]
fn percentile_union_actual_original_required_sql_binding_and_state_source() {
    let mut total = 0;
    for source in sources() {
        let mut seen = 0;
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                let NodeKind::Aggregate { calls, .. } = &node.kind else {
                    continue;
                };
                for (ordinal, call) in calls.iter().enumerate() {
                    if call.binding.function.function_id.as_str()
                        != "builtin.aggregate/percentile_union/v1"
                    {
                        continue;
                    }
                    let request = fragment
                        .call_requests()
                        .get(PhysicalCallSite::Aggregate {
                            node: node.id,
                            call: ordinal as u32,
                        })
                        .unwrap();
                    assert_eq!(request.logical_argument_count, 1);
                    assert_eq!(call.binding.logical_argument_count, 1);
                    assert_eq!(call.binding.function.argument_types.len(), 1);
                    assert_eq!(
                        call.binding.function.result_type.data_type().clone(),
                        DataType::Binary
                    );
                    assert_eq!(
                        call.binding.intermediate_type.data_type().clone(),
                        DataType::Binary
                    );
                    let FunctionArgumentType::Value(selected) =
                        &call.binding.function.argument_types[0]
                    else {
                        panic!("actual value")
                    };
                    assert_eq!(selected.data_type().clone(), DataType::Binary);
                    eprintln!(
                        "PERCENTILE_UNION actual phase={:?} fullbinding={:?} originalrequest={:?} expressions={:?}",
                        call.binding.phase, call.binding, request, call.arguments
                    );
                    seen += 1;
                }
            }
        }
        assert!(seen > 0);
        total += seen;
    }
    assert!(total >= 2);
}
#[test]
fn percentile_union_actual_original_required_sql_compiler_closure() {
    for source in sources() {
        for (fragment,program) in super::super::filter_conjunction_actual_sql_compiler_tests::compiler_results(&source,&super::super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue()) {
        program.unwrap_or_else(|error|panic!("complete percentile UNION required fragment {fragment:?}: {error:?}"));
    }
    }
}
