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

//! Actual original corpus table facts; permanent complete lowering contract.
use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    FunctionArgumentType, NodeKind, PhysicalCallDefinition, PhysicalCallSite,
    StaticFunctionArgument,
};
fn sources() -> Vec<novarocks_sql::compiler::SqlAuthoredPhysicalPlan> {
    vec![
        super::hll_hash_actual_sql_source_tests::hll_sql_source(
            "WITH SESSION streaming_preaggregation_mode = 'force_preaggregation' SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_preaggregation') */ grp, CAST(avg(v) AS DECIMAL(18,4)) AS avg_v, ndv(k) AS ndv_k, hll_union_agg(hll_hash(k)) AS hll_k, CAST(percentile_approx(d,0.5) AS DECIMAL(18,4)) AS p50_d FROM fixture.agg_state_typedesc_contract GROUP BY grp ORDER BY grp",
            &[
                ("grp", DataType::Int32),
                ("k", DataType::Int32),
                ("v", DataType::Int64),
                ("d", DataType::Float64),
            ],
        ),
        super::hll_hash_actual_sql_source_tests::hll_sql_source(
            "select hll_union(hll_hash(c1)) from fixture.t1",
            &[("c1", DataType::Int32), ("c2", DataType::Int32)],
        ),
    ]
}
#[test]
fn hll_payload_aggregate_actual_original_required_sql_full_signature_source() {
    for source in sources() {
        let mut count = 0;
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                let NodeKind::Aggregate { calls, .. } = &node.kind else {
                    continue;
                };
                for (ordinal, call) in calls.iter().enumerate() {
                    let output = match call.binding.function.function_id.as_str() {
                        "builtin.aggregate/hll_union/v1" | "builtin.aggregate/hll_raw_agg/v1" => {
                            DataType::Binary
                        }
                        "builtin.aggregate/hll_union_agg/v1" => DataType::Int64,
                        _ => continue,
                    };
                    let definition =
                        PhysicalCallDefinition::Relational(PhysicalCallSite::Aggregate {
                            node: node.id,
                            call: u32::try_from(ordinal).unwrap(),
                        });
                    let request = fragment.call_requests().get(definition).unwrap();
                    assert_eq!(request.logical_argument_count, 1);
                    assert_eq!(request.arguments.len(), 1);
                    let FunctionArgumentType::Value(selected) =
                        &call.binding.function.argument_types[0]
                    else {
                        panic!("original selected payload")
                    };
                    let StaticFunctionArgument::Value { value_type, .. } = &request.arguments[0]
                    else {
                        panic!("actual original source payload")
                    };
                    assert_eq!(selected, value_type);
                    assert_eq!(selected.data_type().clone(), DataType::Binary);
                    assert!(selected.nullable());
                    assert_eq!(
                        call.binding.intermediate_type.data_type().clone(),
                        DataType::Binary
                    );
                    assert_eq!(
                        call.binding.function.result_type.data_type().clone(),
                        output
                    );
                    assert!(call.binding.function.result_type.nullable());
                    eprintln!(
                        "HLL_PAYLOAD actual source fragment={:?} definition={definition:?} binding={:?} request={request:?}",
                        fragment.id(),
                        call.binding
                    );
                    count += 1;
                }
            }
        }
        assert!(
            count > 0,
            "actual original required HLL payload aggregate remains present"
        );
    }
}
#[test]
fn hll_payload_aggregate_actual_original_required_sql_complete_compilation() {
    for source in sources() {
        let results = super::filter_conjunction_actual_sql_compiler_tests::compiler_results(
            &source,
            &super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue(),
        );
        assert!(!results.is_empty());
        for (fragment, program) in results {
            program.unwrap_or_else(|error| {
                panic!("complete actual HLL payload source fragment {fragment:?}: {error:?}")
            });
        }
    }
}
