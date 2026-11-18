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
//! Public SQL completion inputs for owner and compile-to-execute tests.
use arrow::datatypes::{DataType, Field};
use std::sync::Arc;
pub(super) use novarocks_sql_testkit::approx_sql_source;
fn check_actual_approx() {
    use novarocks_physical_plan::{
        FunctionArgumentType, NodeKind, PhysicalCallSite, StaticFunctionArgument,
    };
    use novarocks_type_contract::FunctionValueType;
    let statements = [
        "SELECT CAST(percentile_approx(v, 0.5) AS INT) AS approx50, CAST(percentile_approx(v, 0.9, 2048) AS INT) AS approx90, percentile_approx(v, array<double>[0.25, 0.5, 0.75], 2048) AS approx_quartiles FROM fixture.t_agg_percentile_semantics",
        "SELECT CAST(percentile_approx_weighted(v, w, 0.5, 2048) AS INT) AS weighted50, CAST(percentile_approx_weighted(v, w, 0.9, 2048) AS INT) AS weighted90, percentile_approx_weighted(v, w, array<double>[0.25, 0.5, 0.75], 2048) AS weighted_quartiles FROM fixture.t_agg_percentile_semantics",
    ];
    for sql in statements {
        let source = approx_sql_source(sql);
        let mut seen = 0;
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                let NodeKind::Aggregate { calls, .. } = &node.kind else {
                    continue;
                };
                for (ordinal, call) in calls.iter().enumerate() {
                    let name = call.binding.function.function_id.as_str();
                    if !matches!(
                        name,
                        "builtin.aggregate/percentile_approx/v1"
                            | "builtin.aggregate/percentile_approx_weighted/v1"
                    ) {
                        continue;
                    }
                    let site = PhysicalCallSite::Aggregate {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    };
                    let request = fragment.call_requests().get(site).unwrap();
                    assert_eq!(
                        request.logical_argument_count,
                        call.binding.function.argument_types.len()
                    );
                    for (actual, selected) in request
                        .arguments
                        .iter()
                        .zip(call.binding.function.argument_types.iter())
                    {
                        let (
                            StaticFunctionArgument::Value {
                                value_type: actual, ..
                            },
                            FunctionArgumentType::Value(selected),
                        ) = (actual, selected)
                        else {
                            panic!("actual Value channel")
                        };
                        assert_eq!(actual, selected);
                    }
                    let role = if name == "builtin.aggregate/percentile_approx/v1" {
                        1
                    } else {
                        2
                    };
                    let FunctionArgumentType::Value(rate) =
                        &call.binding.function.argument_types[role]
                    else {
                        panic!("actual rate")
                    };
                    let expected = if matches!(rate.data_type(), DataType::List(_)) {
                        DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
                    } else {
                        DataType::Float64
                    };
                    assert_eq!(
                        call.binding.function.result_type,
                        FunctionValueType::new(expected, true)
                    );
                    assert_eq!(
                        call.binding.intermediate_type,
                        FunctionValueType::new(DataType::Binary, true)
                    );
                    assert!(
                        matches!(
                            rate.data_type(),
                            DataType::Decimal128(..) | DataType::List(_)
                        ),
                        "native statements original authored rates"
                    );
                    eprintln!(
                        "approximate actual SQL mode=compiled source={site:?} phase={:?} logical={:?} rate={rate:?} output={:?} state={:?}",
                        call.binding.phase,
                        request.arguments,
                        call.binding.function.result_type,
                        call.binding.intermediate_type
                    );
                    seen += 1;
                }
            }
        }
        assert!(
            seen >= 3,
            "all original required three quantile projections"
        );
    }
}

#[test]
fn approximate_percentile_actual_candidate_sql_source() {
    check_actual_approx()
}
