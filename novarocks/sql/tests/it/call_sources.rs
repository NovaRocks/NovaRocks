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
mod left_right {
    //! The actual SQL case with no-op folding retains the original emitted call and request authors.
    //! Prior native PASS alone does not establish that a call survived real constant folding.
    use arrow::datatypes::DataType;
    use novarocks_physical_plan::{
        ExprKind, FunctionArgumentType, PhysicalCallDefinition, StaticFunctionArgument,
    };
    use novarocks_sql_testkit::sql_source;

    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    const SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/sql/correctness/project/sql/project_string_int_argument_semantics.sql"
    ));
    fn check() {
        let owner = sql_source(SQL, DataType::Int64);
        let mut count = 0;
        for fragment in owner.plan().fragments().values() {
            for (id, node) in fragment.expressions().iter() {
                let ExprKind::FunctionCall { function, args } = &node.kind else {
                    continue;
                };
                if !["builtin.scalar/left/v1", "builtin.scalar/right/v1"]
                    .contains(&function.function_id.as_str())
                {
                    continue;
                }
                let request = fragment
                    .call_requests()
                    .get(PhysicalCallDefinition::Expression(*id))
                    .unwrap();
                assert_eq!(args.len(), 2);
                assert_eq!(request.logical_argument_count, 2);
                assert_eq!(request.arguments.len(), 2);
                assert_eq!(function.argument_types.len(), 2);
                for (index, ((argument, selected), child)) in request
                    .arguments
                    .iter()
                    .zip(function.argument_types.iter())
                    .zip(args.iter())
                    .enumerate()
                {
                    let StaticFunctionArgument::Value { value_type, .. } = argument else {
                        panic!("real source value argument")
                    };
                    let FunctionArgumentType::Value(bound) = selected else {
                        panic!("real selected value argument")
                    };
                    assert_eq!(value_type, bound);
                    assert_eq!(bound.logical_type(), ValueLogicalType::Physical);
                    assert_eq!(
                        bound.data_type().clone(),
                        if index == 0 {
                            DataType::Utf8
                        } else {
                            DataType::Int64
                        }
                    );
                    assert_eq!(&fragment.expressions().get(*child).unwrap().ty, bound);
                }
                assert_eq!(node.ty, function.result_type);
                assert_eq!(node.ty, FunctionValueType::new(DataType::Utf8, true));
                eprintln!(
                    "left_right actual retained source mode=compiled fragment={:?} definition={id:?} signature={function:?} original_request={request:?} ordered_children={args:?}",
                    fragment.id()
                );
                count += 1;
            }
        }
        assert!(
            count == 4,
            "actual corpus SQL keeps a real function call with no-op fold"
        );
        eprintln!("left_right actual retained call count mode=compiled count={count}");
    }

    #[test]
    fn left_right_actual_candidate_sql_kept_call_source() {
        check()
    }
}
mod exact_percentile {
    //! Actual original SQL request/binding source, prior to owner correction.
    use arrow::datatypes::DataType;
    use novarocks_physical_plan::{
        FunctionArgumentType, NodeKind, PhysicalCallDefinition, PhysicalCallSite,
        StaticFunctionArgument,
    };
    use novarocks_sql_testkit::sql_source;

    use novarocks_type_contract::FunctionValueType;
    fn check() {
        for name in ["percentile_cont", "percentile_disc", "percentile_disc_lc"] {
            for rate in ["0", "1", "0.25", "0.5", "0.75", "CAST(0.5 AS DOUBLE)"] {
                let sql = format!("SELECT {name}(k,{rate}) AS observed FROM fixture");
                let source = sql_source(&sql, DataType::Int32);
                let mut count = 0;
                for fragment in source.plan().fragments().values() {
                    for node in fragment.nodes().values() {
                        let NodeKind::Aggregate { calls, .. } = &node.kind else {
                            continue;
                        };
                        for (ordinal, call) in calls.iter().enumerate() {
                            let site = PhysicalCallSite::Aggregate {
                                node: node.id,
                                call: ordinal as u32,
                            };
                            let request = fragment
                                .call_requests()
                                .get(PhysicalCallDefinition::Relational(site))
                                .unwrap();
                            assert_eq!(request.logical_argument_count, 2);
                            assert_eq!(call.binding.function.argument_types.len(), 2);
                            let StaticFunctionArgument::Value {
                                value_type: actual_rate,
                                ..
                            } = &request.arguments[1]
                            else {
                                panic!("original rate Value")
                            };
                            let FunctionArgumentType::Value(bound_rate) =
                                &call.binding.function.argument_types[1]
                            else {
                                panic!("selected rate Value")
                            };
                            assert_eq!(actual_rate, bound_rate);
                            // Record the actual authored type, rather than guessing
                            // it from ConstantValue or retagging a numeric literal.
                            eprintln!(
                                "original SQL rate source sql={sql:?} mode=compiled phase={:?} rate={actual_rate:?} state={:?} output={:?}",
                                call.binding.phase,
                                call.binding.intermediate_type,
                                call.binding.function.result_type
                            );
                            if !rate.starts_with("CAST") {
                                assert_ne!(actual_rate.data_type().clone(), DataType::Float64);
                            }
                            let FunctionArgumentType::Value(value) =
                                &call.binding.function.argument_types[0]
                            else {
                                panic!("value")
                            };
                            let mut expected = value.clone();
                            expected = expected
                                .with_nullable(true)
                                .expect("valid test root nullability");
                            assert_eq!(call.binding.function.result_type, expected);
                            assert_eq!(
                                call.binding.intermediate_type,
                                FunctionValueType::new(DataType::Binary, true)
                            );
                            count += 1;
                        }
                    }
                }
                assert!(count > 0, "actual aggregate source remains");
            }
        }
    }

    #[test]
    fn exact_percentile_candidate_sql_rate_source() {
        check();
    }
}
mod canonical_calls {
    use arrow::datatypes::DataType;
    use novarocks_sql::compiler::{SqlCompileControl, check_pure_call_definitions_observed};
    use novarocks_sql_testkit::sql_source;
    #[test]
    fn e08s1_late_actual_source_same_journal_supported_canonical_calls_and_over_are_accepted() {
        {
            let source = sql_source(
                "SELECT reverse(CAST(k AS VARCHAR)), count(k) OVER () FROM fixture",
                DataType::Int32,
            );
            check_pure_call_definitions_observed(&source, &SqlCompileControl::unbounded()).unwrap();
        }
    }
}
