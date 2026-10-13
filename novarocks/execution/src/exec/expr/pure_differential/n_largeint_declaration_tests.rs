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
//! The complete declaration oracle is independent of the raw16 legacy ABI.
use super::HarnessControl;
use arrow::datatypes::DataType;
use novarocks_functions::builtin::catalogue::builtin_engine_function_catalog;
use novarocks_functions::{
    AggregateKernelPhase, FunctionArgument, FunctionArgumentType, FunctionBindingRequest,
    FunctionKind, FunctionResultType,
};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn value_type(value: &FunctionValueType) -> Value {
    let carrier = match &value.data_type {
        DataType::List(item) => json!({
            "kind": "List",
            "item": {
                "name": item.name(),
                "carrier": format!("{:?}", item.data_type()),
                "nullable": item.is_nullable(),
                "metadata": item.metadata().iter().collect::<BTreeMap<_, _>>(),
            },
        }),
        other => json!({"kind": format!("{other:?}")}),
    };
    json!({"carrier": carrier, "logical": format!("{:?}",value.logical_type), "nullable": value.nullable})
}

fn declarations() -> Value {
    let catalog = builtin_engine_function_catalog();
    let mut profiles = Vec::new();
    for name in ["min_n", "max_n"] {
        for value_nullable in [false, true] {
            for limit_carrier in [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
            ] {
                for limit_nullable in [false, true] {
                    let value = FunctionValueType::try_with_logical_type(
                        DataType::FixedSizeBinary(16),
                        value_nullable,
                        ValueLogicalType::LargeInt,
                    )
                    .unwrap();
                    let limit = FunctionValueType::new(limit_carrier.clone(), limit_nullable);
                    let arguments = [
                        FunctionArgument::Value {
                            value_type: value,
                            constant: None,
                        },
                        FunctionArgument::Value {
                            value_type: limit,
                            constant: None,
                        },
                    ];
                    let bound = catalog
                        .resolve_bound_user(
                            name,
                            FunctionKind::Aggregate,
                            FunctionBindingRequest {
                                arguments: &arguments,
                                logical_argument_count: 2,
                                expected_result_type: None,
                            },
                            &HarnessControl,
                        )
                        .unwrap();
                    let selected = &bound.selected;
                    let inputs = selected
                        .argument_types
                        .iter()
                        .map(|argument| match argument {
                            FunctionArgumentType::Value(value) => value_type(value),
                            _ => panic!("N declaration must have value arguments"),
                        })
                        .collect::<Vec<_>>();
                    let FunctionResultType::Scalar(result) = &selected.result_type else {
                        panic!("N declaration must have a scalar result")
                    };
                    let state = selected.aggregate.as_ref().unwrap();
                    let declaration = catalog
                        .pure_overload_declaration_observed(
                            &bound.function_id,
                            FunctionKind::Aggregate,
                            &selected.overload,
                            &HarnessControl,
                        )
                        .unwrap();
                    let phases = [
                        AggregateKernelPhase::Single,
                        AggregateKernelPhase::Partial,
                        AggregateKernelPhase::Intermediate,
                        AggregateKernelPhase::Final,
                    ]
                    .map(|phase| {
                        json!({
                            "phase": format!("{phase:?}"),
                            "logical_arguments": phase.consumes_logical_arguments(),
                            "final_result": phase.produces_final_result(),
                        })
                    });
                    profiles.push(json!({
                        "name": name,
                        "function": bound.function_id.as_str(),
                        "overload": selected.overload.as_str(),
                        "arguments": inputs,
                        "result": value_type(result),
                        "intermediate": value_type(&state.intermediate_type),
                        "state_format": state.state_format.as_str(),
                        "state_arguments": format!("{:?}",state.state_argument_contract),
                        "implementation": declaration.implementation().implementation.as_str(),
                        "abi": format!("{:?}", declaration.implementation().abi),
                        "effects": format!("{:?}",declaration.effects()),
                        "phases": phases,
                    }));
                }
            }
        }
    }
    assert_eq!(profiles.len(), 32);
    json!({"schema": 1, "source_commit": "9b13107a266db258cb2807bae22ea188e46ccd2b", "profiles": profiles})
}

#[test]
fn n_largeint_complete_declarations_match_preinstallation_oracle() {
    let original: Value = serde_json::from_str(include_str!("n_largeint_declaration_oracle.json"))
        .expect("valid preinstallation declaration oracle");
    assert_eq!(declarations(), original);
}
