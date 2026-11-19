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
//! The actual SQL case with folding disabled retains the original emitted call and request authors.
//! Prior native PASS alone does not establish that a call survived real constant folding.
use super::sql_test_source::sql_source;
use arrow::datatypes::DataType;
use novarocks_expression_contract::ExprKind;
use novarocks_physical_plan::FunctionArgumentType;
use novarocks_physical_plan::StaticFunctionArgument;

use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
const SQL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/project/sql/project_string_int_argument_semantics.sql"
));
fn check() {
    let owner = sql_source(SQL, DataType::Int64);
    let mut count = 0;
    for fragment in owner.plan().fragments().values() {
        for (_source_node, unit, id, node) in super::sql_test_source::unit_definitions(fragment) {
            let ExprKind::FunctionCall { function, args } = &node.kind else {
                continue;
            };
            if function.function_id.as_str() != "builtin.scalar/split_part/v1" {
                continue;
            }
            let request = unit.requests().get(id).unwrap();
            assert_eq!(args.len(), 3);
            assert_eq!(request.logical_argument_count, 3);
            assert_eq!(request.arguments.len(), 3);
            assert_eq!(function.argument_types.len(), 3);
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
                    if index < 2 {
                        DataType::Utf8
                    } else {
                        DataType::Int32
                    }
                );
                assert_eq!(&unit.definition(*child).unwrap().ty, bound);
            }
            assert_eq!(node.ty, function.result_type);
            assert_eq!(node.ty, FunctionValueType::new(DataType::Utf8, true));
            eprintln!(
                "split_part actual retained source mode=compiled fragment={:?} definition={id:?} signature={function:?} original_request={request:?} ordered_children={args:?}",
                fragment.id()
            );
            count += 1;
        }
    }
    assert!(
        count == 4,
        "actual corpus SQL keeps a real function call with folding disabled"
    );
    eprintln!("split_part actual retained call count mode=compiled count={count}");
}

#[test]
fn split_part_actual_candidate_sql_kept_call_source() {
    check()
}
