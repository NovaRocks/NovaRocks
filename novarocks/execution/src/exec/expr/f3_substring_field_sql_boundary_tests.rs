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

//! Real SQL argument binding for SUBSTRING_INDEX.

use super::sql_test_source::sql_source_with_declared_fields;
use arrow::datatypes::{DataType, Field};
use novarocks_expression_contract::ExprKind;
use novarocks_functions::FunctionArgumentType;
use novarocks_physical_plan::StaticFunctionArgument;
use novarocks_type_contract::FunctionValueType;

fn substring_fields() -> Vec<Field> {
    [
        ("txt", DataType::Utf8),
        ("delim", DataType::Utf8),
        ("cnt", DataType::Int32),
    ]
    .into_iter()
    .map(|(n, t)| Field::new(n, t, true))
    .collect()
}

#[test]
fn c12_p3_substring_index_actual_sql_keeps_ordered_three_child_types_for_compiled() {
    let fields = substring_fields();
    let source =
        sql_source_with_declared_fields("SELECT substring_index(txt,delim,cnt) FROM t", &fields);
    let mut found = 0;
    for fragment in source.plan().fragments().values() {
        for (_source_node, unit, id, node) in super::sql_test_source::unit_definitions(fragment) {
            let ExprKind::FunctionCall { function, args } = &node.kind else {
                continue;
            };
            if function.function_id.as_str() != "builtin.scalar/substring_index/v1" {
                continue;
            }
            let request = unit.requests().get(id).unwrap();
            assert_eq!(args.len(), 3);
            assert_eq!(request.logical_argument_count, 3);
            assert_eq!(request.arguments.len(), 3);
            assert_eq!(function.argument_types.len(), 3);
            for (i, arg) in args.iter().enumerate() {
                let StaticFunctionArgument::Value {
                    value_type,
                    constant,
                } = &request.arguments[i]
                else {
                    panic!("actual value channel")
                };
                let FunctionArgumentType::Value(selected) = &function.argument_types[i] else {
                    panic!("actual selected channel")
                };
                assert!(constant.is_none());
                assert_eq!(value_type, selected);
                assert_eq!(
                    selected,
                    &FunctionValueType::new(fields[i].data_type().clone(), true)
                );
                let child = unit.definition(*arg).unwrap();
                assert_eq!(&child.ty, selected);
                assert!(!matches!(child.kind, ExprKind::Constant(_)));
            }
            assert_eq!(node.ty, FunctionValueType::new(DataType::Utf8, true));
            found += 1;
        }
    }
    assert!(found > 0);
}
