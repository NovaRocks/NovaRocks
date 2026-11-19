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

//! Real SQL boundaries for CONCAT, CONCAT_WS, and TRIM variants.

use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::FunctionValueType;

const CONCAT_NAMES: [&str; 2] = ["concat", "concat_ws"];
const TRIM_NAMES: [&str; 3] = ["trim", "ltrim", "rtrim"];

#[test]
fn c12_p2_concat_real_sql_calls_keep_ordered_child_type_correspondence_for_compiled() {
    use crate::exec::expr::sql_test_source::sql_source_with_declared_fields;
    use novarocks_expression_contract::ExprKind;
    use novarocks_physical_plan::FunctionArgumentType;
    use novarocks_physical_plan::StaticFunctionArgument;

    {
        for name in CONCAT_NAMES {
            let fields = ["sep", "a", "b", "c"].map(|n| Field::new(n, DataType::Utf8, true));
            let sql = if name == "concat" {
                "SELECT concat(a,b,c) FROM t"
            } else {
                "SELECT concat_ws(sep,a,b,c) FROM t"
            };
            let owner = sql_source_with_declared_fields(sql, &fields);
            let mut count = 0;
            for f in owner.plan().fragments().values() {
                for (_source_node, unit, id, node) in super::sql_test_source::unit_definitions(f) {
                    let ExprKind::FunctionCall { function, args } = &node.kind else {
                        continue;
                    };
                    if function.function_id.as_str() != format!("builtin.scalar/{name}/v1") {
                        continue;
                    }
                    let request = unit.requests().get(id).unwrap();
                    let arity = if name == "concat" { 3 } else { 4 };
                    assert_eq!(args.len(), arity);
                    assert_eq!(request.logical_argument_count, arity);
                    // This provider fixture has identical opaque column payloads.
                    // Check ordered child/request types here; distinct column value
                    // order is pinned by the literal compiled-slot expectations
                    // in the expression integration tests.
                    for (i, arg) in args.iter().enumerate() {
                        let StaticFunctionArgument::Value {
                            value_type,
                            constant,
                        } = &request.arguments[i]
                        else {
                            panic!("real value source")
                        };
                        assert!(constant.is_none());
                        let FunctionArgumentType::Value(selected) = &function.argument_types[i]
                        else {
                            panic!("selected value source")
                        };
                        assert_eq!(value_type, selected);
                        assert_eq!(selected, &FunctionValueType::new(DataType::Utf8, true));
                        let child = unit.definition(*arg).unwrap();
                        assert_eq!(&child.ty, selected);
                        assert!(!matches!(child.kind, ExprKind::Constant(_)));
                    }
                    assert_eq!(node.ty, FunctionValueType::new(DataType::Utf8, true));
                    println!(
                        "C12-P2 retained compiled/{name} definition={id:?} children={args:?} request={request:?}"
                    );
                    count += 1;
                }
            }
            assert!(count > 0, "real retained call required");
        }
    }
}

#[test]
fn c12_p1_trim_real_sql_column_calls_keep_ordered_requests_for_compiled() {
    use crate::exec::expr::sql_test_source::sql_source_with_single_field;
    use novarocks_expression_contract::ExprKind;
    use novarocks_physical_plan::FunctionArgumentType;
    use novarocks_physical_plan::StaticFunctionArgument;

    {
        for name in TRIM_NAMES {
            let owner = sql_source_with_single_field(
                &format!("SELECT {name}(k) FROM t"),
                Field::new("k", DataType::Utf8, true),
            );
            let mut count = 0;
            for f in owner.plan().fragments().values() {
                for (_source_node, unit, id, node) in super::sql_test_source::unit_definitions(f) {
                    let ExprKind::FunctionCall { function, args } = &node.kind else {
                        continue;
                    };
                    if function.function_id.as_str() != format!("builtin.scalar/{name}/v1") {
                        continue;
                    }
                    let request = unit.requests().get(id).unwrap();
                    assert_eq!(args.len(), 1);
                    assert_eq!(request.logical_argument_count, 1);
                    let StaticFunctionArgument::Value {
                        value_type,
                        constant,
                    } = &request.arguments[0]
                    else {
                        panic!("real value source")
                    };
                    assert!(constant.is_none());
                    let FunctionArgumentType::Value(selected) = &function.argument_types[0] else {
                        panic!("selected value source")
                    };
                    assert_eq!(value_type, selected);
                    assert_eq!(selected, &FunctionValueType::new(DataType::Utf8, true));
                    let child = unit.definition(args[0]).unwrap();
                    assert_eq!(&child.ty, selected);
                    assert!(
                        !matches!(child.kind, ExprKind::Constant(_)),
                        "column call must not become a constant"
                    );
                    assert_eq!(node.ty, FunctionValueType::new(DataType::Utf8, true));
                    println!(
                        "C12-P1 retained compiled/{name} definition={id:?} child={child:?} request={request:?}"
                    );
                    count += 1;
                }
            }
            assert!(count > 0, "real retained call required");
        }
    }
}
