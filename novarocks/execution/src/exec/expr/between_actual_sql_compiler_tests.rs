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

//! Actual SQL/package/Frame witnesses with independent interval truth tables.
use super::sql_test_source::sql_source;
use arrow::datatypes::DataType;
use novarocks_expression_contract::ExprKind;
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;

const REQUIRED: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/filter/sql/filter_decimal_binary_pred_mixed_precision.sql"
));
#[test]
fn between_sql_source_original_required_native_case_retains_actual_bound_decimal_domains() {
    let source: SqlAuthoredPhysicalPlan = sql_source(REQUIRED, DataType::Int64);
    let mut count = 0;
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let Some(unit) = node.expressions.as_deref() else {
                continue;
            };
            for expansion in unit.control_expansions().records() {
                count += 1;
                assert_eq!(expansion.root, expansion.positive);
                assert_eq!(
                    unit.definition(expansion.root).unwrap().ty.data_type(),
                    &DataType::Boolean
                );
                for branch in expansion.branches {
                    let ExprKind::FunctionCall { function, args } =
                        &unit.definition(branch).unwrap().kind
                    else {
                        panic!("ordinary ordering call")
                    };
                    assert_eq!(
                        function.function_id.as_str(),
                        novarocks_type_contract::OperatorFunction::LessThanOrEqual
                            .function_id_text()
                    );
                    let [left, right] = args.as_ref() else {
                        panic!("one binary comparison")
                    };
                    let left = &unit.definition(*left).unwrap().ty;
                    let right = &unit.definition(*right).unwrap().ty;
                    assert!(matches!(left.data_type(), DataType::Decimal128(..)));
                    assert!(left.same_value_domain(right));
                }
            }
        }
    }
    assert_eq!(count, 1);
}
