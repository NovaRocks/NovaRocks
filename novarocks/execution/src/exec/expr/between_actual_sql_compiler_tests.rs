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
fn between_count(source: &SqlAuthoredPhysicalPlan) -> usize {
    source
        .plan()
        .fragments()
        .values()
        .flat_map(super::sql_test_source::unit_definitions)
        .filter(|(_, _, _, e)| matches!(e.kind, ExprKind::Between { .. }))
        .count()
}
#[test]
fn between_sql_source_original_required_native_case_retains_actual_bound_decimal_domains() {
    let source = sql_source(REQUIRED, DataType::Int64);
    assert_eq!(between_count(&source), 1);
    for fragment in source.plan().fragments().values() {
        for (_source_node, unit, _, node) in super::sql_test_source::unit_definitions(fragment) {
            if let ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } = &node.kind
            {
                assert!(!negated);
                assert_eq!(node.ty.data_type().clone(), DataType::Boolean);
                let operand = &unit.definition(*expr).unwrap().ty;
                assert!(matches!(operand.data_type(), DataType::Decimal128(..)));
                for child in [low, high] {
                    assert!(operand.same_value_domain(&unit.definition(*child).unwrap().ty));
                }
                assert_eq!(
                    node.ty.nullable(),
                    operand.nullable()
                        || unit.definition(*low).unwrap().ty.nullable()
                        || unit.definition(*high).unwrap().ty.nullable()
                );
            }
        }
    }
}
