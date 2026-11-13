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
//! Real SQL ORDER source producer, original support before and exact after.
use super::numeric_unary_owned_transaction_tests::programs_with_catalogue;
use super::sql_test_source::sql_source_with_semantics;
use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    FunctionArgumentType, NodeKind, PhysicalCallDefinition, PhysicalCallSite,
    StaticFunctionArgument,
};

// Transitional re-export for the remaining Execution SQL tests.
// Shared inputs live in novarocks-sql-testkit; remove this bridge after migration.
pub(super) use novarocks_sql_testkit::installed_builtin_owner_catalogue;
fn check(sql: &str, group_concat: bool) {
    let semantics = novarocks_sql::sql_mode::SqlSemanticSettings::default();
    let semantics = if group_concat {
        semantics.with_group_concat_max_len(4096)
    } else {
        semantics
    };
    let source = sql_source_with_semantics(sql, DataType::Int8, semantics);
    let mut count = 0usize;
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for (ordinal, call) in calls.iter().enumerate() {
                let site = PhysicalCallSite::Aggregate {
                    node: node.id,
                    call: u32::try_from(ordinal).unwrap(),
                };
                let request = fragment
                    .call_requests()
                    .get(PhysicalCallDefinition::Relational(site))
                    .unwrap();
                let logical = request.logical_argument_count;
                assert_eq!(
                    request.arguments.len() - logical,
                    1,
                    "actual ordered call source {sql}"
                );
                let StaticFunctionArgument::Value {
                    value_type,
                    constant: None,
                } = &request.arguments[logical]
                else {
                    panic!("the real -k ORDER channel has no constant backing")
                };
                assert_eq!(value_type.data_type().clone(), DataType::Int8);
                assert!(
                    !value_type.nullable(),
                    "both modes retain required signed -k in their own ORDER request {sql} site {site:?}"
                );
                let FunctionArgumentType::Value(bound) =
                    &call.binding.function.argument_types[logical]
                else {
                    panic!("actual ORDER selected Value")
                };
                assert_eq!(
                    bound, value_type,
                    "selected ORDER uses its actual request {site:?}"
                );
                if call.binding.phase.consumes_logical_arguments() {
                    assert_eq!(call.order_by.len(), 1);
                    assert_eq!(
                        fragment
                            .expressions()
                            .get(call.order_by[0].expr)
                            .unwrap()
                            .ty,
                        value_type.clone()
                    );
                }
                count += 1;
            }
        }
    }
    assert!(
        count >= 1,
        "actual SQL must retain an actual ordered aggregate call"
    );
    {
        let actual = programs_with_catalogue(&source, &installed_builtin_owner_catalogue());
        assert_eq!(actual.len(), source.plan().fragments().len());
    }
}

#[test]
fn numeric_unary_exact_ordered_sql_array_agg() {
    check(
        "SELECT ARRAY_AGG(k ORDER BY -k) AS observed FROM fixture",
        false,
    );
}

#[test]
fn numeric_unary_exact_ordered_sql_group_concat() {
    check(
        "SELECT GROUP_CONCAT(CAST(k AS VARCHAR) ORDER BY -k) AS observed FROM fixture",
        true,
    );
}
