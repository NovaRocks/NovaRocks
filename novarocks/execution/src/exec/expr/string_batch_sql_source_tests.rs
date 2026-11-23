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
//! Actual string corpus emission and authenticated SQL dependency journal loans.
use super::sql_test_source::sql_source;
use arrow::datatypes::DataType;
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{
    FunctionArgumentType, FunctionResultType, PureCallLifecycle, ResolvedFunctionBinding,
};
use novarocks_sql::compiler::{
    SqlCallDependencySite, SqlCompileControl, builtin_sql_function_catalog,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
const LENGTH_SQL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/project/sql/project_string_length_limit_semantics.sql"
));
const INTEGER_SQL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/project/sql/project_string_int_argument_semantics.sql"
));
// These explicit probes exercise the code-point conversions and the fixed
// character and binary LENGTH overloads.
const REMAINING_MEASUREMENT: &str = "SELECT codepoint('A') AS a, chr(BIGINT '65') AS letter, \
    length(CAST('é' AS CHAR(3))) AS char_count, length(X'C3A9') AS byte_count";
pub(super) const NAMES: [&str; 5] = ["chr", "codepoint", "length", "lpad", "rpad"];

pub(super) fn first_corpus_query(sql: &str) -> &str {
    let frames = novarocks_parser::parser::frame_statements(sql).expect("frame string SQL corpus");
    let first = frames.first().expect("positive first string query").source;
    &sql[first.start()..first.end()]
}

pub(super) fn calls(index: usize) -> Vec<(String, ResolvedFunctionBinding)> {
    let sql = [LENGTH_SQL, INTEGER_SQL, REMAINING_MEASUREMENT][index];
    let owner = sql_source(first_corpus_query(sql), DataType::Int64);
    let control = SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut out = Vec::new();
    for fragment in owner.plan().fragments().values() {
        for (_source_node, _unit, _, source) in super::sql_test_source::unit_definitions(fragment) {
            let ExprKind::FunctionCall { function, .. } = &source.kind else {
                continue;
            };
            // Source selection is test-only. Production admission borrows the installed owner.
            let Some(name) = NAMES
                .iter()
                .find(|name| function.function_id.as_str() == format!("builtin.scalar/{name}/v1"))
            else {
                continue;
            };
            let loan = owner
                .borrow_call_dependency_observed(
                    SqlCallDependencySite::Expression {
                        fragment,
                        node: _source_node,
                        source,
                    },
                    &mut work,
                )
                .unwrap();
            let binding = loan.original_binding().resolved();
            assert_eq!(
                binding.logical_argument_count,
                binding.selected.argument_types.len()
            );
            assert!(
                binding
                    .selected
                    .argument_types
                    .iter()
                    .all(|a| matches!(a, FunctionArgumentType::Value(_)))
            );
            assert!(matches!(
                binding.selected.result_type,
                FunctionResultType::Scalar(_)
            ));
            eprintln!(
                "string static original source case={index} mode=compiled fragment={:?} source={:?} binding={binding:?}",
                fragment.id(),
                source.id
            );
            out.push(((*name).to_string(), binding.clone()));
        }
    }
    work.finish().unwrap();
    assert!(
        !out.is_empty(),
        "actual original emitted selected calls are required"
    );
    out
}
fn assert_metadata_profile(index: usize) {
    let catalog = builtin_sql_function_catalog().snapshot();
    let control = SqlCompileControl::unbounded();
    for (_, binding) in calls(index) {
        catalog
            .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control)
            .unwrap();
        let mut stale = binding.clone();
        let FunctionResultType::Scalar(target) = &mut stale.selected.result_type else {
            unreachable!()
        };
        *target =
            novarocks_type_contract::FunctionValueType::new(DataType::Binary, target.nullable());
        catalog
            .admit_bound_lifecycle_observed(&stale, PureCallLifecycle::Scalar, &control)
            .unwrap();
    }
}
#[test]
fn e08s1_string_batch_original_length_caps_source_keeps_original_loans_without_data() {
    assert_metadata_profile(0)
}
#[test]
fn e08s1_string_batch_original_integer_arguments_and_measurement_members_keep_original_loans() {
    assert_metadata_profile(1);
    assert_metadata_profile(2)
}
