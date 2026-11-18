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
//! Actual corpus call binding and authenticated SQL dependency journal loans.
use super::sql_test_source::sql_source;
use arrow::datatypes::DataType;
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{
    FunctionArgumentType, FunctionResultType, PureCallLifecycle, ResolvedFunctionBinding,
};
use novarocks_sql::compiler::{
    SqlCallDependencySite, SqlCompileControl, builtin_sql_function_catalog,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, ValueLogicalType};
const SQL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/project/sql/project_append_trailing_char_if_absent_semantics.sql"
));

pub(super) fn bindings() -> Vec<ResolvedFunctionBinding> {
    let owner = sql_source(SQL, DataType::Int64);
    let control = SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut bindings = Vec::new();
    for fragment in owner.plan().fragments().values() {
        for (_source_node, _unit, _, source) in super::sql_test_source::unit_definitions(fragment) {
            let ExprKind::FunctionCall { function, .. } = &source.kind else {
                continue;
            };
            if function.function_id.as_str() != "builtin.scalar/append_trailing_char_if_absent/v1" {
                continue;
            }
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
            assert_eq!(binding.logical_argument_count, 2);
            assert_eq!(binding.selected.argument_types.len(), 2);
            for ty in binding.selected.argument_types.iter() {
                let FunctionArgumentType::Value(ty) = ty else {
                    panic!("actual original value source")
                };
                assert_eq!(ty.data_type().clone(), DataType::Utf8);
                assert_eq!(ty.logical_type(), ValueLogicalType::Physical);
            }
            let FunctionResultType::Scalar(output) = &binding.selected.result_type else {
                panic!("actual original scalar result")
            };
            assert_eq!(output.data_type().clone(), DataType::Utf8);
            assert!(output.nullable());
            eprintln!(
                "append static original journal mode=compiled fragment={:?} source={:?} binding={binding:?}",
                fragment.id(),
                source.id
            );
            bindings.push(binding.clone());
        }
    }
    work.finish().unwrap();
    assert!(
        !bindings.is_empty(),
        "actual corpus source retains its original calls"
    );
    bindings
}

#[test]
fn e08s1_append_original_static_actual_corpus_journal_and_deferred_suffix_remain_unchanged() {
    let original = builtin_sql_function_catalog().snapshot();
    let control = SqlCompileControl::unbounded();
    for binding in bindings() {
        original
            .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control)
            .unwrap();
        let mut stale = binding.clone();
        let FunctionResultType::Scalar(output) = &mut stale.selected.result_type else {
            unreachable!()
        };
        *output = output
            .with_nullable(false)
            .expect("valid test root nullability");
        // Original mode does not acquire new candidate support authority.
        original
            .admit_bound_lifecycle_observed(&stale, PureCallLifecycle::Scalar, &control)
            .unwrap();
    }
}
