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
//! Actual TIME corpus journal loans and full SQL metadata profiles.
use super::sql_test_source::sql_source;
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_expression_contract::ExprKind;
use novarocks_functions::{
    FunctionArgument, FunctionResultType, FunctionValueType, PureCallLifecycle,
    ResolvedFunctionBinding,
};
use novarocks_sql::compiler::{
    SqlCallDependencySite, SqlCompileControl, builtin_sql_function_catalog,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
const SQL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/project/sql/project_time_to_sec_roundtrip_negative_semantics.sql"
));
pub(super) fn bindings() -> Vec<ResolvedFunctionBinding> {
    let owner = sql_source(SQL, DataType::Int64);
    let control = SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut out = Vec::new();
    for fragment in owner.plan().fragments().values() {
        for (_source_node, _unit, _, source) in super::sql_test_source::unit_definitions(fragment) {
            let ExprKind::FunctionCall { function, .. } = &source.kind else {
                continue;
            };
            if function.function_id.as_str() != "builtin.scalar/time_to_sec/v1" {
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
            assert_eq!(binding.logical_argument_count, 1);
            assert_eq!(binding.selected.argument_types.len(), 1);
            eprintln!(
                "TIME original corpus source mode=compiled fragment={:?} source={:?} binding={binding:?}",
                fragment.id(),
                source.id
            );
            out.push(binding.clone());
        }
    }
    work.finish().unwrap();
    assert_eq!(
        out.len(),
        3,
        "all three actual original TIME_TO_SEC occurrences remain retained"
    );
    out
}
pub(super) fn full_binding(name: &str, domain: DataType, mask: usize) -> ResolvedFunctionBinding {
    let mut arguments = vec![FunctionArgument::Value {
        value_type: FunctionValueType::new(domain, mask & 1 != 0),
        constant: None,
    }];
    if name == "time_format" {
        arguments.push(FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Utf8, mask & 2 != 0),
            constant: None,
        });
    }
    builtin_sql_function_catalog()
        .snapshot()
        .resolve_scalar_binding(name, &arguments, &SqlCompileControl::unbounded())
        .unwrap()
}
pub(super) fn domains() -> [DataType; 3] {
    [
        DataType::Utf8,
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, None),
    ]
}
#[test]
fn e08s1_time_original_profile_actual_roundtrip_loans_and_original_no_added_validation() {
    let original = builtin_sql_function_catalog().snapshot();
    let control = SqlCompileControl::unbounded();
    for binding in bindings() {
        original
            .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control)
            .unwrap();
        let mut stale = binding.clone();
        let FunctionResultType::Scalar(out) = &mut stale.selected.result_type else {
            unreachable!()
        };
        *out = out
            .with_nullable(false)
            .expect("valid test root nullability");
        original
            .admit_bound_lifecycle_observed(&stale, PureCallLifecycle::Scalar, &control)
            .unwrap();
    }
}
#[test]
fn e08s1_time_original_profile_all_six_declared_domains_nullable_axes_borrow_original_type_author()
{
    let original = builtin_sql_function_catalog().snapshot();
    for name in ["time_to_sec", "time_format"] {
        for domain in domains() {
            for mask in 0..if name == "time_format" { 4 } else { 2 } {
                let binding = full_binding(name, domain.clone(), mask);
                original
                    .admit_bound_lifecycle_observed(
                        &binding,
                        PureCallLifecycle::Scalar,
                        &SqlCompileControl::unbounded(),
                    )
                    .unwrap();
                let FunctionResultType::Scalar(out) = &binding.selected.result_type else {
                    unreachable!()
                };
                assert!(
                    out.nullable(),
                    "original type author permits successful NULL for both families"
                );
                assert_eq!(
                    out.data_type().clone(),
                    if name == "time_to_sec" {
                        DataType::Int64
                    } else {
                        DataType::Utf8
                    }
                );
            }
        }
    }
}
