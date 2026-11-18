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
//! Complete actual required SQL and original schema facts, without another plan author.
use arrow::datatypes::DataType;
use novarocks_expression_contract::ExprKind;
use novarocks_physical_plan::FunctionArgumentType;
use novarocks_physical_plan::StaticFunctionArgument;

const SEMANTICS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/aggregate/sql/agg_percentile_semantics.sql"
));
const UNION: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/aggregate/sql/agg_test_percentile_union.sql"
));
fn sources() -> Vec<novarocks_sql::compiler::SqlAuthoredPhysicalPlan> {
    let sql = SEMANTICS
        .split_once("-- query 5")
        .unwrap()
        .1
        .split_once("-- query 6")
        .unwrap()
        .0
        .replace("${case_db}", "fixture");
    let first = super::approx_percentile_actual_sql_source_tests::approx_sql_source(sql.trim());
    let sql = UNION
        .lines()
        .find(|line| line.starts_with("select percentile_approx_raw("))
        .unwrap();
    let second = super::hll_hash_actual_sql_source_tests::hll_sql_source(
        sql,
        &[("c1", DataType::Int32), ("c2", DataType::Float64)],
    );
    vec![first, second]
}
#[test]
fn percentile_approx_raw_actual_original_required_sql_full_signature_source() {
    let catalog = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let definition = catalog
        .definition(
            "percentile_approx_raw",
            novarocks_functions::FunctionKind::Scalar,
        )
        .unwrap();
    let declaration = definition.binding_declaration().unwrap();
    assert_eq!(
        declaration.overloads().len(),
        1,
        "the actual fixed ANY x ANY declaration"
    );
    for source in sources() {
        let mut seen = 0;
        for fragment in source.plan().fragments().values() {
            for (_source_node, unit, id, node) in super::sql_test_source::unit_definitions(fragment)
            {
                let ExprKind::FunctionCall { function, args } = &node.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/percentile_approx_raw/v1" {
                    continue;
                }
                let request = unit.requests().get(id).unwrap();
                assert_eq!(args.len(), 2);
                assert_eq!(request.logical_argument_count, 2);
                assert_eq!(request.arguments.len(), 2);
                assert_eq!(function.argument_types.len(), 2);
                for (actual, selected) in
                    request.arguments.iter().zip(function.argument_types.iter())
                {
                    let (
                        StaticFunctionArgument::Value {
                            value_type: actual, ..
                        },
                        FunctionArgumentType::Value(selected),
                    ) = (actual, selected)
                    else {
                        panic!("actual Value source")
                    };
                    assert_eq!(actual, selected);
                }
                let FunctionArgumentType::Value(payload) = &function.argument_types[0] else {
                    panic!("actual payload")
                };
                let FunctionArgumentType::Value(quantile) = &function.argument_types[1] else {
                    panic!("actual quantile")
                };
                assert_eq!(payload.data_type().clone(), DataType::Binary);
                assert!(
                    matches!(quantile.data_type(), DataType::Decimal128(..)),
                    "exact original SQL decimal literal author"
                );
                assert_eq!(node.ty.data_type().clone(), DataType::Float64);
                assert_eq!(&unit.definition(args[0]).unwrap().ty, payload);
                eprintln!(
                    "PERCENTILE_APPROX_RAW original definition={id:?} fullbinding={function:?} request={request:?} payloadSource={:?} quantileSource={:?} output={:?}",
                    unit.definition(args[0]),
                    unit.definition(args[1]),
                    node.ty
                );
                seen += 1;
            }
        }
        assert_eq!(seen, 1);
    }
}
#[test]
fn percentile_approx_raw_actual_original_required_sql_compiler_closure() {
    for source in sources() {
        let results = super::filter_conjunction_actual_sql_compiler_tests::compiler_results(
            &source,
            &super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue(),
        );
        assert!(!results.is_empty());
        for (fragment, program) in results {
            program.unwrap_or_else(|error| {
                panic!("complete percentile_approx_raw required fragment {fragment:?}: {error:?}")
            });
        }
    }
}

#[cfg(test)]
#[path = "percentile_union_actual_sql_source_tests.rs"]
mod percentile_union_actual_sql_source_tests;
