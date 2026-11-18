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
//! Actual SQL emitter -> package -> installed owner -> LocalCompiler provenance.
//! This observes the prepared retained call; native real-fold calls are not inferred.
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use super::numeric_unary_owned_transaction_tests::programs_with_catalogue;
use super::sql_test_source::sql_source;
use arrow::datatypes::DataType;
use novarocks_functions::PreparedPureKernel;

const SQL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/sql/correctness/project/sql/project_append_trailing_char_if_absent_semantics.sql"
));
fn check() {
    let source = sql_source(SQL, DataType::Int64);
    let functions = installed_builtin_owner_catalogue();
    let lowered = programs_with_catalogue(&source, &functions);
    let mut observed = 0;
    for (fragment, program) in &lowered {
        for (site, call) in program
            .units()
            .iter()
            .enumerate()
            .flat_map(|(unit, source)| {
                source
                    .calls()
                    .iter()
                    .map(move |(use_id, call)| ((unit, *use_id), call))
            })
        {
            let contract = call.call_contract();
            if contract.function_id().as_str() != "builtin.scalar/append_trailing_char_if_absent/v1"
            {
                continue;
            }
            assert!(matches!(call.prepared(), PreparedPureKernel::Scalar(_)));
            assert_eq!(contract.selected().argument_types.len(), 2);
            let novarocks_functions::FunctionResultType::Scalar(result) =
                &contract.selected().result_type
            else {
                panic!("actual append_trailing selected result must be scalar");
            };
            assert_eq!(result.data_type().clone(), DataType::Utf8);
            assert!(result.nullable());
            eprintln!(
                "append_trailing actual compiled retained call mode=compiled fragment={fragment:?} site={site:?} contract={contract:?} implementation={:?}",
                call.implementation()
            );
            observed += 1;
        }
    }
    assert!(
        observed > 0,
        "same original source must prepare the actual installed scalar owner"
    );
}

#[test]
fn append_trailing_compiled_candidate_kept_call_source() {
    check()
}
