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
//! Real original nullable corpus field source, ordered full requests and permanent complete compiler.
use arrow::datatypes::{DataType, Field};
use novarocks_physical_plan::{NodeKind, PhysicalCallSite};

fn source(full: bool) -> novarocks_sql::compiler::SqlAuthoredPhysicalPlan {
    let fields = vec![
        Field::new("id", DataType::Int32, true),
        Field::new("grp", DataType::Int32, true),
        Field::new("v", DataType::Int32, true),
        Field::new("txt", DataType::Utf8, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("big_v", DataType::Int64, true),
        Field::new("w", DataType::Int64, true),
    ];
    let sql = if full {
        "WITH frequency AS (
            SELECT approx_most_frequent(3, v, 100001) AS counts FROM fixture
         )
         SELECT element_at(counts, BIGINT '3') AS item_count FROM frequency"
    } else {
        "SELECT approx_most_frequent(2, v, 1) AS a,
                approx_most_frequent(3, v, 100001) AS b,
                approx_most_frequent(3, txt, 10) AS c FROM fixture"
    };
    super::sql_test_source::sql_source_with_declared_fields(sql, &fields)
}
#[test]
fn approx_most_frequent_actual_sql_retains_exact_map_binding() {
    {
        let source = source(false);
        let mut found = 0;
        for fragment in source.plan().fragments().values() {
            for node in fragment.nodes().values() {
                let NodeKind::Aggregate { calls, .. } = &node.kind else {
                    continue;
                };
                for (ordinal, call) in calls.iter().enumerate() {
                    if call.binding.function.function_id.as_str()
                        != "builtin.aggregate/approx_most_frequent/v1"
                    {
                        continue;
                    }
                    let site = PhysicalCallSite::Aggregate {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    };
                    let req = fragment.call_requests().get(site).unwrap();
                    assert_eq!(req.logical_argument_count, 3);
                    assert_eq!(
                        req.logical_argument_count,
                        call.binding.function.argument_types.len()
                    );
                    assert_eq!(
                        call.binding.intermediate_type.data_type().clone(),
                        DataType::Binary
                    );
                    assert!(matches!(
                        call.binding.function.result_type.data_type(),
                        DataType::Map(..)
                    ));
                    assert!(call.binding.function.result_type.nullable());
                    eprintln!(
                        "APPROX_MOST_FREQUENT actual mode=compiled fragment={:?} site={site:?} binding={:?} request={req:?}",
                        fragment.id(),
                        call.binding
                    );
                    found += 1;
                }
            }
        }
        assert!(found >= 3);
    }
}
#[test]
fn approx_most_frequent_actual_sql_complete_compilation() {
    let source = source(true);
    let results = super::filter_conjunction_actual_sql_compiler_tests::compiler_results(
        &source,
        &super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue(),
    );
    assert!(!results.is_empty());
    for (fragment, result) in results {
        result.unwrap_or_else(|err| {
            panic!("complete approx_top_k corpus source {fragment:?}: {err:?}")
        });
    }
}
