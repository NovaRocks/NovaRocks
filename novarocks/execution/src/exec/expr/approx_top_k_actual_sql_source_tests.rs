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
        "WITH w1 AS (
            SELECT approx_top_k(v, 3) AS x FROM fixture
         ), entries AS (
            SELECT ordinal, x[ordinal] AS entry,
                   __array_struct_subfield(x, 'item')[ordinal] AS item
            FROM w1 CROSS JOIN (VALUES (1), (2), (3)) AS positions(ordinal)
            WHERE ordinal <= cardinality(x)
         )
         SELECT array_agg(entry ORDER BY item NULLS FIRST, ordinal) AS top_items
         FROM entries"
    } else {
        "SELECT approx_top_k(v) AS a,approx_top_k(v,3) AS b,approx_top_k(v,3,100) AS c FROM fixture"
    };
    super::sql_test_source::sql_source_with_declared_fields(sql, &fields)
}
#[test]
fn approx_top_k_actual_original_required_sql_ordered_full_binding_source() {
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
                        != "builtin.aggregate/approx_top_k/v1"
                    {
                        continue;
                    }
                    let site = PhysicalCallSite::Aggregate {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    };
                    let req = fragment.call_requests().get(site).unwrap();
                    assert!((1..=3).contains(&req.logical_argument_count));
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
                        DataType::List(_)
                    ));
                    assert!(call.binding.function.result_type.nullable());
                    eprintln!(
                        "APPROX_TOP_K actual mode=compiled fragment={:?} site={site:?} binding={:?} request={req:?}",
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
fn approx_top_k_actual_original_required_sql_complete_compilation() {
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
