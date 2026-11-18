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
//! Public SQL completion inputs for owner and compile-to-execute tests.
use arrow::datatypes::DataType;
pub(super) use novarocks_sql_testkit::{
    ndv_sql_source, provider_contract, sql_source_with_columns,
};
const HAVING: &str = "SELECT k FROM fixture.ndv_null_contract GROUP BY k\nHAVING ndv(v) = 0 AND approx_count_distinct(v) = 0 ORDER BY k;";
const EMPTY_WHERE: &str = "SELECT ndv(v) AS ndv_empty, approx_count_distinct(v) AS approx_empty,\n       count(DISTINCT v) AS exact_empty\nFROM fixture.ndv_null_contract WHERE k = 999;";
const NULL_WHERE: &str = "SELECT ndv(v) AS ndv_null, approx_count_distinct(v) AS approx_null,\n       count(DISTINCT v) AS exact_null\nFROM fixture.ndv_null_contract WHERE k = 1;";

fn source_probe() {
    use novarocks_physical_plan::NodeKind;
    for (query, sql) in [(3, EMPTY_WHERE), (4, NULL_WHERE), (7, HAVING)] {
        let source = ndv_sql_source(sql);
        let mut filters = 0;
        let mut multi_predicate_filters = 0;
        let mut aggregate_calls = 0;
        for (fragment_id, fragment) in source.plan().fragments() {
            for node in fragment.nodes().values() {
                match &node.kind {
                    NodeKind::Filter { predicates } => {
                        filters += 1;
                        eprintln!(
                            "NDV actual SQL query={query} mode=compiled fragment={:?} filter={:?} inputs={:?} predicate_count={} output={:?}",
                            fragment_id,
                            node.id,
                            node.inputs,
                            predicates.len(),
                            node.output
                        );
                        assert_eq!(node.inputs.len(), 1);
                        assert!(!predicates.is_empty());
                        let input = fragment.nodes().get(&node.inputs[0]).unwrap();
                        assert_eq!(node.output.columns, input.output.columns);
                        for (ordinal, id) in predicates.iter().enumerate() {
                            let expression = fragment.root_expression(node.id, *id).unwrap();
                            assert_eq!(expression.ty.data_type().clone(), DataType::Boolean);
                            eprintln!(
                                "NDV actual FilterPredicate ordinal={ordinal} id={id:?} expression={expression:?}"
                            );
                        }
                        if predicates.len() > 1 {
                            multi_predicate_filters += 1;
                            assert_eq!(query, 7);
                            assert_eq!(predicates.len(), 2);
                        }
                    }
                    NodeKind::Aggregate { calls, .. } => {
                        aggregate_calls += calls.len();
                        for (ordinal, call) in calls.iter().enumerate() {
                            eprintln!(
                                "NDV actual aggregate query={query} mode=compiled node={:?} call={ordinal} authored={call:?}",
                                node.id
                            );
                        }
                    }
                    NodeKind::Scan {
                        relation,
                        residuals,
                        ..
                    } => {
                        eprintln!(
                            "NDV actual scan query={query} mode=compiled node={:?} relation={relation:?} residuals={residuals:?}",
                            node.id
                        );
                    }
                    _ => {}
                }
            }
            // This is diagnostic observation of the already-emitted original
            // arena. It performs no lowering, decoder, rewriter or evaluation.
            for (_source_node, _unit, id, expression) in
                super::sql_test_source::unit_definitions(fragment)
            {
                eprintln!(
                    "NDV actual original definition query={query} mode=compiled id={id:?} expression={expression:?}"
                );
            }
            for (id, value) in fragment.values() {
                eprintln!(
                    "NDV actual original value query={query} mode=compiled id={id:?} value={value:?}"
                );
            }
        }
        assert!(aggregate_calls > 0);
        if query == 7 {
            assert!(filters > 0);
            assert_eq!(
                multi_predicate_filters, 1,
                "the original HAVING author retains two conjunct roots on one Filter"
            );
        } else {
            assert_eq!(multi_predicate_filters, 0);
        }
    }
}

#[test]
fn ndv_actual_filter_candidate_sql_source() {
    source_probe();
}
