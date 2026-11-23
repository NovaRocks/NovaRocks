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

use crate::analysis::{ExprKind, QueryBody};
struct EmptyCatalog;
impl crate::catalog::PlannerTableProvider for EmptyCatalog {
    fn resolve_table_for_analysis(
        &self,
        _: Option<&str>,
        _: &str,
        table: &str,
    ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
        Err(format!("table not found: {table}"))
    }
}
fn analyze(sql: &str) -> crate::analysis::ResolvedQuery {
    let query = &novarocks_parser::parser::parse_query(sql).unwrap();
    let settings = crate::sql_mode::SqlSemanticSettings::default();
    crate::analyzer::analyze_with_function_catalog_and_sql_semantics(
        query,
        &EmptyCatalog,
        "default",
        crate::functions::builtin_sql_function_catalog(),
        &settings,
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap()
    .0
}
fn state(
    query: &crate::analysis::ResolvedQuery,
) -> &novarocks_type_contract::AggregateStateInterpretation {
    let QueryBody::Select(select) = &query.body else {
        panic!("select")
    };
    let ExprKind::AggregateCall { resolved, .. } = &select.projection[0].expr.kind else {
        panic!("expected aggregate call");
    };
    resolved.aggregate_state_source().unwrap()
}

#[test]
fn actual_array_calls_author_their_original_state_interpretation() {
    for (sql, distinct, ascending, nulls_first) in [
        (
            "select array_agg(distinct 7 order by 7 desc nulls first)",
            true,
            false,
            true,
        ),
        (
            "select array_agg(7 order by 7 asc nulls last)",
            false,
            true,
            false,
        ),
    ] {
        let query = analyze(sql);
        let receipt = state(&query);
        assert_eq!(receipt.distinct, distinct);
        assert_eq!(
            receipt.order_keys.as_ref(),
            [novarocks_type_contract::AggregateStateOrderKey {
                ascending,
                nulls_first
            }]
        );
    }
}
#[test]
fn ordinary_aggregate_receipt_is_authored_from_real_plain_call() {
    let query = analyze("select sum(7)");
    let receipt = state(&query);
    assert!(!receipt.distinct);
    assert!(receipt.order_keys.is_empty());
}
