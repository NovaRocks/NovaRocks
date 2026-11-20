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
fn analyze(sql: &str, mode: &str, limit: Option<i64>) -> crate::analysis::ResolvedQuery {
    let query = &novarocks_parser::parser::parse_query(sql).unwrap();
    let mut settings = crate::sql_mode::SqlSemanticSettings::default()
        .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(mode));
    if let Some(raw) = limit {
        settings = settings.with_group_concat_max_len(raw);
    }
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
fn facts(query: &crate::analysis::ResolvedQuery) -> &crate::binding::GroupConcatSourceFacts {
    let QueryBody::Select(select) = &query.body else {
        panic!("select")
    };
    let ExprKind::AggregateCall { resolved, .. } = &select.projection[0].expr.kind else {
        panic!("aggregate")
    };
    resolved.group_concat_source().unwrap()
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
fn actual_gc_call_captures_lexical_hint_raw_limit_distinct_and_order() {
    let query = analyze(
        "WITH SESSION sql_mode = 'GROUP_CONCAT_LEGACY' select string_agg(distinct 'a' \n,\n '|'\norder by 1 desc nulls first \n)",
        "32",
        Some(-1),
    );
    let actual = facts(&query);
    assert!(actual.legacy);
    assert_eq!(actual.max_len, Some(-1));
    assert!(state(&query).distinct);
    assert_eq!(
        state(&query).order_keys.as_ref(),
        [novarocks_type_contract::AggregateStateOrderKey {
            ascending: false,
            nulls_first: true
        }]
    );
}
#[test]
fn explicit_separator_does_not_guess_mode_and_missing_limit_stays_absent() {
    for mode in ["32", "GROUP_CONCAT_LEGACY"] {
        let query = analyze("select string_agg('a' \n,\n '|'\n\n)", mode, None);
        assert_eq!(facts(&query).legacy, mode == "GROUP_CONCAT_LEGACY");
        assert_eq!(facts(&query).max_len, None);
        assert!(!state(&query).distinct);
        assert!(state(&query).order_keys.is_empty());
    }
}

#[test]
fn actual_array_calls_author_generic_receipt_without_gc_environment() {
    for (sql, distinct, ascending, nulls_first) in [
        (
            "select array_agg(distinct 7 order by 1 desc nulls first)",
            true,
            false,
            true,
        ),
        (
            "select array_agg(7 order by 1 asc nulls last)",
            false,
            true,
            false,
        ),
    ] {
        let query = analyze(sql, "32", None);
        let receipt = state(&query);
        assert_eq!(receipt.distinct, distinct);
        assert_eq!(
            receipt.order_keys.as_ref(),
            [novarocks_type_contract::AggregateStateOrderKey {
                ascending,
                nulls_first
            }]
        );
        let QueryBody::Select(select) = &query.body else {
            panic!("select")
        };
        let ExprKind::AggregateCall { resolved, .. } = &select.projection[0].expr.kind else {
            panic!("aggregate")
        };
        assert!(resolved.group_concat_source().is_none());
    }
}
#[test]
fn ordinary_aggregate_receipt_is_authored_from_real_plain_call() {
    let query = analyze("select sum(7)", "32", None);
    let receipt = state(&query);
    assert!(!receipt.distinct);
    assert!(receipt.order_keys.is_empty());
}
