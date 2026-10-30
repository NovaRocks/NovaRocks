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

use super::*;
use crate::sql::session::{SessionAssignment, session_set_assignment_effects};
use novarocks_parser::ast;

// Each value is also a SQL literal: SQL and HTTP must produce the same frozen
// attempt inputs, while the response text is canonical header input.
const ROUND_TRIPS: &[(&str, &str, &str, &str)] = &[
    (
        "sql_mode",
        "32,GROUP_CONCAT_LEGACY,UNKNOWN_MODE",
        "sql_mode",
        "32,GROUP_CONCAT_LEGACY,UNKNOWN_MODE",
    ),
    ("autocommit", "ON", "autocommit", "true"),
    (
        "decimal_overflow_to_double",
        "1",
        "decimal_overflow_to_double",
        "true",
    ),
    ("query_timeout", "17", "query_timeout", "17"),
    ("group_concat_max_len", "-23", "group_concat_max_len", "-23"),
    ("pipeline_dop", "7", "pipeline_dop", "7"),
    (
        "runtime_filter_scan_wait_time",
        "19",
        "runtime_filter_scan_wait_time",
        "19",
    ),
    (
        "global_runtime_filter_wait_timeout",
        "29",
        "global_runtime_filter_wait_timeout",
        "29",
    ),
    (
        "enable_parquet_reader_page_index",
        "on",
        "enable_parquet_reader_page_index",
        "true",
    ),
    (
        "enable_scan_datacache",
        "TRUE",
        "enable_scan_datacache",
        "true",
    ),
    (
        "enable_populate_datacache",
        "1",
        "enable_populate_datacache",
        "true",
    ),
    (
        "disable_optimizer_rules",
        " rule_a, ,rule_b ",
        "disable_optimizer_rules",
        "rule_a,rule_b",
    ),
    (
        "cbo_disabled_rules",
        "rule_a,rule_b",
        "disable_optimizer_rules",
        "rule_a,rule_b",
    ),
    ("enable_eliminate_agg", "on", "enable_eliminate_agg", "true"),
    ("enable_ukfk_opt", "true", "enable_ukfk_opt", "true"),
    (
        "cbo_broadcast_backend_count",
        "3.5",
        "cbo_broadcast_backend_count",
        "3.5",
    ),
    (
        "cbo_broadcast_node_mem_budget_bytes",
        "4096.25",
        "cbo_broadcast_node_mem_budget_bytes",
        "4096.25",
    ),
    (
        "global_runtime_filter_build_max_size",
        "111",
        "global_runtime_filter_build_max_size",
        "111",
    ),
    (
        "global_runtime_filter_build_min_size",
        "112",
        "global_runtime_filter_build_min_size",
        "112",
    ),
    (
        "global_runtime_filter_probe_min_size",
        "113",
        "global_runtime_filter_probe_min_size",
        "113",
    ),
    (
        "global_runtime_filter_probe_min_selectivity",
        "0.125",
        "global_runtime_filter_probe_min_selectivity",
        "0.125",
    ),
    (
        "cbo_max_reorder_node_use_exhaustive",
        "2",
        "cbo_max_reorder_node_use_exhaustive",
        "2",
    ),
    (
        "cbo_max_reorder_node_use_dp",
        "3",
        "cbo_max_reorder_node_use_dp",
        "3",
    ),
    (
        "cbo_max_reorder_node_use_greedy",
        "5",
        "cbo_max_reorder_node_use_greedy",
        "5",
    ),
    ("cbo_max_reorder_node", "13", "cbo_max_reorder_node", "13"),
    (
        "enable_query_rewrite_table_prune",
        "true",
        "enable_query_rewrite_table_prune",
        "true",
    ),
    (
        "enable_cbo_table_prune",
        "1",
        "enable_cbo_table_prune",
        "true",
    ),
    (
        "enable_table_prune_on_update",
        "on",
        "enable_table_prune_on_update",
        "true",
    ),
    (
        "enable_common_subexpr_reuse",
        "false",
        "enable_common_subexpr_reuse",
        "false",
    ),
    (
        "enable_global_runtime_filter",
        "off",
        "enable_global_runtime_filter",
        "false",
    ),
    (
        "enable_materialized_view_rewrite",
        "0",
        "enable_materialized_view_rewrite",
        "false",
    ),
    (
        "enable_connector_static_predicate_pushdown",
        "false",
        "enable_connector_static_predicate_pushdown",
        "false",
    ),
    (
        "cbo_enable_dp_join_reorder",
        "off",
        "cbo_enable_dp_join_reorder",
        "false",
    ),
    (
        "cbo_enable_greedy_join_reorder",
        "false",
        "cbo_enable_greedy_join_reorder",
        "false",
    ),
    (
        "enable_global_runtime_filter_cross_exchange",
        "0",
        "enable_global_runtime_filter_cross_exchange",
        "false",
    ),
];

fn assignment(sql: &str) -> ast::SetAssignment {
    let statements = novarocks_parser::parse(sql).expect("valid SET statement");
    let [ast::Statement::Session(ast::SessionStatement::Set(statement))] = statements.as_slice()
    else {
        panic!("expected one SET statement");
    };
    statement.assignments[0].clone()
}

fn effects(sql: &str) -> SessionEffects {
    let SessionAssignment::Effects(effects) =
        session_set_assignment_effects(sql, &assignment(sql)).expect("valid assignment")
    else {
        panic!("expected typed session effects");
    };
    effects
}

#[test]
fn every_known_property_round_trips_between_sql_effects_and_header_input() {
    for &(name, input, canonical_name, expected) in ROUND_TRIPS {
        let mut header = SessionSqlState::default();
        header.apply_session_property_text(name, input).unwrap();
        assert_eq!(
            header.render_session_property(name).as_deref(),
            Some(expected),
            "{name}"
        );
        let mut replay = SessionSqlState::default();
        replay
            .apply_session_property_text(canonical_name, expected)
            .unwrap();
        assert_eq!(
            header.clone().into_query_attempt_inputs(),
            replay.into_query_attempt_inputs(),
            "{name}"
        );

        let sql = format!("SET SESSION {name} = '{input}'");
        let changes = effects(&sql);
        assert_eq!(
            changes.set_properties().collect::<Vec<_>>(),
            [(canonical_name, expected.to_owned())],
            "{name}"
        );
        assert!(changes.catalog_schema().is_none(), "{name}");
        assert_eq!(
            changes.reset_properties(),
            if canonical_name == "disable_optimizer_rules" {
                vec!["cbo_disabled_rules".to_owned()]
            } else {
                vec![]
            },
            "{name}"
        );
        let mut connection = SessionSqlState::default();
        connection.apply_effects(&changes).unwrap();
        assert_eq!(
            connection.into_query_attempt_inputs(),
            header.into_query_attempt_inputs(),
            "{name}"
        );
    }
}

#[test]
fn typed_properties_reach_their_original_attempt_input_fields() {
    let mut state = SessionSqlState::default();
    for &(name, input, _, _) in ROUND_TRIPS {
        state.apply_session_property_text(name, input).unwrap();
    }
    let (_, _, execution, optimizer, semantics) = state.into_query_attempt_inputs();
    assert_eq!(execution.query_timeout_secs(), Some(17));
    assert_eq!(execution.group_concat_max_len(), -23);
    assert_eq!(execution.pipeline_dop(), Some(7));
    assert_eq!(execution.runtime_filter_scan_wait_time_ms(), Some(19));
    assert_eq!(execution.runtime_filter_wait_timeout_ms(), Some(29));
    assert!(execution.enable_parquet_reader_page_index());
    assert!(execution.enable_scan_datacache());
    assert!(execution.enable_populate_datacache());
    assert!(semantics.decimal_overflow_to_double());
    assert_eq!(semantics.group_concat_max_len(), Some(-23));
    assert!(semantics.sql_mode().group_concat_legacy());
    assert_eq!(
        semantics.sql_mode().assignment(),
        "32,GROUP_CONCAT_LEGACY,UNKNOWN_MODE"
    );
    assert_eq!(optimizer.disabled_rules, ["rule_a", "rule_b"]);
    assert!(optimizer.enable_eliminate_agg);
    assert!(optimizer.enable_ukfk_opt);
    assert_eq!(optimizer.cbo_broadcast_backend_count, Some(3.5));
    assert_eq!(optimizer.cbo_broadcast_node_mem_budget_bytes, Some(4096.25));
    assert_eq!(optimizer.rf_build_max_bytes, Some(111));
    assert_eq!(optimizer.rf_build_min_bytes, Some(112));
    assert_eq!(optimizer.rf_probe_min_bytes, Some(113));
    assert_eq!(optimizer.rf_probe_min_selectivity, Some(0.125));
    assert_eq!(optimizer.max_reorder_node_use_exhaustive, Some(2));
    assert_eq!(optimizer.max_reorder_node_use_dp, Some(3));
    assert_eq!(optimizer.max_reorder_node_use_greedy, Some(5));
    assert_eq!(optimizer.max_reorder_node, Some(13));
    assert!(optimizer.enable_query_rewrite_table_prune);
    assert!(optimizer.enable_cbo_table_prune);
    assert!(optimizer.enable_table_prune_on_update);
    assert_eq!(optimizer.enable_common_subexpr_reuse, Some(false));
    assert_eq!(optimizer.enable_global_runtime_filter, Some(false));
    assert_eq!(optimizer.enable_materialized_view_rewrite, Some(false));
    assert_eq!(
        optimizer.enable_connector_static_predicate_pushdown,
        Some(false)
    );
    assert_eq!(optimizer.enable_dp_join_reorder, Some(false));
    assert_eq!(optimizer.enable_greedy_join_reorder, Some(false));
    assert_eq!(optimizer.allow_cross_exchange_rf, Some(false));
    assert_eq!(optimizer.effective_backend_count, None);
    assert_eq!(optimizer.optimizer_query_mem_limit_bytes, None);
}

#[test]
fn canonical_text_preserves_disabled_timeout_and_dop_without_inventing_overrides() {
    let mut state = SessionSqlState::default();
    assert_eq!(
        state.render_session_property("runtime_filter_scan_wait_time"),
        None
    );
    assert_eq!(
        state.render_session_property("global_runtime_filter_wait_timeout"),
        None
    );
    assert_eq!(
        state.render_session_property("cbo_broadcast_backend_count"),
        None
    );
    for input in ["0", "-1", "-2147483648"] {
        state
            .apply_session_property_text("pipeline_dop", input)
            .unwrap();
        assert_eq!(
            state.render_session_property("pipeline_dop").as_deref(),
            Some("0")
        );
        assert_eq!(state.execution_settings().pipeline_dop(), None);
    }
    state
        .apply_session_property_text("query_timeout", "0")
        .unwrap();
    assert_eq!(
        state.render_session_property("query_timeout").as_deref(),
        Some("0")
    );
    assert_eq!(state.execution_settings().query_timeout_secs(), None);
    for name in [
        "runtime_filter_scan_wait_time",
        "global_runtime_filter_wait_timeout",
    ] {
        state.apply_session_property_text(name, "0").unwrap();
        assert_eq!(state.render_session_property(name).as_deref(), Some("0"));
    }
}

#[test]
fn headers_reject_unknown_or_invalid_values_without_changing_original_state() {
    let mut state = SessionSqlState::default();
    state
        .apply_session_property_text("query_timeout", "17")
        .unwrap();
    state
        .apply_session_property_text("runtime_filter_scan_wait_time", "31")
        .unwrap();
    state
        .apply_session_property_text("global_runtime_filter_wait_timeout", "37")
        .unwrap();
    for (name, input) in [
        ("not_a_session_property", "true"),
        ("enable_global_runtime_filter", "2"),
        ("query_timeout", "-1"),
        ("query_timeout", "18446744073709551616"),
        ("pipeline_dop", "2147483648"),
        ("runtime_filter_scan_wait_time", "-1"),
        ("global_runtime_filter_wait_timeout", "-1"),
        ("global_runtime_filter_wait_timeout", "2147483648"),
        ("global_runtime_filter_build_max_size", "-1"),
        ("cbo_max_reorder_node", "-1"),
        ("autocommit", "off"),
        ("autocommit", "false"),
        ("autocommit", "0"),
        ("autocommit", "2"),
    ] {
        let before = state.clone().into_query_attempt_inputs();
        let error = state.apply_session_property_text(name, input).unwrap_err();
        assert!(
            matches!(error, StatementSessionError::InvalidSessionProperty { name: actual, .. } if actual == name),
            "{name}"
        );
        assert_eq!(before, state.clone().into_query_attempt_inputs(), "{name}");
    }
}

#[test]
fn sql_noop_and_user_variables_do_not_fabricate_session_effects() {
    for sql in [
        "SET GLOBAL sql_mode = 'GROUP_CONCAT_LEGACY'",
        "SET unknown_setting = 123",
        "SET NAMES utf8mb4",
    ] {
        assert!(effects(sql).is_empty(), "{sql}");
    }
    let sql = "SET @only_this_session = 7";
    assert!(matches!(
        session_set_assignment_effects(sql, &assignment(sql)).unwrap(),
        SessionAssignment::UserVariable { name, value } if name == "@only_this_session" && value == "7"
    ));
    for sql in ["SET CATALOG lake", "SET catalog = 'lake'"] {
        assert!(matches!(
            session_set_assignment_effects(sql, &assignment(sql)).unwrap(),
            SessionAssignment::SelectCatalog(catalog) if catalog == "lake"
        ));
    }
    let sql = "SET GLOBAL query_timeout = 10";
    let error = match session_set_assignment_effects(sql, &assignment(sql)) {
        Err(error) => error,
        Ok(_) => panic!("GLOBAL known setting must be refused"),
    };
    assert_eq!(
        error.user_error().unwrap().code().as_str(),
        "sql.admit.session_global_scope_unsupported"
    );
}

#[test]
fn failed_later_assignment_retains_earlier_literal_set_effects() {
    let sql = "SET query_timeout = 11, runtime_filter_scan_wait_time = -1";
    let statements = novarocks_parser::parse(sql).unwrap();
    let [ast::Statement::Session(ast::SessionStatement::Set(set))] = statements.as_slice() else {
        panic!("expected SET");
    };
    let mut state = SessionSqlState::default();
    let first = session_set_assignment_effects(sql, &set.assignments[0]).unwrap();
    let SessionAssignment::Effects(first) = first else {
        panic!("expected effects")
    };
    state.apply_effects(&first).unwrap();
    let SessionAssignment::Effects(second) =
        session_set_assignment_effects(sql, &set.assignments[1]).unwrap()
    else {
        panic!("expected effects")
    };
    assert!(state.apply_effects(&second).is_err());
    assert_eq!(state.execution_settings().query_timeout_secs(), Some(11));
    assert_eq!(
        state
            .execution_settings()
            .runtime_filter_scan_wait_time_ms(),
        None
    );
}

#[test]
fn header_property_names_are_exact_while_sql_set_retains_case_insensitivity() {
    let mut state = SessionSqlState::default();
    for (name, input) in [
        ("QUERY_TIMEOUT", "11"),
        ("Catalog", "lake"),
        ("CBO_DISABLED_RULES", "old_rule"),
        ("Sql_Mode", "32"),
    ] {
        let before = state.clone().into_query_attempt_inputs();
        let error = state.apply_session_property_text(name, input).unwrap_err();
        assert!(
            matches!(error, StatementSessionError::InvalidSessionProperty { name: actual, .. } if actual == name)
        );
        assert_eq!(before, state.clone().into_query_attempt_inputs());
    }
    let changes = effects("SET QUERY_TIMEOUT = 11");
    state.apply_effects(&changes).unwrap();
    assert_eq!(state.execution_settings().query_timeout_secs(), Some(11));
    assert_eq!(
        changes.set_properties().collect::<Vec<_>>(),
        [("query_timeout", "11".to_owned())]
    );
}
