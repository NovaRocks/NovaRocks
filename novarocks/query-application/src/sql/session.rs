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

//! Query-application session state.

use std::collections::BTreeMap;
use std::fmt;

use novarocks_parser::{
    ast::{self, Fold, Statement},
    printer::print_expr,
};
use novarocks_sql::compiler::SessionOptimizerSettings;
use novarocks_types::naming::DEFAULT_DATABASE;

use crate::{
    session_error::{QueryServiceError, QueryServiceErrorKind},
    sql::session_admit::SessionAdmitError,
};

/// Connection-local SQL state that is independent of a protocol adapter or
/// role-local runtime. Adapters may validate and apply mutations to this
/// state, but do not own a second session representation.
#[derive(Clone)]
pub struct SessionSqlState {
    current_catalog: Option<String>,
    current_database: String,
    execution_settings: SessionExecutionSettings,
    optimizer_settings: SessionOptimizerSettings,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    user_variables: BTreeMap<String, String>,
}

impl Default for SessionSqlState {
    fn default() -> Self {
        Self {
            current_catalog: None,
            current_database: DEFAULT_DATABASE.to_string(),
            execution_settings: SessionExecutionSettings::default(),
            optimizer_settings: SessionOptimizerSettings::default(),
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            user_variables: BTreeMap::new(),
        }
    }
}

impl SessionSqlState {
    pub fn current_catalog(&self) -> Option<&str> {
        self.current_catalog.as_deref()
    }

    pub fn current_database(&self) -> &str {
        &self.current_database
    }

    pub fn sql_semantics(&self) -> &novarocks_sql::sql_mode::SqlSemanticSettings {
        &self.sql_semantics
    }

    pub fn execution_settings(&self) -> &SessionExecutionSettings {
        &self.execution_settings
    }

    /// Stores a catalog the role owner has already resolved against its
    /// external projection. If the default catalog is selected and the old
    /// database does not exist there, reset to the portable default database.
    pub fn apply_resolved_catalog(
        &mut self,
        catalog: Option<String>,
        current_database_exists: bool,
    ) {
        self.current_catalog = catalog;
        if self.current_catalog.is_none() && !current_database_exists {
            self.current_database = DEFAULT_DATABASE.to_string();
        }
    }

    /// Replaces the already-resolved database context selected by a role
    /// adapter. Validation remains with that adapter's external catalog owner.
    pub fn set_resolved_database_context(&mut self, catalog: Option<String>, database: String) {
        self.current_catalog = catalog;
        self.current_database = database;
    }

    /// Transfers the immutable inputs needed to construct one query attempt.
    /// The role adapter may add role-local admission facts, but it cannot mutate
    /// the session after this transfer.
    pub fn into_query_attempt_inputs(
        self,
    ) -> (
        Option<String>,
        String,
        SessionExecutionSettings,
        SessionOptimizerSettings,
        novarocks_sql::sql_mode::SqlSemanticSettings,
    ) {
        (
            self.current_catalog,
            self.current_database,
            self.execution_settings,
            self.optimizer_settings,
            self.sql_semantics,
        )
    }

    /// Records one SQL expression as a connection-local user variable.
    pub fn set_user_variable(&mut self, name: &str, value: String) {
        self.user_variables.insert(name.to_ascii_lowercase(), value);
    }

    /// Rewrites references to this session's user variables with their stored
    /// scalar SQL expressions before a role adapter prepares the statement.
    pub fn substitute_user_variables(&self, statement: Statement) -> Result<Statement, String> {
        if self.user_variables.is_empty() {
            return Ok(statement);
        }

        let mut values = BTreeMap::new();
        for (name, value) in &self.user_variables {
            let statements = novarocks_parser::parse(&format!("SELECT {value}"))
                .map_err(|error| format!("invalid session user variable {name}: {error}"))?;
            let [Statement::Query(query)] = statements.as_slice() else {
                return Err(format!("invalid session user variable {name}"));
            };
            let ast::SetExpr::Select(select) = query.body.as_ref() else {
                return Err(format!("invalid session user variable {name}"));
            };
            let [item] = select.projection.as_slice() else {
                return Err(format!("invalid session user variable {name}"));
            };
            let expression = match item {
                ast::SelectItem::UnnamedExpr(expression)
                | ast::SelectItem::ExprWithAlias {
                    expr: expression, ..
                } => expression.clone(),
                ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. } => {
                    return Err(format!("invalid session user variable {name}"));
                }
            };
            values.insert(name.to_ascii_lowercase(), expression);
        }

        struct Substituter {
            values: BTreeMap<String, ast::Expr>,
        }

        impl Fold for Substituter {
            fn fold_expr(&mut self, expression: ast::Expr) -> ast::Expr {
                if let ast::Expr::UserVariable(variable) = &expression
                    && let Some(value) = self.values.get(&variable.value.to_ascii_lowercase())
                {
                    return value.clone();
                }
                ast::fold_expr(self, expression)
            }
        }

        Ok(Substituter { values }.fold_statement(statement))
    }
}

/// The result of applying a parser-admitted SET assignment to application
/// session state. Catalog selection is returned to the role owner because it
/// requires that owner's external catalog projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionSetAssignmentOutcome {
    Applied,
    SelectCatalog(String),
}

/// Applies the parser and session-state portion of one SET assignment.
///
/// The caller retains only catalog resolution and any role-local projection it
/// requires. This function deliberately has no frontend dependency.
pub fn apply_session_set_assignment(
    source: &str,
    assignment: &ast::SetAssignment,
    state: &mut SessionSqlState,
) -> Result<SessionSetAssignmentOutcome, QueryServiceError> {
    match &assignment.target {
        ast::SetTarget::UserVariable(variable) => {
            let value = match &assignment.value {
                ast::SetValue::Expression(value) => print_expr(value),
                ast::SetValue::Query(_) => {
                    return Err(QueryServiceError::new(
                        QueryServiceErrorKind::Internal,
                        "SET query value bypassed the governed scalar route",
                    ));
                }
                ast::SetValue::Words(_) => {
                    return Err(QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "user variable assignment requires an expression",
                    ));
                }
            };
            state.set_user_variable(&variable.value, value);
            Ok(SessionSetAssignmentOutcome::Applied)
        }
        ast::SetTarget::SystemVariable(variable) => {
            let name = variable.value.to_ascii_lowercase();
            if matches!(assignment.scope, ast::SetScope::Global) && is_known_session_setting(&name)
            {
                return Err(QueryServiceError::from_user_error(
                    SessionAdmitError::GlobalScopeUnsupported.to_user_error(
                        source,
                        assignment.span,
                        format!("SET GLOBAL {name} is not supported"),
                    ),
                ));
            }
            if name == "sql_mode" {
                // Preserve the previously admitted GLOBAL no-op. This feature
                // owns connection and statement settings, not global state.
                if matches!(assignment.scope, ast::SetScope::Global) {
                    return Ok(SessionSetAssignmentOutcome::Applied);
                }
                let mode = match &assignment.value {
                    ast::SetValue::Expression(value) => {
                        novarocks_sql::sql_mode::SqlMode::from_expression(value)
                    }
                    ast::SetValue::Words(words) => {
                        novarocks_sql::sql_mode::SqlMode::from_assignment(
                            &words
                                .iter()
                                .map(|word| match word {
                                    ast::SetWord::Ident(value) => value.value.clone(),
                                    ast::SetWord::Literal(value) => {
                                        novarocks_parser::printer::print_expr(&ast::Expr::Literal(
                                            value.clone(),
                                        ))
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join(" "),
                        )
                    }
                    ast::SetValue::Query(_) => return Err(session_value_expression_error()),
                };
                state.sql_semantics = state.sql_semantics.clone().with_sql_mode(mode);
                return Ok(SessionSetAssignmentOutcome::Applied);
            }
            let value = session_setting_value(&assignment.value)?;
            apply_session_system_variable(state, &name, &value)
        }
        ast::SetTarget::Catalog { .. } => {
            if matches!(assignment.scope, ast::SetScope::Global) {
                return Err(QueryServiceError::from_user_error(
                    SessionAdmitError::GlobalScopeUnsupported.to_user_error(
                        source,
                        assignment.span,
                        "SET GLOBAL CATALOG is not supported",
                    ),
                ));
            }
            Ok(SessionSetAssignmentOutcome::SelectCatalog(
                session_catalog_value(&assignment.value)?,
            ))
        }
        ast::SetTarget::Names { .. } | ast::SetTarget::Transaction { .. } => {
            Ok(SessionSetAssignmentOutcome::Applied)
        }
    }
}

/// Rejects the one transaction-setting spelling that would imply state across
/// NovaRocks' statement-level autocommit boundary.
pub fn admit_session_set_assignment(
    source: &str,
    assignment: &ast::SetAssignment,
) -> Result<(), QueryServiceError> {
    let ast::SetTarget::SystemVariable(variable) = &assignment.target else {
        return Ok(());
    };
    if !variable.value.eq_ignore_ascii_case("autocommit") {
        return Ok(());
    }
    match lower_autocommit_setting(&assignment.value)? {
        AutocommitSetting::Enabled => Ok(()),
        AutocommitSetting::Disabled => Err(QueryServiceError::from_user_error(
            SessionAdmitError::TransactionUnsupported.to_user_error(
                source,
                assignment.span,
                "SET autocommit=0 is not supported because NovaRocks only provides statement-level autocommit frontiers",
            ),
        )),
    }
}

fn apply_session_system_variable(
    state: &mut SessionSqlState,
    name: &str,
    value: &str,
) -> Result<SessionSetAssignmentOutcome, QueryServiceError> {
    if name == "catalog" {
        return Ok(SessionSetAssignmentOutcome::SelectCatalog(
            value.to_string(),
        ));
    }
    if name == "autocommit" {
        debug_assert!(matches!(
            lower_autocommit_value(value),
            Ok(AutocommitSetting::Enabled)
        ));
        return Ok(SessionSetAssignmentOutcome::Applied);
    }
    match name {
        "decimal_overflow_to_double" => {
            state.sql_semantics = state
                .sql_semantics
                .clone()
                .with_decimal_overflow_to_double(parse_bool(value)?);
        }
        "query_timeout" => {
            let seconds = parse_value(value, "query_timeout")?;
            state.execution_settings.set_query_timeout_secs(seconds);
        }
        "group_concat_max_len" => {
            let value = parse_value(value, "group_concat_max_len")?;
            state.execution_settings.set_group_concat_max_len(value);
        }
        "pipeline_dop" => {
            let value = parse_value(value, "pipeline_dop")?;
            state.execution_settings.set_pipeline_dop(value);
        }
        "enable_parquet_reader_page_index" => state
            .execution_settings
            .set_enable_parquet_reader_page_index(parse_bool(value)?),
        "enable_scan_datacache" => state
            .execution_settings
            .set_enable_scan_datacache(parse_bool(value)?),
        "enable_populate_datacache" => state
            .execution_settings
            .set_enable_populate_datacache(parse_bool(value)?),
        "runtime_filter_scan_wait_time" => {
            let value = parse_value(value, "runtime_filter_scan_wait_time")?;
            state
                .execution_settings
                .set_runtime_filter_scan_wait_time_ms(value)
                .map_err(session_setting_error)?;
        }
        "global_runtime_filter_wait_timeout" => {
            let value = parse_value(value, "global_runtime_filter_wait_timeout")?;
            state
                .execution_settings
                .set_runtime_filter_wait_timeout_ms(value)
                .map_err(session_setting_error)?;
        }
        "disable_optimizer_rules" | "cbo_disabled_rules" => {
            state.optimizer_settings.set_disabled_rules(
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|rule| !rule.is_empty())
                    .map(ToOwned::to_owned)
                    .collect(),
            )
        }
        "enable_eliminate_agg" => state
            .optimizer_settings
            .set_enable_eliminate_agg(parse_bool(value)?),
        "enable_ukfk_opt" => state
            .optimizer_settings
            .set_enable_ukfk_opt(parse_bool(value)?),
        _ => apply_optimizer_session_set(&mut state.optimizer_settings, name, value)?,
    }
    Ok(SessionSetAssignmentOutcome::Applied)
}

fn session_setting_value(value: &ast::SetValue) -> Result<String, QueryServiceError> {
    match value {
        ast::SetValue::Expression(value) => Ok(print_expr(value)
            .trim_matches('\'')
            .trim_matches('"')
            .to_string()),
        ast::SetValue::Words(words) => {
            let [ast::SetWord::Ident(value)] = words.as_slice() else {
                return Err(session_value_expression_error());
            };
            if matches!(value.value.to_ascii_lowercase().as_str(), "on" | "off") {
                Ok(value.value.to_ascii_lowercase())
            } else {
                Err(session_value_expression_error())
            }
        }
        ast::SetValue::Query(_) => Err(session_value_expression_error()),
    }
}

fn session_value_expression_error() -> QueryServiceError {
    QueryServiceError::new(
        QueryServiceErrorKind::InvalidValue,
        "session variable assignment requires an expression",
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AutocommitSetting {
    Enabled,
    Disabled,
}

fn lower_autocommit_setting(value: &ast::SetValue) -> Result<AutocommitSetting, QueryServiceError> {
    lower_autocommit_value(&session_setting_value(value)?)
}

fn lower_autocommit_value(value: &str) -> Result<AutocommitSetting, QueryServiceError> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "on" | "true" => Ok(AutocommitSetting::Enabled),
        "0" | "off" | "false" => Ok(AutocommitSetting::Disabled),
        _ => Err(QueryServiceError::new(
            QueryServiceErrorKind::InvalidValue,
            format!("invalid autocommit value `{value}`; expected 1, ON, TRUE, 0, OFF, or FALSE"),
        )),
    }
}

fn session_catalog_value(value: &ast::SetValue) -> Result<String, QueryServiceError> {
    if let ast::SetValue::Expression(value) = value {
        return Ok(print_expr(value)
            .trim_matches('\'')
            .trim_matches('"')
            .to_string());
    }
    let ast::SetValue::Words(words) = value else {
        return Err(catalog_value_error());
    };
    let [ast::SetWord::Ident(catalog)] = words.as_slice() else {
        return Err(catalog_value_error());
    };
    Ok(catalog.value.clone())
}

fn catalog_value_error() -> QueryServiceError {
    QueryServiceError::new(
        QueryServiceErrorKind::InvalidValue,
        "SET CATALOG requires a catalog name",
    )
}

fn is_known_session_setting(name: &str) -> bool {
    matches!(
        name,
        "autocommit"
            | "decimal_overflow_to_double"
            | "catalog"
            | "query_timeout"
            | "group_concat_max_len"
            | "pipeline_dop"
            | "enable_parquet_reader_page_index"
            | "enable_scan_datacache"
            | "enable_populate_datacache"
            | "runtime_filter_scan_wait_time"
            | "global_runtime_filter_wait_timeout"
            | "disable_optimizer_rules"
            | "cbo_disabled_rules"
            | "enable_eliminate_agg"
            | "enable_ukfk_opt"
            | "cbo_broadcast_backend_count"
            | "cbo_broadcast_node_mem_budget_bytes"
            | "global_runtime_filter_build_max_size"
            | "global_runtime_filter_build_min_size"
            | "global_runtime_filter_probe_min_size"
            | "global_runtime_filter_probe_min_selectivity"
            | "cbo_max_reorder_node_use_exhaustive"
            | "cbo_max_reorder_node_use_dp"
            | "cbo_max_reorder_node_use_greedy"
            | "cbo_max_reorder_node"
            | "enable_query_rewrite_table_prune"
            | "enable_cbo_table_prune"
            | "enable_table_prune_on_update"
            | "enable_common_subexpr_reuse"
            | "enable_global_runtime_filter"
            | "enable_materialized_view_rewrite"
            | "enable_connector_static_predicate_pushdown"
            | "cbo_enable_dp_join_reorder"
            | "cbo_enable_greedy_join_reorder"
            | "enable_global_runtime_filter_cross_exchange"
    )
}

fn parse_bool(value: &str) -> Result<bool, QueryServiceError> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "on" | "true" => Ok(true),
        "0" | "off" | "false" => Ok(false),
        _ => Err(QueryServiceError::new(
            QueryServiceErrorKind::InvalidValue,
            format!("invalid boolean value `{value}`"),
        )),
    }
}

fn parse_value<T: std::str::FromStr>(value: &str, name: &str) -> Result<T, QueryServiceError> {
    value.parse().map_err(|_| {
        QueryServiceError::new(
            QueryServiceErrorKind::InvalidValue,
            format!("invalid {name}"),
        )
    })
}

fn session_setting_error(error: SessionSettingError) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::InvalidValue, error.to_string())
}

fn apply_optimizer_session_set(
    settings: &mut SessionOptimizerSettings,
    name: &str,
    value: &str,
) -> Result<(), QueryServiceError> {
    let parse_bool_value = || parse_bool(value);
    let parse_u64_value = || parse_value(value, name);
    let parse_f64_value = || parse_value(value, name);
    let parse_usize_value = || parse_value(value, name);

    match name {
        "cbo_broadcast_backend_count" => settings.set_broadcast_backend_count(parse_f64_value()?),
        "cbo_broadcast_node_mem_budget_bytes" => {
            settings.cbo_broadcast_node_mem_budget_bytes = Some(parse_f64_value()?)
        }
        "global_runtime_filter_build_max_size" => {
            settings.rf_build_max_bytes = Some(parse_u64_value()?)
        }
        "global_runtime_filter_build_min_size" => {
            settings.rf_build_min_bytes = Some(parse_u64_value()?)
        }
        "global_runtime_filter_probe_min_size" => {
            settings.rf_probe_min_bytes = Some(parse_u64_value()?)
        }
        "global_runtime_filter_probe_min_selectivity" => {
            settings.rf_probe_min_selectivity = Some(parse_f64_value()?)
        }
        "cbo_max_reorder_node_use_exhaustive" => {
            settings.max_reorder_node_use_exhaustive = Some(parse_usize_value()?)
        }
        "cbo_max_reorder_node_use_dp" => {
            settings.max_reorder_node_use_dp = Some(parse_usize_value()?)
        }
        "cbo_max_reorder_node_use_greedy" => {
            settings.max_reorder_node_use_greedy = Some(parse_usize_value()?)
        }
        "cbo_max_reorder_node" => settings.max_reorder_node = Some(parse_usize_value()?),
        "enable_query_rewrite_table_prune" => {
            settings.enable_query_rewrite_table_prune = parse_bool_value()?
        }
        "enable_cbo_table_prune" => settings.enable_cbo_table_prune = parse_bool_value()?,
        "enable_table_prune_on_update" => {
            settings.enable_table_prune_on_update = parse_bool_value()?
        }
        "enable_common_subexpr_reuse" => {
            settings.enable_common_subexpr_reuse = Some(parse_bool_value()?)
        }
        "enable_global_runtime_filter" => {
            settings.enable_global_runtime_filter = Some(parse_bool_value()?)
        }
        "enable_materialized_view_rewrite" => {
            settings.enable_materialized_view_rewrite = Some(parse_bool_value()?)
        }
        "enable_connector_static_predicate_pushdown" => {
            settings.enable_connector_static_predicate_pushdown = Some(parse_bool_value()?)
        }
        "cbo_enable_dp_join_reorder" => settings.enable_dp_join_reorder = Some(parse_bool_value()?),
        "cbo_enable_greedy_join_reorder" => {
            settings.enable_greedy_join_reorder = Some(parse_bool_value()?)
        }
        "enable_global_runtime_filter_cross_exchange" => {
            settings.allow_cross_exchange_rf = Some(parse_bool_value()?)
        }
        _ => {}
    }
    Ok(())
}

/// Connection-local settings that SQL admission has validated.
///
/// This type deliberately contains no protocol DTO. MySQL and native role
/// adapters project this state into their own validated wire contracts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionExecutionSettings {
    query_timeout_secs: Option<u64>,
    group_concat_max_len: i64,
    pipeline_dop: Option<i32>,
    enable_parquet_reader_page_index: bool,
    enable_scan_datacache: bool,
    enable_populate_datacache: bool,
    runtime_filter_scan_wait_time_ms: Option<i64>,
    runtime_filter_wait_timeout_ms: Option<i32>,
}

impl Default for SessionExecutionSettings {
    fn default() -> Self {
        Self {
            query_timeout_secs: None,
            group_concat_max_len: 1024,
            pipeline_dop: None,
            enable_parquet_reader_page_index: false,
            enable_scan_datacache: false,
            enable_populate_datacache: false,
            runtime_filter_scan_wait_time_ms: None,
            runtime_filter_wait_timeout_ms: None,
        }
    }
}

impl SessionExecutionSettings {
    pub const fn query_timeout_secs(&self) -> Option<u64> {
        self.query_timeout_secs
    }

    pub fn set_query_timeout_secs(&mut self, seconds: u64) {
        self.query_timeout_secs = (seconds > 0).then_some(seconds);
    }

    pub const fn group_concat_max_len(&self) -> i64 {
        self.group_concat_max_len
    }

    /// Keep the session value verbatim; aggregate lowering clamps it to the
    /// supported minimum before execution.
    pub fn set_group_concat_max_len(&mut self, value: i64) {
        self.group_concat_max_len = value;
    }

    pub const fn pipeline_dop(&self) -> Option<i32> {
        self.pipeline_dop
    }

    pub fn set_pipeline_dop(&mut self, value: i32) {
        self.pipeline_dop = (value > 0).then_some(value);
    }

    pub const fn enable_parquet_reader_page_index(&self) -> bool {
        self.enable_parquet_reader_page_index
    }

    pub fn set_enable_parquet_reader_page_index(&mut self, enabled: bool) {
        self.enable_parquet_reader_page_index = enabled;
    }

    pub const fn enable_scan_datacache(&self) -> bool {
        self.enable_scan_datacache
    }

    pub fn set_enable_scan_datacache(&mut self, enabled: bool) {
        self.enable_scan_datacache = enabled;
    }

    pub const fn enable_populate_datacache(&self) -> bool {
        self.enable_populate_datacache
    }

    pub fn set_enable_populate_datacache(&mut self, enabled: bool) {
        self.enable_populate_datacache = enabled;
    }

    pub const fn runtime_filter_scan_wait_time_ms(&self) -> Option<i64> {
        self.runtime_filter_scan_wait_time_ms
    }

    pub fn set_runtime_filter_scan_wait_time_ms(
        &mut self,
        value: i64,
    ) -> Result<(), SessionSettingError> {
        if value < 0 {
            return Err(SessionSettingError::NegativeRuntimeFilterScanWaitTime);
        }
        self.runtime_filter_scan_wait_time_ms = Some(value);
        Ok(())
    }

    pub const fn runtime_filter_wait_timeout_ms(&self) -> Option<i32> {
        self.runtime_filter_wait_timeout_ms
    }

    pub fn set_runtime_filter_wait_timeout_ms(
        &mut self,
        value: i32,
    ) -> Result<(), SessionSettingError> {
        if value < 0 {
            return Err(SessionSettingError::NegativeRuntimeFilterWaitTimeout);
        }
        self.runtime_filter_wait_timeout_ms = Some(value);
        Ok(())
    }
}

/// A closed validation failure for one SQL-session setting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionSettingError {
    NegativeRuntimeFilterScanWaitTime,
    NegativeRuntimeFilterWaitTimeout,
}

impl fmt::Display for SessionSettingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeRuntimeFilterScanWaitTime => {
                formatter.write_str("runtime_filter_scan_wait_time must be non-negative")
            }
            Self::NegativeRuntimeFilterWaitTimeout => {
                formatter.write_str("global_runtime_filter_wait_timeout must be non-negative")
            }
        }
    }
}

impl std::error::Error for SessionSettingError {}

#[cfg(test)]
mod tests {
    use super::{
        SessionExecutionSettings, SessionSetAssignmentOutcome, SessionSettingError,
        SessionSqlState, admit_session_set_assignment, apply_session_set_assignment,
    };
    use crate::session_error::QueryServiceErrorKind;
    use novarocks_parser::{ast, ast::Statement as ParsedStatement};
    use novarocks_types::naming::DEFAULT_DATABASE;

    #[test]
    fn default_sql_session_state_is_neutral_and_empty() {
        let state = SessionSqlState::default();
        assert_eq!(state.current_catalog(), None);
        assert_eq!(state.current_database(), DEFAULT_DATABASE);
        assert!(state.user_variables.is_empty());
        assert_eq!(
            state.execution_settings(),
            &SessionExecutionSettings::default()
        );
    }

    #[test]
    fn preserves_session_values_before_wire_projection() {
        let mut settings = SessionExecutionSettings::default();
        settings.set_query_timeout_secs(17);
        settings.set_pipeline_dop(4);
        settings
            .set_runtime_filter_scan_wait_time_ms(0)
            .expect("zero is valid");
        settings.set_group_concat_max_len(-1);

        assert_eq!(settings.query_timeout_secs(), Some(17));
        assert_eq!(settings.pipeline_dop(), Some(4));
        assert_eq!(settings.runtime_filter_scan_wait_time_ms(), Some(0));
        assert_eq!(settings.group_concat_max_len(), -1);
    }

    #[test]
    fn preserves_boolean_switches() {
        let mut settings = SessionExecutionSettings::default();
        settings.set_enable_parquet_reader_page_index(true);
        settings.set_enable_scan_datacache(true);
        settings.set_enable_populate_datacache(true);

        assert!(settings.enable_parquet_reader_page_index());
        assert!(settings.enable_scan_datacache());
        assert!(settings.enable_populate_datacache());
    }

    #[test]
    fn rejects_negative_runtime_filter_settings() {
        let mut settings = SessionExecutionSettings::default();
        assert_eq!(
            settings.set_runtime_filter_scan_wait_time_ms(-1),
            Err(SessionSettingError::NegativeRuntimeFilterScanWaitTime)
        );
        assert_eq!(
            settings.set_runtime_filter_wait_timeout_ms(-1),
            Err(SessionSettingError::NegativeRuntimeFilterWaitTimeout)
        );
    }

    #[test]
    fn substitutes_user_variables_without_a_frontend_router() {
        let mut state = SessionSqlState::default();
        state.set_user_variable("@limit", "7".to_string());
        let statement = novarocks_parser::parse("SELECT @limit")
            .expect("parse query")
            .pop()
            .expect("one statement");

        let rendered = novarocks_parser::printer::print_statement(
            &state
                .substitute_user_variables(statement)
                .expect("substitute session variable"),
        );
        assert_eq!(rendered, "SELECT 7");
    }

    #[test]
    fn resolved_catalog_selection_preserves_or_resets_database_explicitly() {
        let mut state = SessionSqlState::default();
        state.set_resolved_database_context(Some("lake".to_string()), "analytics".to_string());

        state.apply_resolved_catalog(Some("other_lake".to_string()), false);
        assert_eq!(state.current_catalog(), Some("other_lake"));
        assert_eq!(state.current_database(), "analytics");

        state.apply_resolved_catalog(None, false);
        assert_eq!(state.current_catalog(), None);
        assert_eq!(state.current_database(), DEFAULT_DATABASE);
    }

    fn set_assignment(sql: &str) -> ast::SetAssignment {
        let statements = novarocks_parser::parse(sql).expect("SET must parse");
        let [ParsedStatement::Session(ast::SessionStatement::Set(statement))] =
            statements.as_slice()
        else {
            panic!("expected one SET statement");
        };
        statement.assignments[0].clone()
    }

    #[test]
    fn applies_boolean_and_optimizer_settings_without_a_frontend_router() {
        let mut state = SessionSqlState::default();
        for sql in [
            "SET enable_eliminate_agg = on",
            "SET cbo_broadcast_node_mem_budget_bytes = 0",
            "SET global_runtime_filter_probe_min_selectivity = 0.0",
            "SET enable_common_subexpr_reuse = false",
            "SET cbo_max_reorder_node_use_exhaustive = 2",
        ] {
            let assignment = set_assignment(sql);
            assert_eq!(
                apply_session_set_assignment(sql, &assignment, &mut state)
                    .expect("setting must apply"),
                SessionSetAssignmentOutcome::Applied,
            );
        }

        let (_, _, _, optimizer_settings, _) = state.into_query_attempt_inputs();
        assert!(optimizer_settings.enable_eliminate_agg);
        assert_eq!(
            optimizer_settings.cbo_broadcast_node_mem_budget_bytes,
            Some(0.0)
        );
        assert_eq!(optimizer_settings.rf_probe_min_selectivity, Some(0.0));
        assert_eq!(optimizer_settings.enable_common_subexpr_reuse, Some(false));
        assert_eq!(optimizer_settings.max_reorder_node_use_exhaustive, Some(2));
    }

    #[test]
    fn keeps_catalog_resolution_outside_application_session_state() {
        let assignment = set_assignment("SET CATALOG external_catalog");
        let mut state = SessionSqlState::default();
        assert_eq!(
            apply_session_set_assignment("SET CATALOG external_catalog", &assignment, &mut state)
                .expect("catalog target must lower"),
            SessionSetAssignmentOutcome::SelectCatalog("external_catalog".to_string()),
        );
        assert_eq!(state.current_catalog(), None);
    }

    #[test]
    fn rejects_disabled_autocommit_and_global_known_settings_at_admission() {
        let disabled = set_assignment("SET autocommit = OFF");
        let error = admit_session_set_assignment("SET autocommit = OFF", &disabled)
            .expect_err("disabled autocommit is unsupported");
        assert_eq!(error.kind(), QueryServiceErrorKind::Parse);
        assert!(error.message().contains("statement-level autocommit"));

        let global = set_assignment("SET GLOBAL query_timeout = 1");
        let error = apply_session_set_assignment(
            "SET GLOBAL query_timeout = 1",
            &global,
            &mut SessionSqlState::default(),
        )
        .expect_err("global known setting is unsupported");
        assert_eq!(error.kind(), QueryServiceErrorKind::Parse);
        assert!(error.message().contains("SET GLOBAL query_timeout"));
    }

    #[test]
    fn sql_mode_set_replaces_whole_connection_setting_and_preserves_other_modes() {
        let mut a = SessionSqlState::default();
        let mut b = SessionSqlState::default();
        for sql in [
            "SET sql_mode='32,GROUP_CONCAT_LEGACY,ALLOW_THROW_EXCEPTION'",
            "SET LOCAL sql_mode='GROUP_CONCAT_LEGACY'",
            "SET SESSION sql_mode='GROUP_CONCAT_LEGACY,ERROR_IF_OVERFLOW,STRUCT_CAST_BY_NAME'",
        ] {
            assert_eq!(
                apply_session_set_assignment(sql, &set_assignment(sql), &mut a).unwrap(),
                SessionSetAssignmentOutcome::Applied
            );
            assert!(a.sql_semantics().sql_mode().group_concat_legacy());
            assert!(!b.sql_semantics().sql_mode().group_concat_legacy());
        }
        assert_eq!(
            a.sql_semantics().sql_mode().assignment(),
            "GROUP_CONCAT_LEGACY,ERROR_IF_OVERFLOW,STRUCT_CAST_BY_NAME"
        );
        let frozen = a.clone().into_query_attempt_inputs().4;
        for sql in [
            "SET @@session.sql_mode=32",
            "SET sql_mode='STRUCT_CAST_BY_NAME'",
            "SET sql_mode=''",
        ] {
            apply_session_set_assignment(sql, &set_assignment(sql), &mut a).unwrap();
            assert!(!a.sql_semantics().sql_mode().group_concat_legacy());
        }
        assert!(frozen.sql_mode().group_concat_legacy());
        let sql = "SET sql_mode=68719477248";
        apply_session_set_assignment(sql, &set_assignment(sql), &mut b).unwrap();
        assert!(b.sql_semantics().sql_mode().group_concat_legacy());
        assert!(b.sql_semantics().sql_mode().allow_throw_exception());
        assert!(!a.sql_semantics().sql_mode().group_concat_legacy());
        let before = b.sql_semantics().clone();
        let sql = "SET GLOBAL sql_mode=32";
        apply_session_set_assignment(sql, &set_assignment(sql), &mut b).unwrap();
        assert_eq!(
            b.sql_semantics(),
            &before,
            "GLOBAL retains its previously admitted no-op"
        );
    }

    #[test]
    fn decimal_setting_is_connection_local_strict_and_preserved_in_attempt_snapshot() {
        let mut state = SessionSqlState::default();
        assert!(!state.sql_semantics().decimal_overflow_to_double());
        for value in ["1", "ON", "TRUE", "'true'"] {
            let sql = format!("SET SESSION decimal_overflow_to_double={value}");
            apply_session_set_assignment(&sql, &set_assignment(&sql), &mut state).unwrap();
            assert!(state.sql_semantics().decimal_overflow_to_double());
        }
        let sql = "SET sql_mode='GROUP_CONCAT_LEGACY'";
        apply_session_set_assignment(sql, &set_assignment(sql), &mut state).unwrap();
        assert!(state.sql_semantics().decimal_overflow_to_double());
        let frozen = state.clone().into_query_attempt_inputs().4;
        for value in ["0", "OFF", "FALSE"] {
            let sql = format!("SET LOCAL decimal_overflow_to_double={value}");
            apply_session_set_assignment(&sql, &set_assignment(&sql), &mut state).unwrap();
            assert!(!state.sql_semantics().decimal_overflow_to_double());
            assert!(state.sql_semantics().sql_mode().group_concat_legacy());
        }
        assert!(frozen.decimal_overflow_to_double());
        assert!(
            !SessionSqlState::default()
                .sql_semantics()
                .decimal_overflow_to_double()
        );
        for sql in [
            "SET decimal_overflow_to_double=2",
            "SET decimal_overflow_to_double='unknown'",
        ] {
            let before = state.sql_semantics().clone();
            assert!(apply_session_set_assignment(sql, &set_assignment(sql), &mut state).is_err());
            assert_eq!(&before, state.sql_semantics());
        }
        let sql = "SET GLOBAL decimal_overflow_to_double=true";
        let error =
            apply_session_set_assignment(sql, &set_assignment(sql), &mut state).unwrap_err();
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.admit.session_global_scope_unsupported"
        );
    }
}
