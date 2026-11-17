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

//! SQL-owned interpretation of the modes consumed by this engine.
//!
//! A setting retains its full assignment, including modes not consumed here.
//! Adding GROUP_CONCAT_LEGACY does not introduce a validator for all MySQL or
//! StarRocks modes. Unknown names and numeric bits retain their existing
//! admission behavior; retaining them is not an implementation claim.

use novarocks_parser::ast::{self, Fold};

const ONLY_FULL_GROUP_BY: u64 = 1 << 5;
const ALLOW_THROW_EXCEPTION: u64 = 1 << 9;
const GROUP_CONCAT_LEGACY: u64 = 1 << 36;
const ERROR_IF_OVERFLOW: u64 = 1 << 35;
const CONSUMED_MASK: u64 =
    ONLY_FULL_GROUP_BY | ALLOW_THROW_EXCEPTION | GROUP_CONCAT_LEGACY | ERROR_IF_OVERFLOW;

/// One complete connection or statement setting. Successive assignments
/// replace this value; comma-separated items within an assignment combine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMode {
    assignment: String,
    consumed_bits: u64,
}

impl Default for SqlMode {
    fn default() -> Self {
        Self::from_assignment("32")
    }
}

impl SqlMode {
    pub fn from_assignment(value: &str) -> Self {
        let consumed_bits = value.split(',').map(str::trim).fold(0, |bits, item| {
            let code = match item.to_ascii_uppercase().as_str() {
                "ONLY_FULL_GROUP_BY" => ONLY_FULL_GROUP_BY,
                "ALLOW_THROW_EXCEPTION" => ALLOW_THROW_EXCEPTION,
                "GROUP_CONCAT_LEGACY" => GROUP_CONCAT_LEGACY,
                "ERROR_IF_OVERFLOW" => ERROR_IF_OVERFLOW,
                _ => item.parse::<u64>().unwrap_or(0) & CONSUMED_MASK,
            };
            bits | code
        });
        Self {
            assignment: value.to_owned(),
            consumed_bits,
        }
    }

    pub fn assignment(&self) -> &str {
        &self.assignment
    }

    pub const fn group_concat_legacy(&self) -> bool {
        self.consumed_bits & GROUP_CONCAT_LEGACY != 0
    }

    pub const fn allow_throw_exception(&self) -> bool {
        self.consumed_bits & ALLOW_THROW_EXCEPTION != 0
    }

    pub const fn error_if_overflow(&self) -> bool {
        self.consumed_bits & ERROR_IF_OVERFLOW != 0
    }

    pub const fn decimal_overflow_policy(&self) -> novarocks_type_contract::DecimalOverflowPolicy {
        if self.error_if_overflow() {
            novarocks_type_contract::DecimalOverflowPolicy::ReportError
        } else {
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull
        }
    }

    /// Interpret a parser-admitted value without evaluating arbitrary SQL.
    /// Unconsumed expressions remain intact, as they were before this feature.
    pub fn from_expression(value: &ast::Expr) -> Self {
        let value = match value {
            ast::Expr::Literal(literal) => match &literal.kind {
                ast::LiteralKind::String(value) | ast::LiteralKind::Decimal(value) => value.clone(),
                ast::LiteralKind::Long { parsed_value, .. } => parsed_value.to_string(),
                ast::LiteralKind::Double(value) => value.get().to_string(),
                _ => novarocks_parser::printer::print_expr(value),
            },
            ast::Expr::Identifier(name) => name.value.clone(),
            ast::Expr::LegacyParenthesized(nested) => {
                return Self::from_expression(&nested.expression);
            }
            _ => novarocks_parser::printer::print_expr(value),
        };
        Self::from_assignment(&value)
    }
}

pub const DEFAULT_RECURSIVE_CTE_MAX_DEPTH: i32 = 5;

/// Immutable SQL semantics frozen once for a connection or statement.
/// Each field keeps its SQL owner; assigning one setting preserves the rest.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SqlSemanticSettings {
    sql_mode: SqlMode,
    decimal_overflow_to_double: bool,
    /// Raw value projected from the actual admitted SessionExecutionSettings.
    group_concat_max_len: Option<i64>,
    recursive_cte_max_depth: Option<i32>,
}

impl SqlSemanticSettings {
    pub const fn recursive_cte_max_depth(&self) -> Option<i32> {
        self.recursive_cte_max_depth
    }

    pub fn with_recursive_cte_max_depth(mut self, value: i32) -> Self {
        self.recursive_cte_max_depth = Some(value);
        self
    }
    pub const fn sql_mode(&self) -> &SqlMode {
        &self.sql_mode
    }

    pub const fn decimal_overflow_to_double(&self) -> bool {
        self.decimal_overflow_to_double
    }

    pub const fn group_concat_max_len(&self) -> Option<i64> {
        self.group_concat_max_len
    }
    pub fn with_group_concat_max_len(mut self, value: i64) -> Self {
        self.group_concat_max_len = Some(value);
        self
    }

    pub fn with_decimal_overflow_to_double(mut self, value: bool) -> Self {
        self.decimal_overflow_to_double = value;
        self
    }

    pub fn with_sql_mode(mut self, value: SqlMode) -> Self {
        self.sql_mode = value;
        self
    }
}

fn decimal_flag(value: &ast::Expr) -> Result<bool, crate::analyze_error::AnalyzeError> {
    let text = match value {
        ast::Expr::Literal(literal) => match &literal.kind {
            ast::LiteralKind::Boolean(value) => return Ok(*value),
            ast::LiteralKind::String(value) => value.as_str(),
            ast::LiteralKind::Long {
                parsed_value: 1, ..
            } => "1",
            ast::LiteralKind::Long {
                parsed_value: 0, ..
            } => "0",
            _ => "",
        },
        ast::Expr::Identifier(name) => name.value.as_str(),
        ast::Expr::LegacyParenthesized(nested) => return decimal_flag(&nested.expression),
        _ => "",
    };
    match text.trim().to_ascii_lowercase().as_str() {
        "1" | "on" | "true" => Ok(true),
        "0" | "off" | "false" => Ok(false),
        _ => Err(crate::analyze_error::AnalyzeError::invalid_argument(
            "invalid decimal_overflow_to_double value; expected 1, ON, TRUE, 0, OFF, or FALSE",
            value.span(),
        )),
    }
}

/// Resolve the root query's assignment facts once for the whole query.
/// SELECT, CTE and subquery analysis inherit this frozen setting.
pub fn query_sql_semantics(
    session: &SqlSemanticSettings,
    query: &ast::Query,
) -> Result<SqlSemanticSettings, crate::analyze_error::AnalyzeError> {
    let mut settings = session.clone();
    for property in &query.session_properties {
        let [name] = property.name.parts.as_slice() else {
            continue;
        };
        if name.value.eq_ignore_ascii_case("sql_mode") {
            settings = settings.with_sql_mode(SqlMode::from_expression(&property.value));
        } else if name
            .value
            .eq_ignore_ascii_case("decimal_overflow_to_double")
        {
            settings = settings.with_decimal_overflow_to_double(decimal_flag(&property.value)?);
        }
    }
    Ok(settings)
}

/// Freeze statement-local overrides at admission for every SQL query producer.
/// This returns an owned setting and never mutates connection state.
pub fn statement_sql_semantics(
    session: &SqlSemanticSettings,
    statement: &ast::Statement,
) -> Result<SqlSemanticSettings, crate::analyze_error::AnalyzeError> {
    let query: Option<&ast::Query> = match statement {
        ast::Statement::Query(query) => Some(query),
        ast::Statement::ExplainQuery(explain) => Some(&explain.query),
        ast::Statement::Dml(ast::DmlStatement::Insert(insert)) => Some(&insert.source),
        ast::Statement::Dml(ast::DmlStatement::CreateTableAsSelect(ctas)) => Some(&ctas.query),
        _ => None,
    };
    query.map_or_else(
        || Ok(session.clone()),
        |query| query_sql_semantics(session, query),
    )
}

/// Canonicalize the existing concat aliases to their declared ordinary args.
/// STRING_AGG owns its final separator, making repeated preparation idempotent.
pub(crate) fn normalize_concat_query(
    query: ast::Query,
    session: &SqlSemanticSettings,
) -> Result<ast::Query, crate::analyze_error::AnalyzeError> {
    struct Normalizer {
        sql_semantics: SqlSemanticSettings,
    }
    impl Fold for Normalizer {
        fn fold_function_call(&mut self, call: ast::FunctionCall) -> ast::FunctionCall {
            let mut call = ast::fold_function_call(self, call);
            if call.arguments.is_empty()
                || call.processing_mode.is_some()
                || call
                    .arguments
                    .iter()
                    .any(|argument| argument.name.is_some())
            {
                return call;
            }
            let [name] = call.name.parts.as_mut_slice() else {
                return call;
            };
            if name.quoted {
                return call;
            }
            let separator = if name.value.eq_ignore_ascii_case("group_concat") {
                let legacy = self.sql_semantics.sql_mode().group_concat_legacy();
                name.value = "string_agg".to_owned();
                name.span = novarocks_parser::SYNTHETIC;
                call.name.span = novarocks_parser::SYNTHETIC;
                if legacy && call.arguments.len() > 1 {
                    None
                } else {
                    Some(if legacy { ", " } else { "," })
                }
            } else if name.value.eq_ignore_ascii_case("string_agg") && call.arguments.len() == 1 {
                Some(",")
            } else {
                None
            };
            if let Some(separator) = separator {
                let value = ast::Expr::Literal(ast::Literal {
                    kind: ast::LiteralKind::String(separator.to_string()),
                    span: call.span,
                });
                call.arguments
                    .push(novarocks_parser::build::call_argument(value, None));
            }
            call
        }
    }
    let mut normalizer = Normalizer {
        sql_semantics: query_sql_semantics(session, &query)?,
    };
    Ok(normalizer.fold_query(query))
}

/// Persisted definitions can contain a SELECT under VALUES or a CTE.
fn query_has_select(query: &ast::Query) -> bool {
    use ast::Visit;
    struct Detector {
        has_select: bool,
    }
    impl Visit for Detector {
        fn visit_session_property(&mut self, _: &ast::SessionProperty) {}
        fn visit_select(&mut self, select: &ast::Select) {
            self.has_select = true;
            ast::walk_select(self, select);
        }
    }
    let mut detector = Detector { has_select: false };
    detector.visit_query(query);
    detector.has_select
}

/// Detect a numeric policy used by this query's SELECTs under one setting.
pub fn query_uses_decimal_overflow_to_double(
    session: &SqlSemanticSettings,
    query: &ast::Query,
) -> Result<bool, crate::analyze_error::AnalyzeError> {
    let settings = query_sql_semantics(session, query)?;
    Ok(settings.decimal_overflow_to_double() && query_has_select(query))
}

/// Detect uncaptured legacy mode under the root query setting.
pub fn query_uses_group_concat_legacy(
    session: &SqlSemanticSettings,
    query: &ast::Query,
) -> Result<bool, crate::analyze_error::AnalyzeError> {
    let settings = query_sql_semantics(session, query)?;
    Ok(settings.sql_mode().group_concat_legacy() && query_has_select(query))
}

/// Detect uncaptured strict overflow policy under the root query setting.
pub fn query_uses_error_if_overflow(
    session: &SqlSemanticSettings,
    query: &ast::Query,
) -> Result<bool, crate::analyze_error::AnalyzeError> {
    let settings = query_sql_semantics(session, query)?;
    Ok(settings.sql_mode().error_if_overflow() && query_has_select(query))
}

/// A v1 definition owns namespaces but does not capture SQL semantics.
/// The borrowed caller snapshot is a consumer fact, never durable metadata.
pub fn validate_persisted_query_semantics(
    query: &ast::Query,
    caller: &SqlSemanticSettings,
) -> Result<(), String> {
    check_persisted_query_semantics(query, caller).map_err(|error| match error {
        PersistedSemanticsError::Analyze(error) => error.to_string(),
        PersistedSemanticsError::Uncaptured(message) => format!("Unsupported: {message}"),
    })
}

pub(crate) fn validate_persisted_query_semantics_typed(
    query: &ast::Query,
    caller: &SqlSemanticSettings,
) -> Result<(), crate::analyze_error::AnalyzeError> {
    check_persisted_query_semantics(query, caller).map_err(|error| match error {
        PersistedSemanticsError::Analyze(error) => error,
        PersistedSemanticsError::Uncaptured(message) => {
            crate::analyze_error::AnalyzeError::unsupported_expression_without_span(message)
        }
    })
}

enum PersistedSemanticsError {
    Analyze(crate::analyze_error::AnalyzeError),
    Uncaptured(&'static str),
}

fn check_persisted_query_semantics(
    query: &ast::Query,
    caller: &SqlSemanticSettings,
) -> Result<(), PersistedSemanticsError> {
    if caller.sql_mode().group_concat_legacy()
        || query_uses_group_concat_legacy(caller, query)
            .map_err(PersistedSemanticsError::Analyze)?
    {
        return Err(PersistedSemanticsError::Uncaptured(
            "GROUP_CONCAT_LEGACY is not captured for persisted VIEW or MATERIALIZED VIEW definition replay",
        ));
    }
    if caller.decimal_overflow_to_double()
        || query_uses_decimal_overflow_to_double(caller, query)
            .map_err(PersistedSemanticsError::Analyze)?
    {
        return Err(PersistedSemanticsError::Uncaptured(
            "decimal_overflow_to_double=true is not captured for persisted VIEW or MATERIALIZED VIEW definition replay",
        ));
    }
    if caller.sql_mode().error_if_overflow()
        || query_uses_error_if_overflow(caller, query).map_err(PersistedSemanticsError::Analyze)?
    {
        return Err(PersistedSemanticsError::Uncaptured(
            "ERROR_IF_OVERFLOW is not captured for persisted VIEW or MATERIALIZED VIEW definition replay",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn semantics(value: &str) -> SqlSemanticSettings {
        SqlSemanticSettings::default().with_sql_mode(SqlMode::from_assignment(value))
    }

    fn query(sql: &str) -> ast::Query {
        let mut statements = novarocks_parser::parse(sql).expect("fixture parses");
        let ast::Statement::Query(query) = statements.remove(0) else {
            panic!("query");
        };
        query
    }

    // Author query-level facts explicitly; UNION branches never grant them.
    fn query_with_assignments(sql: &str, assignments: &str) -> ast::Query {
        let mut value = query(sql);
        let assignment_source = query(&format!("SELECT /*+ SET_VAR({assignments}) */ 1"));
        assert!(!assignment_source.session_properties.is_empty());
        value.session_properties = assignment_source.session_properties;
        value
    }

    fn calls(query: &ast::Query) -> Vec<ast::FunctionCall> {
        struct Collector(Vec<ast::FunctionCall>);
        impl ast::Visit for Collector {
            fn visit_function_call(&mut self, call: &ast::FunctionCall) {
                if matches!(
                    call.name
                        .parts
                        .last()
                        .map(|name| name.value.to_ascii_lowercase())
                        .as_deref(),
                    Some("group_concat" | "string_agg")
                ) {
                    self.0.push(call.clone());
                }
                ast::walk_function_call(self, call);
            }
        }
        let mut collector = Collector(Vec::new());
        ast::Visit::visit_query(&mut collector, query);
        collector.0
    }

    fn display(sql: &str, mode: &str) -> String {
        let query = normalize_concat_query(query(sql), &semantics(mode)).unwrap();
        let calls = calls(&query);
        crate::analyzer::display_expr_for_test(&ast::Expr::FunctionCall(ast::AstBox::new(
            calls[0].clone(),
        )))
    }

    #[test]
    fn persisted_definition_detector_uses_one_query_setting() {
        let legacy = semantics("GROUP_CONCAT_LEGACY");
        let modern = semantics("32");
        let original_legacy = legacy.clone();
        let original_modern = modern.clone();
        for sql in [
            "SELECT 1",
            "WITH c AS (SELECT 1) SELECT * FROM c",
            "SELECT * FROM (SELECT 1) d",
            "SELECT 1 UNION ALL SELECT 2",
            "SELECT (SELECT 1)",
        ] {
            assert!(query_uses_group_concat_legacy(&legacy, &query(sql)).unwrap());
            assert!(!query_uses_group_concat_legacy(&modern, &query(sql)).unwrap());
            let reset = query_with_assignments(sql, "sql_mode=32");
            assert!(!query_uses_group_concat_legacy(&legacy, &reset).unwrap());
            let enabled = query_with_assignments(sql, "sql_mode='GROUP_CONCAT_LEGACY'");
            assert!(query_uses_group_concat_legacy(&modern, &enabled).unwrap());
        }
        assert_eq!(legacy, original_legacy);
        assert_eq!(modern, original_modern);
    }

    #[test]
    fn persisted_replay_rejects_legacy_caller_and_stored_legacy_hint() {
        let modern = semantics("32");
        let legacy = semantics("GROUP_CONCAT_LEGACY");
        assert!(validate_persisted_query_semantics(&query("SELECT 1"), &modern).is_ok());
        assert!(
            validate_persisted_query_semantics(&query("SELECT 1"), &legacy)
                .unwrap_err()
                .starts_with("Unsupported:")
        );
        let stored = query("SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ 1");
        assert!(validate_persisted_query_semantics(&stored, &modern).is_err());
        assert_eq!(modern.sql_mode().assignment(), "32");
        assert_eq!(legacy.sql_mode().assignment(), "GROUP_CONCAT_LEGACY");
    }

    #[test]
    fn combines_consumed_tokens_and_preserves_unconsumed_assignment() {
        for setting in [
            "GROUP_CONCAT_LEGACY",
            "68719476736",
            "32, group_concat_legacy",
            "GROUP_CONCAT_LEGACY,512,ERROR_IF_OVERFLOW,STRUCT_CAST_BY_NAME",
        ] {
            let mode = SqlMode::from_assignment(setting);
            assert!(mode.group_concat_legacy());
            assert_eq!(mode.assignment(), setting);
        }
        for setting in [
            "",
            "0",
            "32",
            "ERROR_IF_OVERFLOW",
            "STRUCT_CAST_BY_NAME",
            "1",
            "FUTURE_MODE",
            "18446744073709551615_BAD",
            "-1",
            "NOT_GROUP_CONCAT_LEGACY",
            "34359738368",
        ] {
            let mode = SqlMode::from_assignment(setting);
            assert!(!mode.group_concat_legacy());
            assert_eq!(mode.assignment(), setting);
        }
        assert!(SqlMode::from_assignment("512").allow_throw_exception());
        assert!(!SqlMode::from_assignment("NOT_ALLOW_THROW_EXCEPTION").allow_throw_exception());
    }

    #[test]
    fn last_sql_mode_assignment_wins_without_mutating_session() {
        let session = semantics("GROUP_CONCAT_LEGACY,ALLOW_THROW_EXCEPTION");
        for sql in [
            "SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ /*+ SET_VAR(sql_mode=32) */ 1",
            "SELECT /*+ SET_VAR('sql_mode'='68719476736', sql_mode='32') */ 1",
        ] {
            let effective = query_sql_semantics(&session, &query(sql)).unwrap();
            assert!(!effective.sql_mode().group_concat_legacy());
            assert!(!effective.sql_mode().allow_throw_exception());
        }
        let effective = query_sql_semantics(
            &SqlSemanticSettings::default(),
            &query("SELECT /*+ SET_VAR(sql_mode=68719477248) */ /*+ SET_VAR(query_timeout=1) */ 1"),
        )
        .unwrap();
        assert!(effective.sql_mode().group_concat_legacy());
        assert!(effective.sql_mode().allow_throw_exception());
        assert!(session.sql_mode().group_concat_legacy());
        assert!(session.sql_mode().allow_throw_exception());
        for value in [
            "GROUP_CONCAT_LEGACY",
            "'GROUP_CONCAT_LEGACY, ONLY_full_group_by'",
            "'68719476768'",
            "68719476768",
        ] {
            assert!(
                query_sql_semantics(
                    &SqlSemanticSettings::default(),
                    &query(&format!("SELECT /*+ SET_VAR('sql_mode'={value}) */ 1"))
                )
                .unwrap()
                .sql_mode()
                .group_concat_legacy()
            );
        }
    }

    #[test]
    fn separator_normalization_is_idempotent_and_display_uses_its_contract() {
        assert_eq!(
            display("SELECT group_concat(v,'-' ORDER BY id)", "32"),
            "group_concat(v,'-' ORDER BY id ASC SEPARATOR ',')"
        );
        assert_eq!(
            display(
                "SELECT group_concat(v,'-' ORDER BY id)",
                "GROUP_CONCAT_LEGACY"
            ),
            "group_concat(v ORDER BY id ASC SEPARATOR '-')"
        );
        assert_eq!(
            display("SELECT group_concat(v ORDER BY id)", "GROUP_CONCAT_LEGACY"),
            "group_concat(v ORDER BY id ASC SEPARATOR ', ')"
        );
        for mode in ["32", "GROUP_CONCAT_LEGACY"] {
            assert_eq!(
                display("SELECT group_concat(v,'-' ORDER BY id SEPARATOR '|')", mode),
                "group_concat(v,'-' ORDER BY id ASC SEPARATOR '|')"
            );
            assert_eq!(
                display("SELECT string_agg(v,sep ORDER BY id)", mode),
                "group_concat(v ORDER BY id ASC SEPARATOR sep)"
            );
            let once =
                normalize_concat_query(query("SELECT group_concat(v,'-')"), &semantics(mode))
                    .unwrap();
            assert_eq!(
                normalize_concat_query(once.clone(), &semantics(mode)).unwrap(),
                once
            );
        }
        assert_eq!(
            display(
                "SELECT group_concat(v,u,sep ORDER BY 1,2)",
                "GROUP_CONCAT_LEGACY"
            ),
            "group_concat(v,u ORDER BY v ASC, u ASC SEPARATOR sep)"
        );
    }

    #[test]
    fn canonical_concat_alias_preserves_source_spans_and_skips_quoted_qualified_and_zero_calls() {
        for mode in ["32", "GROUP_CONCAT_LEGACY"] {
            let settings = semantics(mode);
            let raw = query("SELECT group_concat(v,u)");
            let original = calls(&raw).remove(0);
            let prepared = normalize_concat_query(raw, &settings).unwrap();
            let call = calls(&prepared).remove(0);
            assert_eq!(call.name.parts[0].value, "string_agg");
            assert_eq!(call.name.span, novarocks_parser::SYNTHETIC);
            assert_eq!(call.name.parts[0].span, novarocks_parser::SYNTHETIC);
            assert_eq!(call.span, original.span);
            assert_eq!(
                &call.arguments[..original.arguments.len()],
                original.arguments.as_slice()
            );
            assert_eq!(
                normalize_concat_query(prepared.clone(), &settings).unwrap(),
                prepared
            );
            assert_eq!(
                display("SELECT string_agg(v)", mode),
                "group_concat(v SEPARATOR ',')"
            );
            for source in [
                "SELECT group_concat()",
                "SELECT string_agg()",
                "SELECT schema.group_concat(v,u)",
                "SELECT schema.string_agg(v,u)",
            ] {
                let original = query(source);
                assert_eq!(
                    normalize_concat_query(original.clone(), &settings).unwrap(),
                    original
                );
            }
            for source in ["SELECT group_concat(v,u)", "SELECT string_agg(v,u)"] {
                let mut quoted = query(source);
                let ast::SetExpr::Select(select) = quoted.body.as_mut() else {
                    panic!("select");
                };
                let ast::SelectItem::SingleColumn(projection_column) = &mut select.projection[0]
                else {
                    panic!("call");
                };
                assert!(projection_column.alias.is_none(), "anonymous projection");
                let ast::Expr::FunctionCall(call) = &mut projection_column.expression else {
                    panic!("call");
                };
                call.name.parts[0].quoted = true;
                assert_eq!(
                    normalize_concat_query(quoted.clone(), &settings).unwrap(),
                    quoted
                );
            }
        }
    }

    #[test]
    fn statement_admission_freezes_select_explain_insert_and_ctas_overrides() {
        let session = semantics("GROUP_CONCAT_LEGACY,ALLOW_THROW_EXCEPTION");
        for sql in [
            "SELECT /*+ SET_VAR(sql_mode=32) */ group_concat('a','-')",
            "EXPLAIN SELECT /*+ SET_VAR(sql_mode=32) */ group_concat('a','-')",
            "INSERT INTO t SELECT /*+ SET_VAR(sql_mode=32) */ group_concat('a','-')",
            "CREATE TABLE t AS SELECT /*+ SET_VAR(sql_mode=32) */ group_concat('a','-')",
        ] {
            let statements = novarocks_parser::parse(sql).expect("statement parses");
            let effective = statement_sql_semantics(&session, &statements[0]).unwrap();
            assert!(!effective.sql_mode().group_concat_legacy());
            assert!(!effective.sql_mode().allow_throw_exception());
        }
        assert!(session.sql_mode().group_concat_legacy());
        assert!(session.sql_mode().allow_throw_exception());
    }

    #[test]
    fn concat_calls_in_ctes_subqueries_and_union_inherit_query_policy() {
        fn assert_calls(sql: &str, mode: &str, counts: &[usize], separators: &[&str]) {
            let caller = SqlSemanticSettings::default();
            let original_caller = caller.clone();
            let query = query_with_assignments(sql, &format!("sql_mode='{mode}'"));
            let normalized = normalize_concat_query(query, &caller).unwrap();
            let collected = calls(&normalized);
            assert!(collected.iter().all(|call| call.name.parts.len() == 1
                && !call.name.parts[0].quoted
                && call.name.parts[0].value.eq_ignore_ascii_case("string_agg")
                && call.arguments.len() >= 2));
            assert_eq!(collected.len(), counts.len());
            assert_eq!(
                collected
                    .iter()
                    .map(|call| call.arguments.len())
                    .collect::<Vec<_>>(),
                counts
            );
            let actual = collected
                .iter()
                .map(|call| {
                    novarocks_parser::printer::print_expr(
                        &call.arguments.last().expect("canonical separator").value,
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, separators);
            let effective = query_sql_semantics(&caller, &normalized).unwrap();
            assert_eq!(effective.sql_mode().assignment(), mode);
            assert_eq!(caller, original_caller);
        }
        let nested = "WITH c AS (SELECT group_concat(v) x FROM t) SELECT group_concat(v,'-'), (SELECT group_concat(v) FROM t), (SELECT group_concat(v,'-') FROM t) FROM t";
        assert_calls(nested, "32", &[2, 3, 2, 3], &["','", "','", "','", "','"]);
        assert_calls(
            nested,
            "GROUP_CONCAT_LEGACY",
            &[2, 2, 2, 2],
            &["', '", "'-'", "', '", "'-'"],
        );
        let scalar = "SELECT group_concat(v), (SELECT group_concat(v,'-') FROM t), (SELECT group_concat(v) FROM t) FROM t";
        assert_calls(scalar, "32", &[2, 3, 2], &["','", "','", "','"]);
        assert_calls(
            scalar,
            "GROUP_CONCAT_LEGACY",
            &[2, 2, 2],
            &["', '", "'-'", "', '"],
        );
        let branches = "SELECT group_concat(v) FROM t UNION ALL SELECT group_concat(v,'-') FROM t";
        assert_calls(branches, "32", &[2, 3], &["','", "','"]);
        assert_calls(branches, "GROUP_CONCAT_LEGACY", &[2, 2], &["', '", "'-'"]);
    }

    #[test]
    fn decimal_query_assignments_preserve_mode_and_validate_at_the_root() {
        let session = semantics("GROUP_CONCAT_LEGACY").with_decimal_overflow_to_double(true);
        let original_session = session.clone();
        for value in ["0", "OFF", "false", "'false'"] {
            let resolved = query_sql_semantics(
                &session,
                &query_with_assignments("SELECT 1", &format!("decimal_overflow_to_double={value}")),
            )
            .unwrap();
            assert!(!resolved.decimal_overflow_to_double());
            assert!(resolved.sql_mode().group_concat_legacy());
        }
        let value = query_sql_semantics(&session, &query(
            "SELECT /*+ SET_VAR(sql_mode=32,decimal_overflow_to_double=0) */ /*+ SET_VAR(decimal_overflow_to_double=1) */ 1"
        )).unwrap();
        assert!(value.decimal_overflow_to_double());
        assert!(!value.sql_mode().group_concat_legacy());
        for (sql, value) in [
            ("SELECT 1", "2"),
            ("SELECT 1 FROM (SELECT 1) t", "'bad'"),
            ("WITH t AS (SELECT 1) SELECT 1 FROM t", "1+1"),
            ("SELECT 1 UNION ALL SELECT 1", "NULL"),
        ] {
            let query = query_with_assignments(sql, &format!("decimal_overflow_to_double={value}"));
            let error = normalize_concat_query(query.clone(), &session).unwrap_err();
            assert_eq!(error.code().as_str(), "sql.analyze.invalid_argument");
            assert!(error.span().is_some());
            let error =
                statement_sql_semantics(&session, &ast::Statement::Query(query)).unwrap_err();
            assert_eq!(error.code().as_str(), "sql.analyze.invalid_argument");
            assert!(error.span().is_some());
        }
        assert_eq!(session, original_session);
    }

    #[test]
    fn decimal_replay_rejects_uncaptured_caller_or_query_policy() {
        let ordinary = SqlSemanticSettings::default();
        let promoted = ordinary.clone().with_decimal_overflow_to_double(true);
        let original_ordinary = ordinary.clone();
        let original_promoted = promoted.clone();
        for sql in [
            "SELECT 1",
            "SELECT 1 FROM (SELECT 1) t",
            "WITH t AS (SELECT 1) SELECT 1 FROM t",
            "SELECT 1 UNION ALL SELECT 1",
        ] {
            assert!(validate_persisted_query_semantics(&query(sql), &ordinary).is_ok());
            assert!(
                validate_persisted_query_semantics(&query(sql), &promoted)
                    .unwrap_err()
                    .contains("decimal_overflow_to_double=true is not captured")
            );
            let enabled = query_with_assignments(sql, "decimal_overflow_to_double=true");
            assert!(
                validate_persisted_query_semantics(&enabled, &ordinary)
                    .unwrap_err()
                    .contains("decimal_overflow_to_double=true is not captured")
            );
            let disabled = query_with_assignments(sql, "decimal_overflow_to_double=false");
            assert!(validate_persisted_query_semantics(&disabled, &ordinary).is_ok());
            // A query override cannot make an uncaptured caller a replay context.
            assert!(validate_persisted_query_semantics(&disabled, &promoted).is_err());
        }
        assert_eq!(ordinary, original_ordinary);
        assert_eq!(promoted, original_promoted);
    }

    #[test]
    fn decimal_usage_observes_one_effective_query_setting() {
        let on = SqlSemanticSettings::default().with_decimal_overflow_to_double(true);
        let off = SqlSemanticSettings::default();
        let original_on = on.clone();
        let original_off = off.clone();
        for sql in [
            "SELECT 1",
            "SELECT 1 UNION ALL SELECT 2",
            "WITH c AS (SELECT 1) SELECT * FROM c",
            "SELECT (SELECT 1)",
        ] {
            assert!(query_uses_decimal_overflow_to_double(&on, &query(sql)).unwrap());
            assert!(!query_uses_decimal_overflow_to_double(&off, &query(sql)).unwrap());
            let disabled = query_with_assignments(sql, "decimal_overflow_to_double=false");
            assert!(!query_uses_decimal_overflow_to_double(&on, &disabled).unwrap());
            let enabled = query_with_assignments(sql, "decimal_overflow_to_double=true");
            assert!(query_uses_decimal_overflow_to_double(&off, &enabled).unwrap());
        }
        assert_eq!(on, original_on);
        assert_eq!(off, original_off);
    }

    #[test]
    fn overflow_error_policy_is_independent_from_allow_throw_and_promotion() {
        use novarocks_type_contract::DecimalOverflowPolicy;
        let report = SqlMode::from_assignment("ERROR_IF_OVERFLOW");
        assert!(report.error_if_overflow());
        assert!(!report.allow_throw_exception());
        assert_eq!(
            report.decimal_overflow_policy(),
            DecimalOverflowPolicy::ReportError
        );
        let allow = SqlMode::from_assignment("ALLOW_THROW_EXCEPTION");
        assert!(allow.allow_throw_exception());
        assert!(!allow.error_if_overflow());
        assert_eq!(
            allow.decimal_overflow_policy(),
            DecimalOverflowPolicy::OutputNull
        );
        for mode in [
            "34359738368",
            "ERROR_IF_OVERFLOW,ALLOW_THROW_EXCEPTION",
            "34359738880",
        ] {
            assert!(SqlMode::from_assignment(mode).error_if_overflow());
        }
        let both = SqlSemanticSettings::default()
            .with_decimal_overflow_to_double(true)
            .with_sql_mode(report);
        assert!(both.decimal_overflow_to_double());
        assert!(both.sql_mode().error_if_overflow());
        let reassigned = both.clone().with_sql_mode(allow);
        assert!(reassigned.decimal_overflow_to_double());
        assert!(!reassigned.sql_mode().error_if_overflow());
        assert!(both.sql_mode().error_if_overflow());
    }

    #[test]
    fn overflow_definition_usage_observes_query_policy_and_replay_boundary() {
        let ordinary = SqlSemanticSettings::default();
        let strict = ordinary
            .clone()
            .with_sql_mode(SqlMode::from_assignment("ERROR_IF_OVERFLOW"));
        let original_ordinary = ordinary.clone();
        let original_strict = strict.clone();
        for sql in [
            "SELECT 1",
            "SELECT 1 UNION ALL SELECT 2",
            "WITH c AS (SELECT 1) SELECT * FROM c",
            "SELECT (SELECT 1)",
        ] {
            assert!(!query_uses_error_if_overflow(&ordinary, &query(sql)).unwrap());
            assert!(query_uses_error_if_overflow(&strict, &query(sql)).unwrap());
            let reset = query_with_assignments(sql, "sql_mode=32");
            assert!(!query_uses_error_if_overflow(&strict, &reset).unwrap());
            let enabled = query_with_assignments(sql, "sql_mode='ERROR_IF_OVERFLOW'");
            assert!(query_uses_error_if_overflow(&ordinary, &enabled).unwrap());
            assert!(
                validate_persisted_query_semantics(&enabled, &ordinary)
                    .unwrap_err()
                    .contains("ERROR_IF_OVERFLOW is not captured")
            );
            assert!(validate_persisted_query_semantics(&query(sql), &strict).is_err());
            assert!(validate_persisted_query_semantics(&query(sql), &ordinary).is_ok());
        }
        assert_eq!(ordinary, original_ordinary);
        assert_eq!(strict, original_strict);
    }
}
