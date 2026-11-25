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
//! Unknown modes remain in the original assignment.

use novarocks_parser::ast;
use std::sync::Arc;

const ONLY_FULL_GROUP_BY: u64 = 1 << 5;
const ALLOW_THROW_EXCEPTION: u64 = 1 << 9;
const ERROR_IF_OVERFLOW: u64 = 1 << 35;
const CONSUMED_MASK: u64 = ONLY_FULL_GROUP_BY | ALLOW_THROW_EXCEPTION | ERROR_IF_OVERFLOW;
const DEFAULT_SQL_MODE_BITS: u64 = ONLY_FULL_GROUP_BY;

/// One complete connection or statement setting. Successive assignments
/// replace this value; comma-separated items within an assignment combine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMode {
    assignment: String,
    consumed_bits: u64,
}

impl Default for SqlMode {
    fn default() -> Self {
        Self {
            assignment: DEFAULT_SQL_MODE_BITS.to_string(),
            consumed_bits: DEFAULT_SQL_MODE_BITS,
        }
    }
}

impl SqlMode {
    pub fn from_assignment(value: &str) -> Self {
        let consumed_bits = value.split(',').map(str::trim).fold(0, |bits, item| {
            let code = match item.to_ascii_uppercase().as_str() {
                "ONLY_FULL_GROUP_BY" => ONLY_FULL_GROUP_BY,
                "ALLOW_THROW_EXCEPTION" => ALLOW_THROW_EXCEPTION,

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
    pub fn from_expression(value: &ast::Expr) -> Result<Self, crate::analyze_error::AnalyzeError> {
        fn render_assignment(
            value: &ast::Expr,
        ) -> Result<String, crate::analyze_error::AnalyzeError> {
            novarocks_parser::printer::print_expression(value).map_err(|error| {
                crate::analyze_error::AnalyzeError::invalid_argument(
                    error.to_string(),
                    value.span(),
                )
            })
        }
        let value = match value {
            ast::Expr::Literal(literal) => match &literal.kind {
                ast::LiteralKind::String(value) | ast::LiteralKind::Decimal(value) => value.clone(),
                ast::LiteralKind::Long { parsed_value, .. } => parsed_value.to_string(),
                ast::LiteralKind::Double(value) => value.get().to_string(),
                _ => render_assignment(value)?,
            },
            ast::Expr::Identifier(name) => name.value.clone(),
            _ => render_assignment(value)?,
        };
        Ok(Self::from_assignment(&value))
    }
}

pub const DEFAULT_RECURSIVE_CTE_MAX_DEPTH: i32 = 5;

/// Session expressions read the admitted caller even when resolving names
/// against a stored view's namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatementSessionEnvironment {
    principal: Arc<str>,
    catalog: Option<String>,
    schema: Option<String>,
}

impl StatementSessionEnvironment {
    pub fn new(principal: Arc<str>, catalog: Option<String>, schema: Option<String>) -> Self {
        Self {
            principal,
            catalog,
            schema,
        }
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }

    pub fn catalog(&self) -> Option<&str> {
        self.catalog.as_deref()
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }
}

/// Immutable SQL semantics frozen once for a connection or statement.
/// Each field keeps its SQL owner; assigning one setting preserves the rest.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SqlSemanticSettings {
    // Computation semantics.
    sql_mode: SqlMode,
    recursive_cte_max_depth: Option<i32>,
    // Statement environment.
    statement_time: Option<novarocks_type_contract::StatementTimeEnvironment>,
    statement_session: Option<Arc<StatementSessionEnvironment>>,
}

impl SqlSemanticSettings {
    /// Test the effective computation semantics of an admitted query.
    pub fn has_default_computation_semantics(&self) -> bool {
        let Self {
            sql_mode,
            recursive_cte_max_depth,
            statement_time: _,
            statement_session: _,
        } = self;
        sql_mode.consumed_bits == DEFAULT_SQL_MODE_BITS
            && recursive_cte_max_depth.unwrap_or(DEFAULT_RECURSIVE_CTE_MAX_DEPTH)
                == DEFAULT_RECURSIVE_CTE_MAX_DEPTH
    }

    /// Use the computation fixed for an MV while retaining the statement's
    /// time and caller environment.
    pub fn with_default_computation_semantics(self) -> Self {
        let Self {
            sql_mode: _,
            recursive_cte_max_depth: _,
            statement_time,
            statement_session,
        } = self;
        Self {
            statement_time,
            statement_session,
            ..Self::default()
        }
    }

    pub fn statement_session(&self) -> Option<&StatementSessionEnvironment> {
        self.statement_session.as_deref()
    }

    pub fn with_statement_session(mut self, value: Option<StatementSessionEnvironment>) -> Self {
        self.statement_session = value.map(Arc::new);
        self
    }

    pub const fn statement_time(
        &self,
    ) -> Option<novarocks_type_contract::StatementTimeEnvironment> {
        self.statement_time
    }

    pub fn with_statement_time(
        mut self,
        value: novarocks_type_contract::StatementTimeEnvironment,
    ) -> Self {
        self.statement_time = Some(value);
        self
    }

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

    pub fn with_sql_mode(mut self, value: SqlMode) -> Self {
        self.sql_mode = value;
        self
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
            settings = settings.with_sql_mode(SqlMode::from_expression(&property.value)?);
        }
    }
    Ok(settings)
}

/// Freeze statement-local overrides at admission for every SQL query producer.
/// This returns an owned setting and never mutates connection state.
pub fn statement_sql_semantics(
    session: &SqlSemanticSettings,
    statement: &ast::TrinoStatement,
) -> Result<SqlSemanticSettings, crate::analyze_error::AnalyzeError> {
    let mut source = statement;
    let query = loop {
        match source {
            ast::TrinoStatement::Query(query) => break Some(query),
            ast::TrinoStatement::Explain(explain) => source = &explain.statement,
            ast::TrinoStatement::ExplainAnalyze(explain) => source = &explain.statement,
            ast::TrinoStatement::Insert(insert) => break Some(&*insert.query),
            ast::TrinoStatement::CreateTableAsSelect(ctas) => break Some(&*ctas.query),
            _ => break None,
        }
    };
    query.map_or_else(
        || Ok(session.clone()),
        |query| query_sql_semantics(session, query),
    )
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
        novarocks_parser::parser::parse_query(sql).expect("fixture parses")
    }

    // Author query-level facts explicitly; UNION branches never grant them.
    fn query_with_assignments(sql: &str, assignments: &str) -> ast::Query {
        query(&format!("WITH SESSION {assignments} {sql}"))
    }

    #[test]
    fn combines_consumed_tokens_and_preserves_unconsumed_assignment() {
        for setting in [
            "GROUP_CONCAT_LEGACY",
            "68719476736",
            "32,group_concat_legacy",
            "FUTURE_MODE",
        ] {
            let mode = SqlMode::from_assignment(setting);
            assert_eq!(mode.assignment(), setting);
            assert!(!mode.error_if_overflow());
            assert!(!mode.allow_throw_exception());
        }
        let mode = SqlMode::from_assignment("ERROR_IF_OVERFLOW,512,STRUCT_CAST_BY_NAME");
        assert!(mode.error_if_overflow());
        assert!(mode.allow_throw_exception());
        assert!(SqlMode::from_assignment("512").allow_throw_exception());
        assert!(!SqlMode::from_assignment("NOT_ALLOW_THROW_EXCEPTION").allow_throw_exception());
    }

    #[test]
    fn last_sql_mode_assignment_wins_without_mutating_session() {
        let query = |sql: &str| novarocks_parser::parser::parse_query(sql).expect("fixture parses");
        let session = semantics("ERROR_IF_OVERFLOW,ALLOW_THROW_EXCEPTION");
        for sql in [
            "WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW', sql_mode = 32 SELECT 1",
            "WITH SESSION sql_mode = '68719476736', sql_mode = '32' SELECT 1",
        ] {
            let effective = query_sql_semantics(&session, &query(sql)).unwrap();
            assert!(!effective.sql_mode().allow_throw_exception());
        }
        let effective = query_sql_semantics(
            &SqlSemanticSettings::default(),
            &query("WITH SESSION sql_mode = 34359738880, query_timeout = 1 SELECT 1"),
        )
        .unwrap();
        assert!(effective.sql_mode().allow_throw_exception());
        assert!(session.sql_mode().allow_throw_exception());
        for value in [
            "ERROR_IF_OVERFLOW",
            "'ERROR_IF_OVERFLOW, ONLY_full_group_by'",
            "'34359738400'",
            "34359738400",
        ] {
            assert!(
                query_sql_semantics(
                    &SqlSemanticSettings::default(),
                    &query(&format!("WITH SESSION sql_mode = {value} SELECT 1"))
                )
                .unwrap()
                .sql_mode()
                .error_if_overflow()
            );
        }
    }

    #[test]
    fn mv_computation_eligibility_uses_consumed_modes_and_effective_depth() {
        for assignment in [
            "32",
            "ONLY_FULL_GROUP_BY",
            "32,UNCONSUMED_MODE",
            "ONLY_FULL_GROUP_BY,68719476736",
        ] {
            assert!(semantics(assignment).has_default_computation_semantics());
        }
        for assignment in ["0", "32,ALLOW_THROW_EXCEPTION", "32,ERROR_IF_OVERFLOW"] {
            assert!(!semantics(assignment).has_default_computation_semantics());
        }

        let defaults = SqlSemanticSettings::default();
        assert!(defaults.has_default_computation_semantics());
        assert!(
            defaults
                .clone()
                .with_recursive_cte_max_depth(DEFAULT_RECURSIVE_CTE_MAX_DEPTH)
                .has_default_computation_semantics()
        );
        assert!(
            !defaults
                .with_recursive_cte_max_depth(DEFAULT_RECURSIVE_CTE_MAX_DEPTH + 1)
                .has_default_computation_semantics()
        );
    }

    #[test]
    fn mv_default_computation_preserves_admitted_statement_environment() {
        use novarocks_type_contract::{StatementStartUtc, StatementTimeEnvironment, TimeZoneKey};

        let session = StatementSessionEnvironment::new(
            Arc::from("mv-reader"),
            Some("ice".into()),
            Some("analytics".into()),
        );
        let time = StatementTimeEnvironment {
            start: StatementStartUtc::try_new(1_700_000_000, 0).unwrap(),
            zone: TimeZoneKey::UTC,
        };
        let settings = semantics("32,ALLOW_THROW_EXCEPTION")
            .with_recursive_cte_max_depth(2)
            .with_statement_session(Some(session.clone()))
            .with_statement_time(time)
            .with_default_computation_semantics();

        assert!(settings.has_default_computation_semantics());
        assert_eq!(settings.recursive_cte_max_depth(), None);
        assert_eq!(settings.statement_session(), Some(&session));
        assert_eq!(settings.statement_time(), Some(time));
    }

    #[test]
    fn statement_admission_freezes_root_query_overrides() {
        let session = semantics("ERROR_IF_OVERFLOW,ALLOW_THROW_EXCEPTION");
        for sql in [
            "WITH SESSION sql_mode = 32 SELECT LISTAGG('a', '-') WITHIN GROUP (ORDER BY 1)",
            "EXPLAIN WITH SESSION sql_mode = 32  SELECT LISTAGG('a', '-') WITHIN GROUP (ORDER BY 1)",
            "EXPLAIN ANALYZE WITH SESSION sql_mode = 32 SELECT LISTAGG('a', '-') WITHIN GROUP (ORDER BY 1)",
        ] {
            let statement =
                novarocks_parser::parser::parse_statement(sql).expect("statement parses");
            let effective = statement_sql_semantics(&session, &statement).unwrap();
            assert!(!effective.sql_mode().allow_throw_exception());
        }
        assert!(session.sql_mode().allow_throw_exception());
    }

    #[test]
    fn overflow_error_policy_is_independent_from_allow_throw() {
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
