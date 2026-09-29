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
                ast::LiteralKind::String(value) | ast::LiteralKind::Number(value) => value.clone(),
                _ => novarocks_parser::printer::print_expr(value),
            },
            ast::Expr::Identifier(name) => name.value.clone(),
            ast::Expr::Nested(nested) => return Self::from_expression(&nested.expression),
            _ => novarocks_parser::printer::print_expr(value),
        };
        Self::from_assignment(&value)
    }
}

/// Immutable SQL semantics frozen once for a connection or statement.
/// Each field keeps its SQL owner; assigning one setting preserves the rest.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SqlSemanticSettings {
    sql_mode: SqlMode,
    decimal_overflow_to_double: bool,
}

impl SqlSemanticSettings {
    pub const fn sql_mode(&self) -> &SqlMode {
        &self.sql_mode
    }

    pub const fn decimal_overflow_to_double(&self) -> bool {
        self.decimal_overflow_to_double
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

fn semantic_key(expression: &ast::Expr, key: &str) -> bool {
    match expression {
        ast::Expr::Identifier(name) => name.value.eq_ignore_ascii_case(key),
        ast::Expr::Literal(literal) => matches!(&literal.kind,
            ast::LiteralKind::String(value) if value.eq_ignore_ascii_case(key)),
        _ => false,
    }
}

fn decimal_flag(value: &ast::Expr) -> Result<bool, crate::analyze_error::AnalyzeError> {
    let text = match value {
        ast::Expr::Literal(literal) => match &literal.kind {
            ast::LiteralKind::Boolean(value) => return Ok(*value),
            ast::LiteralKind::String(value) | ast::LiteralKind::Number(value) => value.as_str(),
            _ => "",
        },
        ast::Expr::Identifier(name) => name.value.as_str(),
        ast::Expr::Nested(nested) => return decimal_flag(&nested.expression),
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

pub fn select_sql_semantics(
    session: &SqlSemanticSettings,
    select: &ast::Select,
) -> Result<SqlSemanticSettings, crate::analyze_error::AnalyzeError> {
    let mut settings = session.clone();
    for hint in &select.hints {
        if !hint.name.value.eq_ignore_ascii_case("set_var") {
            continue;
        }
        let ast::SelectHintValue::Call { arguments } = &hint.value else {
            continue;
        };
        for argument in arguments {
            match argument {
                ast::Expr::Binary(binary)
                    if semantic_key(&binary.left, "decimal_overflow_to_double") =>
                {
                    if binary.operator != ast::BinaryOperator::Equal {
                        return Err(crate::analyze_error::AnalyzeError::invalid_argument(
                            "decimal_overflow_to_double hint requires a boolean assignment",
                            argument.span(),
                        ));
                    }
                    settings =
                        settings.with_decimal_overflow_to_double(decimal_flag(&binary.right)?);
                }
                ast::Expr::Binary(binary)
                    if binary.operator == ast::BinaryOperator::Equal
                        && semantic_key(&binary.left, "sql_mode") =>
                {
                    settings = settings.with_sql_mode(SqlMode::from_expression(&binary.right));
                }
                value if semantic_key(value, "decimal_overflow_to_double") => {
                    return Err(crate::analyze_error::AnalyzeError::invalid_argument(
                        "decimal_overflow_to_double hint requires a boolean assignment",
                        value.span(),
                    ));
                }
                _ => {}
            }
        }
    }
    Ok(settings)
}

/// Resolve hints belonging to this query's root SELECT only. Nested SELECTs,
/// CTEs, and set-operation siblings cannot mutate the enclosing statement.
pub fn query_sql_semantics(
    session: &SqlSemanticSettings,
    query: &ast::Query,
) -> Result<SqlSemanticSettings, crate::analyze_error::AnalyzeError> {
    let mut body = query.body.as_ref();
    while let ast::SetExpr::Query(query) = body {
        body = query.body.as_ref();
    }
    match body {
        ast::SetExpr::Select(select) => select_sql_semantics(session, select),
        _ => Ok(session.clone()),
    }
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
        |query| {
            // Validate every lexical override before frontend View expansion,
            // whose existing port returns String rather than typed errors.
            query_uses_decimal_overflow_to_double(session, query)?;
            query_sql_semantics(session, query)
        },
    )
}

/// Make separator ownership explicit once, before name/type resolution.
/// Explicit SEPARATOR makes this pass idempotent across compiler continuations
/// and analyzer preparation. STRING_AGG owns its positional separator contract
/// independently of GROUP_CONCAT's modern/legacy choice.
pub(crate) fn normalize_concat_query(
    query: ast::Query,
    session: &SqlSemanticSettings,
) -> Result<ast::Query, crate::analyze_error::AnalyzeError> {
    struct Normalizer {
        sql_semantics: SqlSemanticSettings,
        error: Option<crate::analyze_error::AnalyzeError>,
    }
    impl Fold for Normalizer {
        fn fold_query(&mut self, query: ast::Query) -> ast::Query {
            let enclosing = self.sql_semantics.clone();
            if self.error.is_some() {
                return query;
            }
            self.sql_semantics = match query_sql_semantics(&enclosing, &query) {
                Ok(settings) => settings,
                Err(error) => {
                    self.error = Some(error);
                    return query;
                }
            };
            let query = ast::fold_query(self, query);
            self.sql_semantics = enclosing;
            query
        }

        fn fold_select(&mut self, select: ast::Select) -> ast::Select {
            let enclosing = self.sql_semantics.clone();
            if self.error.is_some() {
                return select;
            }
            self.sql_semantics = match select_sql_semantics(&enclosing, &select) {
                Ok(settings) => settings,
                Err(error) => {
                    self.error = Some(error);
                    return select;
                }
            };
            let select = ast::fold_select(self, select);
            self.sql_semantics = enclosing;
            select
        }

        fn fold_function_call(&mut self, call: ast::FunctionCall) -> ast::FunctionCall {
            let mut call = ast::fold_function_call(self, call);
            let name = call
                .name
                .parts
                .last()
                .map(|name| name.value.to_ascii_lowercase());
            if call.separator.is_some() || call.arguments.is_empty() {
                return call;
            }
            let positional_separator = match name.as_deref() {
                Some("group_concat") => self.sql_semantics.sql_mode().group_concat_legacy(),
                Some("string_agg") => true,
                _ => return call,
            };
            let separator = if positional_separator && call.arguments.len() > 1 {
                call.arguments
                    .pop()
                    .expect("at least two positional arguments")
            } else {
                ast::Expr::Literal(ast::Literal {
                    kind: ast::LiteralKind::String(
                        if name.as_deref() == Some("group_concat")
                            && self.sql_semantics.sql_mode().group_concat_legacy()
                        {
                            ", "
                        } else {
                            ","
                        }
                        .to_string(),
                    ),
                    span: call.span,
                })
            };
            call.separator = Some(Box::new(separator));
            call
        }
    }
    let mut normalizer = Normalizer {
        sql_semantics: session.clone(),
        error: None,
    };
    let query = normalizer.fold_query(query);
    match normalizer.error {
        Some(error) => Err(error),
        None => Ok(query),
    }
}

/// Detect the effective numeric semantic setting in every definition scope.
/// Persistent definitions do not yet capture this setting for reanalysis.
pub fn query_uses_decimal_overflow_to_double(
    session: &SqlSemanticSettings,
    query: &ast::Query,
) -> Result<bool, crate::analyze_error::AnalyzeError> {
    struct Detector {
        settings: SqlSemanticSettings,
        uses: bool,
        error: Option<crate::analyze_error::AnalyzeError>,
    }
    impl Fold for Detector {
        fn fold_query(&mut self, query: ast::Query) -> ast::Query {
            if self.error.is_some() {
                return query;
            }
            let enclosing = self.settings.clone();
            match query_sql_semantics(&enclosing, &query) {
                Ok(settings) => self.settings = settings,
                Err(error) => {
                    self.error = Some(error);
                    return query;
                }
            }
            let query = ast::fold_query(self, query);
            self.settings = enclosing;
            query
        }
        fn fold_select(&mut self, select: ast::Select) -> ast::Select {
            if self.error.is_some() {
                return select;
            }
            let enclosing = self.settings.clone();
            match select_sql_semantics(&enclosing, &select) {
                Ok(settings) => self.settings = settings,
                Err(error) => {
                    self.error = Some(error);
                    return select;
                }
            }
            self.uses |= self.settings.decimal_overflow_to_double();
            let select = ast::fold_select(self, select);
            self.settings = enclosing;
            select
        }
    }
    let mut detector = Detector {
        settings: session.clone(),
        uses: false,
        error: None,
    };
    detector.fold_query(query.clone());
    match detector.error {
        Some(error) => Err(error),
        None => Ok(detector.uses),
    }
}

/// Detect uncaptured legacy mode at each actual SELECT in lexical scope.
pub fn query_uses_group_concat_legacy(
    session: &SqlSemanticSettings,
    query: &ast::Query,
) -> Result<bool, crate::analyze_error::AnalyzeError> {
    struct Detector {
        settings: SqlSemanticSettings,
        uses: bool,
        error: Option<crate::analyze_error::AnalyzeError>,
    }
    impl Fold for Detector {
        fn fold_query(&mut self, query: ast::Query) -> ast::Query {
            if self.error.is_some() {
                return query;
            }
            let enclosing = self.settings.clone();
            match query_sql_semantics(&enclosing, &query) {
                Ok(settings) => self.settings = settings,
                Err(error) => {
                    self.error = Some(error);
                    return query;
                }
            }
            let query = ast::fold_query(self, query);
            self.settings = enclosing;
            query
        }
        fn fold_select(&mut self, select: ast::Select) -> ast::Select {
            if self.error.is_some() {
                return select;
            }
            let enclosing = self.settings.clone();
            match select_sql_semantics(&enclosing, &select) {
                Ok(settings) => self.settings = settings,
                Err(error) => {
                    self.error = Some(error);
                    return select;
                }
            }
            self.uses |= self.settings.sql_mode().group_concat_legacy();
            let select = ast::fold_select(self, select);
            self.settings = enclosing;
            select
        }
    }
    let mut detector = Detector {
        settings: session.clone(),
        uses: false,
        error: None,
    };
    detector.fold_query(query.clone());
    match detector.error {
        Some(error) => Err(error),
        None => Ok(detector.uses),
    }
}

/// Detect uncaptured ERROR_IF_OVERFLOW at each actual SELECT in lexical scope.
pub fn query_uses_error_if_overflow(
    session: &SqlSemanticSettings,
    query: &ast::Query,
) -> Result<bool, crate::analyze_error::AnalyzeError> {
    struct Detector {
        settings: SqlSemanticSettings,
        uses: bool,
        error: Option<crate::analyze_error::AnalyzeError>,
    }
    impl Fold for Detector {
        fn fold_query(&mut self, query: ast::Query) -> ast::Query {
            if self.error.is_some() {
                return query;
            }
            let enclosing = self.settings.clone();
            match query_sql_semantics(&enclosing, &query) {
                Ok(settings) => self.settings = settings,
                Err(error) => {
                    self.error = Some(error);
                    return query;
                }
            }
            let query = ast::fold_query(self, query);
            self.settings = enclosing;
            query
        }
        fn fold_select(&mut self, select: ast::Select) -> ast::Select {
            if self.error.is_some() {
                return select;
            }
            let enclosing = self.settings.clone();
            match select_sql_semantics(&enclosing, &select) {
                Ok(settings) => self.settings = settings,
                Err(error) => {
                    self.error = Some(error);
                    return select;
                }
            }
            self.uses |= self.settings.sql_mode().error_if_overflow();
            let select = ast::fold_select(self, select);
            self.settings = enclosing;
            select
        }
    }
    let mut detector = Detector {
        settings: session.clone(),
        uses: false,
        error: None,
    };
    detector.fold_query(query.clone());
    match detector.error {
        Some(error) => Err(error),
        None => Ok(detector.uses),
    }
}

/// A v1 definition owns namespaces but does not capture SQL semantics.
/// The borrowed caller snapshot is a consumer fact, never durable metadata.
pub fn validate_persisted_query_semantics(
    query: &ast::Query,
    caller: &SqlSemanticSettings,
) -> Result<(), String> {
    if caller.sql_mode().group_concat_legacy()
        || query_uses_group_concat_legacy(caller, query).map_err(|error| error.to_string())?
    {
        return Err("Unsupported: GROUP_CONCAT_LEGACY is not captured for persisted VIEW or MATERIALIZED VIEW definition replay".to_string());
    }
    if caller.decimal_overflow_to_double()
        || query_uses_decimal_overflow_to_double(caller, query)
            .map_err(|error| error.to_string())?
    {
        return Err("Unsupported: decimal_overflow_to_double=true is not captured for persisted VIEW or MATERIALIZED VIEW definition replay".to_string());
    }
    if caller.sql_mode().error_if_overflow()
        || query_uses_error_if_overflow(caller, query).map_err(|error| error.to_string())?
    {
        return Err("Unsupported: ERROR_IF_OVERFLOW is not captured for persisted VIEW or MATERIALIZED VIEW definition replay".to_string());
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
        crate::analyzer::display_expr_for_test(&ast::Expr::FunctionCall(calls[0].clone()))
    }

    #[test]
    fn persisted_definition_detector_respects_actual_select_scopes() {
        let legacy = semantics("GROUP_CONCAT_LEGACY");
        let modern = semantics("32");
        for sql in [
            "SELECT 1",
            "WITH c AS (SELECT 1) SELECT * FROM c",
            "SELECT * FROM (SELECT 1) d",
            "SELECT 1 UNION ALL SELECT 2",
        ] {
            assert!(query_uses_group_concat_legacy(&legacy, &query(sql)).unwrap());
            assert!(!query_uses_group_concat_legacy(&modern, &query(sql)).unwrap());
        }
        for sql in [
            "SELECT /*+ SET_VAR(sql_mode=32) */ 1",
            "SELECT /*+ SET_VAR(sql_mode=32) */ 1 UNION ALL SELECT /*+ SET_VAR(sql_mode=32) */ 2",
        ] {
            assert!(!query_uses_group_concat_legacy(&legacy, &query(sql)).unwrap());
        }
        for sql in [
            "WITH c AS (SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ 1) SELECT * FROM c",
            "SELECT * FROM (SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ 1) d",
            "SELECT 1 UNION ALL SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ 2",
            "SELECT (SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ 1)",
        ] {
            assert!(query_uses_group_concat_legacy(&modern, &query(sql)).unwrap());
        }
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
    fn nested_and_cte_hints_shadow_without_leaking_to_siblings_or_parent() {
        let normalized = normalize_concat_query(
            query(
                "WITH c AS (SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ group_concat(v) x FROM t) \
             SELECT group_concat(v,'-'), \
                (SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ group_concat(v) FROM t), \
                (SELECT group_concat(v,'-') FROM t) FROM t",
            ),
            &SqlSemanticSettings::default(),
        ).unwrap();
        let collected = calls(&normalized);
        assert_eq!(collected.len(), 4);
        assert_eq!(
            collected
                .iter()
                .map(|call| call.arguments.len())
                .collect::<Vec<_>>(),
            vec![1, 2, 1, 2]
        );
        let separators = collected
            .iter()
            .map(|call| {
                novarocks_parser::printer::print_expr(
                    call.separator.as_deref().expect("explicit separator"),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(separators, vec!["', '", "','", "', '", "','"]);
        let parent = query_sql_semantics(&SqlSemanticSettings::default(), &normalized).unwrap();
        assert!(!parent.sql_mode().group_concat_legacy());
        let inherited = normalize_concat_query(
            query(
                "SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ group_concat(v), \
                (SELECT /*+ SET_VAR(sql_mode=32) */ group_concat(v,'-') FROM t), \
                (SELECT group_concat(v) FROM t) FROM t",
            ),
            &SqlSemanticSettings::default(),
        )
        .unwrap();
        assert_eq!(
            calls(&inherited)
                .iter()
                .map(|call| call.arguments.len())
                .collect::<Vec<_>>(),
            vec![1, 2, 1]
        );
        let branches = normalize_concat_query(
            query(
                "SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ group_concat(v) FROM t \
             UNION ALL SELECT group_concat(v,'-') FROM t",
            ),
            &SqlSemanticSettings::default(),
        )
        .unwrap();
        let calls = calls(&branches);
        assert_eq!(
            calls
                .iter()
                .map(|call| call.arguments.len())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(
            !query_sql_semantics(&SqlSemanticSettings::default(), &branches)
                .unwrap()
                .sql_mode()
                .group_concat_legacy()
        );
    }
    #[test]
    fn decimal_flag_assignments_preserve_mode_and_apply_only_in_their_scope() {
        let session = semantics("GROUP_CONCAT_LEGACY").with_decimal_overflow_to_double(true);
        for value in ["0", "OFF", "false", "'false'"] {
            let resolved = query_sql_semantics(
                &session,
                &query(&format!(
                    "SELECT /*+ SET_VAR(decimal_overflow_to_double={value}) */ 1"
                )),
            )
            .unwrap();
            assert!(!resolved.decimal_overflow_to_double());
            assert!(resolved.sql_mode().group_concat_legacy());
        }
        let value = query_sql_semantics(&session, &query(
            "SELECT /*+ SET_VAR(sql_mode=32,decimal_overflow_to_double=0) */              /*+ SET_VAR(decimal_overflow_to_double=1) */ 1"
        )).unwrap();
        assert!(value.decimal_overflow_to_double());
        assert!(!value.sql_mode().group_concat_legacy());
        assert!(session.sql_mode().group_concat_legacy());
        assert!(session.decimal_overflow_to_double());
        for sql in [
            "SELECT /*+ SET_VAR(decimal_overflow_to_double=2) */ 1",
            "SELECT 1 FROM (SELECT /*+ SET_VAR(decimal_overflow_to_double='bad') */ 1) t",
            "WITH t AS (SELECT /*+ SET_VAR(decimal_overflow_to_double=1+1) */ 1) SELECT 1 FROM t",
            "SELECT 1 UNION ALL SELECT /*+ SET_VAR(decimal_overflow_to_double=NULL) */ 1",
        ] {
            let error = normalize_concat_query(query(sql), &session).unwrap_err();
            assert_eq!(error.code().as_str(), "sql.analyze.invalid_argument");
            assert!(error.span().is_some());
            let statements = novarocks_parser::parse(sql).unwrap();
            let error = statement_sql_semantics(&session, &statements[0]).unwrap_err();
            assert_eq!(error.code().as_str(), "sql.analyze.invalid_argument");
            assert!(error.span().is_some());
        }
    }
    #[test]
    fn decimal_replay_rejects_consumer_and_stored_local_semantics() {
        let ordinary = SqlSemanticSettings::default();
        let promoted = ordinary.clone().with_decimal_overflow_to_double(true);
        assert!(validate_persisted_query_semantics(&query("SELECT 1"), &ordinary).is_ok());
        assert!(
            validate_persisted_query_semantics(&query("SELECT 1"), &promoted)
                .unwrap_err()
                .contains("decimal_overflow_to_double=true is not captured")
        );
        for sql in [
            "SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ 1",
            "SELECT 1 FROM (SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ 1) t",
            "WITH t AS (SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ 1) SELECT 1 FROM t",
        ] {
            assert!(validate_persisted_query_semantics(&query(sql), &ordinary).is_err());
        }
    }
    #[test]
    fn decimal_usage_only_observes_effective_select_scopes() {
        let on = SqlSemanticSettings::default().with_decimal_overflow_to_double(true);
        let off = SqlSemanticSettings::default();
        for sql in [
            "SELECT /*+ SET_VAR(decimal_overflow_to_double=false) */ 1",
            "SELECT /*+ SET_VAR(decimal_overflow_to_double=false) */ 1 UNION ALL SELECT /*+ SET_VAR(decimal_overflow_to_double=false) */ 2",
        ] {
            assert!(!query_uses_decimal_overflow_to_double(&on, &query(sql)).unwrap());
        }
        for sql in [
            "SELECT 1 UNION ALL SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ 2",
            "WITH c AS (SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ 1) SELECT * FROM c",
            "SELECT (SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ 1)",
        ] {
            assert!(query_uses_decimal_overflow_to_double(&off, &query(sql)).unwrap());
        }
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
    fn overflow_definition_usage_observes_lexical_selects_and_replay_boundary() {
        let ordinary = SqlSemanticSettings::default();
        let strict = ordinary
            .clone()
            .with_sql_mode(SqlMode::from_assignment("ERROR_IF_OVERFLOW"));
        for sql in [
            "SELECT /*+ SET_VAR(sql_mode=32) */ 1",
            "SELECT /*+ SET_VAR(sql_mode=32) */ 1 UNION ALL SELECT /*+ SET_VAR(sql_mode=32) */ 2",
        ] {
            assert!(!query_uses_error_if_overflow(&strict, &query(sql)).unwrap());
        }
        for sql in [
            "SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ 1",
            "SELECT 1 UNION ALL SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ 2",
            "WITH c AS (SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ 1) SELECT * FROM c",
            "SELECT (SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ 1)",
        ] {
            assert!(query_uses_error_if_overflow(&ordinary, &query(sql)).unwrap());
            assert!(
                validate_persisted_query_semantics(&query(sql), &ordinary)
                    .unwrap_err()
                    .contains("ERROR_IF_OVERFLOW is not captured")
            );
        }
        assert!(validate_persisted_query_semantics(&query("SELECT 1"), &strict).is_err());
        assert!(validate_persisted_query_semantics(&query("SELECT 1"), &ordinary).is_ok());
    }
}
