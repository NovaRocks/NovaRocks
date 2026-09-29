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
const CONSUMED_MASK: u64 = ONLY_FULL_GROUP_BY | ALLOW_THROW_EXCEPTION | GROUP_CONCAT_LEGACY;

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
}

impl SqlSemanticSettings {
    pub const fn sql_mode(&self) -> &SqlMode {
        &self.sql_mode
    }

    pub fn with_sql_mode(mut self, value: SqlMode) -> Self {
        self.sql_mode = value;
        self
    }
}

fn sql_mode_key(expression: &ast::Expr) -> bool {
    match expression {
        ast::Expr::Identifier(name) => name.value.eq_ignore_ascii_case("sql_mode"),
        ast::Expr::Literal(literal) => matches!(&literal.kind,
            ast::LiteralKind::String(value) if value.eq_ignore_ascii_case("sql_mode")),
        _ => false,
    }
}

fn select_sql_semantics(
    session: &SqlSemanticSettings,
    select: &ast::Select,
) -> SqlSemanticSettings {
    let mut settings = session.clone();
    for hint in &select.hints {
        if !hint.name.value.eq_ignore_ascii_case("set_var") {
            continue;
        }
        let ast::SelectHintValue::Call { arguments } = &hint.value else {
            continue;
        };
        for argument in arguments {
            if let ast::Expr::Binary(binary) = argument
                && binary.operator == ast::BinaryOperator::Equal
                && sql_mode_key(&binary.left)
            {
                settings = settings.with_sql_mode(SqlMode::from_expression(&binary.right));
            }
        }
    }
    settings
}

/// Resolve hints belonging to this query's root SELECT only. Nested SELECTs,
/// CTEs, and set-operation siblings cannot mutate the enclosing statement.
pub fn query_sql_semantics(
    session: &SqlSemanticSettings,
    query: &ast::Query,
) -> SqlSemanticSettings {
    let mut body = query.body.as_ref();
    while let ast::SetExpr::Query(query) = body {
        body = query.body.as_ref();
    }
    match body {
        ast::SetExpr::Select(select) => select_sql_semantics(session, select),
        _ => session.clone(),
    }
}

/// Freeze statement-local overrides at admission for every SQL query producer.
/// This returns an owned setting and never mutates connection state.
pub fn statement_sql_semantics(
    session: &SqlSemanticSettings,
    statement: &ast::Statement,
) -> SqlSemanticSettings {
    let query: Option<&ast::Query> = match statement {
        ast::Statement::Query(query) => Some(query),
        ast::Statement::ExplainQuery(explain) => Some(&explain.query),
        ast::Statement::Dml(ast::DmlStatement::Insert(insert)) => Some(&insert.source),
        ast::Statement::Dml(ast::DmlStatement::CreateTableAsSelect(ctas)) => Some(&ctas.query),
        _ => None,
    };
    query.map_or_else(
        || session.clone(),
        |query| query_sql_semantics(session, query),
    )
}

/// Make separator ownership explicit once, before name/type resolution.
/// Explicit SEPARATOR makes this pass idempotent across compiler continuations
/// and analyzer preparation. STRING_AGG owns its positional separator contract
/// independently of GROUP_CONCAT's modern/legacy choice.
pub(crate) fn normalize_concat_query(
    query: ast::Query,
    session: &SqlSemanticSettings,
) -> ast::Query {
    struct Normalizer {
        sql_semantics: SqlSemanticSettings,
    }
    impl Fold for Normalizer {
        fn fold_query(&mut self, query: ast::Query) -> ast::Query {
            let enclosing = self.sql_semantics.clone();
            self.sql_semantics = query_sql_semantics(&enclosing, &query);
            let query = ast::fold_query(self, query);
            self.sql_semantics = enclosing;
            query
        }

        fn fold_select(&mut self, select: ast::Select) -> ast::Select {
            let enclosing = self.sql_semantics.clone();
            self.sql_semantics = select_sql_semantics(&enclosing, &select);
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
    Normalizer {
        sql_semantics: session.clone(),
    }
    .fold_query(query)
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
        let query = normalize_concat_query(query(sql), &semantics(mode));
        let calls = calls(&query);
        crate::analyzer::display_expr_for_test(&ast::Expr::FunctionCall(calls[0].clone()))
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
            let effective = query_sql_semantics(&session, &query(sql));
            assert!(!effective.sql_mode().group_concat_legacy());
            assert!(!effective.sql_mode().allow_throw_exception());
        }
        let effective = query_sql_semantics(
            &SqlSemanticSettings::default(),
            &query("SELECT /*+ SET_VAR(sql_mode=68719477248) */ /*+ SET_VAR(query_timeout=1) */ 1"),
        );
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
                normalize_concat_query(query("SELECT group_concat(v,'-')"), &semantics(mode));
            assert_eq!(normalize_concat_query(once.clone(), &semantics(mode)), once);
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
            let effective = statement_sql_semantics(&session, &statements[0]);
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
        );
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
        let parent = query_sql_semantics(&SqlSemanticSettings::default(), &normalized);
        assert!(!parent.sql_mode().group_concat_legacy());
        let inherited = normalize_concat_query(
            query(
                "SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ group_concat(v), \
                (SELECT /*+ SET_VAR(sql_mode=32) */ group_concat(v,'-') FROM t), \
                (SELECT group_concat(v) FROM t) FROM t",
            ),
            &SqlSemanticSettings::default(),
        );
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
        );
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
                .sql_mode()
                .group_concat_legacy()
        );
    }
}
