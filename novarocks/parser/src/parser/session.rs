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

//! Session-command grammar.

use crate::{
    ParseError, Span, Token, TokenKind,
    ast::{
        KillKind, KillStatement, SessionStatement, SetAssignment, SetScope, SetStatement,
        Statement, TransactionControlKind, TransactionControlStatement, UseStatement,
    },
    token::Symbol,
};

use super::{StatementParser, pratt::PrattParser};

pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("SET") {
        return parse_set(parser).map(Some);
    }
    if parser.current_is_word("USE") {
        return parse_use(parser).map(Some);
    }
    if parser.current_is_word("BEGIN")
        || parser.current_is_word("START")
        || parser.current_is_word("COMMIT")
        || parser.current_is_word("ROLLBACK")
        || parser.current_is_word("SAVEPOINT")
    {
        return parse_transaction_control(parser).map(Some);
    }
    if parser.current_is_word("KILL") {
        return parse_kill(parser).map(Some);
    }
    Ok(None)
}

fn parse_transaction_control(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Statement, ParseError> {
    let start = parser.current_span().start();
    let kind = if parser.consume_if_word("BEGIN") {
        TransactionControlKind::Begin
    } else if parser.consume_if_word("START") {
        parser.consume_word("TRANSACTION")?;
        TransactionControlKind::StartTransaction
    } else if parser.consume_if_word("COMMIT") {
        TransactionControlKind::Commit
    } else if parser.consume_if_word("ROLLBACK") {
        TransactionControlKind::Rollback
    } else {
        parser.consume_word("SAVEPOINT")?;
        TransactionControlKind::Savepoint
    };
    // Transaction modifiers do not acquire semantics here.  The closed AST
    // tells admission that this is a cross-statement transaction request, and
    // admission rejects it before a provider call; retaining the modifiers
    // would only create a second, unusable transaction grammar.
    while !parser.is_end() && !parser.current_is_symbol(Symbol::Semicolon) {
        parser.advance();
        parser.skip_trivia();
    }
    let end = parser.current_span().start();
    Ok(Statement::Session(SessionStatement::TransactionControl(
        TransactionControlStatement {
            kind,
            span: Span::new(start, end),
        },
    )))
}

fn parse_set(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("SET")?.start();
    let mut assignments = Vec::new();
    loop {
        assignments.push(parse_assignment(parser)?);
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    let end = assignments
        .last()
        .expect("SET has at least one assignment")
        .span
        .end();
    Ok(Statement::Session(SessionStatement::Set(SetStatement {
        assignments,
        span: Span::new(start, end),
    })))
}

fn parse_assignment(parser: &mut StatementParser<'_, '_>) -> Result<SetAssignment, ParseError> {
    let start = parser.current_span().start();
    let scope = parse_optional_scope(parser);
    let name = parser.parse_contextual_ident()?;
    parser.consume_symbol(Symbol::Eq)?;
    let value = parse_expression_until_assignment_end(parser)?;
    let end = value.span().end();
    Ok(SetAssignment {
        scope,
        name,
        value,
        span: Span::new(start, end),
    })
}

fn parse_optional_scope(parser: &mut StatementParser<'_, '_>) -> SetScope {
    if parser.consume_if_word("SESSION") {
        SetScope::Session
    } else if parser.consume_if_word("LOCAL") {
        SetScope::Local
    } else if parser.consume_if_word("GLOBAL") {
        SetScope::Global
    } else {
        SetScope::Default
    }
}

fn parse_expression_until_assignment_end(
    parser: &mut StatementParser<'_, '_>,
) -> Result<crate::ast::Expr, ParseError> {
    let begin = parser.position;
    let end = assignment_value_end(parser);
    if begin == end {
        return Err(parser.unexpected("expression"));
    }
    let mut tokens: Vec<Token> = parser.tokens[begin..end].to_vec();
    let boundary = parser
        .tokens
        .get(end)
        .map_or_else(|| parser.current_span(), |token| token.span);
    tokens.push(Token::new(TokenKind::End, boundary));
    let expression = PrattParser::new(parser.source, &tokens).parse()?;
    parser.position = end;
    parser.skip_trivia();
    Ok(expression)
}

fn assignment_value_end(parser: &StatementParser<'_, '_>) -> usize {
    let mut end = parser.position;
    let mut nesting = 0usize;
    while let Some(token) = parser.tokens.get(end) {
        match token.kind {
            TokenKind::End => break,
            TokenKind::Symbol(Symbol::LParen | Symbol::LBracket | Symbol::LBrace) => nesting += 1,
            TokenKind::Symbol(Symbol::RParen | Symbol::RBracket | Symbol::RBrace)
                if nesting > 0 =>
            {
                nesting -= 1;
            }
            TokenKind::Symbol(Symbol::Comma | Symbol::Semicolon) if nesting == 0 => break,
            _ => {}
        }
        end += 1;
    }
    end
}

fn parse_use(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("USE")?.start();
    let first = parser.parse_contextual_ident()?;
    let (catalog, database) = if parser.consume_if_symbol(Symbol::Dot) {
        (Some(first), parser.parse_contextual_ident()?)
    } else {
        (None, first)
    };
    let end = database.span.end();
    Ok(Statement::Session(SessionStatement::Use(UseStatement {
        catalog,
        database,
        span: Span::new(start, end),
    })))
}

fn parse_kill(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("KILL")?.start();
    let kind = if parser.consume_if_word("QUERY") {
        KillKind::Query
    } else if parser.consume_if_word("CONNECTION") {
        KillKind::Connection
    } else {
        KillKind::Default
    };
    if !matches!(
        parser.current().map(|token| &token.kind),
        Some(TokenKind::Number)
    ) {
        return Err(parser.unexpected("connection id"));
    }
    let connection_id = parser.parse_literal()?;
    let end = connection_id.span.end();
    Ok(Statement::Session(SessionStatement::Kill(KillStatement {
        kind,
        connection_id,
        span: Span::new(start, end),
    })))
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{KillKind, SessionStatement, SetScope, Statement, TransactionControlKind},
        parser,
        printer::print_statement,
    };

    #[test]
    fn parses_independently_scoped_set_assignments() {
        let statements = parser::parse(
            "SET SESSION query_timeout = 1, GLOBAL pipeline_dop = 2, LOCAL sql_mode = '42'",
        )
        .expect("SET assignments should parse");
        let Statement::Session(SessionStatement::Set(set)) = &statements[0] else {
            panic!("expected SET session statement");
        };
        assert_eq!(set.assignments.len(), 3);
        assert_eq!(set.assignments[0].scope, SetScope::Session);
        assert_eq!(set.assignments[1].scope, SetScope::Global);
        assert_eq!(set.assignments[2].scope, SetScope::Local);
        assert_eq!(set.assignments[2].name.value, "sql_mode");
    }

    #[test]
    fn rejects_retired_session_syntax() {
        for source in [
            "SELECT @x",
            "SELECT @@time_zone",
            "SET @x = 1",
            "SET NAMES utf8",
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "SET CATALOG warehouse",
            "SET SESSION TRANSACTION READ ONLY",
            "SET pipeline_dop = on",
        ] {
            assert!(parser::parse(source).is_err(), "{source}");
        }
    }

    #[test]
    fn parses_expression_assignment_use_and_all_kill_forms() {
        let statements = parser::parse(
            "SET pipeline_dop = (SELECT 42); USE `catalog`.`analytics`; KILL 1; KILL QUERY 2; KILL CONNECTION 3",
        )
        .expect("session statements should parse");
        let Statement::Session(SessionStatement::Set(set)) = &statements[0] else {
            panic!("expected SET statement");
        };
        assert!(matches!(
            &set.assignments[0].value,
            crate::ast::Expr::Subquery(_)
        ));
        let Statement::Session(SessionStatement::Use(use_statement)) = &statements[1] else {
            panic!("expected USE statement");
        };
        assert_eq!(
            use_statement
                .catalog
                .as_ref()
                .map(|catalog| catalog.value.as_str()),
            Some("catalog")
        );
        assert_eq!(use_statement.database.value, "analytics");
        assert_eq!(print_statement(&statements[1]), "USE `catalog`.`analytics`");
        for (statement, kind) in
            statements[2..]
                .iter()
                .zip([KillKind::Default, KillKind::Query, KillKind::Connection])
        {
            let Statement::Session(SessionStatement::Kill(kill)) = statement else {
                panic!("expected KILL statement");
            };
            assert_eq!(kill.kind, kind);
        }
    }

    #[test]
    fn kill_analyze_is_a_typed_syntax_error() {
        let source = "KILL ANALYZE 17";
        assert_eq!(
            parser::parse(source)
                .unwrap_err()
                .to_user_error(source)
                .code()
                .as_str(),
            "sql.parse.unexpected_token"
        );
    }

    #[test]
    fn session_commands_reject_trailing_tokens_with_parser_errors() {
        for source in [
            "USE default extra",
            "USE catalog.database.extra",
            "KILL QUERY 1 extra",
            "KILL QUERY '1'",
        ] {
            let error = parser::parse(source).expect_err("trailing token must fail");
            assert_eq!(
                error.to_user_error(source).code().as_str(),
                "sql.parse.unexpected_token"
            );
        }
    }

    #[test]
    fn set_values_use_the_ordinary_expression_grammar() {
        let statements =
            parser::parse("SET pipeline_dop = 1 + 2, enable_eliminate_agg = off").unwrap();
        let Statement::Session(SessionStatement::Set(set)) = &statements[0] else {
            panic!("expected SET");
        };
        assert!(matches!(
            &set.assignments[0].value,
            crate::ast::Expr::Binary(_)
        ));
        assert!(matches!(
            &set.assignments[1].value,
            crate::ast::Expr::Identifier(_)
        ));
        assert_eq!(
            print_statement(&statements[0]),
            "SET pipeline_dop = 1 + 2, enable_eliminate_agg = off"
        );
        assert!(parser::parse("SET pipeline_dop = on").is_err());
    }

    #[test]
    fn contextual_session_words_remain_query_identifiers() {
        assert!(matches!(
            parser::parse("SELECT * FROM session"),
            Ok(statements) if matches!(statements.as_slice(), [Statement::Query(_)])
        ));
    }

    #[test]
    fn parses_transaction_control_as_a_closed_session_family() {
        let statements = parser::parse(
            "BEGIN WORK; START TRANSACTION READ WRITE; COMMIT AND CHAIN; \
             ROLLBACK TO SAVEPOINT before_publish; SAVEPOINT before_publish",
        )
        .expect("transaction controls should remain typed through admission");
        let kinds = statements
            .iter()
            .map(|statement| match statement {
                Statement::Session(SessionStatement::TransactionControl(statement)) => {
                    statement.kind
                }
                other => panic!("expected transaction control, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                TransactionControlKind::Begin,
                TransactionControlKind::StartTransaction,
                TransactionControlKind::Commit,
                TransactionControlKind::Rollback,
                TransactionControlKind::Savepoint,
            ]
        );
        assert_eq!(print_statement(&statements[1]), "START TRANSACTION");
    }
}
