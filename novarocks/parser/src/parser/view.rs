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

//! View statement grammar.

use crate::{
    ParseError, Span,
    ast::{CreateView, DropView, ShowCreateView, ShowViews, Statement, ViewStatement},
    token::Symbol,
};

use super::{StatementParser, query};

pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("CREATE")
        && (parser.peek_word(1, "VIEW")
            || (parser.peek_word(1, "OR")
                && parser.peek_word(2, "REPLACE")
                && parser.peek_word(3, "VIEW")))
    {
        return parse_create(parser).map(Some);
    }
    if parser.current_is_word("DROP") && parser.peek_word(1, "VIEW") {
        return parse_drop(parser).map(Some);
    }
    if parser.current_is_word("SHOW")
        && parser.peek_word(1, "CREATE")
        && parser.peek_word(2, "VIEW")
    {
        return parse_show_create(parser).map(Some);
    }
    if parser.current_is_word("SHOW")
        && (parser.peek_word(1, "VIEWS") || parser.peek_word(1, "VIEW"))
    {
        return parse_show(parser).map(Some);
    }
    Ok(None)
}

fn parse_create(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("CREATE")?.start();
    let or_replace = if parser.consume_if_word("OR") {
        parser.consume_word("REPLACE")?;
        true
    } else {
        false
    };
    parser.consume_word("VIEW")?;
    let if_not_exists = consume_if_not_exists(parser)?;
    if or_replace && if_not_exists {
        return Err(parser.unexpected("CREATE OR REPLACE VIEW without IF NOT EXISTS"));
    }
    let name = parser.parse_object_name()?;
    let columns = if parser.current_is_symbol(Symbol::LParen) {
        parse_columns(parser)?
    } else {
        Vec::new()
    };
    let comment = if parser.consume_if_word("COMMENT") {
        Some(parser.parse_literal()?)
    } else {
        None
    };
    parser.consume_word("AS")?;
    let body_start = parser.current_offset();
    let query = query::parse_query(parser)?;
    let body_end = parser.tokens[..parser.position]
        .iter()
        .rfind(|token| !matches!(token.kind, crate::TokenKind::Trivia(_)))
        .expect("a parsed query consumed at least one significant token")
        .span
        .end();
    let query_source_span = Span::new(body_start, body_end);
    let raw_query_source = parser.source_slice(query_source_span).to_owned();
    let span = Span::new(start, query.span.end());
    Ok(Statement::View(ViewStatement::Create(CreateView {
        or_replace,
        if_not_exists,
        name,
        columns,
        comment,
        query,
        raw_query_source,
        query_source_span,
        span,
    })))
}

fn parse_drop(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("DROP")?.start();
    parser.consume_word("VIEW")?;
    let if_exists = consume_if_exists(parser)?;
    let name = parser.parse_object_name()?;
    let span = Span::new(start, name.span.end());
    Ok(Statement::View(ViewStatement::Drop(DropView {
        if_exists,
        name,
        span,
    })))
}

fn parse_show(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("SHOW")?.start();
    parser.consume_word("VIEWS")?;
    let database = if parser.consume_if_word("FROM") || parser.consume_if_word("IN") {
        Some(parser.parse_object_name()?)
    } else {
        None
    };
    if parser.current_is_word("LIKE") || parser.current_is_word("WHERE") {
        return Err(parser.unexpected("unfiltered SHOW VIEWS"));
    }
    let span = Span::new(start, parser.current_offset());
    Ok(Statement::View(ViewStatement::Show(ShowViews {
        database,
        span,
    })))
}

fn parse_show_create(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("SHOW")?.start();
    parser.consume_word("CREATE")?;
    parser.consume_word("VIEW")?;
    let name = parser.parse_object_name()?;
    let span = Span::new(start, name.span.end());
    Ok(Statement::View(ViewStatement::ShowCreate(ShowCreateView {
        name,
        span,
    })))
}

fn parse_columns(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<crate::ast::Ident>, ParseError> {
    parser.consume_symbol(Symbol::LParen)?;
    let mut columns = Vec::new();
    loop {
        columns.push(parser.parse_ident()?);
        if parser.consume_if_symbol(Symbol::RParen) {
            break;
        }
        parser.consume_symbol(Symbol::Comma)?;
    }
    Ok(columns)
}

fn consume_if_not_exists(parser: &mut StatementParser<'_, '_>) -> Result<bool, ParseError> {
    if !parser.consume_if_word("IF") {
        return Ok(false);
    }
    parser.consume_word("NOT")?;
    parser.consume_word("EXISTS")?;
    Ok(true)
}

fn consume_if_exists(parser: &mut StatementParser<'_, '_>) -> Result<bool, ParseError> {
    if !parser.consume_if_word("IF") {
        return Ok(false);
    }
    parser.consume_word("EXISTS")?;
    Ok(true)
}

#[cfg(test)]
mod source_tests {
    use super::*;

    #[test]
    fn view_body_source_keeps_internal_trivia_spelling_and_statement_boundaries() {
        let source = "CREATE VIEW v AS /* before */\nSELECT /* inside */ `MiXeD`  FROM t -- after\n; SELECT 9";
        let statements = crate::parse(source).unwrap();
        let Statement::View(ViewStatement::Create(create)) = &statements[0] else {
            panic!("view")
        };
        assert_eq!(
            create.raw_query_source,
            "SELECT /* inside */ `MiXeD`  FROM t"
        );
        assert_eq!(
            source.get(create.query_source_span.start()..create.query_source_span.end()),
            Some(create.raw_query_source.as_str())
        );
        assert_eq!(statements.len(), 2);
    }

    #[test]
    fn view_body_source_at_eof_excludes_trivia_outside_its_token_range() {
        let source = "CREATE VIEW v AS SELECT 1 /* tail */  ";
        let statements = crate::parse(source).unwrap();
        let Statement::View(ViewStatement::Create(create)) = &statements[0] else {
            panic!("view")
        };
        assert_eq!(create.raw_query_source, "SELECT 1");
    }

    #[test]
    fn view_body_source_uses_consumed_tokens_including_parentheses_and_offset_rows() {
        for body in [
            "(SELECT 1 AS x)",
            "SELECT 1 AS x OFFSET 0 ROWS",
            "SELECT 1 AS x FETCH FIRST 2 ROWS ONLY",
        ] {
            let statements =
                crate::parse(&format!("CREATE VIEW v AS /* before */ {body} -- after\n;")).unwrap();
            let Statement::View(ViewStatement::Create(create)) = &statements[0] else {
                panic!("view")
            };
            assert_eq!(create.raw_query_source, body);
        }
    }
}
