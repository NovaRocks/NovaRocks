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

//! Catalog and truncate statement grammar.

use crate::{
    ParseError, Span, TokenKind,
    ast::legacy::catalog::{
        CatalogProperty, CreateCatalog, CreateDatabase, DropCatalog, DropDatabase, DropTable,
        ShowCreateTable, TruncateTable,
    },
    ast::{CatalogStatement, Literal, LiteralKind, Statement},
    token::Symbol,
};

use super::StatementParser;

pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("TRUNCATE") && parser.peek_word(1, "TABLE") {
        return parse_truncate(parser).map(Some);
    }
    if parser.current_is_word("CREATE")
        && (parser.peek_word(1, "CATALOG")
            || (parser.peek_word(1, "EXTERNAL") && parser.peek_word(2, "CATALOG"))
            || parser.peek_word(1, "DATABASE"))
    {
        return parse_create(parser).map(Some);
    }
    if parser.current_is_word("DROP")
        && ["CATALOG", "DATABASE", "TABLE"]
            .iter()
            .any(|word| parser.peek_word(1, word))
    {
        return parse_drop(parser).map(Some);
    }
    if parser.current_is_word("SHOW")
        && parser.peek_word(1, "CREATE")
        && parser.peek_word(2, "TABLE")
    {
        return parse_show_create(parser).map(Some);
    }
    Ok(None)
}

fn parse_truncate(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.current_span().start();
    parser.consume_word("TRUNCATE")?;
    parser.consume_word("TABLE")?;
    let raw_name = parser.parse_object_name()?;
    if parser.current_is_word("PARTITION") {
        return Err(parser.unexpected("bare TRUNCATE TABLE target"));
    }
    if parser.current_is_word("WHERE") {
        return Err(parser.unexpected("bare TRUNCATE TABLE target"));
    }
    let end = raw_name.span.end();
    let (name, target_ref) = split_truncate_target(raw_name)?;
    Ok(Statement::Catalog(CatalogStatement::TruncateTable(
        TruncateTable {
            name,
            target_ref,
            span: Span::new(start, end),
        },
    )))
}

fn split_truncate_target(
    mut raw_name: crate::ast::ObjectName,
) -> Result<(crate::ast::ObjectName, String), ParseError> {
    let Some(last) = raw_name.parts.last() else {
        return Ok((raw_name, "main".to_owned()));
    };
    let last_value = last.value.clone();
    let last_span = last.span;
    if let Some(target_ref) = last_value.strip_prefix("branch_")
        && !target_ref.is_empty()
    {
        raw_name.parts.pop();
        let end = raw_name
            .parts
            .last()
            .map(|part| part.span.end())
            .unwrap_or(raw_name.span.start());
        raw_name.span = Span::new(raw_name.span.start(), end);
        return Ok((raw_name, target_ref.to_owned()));
    }
    if let Some(target_ref) = last.value.strip_prefix("tag_")
        && !target_ref.is_empty()
    {
        return Err(ParseError::UnexpectedToken {
            expected: "writable branch target",
            found: format!("tag_{target_ref}"),
            span: last_span,
        });
    }
    Ok((raw_name, "main".to_owned()))
}

fn parse_create(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.current_span().start();
    parser.consume_word("CREATE")?;
    let external = parser.consume_if_word("EXTERNAL");
    if parser.consume_if_word("DATABASE") {
        if external {
            return Err(parser.unexpected("CATALOG"));
        }
        let if_not_exists = consume_if_not_exists(parser)?;
        let name = parser.parse_object_name()?;
        let end = name.span.end();
        return Ok(Statement::Catalog(CatalogStatement::CreateDatabase(
            CreateDatabase {
                if_not_exists,
                name,
                span: Span::new(start, end),
            },
        )));
    }
    parser.consume_word("CATALOG")?;
    let if_not_exists = consume_if_not_exists(parser)?;
    let name = parser.parse_ident()?;
    let comment = if parser.consume_if_word("COMMENT") {
        Some(parser.parse_literal()?)
    } else {
        None
    };
    let properties = if parser.consume_if_word("WITH") {
        parser.consume_word("PROPERTIES")?;
        parse_properties(parser)?
    } else if parser.consume_if_word("PROPERTIES") {
        parse_properties(parser)?
    } else {
        Vec::new()
    };
    let end = parser.current_offset();
    Ok(Statement::Catalog(CatalogStatement::CreateCatalog(
        CreateCatalog {
            external,
            if_not_exists,
            name,
            comment,
            properties,
            span: Span::new(start, end),
        },
    )))
}

fn parse_drop(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.current_span().start();
    parser.consume_word("DROP")?;
    if parser.consume_if_word("CATALOG") {
        let if_exists = consume_if_exists(parser)?;
        let name = parser.parse_ident()?;
        let end = name.span.end();
        return Ok(Statement::Catalog(CatalogStatement::DropCatalog(
            DropCatalog {
                if_exists,
                name,
                span: Span::new(start, end),
            },
        )));
    }
    let database = parser.consume_if_word("DATABASE");
    if !database {
        parser.consume_word("TABLE")?;
    }
    let if_exists = consume_if_exists(parser)?;
    let name = parser.parse_object_name()?;
    let force = parser.consume_if_word("FORCE");
    let end = parser.current_offset();
    if database {
        Ok(Statement::Catalog(CatalogStatement::DropDatabase(
            DropDatabase {
                if_exists,
                force,
                name,
                span: Span::new(start, end),
            },
        )))
    } else {
        Ok(Statement::Catalog(CatalogStatement::DropTable(DropTable {
            if_exists,
            force,
            name,
            span: Span::new(start, end),
        })))
    }
}

fn parse_show_create(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.current_span().start();
    parser.consume_word("SHOW")?;
    parser.consume_word("CREATE")?;
    parser.consume_word("TABLE")?;
    let name = parser.parse_object_name()?;
    let end = name.span.end();
    Ok(Statement::Catalog(CatalogStatement::ShowCreateTable(
        ShowCreateTable {
            name,
            span: Span::new(start, end),
        },
    )))
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

fn parse_properties(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<CatalogProperty>, ParseError> {
    parser.consume_symbol(Symbol::LParen)?;
    let mut properties = Vec::new();
    if parser.consume_if_symbol(Symbol::RParen) {
        return Ok(properties);
    }
    loop {
        let key = parse_property_value(parser)?;
        parser.consume_symbol(Symbol::Eq)?;
        let value = parse_property_value(parser)?;
        let span = Span::new(key.span.start(), value.span.end());
        properties.push(CatalogProperty { key, value, span });
        if parser.consume_if_symbol(Symbol::RParen) {
            return Ok(properties);
        }
        parser.consume_symbol(Symbol::Comma)?;
    }
}

fn parse_property_value(parser: &mut StatementParser<'_, '_>) -> Result<Literal, ParseError> {
    if parser.current().is_some_and(|token| {
        matches!(
            token.kind,
            TokenKind::String | TokenKind::Number | TokenKind::HexNumber
        )
    }) {
        return parser.parse_literal();
    }
    let ident = parser.parse_ident()?;
    Ok(Literal {
        kind: LiteralKind::String(ident.value),
        span: ident.span,
    })
}
