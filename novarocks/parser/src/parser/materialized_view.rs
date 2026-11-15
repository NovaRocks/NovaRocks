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

//! Materialized-view statement grammar.

use crate::{
    ParseError, Span, TokenKind,
    ast::materialized_view::{
        AlterMaterializedView, CreateMaterializedView, DropMaterializedView,
        ExplainRefreshMaterializedView, MaterializedViewAlterAction, MaterializedViewDistribution,
        MaterializedViewExplainLevel, MaterializedViewPartitionArgument,
        MaterializedViewPartitionField, MaterializedViewProperty, MaterializedViewRefreshMode,
        MaterializedViewRefreshPolicy, RefreshMaterializedView,
    },
    ast::{Ident, Literal, LiteralKind, MaterializedViewStatement, Statement},
    token::Symbol,
};

use super::{StatementParser, query};

pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("CREATE") && parser.peek_word(1, "MATERIALIZED") {
        return parse_create(parser).map(Some);
    }
    if parser.current_is_word("DROP") && parser.peek_word(1, "MATERIALIZED") {
        return parse_drop(parser).map(Some);
    }
    if parser.current_is_word("ALTER") && parser.peek_word(1, "MATERIALIZED") {
        return parse_alter(parser).map(Some);
    }
    if parser.current_is_word("REFRESH") && parser.peek_word(1, "MATERIALIZED") {
        return parse_refresh(parser).map(Some);
    }
    if parser.current_is_word("EXPLAIN")
        && (parser.peek_word(1, "REFRESH")
            || (["VERBOSE", "COSTS", "ANALYZE"]
                .iter()
                .any(|word| parser.peek_word(1, word))
                && parser.peek_word(2, "REFRESH")))
    {
        return parse_explain_refresh(parser).map(Some);
    }
    Ok(None)
}

fn parse_create(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("CREATE")?.start();
    parser.consume_word("MATERIALIZED")?;
    parser.consume_word("VIEW")?;
    let if_not_exists = consume_if_not_exists(parser)?;
    let name = parser.parse_object_name()?;
    let mut comment = None;
    let mut partition_by = None;
    let mut distribution = None;
    let mut refresh = None;
    let mut primary_key = None;
    let mut properties = Vec::new();
    let query;
    let body_start;
    loop {
        if parser.consume_if_word("COMMENT") {
            if comment.is_some() {
                return Err(parser.unexpected("one COMMENT clause"));
            }
            comment = Some(parser.parse_literal()?);
        } else if parser.consume_if_word("PARTITION") {
            parser.consume_word("BY")?;
            if partition_by.is_some() {
                return Err(parser.unexpected("one PARTITION BY clause"));
            }
            partition_by = Some(parse_partition_fields(parser)?);
        } else if parser.consume_if_word("DISTRIBUTED") {
            parser.consume_word("BY")?;
            parser.consume_word("HASH")?;
            if distribution.is_some() {
                return Err(parser.unexpected("one DISTRIBUTED BY clause"));
            }
            distribution = Some(parse_distribution(parser)?);
        } else if parser.consume_if_word("REFRESH") {
            if refresh.is_some() {
                return Err(parser.unexpected("one REFRESH clause"));
            }
            refresh = Some(parse_refresh_policy(parser)?);
        } else if parser.consume_if_word("PRIMARY") {
            parser.consume_word("KEY")?;
            if primary_key.is_some() {
                return Err(parser.unexpected("one PRIMARY KEY clause"));
            }
            primary_key = Some(parse_ident_list(parser)?);
        } else if parser.consume_if_word("PROPERTIES") {
            if !properties.is_empty() {
                return Err(parser.unexpected("one PROPERTIES clause"));
            }
            properties = parse_properties(parser)?;
        } else if parser.consume_if_word("AS") {
            body_start = parser.current_offset();
            query = query::parse_query(parser)?;
            break;
        } else {
            return Err(parser.unexpected("materialized view clause or AS"));
        }
    }
    let body_end = parser.tokens[..parser.position]
        .iter()
        .rfind(|token| !matches!(token.kind, crate::TokenKind::Trivia(_)))
        .expect("a parsed query consumed at least one significant token")
        .span
        .end();
    let query_source_span = Span::new(body_start, body_end);
    let raw_query_source = parser.source_slice(query_source_span).to_owned();
    let span = Span::new(start, query.span.end());
    let Some(distribution) = distribution else {
        return Err(parser.unexpected("DISTRIBUTED BY HASH (...) BUCKETS <count>"));
    };
    if distribution.buckets.is_none() {
        return Err(parser.unexpected("BUCKETS <count>"));
    }
    Ok(Statement::MaterializedView(
        MaterializedViewStatement::Create(CreateMaterializedView {
            if_not_exists,
            name,
            comment,
            partition_by,
            distribution: Some(distribution),
            refresh,
            primary_key,
            properties,
            query,
            raw_query_source,
            query_source_span,
            span,
        }),
    ))
}

fn parse_drop(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("DROP")?.start();
    parser.consume_word("MATERIALIZED")?;
    parser.consume_word("VIEW")?;
    let if_exists = consume_if_exists(parser)?;
    let name = parser.parse_object_name()?;
    if parser.consume_if_word("FORCE") {
        return Err(parser.unexpected("DROP MATERIALIZED VIEW without FORCE"));
    }
    let span = Span::new(start, name.span.end());
    Ok(Statement::MaterializedView(
        MaterializedViewStatement::Drop(DropMaterializedView {
            if_exists,
            name,
            span,
        }),
    ))
}

fn parse_alter(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("ALTER")?.start();
    parser.consume_word("MATERIALIZED")?;
    parser.consume_word("VIEW")?;
    let name = parser.parse_object_name()?;
    let action = if parser.consume_if_word("SET") {
        if parser.consume_if_word("REFRESH") {
            MaterializedViewAlterAction::SetRefresh(parse_refresh_policy(parser)?)
        } else if parser.consume_if_word("TBLPROPERTIES") {
            MaterializedViewAlterAction::SetProperties(parse_properties(parser)?)
        } else {
            return Err(parser.unexpected("REFRESH or TBLPROPERTIES"));
        }
    } else if parser.consume_if_word("PAUSE") {
        parser.consume_word("REFRESH")?;
        MaterializedViewAlterAction::PauseRefresh
    } else if parser.consume_if_word("RESUME") {
        parser.consume_word("REFRESH")?;
        MaterializedViewAlterAction::ResumeRefresh
    } else if parser.consume_if_word("REPARTITION") {
        parser.consume_word("BY")?;
        MaterializedViewAlterAction::Repartition(parse_partition_fields(parser)?)
    } else {
        return Err(parser.unexpected("SET, PAUSE, RESUME, or REPARTITION"));
    };
    let span = Span::new(start, parser.current_offset());
    Ok(Statement::MaterializedView(
        MaterializedViewStatement::Alter(AlterMaterializedView { name, action, span }),
    ))
}

fn parse_refresh(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("REFRESH")?.start();
    parser.consume_word("MATERIALIZED")?;
    parser.consume_word("VIEW")?;
    let name = parser.parse_object_name()?;
    let full = parser.consume_if_word("FULL");
    let mode = if parser.consume_if_word("WITH") {
        let mode = if parser.consume_if_word("SYNC") {
            MaterializedViewRefreshMode::Sync
        } else if parser.consume_if_word("ASYNC") {
            MaterializedViewRefreshMode::Async
        } else {
            return Err(parser.unexpected("SYNC or ASYNC"));
        };
        parser.consume_word("MODE")?;
        Some(mode)
    } else {
        None
    };
    let span = Span::new(start, parser.current_offset());
    Ok(Statement::MaterializedView(
        MaterializedViewStatement::Refresh(RefreshMaterializedView {
            name,
            full,
            mode,
            span,
        }),
    ))
}

fn parse_explain_refresh(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.consume_word("EXPLAIN")?.start();
    let level = if parser.consume_if_word("VERBOSE") {
        MaterializedViewExplainLevel::Verbose
    } else if parser.consume_if_word("COSTS") {
        MaterializedViewExplainLevel::Costs
    } else if parser.current_is_word("ANALYZE") {
        return Err(parser.unexpected("VERBOSE, COSTS, or REFRESH MATERIALIZED VIEW"));
    } else {
        MaterializedViewExplainLevel::Default
    };
    let refresh = match parse_refresh(parser)? {
        Statement::MaterializedView(MaterializedViewStatement::Refresh(refresh)) => refresh,
        _ => unreachable!("REFRESH parser must produce a refresh AST"),
    };
    let span = Span::new(start, refresh.span.end());
    Ok(Statement::MaterializedView(
        MaterializedViewStatement::ExplainRefresh(ExplainRefreshMaterializedView {
            level,
            refresh,
            span,
        }),
    ))
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

fn parse_distribution(
    parser: &mut StatementParser<'_, '_>,
) -> Result<MaterializedViewDistribution, ParseError> {
    let start = parser.consume_symbol(Symbol::LParen)?.start();
    let mut hash_columns = Vec::new();
    loop {
        hash_columns.push(parser.parse_ident()?);
        if parser.consume_if_symbol(Symbol::RParen) {
            break;
        }
        parser.consume_symbol(Symbol::Comma)?;
    }
    let buckets = if parser.consume_if_word("BUCKETS") {
        Some(parser.parse_literal()?)
    } else {
        None
    };
    Ok(MaterializedViewDistribution {
        hash_columns,
        buckets,
        span: Span::new(start, parser.current_offset()),
    })
}

fn parse_partition_fields(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<MaterializedViewPartitionField>, ParseError> {
    let parens = parser.consume_if_symbol(Symbol::LParen);
    let mut fields = Vec::new();
    loop {
        fields.push(parse_partition_field(parser)?);
        if parens && parser.consume_if_symbol(Symbol::RParen) {
            break;
        }
        if !parens && !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
        if parens {
            parser.consume_symbol(Symbol::Comma)?;
        }
    }
    Ok(fields)
}

fn parse_partition_field(
    parser: &mut StatementParser<'_, '_>,
) -> Result<MaterializedViewPartitionField, ParseError> {
    let name = parse_partition_name(parser)?;
    if !parser.consume_if_symbol(Symbol::LParen) {
        return Ok(MaterializedViewPartitionField::Identity(name));
    }
    let start = name.span.start();
    let mut arguments = Vec::new();
    if !parser.consume_if_symbol(Symbol::RParen) {
        loop {
            arguments.push(parse_partition_argument(parser)?);
            if parser.consume_if_symbol(Symbol::RParen) {
                break;
            }
            parser.consume_symbol(Symbol::Comma)?;
        }
    }
    Ok(MaterializedViewPartitionField::Transform {
        name,
        arguments,
        span: Span::new(start, parser.current_offset()),
    })
}

fn parse_partition_name(parser: &mut StatementParser<'_, '_>) -> Result<Ident, ParseError> {
    if !parser
        .current()
        .is_some_and(|token| matches!(token.kind, TokenKind::Keyword(_)))
    {
        return parser.parse_ident();
    }
    let span = parser.current_span();
    let ident = Ident {
        value: parser.source_slice(span).to_owned(),
        quoted: false,
        quote_style: None,
        span,
    };
    parser.advance();
    parser.skip_trivia();
    Ok(ident)
}

fn parse_partition_argument(
    parser: &mut StatementParser<'_, '_>,
) -> Result<MaterializedViewPartitionArgument, ParseError> {
    if parser.current().is_some_and(|token| {
        matches!(
            token.kind,
            TokenKind::String | TokenKind::Number | TokenKind::HexNumber
        )
    }) {
        return Ok(MaterializedViewPartitionArgument::Literal(
            parser.parse_literal()?,
        ));
    }
    Ok(MaterializedViewPartitionArgument::Ident(
        parser.parse_ident()?,
    ))
}

fn parse_ident_list(parser: &mut StatementParser<'_, '_>) -> Result<Vec<Ident>, ParseError> {
    parser.consume_symbol(Symbol::LParen)?;
    if parser.current_is_symbol(Symbol::RParen) {
        return Err(parser.unexpected("PRIMARY KEY clause requires at least one column"));
    }
    let mut idents = Vec::new();
    loop {
        idents.push(parser.parse_ident()?);
        if parser.consume_if_symbol(Symbol::RParen) {
            return Ok(idents);
        }
        parser.consume_symbol(Symbol::Comma)?;
    }
}

fn parse_refresh_policy(
    parser: &mut StatementParser<'_, '_>,
) -> Result<MaterializedViewRefreshPolicy, ParseError> {
    if parser.consume_if_word("IMMEDIATE") {
        return Ok(MaterializedViewRefreshPolicy::Immediate);
    }
    let deferred = parser.consume_if_word("DEFERRED");
    if parser.consume_if_word("MANUAL") {
        return Ok(MaterializedViewRefreshPolicy::Manual { deferred });
    }
    parser.consume_word("ASYNC")?;
    if parser.consume_if_word("ON") {
        parser.consume_word("CHANGE")?;
        return Ok(MaterializedViewRefreshPolicy::AsyncOnChange { deferred });
    }
    parser.consume_word("EVERY")?;
    parser.consume_word("INTERVAL")?;
    let interval = parser.parse_literal()?;
    let unit = parser.parse_ident()?;
    Ok(MaterializedViewRefreshPolicy::AsyncEvery {
        deferred,
        interval,
        unit,
    })
}

fn parse_properties(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<MaterializedViewProperty>, ParseError> {
    parser.consume_symbol(Symbol::LParen)?;
    let mut properties = Vec::new();
    if parser.consume_if_symbol(Symbol::RParen) {
        return Ok(properties);
    }
    loop {
        let key = parse_property_value(parser)?;
        parser.consume_symbol(Symbol::Eq)?;
        let value = parse_property_value(parser)?;
        properties.push(MaterializedViewProperty {
            span: Span::new(key.span.start(), value.span.end()),
            key,
            value,
        });
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
