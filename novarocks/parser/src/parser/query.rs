// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Query-expression grammar. This module owns syntax only; it does not select
//! a production execution owner.

use crate::{
    Span, Token, TokenKind,
    ast::legacy::JoinOperator,
    ast::{
        AliasedRelation, AllColumns, AllRows, AstBox, AstVec, Cte, ExplainFormat, ExplainQuery,
        Expr, FetchFirst, GroupBy, GroupingElement, GroupingSets, GroupingSetsKind, Ident, Join,
        JoinCriteria, JoinOn, JoinType, JoinUsing, Lateral, Limit, Literal, LiteralKind,
        NaturalJoin, NullOrdering, Offset, OrderBy, Query, QueryLimit, QueryPeriod,
        QueryPeriodRangeType, Relation, RowExpr, RowField, Select, SelectItem, SetExpr,
        SetOperator, SimpleGroupBy, SingleColumn, SortItem, SortOrdering, Statement, Table,
        TableFunctionArgument, TableFunctionArgumentValue, TableFunctionInvocation, TableSubquery,
        Unnest, Values, WindowDefinition, With,
    },
    lex,
    token::{Keyword, Symbol},
};

/// One structured optimizer directive in a `SELECT /*+ ... */` comment.
#[derive(Clone, Debug, Eq, PartialEq)]
struct SelectHint {
    name: Ident,
    value: SelectHintValue,
    span: Span,
}

/// The typed payload shape of a [`SelectHint`].
///
/// Keep assignment-style directives distinct from function-style directives:
/// `new_planner_agg_stage = 3` is not a function call and must not be
/// reconstructed from opaque comment text.
#[derive(Clone, Debug, Eq, PartialEq)]
enum SelectHintValue {
    Bare,
    Call { arguments: Vec<Expr> },
    Assignment { value: Expr },
}

use super::{
    StatementParser,
    pratt::{PrattParser, parse_table_function_expression, parse_window_specification},
};

pub(super) fn parse(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Option<Statement>, crate::ParseError> {
    if !(parser.current_is_keyword(Keyword::Select)
        || parser.current_is_keyword(Keyword::Values)
        || parser.current_is_keyword(Keyword::With)
        || parser.current_is_keyword(Keyword::Explain)
        || parser.current_is_symbol(Symbol::LParen))
    {
        return Ok(None);
    }

    if parser.current_is_keyword(Keyword::Explain) {
        let start = parser.consume_word("EXPLAIN")?;
        let mut logical = false;
        let format = if parser.consume_if_word("ANALYZE") {
            ExplainFormat::Analyze
        } else if parser.consume_if_word("VERBOSE") {
            ExplainFormat::Verbose
        } else if parser.consume_if_word("COSTS") {
            ExplainFormat::Costs
        } else if parser.consume_if_word("CONTRACT") {
            ExplainFormat::Contract
        } else if parser.consume_if_word("LOGICAL") {
            logical = true;
            if parser.consume_if_word("VERBOSE") {
                ExplainFormat::Verbose
            } else if parser.consume_if_word("COSTS") {
                ExplainFormat::Costs
            } else {
                ExplainFormat::Default
            }
        } else {
            ExplainFormat::Default
        };
        let query = parse_query(parser)?;
        return Ok(Some(Statement::ExplainQuery(ExplainQuery {
            format,
            logical,
            span: Span::new(start.start(), query.span.end()),
            query: Box::new(query),
        })));
    }

    Ok(Some(Statement::Query(parse_query(parser)?)))
}

#[recursive::recursive]
pub(super) fn parse_query(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Query, crate::ParseError> {
    let start = parser.current_span().start();
    let with = if parser.consume_if_word("WITH") {
        Some(parse_with(parser, start)?)
    } else {
        None
    };
    let (mut body, mut session_properties) = parse_set_expr(parser)?;
    while let Some(operator) = parse_set_operator(parser) {
        let distinct = if parser.consume_if_word("ALL") {
            false
        } else {
            parser.consume_if_word("DISTINCT");
            true
        };
        let (right, _) = parse_set_expr(parser)?;
        session_properties.clear();
        body = crate::build::set_operation(body, operator, distinct, right, None);
    }

    let order_by = if parser.current_is_word("ORDER") {
        let order_start = parser.current_span().start();
        parser.consume_word("ORDER")?;
        parser.consume_word("BY")?;
        Some(parse_order_by(parser, order_start)?)
    } else {
        None
    };
    let mut limit = None;
    let mut offset = None;
    let mut end = order_by
        .as_ref()
        .map_or(body.span().end(), |node| node.span.end());
    loop {
        if parser.current_is_word("LIMIT") {
            if limit.is_some() {
                return Err(parser.unexpected("a single LIMIT or FETCH clause"));
            }
            let limit_start = parser.consume_word("LIMIT")?.start();
            let first = parse_row_count(
                parser,
                true,
                &["OFFSET", "FETCH", "LIMIT"],
                &[Symbol::Comma, Symbol::Semicolon, Symbol::RParen],
            )?;
            let count = if parser.consume_if_symbol(Symbol::Comma) {
                if offset.is_some() {
                    return Err(parser.unexpected("one OFFSET clause"));
                }
                if matches!(&first, Expr::AllRows(_)) {
                    return Err(parser.unexpected("integer comma-LIMIT offset"));
                }
                let offset_span = first.span();
                offset = Some(Offset {
                    row_count: first,
                    span: offset_span,
                });
                parse_row_count(
                    parser,
                    false,
                    &["OFFSET", "FETCH", "LIMIT"],
                    &[Symbol::Semicolon, Symbol::RParen],
                )?
            } else {
                first
            };
            end = count.span().end();
            limit = Some(QueryLimit::Limit(Limit {
                row_count: count,
                span: Span::new(limit_start, end),
            }));
        } else if parser.current_is_word("OFFSET") {
            if offset.is_some() {
                return Err(parser.unexpected("one OFFSET clause"));
            }
            let offset_start = parser.consume_word("OFFSET")?.start();
            let row_count = parse_row_count(
                parser,
                false,
                &["ROW", "ROWS", "FETCH", "LIMIT", "OFFSET"],
                &[Symbol::Semicolon, Symbol::RParen],
            )?;
            end = row_count.span().end();
            if parser.current_is_word("ROW") {
                end = parser.consume_word("ROW")?.end();
            } else if parser.current_is_word("ROWS") {
                end = parser.consume_word("ROWS")?.end();
            }
            offset = Some(Offset {
                row_count,
                span: Span::new(offset_start, end),
            });
        } else if parser.current_is_word("FETCH") {
            if limit.is_some() {
                return Err(parser.unexpected("FETCH without LIMIT"));
            }
            let fetch_start = parser.consume_word("FETCH")?.start();
            let fetch = parse_fetch(parser, fetch_start)?;
            end = fetch.span.end();
            limit = Some(QueryLimit::FetchFirst(fetch));
        } else {
            break;
        }
    }
    let modifiers = crate::ast::QueryModifiers {
        order_by,
        offset,
        limit,
    };
    let modifiers = if let SetExpr::Select(select) = &mut body {
        select.modifiers = modifiers;
        Default::default()
    } else {
        modifiers
    };
    Ok(Query {
        session_properties: session_properties,
        functions: AstVec::new(),
        with: with,
        body: AstBox::new(body),
        modifiers: modifiers,
        span: Span::new(start, end),
    })
}

// The old entry remains active until cutover. Only a root SELECT hint maps
// into the query assignment vocabulary; set-operation branches are local.
fn legacy_session_properties(hints: &[SelectHint]) -> Vec<crate::ast::SessionProperty> {
    let mut properties = Vec::new();
    for hint in hints {
        match &hint.value {
            SelectHintValue::Call { arguments }
                if hint.name.value.eq_ignore_ascii_case("set_var") =>
            {
                for argument in arguments {
                    let (name, value) = match argument {
                        Expr::Predicated(node)
                            if matches!(
                                node.predicate,
                                crate::ast::Predicate::Comparison(_)
                                    | crate::ast::Predicate::DistinctFrom(_)
                            ) =>
                        {
                            (
                                &node.value,
                                match &node.predicate {
                                    crate::ast::Predicate::Comparison(predicate)
                                        if predicate.operator
                                            == crate::ast::ComparisonOperator::Equal =>
                                    {
                                        &predicate.right
                                    }
                                    _ => argument,
                                },
                            )
                        }
                        Expr::Binary(binary) => (&*binary.left, argument),
                        other => (other, other),
                    };
                    let name = match name {
                        Expr::Identifier(name) => name.clone(),
                        Expr::Literal(crate::ast::Literal {
                            kind: crate::ast::LiteralKind::String(value),
                            span,
                        }) => crate::ast::Ident {
                            value: value.clone(),
                            quoted: false,
                            span: *span,
                        },
                        _ => continue,
                    };
                    properties.push(crate::ast::SessionProperty {
                        span: name.span.cover(value.span()),
                        name: crate::ast::ObjectName {
                            span: name.span,
                            parts: vec![name],
                        },
                        value: value.clone(),
                    });
                }
            }
            SelectHintValue::Assignment { value } => properties.push(crate::ast::SessionProperty {
                name: crate::ast::ObjectName {
                    parts: vec![hint.name.clone()],
                    span: hint.name.span,
                },
                value: value.clone(),
                span: hint.span,
            }),
            _ => {}
        }
    }
    properties
}

fn parse_with(
    parser: &mut StatementParser<'_, '_>,
    start: usize,
) -> Result<With, crate::ParseError> {
    let recursive = parser.consume_if_word("RECURSIVE");
    let mut ctes = Vec::new();
    loop {
        let name = parser.parse_ident()?;
        let cte_start = name.span.start();
        let mut columns = None;
        if parser.consume_if_symbol(Symbol::LParen) {
            let mut names = Vec::new();
            loop {
                names.push(parser.parse_ident()?);
                if !parser.consume_if_symbol(Symbol::Comma) {
                    break;
                }
            }
            parser.consume_symbol(Symbol::RParen)?;
            columns = Some(names);
        }
        parser.consume_word("AS")?;
        parser.consume_symbol(Symbol::LParen)?;
        let mut query = parse_query(parser)?;
        query.session_properties.clear();
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        ctes.push(Cte {
            name: name,
            columns: columns,
            query: AstBox::new(query),
            span: Span::new(cte_start, end),
        });
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    let end = ctes.last().map_or(start, |cte| cte.span.end());
    Ok(With {
        recursive: recursive,
        ctes: ctes.into(),
        span: Span::new(start, end),
    })
}

fn parse_set_expr(
    parser: &mut StatementParser<'_, '_>,
) -> Result<(SetExpr, AstVec<crate::ast::SessionProperty>), crate::ParseError> {
    if parser.current_is_keyword(Keyword::Select) {
        let (select, hints) = parse_select(parser)?;
        return Ok((
            SetExpr::Select(AstBox::new(select)),
            legacy_session_properties(&hints).into(),
        ));
    }
    if parser.current_is_keyword(Keyword::Values) {
        return Ok((SetExpr::Values(parse_values(parser)?), AstVec::new()));
    }
    if parser.current_is_symbol(Symbol::LParen) {
        let start = parser.consume_symbol(Symbol::LParen)?.start();
        let mut query = parse_query(parser)?;
        let properties = std::mem::take(&mut query.session_properties);
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        return Ok((
            SetExpr::Query(AstBox::new(TableSubquery {
                query: AstBox::new(query),
                span: Span::new(start, end),
            })),
            properties,
        ));
    }
    Err(parser.unexpected("SELECT, VALUES, or parenthesized query"))
}

fn parse_set_operator(parser: &mut StatementParser<'_, '_>) -> Option<SetOperator> {
    if parser.consume_if_word("UNION") {
        Some(SetOperator::Union)
    } else if parser.consume_if_word("INTERSECT") {
        Some(SetOperator::Intersect)
    } else if parser.consume_if_word("EXCEPT") {
        Some(SetOperator::Except)
    } else {
        None
    }
}

fn parse_select(
    parser: &mut StatementParser<'_, '_>,
) -> Result<(Select, Vec<SelectHint>), crate::ParseError> {
    let select = parser.consume_word("SELECT")?;
    let start = select.start();
    let hints = parse_select_hints(parser, select)?;
    let distinct = if parser.consume_if_word("ALL") {
        false
    } else {
        parser.consume_if_word("DISTINCT")
    };
    let projection = parse_projection(parser)?;
    let from = if parser.consume_if_word("FROM") {
        Some(parse_from(parser)?)
    } else {
        None
    };
    let selection = if parser.consume_if_word("WHERE") {
        Some(parse_expression_until(
            parser,
            query_clause_words(),
            &[Symbol::Semicolon, Symbol::RParen],
        )?)
    } else {
        None
    };
    let group_by = if parser.consume_if_word("GROUP") {
        parser.consume_word("BY")?;
        Some(parse_group_by(parser)?)
    } else {
        None
    };
    let having = if parser.consume_if_word("HAVING") {
        Some(parse_expression_until(
            parser,
            query_clause_words(),
            &[Symbol::Semicolon, Symbol::RParen],
        )?)
    } else {
        None
    };
    let qualify = if parser.consume_if_word("QUALIFY") {
        Some(parse_expression_until(
            parser,
            query_clause_words(),
            &[Symbol::Semicolon, Symbol::RParen],
        )?)
    } else {
        None
    };
    let windows = if parser.consume_if_word("WINDOW") {
        parse_named_windows(parser)?
    } else {
        Vec::new()
    };
    let end = qualify
        .as_ref()
        .or(having.as_ref())
        .or(selection.as_ref())
        .map_or_else(
            || {
                windows.last().map_or_else(
                    || {
                        from.as_ref().map_or_else(
                            || projection.last().map_or(start, |item| item.span().end()),
                            |relation| relation.span().end(),
                        )
                    },
                    |window| window.span.end(),
                )
            },
            |expr| expr.span().end(),
        );
    Ok((
        Select {
            distinct: distinct,
            projection: projection.into(),
            from: from,
            selection: selection,
            group_by: group_by,
            having: having,
            qualify: qualify,
            windows: windows.into(),
            modifiers: Default::default(),
            span: Span::new(start, end),
        },
        hints,
    ))
}

/// Parses typed optimizer directives embedded in trivia immediately following
/// `SELECT`. The lexer deliberately keeps a hint comment as one trivia token,
/// so this bounded grammar reparses only the comment payload rather than
/// retaining comment text or tokens in the AST.
fn parse_select_hints(
    parser: &StatementParser<'_, '_>,
    select: Span,
) -> Result<Vec<SelectHint>, crate::ParseError> {
    let trivia = parser.source_slice(Span::new(select.end(), parser.current_offset()));
    let mut hints = Vec::new();
    let mut cursor = 0;
    while let Some(relative_start) = trivia[cursor..].find("/*+") {
        let comment_start = cursor + relative_start;
        let payload_start = comment_start + 3;
        let Some(relative_end) = trivia[payload_start..].find("*/") else {
            return Err(crate::ParseError::UnexpectedToken {
                expected: "closed SELECT optimizer hint",
                found: "EOF".to_owned(),
                span: Span::new(select.end() + comment_start, parser.current_offset()),
            });
        };
        let payload_end = payload_start + relative_end;
        let comment_span = Span::new(select.end() + comment_start, select.end() + payload_end + 2);
        hints.extend(parse_select_hint_payload(
            parser.source,
            &trivia[payload_start..payload_end],
            select.end() + payload_start,
            comment_span,
        )?);
        cursor = payload_end + 2;
    }
    Ok(hints)
}

fn parse_select_hint_payload(
    source: &str,
    payload: &str,
    payload_start: usize,
    comment_span: Span,
) -> Result<Vec<SelectHint>, crate::ParseError> {
    let tokens = lex(payload)
        .map_err(|error| crate::ParseError::UnexpectedToken {
            expected: "valid SELECT optimizer hint",
            found: format!("{error:?}"),
            span: comment_span,
        })?
        .into_iter()
        .map(|mut token| {
            token.span = Span::new(
                payload_start + token.span.start(),
                payload_start + token.span.end(),
            );
            token
        })
        .collect::<Vec<_>>();
    let mut parser = StatementParser::new(source, &tokens);
    parser.skip_trivia();
    let mut hints = Vec::new();
    while !parser.is_end() {
        let name = parser.parse_ident()?;
        let start = name.span.start();
        let (value, end) = if parser.consume_if_symbol(Symbol::LParen) {
            let mut arguments = Vec::new();
            if !parser.current_is_symbol(Symbol::RParen) {
                loop {
                    arguments.push(parse_expression_until(
                        &mut parser,
                        &[],
                        &[Symbol::Comma, Symbol::RParen],
                    )?);
                    if !parser.consume_if_symbol(Symbol::Comma) {
                        break;
                    }
                }
            }
            (
                SelectHintValue::Call { arguments },
                parser.consume_symbol(Symbol::RParen)?.end(),
            )
        } else if parser.consume_if_symbol(Symbol::Eq) {
            let value = parse_expression_until(&mut parser, &[], &[])?;
            let end = value.span().end();
            (SelectHintValue::Assignment { value }, end)
        } else {
            (SelectHintValue::Bare, name.span.end())
        };
        hints.push(SelectHint {
            name,
            value,
            span: Span::new(start, end),
        });
    }
    Ok(hints)
}

fn parse_named_windows(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<WindowDefinition>, crate::ParseError> {
    let mut windows = Vec::new();
    loop {
        let name = parser.parse_ident()?;
        let start = name.span.start();
        parser.consume_word("AS")?;
        let specification = parse_window_specification_until(parser)?;
        let span = Span::new(start, specification.span.end());
        windows.push(WindowDefinition {
            name,
            specification,
            span,
        });
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    Ok(windows)
}

fn parse_window_specification_until(
    parser: &mut StatementParser<'_, '_>,
) -> Result<crate::ast::WindowSpecification, crate::ParseError> {
    if !parser.current_is_symbol(Symbol::LParen) {
        return Err(parser.unexpected("'(' after WINDOW name AS"));
    }
    let begin = parser.position;
    let mut end = begin;
    let mut nesting = 0usize;
    while let Some(token) = parser.tokens.get(end) {
        match token.kind {
            TokenKind::Symbol(Symbol::LParen) => nesting += 1,
            TokenKind::Symbol(Symbol::RParen) => {
                nesting = nesting.saturating_sub(1);
                if nesting == 0 {
                    end += 1;
                    break;
                }
            }
            TokenKind::End => return Err(parser.unexpected("')' after WINDOW specification")),
            _ => {}
        }
        end += 1;
    }
    if nesting != 0 {
        return Err(parser.unexpected("')' after WINDOW specification"));
    }
    let boundary = parser
        .tokens
        .get(end)
        .map_or(parser.boundary_span, |token| token.span);
    let specification =
        parse_window_specification(parser.source, &parser.tokens[begin..end], boundary)?;
    parser.position = end;
    parser.skip_trivia();
    Ok(specification)
}

fn parse_projection(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<SelectItem>, crate::ParseError> {
    let mut items = Vec::new();
    loop {
        if parser.current_is_symbol(Symbol::Star) {
            let span = parser.consume_symbol(Symbol::Star)?;
            items.push(SelectItem::AllColumns(AstBox::new(AllColumns {
                target: None,
                aliases: Vec::new(),
                span,
            })));
        } else if qualified_wildcard_ahead(parser) {
            let start = parser.current_span().start();
            let mut prefix = vec![parser.parse_ident()?];
            loop {
                parser.consume_symbol(Symbol::Dot)?;
                if parser.current_is_symbol(Symbol::Star) {
                    let end = parser.consume_symbol(Symbol::Star)?.end();
                    let SelectItem::AllColumns(mut item) = crate::build::qualified_wildcard(prefix)
                        .expect("qualified wildcard has a prefix")
                    else {
                        unreachable!()
                    };
                    item.span = Span::new(start, end);
                    items.push(SelectItem::AllColumns(item));
                    break;
                }
                prefix.push(parser.parse_ident()?);
            }
        } else {
            let (expr, implicit_alias) = parse_projection_expression(
                parser,
                &[
                    "AS",
                    "FROM",
                    "WHERE",
                    "GROUP",
                    "HAVING",
                    "QUALIFY",
                    "ORDER",
                    "LIMIT",
                    "OFFSET",
                    "FETCH",
                    "UNION",
                    "INTERSECT",
                    "EXCEPT",
                ],
                &[Symbol::Comma, Symbol::Semicolon, Symbol::RParen],
            )?;
            let alias = if parser.consume_if_word("AS") {
                Some(parser.parse_ident()?)
            } else {
                implicit_alias
            };
            let span = alias
                .as_ref()
                .map_or(expr.span(), |alias| expr.span().cover(alias.span));
            items.push(SelectItem::SingleColumn(AstBox::new(SingleColumn {
                expression: expr,
                alias,
                span,
            })));
        }
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    Ok(items)
}

fn qualified_wildcard_ahead(parser: &StatementParser<'_, '_>) -> bool {
    let mut position = parser.position;
    loop {
        while matches!(
            parser.tokens.get(position).map(|token| &token.kind),
            Some(TokenKind::Trivia(_))
        ) {
            position += 1;
        }
        let Some(token) = parser.tokens.get(position) else {
            return false;
        };
        let identifier = match token.kind {
            TokenKind::Ident | TokenKind::QuotedIdent => true,
            TokenKind::Keyword(keyword) => {
                crate::keyword_class(keyword) == crate::KeywordClass::NonReserved
            }
            _ => false,
        };
        if !identifier {
            return false;
        }
        position += 1;
        while matches!(
            parser.tokens.get(position).map(|token| &token.kind),
            Some(TokenKind::Trivia(_))
        ) {
            position += 1;
        }
        if !matches!(
            parser.tokens.get(position).map(|token| &token.kind),
            Some(TokenKind::Symbol(Symbol::Dot))
        ) {
            return false;
        }
        position += 1;
        while matches!(
            parser.tokens.get(position).map(|token| &token.kind),
            Some(TokenKind::Trivia(_))
        ) {
            position += 1;
        }
        if matches!(
            parser.tokens.get(position).map(|token| &token.kind),
            Some(TokenKind::Symbol(Symbol::Star))
        ) {
            return true;
        }
    }
}

fn parse_projection_expression(
    parser: &mut StatementParser<'_, '_>,
    words: &[&str],
    symbols: &[Symbol],
) -> Result<(Expr, Option<crate::ast::Ident>), crate::ParseError> {
    let begin = parser.position;
    let end = expression_end(parser, words, symbols);
    let full = parse_expression_range(parser, begin, end);
    if let Ok(expression) = full {
        parser.position = end;
        parser.skip_trivia();
        return Ok((expression, None));
    }
    let alias_index = (begin..end)
        .rev()
        .find(|index| !matches!(parser.tokens[*index].kind, TokenKind::Trivia(_)))
        .filter(|index| {
            matches!(
                parser.tokens[*index].kind,
                TokenKind::Ident | TokenKind::QuotedIdent
            )
        })
        .ok_or_else(|| parser.unexpected("expression"))?;
    let expression = parse_expression_range(parser, begin, alias_index)?;
    parser.position = alias_index;
    parser.skip_trivia();
    let alias = parser.parse_ident()?;
    Ok((expression, Some(alias)))
}

fn parse_from(parser: &mut StatementParser<'_, '_>) -> Result<Relation, crate::ParseError> {
    let mut result = None;
    loop {
        let mut left = parse_table_factor(parser)?;
        loop {
            let natural = if parser.current_is_keyword(Keyword::Natural) {
                Some(parser.consume_word("NATURAL")?)
            } else {
                None
            };
            let Some(operator) = parse_join_operator(parser) else {
                if natural.is_some() {
                    return Err(parser.unexpected("JOIN after NATURAL"));
                }
                break;
            };
            let right = parse_table_factor(parser)?;
            let criteria = if let Some(span) = natural {
                Some(JoinCriteria::Natural(NaturalJoin { span }))
            } else if parser.consume_if_word("ON") {
                let expression = parse_expression_until(
                    parser,
                    join_or_query_clause_words(),
                    &[Symbol::Comma, Symbol::Semicolon, Symbol::RParen],
                )?;
                let span = expression.span();
                Some(JoinCriteria::On(JoinOn { expression, span }))
            } else if parser.consume_if_word("USING") {
                let start = parser.consume_symbol(Symbol::LParen)?.start();
                let mut columns = vec![parser.parse_ident()?];
                while parser.consume_if_symbol(Symbol::Comma) {
                    columns.push(parser.parse_ident()?);
                }
                let end = parser.consume_symbol(Symbol::RParen)?.end();
                Some(JoinCriteria::Using(JoinUsing {
                    columns,
                    span: Span::new(start, end),
                }))
            } else {
                None
            };
            let kind = match operator {
                JoinOperator::Cross if criteria.is_none() => JoinType::Cross,
                JoinOperator::Inner | JoinOperator::InnerExplicit if criteria.is_some() => {
                    JoinType::Inner
                }
                JoinOperator::LeftOuter | JoinOperator::LeftOuterExplicit if criteria.is_some() => {
                    JoinType::Left
                }
                JoinOperator::RightOuter | JoinOperator::RightOuterExplicit
                    if criteria.is_some() =>
                {
                    JoinType::Right
                }
                JoinOperator::FullOuter | JoinOperator::FullOuterExplicit if criteria.is_some() => {
                    JoinType::Full
                }
                other => JoinType::Legacy(other),
            };
            let end = criteria
                .as_ref()
                .map_or(right.span().end(), |criteria| match criteria {
                    JoinCriteria::On(n) => n.span.end(),
                    JoinCriteria::Using(n) => n.span.end(),
                    JoinCriteria::Natural(_) => right.span().end(),
                });
            let span = Span::new(left.span().start(), end);
            left = Relation::Join(AstBox::new(Join {
                left: AstBox::new(left),
                right: AstBox::new(right),
                kind,
                criteria,
                span,
            }));
        }
        result = Some(match result {
            None => left,
            Some(previous) => crate::build::join(previous, JoinType::Implicit, left, None),
        });
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    Ok(result.expect("FROM parses at least one relation"))
}

fn finish_relation(
    parser: &mut StatementParser<'_, '_>,
    relation: Relation,
) -> Result<Relation, crate::ParseError> {
    let Some(alias) = parse_optional_table_alias(parser)? else {
        return Ok(relation);
    };
    let span = Span::new(relation.span().start(), alias.span.end());
    Ok(Relation::Aliased(AstBox::new(AliasedRelation {
        relation: AstBox::new(relation),
        alias: alias.name,
        column_names: alias.columns,
        span,
    })))
}

fn parse_table_factor(parser: &mut StatementParser<'_, '_>) -> Result<Relation, crate::ParseError> {
    let start = parser.current_span().start();
    let lateral = parser.consume_if_word("LATERAL");
    if parser.consume_if_symbol(Symbol::LParen) {
        if !(parser.current_is_keyword(Keyword::Select)
            || parser.current_is_keyword(Keyword::Values)
            || parser.current_is_keyword(Keyword::With)
            || parser.current_is_symbol(Symbol::LParen))
        {
            return Err(parser.unexpected("query after '(' in FROM"));
        }
        let mut query = parse_query(parser)?;
        query.session_properties.clear();
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        let span = Span::new(start, end);
        let relation = if lateral {
            Relation::Lateral(AstBox::new(Lateral {
                query: AstBox::new(query),
                span,
            }))
        } else {
            Relation::TableSubquery(AstBox::new(TableSubquery {
                query: AstBox::new(query),
                span,
            }))
        };
        return finish_relation(parser, relation);
    }
    if parser.current_is_keyword(Keyword::Unnest) {
        parser.consume_word("UNNEST")?;
        parser.consume_symbol(Symbol::LParen)?;
        let mut expressions = Vec::new();
        loop {
            expressions.push(parse_expression_until(
                parser,
                &[],
                &[Symbol::Comma, Symbol::RParen],
            )?);
            if !parser.consume_if_symbol(Symbol::Comma) {
                break;
            }
        }
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        return finish_relation(
            parser,
            Relation::Unnest(AstBox::new(Unnest {
                expressions: expressions.into(),
                with_ordinality: false,
                span: Span::new(start, end),
            })),
        );
    }
    if parser.current_is_word("TABLE") {
        parser.consume_word("TABLE")?;
        parser.consume_symbol(Symbol::LParen)?;
        let expr = parse_table_function_expression_until(parser, &[], &[Symbol::RParen])?;
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        let relation = old_table_function(
            expr,
            lateral,
            crate::ast::legacy::TableFunctionSyntax::TableWrapper,
            Span::new(start, end),
        );
        return finish_relation(parser, relation);
    }
    let function_start = parser.position;
    let name = parser.parse_object_name()?;
    if parser.current_is_symbol(Symbol::LParen) {
        let end = table_function_end(parser)?;
        let expr = parse_table_function_expression_range(parser, function_start, end)?;
        let span = Span::new(start, expr.span().end());
        parser.position = end;
        parser.skip_trivia();
        let relation = old_table_function(
            expr,
            lateral,
            crate::ast::legacy::TableFunctionSyntax::BareCall,
            span,
        );
        return finish_relation(parser, relation);
    }
    let query_period = parse_table_version(parser)?;
    let end = query_period
        .as_ref()
        .map_or(name.span.end(), |period| period.span.end());
    // Keep the actual whole qualified-name token, including its quote facts.
    finish_relation(
        parser,
        Relation::Table(AstBox::new(Table {
            name,
            query_period,
            branch: None,
            span: Span::new(start, end),
        })),
    )
}

fn old_table_function(
    expr: Expr,
    lateral: bool,
    syntax: crate::ast::legacy::TableFunctionSyntax,
    span: Span,
) -> Relation {
    let plain=match &expr {
        Expr::FunctionCall(call)=>!call.distinct && call.order_by.is_none() && call.filter.is_none()
            && call.window.is_none() && call.null_treatment.is_none() && call.processing_mode.is_none()
            && call.arguments.iter().all(|arg|!matches!(&arg.value,Expr::Binary(binary) if binary.operator==crate::ast::BinaryOperator::NamedArgument && !matches!(binary.left.as_ref(),Expr::Identifier(_)))),
        _=>false,
    };
    if !plain {
        return Relation::LegacyTableFunction(AstBox::new(crate::ast::legacy::TableFunction {
            expr,
            lateral,
            syntax,
            span,
        }));
    }
    let Expr::FunctionCall(call) = expr else {
        unreachable!()
    };
    let call = call.into_inner();
    let arguments = call
        .arguments
        .into_iter()
        .map(|argument| {
            let crate::ast::CallArgument { name, value, span } = argument;
            let (name, value) = match value {
                Expr::Binary(binary)
                    if binary.operator == crate::ast::BinaryOperator::NamedArgument =>
                {
                    let Expr::Identifier(name) = *binary.left else {
                        unreachable!()
                    };
                    (Some(name), *binary.right)
                }
                value => (name, value),
            };
            TableFunctionArgument {
                name,
                value: TableFunctionArgumentValue::Expression(value),
                span,
            }
        })
        .collect();
    Relation::TableFunction(AstBox::new(TableFunctionInvocation {
        name: call.name,
        arguments,
        copartitioning: Vec::new(),
        span,
    }))
}

fn table_function_end(parser: &StatementParser<'_, '_>) -> Result<usize, crate::ParseError> {
    let mut index = parser.position;
    let mut depth = 0usize;
    while let Some(token) = parser.tokens.get(index) {
        match token.kind {
            TokenKind::Symbol(Symbol::LParen) => depth += 1,
            TokenKind::Symbol(Symbol::RParen) => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| parser.unexpected("table function"))?;
                if depth == 0 {
                    return Ok(index + 1);
                }
            }
            TokenKind::End => break,
            _ => {}
        }
        index += 1;
    }
    Err(parser.unexpected(") after table function"))
}

fn parse_table_version(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Option<QueryPeriod>, crate::ParseError> {
    if !parser.current_is_keyword(Keyword::For) {
        return Ok(None);
    }
    let start = parser.consume_word("FOR")?.start();
    let kind = if parser.consume_if_word("VERSION") {
        QueryPeriodRangeType::Version
    } else if parser.consume_if_word("SYSTEM_TIME") {
        QueryPeriodRangeType::Timestamp
    } else if parser.consume_if_word("SYSTEM") {
        parser.consume_word("TIME")?;
        QueryPeriodRangeType::Timestamp
    } else {
        return Err(parser.unexpected("VERSION or SYSTEM_TIME after FOR"));
    };
    parser.consume_word("AS")?;
    parser.consume_word("OF")?;
    let value = parse_atomic_expression(parser)?;
    Ok(Some(QueryPeriod {
        range_type: kind,
        start: None,
        span: Span::new(start, value.span().end()),
        end: Some(AstBox::new(value)),
    }))
}

fn parse_atomic_expression(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Expr, crate::ParseError> {
    let begin = parser.position;
    let mut end = begin;
    let mut significant = 0usize;
    while let Some(token) = parser.tokens.get(end) {
        if !matches!(token.kind, TokenKind::Trivia(_)) {
            significant += 1;
            if significant == 1
                && matches!(token.kind, TokenKind::Symbol(Symbol::Plus | Symbol::Minus))
            {
                end += 1;
                continue;
            }
            end += 1;
            break;
        }
        end += 1;
    }
    if significant == 0 {
        return Err(parser.unexpected("table version expression"));
    }
    let expression = parse_expression_range(parser, begin, end)?;
    parser.position = end;
    parser.skip_trivia();
    Ok(expression)
}

struct ParsedTableAlias {
    name: Ident,
    columns: Option<Vec<Ident>>,
    span: Span,
}

fn parse_optional_table_alias(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Option<ParsedTableAlias>, crate::ParseError> {
    let explicit_as = parser.consume_if_word("AS");
    let has_alias = matches!(
        parser.current().map(|token| &token.kind),
        Some(
            TokenKind::Ident
                | TokenKind::QuotedIdent
                | TokenKind::Keyword(
                    Keyword::At | Keyword::Catalog | Keyword::Stats | Keyword::Unnest
                )
        )
    );
    if !explicit_as && !has_alias {
        return Ok(None);
    }
    let name = parser.parse_ident()?;
    let start = name.span.start();
    let mut columns = None;
    let mut end = name.span.end();
    if parser.consume_if_symbol(Symbol::LParen) {
        let mut names = Vec::new();
        loop {
            let column = parser.parse_ident()?;
            names.push(column);
            if !parser.consume_if_symbol(Symbol::Comma) {
                break;
            }
        }
        end = parser.consume_symbol(Symbol::RParen)?.end();
        columns = Some(names);
    }
    Ok(Some(ParsedTableAlias {
        name,
        columns,
        span: Span::new(start, end),
    }))
}

fn parse_join_operator(parser: &mut StatementParser<'_, '_>) -> Option<JoinOperator> {
    if parser.consume_if_word("JOIN") {
        return Some(JoinOperator::Inner);
    }
    if parser.consume_if_word("INNER") && parser.consume_if_word("JOIN") {
        return Some(JoinOperator::InnerExplicit);
    }
    if parser.consume_if_word("CROSS") {
        parser.consume_if_word("JOIN");
        return Some(JoinOperator::Cross);
    }
    if parser.consume_if_word("LEFT") {
        let explicit_outer = parser.consume_if_word("OUTER");
        if parser.consume_if_word("SEMI") {
            parser.consume_if_word("JOIN");
            return Some(JoinOperator::LeftSemi);
        }
        if parser.consume_if_word("ANTI") {
            parser.consume_if_word("JOIN");
            return Some(JoinOperator::LeftAnti);
        }
        parser.consume_if_word("JOIN");
        return Some(if explicit_outer {
            JoinOperator::LeftOuterExplicit
        } else {
            JoinOperator::LeftOuter
        });
    }
    if parser.consume_if_word("RIGHT") {
        let explicit_outer = parser.consume_if_word("OUTER");
        if parser.consume_if_word("SEMI") {
            parser.consume_if_word("JOIN");
            return Some(JoinOperator::RightSemi);
        }
        if parser.consume_if_word("ANTI") {
            parser.consume_if_word("JOIN");
            return Some(JoinOperator::RightAnti);
        }
        parser.consume_if_word("JOIN");
        return Some(if explicit_outer {
            JoinOperator::RightOuterExplicit
        } else {
            JoinOperator::RightOuter
        });
    }
    if parser.consume_if_word("FULL") {
        let explicit_outer = parser.consume_if_word("OUTER");
        parser.consume_if_word("JOIN");
        return Some(if explicit_outer {
            JoinOperator::FullOuterExplicit
        } else {
            JoinOperator::FullOuter
        });
    }
    None
}

fn parse_group_by(parser: &mut StatementParser<'_, '_>) -> Result<GroupBy, crate::ParseError> {
    let start = parser.current_span().start();
    let kind = if parser.consume_if_word("ROLLUP") {
        Some(GroupingSetsKind::Rollup)
    } else if parser.consume_if_word("CUBE") {
        Some(GroupingSetsKind::Cube)
    } else {
        None
    };
    if let Some(kind) = kind {
        let (expressions, span) = parse_parenthesized_expressions(parser)?;
        let span = Span::new(start, span.end());
        // The old zero-argument forms denote exactly one empty grouping set.
        // A ROLLUP/CUBE containing one empty member would duplicate that group.
        let (kind, sets) = if expressions.is_empty() {
            (GroupingSetsKind::Explicit, vec![Vec::new().into()].into())
        } else {
            (
                kind,
                expressions
                    .into_iter()
                    .map(|expression| vec![expression].into())
                    .collect(),
            )
        };
        return Ok(GroupBy {
            distinct: false,
            elements: vec![GroupingElement::Sets(GroupingSets { kind, sets, span })].into(),
            span,
        });
    }
    if parser.consume_if_word("GROUPING") {
        parser.consume_word("SETS")?;
        parser.consume_symbol(Symbol::LParen)?;
        let mut sets = Vec::new();
        loop {
            let (expressions, _) = parse_parenthesized_expressions(parser)?;
            sets.push(expressions.into());
            if !parser.consume_if_symbol(Symbol::Comma) {
                break;
            }
        }
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        let span = Span::new(start, end);
        return Ok(GroupBy {
            distinct: false,
            elements: vec![GroupingElement::Sets(GroupingSets {
                kind: GroupingSetsKind::Explicit,
                sets: sets.into(),
                span,
            })]
            .into(),
            span,
        });
    }
    let mut elements = Vec::new();
    loop {
        // Preserve the old expression grammar. A Row expression is not evidence
        // that the source authored a multi-column groupingSet production.
        let expression = parse_expression_until(
            parser,
            query_clause_words(),
            &[Symbol::Comma, Symbol::Semicolon, Symbol::RParen],
        )?;
        let span = expression.span();
        elements.push(GroupingElement::Simple(SimpleGroupBy {
            columns: vec![expression].into(),
            span,
        }));
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    let end = elements
        .last()
        .map_or(start, |element| element.span().end());
    Ok(GroupBy {
        distinct: false,
        elements: elements.into(),
        span: Span::new(start, end),
    })
}

fn parse_parenthesized_expressions(
    parser: &mut StatementParser<'_, '_>,
) -> Result<(Vec<Expr>, Span), crate::ParseError> {
    let start = parser.consume_symbol(Symbol::LParen)?.start();
    let mut expressions = Vec::new();
    if !parser.current_is_symbol(Symbol::RParen) {
        loop {
            expressions.push(parse_expression_until(
                parser,
                query_clause_words(),
                &[Symbol::Comma, Symbol::RParen],
            )?);
            if !parser.consume_if_symbol(Symbol::Comma) {
                break;
            }
        }
    }
    let end = parser.consume_symbol(Symbol::RParen)?.end();
    Ok((expressions, Span::new(start, end)))
}

fn parse_order_by(
    parser: &mut StatementParser<'_, '_>,
    order_start: usize,
) -> Result<OrderBy, crate::ParseError> {
    let mut items = Vec::new();
    loop {
        let expr = parse_expression_until(
            parser,
            &[
                "ASC",
                "DESC",
                "NULLS",
                "LIMIT",
                "OFFSET",
                "FETCH",
                "UNION",
                "INTERSECT",
                "EXCEPT",
            ],
            &[Symbol::Comma, Symbol::Semicolon, Symbol::RParen],
        )?;
        let mut item_end = expr.span().end();
        let asc = if parser.current_is_word("ASC") {
            item_end = parser.consume_word("ASC")?.end();
            Some(true)
        } else if parser.current_is_word("DESC") {
            item_end = parser.consume_word("DESC")?.end();
            Some(false)
        } else {
            None
        };
        let nulls_first = if parser.consume_if_word("NULLS") {
            if parser.current_is_word("FIRST") {
                item_end = parser.consume_word("FIRST")?.end();
                Some(true)
            } else if parser.current_is_word("LAST") {
                item_end = parser.consume_word("LAST")?.end();
                Some(false)
            } else {
                return Err(parser.unexpected("FIRST or LAST after NULLS"));
            }
        } else {
            None
        };
        let span = Span::new(expr.span().start(), item_end);
        items.push(SortItem {
            sort_key: expr,
            ordering: if asc == Some(false) {
                SortOrdering::Descending
            } else {
                SortOrdering::Ascending
            },
            null_ordering: match nulls_first {
                Some(true) => NullOrdering::First,
                Some(false) => NullOrdering::Last,
                None => NullOrdering::Undefined,
            },
            span,
        });
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    let end = items.last().expect("nonempty parsed ORDER BY").span.end();
    Ok(OrderBy {
        items: items.into(),
        span: Span::new(order_start, end),
    })
}

fn parse_fetch(
    parser: &mut StatementParser<'_, '_>,
    start: usize,
) -> Result<FetchFirst, crate::ParseError> {
    let mut end = start + "FETCH".len();
    for word in ["FIRST", "NEXT"] {
        if parser.current_is_word(word) {
            end = parser.consume_word(word)?.end();
        }
    }
    let row_count = if parser.current_is_word("ROW")
        || parser.current_is_word("ROWS")
        || parser.current_is_word("ONLY")
    {
        None
    } else {
        let count = parse_row_count(
            parser,
            false,
            &[
                "ROW", "ROWS", "ONLY", "PERCENT", "WITH", "LIMIT", "OFFSET", "FETCH",
            ],
            &[Symbol::Semicolon, Symbol::RParen],
        )?;
        end = count.span().end();
        Some(count)
    };
    if parser.current_is_word("PERCENT") {
        return Err(parser.unexpected("FETCH without PERCENT"));
    }
    for word in ["ROW", "ROWS", "ONLY"] {
        if parser.current_is_word(word) {
            end = parser.consume_word(word)?.end();
        }
    }
    Ok(FetchFirst {
        row_count,
        with_ties: false,
        span: Span::new(start, end),
    })
}

fn parse_row_count(
    parser: &mut StatementParser<'_, '_>,
    allow_all: bool,
    words: &[&str],
    symbols: &[Symbol],
) -> Result<Expr, crate::ParseError> {
    if allow_all && parser.current_is_word("ALL") {
        let span = parser.consume_word("ALL")?;
        return Ok(Expr::AllRows(AllRows { span }));
    }
    let node = parse_expression_until(parser, words, symbols)?;
    if matches!(
        &node,
        Expr::Literal(Literal {
            kind: LiteralKind::Long { .. },
            ..
        }) | Expr::Parameter(_)
    ) {
        return Ok(node);
    }
    Err(crate::ParseError::UnexpectedToken {
        expected: "an integer row count",
        found: "a non-long row-count expression".to_owned(),
        span: node.span(),
    })
}

fn parse_values(parser: &mut StatementParser<'_, '_>) -> Result<Values, crate::ParseError> {
    let start = parser.consume_word("VALUES")?.start();
    let mut rows = Vec::new();
    loop {
        let row_start = parser.consume_symbol(Symbol::LParen)?.start();
        let mut row = Vec::new();
        loop {
            row.push(parse_expression_until(
                parser,
                &[],
                &[Symbol::Comma, Symbol::RParen],
            )?);
            if !parser.consume_if_symbol(Symbol::Comma) {
                break;
            }
        }
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        rows.push(Expr::Row(RowExpr {
            fields: row
                .into_iter()
                .map(|expression| RowField {
                    name: None,
                    span: expression.span(),
                    expression,
                })
                .collect(),
            span: Span::new(row_start, end),
        }));
        if !parser.consume_if_symbol(Symbol::Comma) {
            return Ok(Values {
                rows: rows.into(),
                span: Span::new(start, end),
            });
        }
    }
}

fn parse_expression_until(
    parser: &mut StatementParser<'_, '_>,
    words: &[&str],
    symbols: &[Symbol],
) -> Result<Expr, crate::ParseError> {
    let begin = parser.position;
    let end = expression_end(parser, words, symbols);
    if end == begin {
        return Err(parser.unexpected("expression"));
    }
    let expression = parse_expression_range(parser, begin, end)?;
    parser.position = end;
    parser.skip_trivia();
    Ok(expression)
}

fn expression_end(parser: &StatementParser<'_, '_>, words: &[&str], symbols: &[Symbol]) -> usize {
    let begin = parser.position;
    let mut end = begin;
    let mut nesting = 0usize;
    while let Some(token) = parser.tokens.get(end) {
        match token.kind {
            TokenKind::End => break,
            TokenKind::Symbol(Symbol::LParen | Symbol::LBracket | Symbol::LBrace) => nesting += 1,
            TokenKind::Symbol(Symbol::RParen | Symbol::RBracket | Symbol::RBrace)
                if nesting > 0 =>
            {
                nesting -= 1
            }
            TokenKind::Symbol(symbol) if nesting == 0 && symbols.contains(&symbol) => break,
            _ if nesting == 0
                && words.iter().any(|word| token_is_word(parser, token, word))
                && !is_distinct_from_separator(parser, begin, end) =>
            {
                break;
            }
            _ => {}
        }
        end += 1;
    }
    end
}

// FROM in IS [NOT] DISTINCT FROM belongs to the expression, before any
// subsequent FROM that starts the relation clause.
fn is_distinct_from_separator(
    parser: &StatementParser<'_, '_>,
    begin: usize,
    index: usize,
) -> bool {
    if !token_is_word(parser, &parser.tokens[index], "FROM") {
        return false;
    }
    let mut previous = parser.tokens[begin..index]
        .iter()
        .rev()
        .filter(|token| !matches!(token.kind, TokenKind::Trivia(_)));
    if !previous
        .next()
        .is_some_and(|token| token_is_word(parser, token, "DISTINCT"))
    {
        return false;
    }
    let Some(mut token) = previous.next() else {
        return false;
    };
    if token_is_word(parser, token, "NOT") {
        let Some(before_not) = previous.next() else {
            return false;
        };
        token = before_not;
    }
    token_is_word(parser, token, "IS")
}

fn parse_expression_range(
    parser: &StatementParser<'_, '_>,
    begin: usize,
    end: usize,
) -> Result<Expr, crate::ParseError> {
    let boundary = parser
        .tokens
        .get(end)
        .map_or(parser.boundary_span, |token| token.span);
    PrattParser::bounded(parser.source, &parser.tokens[begin..end], boundary).parse()
}

fn parse_table_function_expression_until(
    parser: &mut StatementParser<'_, '_>,
    words: &[&str],
    symbols: &[Symbol],
) -> Result<Expr, crate::ParseError> {
    let begin = parser.position;
    let end = expression_end(parser, words, symbols);
    if end == begin {
        return Err(parser.unexpected("table function expression"));
    }
    let expression = parse_table_function_expression_range(parser, begin, end)?;
    parser.position = end;
    parser.skip_trivia();
    Ok(expression)
}

fn parse_table_function_expression_range(
    parser: &StatementParser<'_, '_>,
    begin: usize,
    end: usize,
) -> Result<Expr, crate::ParseError> {
    let boundary = parser
        .tokens
        .get(end)
        .map_or(parser.boundary_span, |token| token.span);
    parse_table_function_expression(parser.source, &parser.tokens[begin..end], boundary)
}

fn token_is_word(parser: &StatementParser<'_, '_>, token: &Token, word: &str) -> bool {
    matches!(token.kind, TokenKind::Ident | TokenKind::Keyword(_))
        && parser.source_slice(token.span).eq_ignore_ascii_case(word)
}

fn query_clause_words() -> &'static [&'static str] {
    &[
        "GROUP",
        "HAVING",
        "QUALIFY",
        "WINDOW",
        "ORDER",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "UNION",
        "INTERSECT",
        "EXCEPT",
    ]
}

fn join_or_query_clause_words() -> &'static [&'static str] {
    &[
        "JOIN",
        "INNER",
        "LEFT",
        "RIGHT",
        "FULL",
        "CROSS",
        "WHERE",
        "GROUP",
        "HAVING",
        "QUALIFY",
        "WINDOW",
        "ORDER",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "UNION",
        "INTERSECT",
        "EXCEPT",
    ]
}
