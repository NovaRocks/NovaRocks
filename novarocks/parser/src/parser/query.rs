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
    ast::{
        Cte, ExplainFormat, ExplainQuery, Expr, Fetch, GroupBy, Join, JoinConstraint, JoinOperator,
        NamedWindow, Offset, OffsetRows, OrderByExpr, Query, Select, SelectHint, SelectHintValue,
        SelectItem, SelectQuantifier, SetExpr, SetOperation, SetOperator, SetQuantifier, Statement,
        TableFactor, TableVersion, TableVersionKind, TableWithJoins, Values, WildcardOptions, With,
    },
    lex,
    token::{Keyword, Symbol},
};

use super::{
    StatementParser,
    pratt::{PrattParser, parse_table_function_expression, parse_window_spec},
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

/// Bound the syntax-tree depth of a homogeneous UNION ALL chain. Analysis
/// still folds its operands in source order, retaining pairwise type widening.
fn balance_union_all(expression: SetExpr) -> SetExpr {
    if !matches!(&expression, SetExpr::SetOperation(operation)
        if operation.operator == SetOperator::Union && operation.quantifier == SetQuantifier::All)
    {
        return expression;
    }
    let mut pending = vec![expression];
    let mut operands = Vec::new();
    while let Some(current) = pending.pop() {
        match current {
            SetExpr::SetOperation(operation)
                if operation.operator == SetOperator::Union
                    && operation.quantifier == SetQuantifier::All =>
            {
                pending.push(*operation.right);
                pending.push(*operation.left);
            }
            other => operands.push(other),
        }
    }
    while operands.len() > 1 {
        let mut next_level = Vec::with_capacity(operands.len().div_ceil(2));
        let mut values = operands.into_iter();
        while let Some(left) = values.next() {
            if let Some(right) = values.next() {
                let span = Span::new(left.span().start(), right.span().end());
                next_level.push(SetExpr::SetOperation(SetOperation {
                    left: Box::new(left),
                    operator: SetOperator::Union,
                    quantifier: SetQuantifier::All,
                    right: Box::new(right),
                    span,
                }));
            } else {
                next_level.push(left);
            }
        }
        operands = next_level;
    }
    operands.pop().expect("a UNION ALL chain has operands")
}

pub(super) fn parse_query(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Query, crate::ParseError> {
    let start = parser.current_span().start();
    let with = if parser.consume_if_word("WITH") {
        Some(parse_with(parser, start)?)
    } else {
        None
    };
    let mut body = parse_set_expr(parser)?;
    while let Some(operator) = parse_set_operator(parser) {
        let operator_start = body.span().start();
        let quantifier = if parser.consume_if_word("ALL") {
            SetQuantifier::All
        } else if parser.consume_if_word("DISTINCT") {
            SetQuantifier::Distinct
        } else {
            SetQuantifier::None
        };
        let right = parse_set_expr(parser)?;
        if operator != SetOperator::Union || quantifier != SetQuantifier::All {
            body = balance_union_all(body);
        }
        let span = Span::new(operator_start, right.span().end());
        body = SetExpr::SetOperation(SetOperation {
            left: Box::new(body),
            operator,
            quantifier,
            right: Box::new(right),
            span,
        });
    }
    body = balance_union_all(body);

    let mut order_by = Vec::new();
    if parser.consume_if_word("ORDER") {
        parser.consume_word("BY")?;
        order_by = parse_order_by(parser)?;
    }
    let (limit, limit_offset, limit_comma_offset) = if parser.consume_if_word("LIMIT") {
        let first = parse_expression_until(
            parser,
            &["OFFSET", "FETCH"],
            &[Symbol::Comma, Symbol::Semicolon, Symbol::RParen],
        )?;
        if parser.consume_if_symbol(Symbol::Comma) {
            let limit = parse_expression_until(
                parser,
                &["OFFSET", "FETCH"],
                &[Symbol::Semicolon, Symbol::RParen],
            )?;
            let offset_span = Span::new(first.span().start(), first.span().end());
            (
                Some(limit),
                Some(Offset {
                    value: first,
                    rows: OffsetRows::None,
                    span: offset_span,
                }),
                true,
            )
        } else {
            (Some(first), None, false)
        }
    } else {
        (None, None, false)
    };
    let offset = if parser.consume_if_word("OFFSET") {
        if limit_offset.is_some() {
            return Err(parser.unexpected("end of LIMIT offset syntax"));
        }
        let value = parse_expression_until(
            parser,
            &["ROW", "ROWS", "FETCH"],
            &[Symbol::Semicolon, Symbol::RParen],
        )?;
        let end = value.span().end();
        let rows = if parser.consume_if_word("ROW") {
            OffsetRows::Row
        } else if parser.consume_if_word("ROWS") {
            OffsetRows::Rows
        } else {
            OffsetRows::None
        };
        Some(Offset {
            value,
            rows,
            span: Span::new(start, end),
        })
    } else {
        limit_offset
    };
    let fetch = if parser.consume_if_word("FETCH") {
        Some(parse_fetch(parser, start)?)
    } else {
        None
    };
    let end = fetch
        .as_ref()
        .map_or_else(
            || {
                offset.as_ref().map_or_else(
                    || {
                        limit.as_ref().map_or_else(
                            || order_by.last().map_or(body.span(), |order| order.span),
                            |limit| limit.span(),
                        )
                    },
                    |offset| offset.span,
                )
            },
            |fetch| fetch.span,
        )
        .end();
    Ok(Query {
        with,
        body: Box::new(body),
        order_by,
        limit,
        offset,
        limit_comma_offset,
        fetch,
        span: Span::new(start, end),
    })
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
        let mut columns = Vec::new();
        if parser.consume_if_symbol(Symbol::LParen) {
            loop {
                columns.push(parser.parse_ident()?);
                if !parser.consume_if_symbol(Symbol::Comma) {
                    break;
                }
            }
            parser.consume_symbol(Symbol::RParen)?;
        }
        parser.consume_word("AS")?;
        parser.consume_symbol(Symbol::LParen)?;
        let query = parse_query(parser)?;
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        ctes.push(Cte {
            name,
            columns,
            query: Box::new(query),
            span: Span::new(cte_start, end),
        });
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    let end = ctes.last().map_or(start, |cte| cte.span.end());
    Ok(With {
        recursive,
        ctes,
        span: Span::new(start, end),
    })
}

fn parse_set_expr(parser: &mut StatementParser<'_, '_>) -> Result<SetExpr, crate::ParseError> {
    if parser.current_is_keyword(Keyword::Select) {
        return Ok(SetExpr::Select(Box::new(parse_select(parser)?)));
    }
    if parser.current_is_keyword(Keyword::Values) {
        return Ok(SetExpr::Values(parse_values(parser)?));
    }
    if parser.current_is_symbol(Symbol::LParen) {
        parser.consume_symbol(Symbol::LParen)?;
        let query = parse_query(parser)?;
        parser.consume_symbol(Symbol::RParen)?;
        return Ok(SetExpr::Query(Box::new(query)));
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

fn parse_select(parser: &mut StatementParser<'_, '_>) -> Result<Select, crate::ParseError> {
    let select = parser.consume_word("SELECT")?;
    let start = select.start();
    let hints = parse_select_hints(parser, select)?;
    let quantifier = if parser.consume_if_word("ALL") {
        SelectQuantifier::All(Span::new(start, parser.current_offset()))
    } else if parser.consume_if_word("DISTINCT") {
        SelectQuantifier::Distinct {
            on: Vec::new(),
            span: Span::new(start, parser.current_offset()),
        }
    } else {
        SelectQuantifier::None
    };
    let projection = parse_projection(parser)?;
    let from = if parser.consume_if_word("FROM") {
        parse_from(parser)?
    } else {
        Vec::new()
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
        parse_group_by(parser)?
    } else {
        GroupBy::None
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
                        from.last().map_or_else(
                            || projection.last().map_or(start, |item| item.span().end()),
                            |relation| relation.span.end(),
                        )
                    },
                    |window| window.span.end(),
                )
            },
            |expr| expr.span().end(),
        );
    Ok(Select {
        hints,
        quantifier,
        projection,
        from,
        selection,
        group_by,
        having,
        qualify,
        windows,
        span: Span::new(start, end),
    })
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
) -> Result<Vec<NamedWindow>, crate::ParseError> {
    let mut windows = Vec::new();
    loop {
        let name = parser.parse_ident()?;
        let start = name.span.start();
        parser.consume_word("AS")?;
        let specification = parse_window_spec_until(parser)?;
        let span = Span::new(start, specification.span.end());
        windows.push(NamedWindow {
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

fn parse_window_spec_until(
    parser: &mut StatementParser<'_, '_>,
) -> Result<crate::ast::WindowSpec, crate::ParseError> {
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
        .map_or_else(|| parser.current_span(), |token| token.span);
    let mut tokens = parser.tokens[begin..end].to_vec();
    tokens.push(Token::new(TokenKind::End, boundary));
    let specification = parse_window_spec(parser.source, &tokens)?;
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
            items.push(SelectItem::Wildcard {
                options: WildcardOptions::default(),
                span,
            });
        } else if qualified_wildcard_ahead(parser) {
            let start = parser.current_span().start();
            let mut prefix = vec![parser.parse_ident()?];
            loop {
                parser.consume_symbol(Symbol::Dot)?;
                if parser.current_is_symbol(Symbol::Star) {
                    let end = parser.consume_symbol(Symbol::Star)?.end();
                    items.push(SelectItem::QualifiedWildcard {
                        prefix,
                        options: WildcardOptions::default(),
                        span: Span::new(start, end),
                    });
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
            if parser.consume_if_word("AS") {
                let alias = parser.parse_ident()?;
                let span = Span::new(expr.span().start(), alias.span.end());
                items.push(SelectItem::ExprWithAlias {
                    expr,
                    alias,
                    explicit_as: true,
                    span,
                });
            } else if let Some(alias) = implicit_alias {
                let span = Span::new(expr.span().start(), alias.span.end());
                items.push(SelectItem::ExprWithAlias {
                    expr,
                    alias,
                    explicit_as: false,
                    span,
                });
            } else {
                items.push(SelectItem::UnnamedExpr(expr));
            }
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

fn parse_from(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<TableWithJoins>, crate::ParseError> {
    let mut relations = Vec::new();
    loop {
        let relation = parse_table_factor(parser)?;
        let start = relation.span().start();
        let mut joins = Vec::new();
        loop {
            let join_start = parser.current_span().start();
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
            let relation = parse_table_factor(parser)?;
            let constraint = if let Some(span) = natural {
                JoinConstraint::Natural(span)
            } else if parser.consume_if_word("ON") {
                JoinConstraint::On(parse_expression_until(
                    parser,
                    join_or_query_clause_words(),
                    &[Symbol::Comma, Symbol::Semicolon, Symbol::RParen],
                )?)
            } else if parser.consume_if_word("USING") {
                let using_start = parser.consume_symbol(Symbol::LParen)?.start();
                let mut columns = vec![parser.parse_ident()?];
                while parser.consume_if_symbol(Symbol::Comma) {
                    columns.push(parser.parse_ident()?);
                }
                let end = parser.consume_symbol(Symbol::RParen)?.end();
                JoinConstraint::Using {
                    columns,
                    span: Span::new(using_start, end),
                }
            } else {
                JoinConstraint::None
            };
            let end = match &constraint {
                JoinConstraint::On(expr) => expr.span().end(),
                JoinConstraint::Using { span, .. } | JoinConstraint::Natural(span) => span.end(),
                JoinConstraint::None => relation.span().end(),
            };
            joins.push(Join {
                relation,
                operator,
                constraint,
                span: Span::new(join_start, end),
            });
        }
        let end = joins
            .last()
            .map_or_else(|| relation.span().end(), |join| join.span.end());
        relations.push(TableWithJoins {
            relation,
            joins,
            span: Span::new(start, end),
        });
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    Ok(relations)
}

fn parse_table_factor(
    parser: &mut StatementParser<'_, '_>,
) -> Result<TableFactor, crate::ParseError> {
    let lateral = parser.consume_if_word("LATERAL");
    if parser.current_is_symbol(Symbol::LParen) {
        let start = parser.consume_symbol(Symbol::LParen)?.start();
        if !(parser.current_is_keyword(Keyword::Select)
            || parser.current_is_keyword(Keyword::Values)
            || parser.current_is_keyword(Keyword::With)
            || parser.current_is_symbol(Symbol::LParen))
        {
            return Err(parser.unexpected("query after '(' in FROM"));
        }
        let subquery = parse_query(parser)?;
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        let alias = parse_optional_table_alias(parser)?;
        let span_end = alias.as_ref().map_or(end, |alias| alias.span.end());
        return Ok(TableFactor::Derived {
            lateral,
            subquery: Box::new(subquery),
            alias,
            span: Span::new(start, span_end),
        });
    }
    if parser.current_is_keyword(Keyword::Unnest) {
        let keyword_span = parser.consume_word("UNNEST")?;
        let start = keyword_span.start();
        parser.consume_symbol(Symbol::LParen)?;
        let mut array_exprs = Vec::new();
        loop {
            array_exprs.push(parse_expression_until(
                parser,
                &[],
                &[Symbol::Comma, Symbol::RParen],
            )?);
            if !parser.consume_if_symbol(Symbol::Comma) {
                break;
            }
        }
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        let alias = parse_optional_table_alias(parser)?;
        let span_end = alias.as_ref().map_or(end, |alias| alias.span.end());
        return Ok(TableFactor::Unnest {
            keyword: crate::ast::Ident {
                value: parser.source_slice(keyword_span).to_owned(),
                quoted: false,
                quote_style: None,
                span: keyword_span,
            },
            lateral,
            array_exprs,
            with_offset: false,
            alias,
            span: Span::new(start, span_end),
        });
    }
    if parser.current_is_word("TABLE") {
        let start = parser.consume_word("TABLE")?.start();
        parser.consume_symbol(Symbol::LParen)?;
        let expr = parse_table_function_expression_until(parser, &[], &[Symbol::RParen])?;
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        let alias = parse_optional_table_alias(parser)?;
        let span_end = alias.as_ref().map_or(end, |alias| alias.span.end());
        return Ok(TableFactor::TableFunction {
            lateral,
            syntax: crate::ast::TableFunctionSyntax::TableWrapper,
            expr,
            alias,
            span: Span::new(start, span_end),
        });
    }
    let function_start = parser.position;
    let name = parser.parse_object_name()?;
    let start = name.span.start();
    if parser.current_is_symbol(Symbol::LParen) {
        let end = table_function_end(parser)?;
        let expr = parse_table_function_expression_range(parser, function_start, end)?;
        parser.position = end;
        parser.skip_trivia();
        let alias = parse_optional_table_alias(parser)?;
        let span_end = alias
            .as_ref()
            .map_or(expr.span().end(), |alias| alias.span.end());
        return Ok(TableFactor::TableFunction {
            lateral,
            syntax: crate::ast::TableFunctionSyntax::BareCall,
            expr,
            alias,
            span: Span::new(start, span_end),
        });
    }
    let (name, metadata) = split_metadata_table_suffix(name);
    let version = parse_table_version(parser)?;
    let relation_end = version.as_ref().map_or_else(
        || {
            metadata
                .as_ref()
                .map_or(name.span.end(), |metadata| metadata.span.end())
        },
        |version| version.span.end(),
    );
    let alias = parse_optional_table_alias(parser)?;
    let end = alias
        .as_ref()
        .map_or(relation_end, |alias| alias.span.end());
    Ok(TableFactor::Table {
        name,
        metadata,
        alias,
        version,
        span: Span::new(start, end),
    })
}

/// Splits the final unquoted object-name part at its first `$` into the base
/// table name and a parser-owned metadata suffix. The split deliberately
/// accepts every non-empty suffix; catalog/provider capability is an analyze
/// or admission concern, not a grammar restriction.
fn split_metadata_table_suffix(
    mut name: crate::ast::ObjectName,
) -> (crate::ast::ObjectName, Option<crate::ast::Ident>) {
    let Some(last) = name.parts.last_mut() else {
        return (name, None);
    };
    if last.quoted {
        return (name, None);
    }
    let Some((base, metadata_type)) = last.value.split_once('$') else {
        return (name, None);
    };
    if base.is_empty() || metadata_type.is_empty() {
        return (name, None);
    }

    let suffix_start = last.span.start() + base.len();
    let base = base.to_owned();
    let metadata_type = metadata_type.to_owned();
    let suffix_end = last.span.end();
    last.value = base;
    last.span = Span::new(last.span.start(), suffix_start);
    name.span = Span::new(name.span.start(), suffix_start);
    (
        name,
        Some(crate::ast::Ident {
            value: metadata_type,
            quoted: false,
            quote_style: None,
            span: Span::new(suffix_start + 1, suffix_end),
        }),
    )
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
) -> Result<Option<TableVersion>, crate::ParseError> {
    if !parser.current_is_keyword(Keyword::For) {
        return Ok(None);
    }
    let start = parser.consume_word("FOR")?.start();
    let kind = if parser.consume_if_word("VERSION") {
        TableVersionKind::ForVersionAsOf
    } else if parser.consume_if_word("SYSTEM_TIME") {
        TableVersionKind::ForSystemTimeAsOf
    } else if parser.consume_if_word("SYSTEM") {
        parser.consume_word("TIME")?;
        TableVersionKind::ForSystemTimeAsOf
    } else {
        return Err(parser.unexpected("VERSION or SYSTEM_TIME after FOR"));
    };
    parser.consume_word("AS")?;
    parser.consume_word("OF")?;
    let value = parse_atomic_expression(parser)?;
    Ok(Some(TableVersion {
        kind,
        span: Span::new(start, value.span().end()),
        value,
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

fn parse_optional_table_alias(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Option<crate::ast::TableAlias>, crate::ParseError> {
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
    let mut columns = Vec::new();
    let mut end = name.span.end();
    if parser.consume_if_symbol(Symbol::LParen) {
        loop {
            let column = parser.parse_ident()?;
            columns.push(column);
            if !parser.consume_if_symbol(Symbol::Comma) {
                break;
            }
        }
        end = parser.consume_symbol(Symbol::RParen)?.end();
    }
    Ok(Some(crate::ast::TableAlias {
        name,
        columns,
        explicit_as,
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
    if parser.consume_if_word("ROLLUP") {
        let (expressions, span) = parse_parenthesized_expressions(parser)?;
        return Ok(GroupBy::Rollup {
            expressions,
            span: Span::new(start, span.end()),
        });
    }
    if parser.consume_if_word("CUBE") {
        let (expressions, span) = parse_parenthesized_expressions(parser)?;
        return Ok(GroupBy::Cube {
            expressions,
            span: Span::new(start, span.end()),
        });
    }
    if parser.consume_if_word("GROUPING") {
        parser.consume_word("SETS")?;
        parser.consume_symbol(Symbol::LParen)?;
        let mut sets = Vec::new();
        loop {
            parser.consume_symbol(Symbol::LParen)?;
            let mut set = Vec::new();
            if !parser.current_is_symbol(Symbol::RParen) {
                loop {
                    set.push(parse_expression_until(
                        parser,
                        query_clause_words(),
                        &[Symbol::Comma, Symbol::RParen],
                    )?);
                    if !parser.consume_if_symbol(Symbol::Comma) {
                        break;
                    }
                }
            }
            parser.consume_symbol(Symbol::RParen)?;
            sets.push(set);
            if !parser.consume_if_symbol(Symbol::Comma) {
                break;
            }
        }
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        return Ok(GroupBy::GroupingSets {
            sets,
            span: Span::new(start, end),
        });
    }
    let mut expressions = Vec::new();
    loop {
        expressions.push(parse_expression_until(
            parser,
            query_clause_words(),
            &[Symbol::Comma, Symbol::Semicolon, Symbol::RParen],
        )?);
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    let end = expressions.last().map_or(start, |expr| expr.span().end());
    Ok(GroupBy::Expressions {
        expressions,
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
) -> Result<Vec<OrderByExpr>, crate::ParseError> {
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
        let asc = if parser.consume_if_word("ASC") {
            Some(true)
        } else if parser.consume_if_word("DESC") {
            Some(false)
        } else {
            None
        };
        let nulls_first = if parser.consume_if_word("NULLS") {
            if parser.consume_if_word("FIRST") {
                Some(true)
            } else if parser.consume_if_word("LAST") {
                Some(false)
            } else {
                return Err(parser.unexpected("FIRST or LAST after NULLS"));
            }
        } else {
            None
        };
        let span = Span::new(expr.span().start(), parser.current_offset());
        items.push(OrderByExpr {
            expr,
            asc,
            nulls_first,
            span,
        });
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    Ok(items)
}

fn parse_fetch(
    parser: &mut StatementParser<'_, '_>,
    start: usize,
) -> Result<Fetch, crate::ParseError> {
    parser.consume_if_word("FIRST");
    parser.consume_if_word("NEXT");
    let quantity = if parser.current_is_word("ROW")
        || parser.current_is_word("ROWS")
        || parser.current_is_word("ONLY")
    {
        None
    } else {
        Some(parse_expression_until(
            parser,
            &["ROW", "ROWS", "ONLY"],
            &[Symbol::Semicolon, Symbol::RParen],
        )?)
    };
    parser.consume_if_word("ROW");
    parser.consume_if_word("ROWS");
    parser.consume_if_word("ONLY");
    let end = quantity.as_ref().map_or(start, |expr| expr.span().end());
    Ok(Fetch {
        quantity,
        percent: false,
        with_ties: false,
        span: Span::new(start, end),
    })
}

fn parse_values(parser: &mut StatementParser<'_, '_>) -> Result<Values, crate::ParseError> {
    let start = parser.consume_word("VALUES")?.start();
    let mut rows = Vec::new();
    loop {
        parser.consume_symbol(Symbol::LParen)?;
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
        rows.push(row);
        if !parser.consume_if_symbol(Symbol::Comma) {
            return Ok(Values {
                rows,
                explicit_row: false,
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
            _ if nesting == 0 && words.iter().any(|word| token_is_word(parser, token, word)) => {
                break;
            }
            _ => {}
        }
        end += 1;
    }
    end
}

fn parse_expression_range(
    parser: &StatementParser<'_, '_>,
    begin: usize,
    end: usize,
) -> Result<Expr, crate::ParseError> {
    let mut expression_tokens: Vec<Token> = parser.tokens[begin..end].to_vec();
    let boundary = parser
        .tokens
        .get(end)
        .map_or_else(|| parser.current_span(), |token| token.span);
    expression_tokens.push(Token::new(TokenKind::End, boundary));
    PrattParser::new(parser.source, &expression_tokens).parse()
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
    let mut expression_tokens: Vec<Token> = parser.tokens[begin..end].to_vec();
    let boundary = parser
        .tokens
        .get(end)
        .map_or_else(|| parser.current_span(), |token| token.span);
    expression_tokens.push(Token::new(TokenKind::End, boundary));
    parse_table_function_expression(parser.source, &expression_tokens)
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
