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

//! Row DML grammar owned by SQLP-5.

use crate::ast::legacy::TableAlias;
use crate::{
    ParseError, Span, Token, TokenKind,
    ast::{
        AddEqualityDelete, Assignment, Delete, DmlStatement, Insert, InsertPartitionEntry,
        InsertPartitions, Merge, MergeClause, MergeMatchedAction, MergeNotMatchedAction,
        MutationSource, Statement, Update,
    },
    token::Symbol,
};

use super::{StatementParser, pratt::PrattParser, query};

pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("INSERT") {
        return Ok(Some(Statement::Dml(DmlStatement::Insert(parse_insert(
            parser,
        )?))));
    }
    if parser.current_is_word("DELETE") {
        return Ok(Some(Statement::Dml(DmlStatement::Delete(parse_delete(
            parser,
        )?))));
    }
    if parser.current_is_word("UPDATE") {
        return Ok(Some(Statement::Dml(DmlStatement::Update(parse_update(
            parser,
        )?))));
    }
    if parser.current_is_word("MERGE") {
        return Ok(Some(Statement::Dml(DmlStatement::Merge(parse_merge(
            parser,
        )?))));
    }
    if looks_like_equality_delete(parser) {
        return Ok(Some(Statement::Dml(DmlStatement::AddEqualityDelete(
            parse_equality_delete(parser)?,
        ))));
    }
    Ok(None)
}

fn parse_insert(parser: &mut StatementParser<'_, '_>) -> Result<Insert, ParseError> {
    let start = parser.consume_word("INSERT")?.start();
    let overwrite = parser.consume_if_word("OVERWRITE");
    let dynamic_start = if overwrite && parser.current_is_word("PARTITIONS") {
        let start = parser.current_offset();
        parser.consume_word("PARTITIONS")?;
        Some(start)
    } else {
        None
    };
    let has_into = parser.consume_if_word("INTO");
    parser.consume_if_word("TABLE");
    let target = parser.parse_object_name()?;
    let columns = if parser.current_is_symbol(Symbol::LParen) {
        parse_ident_list(parser)?
    } else {
        Vec::new()
    };
    let partitions = if let Some(partition_start) = dynamic_start {
        Some(InsertPartitions {
            entries: Vec::new(),
            dynamic: true,
            span: Span::new(partition_start, target.span.end()),
        })
    } else if parser.current_is_word("PARTITION") || parser.current_is_word("PARTITIONS") {
        let partition_start = parser.current_span().start();
        parser.advance();
        parser.skip_trivia();
        let entries = parse_partition_entries(parser)?;
        let end = entries
            .last()
            .map_or_else(|| parser.current_offset(), |entry| entry.span.end());
        Some(InsertPartitions {
            entries,
            dynamic: false,
            span: Span::new(partition_start, end),
        })
    } else {
        None
    };
    if !(parser.current_is_word("SELECT")
        || parser.current_is_word("VALUES")
        || parser.current_is_word("WITH")
        || parser.current_is_symbol(Symbol::LParen))
    {
        return Err(parser.unexpected("SELECT, VALUES, WITH, or '('"));
    }
    let source = query::parse_query(parser)?;
    Ok(Insert {
        overwrite,
        has_into,
        target,
        columns,
        partitions,
        span: Span::new(start, source.span.end()),
        source,
    })
}

fn parse_delete(parser: &mut StatementParser<'_, '_>) -> Result<Delete, ParseError> {
    let start = parser.consume_word("DELETE")?.start();
    parser.consume_word("FROM")?;
    let target = parser.parse_object_name()?;
    let selection = if parser.consume_if_word("WHERE") {
        Some(parse_expr_until(parser, &[], &[Symbol::Semicolon])?)
    } else {
        None
    };
    let end = selection
        .as_ref()
        .map_or(target.span.end(), |expr| expr.span().end());
    Ok(Delete {
        target,
        selection,
        span: Span::new(start, end),
    })
}

fn parse_update(parser: &mut StatementParser<'_, '_>) -> Result<Update, ParseError> {
    let start = parser.consume_word("UPDATE")?.start();
    let target = parser.parse_object_name()?;
    let alias = parse_optional_alias(parser)?;
    parser.consume_word("SET")?;
    let assignments = parse_assignments(parser, &["FROM", "WHERE"])?;
    let source = if parser.consume_if_word("FROM") {
        Some(parse_source(parser)?)
    } else {
        None
    };
    let selection = if parser.consume_if_word("WHERE") {
        Some(parse_expr_until(parser, &[], &[Symbol::Semicolon])?)
    } else {
        None
    };
    let end = selection
        .as_ref()
        .map(|v| v.span().end())
        .or_else(|| source.as_ref().map(|value| value.span().end()))
        .or_else(|| assignments.last().map(|v| v.span.end()))
        .unwrap_or(target.span.end());
    Ok(Update {
        target,
        alias,
        assignments,
        source,
        selection,
        span: Span::new(start, end),
    })
}

fn parse_merge(parser: &mut StatementParser<'_, '_>) -> Result<Merge, ParseError> {
    let start = parser.consume_word("MERGE")?.start();
    parser.consume_word("INTO")?;
    let target = parser.parse_object_name()?;
    let target_alias = parse_optional_alias(parser)?;
    parser.consume_word("USING")?;
    let source = parse_source(parser)?;
    parser.consume_word("ON")?;
    let on = parse_expr_until(parser, &["WHEN"], &[Symbol::Semicolon])?;
    let mut clauses = Vec::new();
    while parser.consume_if_word("WHEN") {
        clauses.push(parse_merge_clause(parser)?);
    }
    if clauses.is_empty() {
        return Err(parser.unexpected("WHEN clause"));
    }
    let end = clauses.last().map_or(on.span().end(), MergeClause::span);
    Ok(Merge {
        target,
        target_alias,
        source,
        on,
        clauses,
        span: Span::new(start, end),
    })
}

fn parse_merge_clause(parser: &mut StatementParser<'_, '_>) -> Result<MergeClause, ParseError> {
    let start = parser.current_offset();
    let not = parser.consume_if_word("NOT");
    parser.consume_word("MATCHED")?;
    let by_source = if not && parser.consume_if_word("BY") {
        if parser.consume_if_word("SOURCE") {
            true
        } else {
            parser.consume_word("TARGET")?;
            false
        }
    } else {
        false
    };
    let predicate = if parser.consume_if_word("AND") {
        Some(parse_expr_until(parser, &["THEN"], &[])?)
    } else {
        None
    };
    parser.consume_word("THEN")?;
    if !not || by_source {
        let action = parse_matched_action(parser)?;
        let end = matched_end(&action);
        return Ok(if by_source {
            MergeClause::NotMatchedBySource {
                predicate,
                action,
                span: Span::new(start, end),
            }
        } else {
            MergeClause::Matched {
                predicate,
                action,
                span: Span::new(start, end),
            }
        });
    }
    parser.consume_word("INSERT")?;
    let columns = if parser.current_is_symbol(Symbol::LParen) {
        parse_ident_list(parser)?
    } else {
        Vec::new()
    };
    parser.consume_word("VALUES")?;
    parser.consume_symbol(Symbol::LParen)?;
    let values = parse_expr_list(parser, &[Symbol::RParen])?;
    let end = parser.consume_symbol(Symbol::RParen)?.end();
    Ok(MergeClause::NotMatched {
        predicate,
        action: MergeNotMatchedAction {
            columns,
            values,
            span: Span::new(start, end),
        },
        span: Span::new(start, end),
    })
}

fn parse_matched_action(
    parser: &mut StatementParser<'_, '_>,
) -> Result<MergeMatchedAction, ParseError> {
    let start = parser.current_offset();
    if parser.consume_if_word("DELETE") {
        return Ok(MergeMatchedAction::Delete {
            span: Span::new(start, parser.current_offset()),
        });
    }
    parser.consume_word("UPDATE")?;
    parser.consume_word("SET")?;
    let assignments = parse_assignments(parser, &["WHEN"])?;
    let end = assignments.last().map_or(start, |v| v.span.end());
    Ok(MergeMatchedAction::Update {
        assignments,
        span: Span::new(start, end),
    })
}
fn matched_end(action: &MergeMatchedAction) -> usize {
    match action {
        MergeMatchedAction::Update { span, .. } | MergeMatchedAction::Delete { span } => span.end(),
    }
}

fn parse_equality_delete(
    parser: &mut StatementParser<'_, '_>,
) -> Result<AddEqualityDelete, ParseError> {
    let start = parser.consume_word("ALTER")?.start();
    parser.consume_word("TABLE")?;
    let target = parser.parse_object_name()?;
    parser.consume_word("ADD")?;
    parser.consume_word("EQUALITY")?;
    parser.consume_word("DELETE")?;
    let columns = parse_ident_list(parser)?;
    parser.consume_word("VALUES")?;
    let mut rows = Vec::new();
    let mut end;
    loop {
        parser.consume_symbol(Symbol::LParen)?;
        let row = parse_expr_list(parser, &[Symbol::RParen])?;
        end = parser.consume_symbol(Symbol::RParen)?.end();
        rows.push(row);
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    Ok(AddEqualityDelete {
        target,
        columns,
        rows,
        span: Span::new(start, end),
    })
}

fn parse_source(parser: &mut StatementParser<'_, '_>) -> Result<MutationSource, ParseError> {
    let start = parser.current_offset();
    let lateral = parser.consume_if_word("LATERAL");
    if parser.current_is_symbol(Symbol::LParen) {
        parser.consume_symbol(Symbol::LParen)?;
        if !(parser.current_is_word("SELECT")
            || parser.current_is_word("VALUES")
            || parser.current_is_word("WITH")
            || parser.current_is_symbol(Symbol::LParen))
        {
            return Err(parser.unexpected("query after '('"));
        }
        let query = Box::new(query::parse_query(parser)?);
        let close = parser.consume_symbol(Symbol::RParen)?.end();
        let alias = parse_optional_alias(parser)?;
        let end = alias.as_ref().map_or(close, |alias| alias.span.end());
        return Ok(MutationSource::Query {
            lateral: lateral,
            query: query,
            alias: alias,
            span: Span::new(start, end),
        });
    }
    if lateral {
        return Err(parser.unexpected("derived query after LATERAL"));
    }
    let name = parser.parse_object_name()?;
    let alias = parse_optional_alias(parser)?;
    let end = alias
        .as_ref()
        .map_or(name.span.end(), |alias| alias.span.end());
    Ok(MutationSource::Table {
        name,
        alias,
        span: Span::new(start, end),
    })
}

fn parse_assignments(
    parser: &mut StatementParser<'_, '_>,
    words: &[&str],
) -> Result<Vec<Assignment>, ParseError> {
    let mut values = Vec::new();
    loop {
        let target = parser.parse_object_name()?;
        let start = target.span.start();
        parser.consume_symbol(Symbol::Eq)?;
        let value = parse_expr_until(parser, words, &[Symbol::Comma, Symbol::Semicolon])?;
        let span = Span::new(start, value.span().end());
        values.push(Assignment {
            target,
            value,
            span,
        });
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    Ok(values)
}
fn parse_ident_list(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<crate::ast::Ident>, ParseError> {
    parser.consume_symbol(Symbol::LParen)?;
    let mut values = Vec::new();
    loop {
        values.push(parser.parse_ident()?);
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    parser.consume_symbol(Symbol::RParen)?;
    Ok(values)
}
fn parse_partition_entries(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<InsertPartitionEntry>, ParseError> {
    parser.consume_symbol(Symbol::LParen)?;
    let mut entries = Vec::new();
    loop {
        let name = parser.parse_ident()?;
        let start = name.span.start();
        let value = if parser.consume_if_symbol(Symbol::Eq) {
            Some(parse_expr_until(
                parser,
                &[],
                &[Symbol::Comma, Symbol::RParen],
            )?)
        } else {
            None
        };
        let end = value.as_ref().map_or(name.span.end(), |v| v.span().end());
        entries.push(InsertPartitionEntry {
            name,
            value,
            span: Span::new(start, end),
        });
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    parser.consume_symbol(Symbol::RParen)?;
    Ok(entries)
}
fn parse_expr_list(
    parser: &mut StatementParser<'_, '_>,
    terminators: &[Symbol],
) -> Result<Vec<crate::ast::Expr>, ParseError> {
    let mut values = Vec::new();
    loop {
        values.push(parse_expr_until(
            parser,
            &[],
            &[Symbol::Comma, terminators[0]],
        )?);
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    Ok(values)
}
fn parse_expr_until(
    parser: &mut StatementParser<'_, '_>,
    words: &[&str],
    symbols: &[Symbol],
) -> Result<crate::ast::Expr, ParseError> {
    let begin = parser.position;
    let end = expression_end(parser, words, symbols);
    if end == begin {
        return Err(parser.unexpected("expression"));
    }
    let mut tokens: Vec<Token> = parser.tokens[begin..end].to_vec();
    let boundary = parser
        .tokens
        .get(end)
        .map_or_else(|| parser.current_span(), |token| token.span);
    tokens.push(Token::new(TokenKind::End, boundary));
    let value = PrattParser::new(parser.source, &tokens).parse()?;
    parser.position = end;
    parser.skip_trivia();
    Ok(value)
}
fn expression_end(parser: &StatementParser<'_, '_>, words: &[&str], symbols: &[Symbol]) -> usize {
    let mut index = parser.position;
    let mut nesting = 0usize;
    while let Some(token) = parser.tokens.get(index) {
        match token.kind {
            TokenKind::End => break,
            TokenKind::Symbol(Symbol::LParen | Symbol::LBracket | Symbol::LBrace) => nesting += 1,
            TokenKind::Symbol(Symbol::RParen | Symbol::RBracket | Symbol::RBrace)
                if nesting > 0 =>
            {
                nesting -= 1
            }
            TokenKind::Symbol(symbol) if nesting == 0 && symbols.contains(&symbol) => break,
            _ if nesting == 0 && words.iter().any(|word| is_word(parser, token, word)) => break,
            _ => {}
        }
        index += 1;
    }
    index
}
fn parse_optional_alias(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Option<TableAlias>, ParseError> {
    let explicit_as = parser.consume_if_word("AS");
    let has_alias = matches!(
        parser.current().map(|t| &t.kind),
        Some(TokenKind::Ident | TokenKind::QuotedIdent)
    );
    if !explicit_as && !has_alias {
        return Ok(None);
    }
    let name = parser.parse_ident()?;
    let start = name.span.start();
    let columns = if parser.current_is_symbol(Symbol::LParen) {
        parse_ident_list(parser)?
    } else {
        Vec::new()
    };
    let end = columns.last().map_or(name.span.end(), |v| v.span.end());
    Ok(Some(TableAlias {
        name,
        columns,
        explicit_as,
        span: Span::new(start, end),
    }))
}
fn looks_like_equality_delete(parser: &StatementParser<'_, '_>) -> bool {
    let mut index = next(parser, parser.position);
    if !word_at(parser, index, "ALTER") {
        return false;
    }
    index = next(parser, index + 1);
    if !word_at(parser, index, "TABLE") {
        return false;
    }
    index = next(parser, index + 1);
    if !name_at(parser, index) {
        return false;
    }
    index = next(parser, index + 1);
    while matches!(
        parser.tokens.get(index).map(|t| &t.kind),
        Some(TokenKind::Symbol(Symbol::Dot))
    ) {
        index = next(parser, index + 1);
        if !name_at(parser, index) {
            return false;
        }
        index = next(parser, index + 1);
    }
    word_at(parser, index, "ADD")
        && word_at(parser, next(parser, index + 1), "EQUALITY")
        && word_at(parser, next(parser, next(parser, index + 1) + 1), "DELETE")
}
fn next(parser: &StatementParser<'_, '_>, mut index: usize) -> usize {
    while parser
        .tokens
        .get(index)
        .is_some_and(|t| matches!(t.kind, TokenKind::Trivia(_)))
    {
        index += 1;
    }
    index
}
fn is_word(parser: &StatementParser<'_, '_>, token: &Token, word: &str) -> bool {
    matches!(token.kind, TokenKind::Ident | TokenKind::Keyword(_))
        && parser.source_slice(token.span).eq_ignore_ascii_case(word)
}
fn word_at(parser: &StatementParser<'_, '_>, index: usize, word: &str) -> bool {
    parser
        .tokens
        .get(index)
        .is_some_and(|t| is_word(parser, t, word))
}
fn name_at(parser: &StatementParser<'_, '_>, index: usize) -> bool {
    parser
        .tokens
        .get(index)
        .is_some_and(|t| matches!(t.kind, TokenKind::Ident | TokenKind::QuotedIdent))
}

impl MergeClause {
    fn span(&self) -> usize {
        match self {
            Self::Matched { span, .. }
            | Self::NotMatched { span, .. }
            | Self::NotMatchedBySource { span, .. } => span.end(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{DmlStatement, MutationSource, Statement},
        parser,
    };
    #[test]
    fn parses_derived_sources_without_consuming_dml_clauses() {
        let statements = parser::parse("UPDATE t SET v = s.v FROM (SELECT id, v FROM src) s WHERE t.id = s.id; MERGE INTO t USING LATERAL (SELECT id, v FROM src) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED BY SOURCE THEN DELETE").unwrap();
        let Statement::Dml(DmlStatement::Update(update)) = &statements[0] else {
            panic!("update")
        };
        assert!(matches!(update.source, Some(MutationSource::Query { .. })));
        assert!(update.selection.is_some());
        let Statement::Dml(DmlStatement::Merge(merge)) = &statements[1] else {
            panic!("merge")
        };
        assert!(matches!(
            merge.source,
            MutationSource::Query { lateral: true, .. }
        ));
        assert_eq!(merge.clauses.len(), 2);
    }
    #[test]
    fn parses_dynamic_overwrite_and_equality_delete() {
        let statements = parser::parse("INSERT OVERWRITE PARTITIONS TABLE db.t VALUES (1); ALTER TABLE db.t ADD EQUALITY DELETE (id) VALUES (1), (2)").unwrap();
        let Statement::Dml(DmlStatement::Insert(insert)) = &statements[0] else {
            panic!("insert")
        };
        assert!(insert.partitions.as_ref().is_some_and(|v| v.dynamic));
        assert!(matches!(
            statements[1],
            Statement::Dml(DmlStatement::AddEqualityDelete(_))
        ));
    }
}
