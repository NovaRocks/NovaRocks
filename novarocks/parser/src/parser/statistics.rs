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

//! Statistics command grammar.

use crate::{
    ParseError, Span, TokenKind,
    ast::statistics::{
        AnalyzeMode, AnalyzeTable, CancelAnalyze, DropHistogram, DropMultipleColumnsStats,
        DropStats, ShowAnalyzeJobs, ShowTableStats,
    },
    ast::{Statement, StatisticsStatement},
    token::Symbol,
};

use super::StatementParser;

pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("ANALYZE") {
        return parse_analyze(parser).map(Some);
    }
    if parser.current_is_word("CANCEL") && parser.peek_word(1, "ANALYZE") {
        return parse_cancel(parser).map(Some);
    }
    if parser.current_is_word("KILL") && parser.peek_word(1, "ANALYZE") {
        return parse_cancel(parser).map(Some);
    }
    if parser.current_is_word("SHOW")
        && ["ANALYZE", "TABLE"]
            .iter()
            .any(|word| parser.peek_word(1, word))
    {
        return parse_show(parser).map(Some);
    }
    if parser.current_is_word("DROP")
        && ["STATS", "HISTOGRAM", "MULTIPLE"]
            .iter()
            .any(|word| parser.peek_word(1, word))
    {
        return parse_drop(parser).map(Some);
    }
    Ok(None)
}

fn parse_analyze(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.current_span().start();
    parser.consume_word("ANALYZE")?;
    let mode = if parser.consume_if_word("FULL") {
        AnalyzeMode::Full
    } else if parser.consume_if_word("SAMPLE") {
        AnalyzeMode::Sample
    } else {
        AnalyzeMode::Default
    };
    parser.consume_word("TABLE")?;
    let name = parser.parse_object_name()?;
    let columns = parse_optional_columns(parser)?;
    let with_sync_mode = if parser.consume_if_word("WITH") {
        parser.consume_word("SYNC")?;
        parser.consume_word("MODE")?;
        true
    } else {
        false
    };
    let end = parser.current_offset();
    Ok(Statement::Statistics(StatisticsStatement::AnalyzeTable(
        AnalyzeTable {
            mode,
            name,
            columns,
            with_sync_mode,
            span: Span::new(start, end),
        },
    )))
}

fn parse_cancel(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.current_span().start();
    if parser.current_is_word("KILL") {
        parser.consume_word("KILL")?;
    } else {
        parser.consume_word("CANCEL")?;
    }
    parser.consume_word("ANALYZE")?;
    let job_start = parser.current_span().start();
    if parser.current().is_none_or(|token| {
        matches!(
            token.kind,
            TokenKind::End | TokenKind::Symbol(Symbol::Semicolon)
        )
    }) {
        return Err(parser.unexpected("ANALYZE job id"));
    }
    let mut job_id = String::new();
    let mut end = job_start;
    while parser.current().is_some_and(|token| {
        !matches!(
            token.kind,
            TokenKind::End | TokenKind::Symbol(Symbol::Semicolon)
        )
    }) {
        let span = parser.current_span();
        if matches!(
            parser.current().map(|token| &token.kind),
            Some(TokenKind::Trivia(_))
        ) {
            return Err(parser.unexpected("single ANALYZE job id"));
        }
        job_id.push_str(parser.source_slice(span));
        end = span.end();
        parser.advance();
    }
    Ok(Statement::Statistics(StatisticsStatement::CancelAnalyze(
        CancelAnalyze {
            job_id,
            span: Span::new(start, end),
        },
    )))
}

fn parse_show(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.current_span().start();
    parser.consume_word("SHOW")?;
    if parser.consume_if_word("ANALYZE") {
        let _ = parser.consume_if_word("JOBS");
        return Ok(Statement::Statistics(StatisticsStatement::ShowAnalyzeJobs(
            ShowAnalyzeJobs {
                span: Span::new(start, parser.current_offset()),
            },
        )));
    }
    if parser.consume_if_word("TABLE") {
        parser.consume_word("STATS")?;
        let name = parser.parse_object_name()?;
        let end = name.span.end();
        return Ok(Statement::Statistics(StatisticsStatement::ShowTableStats(
            ShowTableStats {
                name,
                span: Span::new(start, end),
            },
        )));
    }
    Err(parser.unexpected("ANALYZE or TABLE STATS"))
}

fn parse_drop(parser: &mut StatementParser<'_, '_>) -> Result<Statement, ParseError> {
    let start = parser.current_span().start();
    parser.consume_word("DROP")?;
    if parser.consume_if_word("STATS") {
        let name = parser.parse_object_name()?;
        let end = name.span.end();
        return Ok(Statement::Statistics(StatisticsStatement::DropStats(
            DropStats {
                name,
                span: Span::new(start, end),
            },
        )));
    }
    if parser.consume_if_word("HISTOGRAM") {
        parser.consume_word("ON")?;
        let name = parser.parse_object_name()?;
        let columns = parse_required_columns(parser)?;
        let end = parser.current_offset();
        return Ok(Statement::Statistics(StatisticsStatement::DropHistogram(
            DropHistogram {
                name,
                columns,
                span: Span::new(start, end),
            },
        )));
    }
    parser.consume_word("MULTIPLE")?;
    parser.consume_word("COLUMNS")?;
    parser.consume_word("STATS")?;
    let name = parser.parse_object_name()?;
    let end = name.span.end();
    Ok(Statement::Statistics(
        StatisticsStatement::DropMultipleColumnsStats(DropMultipleColumnsStats {
            name,
            span: Span::new(start, end),
        }),
    ))
}

fn parse_optional_columns(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<crate::ast::Ident>, ParseError> {
    if parser.current_is_symbol(Symbol::LParen) {
        parse_required_columns(parser)
    } else {
        Ok(Vec::new())
    }
}

fn parse_required_columns(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<crate::ast::Ident>, ParseError> {
    parser.consume_symbol(Symbol::LParen)?;
    let mut columns = Vec::new();
    loop {
        columns.push(parser.parse_ident()?);
        if parser.consume_if_symbol(Symbol::RParen) {
            return Ok(columns);
        }
        parser.consume_symbol(Symbol::Comma)?;
    }
}
