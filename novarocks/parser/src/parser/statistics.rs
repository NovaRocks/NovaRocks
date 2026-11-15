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

//! Statistics command grammar. Legacy mode/job grammar has no AST.
use super::StatementParser;
use crate::{
    ParseError, Span,
    ast::{AnalyzeTable, ShowStats, Statement, StatisticsStatement},
    token::Symbol,
};
pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("ANALYZE") {
        let start = parser.consume_word("ANALYZE")?.start();
        parser.consume_word("TABLE")?;
        let name = parser.parse_object_name()?;
        let columns = if parser.consume_if_symbol(Symbol::LParen) {
            let mut columns = Vec::new();
            loop {
                columns.push(parser.parse_ident()?);
                if parser.consume_if_symbol(Symbol::RParen) {
                    break columns;
                }
                parser.consume_symbol(Symbol::Comma)?;
            }
        } else {
            Vec::new()
        };
        return Ok(Some(Statement::Statistics(
            StatisticsStatement::AnalyzeTable(AnalyzeTable {
                name,
                columns,
                span: Span::new(start, parser.current_offset()),
            }),
        )));
    }
    if parser.current_is_word("SHOW") && parser.peek_word(1, "STATS") {
        let start = parser.consume_word("SHOW")?.start();
        parser.consume_word("STATS")?;
        parser.consume_word("FOR")?;
        if parser.current_is_symbol(Symbol::LParen) {
            return Err(ParseError::UnsupportedStatement {
                statement: "SHOW STATS FOR (query)".into(),
                span: parser.current_span(),
            });
        }
        let name = parser.parse_object_name()?;
        let end = name.span.end();
        return Ok(Some(Statement::Statistics(StatisticsStatement::ShowStats(
            ShowStats {
                name,
                span: Span::new(start, end),
            },
        ))));
    }
    Ok(None)
}
