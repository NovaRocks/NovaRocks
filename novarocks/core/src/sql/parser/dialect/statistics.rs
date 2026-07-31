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

//! Typed parser for the supported statistics application commands.
//!
//! These probes intentionally cover only the unified statistics surface. In
//! particular, legacy histogram, multi-column and raw information-schema
//! compatibility commands are not accepted here.

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::{convert_object_name, peek_word_eq};
use crate::sql::parser::ast::{
    AnalyzeTableStmt, CancelAnalyzeStmt, ShowAnalyzeJobsStmt, ShowTableStatsStmt, Statement,
};

pub(crate) fn looks_like_analyze_table(parser: &Parser<'_>) -> bool {
    peek_word_eq(parser, 0, "ANALYZE") && peek_word_eq(parser, 1, "TABLE")
}

pub(crate) fn looks_like_show_analyze_jobs(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::SHOW)
        && peek_word_eq(parser, 1, "ANALYZE")
        && peek_word_eq(parser, 2, "JOBS")
}

pub(crate) fn looks_like_cancel_analyze(parser: &Parser<'_>) -> bool {
    peek_word_eq(parser, 0, "CANCEL") && peek_word_eq(parser, 1, "ANALYZE")
}

pub(crate) fn looks_like_show_table_stats(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::SHOW)
        && peek_word_eq(parser, 1, "TABLE")
        && peek_word_eq(parser, 2, "STATS")
}

pub(crate) fn parse_analyze_table(parser: &mut Parser<'_>) -> Result<Statement, String> {
    expect_word(parser, "ANALYZE", "ANALYZE TABLE")?;
    expect_word(parser, "TABLE", "ANALYZE TABLE")?;
    let name = convert_object_name(
        parser
            .parse_object_name(false)
            .map_err(|error| format!("ANALYZE TABLE: {error}"))?,
    )?;
    let columns = parse_optional_columns(parser, "ANALYZE TABLE")?;
    expect_statement_end(parser, "ANALYZE TABLE")?;
    Ok(Statement::AnalyzeTable(AnalyzeTableStmt { name, columns }))
}

pub(crate) fn parse_show_analyze_jobs(parser: &mut Parser<'_>) -> Result<Statement, String> {
    parser
        .expect_keyword(Keyword::SHOW)
        .map_err(|error| format!("SHOW ANALYZE JOBS: {error}"))?;
    expect_word(parser, "ANALYZE", "SHOW ANALYZE JOBS")?;
    expect_word(parser, "JOBS", "SHOW ANALYZE JOBS")?;
    expect_statement_end(parser, "SHOW ANALYZE JOBS")?;
    Ok(Statement::ShowAnalyzeJobs(ShowAnalyzeJobsStmt))
}

pub(crate) fn parse_cancel_analyze(parser: &mut Parser<'_>) -> Result<Statement, String> {
    expect_word(parser, "CANCEL", "CANCEL ANALYZE")?;
    expect_word(parser, "ANALYZE", "CANCEL ANALYZE")?;
    // UUIDv7 begins with a digit, so sqlparser tokenizes an unquoted UUID as
    // a Number plus word/minus tokens rather than as one identifier. Consume
    // the single lexical argument and validate its canonical UUID shape here;
    // accepting a free-form string would make durable-job addressing fuzzy.
    let job_id = parse_uuid_argument(parser, "CANCEL ANALYZE")?;
    Ok(Statement::CancelAnalyze(CancelAnalyzeStmt { job_id }))
}

pub(crate) fn parse_show_table_stats(parser: &mut Parser<'_>) -> Result<Statement, String> {
    parser
        .expect_keyword(Keyword::SHOW)
        .map_err(|error| format!("SHOW TABLE STATS: {error}"))?;
    expect_word(parser, "TABLE", "SHOW TABLE STATS")?;
    expect_word(parser, "STATS", "SHOW TABLE STATS")?;
    let name = convert_object_name(
        parser
            .parse_object_name(false)
            .map_err(|error| format!("SHOW TABLE STATS: {error}"))?,
    )?;
    expect_statement_end(parser, "SHOW TABLE STATS")?;
    Ok(Statement::ShowTableStats(ShowTableStatsStmt { name }))
}

fn parse_optional_columns(parser: &mut Parser<'_>, context: &str) -> Result<Vec<String>, String> {
    if !parser.consume_token(&Token::LParen) {
        return Ok(Vec::new());
    }
    let mut columns = Vec::new();
    loop {
        let column = parser
            .parse_identifier()
            .map_err(|error| format!("{context}: expected column name: {error}"))?
            .value;
        if columns
            .iter()
            .any(|existing: &String| existing.eq_ignore_ascii_case(&column))
        {
            return Err(format!("{context}: duplicate column '{column}'"));
        }
        columns.push(column);
        if parser.consume_token(&Token::RParen) {
            return Ok(columns);
        }
        parser
            .expect_token(&Token::Comma)
            .map_err(|error| format!("{context}: expected ',' or ')': {error}"))?;
    }
}

fn expect_word(parser: &mut Parser<'_>, word: &str, context: &str) -> Result<(), String> {
    match parser.next_token().token {
        Token::Word(token_word) if token_word.value.eq_ignore_ascii_case(word) => Ok(()),
        other => Err(format!("{context}: expected {word}, got {other}")),
    }
}

fn expect_statement_end(parser: &mut Parser<'_>, context: &str) -> Result<(), String> {
    if parser.consume_token(&Token::SemiColon) && parser.peek_token_ref().token == Token::SemiColon
    {
        return Err(format!("{context}: only one final semicolon is allowed"));
    }
    match parser.peek_token_ref().token {
        Token::EOF => Ok(()),
        ref other => Err(format!("{context}: unexpected trailing token {other}")),
    }
}

fn parse_uuid_argument(parser: &mut Parser<'_>, context: &str) -> Result<String, String> {
    let mut value = String::new();
    loop {
        let token = parser.next_token().token;
        match token {
            Token::EOF => break,
            Token::SemiColon => {
                if parser.peek_token_ref().token == Token::SemiColon {
                    return Err(format!("{context}: only one final semicolon is allowed"));
                }
                if parser.peek_token_ref().token != Token::EOF {
                    return Err(format!(
                        "{context}: unexpected trailing token {}",
                        parser.peek_token_ref().token
                    ));
                }
                break;
            }
            Token::SingleQuotedString(value_part) => {
                if !value.is_empty() {
                    return Err(format!("{context}: expected exactly one UUID job id"));
                }
                value = value_part;
            }
            other => value.push_str(&other.to_string()),
        }
    }
    let parsed = uuid::Uuid::parse_str(&value)
        .map_err(|_| format!("{context} expects a canonical UUIDv7 job id"))?;
    if parsed.get_version_num() != 7 || parsed.to_string() != value.to_ascii_lowercase() {
        return Err(format!("{context} expects a canonical UUIDv7 job id"));
    }
    Ok(parsed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::dialect::StarRocksDialect;

    fn parse_one(sql: &str) -> Result<Statement, String> {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(sql)
            .map_err(|error| error.to_string())?;
        if looks_like_analyze_table(&parser) {
            return parse_analyze_table(&mut parser);
        }
        if looks_like_show_analyze_jobs(&parser) {
            return parse_show_analyze_jobs(&mut parser);
        }
        if looks_like_cancel_analyze(&parser) {
            return parse_cancel_analyze(&mut parser);
        }
        if looks_like_show_table_stats(&parser) {
            return parse_show_table_stats(&mut parser);
        }
        Err("not a statistics statement".to_string())
    }

    #[test]
    fn parses_the_unified_statistics_commands() {
        let analyze = parse_one("ANALYZE TABLE c.d.t (a, `b`)").expect("analyze parse");
        assert!(matches!(
            analyze,
            Statement::AnalyzeTable(AnalyzeTableStmt { name, columns })
                if name.parts == ["c", "d", "t"] && columns == ["a", "b"]
        ));
        assert!(matches!(
            parse_one("SHOW ANALYZE JOBS").expect("show jobs parse"),
            Statement::ShowAnalyzeJobs(_)
        ));
        assert!(matches!(
            parse_one("CANCEL ANALYZE 018f8c30-8a95-7b4e-b515-4da6f2aeb419")
                .expect("cancel parse"),
            Statement::CancelAnalyze(CancelAnalyzeStmt { job_id })
                if job_id == "018f8c30-8a95-7b4e-b515-4da6f2aeb419"
        ));
        assert!(matches!(
            parse_one("SHOW TABLE STATS db.t").expect("show table stats parse"),
            Statement::ShowTableStats(ShowTableStatsStmt { name }) if name.parts == ["db", "t"]
        ));
    }

    #[test]
    fn rejects_legacy_statistics_grammar() {
        let error = parse_one("ANALYZE SAMPLE TABLE t").expect_err("legacy sample rejected");
        assert!(error.contains("not a statistics statement"), "{error}");
        let error =
            parse_one("ANALYZE TABLE t UPDATE HISTOGRAM ON c").expect_err("histogram rejected");
        assert!(error.contains("unexpected trailing token"), "{error}");
    }
}
