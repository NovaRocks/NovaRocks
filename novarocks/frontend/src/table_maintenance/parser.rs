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

use std::collections::{BTreeMap, BTreeSet};

use crate::query_execution::maintenance::MaintenanceRequestContext;
use chrono::{DateTime, NaiveDateTime, Utc};
use novarocks_catalog::identifier::normalize_identifier;
use sqlparser::ast::{ObjectName, ObjectNamePart};
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParsedMaintenanceStatement {
    Execute {
        name_parts: Vec<String>,
        action: ParsedMaintenanceAction,
    },
    SubmitOptimize {
        name_parts: Vec<String>,
    },
    ShowOptimize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParsedMaintenanceAction {
    RewriteDataFiles {
        options: BTreeMap<String, String>,
        branch: Option<String>,
        where_clause: Option<String>,
    },
    RewriteManifests {
        use_caching: Option<bool>,
        spec_id: Option<i32>,
    },
    ExpireSnapshots {
        older_than_ms: Option<i64>,
        retain_last: Option<u32>,
    },
    RemoveOrphanFiles {
        older_than_ms: i64,
    },
    RewritePositionDeleteFiles {
        options: BTreeMap<String, String>,
        where_clause: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ParsedShowOptimize {
    pub(super) catalog: Option<String>,
    pub(super) database: Option<String>,
    pub(super) table_name: Option<String>,
    pub(super) order_by_create_time_desc: bool,
    pub(super) limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MaintenanceCandidate {
    Optimize,
    RewriteManifests,
    ExpireSnapshots,
    RemoveOrphanFiles,
    ShowOptimize,
    SparkCall,
}

#[derive(Debug)]
struct MaintenanceSqlDialect;

impl sqlparser::dialect::Dialect for MaintenanceSqlDialect {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_' || ch == '$'
    }

    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }
}

pub fn parse_maintenance_statement(
    sql: &str,
    context: MaintenanceRequestContext<'_>,
) -> Result<Option<ParsedMaintenanceStatement>, String> {
    let Some(candidate) = maintenance_candidate(sql) else {
        return Ok(None);
    };
    let statement = match candidate {
        MaintenanceCandidate::Optimize => {
            let name_parts = parse_alter_table_optimize(sql)?;
            ParsedMaintenanceStatement::SubmitOptimize { name_parts }
        }
        MaintenanceCandidate::RewriteManifests => {
            let name_parts = parse_alter_table_rewrite_manifests(sql)?;
            ParsedMaintenanceStatement::Execute {
                name_parts,
                action: ParsedMaintenanceAction::RewriteManifests {
                    use_caching: None,
                    spec_id: None,
                },
            }
        }
        MaintenanceCandidate::ExpireSnapshots => {
            let (name_parts, older_than_ms, retain_last) = parse_alter_table_expire_snapshots(sql)?;
            ParsedMaintenanceStatement::Execute {
                name_parts,
                action: ParsedMaintenanceAction::ExpireSnapshots {
                    older_than_ms,
                    retain_last,
                },
            }
        }
        MaintenanceCandidate::RemoveOrphanFiles => {
            let (name_parts, older_than_ms) = parse_alter_table_remove_orphan_files(sql)?;
            ParsedMaintenanceStatement::Execute {
                name_parts,
                action: ParsedMaintenanceAction::RemoveOrphanFiles { older_than_ms },
            }
        }
        MaintenanceCandidate::ShowOptimize => {
            let _ = parse_show_optimize(sql)?;
            ParsedMaintenanceStatement::ShowOptimize
        }
        MaintenanceCandidate::SparkCall => parse_call_action(sql, context)?,
    };
    Ok(Some(statement))
}

pub(super) fn parse_show_optimize(sql: &str) -> Result<ParsedShowOptimize, String> {
    let mut parser = maintenance_parser(sql, "parse SHOW ALTER TABLE OPTIMIZE")?;
    parser
        .expect_keyword(Keyword::SHOW)
        .map_err(|error| error.to_string())?;
    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|error| error.to_string())?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|error| error.to_string())?;
    expect_word(&mut parser, "OPTIMIZE")?;

    let (catalog, database) =
        if parser.parse_keyword(Keyword::FROM) || parser.parse_keyword(Keyword::IN) {
            let name_parts =
                normalize_object_name(parser.parse_object_name(false).map_err(|error| {
                    format!("parse SHOW ALTER TABLE OPTIMIZE namespace: {error}")
                })?)?;
            match name_parts.as_slice() {
                [database] => (None, Some(database.clone())),
                [catalog, database] => (Some(catalog.clone()), Some(database.clone())),
                _ => {
                    return Err(
                        "SHOW ALTER TABLE OPTIMIZE FROM only supports db or catalog.db".to_string(),
                    );
                }
            }
        } else {
            (None, None)
        };

    let table_name = if parser.parse_keyword(Keyword::WHERE) {
        let identifier = parser
            .parse_identifier()
            .map_err(|error| format!("parse SHOW ALTER TABLE OPTIMIZE WHERE column: {error}"))?;
        if !identifier.value.eq_ignore_ascii_case("TableName") || !parser.consume_token(&Token::Eq)
        {
            return Err(
                "SHOW ALTER TABLE OPTIMIZE only supports WHERE TableName = '...'".to_string(),
            );
        }
        let value = parser.parse_literal_string().map_err(|error| {
            format!("parse SHOW ALTER TABLE OPTIMIZE TableName filter: {error}")
        })?;
        Some(normalize_identifier(&value)?)
    } else {
        None
    };

    let mut order_by_create_time_desc = false;
    if parser.parse_keyword(Keyword::ORDER) {
        parser
            .expect_keyword(Keyword::BY)
            .map_err(|error| format!("parse SHOW ALTER TABLE OPTIMIZE ORDER BY: {error}"))?;
        let identifier = parser
            .parse_identifier()
            .map_err(|error| format!("parse SHOW ALTER TABLE OPTIMIZE ORDER BY column: {error}"))?;
        if !identifier.value.eq_ignore_ascii_case("CreateTime") {
            return Err("SHOW ALTER TABLE OPTIMIZE only supports ORDER BY CreateTime".to_string());
        }
        if parser.parse_keyword(Keyword::DESC) {
            order_by_create_time_desc = true;
        } else {
            let _ = parser.parse_keyword(Keyword::ASC);
        }
    }

    let limit = if parser.parse_keyword(Keyword::LIMIT) {
        let token = parser.next_token();
        let value = match token.token {
            Token::Number(value, false) => value,
            other => {
                return Err(format!(
                    "SHOW ALTER TABLE OPTIMIZE LIMIT expects number, got {other}"
                ));
            }
        };
        Some(
            value
                .parse::<usize>()
                .map_err(|error| format!("parse SHOW ALTER TABLE OPTIMIZE LIMIT: {error}"))?,
        )
    } else {
        None
    };

    consume_optional_final_semicolon(&mut parser)?;
    expect_statement_eof(&parser)?;
    Ok(ParsedShowOptimize {
        catalog,
        database,
        table_name,
        order_by_create_time_desc,
        limit,
    })
}

pub(super) fn is_spark_maintenance_call(sql: &str) -> bool {
    matches!(
        maintenance_candidate(sql),
        Some(MaintenanceCandidate::SparkCall)
    )
}

fn maintenance_candidate(sql: &str) -> Option<MaintenanceCandidate> {
    let mut parser = Parser::new(&MaintenanceSqlDialect).try_with_sql(sql).ok()?;
    if parser.parse_keyword(Keyword::CALL) {
        return Some(MaintenanceCandidate::SparkCall);
    }
    if parser.parse_keyword(Keyword::SHOW) {
        return (parser.parse_keyword(Keyword::ALTER)
            && parser.parse_keyword(Keyword::TABLE)
            && peek_word(&parser, 0, "OPTIMIZE"))
        .then_some(MaintenanceCandidate::ShowOptimize);
    }
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return None;
    }
    parser.parse_object_name(false).ok()?;
    if peek_word(&parser, 0, "OPTIMIZE") {
        Some(MaintenanceCandidate::Optimize)
    } else if peek_word(&parser, 0, "REWRITE") && peek_word(&parser, 1, "MANIFESTS") {
        Some(MaintenanceCandidate::RewriteManifests)
    } else if peek_word(&parser, 0, "EXPIRE") && peek_word(&parser, 1, "SNAPSHOTS") {
        Some(MaintenanceCandidate::ExpireSnapshots)
    } else if peek_word(&parser, 0, "REMOVE")
        && peek_word(&parser, 1, "ORPHAN")
        && peek_word(&parser, 2, "FILES")
    {
        Some(MaintenanceCandidate::RemoveOrphanFiles)
    } else {
        None
    }
}

fn parse_alter_table_optimize(sql: &str) -> Result<Vec<String>, String> {
    let (mut parser, name_parts) = parse_alter_table_prefix(sql, "parse ALTER TABLE OPTIMIZE")?;
    expect_word(&mut parser, "OPTIMIZE")?;
    if peek_word(&parser, 0, "PARTITION") {
        return Err("OPTIMIZE only supports whole-table compaction".to_string());
    }
    consume_optional_final_semicolon(&mut parser)?;
    expect_statement_eof(&parser).map_err(|error| {
        if peek_word(&parser, 0, "PARTITION") {
            "OPTIMIZE only supports whole-table compaction".to_string()
        } else {
            format!("unsupported trailing ALTER TABLE OPTIMIZE tokens: {error}")
        }
    })?;
    Ok(name_parts)
}

fn parse_alter_table_rewrite_manifests(sql: &str) -> Result<Vec<String>, String> {
    let (mut parser, name_parts) =
        parse_alter_table_prefix(sql, "parse ALTER TABLE REWRITE MANIFESTS")?;
    reject_branch_suffix(&name_parts, "REWRITE MANIFESTS")?;
    expect_word(&mut parser, "REWRITE")?;
    expect_word(&mut parser, "MANIFESTS")?;
    consume_optional_final_semicolon(&mut parser)?;
    expect_statement_eof(&parser).map_err(|error| {
        format!("unsupported trailing ALTER TABLE REWRITE MANIFESTS tokens: {error}")
    })?;
    Ok(name_parts)
}

fn parse_alter_table_expire_snapshots(
    sql: &str,
) -> Result<(Vec<String>, Option<i64>, Option<u32>), String> {
    let (mut parser, name_parts) =
        parse_alter_table_prefix(sql, "parse ALTER TABLE EXPIRE SNAPSHOTS")?;
    reject_branch_suffix(&name_parts, "EXPIRE SNAPSHOTS")?;
    expect_word(&mut parser, "EXPIRE")?;
    expect_word(&mut parser, "SNAPSHOTS")?;

    let mut older_than_ms = None;
    let mut retain_last = None;
    loop {
        if peek_word(&parser, 0, "OLDER") {
            if older_than_ms.is_some() {
                return Err("EXPIRE SNAPSHOTS: duplicate OLDER THAN clause".to_string());
            }
            expect_word(&mut parser, "OLDER")?;
            expect_word(&mut parser, "THAN")?;
            older_than_ms = Some(parse_expire_timestamp_ms(&mut parser)?);
            continue;
        }
        if peek_word(&parser, 0, "RETAIN") {
            if retain_last.is_some() {
                return Err("EXPIRE SNAPSHOTS: duplicate RETAIN LAST clause".to_string());
            }
            expect_word(&mut parser, "RETAIN")?;
            expect_word(&mut parser, "LAST")?;
            let value = parse_expire_uint(&mut parser)?;
            if value == 0 {
                return Err("EXPIRE SNAPSHOTS: RETAIN LAST must be >= 1".to_string());
            }
            retain_last = Some(
                value
                    .try_into()
                    .map_err(|_| "EXPIRE SNAPSHOTS: RETAIN LAST value too large".to_string())?,
            );
            continue;
        }
        break;
    }
    if older_than_ms.is_none() && retain_last.is_none() {
        return Err(
            "EXPIRE SNAPSHOTS requires at least OLDER THAN or RETAIN LAST clause".to_string(),
        );
    }
    consume_optional_final_semicolon(&mut parser)?;
    expect_statement_eof(&parser)
        .map_err(|error| format!("unsupported trailing tokens: {error}"))?;
    Ok((name_parts, older_than_ms, retain_last))
}

fn parse_alter_table_remove_orphan_files(sql: &str) -> Result<(Vec<String>, i64), String> {
    let (mut parser, name_parts) =
        parse_alter_table_prefix(sql, "parse ALTER TABLE REMOVE ORPHAN FILES")?;
    reject_branch_suffix(&name_parts, "REMOVE ORPHAN FILES")?;
    expect_word(&mut parser, "REMOVE")?;
    expect_word(&mut parser, "ORPHAN")?;
    expect_word(&mut parser, "FILES")?;
    if !peek_word(&parser, 0, "OLDER") {
        return Err(
            "REMOVE ORPHAN FILES requires OLDER THAN clause (e.g. OLDER THAN '2026-01-01')"
                .to_string(),
        );
    }
    expect_word(&mut parser, "OLDER")?;
    expect_word(&mut parser, "THAN")?;
    let older_than_ms = parse_expire_timestamp_ms(&mut parser)?;
    consume_optional_final_semicolon(&mut parser)?;
    expect_statement_eof(&parser)
        .map_err(|error| format!("unsupported trailing tokens: {error}"))?;
    Ok((name_parts, older_than_ms))
}

fn parse_alter_table_prefix<'a>(
    sql: &'a str,
    context: &str,
) -> Result<(Parser<'a>, Vec<String>), String> {
    let mut parser = maintenance_parser(sql, context)?;
    parser
        .expect_keyword(Keyword::ALTER)
        .map_err(|error| error.to_string())?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|error| error.to_string())?;
    let name = parser
        .parse_object_name(false)
        .map_err(|error| error.to_string())?;
    Ok((parser, normalize_object_name(name)?))
}

fn parse_expire_timestamp_ms(parser: &mut Parser<'_>) -> Result<i64, String> {
    let token = parser.next_token();
    match token.token {
        Token::SingleQuotedString(value) => {
            if let Ok(value) = DateTime::parse_from_rfc3339(&value) {
                return Ok(value.with_timezone(&Utc).timestamp_millis());
            }
            if let Ok(value) = NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S") {
                return Ok(value.and_utc().timestamp_millis());
            }
            Err(format!(
                "EXPIRE SNAPSHOTS: cannot parse timestamp '{value}'; \
                 expected RFC 3339 (e.g. '2026-04-01T00:00:00Z') \
                 or 'YYYY-MM-DD HH:MM:SS'"
            ))
        }
        Token::Number(value, _) => value.parse::<i64>().map_err(|error| {
            format!("EXPIRE SNAPSHOTS: invalid epoch-ms integer '{value}': {error}")
        }),
        other => Err(format!(
            "EXPIRE SNAPSHOTS: expected timestamp literal (quoted string or integer), got {other}"
        )),
    }
}

fn parse_expire_uint(parser: &mut Parser<'_>) -> Result<u64, String> {
    let token = parser.next_token();
    match token.token {
        Token::Number(value, _) => value.parse::<u64>().map_err(|error| {
            format!("EXPIRE SNAPSHOTS: invalid RETAIN LAST value '{value}': {error}")
        }),
        other => Err(format!(
            "EXPIRE SNAPSHOTS: expected integer for RETAIN LAST, got {other}"
        )),
    }
}

fn reject_branch_suffix(name_parts: &[String], action: &str) -> Result<(), String> {
    if name_parts.len() >= 2
        && name_parts
            .last()
            .is_some_and(|part| part.starts_with("branch_") || part.starts_with("tag_"))
    {
        return Err(format!(
            "{action} does not support branch/tag suffix on table name: {}",
            name_parts.join(".")
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcedureArgMode {
    Named,
    Positional,
    Empty,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProcedureArgValue {
    String(String),
    Boolean(bool),
    Integer(i64),
    TimestampMillis(i64),
    StringMap(BTreeMap<String, String>),
    Null,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProcedureArg {
    name: Option<String>,
    value: ProcedureArgValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CallProcedure {
    catalog: String,
    procedure: String,
    args: Vec<ProcedureArg>,
    mode: ProcedureArgMode,
}

fn parse_call_action(
    sql: &str,
    context: MaintenanceRequestContext<'_>,
) -> Result<ParsedMaintenanceStatement, String> {
    let statement = parse_call_procedure(sql)?;
    let named = normalize_procedure_args(&statement)?;
    let table = required_string_arg(&named, "table")?;
    let name_parts =
        resolve_procedure_table_name(&statement.catalog, context.current_database, &table)?;

    let older_than_ms = optional_timestamp_arg(&named, "older_than")?;
    let retain_last = optional_u32_arg(&named, "retain_last")?;
    let use_caching = optional_bool_arg(&named, "use_caching")?;
    let spec_id = optional_i32_arg(&named, "spec_id")?;
    let branch = optional_string_arg(&named, "branch")?;
    let where_clause = optional_string_arg(&named, "where")?;
    let options = optional_string_map_arg(&named, "options")?.unwrap_or_default();

    validate_supported_args(&statement.procedure, named.keys())?;
    validate_current_task_args(&statement.procedure, named.keys())?;

    let action = match statement.procedure.as_str() {
        "rewrite_data_files" => {
            validate_rewrite_data_files(&options, branch.as_ref(), where_clause.as_ref())?;
            ParsedMaintenanceAction::RewriteDataFiles {
                options,
                branch,
                where_clause,
            }
        }
        "rewrite_manifests" => ParsedMaintenanceAction::RewriteManifests {
            use_caching,
            spec_id,
        },
        "expire_snapshots" => {
            if older_than_ms.is_none() && retain_last.is_none() {
                return Err("expire_snapshots requires `older_than` or `retain_last`".to_string());
            }
            ParsedMaintenanceAction::ExpireSnapshots {
                older_than_ms,
                retain_last,
            }
        }
        "remove_orphan_files" => ParsedMaintenanceAction::RemoveOrphanFiles {
            older_than_ms: older_than_ms.ok_or_else(|| {
                "remove_orphan_files requires `older_than` TIMESTAMP argument".to_string()
            })?,
        },
        "rewrite_position_delete_files" => ParsedMaintenanceAction::RewritePositionDeleteFiles {
            options,
            where_clause,
        },
        other => return Err(format!("unsupported Iceberg system procedure `{other}`")),
    };
    Ok(ParsedMaintenanceStatement::Execute { name_parts, action })
}

fn parse_call_procedure(sql: &str) -> Result<CallProcedure, String> {
    let mut parser = maintenance_parser(sql, "parse CALL procedure")?;
    parser
        .expect_keyword(Keyword::CALL)
        .map_err(|error| format!("parse CALL procedure: {error}"))?;
    let name_parts = normalize_object_name(
        parser
            .parse_object_name(false)
            .map_err(|error| format!("parse CALL procedure name: {error}"))?,
    )?;
    let [catalog, namespace, procedure] = name_parts.as_slice() else {
        return Err("CALL procedure name must be catalog.system.procedure".to_string());
    };
    if namespace != "system" {
        return Err("Iceberg procedures must use system namespace".to_string());
    }
    let args = parse_arg_list(&mut parser)?;
    let mode = classify_arg_mode(&args)?;
    ensure_no_duplicate_named_args(&args)?;
    consume_optional_final_semicolon(&mut parser)?;
    expect_call_eof(&parser)?;
    Ok(CallProcedure {
        catalog: catalog.clone(),
        procedure: procedure.clone(),
        args,
        mode,
    })
}

fn parse_arg_list(parser: &mut Parser<'_>) -> Result<Vec<ProcedureArg>, String> {
    parser
        .expect_token(&Token::LParen)
        .map_err(|error| format!("CALL procedure expected argument list: {error}"))?;
    if parser.consume_token(&Token::RParen) {
        return Ok(Vec::new());
    }
    let mut args = Vec::new();
    loop {
        args.push(parse_arg(parser)?);
        if parser.consume_token(&Token::Comma) {
            continue;
        }
        break;
    }
    parser
        .expect_token(&Token::RParen)
        .map_err(|error| format!("CALL procedure expected end of argument list: {error}"))?;
    Ok(args)
}

fn parse_arg(parser: &mut Parser<'_>) -> Result<ProcedureArg, String> {
    let name = if matches!(parser.peek_token_ref().token, Token::Word(_))
        && matches!(parser.peek_nth_token_ref(1).token, Token::RArrow)
    {
        let identifier = parser
            .parse_identifier()
            .map_err(|error| format!("CALL procedure expected argument name: {error}"))?;
        match parser.next_token().token {
            Token::RArrow => {}
            other => return Err(format!("CALL procedure expected =>, got {other}")),
        }
        Some(normalize_identifier(&identifier.value)?)
    } else {
        None
    };
    Ok(ProcedureArg {
        name,
        value: parse_arg_value(parser)?,
    })
}

fn parse_arg_value(parser: &mut Parser<'_>) -> Result<ProcedureArgValue, String> {
    if parser.parse_keyword(Keyword::TIMESTAMP) {
        let value = parse_single_quoted_string(parser, "TIMESTAMP")?;
        return parse_timestamp_millis(&value)
            .map(ProcedureArgValue::TimestampMillis)
            .map_err(|error| format!("CALL procedure invalid TIMESTAMP literal: {error}"));
    }
    if parser.parse_keyword(Keyword::MAP) {
        return parse_string_map(parser);
    }
    match parser.next_token().token {
        Token::SingleQuotedString(value) => Ok(ProcedureArgValue::String(value)),
        Token::Number(value, _) => value
            .parse::<i64>()
            .map(ProcedureArgValue::Integer)
            .map_err(|error| format!("CALL procedure invalid integer argument '{value}': {error}")),
        Token::Word(word) if word.keyword == Keyword::TRUE => Ok(ProcedureArgValue::Boolean(true)),
        Token::Word(word) if word.keyword == Keyword::FALSE => {
            Ok(ProcedureArgValue::Boolean(false))
        }
        Token::Word(word) if word.keyword == Keyword::NULL => Ok(ProcedureArgValue::Null),
        other => Err(format!(
            "CALL procedure unsupported argument value: {other}"
        )),
    }
}

fn parse_string_map(parser: &mut Parser<'_>) -> Result<ProcedureArgValue, String> {
    parser
        .expect_token(&Token::LParen)
        .map_err(|error| format!("CALL procedure map expects (: {error}"))?;
    let mut values = Vec::new();
    if !parser.consume_token(&Token::RParen) {
        loop {
            values.push(parse_single_quoted_string(parser, "map key/value")?);
            if parser.consume_token(&Token::Comma) {
                continue;
            }
            break;
        }
        parser
            .expect_token(&Token::RParen)
            .map_err(|error| format!("CALL procedure map expects ): {error}"))?;
    }
    if values.len() % 2 != 0 {
        return Err("CALL procedure map requires an even number of string literals".to_string());
    }
    let mut map = BTreeMap::new();
    for entry in values.chunks_exact(2) {
        if map.insert(entry[0].clone(), entry[1].clone()).is_some() {
            return Err(format!("duplicate CALL procedure map key '{}'", entry[0]));
        }
    }
    Ok(ProcedureArgValue::StringMap(map))
}

fn parse_single_quoted_string(parser: &mut Parser<'_>, context: &str) -> Result<String, String> {
    match parser.next_token().token {
        Token::SingleQuotedString(value) => Ok(value),
        other => Err(format!(
            "CALL procedure {context} expects a single quoted string, got {other}"
        )),
    }
}

fn parse_timestamp_millis(value: &str) -> Result<i64, String> {
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        return Ok(value.with_timezone(&Utc).timestamp_millis());
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(value.and_utc().timestamp_millis());
    }
    if !value.is_empty() && value.chars().all(|character| character.is_ascii_digit()) {
        return value
            .parse::<i64>()
            .map_err(|error| format!("invalid epoch-ms timestamp '{value}': {error}"));
    }
    Err(format!(
        "cannot parse timestamp '{value}'; expected RFC3339, epoch-ms, or YYYY-MM-DD HH:MM:SS"
    ))
}

fn classify_arg_mode(args: &[ProcedureArg]) -> Result<ProcedureArgMode, String> {
    let has_named = args.iter().any(|arg| arg.name.is_some());
    let has_positional = args.iter().any(|arg| arg.name.is_none());
    match (has_named, has_positional) {
        (false, false) => Ok(ProcedureArgMode::Empty),
        (true, false) => Ok(ProcedureArgMode::Named),
        (false, true) => Ok(ProcedureArgMode::Positional),
        (true, true) => Err("CALL procedure cannot mix positional and named arguments".to_string()),
    }
}

fn ensure_no_duplicate_named_args(args: &[ProcedureArg]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for arg in args {
        let Some(name) = &arg.name else {
            continue;
        };
        if !seen.insert(name) {
            return Err(format!("duplicate CALL procedure argument '{name}'"));
        }
    }
    Ok(())
}

fn normalize_procedure_args(
    statement: &CallProcedure,
) -> Result<BTreeMap<String, ProcedureArgValue>, String> {
    let mut named = BTreeMap::new();
    match statement.mode {
        ProcedureArgMode::Empty => {}
        ProcedureArgMode::Named => {
            for arg in &statement.args {
                let Some(name) = &arg.name else {
                    return Err(
                        "CALL procedure cannot mix positional and named arguments".to_string()
                    );
                };
                insert_procedure_arg(&mut named, name, arg.value.clone())?;
            }
        }
        ProcedureArgMode::Positional => {
            let names = positional_names(&statement.procedure)?;
            if statement.args.len() > names.len() {
                return Err(format!(
                    "Iceberg system procedure `{}` accepts at most {} positional arguments, got {}",
                    statement.procedure,
                    names.len(),
                    statement.args.len()
                ));
            }
            for (arg, name) in statement.args.iter().zip(names.iter()) {
                insert_procedure_arg(&mut named, name, arg.value.clone())?;
            }
        }
    }
    Ok(named)
}

fn insert_procedure_arg(
    named: &mut BTreeMap<String, ProcedureArgValue>,
    name: &str,
    value: ProcedureArgValue,
) -> Result<(), String> {
    if named.insert(name.to_string(), value).is_some() {
        return Err(format!("duplicate CALL procedure argument `{name}`"));
    }
    Ok(())
}

fn positional_names(procedure: &str) -> Result<&'static [&'static str], String> {
    match procedure {
        "rewrite_data_files" => Ok(&[
            "table",
            "strategy",
            "sort_order",
            "options",
            "where",
            "branch",
        ]),
        "rewrite_manifests" => Ok(&["table", "use_caching", "spec_id"]),
        "expire_snapshots" => Ok(&[
            "table",
            "older_than",
            "retain_last",
            "max_concurrent_deletes",
            "stream_results",
            "snapshot_ids",
            "clean_expired_metadata",
        ]),
        "remove_orphan_files" => Ok(&[
            "table",
            "older_than",
            "location",
            "dry_run",
            "max_concurrent_deletes",
            "file_list_view",
            "equal_schemes",
            "equal_authorities",
            "prefix_mismatch_mode",
            "prefix_listing",
            "stream_results",
        ]),
        "rewrite_position_delete_files" => Ok(&["table", "options", "where"]),
        other => Err(format!("unsupported Iceberg system procedure `{other}`")),
    }
}

fn validate_supported_args<'a>(
    procedure: &str,
    keys: impl IntoIterator<Item = &'a String>,
) -> Result<(), String> {
    let allowed = positional_names(procedure)?;
    for key in keys {
        if !allowed.contains(&key.as_str()) {
            return Err(format!(
                "unsupported argument `{key}` for Iceberg system procedure `{procedure}`"
            ));
        }
    }
    Ok(())
}

fn validate_current_task_args<'a>(
    procedure: &str,
    keys: impl IntoIterator<Item = &'a String>,
) -> Result<(), String> {
    let implemented = match procedure {
        "rewrite_data_files" => &["table", "options", "where", "branch"][..],
        "rewrite_manifests" => &["table", "use_caching", "spec_id"],
        "expire_snapshots" => &["table", "older_than", "retain_last"],
        "remove_orphan_files" => &["table", "older_than"],
        "rewrite_position_delete_files" => &["table", "options", "where"],
        other => return Err(format!("unsupported Iceberg system procedure `{other}`")),
    };
    for key in keys {
        if !implemented.contains(&key.as_str()) {
            return Err(format!(
                "argument `{key}` for Iceberg system procedure `{procedure}` is not implemented in NovaRocks yet"
            ));
        }
    }
    Ok(())
}

fn validate_rewrite_data_files(
    options: &BTreeMap<String, String>,
    branch: Option<&String>,
    where_clause: Option<&String>,
) -> Result<(), String> {
    if where_clause.is_some() {
        return Err("rewrite_data_files where is not supported in NovaRocks yet".to_string());
    }
    if branch.is_some() {
        return Err("rewrite_data_files branch is not supported in NovaRocks yet".to_string());
    }
    for (key, value) in options {
        match key.as_str() {
            "rewrite-all" if value.eq_ignore_ascii_case("true") => {}
            "rewrite-all" => {
                return Err("rewrite_data_files option `rewrite-all` must be `true`".to_string());
            }
            other => return Err(format!("unsupported rewrite_data_files option `{other}`")),
        }
    }
    Ok(())
}

fn required_string_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<String, String> {
    match named.get(name) {
        Some(ProcedureArgValue::String(value)) => Ok(value.clone()),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a string, got {}",
            procedure_arg_type(value)
        )),
        None => Err(format!("CALL procedure requires `{name}` argument")),
    }
}

fn optional_string_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<String>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::String(value)) => Ok(Some(value.clone())),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a string, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_bool_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<bool>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::Boolean(value)) => Ok(Some(*value)),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a boolean, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_timestamp_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<i64>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::TimestampMillis(value)) => Ok(Some(*value)),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a TIMESTAMP literal, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_u32_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<u32>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::Integer(value)) => {
            if *value <= 0 {
                return Err(format!("CALL procedure argument `{name}` must be >= 1"));
            }
            u32::try_from(*value)
                .map(Some)
                .map_err(|_| format!("CALL procedure argument `{name}` is too large"))
        }
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be an integer, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_i32_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<i32>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::Integer(value)) => i32::try_from(*value)
            .map(Some)
            .map_err(|_| format!("CALL procedure argument `{name}` does not fit i32")),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be an integer, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn optional_string_map_arg(
    named: &BTreeMap<String, ProcedureArgValue>,
    name: &str,
) -> Result<Option<BTreeMap<String, String>>, String> {
    match named.get(name) {
        Some(ProcedureArgValue::StringMap(value)) => Ok(Some(value.clone())),
        Some(ProcedureArgValue::Null) | None => Ok(None),
        Some(value) => Err(format!(
            "CALL procedure argument `{name}` must be a string map, got {}",
            procedure_arg_type(value)
        )),
    }
}

fn procedure_arg_type(value: &ProcedureArgValue) -> &'static str {
    match value {
        ProcedureArgValue::String(_) => "string",
        ProcedureArgValue::Boolean(_) => "boolean",
        ProcedureArgValue::Integer(_) => "integer",
        ProcedureArgValue::TimestampMillis(_) => "timestamp",
        ProcedureArgValue::StringMap(_) => "string map",
        ProcedureArgValue::Null => "null",
    }
}

fn resolve_procedure_table_name(
    call_catalog: &str,
    current_database: &str,
    raw_table: &str,
) -> Result<Vec<String>, String> {
    let parts = raw_table
        .split('.')
        .map(normalize_identifier)
        .collect::<Result<Vec<_>, _>>()?;
    let call_catalog = normalize_identifier(call_catalog)?;
    match parts.as_slice() {
        [table] => Ok(vec![
            call_catalog,
            normalize_identifier(current_database)?,
            table.clone(),
        ]),
        [namespace, table] => Ok(vec![call_catalog, namespace.clone(), table.clone()]),
        [catalog, namespace, table] => {
            if catalog != &call_catalog {
                return Err(format!(
                    "CALL procedure table catalog `{catalog}` does not match procedure catalog `{call_catalog}`"
                ));
            }
            Ok(vec![call_catalog, namespace.clone(), table.clone()])
        }
        _ => Err(format!(
            "CALL procedure table must be `table`, `namespace.table`, or `catalog.namespace.table`, got `{raw_table}`"
        )),
    }
}

fn maintenance_parser<'a>(sql: &'a str, context: &str) -> Result<Parser<'a>, String> {
    Parser::new(&MaintenanceSqlDialect)
        .try_with_sql(sql)
        .map_err(|error| format!("{context}: {error}"))
}

fn normalize_object_name(name: ObjectName) -> Result<Vec<String>, String> {
    raw_object_name_parts(&name)?
        .into_iter()
        .map(|part| normalize_identifier(&part))
        .collect()
}

fn raw_object_name_parts(name: &ObjectName) -> Result<Vec<String>, String> {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(identifier) => Ok(identifier.value.clone()),
            other => Err(format!("unsupported object name part: {other}")),
        })
        .collect()
}

fn expect_word(parser: &mut Parser<'_>, expected: &str) -> Result<(), String> {
    match parser.next_token().token {
        Token::Word(word) if word.value.eq_ignore_ascii_case(expected) => Ok(()),
        other => Err(format!("expected {expected}, got {other}")),
    }
}

fn peek_word(parser: &Parser<'_>, offset: usize, expected: &str) -> bool {
    matches!(
        &parser.peek_nth_token(offset).token,
        Token::Word(word) if word.value.eq_ignore_ascii_case(expected)
    )
}

fn consume_optional_final_semicolon(parser: &mut Parser<'_>) -> Result<(), String> {
    if parser.consume_token(&Token::SemiColon) && parser.peek_token_ref().token == Token::SemiColon
    {
        return Err("only one final semicolon is allowed".to_string());
    }
    Ok(())
}

fn expect_statement_eof(parser: &Parser<'_>) -> Result<(), String> {
    match parser.peek_token_ref().token {
        Token::EOF => Ok(()),
        ref other => Err(format!("unexpected token after statement: {other}")),
    }
}

fn expect_call_eof(parser: &Parser<'_>) -> Result<(), String> {
    match parser.peek_token_ref().token {
        Token::EOF => Ok(()),
        ref other => Err(format!("unexpected token after CALL procedure: {other}")),
    }
}
