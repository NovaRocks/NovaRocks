use std::collections::{BTreeMap, BTreeSet};

use crate::engine::catalog::normalize_identifier;
use crate::sql::parser::dialect::StarRocksDialect;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlparser::ast::ObjectName;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProcedureArgMode {
    Named,
    Positional,
    Empty,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ProcedureArgValue {
    String(String),
    Boolean(bool),
    Integer(i64),
    TimestampMillis(i64),
    StringMap(BTreeMap<String, String>),
    Null,
}

impl ProcedureArgValue {
    pub(crate) fn as_string(&self) -> Option<&str> {
        match self {
            ProcedureArgValue::String(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn as_bool(&self) -> Option<bool> {
        match self {
            ProcedureArgValue::Boolean(value) => Some(*value),
            _ => None,
        }
    }

    pub(crate) fn as_string_map(&self) -> Option<&BTreeMap<String, String>> {
        match self {
            ProcedureArgValue::StringMap(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProcedureArg {
    pub(crate) name: Option<String>,
    pub(crate) value: ProcedureArgValue,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CallProcedureStmt {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) procedure: String,
    pub(crate) args: Vec<ProcedureArg>,
    pub(crate) mode: ProcedureArgMode,
}

impl CallProcedureStmt {
    pub(crate) fn arg(&self, name: &str) -> Option<&ProcedureArgValue> {
        let normalized = normalize_identifier(name).ok()?;
        self.args.iter().find_map(|arg| {
            if arg.name.as_deref() == Some(normalized.as_str()) {
                Some(&arg.value)
            } else {
                None
            }
        })
    }
}

pub(crate) fn looks_like_call_procedure(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    parser.parse_keyword(Keyword::CALL)
}

pub(crate) fn parse_call_procedure_sql(sql: &str) -> Result<CallProcedureStmt, String> {
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(sql)
        .map_err(|e| format!("parse CALL procedure: {e}"))?;
    parser
        .expect_keyword(Keyword::CALL)
        .map_err(|e| format!("parse CALL procedure: {e}"))?;

    let parts = normalize_object_name(
        parser
            .parse_object_name(false)
            .map_err(|e| format!("parse CALL procedure name: {e}"))?,
    )?;
    let [catalog, namespace, procedure] = parts.as_slice() else {
        return Err("CALL procedure name must be catalog.system.procedure".to_string());
    };
    if namespace != "system" {
        return Err("Iceberg procedures must use system namespace".to_string());
    }

    let args = parse_arg_list(&mut parser)?;
    let mode = classify_arg_mode(&args)?;
    ensure_no_duplicate_named_args(&args)?;
    consume_optional_final_semicolon(&mut parser)?;
    expect_parser_eof(&parser)?;

    Ok(CallProcedureStmt {
        catalog: catalog.clone(),
        namespace: namespace.clone(),
        procedure: procedure.clone(),
        args,
        mode,
    })
}

fn normalize_object_name(name: ObjectName) -> Result<Vec<String>, String> {
    name.0
        .into_iter()
        .map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => normalize_identifier(&ident.value),
            other => Err(format!("unsupported CALL procedure name part: {other}")),
        })
        .collect()
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
        if !seen.insert(name.clone()) {
            return Err(format!("duplicate CALL procedure argument '{name}'"));
        }
    }
    Ok(())
}

fn parse_arg_list(parser: &mut Parser<'_>) -> Result<Vec<ProcedureArg>, String> {
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| format!("CALL procedure expected argument list: {e}"))?;
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
        .map_err(|e| format!("CALL procedure expected end of argument list: {e}"))?;
    Ok(args)
}

fn parse_arg(parser: &mut Parser<'_>) -> Result<ProcedureArg, String> {
    let name = if matches!(parser.peek_token_ref().token, Token::Word(_))
        && token_at_is_fat_arrow(parser, 1)
    {
        let ident = parser
            .parse_identifier()
            .map_err(|e| format!("CALL procedure expected argument name: {e}"))?;
        consume_fat_arrow(parser)?;
        Some(normalize_identifier(&ident.value)?)
    } else {
        None
    };
    let value = parse_arg_value(parser)?;
    Ok(ProcedureArg { name, value })
}

fn parse_arg_value(parser: &mut Parser<'_>) -> Result<ProcedureArgValue, String> {
    if parser.parse_keyword(Keyword::TIMESTAMP) {
        let value = parse_single_quoted_string(parser, "TIMESTAMP")?;
        return parse_timestamp_millis(&value)
            .map(ProcedureArgValue::TimestampMillis)
            .map_err(|err| format!("CALL procedure invalid TIMESTAMP literal: {err}"));
    }
    if parser.parse_keyword(Keyword::MAP) {
        return parse_string_map(parser);
    }

    let token = parser.next_token();
    match token.token {
        Token::SingleQuotedString(value) => Ok(ProcedureArgValue::String(value)),
        Token::Number(value, _) => value
            .parse::<i64>()
            .map(ProcedureArgValue::Integer)
            .map_err(|e| format!("CALL procedure invalid integer argument '{value}': {e}")),
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
        .map_err(|e| format!("CALL procedure map expects (: {e}"))?;
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
            .map_err(|e| format!("CALL procedure map expects ): {e}"))?;
    }
    if values.len() % 2 != 0 {
        return Err("CALL procedure map requires an even number of string literals".to_string());
    }

    let mut map = BTreeMap::new();
    for entry in values.chunks_exact(2) {
        let key = entry[0].clone();
        let value = entry[1].clone();
        if map.insert(key.clone(), value).is_some() {
            return Err(format!("duplicate CALL procedure map key '{key}'"));
        }
    }
    Ok(ProcedureArgValue::StringMap(map))
}

fn parse_single_quoted_string(parser: &mut Parser<'_>, context: &str) -> Result<String, String> {
    let token = parser.next_token();
    match token.token {
        Token::SingleQuotedString(value) => Ok(value),
        other => Err(format!(
            "CALL procedure {context} expects a single quoted string, got {other}"
        )),
    }
}

fn parse_timestamp_millis(value: &str) -> Result<i64, String> {
    if let Ok(timestamp_ms) = parse_rfc3339_millis(value) {
        return Ok(timestamp_ms);
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(value.and_utc().timestamp_millis());
    }
    if value.chars().all(|ch| ch.is_ascii_digit()) && !value.is_empty() {
        return value
            .parse::<i64>()
            .map_err(|e| format!("invalid epoch-ms timestamp '{value}': {e}"));
    }
    Err(format!(
        "cannot parse timestamp '{value}'; expected RFC3339, epoch-ms, or YYYY-MM-DD HH:MM:SS"
    ))
}

fn parse_rfc3339_millis(value: &str) -> Result<i64, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|dt| dt.with_timezone(&Utc).timestamp_millis())
}

fn token_at_is_fat_arrow(parser: &Parser<'_>, offset: usize) -> bool {
    matches!(parser.peek_nth_token_ref(offset).token, Token::RArrow)
}

fn consume_fat_arrow(parser: &mut Parser<'_>) -> Result<(), String> {
    match parser.next_token().token {
        Token::RArrow => Ok(()),
        other => Err(format!("CALL procedure expected =>, got {other}")),
    }
}

fn consume_optional_final_semicolon(parser: &mut Parser<'_>) -> Result<(), String> {
    if parser.consume_token(&Token::SemiColon) && parser.peek_token_ref().token == Token::SemiColon
    {
        return Err("only one final semicolon is allowed".to_string());
    }
    Ok(())
}

fn expect_parser_eof(parser: &Parser<'_>) -> Result<(), String> {
    match parser.peek_token_ref().token {
        Token::EOF => Ok(()),
        ref other => Err(format!("unexpected token after CALL procedure: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn looks_like_call_detects_system_procedure() {
        assert!(looks_like_call_procedure(
            "CALL ice.system.rewrite_manifests(table => 'db.t')"
        ));
        assert!(looks_like_call_procedure(
            "CALL ice.admin.rewrite_manifests(table => 'db.t')"
        ));
        assert!(!looks_like_call_procedure("SELECT 1"));
    }

    #[test]
    fn parse_named_arguments() {
        let stmt = parse_call_procedure_sql(
            "CALL ice.system.rewrite_position_delete_files(table => 'db.t', options => map('rewrite-all', 'true'))",
        )
        .unwrap();
        assert_eq!(stmt.catalog, "ice");
        assert_eq!(stmt.namespace, "system");
        assert_eq!(stmt.procedure, "rewrite_position_delete_files");
        assert_eq!(stmt.args.len(), 2);
        assert!(matches!(stmt.mode, ProcedureArgMode::Named));
        assert_eq!(stmt.arg("table").unwrap().as_string().unwrap(), "db.t");
        assert_eq!(
            stmt.arg("options")
                .unwrap()
                .as_string_map()
                .unwrap()
                .get("rewrite-all")
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn plain_string_epoch_millis_stays_string() {
        let stmt =
            parse_call_procedure_sql("CALL ice.system.rewrite_manifests(table => '1700000000000')")
                .unwrap();
        assert_eq!(
            stmt.arg("table").unwrap(),
            &ProcedureArgValue::String("1700000000000".to_string())
        );
    }

    #[test]
    fn explicit_timestamp_literal_parses_timestamp_millis() {
        let stmt = parse_call_procedure_sql(
            "CALL ice.system.expire_snapshots(table => 'db.t', older_than => TIMESTAMP '2026-01-01 00:00:00')",
        )
        .unwrap();
        assert_eq!(
            stmt.arg("older_than").unwrap(),
            &ProcedureArgValue::TimestampMillis(1767225600000)
        );
    }

    #[test]
    fn parse_positional_arguments() {
        let stmt =
            parse_call_procedure_sql("CALL ice.system.rewrite_manifests('db.t', false)").unwrap();
        assert_eq!(stmt.catalog, "ice");
        assert_eq!(stmt.procedure, "rewrite_manifests");
        assert!(matches!(stmt.mode, ProcedureArgMode::Positional));
        assert_eq!(stmt.args[0].name, None);
        assert_eq!(stmt.args[0].value.as_string().unwrap(), "db.t");
        assert_eq!(stmt.args[1].value.as_bool().unwrap(), false);
    }

    #[test]
    fn named_rewrite_manifests_to_action_request() {
        let stmt =
            parse_call_procedure_sql("CALL ice.system.rewrite_manifests(table => 'db.t')").unwrap();
        let req =
            crate::engine::iceberg_maintenance::MaintenanceActionRequest::from_call(&stmt, "db")
                .unwrap();
        assert_eq!(req.catalog, "ice");
        assert_eq!(req.namespace, "db");
        assert_eq!(req.table, "t");
        assert_eq!(
            req.kind,
            crate::engine::iceberg_maintenance::MaintenanceActionKind::RewriteManifests
        );
    }

    #[test]
    fn positional_rewrite_manifests_to_action_request() {
        let stmt =
            parse_call_procedure_sql("CALL ice.system.rewrite_manifests('db.t', false)").unwrap();
        let req =
            crate::engine::iceberg_maintenance::MaintenanceActionRequest::from_call(&stmt, "db")
                .unwrap();
        assert_eq!(req.use_caching, Some(false));
    }

    #[test]
    fn unknown_procedure_rejected() {
        let stmt =
            parse_call_procedure_sql("CALL ice.system.unknown_proc(table => 'db.t')").unwrap();
        let err =
            crate::engine::iceberg_maintenance::MaintenanceActionRequest::from_call(&stmt, "db")
                .unwrap_err();
        assert!(err.contains("unsupported Iceberg system procedure"));
    }

    #[test]
    fn remove_orphan_dry_run_rejected_until_supported() {
        let stmt = parse_call_procedure_sql(
            "CALL ice.system.remove_orphan_files(table => 'db.t', older_than => TIMESTAMP '2026-01-01 00:00:00', dry_run => true)",
        )
        .unwrap();
        let err =
            crate::engine::iceberg_maintenance::MaintenanceActionRequest::from_call(&stmt, "db")
                .unwrap_err();
        assert!(err.contains("not implemented"));
    }

    #[test]
    fn rejects_mixed_named_and_positional_arguments() {
        let err = parse_call_procedure_sql(
            "CALL ice.system.rewrite_manifests('db.t', use_caching => false)",
        )
        .unwrap_err();
        assert!(err.contains("cannot mix positional and named arguments"));
    }

    #[test]
    fn rejects_non_system_namespace() {
        let err = parse_call_procedure_sql("CALL ice.admin.rewrite_manifests(table => 'db.t')")
            .unwrap_err();
        assert!(err.contains("Iceberg procedures must use system namespace"));
    }

    #[test]
    fn rejects_duplicate_named_arguments_after_normalization() {
        let err = parse_call_procedure_sql(
            "CALL ice.system.rewrite_manifests(table => 'db.t', TABLE => 'db.u')",
        )
        .unwrap_err();
        assert!(err.contains("duplicate CALL procedure argument 'table'"));
    }
}
