pub(crate) mod create_catalog;
pub(crate) mod create_table;
pub(crate) mod drop;
pub(crate) mod materialized_view;

use sqlparser::ast as sqlast;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::sql::parser::ast::{ObjectName, SqlType};

/// Custom StarRocks dialect for sqlparser.
#[derive(Debug)]
pub(crate) struct StarRocksDialect;

impl sqlparser::dialect::Dialect for StarRocksDialect {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_' || ch == '$'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        false
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn supports_limit_comma(&self) -> bool {
        true
    }
}

/// Peek at a token by offset and check if it matches a word (case-insensitive).
pub(crate) fn peek_word_eq(parser: &Parser<'_>, offset: usize, word: &str) -> bool {
    // sqlparser 0.61 uses const-generic peek_tokens_ref<N>().
    // We use peek_nth_token_ref for arbitrary offsets.
    let token = parser.peek_nth_token_ref(offset);
    match &token.token {
        Token::Word(w) => w.value.eq_ignore_ascii_case(word),
        _ => false,
    }
}

/// Convert a sqlparser `sqlast::ObjectName` to our custom `ObjectName`.
pub(crate) fn convert_object_name(name: sqlast::ObjectName) -> Result<ObjectName, String> {
    let parts: Vec<String> = name
        .0
        .into_iter()
        .map(|part| match part {
            sqlast::ObjectNamePart::Identifier(ident) => Ok(ident.value),
            other => Err(format!("unsupported object name part: {other}")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if parts.is_empty() {
        return Err("empty object name".to_string());
    }
    Ok(ObjectName { parts })
}

/// Convert a sqlparser data type to our custom SqlType.
pub(crate) fn convert_sql_type(data_type: sqlast::DataType) -> Result<SqlType, String> {
    match data_type {
        sqlast::DataType::TinyInt(_) => Ok(SqlType::TinyInt),
        sqlast::DataType::SmallInt(_) => Ok(SqlType::SmallInt),
        sqlast::DataType::Int(_) | sqlast::DataType::Integer(_) => Ok(SqlType::Int),
        sqlast::DataType::BigInt(_) => Ok(SqlType::BigInt),
        sqlast::DataType::Float(_) => Ok(SqlType::Float),
        sqlast::DataType::Double(_) | sqlast::DataType::DoublePrecision => Ok(SqlType::Double),
        sqlast::DataType::Boolean => Ok(SqlType::Boolean),
        sqlast::DataType::Varchar(_)
        | sqlast::DataType::CharVarying(_)
        | sqlast::DataType::Text => Ok(SqlType::String),
        sqlast::DataType::Char(_) | sqlast::DataType::Character(_) => Ok(SqlType::String),
        sqlast::DataType::String(_) => Ok(SqlType::String),
        sqlast::DataType::Date => Ok(SqlType::Date),
        sqlast::DataType::Datetime(_) | sqlast::DataType::Timestamp(_, _) => Ok(SqlType::DateTime),
        sqlast::DataType::Decimal(info)
        | sqlast::DataType::Dec(info)
        | sqlast::DataType::Numeric(info) => match info {
            sqlast::ExactNumberInfo::PrecisionAndScale(p, s) => Ok(SqlType::Decimal {
                precision: p as u8,
                scale: s as i8,
            }),
            sqlast::ExactNumberInfo::Precision(p) => Ok(SqlType::Decimal {
                precision: p as u8,
                scale: 0,
            }),
            sqlast::ExactNumberInfo::None => Ok(SqlType::Decimal {
                precision: 38,
                scale: 0,
            }),
        },
        sqlast::DataType::Array(elem_def) => {
            let inner = match elem_def {
                sqlast::ArrayElemTypeDef::AngleBracket(inner_type) => {
                    convert_sql_type(*inner_type)?
                }
                sqlast::ArrayElemTypeDef::SquareBracket(inner_type, _) => {
                    convert_sql_type(*inner_type)?
                }
                sqlast::ArrayElemTypeDef::Parenthesis(inner_type) => convert_sql_type(*inner_type)?,
                sqlast::ArrayElemTypeDef::None => {
                    return Err("ARRAY type requires an element type".to_string());
                }
            };
            Ok(SqlType::Array(Box::new(inner)))
        }
        sqlast::DataType::Map(key_type, value_type) => Ok(SqlType::Map(
            Box::new(convert_sql_type(*key_type)?),
            Box::new(convert_sql_type(*value_type)?),
        )),
        sqlast::DataType::Struct(fields, _) => Ok(SqlType::Struct(
            fields
                .into_iter()
                .enumerate()
                .map(|(idx, field)| {
                    let name = field.field_name.map(|ident| ident.value).ok_or_else(|| {
                        format!("STRUCT field at position {} requires a name", idx + 1)
                    })?;
                    let field_type = convert_sql_type(field.field_type)?;
                    Ok((name, field_type))
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
        sqlast::DataType::Varbinary(_) | sqlast::DataType::Binary(_) => Ok(SqlType::Binary),
        sqlast::DataType::Custom(name, modifiers) => {
            let n = name.to_string().to_lowercase();
            match n.as_str() {
                "string" => Ok(SqlType::String),
                "largeint" => Ok(SqlType::LargeInt),
                "json" | "jsonb" => Ok(SqlType::String),
                "varbinary" | "binary" => Ok(SqlType::Binary),
                "decimal32" | "decimal64" | "decimal128" => {
                    let (precision, scale) = parse_custom_decimal_modifiers(&modifiers);
                    Ok(SqlType::Decimal { precision, scale })
                }
                _ => Err(format!("unsupported data type: {name}")),
            }
        }
        other => Err(format!("unsupported data type: {other}")),
    }
}

/// Parse precision and scale from custom type modifiers like `["10", "2"]`.
/// Returns default `(38, 0)` when modifiers are missing or unparseable.
fn parse_custom_decimal_modifiers(modifiers: &[String]) -> (u8, i8) {
    match modifiers.len() {
        0 => (38, 0),
        1 => {
            let p = modifiers[0].trim().parse::<u8>().unwrap_or(38);
            (p, 0)
        }
        _ => {
            let p = modifiers[0].trim().parse::<u8>().unwrap_or(38);
            let s = modifiers[1].trim().parse::<i8>().unwrap_or(0);
            (p, s)
        }
    }
}

// ---------------------------------------------------------------------------
// Token-level lookahead helpers (moved from sqlparser_backend)
// ---------------------------------------------------------------------------

pub(crate) fn looks_like_create_catalog(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::CREATE)
        && ((peek_word_eq(parser, 1, "EXTERNAL") && peek_word_eq(parser, 2, "CATALOG"))
            || peek_word_eq(parser, 1, "CATALOG"))
}

pub(crate) fn looks_like_create_table(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::CREATE)
        && (peek_word_eq(parser, 1, "TABLE")
            || (peek_word_eq(parser, 1, "TEMPORARY") && peek_word_eq(parser, 2, "TABLE"))
            || (peek_word_eq(parser, 1, "EXTERNAL") && peek_word_eq(parser, 2, "TABLE")))
}

pub(crate) fn looks_like_create_database(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::CREATE) && peek_word_eq(parser, 1, "DATABASE")
}

pub(crate) fn looks_like_drop_statement(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::DROP)
        && (peek_word_eq(parser, 1, "TABLE")
            || peek_word_eq(parser, 1, "DATABASE")
            || peek_word_eq(parser, 1, "CATALOG"))
}

/// Parse a CREATE DATABASE statement and return just the database name.
pub(crate) fn parse_create_database_name(parser: &mut Parser<'_>) -> Result<ObjectName, String> {
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::DATABASE)
        .map_err(|e| e.to_string())?;
    // Allow IF NOT EXISTS — just skip the keywords (create_database is idempotent).
    let _if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;
    Ok(name)
}

// ---------------------------------------------------------------------------
// SQL normalization utilities (moved from sqlparser_backend)
// ---------------------------------------------------------------------------

/// Normalize SQL syntax for parsing. This applies rewrites that make
/// StarRocks-specific syntax compatible with the sqlparser crate.
pub(crate) fn normalize_for_raw_parse(sql: &str) -> Result<String, String> {
    let sql = rewrite_set_user_variables(sql)?;
    let sql = rewrite_from_dual(&sql)?;
    let sql = normalize_function_syntax(&sql)?;
    Ok(rewrite_create_table_nested_generic_closers(&sql))
}

/// Strip a bare `FROM dual` so the managed-lake path doesn't need a real
/// `dual` table. Only rewrites when the `FROM dual` appears at top-level
/// with nothing meaningful after it (end of string, `;`, or a comment).
/// Anything else (WHERE/GROUP/HAVING/LIMIT/ORDER/JOIN) is left untouched
/// so downstream parsing reports the familiar "unknown table" error.
fn rewrite_from_dual(sql: &str) -> Result<String, String> {
    let bytes = sql.as_bytes();
    let mut idx = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if single_quote {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                single_quote = false;
            }
            idx += 1;
            continue;
        }
        if double_quote {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                double_quote = false;
            }
            idx += 1;
            continue;
        }
        if backtick {
            if byte == b'`' {
                backtick = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => single_quote = true,
            b'"' => double_quote = true,
            b'`' => backtick = true,
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                let comment_end = sql[idx + 2..]
                    .find("*/")
                    .map(|offset| idx + 2 + offset)
                    .ok_or_else(|| "unterminated comment in SQL".to_string())?;
                idx = comment_end + 2;
                continue;
            }
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                let line_end = sql[idx..]
                    .find('\n')
                    .map(|offset| idx + offset)
                    .unwrap_or(sql.len());
                idx = line_end;
                continue;
            }
            _ if starts_with_keyword(bytes, idx, "from")
                && !is_identifier_byte(bytes.get(idx.wrapping_sub(1)).copied())
                && !is_identifier_byte(bytes.get(idx + "from".len()).copied()) =>
            {
                let dual_start = skip_ascii_whitespace(bytes, idx + "from".len());
                if dual_start == idx + "from".len() {
                    idx += 1;
                    continue;
                }
                let dual_end = dual_start + "dual".len();
                if !starts_with_keyword(bytes, dual_start, "dual")
                    || is_identifier_byte(bytes.get(dual_end).copied())
                {
                    idx += 1;
                    continue;
                }
                let suffix_start = skip_ascii_whitespace(bytes, dual_end);
                if !matches_from_dual_suffix(bytes, suffix_start) {
                    idx += 1;
                    continue;
                }

                let prefix_end = trim_trailing_ascii_whitespace(sql, idx);
                let mut rewritten = String::with_capacity(sql.len());
                rewritten.push_str(&sql[..prefix_end]);
                if suffix_start < sql.len()
                    && starts_with_comment(bytes, suffix_start)
                    && prefix_end > 0
                {
                    rewritten.push(' ');
                }
                rewritten.push_str(&sql[suffix_start..]);
                return Ok(rewritten);
            }
            _ => {}
        }
        idx += 1;
    }
    Ok(sql.to_string())
}

fn skip_ascii_whitespace(bytes: &[u8], mut idx: usize) -> usize {
    while bytes.get(idx).is_some_and(u8::is_ascii_whitespace) {
        idx += 1;
    }
    idx
}

fn trim_trailing_ascii_whitespace(sql: &str, mut end: usize) -> usize {
    let bytes = sql.as_bytes();
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    end
}

fn starts_with_comment(bytes: &[u8], idx: usize) -> bool {
    bytes.get(idx) == Some(&b'/') && bytes.get(idx + 1) == Some(&b'*')
        || bytes.get(idx) == Some(&b'-') && bytes.get(idx + 1) == Some(&b'-')
}

fn matches_from_dual_suffix(bytes: &[u8], idx: usize) -> bool {
    idx >= bytes.len() || bytes.get(idx) == Some(&b';') || starts_with_comment(bytes, idx)
}

fn rewrite_set_user_variables(sql: &str) -> Result<String, String> {
    let assignments = extract_set_user_variable_assignments(sql)?;
    if assignments.is_empty() {
        return Ok(sql.to_string());
    }
    substitute_user_variables(sql, &assignments)
}

pub(crate) fn normalize_function_syntax(sql: &str) -> Result<String, String> {
    let sql = rewrite_group_concat_separator(sql)?;
    let sql = rewrite_cast_target_type_syntax(&sql)?;
    let sql = rewrite_typed_array_literals(&sql)?;
    rewrite_legacy_map_literals(&sql)
}

fn rewrite_cast_target_type_syntax(sql: &str) -> Result<String, String> {
    let mut output = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut idx = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;

    while idx < bytes.len() {
        let byte = bytes[idx];
        if single_quote {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                single_quote = false;
            }
            idx = push_original_char(&mut output, sql, idx);
            continue;
        }
        if double_quote {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                double_quote = false;
            }
            idx = push_original_char(&mut output, sql, idx);
            continue;
        }
        if backtick {
            if byte == b'`' {
                backtick = false;
            }
            idx = push_original_char(&mut output, sql, idx);
            continue;
        }

        match byte {
            b'\'' => {
                single_quote = true;
                idx = push_original_char(&mut output, sql, idx);
                continue;
            }
            b'"' => {
                double_quote = true;
                idx = push_original_char(&mut output, sql, idx);
                continue;
            }
            b'`' => {
                backtick = true;
                idx = push_original_char(&mut output, sql, idx);
                continue;
            }
            _ => {}
        }

        if starts_with_keyword(bytes, idx, "cast")
            && !is_identifier_byte(bytes.get(idx.wrapping_sub(1)).copied())
        {
            let mut cursor = idx + "cast".len();
            while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            if cursor < bytes.len() && bytes[cursor] == b'(' {
                let close_idx = find_matching_paren(sql, cursor)?;
                let body = &sql[cursor + 1..close_idx];
                let rewritten_body = rewrite_cast_call_body(body)?;
                output.push_str(&sql[idx..cursor + 1]);
                output.push_str(&rewritten_body);
                output.push(')');
                idx = close_idx + 1;
                continue;
            }
        }

        idx = push_original_char(&mut output, sql, idx);
    }

    Ok(output)
}

fn rewrite_cast_call_body(body: &str) -> Result<String, String> {
    let Some(as_idx) = find_top_level_keyword(body, "as") else {
        return Ok(body.to_string());
    };
    let expr = body[..as_idx].trim_end();
    let target = body[as_idx + "as".len()..].trim_start();
    let rewritten_target = rewrite_map_type_generics(target)?;
    if rewritten_target == target {
        Ok(body.to_string())
    } else {
        Ok(format!("{expr} AS {rewritten_target}"))
    }
}

fn rewrite_map_type_generics(target: &str) -> Result<String, String> {
    let mut output = String::with_capacity(target.len());
    let bytes = target.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        if starts_with_keyword(bytes, idx, "map")
            && !is_identifier_byte(bytes.get(idx.wrapping_sub(1)).copied())
        {
            let mut cursor = idx + "map".len();
            while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            if cursor < bytes.len() && bytes[cursor] == b'<' {
                let end_idx = find_matching_delimiter(target, cursor, b'<', b'>')?;
                let inner = rewrite_map_type_generics(&target[cursor + 1..end_idx])?;
                output.push_str("MAP(");
                output.push_str(&inner);
                output.push(')');
                idx = end_idx + 1;
                continue;
            }
        }
        idx = push_original_char(&mut output, target, idx);
    }
    Ok(output)
}

fn extract_set_user_variable_assignments(sql: &str) -> Result<Vec<(String, String)>, String> {
    let bytes = sql.as_bytes();
    let mut assignments = Vec::new();
    let mut idx = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if single_quote {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                single_quote = false;
            }
            idx += 1;
            continue;
        }
        if double_quote {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                double_quote = false;
            }
            idx += 1;
            continue;
        }
        if backtick {
            if byte == b'`' {
                backtick = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => single_quote = true,
            b'"' => double_quote = true,
            b'`' => backtick = true,
            b'/' if bytes.get(idx + 1) == Some(&b'*') && bytes.get(idx + 2) == Some(&b'+') => {
                let comment_end = sql[idx + 3..]
                    .find("*/")
                    .map(|offset| idx + 3 + offset)
                    .ok_or_else(|| "unterminated optimizer hint comment".to_string())?;
                collect_set_user_variable_assignments(
                    &sql[idx + 3..comment_end],
                    &mut assignments,
                )?;
                idx = comment_end + 2;
                continue;
            }
            _ => {}
        }
        idx += 1;
    }
    Ok(assignments)
}

fn collect_set_user_variable_assignments(
    hint_text: &str,
    assignments: &mut Vec<(String, String)>,
) -> Result<(), String> {
    let lower = hint_text.to_ascii_lowercase();
    let mut search_idx = 0usize;
    while let Some(rel) = lower[search_idx..].find("set_user_variable") {
        let keyword_idx = search_idx + rel;
        let mut open_idx = keyword_idx + "set_user_variable".len();
        while hint_text
            .as_bytes()
            .get(open_idx)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            open_idx += 1;
        }
        if hint_text.as_bytes().get(open_idx) != Some(&b'(') {
            search_idx = keyword_idx + "set_user_variable".len();
            continue;
        }
        let close_idx = find_matching_paren(hint_text, open_idx)?;
        let body = &hint_text[open_idx + 1..close_idx];
        for assignment in split_top_level_items(body, b',') {
            if assignment.trim().is_empty() {
                continue;
            }
            let Some(eq_idx) = find_top_level_char(assignment, b'=') else {
                return Err(format!(
                    "invalid set_user_variable hint assignment: {}",
                    assignment.trim()
                ));
            };
            let name = assignment[..eq_idx].trim().to_ascii_lowercase();
            let value = assignment[eq_idx + 1..].trim();
            if !name.starts_with('@') || value.is_empty() {
                return Err(format!(
                    "invalid set_user_variable hint assignment: {}",
                    assignment.trim()
                ));
            }
            if let Some(existing_idx) = assignments.iter().position(|(key, _)| key == &name) {
                assignments.remove(existing_idx);
            }
            assignments.push((name, value.to_string()));
        }
        search_idx = close_idx + 1;
    }
    Ok(())
}

fn substitute_user_variables(
    sql: &str,
    assignments: &[(String, String)],
) -> Result<String, String> {
    let assignment_map = assignments
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<std::collections::HashMap<_, _>>();

    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len());
    let mut idx = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if single_quote {
            output.push(byte as char);
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                single_quote = false;
            }
            idx += 1;
            continue;
        }
        if double_quote {
            output.push(byte as char);
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                double_quote = false;
            }
            idx += 1;
            continue;
        }
        if backtick {
            output.push(byte as char);
            if byte == b'`' {
                backtick = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => {
                single_quote = true;
                output.push('\'');
                idx += 1;
            }
            b'"' => {
                double_quote = true;
                output.push('"');
                idx += 1;
            }
            b'`' => {
                backtick = true;
                output.push('`');
                idx += 1;
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                let comment_end = sql[idx + 2..]
                    .find("*/")
                    .map(|offset| idx + 2 + offset)
                    .ok_or_else(|| "unterminated comment in SQL".to_string())?;
                output.push_str(&sql[idx..comment_end + 2]);
                idx = comment_end + 2;
            }
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                let line_end = sql[idx..]
                    .find('\n')
                    .map(|offset| idx + offset)
                    .unwrap_or(sql.len());
                output.push_str(&sql[idx..line_end]);
                idx = line_end;
            }
            b'@' => {
                let end_idx = find_variable_name_end(bytes, idx);
                let variable_name = sql[idx..end_idx].to_ascii_lowercase();
                if let Some(value) = assignment_map.get(variable_name.as_str()) {
                    output.push_str(value);
                    idx = end_idx;
                } else {
                    output.push_str(&sql[idx..end_idx]);
                    idx = end_idx;
                }
            }
            _ => {
                idx = push_original_char(&mut output, sql, idx);
            }
        }
    }
    Ok(output)
}

fn rewrite_group_concat_separator(sql: &str) -> Result<String, String> {
    let mut output = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        if starts_with_keyword(bytes, idx, "group_concat")
            && !is_identifier_byte(bytes.get(idx.wrapping_sub(1)).copied())
        {
            let name_end = idx + "group_concat".len();
            output.push_str(&sql[idx..name_end]);
            let mut cursor = name_end;
            while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                output.push(bytes[cursor] as char);
                cursor += 1;
            }
            if cursor >= bytes.len() || bytes[cursor] != b'(' {
                idx = cursor;
                continue;
            }
            let call_end = find_matching_paren(sql, cursor)?;
            let inner = &sql[cursor + 1..call_end];
            let rewritten = rewrite_group_concat_inner(inner)?;
            output.push('(');
            output.push_str(&rewritten);
            output.push(')');
            idx = call_end + 1;
        } else {
            idx = push_original_char(&mut output, sql, idx);
        }
    }
    Ok(output)
}

fn rewrite_group_concat_inner(inner: &str) -> Result<String, String> {
    if let Some(separator_pos) = find_top_level_keyword(inner, "separator") {
        let separator_start = separator_pos + "separator".len();
        let before_separator = inner[..separator_pos].trim_end();
        let separator_expr = inner[separator_start..].trim();
        if before_separator.is_empty() || separator_expr.is_empty() {
            return Err("invalid GROUP_CONCAT separator syntax".to_string());
        }
        if let Some(order_by_pos) = find_top_level_order_by(before_separator) {
            let args = before_separator[..order_by_pos].trim_end();
            let order_by = before_separator[order_by_pos..].trim_start();
            Ok(format!("{args}, {separator_expr} {order_by}"))
        } else {
            Ok(format!("{before_separator}, {separator_expr}"))
        }
    } else if let Some(order_by_pos) = find_top_level_order_by(inner) {
        let args = inner[..order_by_pos].trim_end();
        let order_by = inner[order_by_pos..].trim_start();
        if args.is_empty() {
            Ok(inner.to_string())
        } else {
            Ok(format!("{args}, ',' {order_by}"))
        }
    } else {
        let args = inner.trim_end();
        if args.is_empty() {
            Ok(inner.to_string())
        } else {
            Ok(format!("{args}, ','"))
        }
    }
}

fn rewrite_typed_array_literals(sql: &str) -> Result<String, String> {
    let mut output = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        if starts_with_keyword(bytes, idx, "array")
            && !is_identifier_byte(bytes.get(idx.wrapping_sub(1)).copied())
        {
            let type_start = idx;
            let mut cursor = idx + "array".len();
            while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            if cursor < bytes.len() && bytes[cursor] == b'<' {
                let type_end = find_matching_delimiter(sql, cursor, b'<', b'>')?;
                let mut literal_start = type_end + 1;
                while literal_start < bytes.len() && bytes[literal_start].is_ascii_whitespace() {
                    literal_start += 1;
                }
                if literal_start < bytes.len() && bytes[literal_start] == b'[' {
                    let literal_end = find_matching_delimiter(sql, literal_start, b'[', b']')?;
                    output.push_str("CAST(");
                    output.push_str(&sql[literal_start..=literal_end]);
                    output.push_str(" AS ");
                    output.push_str(&sql[type_start..=type_end]);
                    output.push(')');
                    idx = literal_end + 1;
                    continue;
                }
            }
        }
        idx = push_original_char(&mut output, sql, idx);
    }
    Ok(output)
}

fn rewrite_legacy_map_literals(sql: &str) -> Result<String, String> {
    let mut output = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        if starts_with_keyword(bytes, idx, "map")
            && !is_identifier_byte(bytes.get(idx.wrapping_sub(1)).copied())
        {
            let name_end = idx + "map".len();
            let mut cursor = name_end;
            while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            if cursor < bytes.len() && bytes[cursor] == b'{' {
                output.push_str("map(");
                let (body, end_idx) = rewrite_legacy_map_literal_body(sql, cursor)?;
                output.push_str(&body);
                output.push(')');
                idx = end_idx + 1;
                continue;
            }
        }
        idx = push_original_char(&mut output, sql, idx);
    }
    Ok(output)
}

fn rewrite_legacy_map_literal_body(sql: &str, open_idx: usize) -> Result<(String, usize), String> {
    let bytes = sql.as_bytes();
    let mut output = String::new();
    let mut idx = open_idx + 1;
    let mut paren_depth = 0usize;
    let mut square_depth = 0usize;
    let mut brace_depth = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;

    while idx < bytes.len() {
        let byte = bytes[idx];
        if single_quote {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                output.push('\'');
                single_quote = false;
                idx += 1;
            } else {
                idx = push_original_char(&mut output, sql, idx);
            }
            continue;
        }
        if double_quote {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                output.push('"');
                double_quote = false;
                idx += 1;
            } else {
                idx = push_original_char(&mut output, sql, idx);
            }
            continue;
        }
        if backtick {
            if byte == b'`' {
                output.push('`');
                backtick = false;
                idx += 1;
            } else {
                idx = push_original_char(&mut output, sql, idx);
            }
            continue;
        }

        if starts_with_keyword(bytes, idx, "map")
            && !is_identifier_byte(bytes.get(idx.wrapping_sub(1)).copied())
        {
            let name_end = idx + "map".len();
            let mut cursor = name_end;
            while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            if cursor < bytes.len() && bytes[cursor] == b'{' {
                output.push_str("map(");
                let (body, end_idx) = rewrite_legacy_map_literal_body(sql, cursor)?;
                output.push_str(&body);
                output.push(')');
                idx = end_idx + 1;
                continue;
            }
        }

        match byte {
            b'\'' => {
                single_quote = true;
                output.push('\'');
            }
            b'"' => {
                double_quote = true;
                output.push('"');
            }
            b'`' => {
                backtick = true;
                output.push('`');
            }
            b'(' => {
                paren_depth += 1;
                output.push('(');
            }
            b')' => {
                paren_depth = paren_depth.saturating_sub(1);
                output.push(')');
            }
            b'[' => {
                square_depth += 1;
                output.push('[');
            }
            b']' => {
                square_depth = square_depth.saturating_sub(1);
                output.push(']');
            }
            b'{' => {
                brace_depth += 1;
                output.push('{');
            }
            b'}' => {
                if brace_depth == 0 {
                    return Ok((output, idx));
                }
                brace_depth -= 1;
                output.push('}');
            }
            b':' if paren_depth == 0 && square_depth == 0 && brace_depth == 0 => {
                output.push(',');
            }
            _ => {
                idx = push_original_char(&mut output, sql, idx);
                continue;
            }
        }
        idx += 1;
    }

    Err("unterminated legacy MAP literal in SQL".to_string())
}

fn rewrite_create_table_nested_generic_closers(sql: &str) -> String {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    if !(lower.starts_with("create table")
        || lower.starts_with("create temporary table")
        || lower.starts_with("create external table"))
    {
        return sql.to_string();
    }

    let mut output = String::with_capacity(sql.len() + 8);
    let bytes = sql.as_bytes();
    let mut idx = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;

    while idx < bytes.len() {
        let byte = bytes[idx];
        if single_quote {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                output.push('\'');
                single_quote = false;
                idx += 1;
            } else {
                idx = push_original_char(&mut output, sql, idx);
            }
            continue;
        }
        if double_quote {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                output.push('"');
                double_quote = false;
                idx += 1;
            } else {
                idx = push_original_char(&mut output, sql, idx);
            }
            continue;
        }
        if backtick {
            if byte == b'`' {
                output.push('`');
                backtick = false;
                idx += 1;
            } else {
                idx = push_original_char(&mut output, sql, idx);
            }
            continue;
        }

        match byte {
            b'\'' => {
                single_quote = true;
                output.push('\'');
                idx += 1;
            }
            b'"' => {
                double_quote = true;
                output.push('"');
                idx += 1;
            }
            b'`' => {
                backtick = true;
                output.push('`');
                idx += 1;
            }
            b'>' => {
                let mut end = idx + 1;
                while end < bytes.len() && bytes[end] == b'>' {
                    end += 1;
                }
                let count = end - idx;
                output.push('>');
                for _ in 1..count {
                    output.push(' ');
                    output.push('>');
                }
                idx = end;
            }
            _ => {
                idx = push_original_char(&mut output, sql, idx);
            }
        }
    }

    output
}

fn find_matching_paren(sql: &str, open_idx: usize) -> Result<usize, String> {
    find_matching_delimiter(sql, open_idx, b'(', b')')
}

fn find_matching_delimiter(
    sql: &str,
    open_idx: usize,
    open_byte: u8,
    close_byte: u8,
) -> Result<usize, String> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut idx = open_idx;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if single_quote {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                single_quote = false;
            }
        } else if double_quote {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                double_quote = false;
            }
        } else if backtick {
            if byte == b'`' {
                backtick = false;
            }
        } else {
            match byte {
                b'\'' => single_quote = true,
                b'"' => double_quote = true,
                b'`' => backtick = true,
                value if value == open_byte => depth += 1,
                value if value == close_byte => {
                    depth = depth
                        .checked_sub(1)
                        .ok_or_else(|| "unbalanced parentheses in SQL".to_string())?;
                    if depth == 0 {
                        return Ok(idx);
                    }
                }
                _ => {}
            }
        }
        idx += 1;
    }
    Err("unterminated function call in SQL".to_string())
}

fn find_top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut idx = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if single_quote {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                single_quote = false;
            }
            idx += 1;
            continue;
        }
        if double_quote {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                double_quote = false;
            }
            idx += 1;
            continue;
        }
        if backtick {
            if byte == b'`' {
                backtick = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => single_quote = true,
            b'"' => double_quote = true,
            b'`' => backtick = true,
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {
                if depth == 0
                    && starts_with_keyword(bytes, idx, keyword)
                    && !is_identifier_byte(bytes.get(idx.wrapping_sub(1)).copied())
                    && !is_identifier_byte(bytes.get(idx + keyword.len()).copied())
                {
                    return Some(idx);
                }
            }
        }
        idx += 1;
    }
    None
}

fn find_top_level_order_by(sql: &str) -> Option<usize> {
    let order_pos = find_top_level_keyword(sql, "order")?;
    let rest = &sql[order_pos + "order".len()..];
    let by_offset = rest.char_indices().find_map(|(offset, ch)| {
        if ch.is_whitespace() {
            None
        } else if rest[offset..].len() >= 2 && rest[offset..].to_ascii_lowercase().starts_with("by")
        {
            Some(offset)
        } else {
            None
        }
    })?;
    let by_absolute = order_pos + "order".len() + by_offset;
    if starts_with_keyword(sql.as_bytes(), by_absolute, "by")
        && !is_identifier_byte(sql.as_bytes().get(by_absolute + 2).copied())
    {
        Some(order_pos)
    } else {
        None
    }
}

fn starts_with_keyword(bytes: &[u8], idx: usize, keyword: &str) -> bool {
    let keyword_bytes = keyword.as_bytes();
    bytes
        .get(idx..idx + keyword_bytes.len())
        .is_some_and(|slice| slice.eq_ignore_ascii_case(keyword_bytes))
}

fn find_top_level_char(sql: &str, target: u8) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut idx = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if single_quote {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                single_quote = false;
            }
            idx += 1;
            continue;
        }
        if double_quote {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                double_quote = false;
            }
            idx += 1;
            continue;
        }
        if backtick {
            if byte == b'`' {
                backtick = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => single_quote = true,
            b'"' => double_quote = true,
            b'`' => backtick = true,
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            value if depth == 0 && value == target => return Some(idx),
            _ => {}
        }
        idx += 1;
    }
    None
}

fn split_top_level_items(sql: &str, delimiter: u8) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut idx = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if single_quote {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                single_quote = false;
            }
            idx += 1;
            continue;
        }
        if double_quote {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                double_quote = false;
            }
            idx += 1;
            continue;
        }
        if backtick {
            if byte == b'`' {
                backtick = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => single_quote = true,
            b'"' => double_quote = true,
            b'`' => backtick = true,
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            value if depth == 0 && value == delimiter => {
                out.push(sql[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
        idx += 1;
    }
    out.push(sql[start..].trim());
    out
}

fn is_identifier_byte(byte: Option<u8>) -> bool {
    byte.is_some_and(|value| value == b'_' || value.is_ascii_alphanumeric())
}

fn is_variable_name_byte(byte: u8) -> bool {
    byte == b'_' || byte.is_ascii_alphanumeric()
}

fn find_variable_name_end(bytes: &[u8], start_idx: usize) -> usize {
    let mut idx = start_idx + 1;
    while idx < bytes.len() && is_variable_name_byte(bytes[idx]) {
        idx += 1;
    }
    idx
}

fn push_original_char(output: &mut String, sql: &str, idx: usize) -> usize {
    let end = idx + utf8_char_width(sql.as_bytes()[idx]);
    output.push_str(&sql[idx..end]);
    end
}

fn utf8_char_width(first_byte: u8) -> usize {
    match first_byte {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF7 => 4,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn normalize_function_syntax_rewrites_legacy_map_literals() {
        let normalized = super::normalize_for_raw_parse(
            "INSERT INTO t VALUES (map{'k1': 1, 'k2': map{'nested': 2}}, [map{\"k3\": 3}])",
        )
        .expect("normalize should succeed");
        assert_eq!(
            normalized,
            "INSERT INTO t VALUES (map('k1', 1, 'k2', map('nested', 2)), [map(\"k3\", 3)])"
        );
    }

    #[test]
    fn normalize_for_raw_parse_splits_nested_generic_closers_in_create_table() {
        let normalized = super::normalize_for_raw_parse(
            "CREATE TABLE t (c1 ARRAY<ARRAY<INT>>, c2 ARRAY<STRUCT<f1 INT>>) DUPLICATE KEY(c1) DISTRIBUTED BY HASH(c1) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")",
        )
        .expect("normalize should succeed");
        assert!(normalized.contains("ARRAY<ARRAY<INT> >"));
        assert!(normalized.contains("ARRAY<STRUCT<f1 INT> >"));
    }

    #[test]
    fn normalize_for_raw_parse_preserves_utf8_text() {
        let normalized = super::normalize_for_raw_parse("SELECT '王武程咬金', '中国'")
            .expect("normalize should succeed");
        assert_eq!(normalized, "SELECT '王武程咬金', '中国'");
    }

    #[test]
    fn normalize_for_raw_parse_injects_group_concat_default_separator() {
        let normalized = super::normalize_for_raw_parse("SELECT group_concat(name ORDER BY 1)")
            .expect("normalize should succeed");
        assert_eq!(normalized, "SELECT group_concat(name, ',' ORDER BY 1)");
    }

    #[test]
    fn normalize_for_raw_parse_rewrites_cast_map_target_syntax() {
        let normalized =
            super::normalize_for_raw_parse("SELECT CAST(NULL AS MAP<INT, MAP<INT, INT>>)")
                .expect("normalize should succeed");
        assert_eq!(normalized, "SELECT CAST(NULL AS MAP(INT, MAP(INT, INT)))");
    }

    #[test]
    fn normalize_for_raw_parse_rewrites_set_user_variable_hint_references() {
        let normalized = super::normalize_for_raw_parse(
            "WITH tt AS (SELECT @v1 AS v1, c1 FROM t1) \
             SELECT /*+ set_user_variable(@v1 = 0.5) */ v1 FROM tt",
        )
        .expect("normalize should succeed");
        assert_eq!(
            normalized,
            "WITH tt AS (SELECT 0.5 AS v1, c1 FROM t1) \
             SELECT /*+ set_user_variable(@v1 = 0.5) */ v1 FROM tt"
        );
    }

    #[test]
    fn normalize_for_raw_parse_rewrites_multiple_set_user_variables() {
        let normalized = super::normalize_for_raw_parse(
            "SELECT /*+ set_user_variable(@v1 = 0.5, @v2 = 4096) */ @v1, @v2 + 1",
        )
        .expect("normalize should succeed");
        assert_eq!(
            normalized,
            "SELECT /*+ set_user_variable(@v1 = 0.5, @v2 = 4096) */ 0.5, 4096 + 1"
        );
    }

    #[test]
    fn normalize_for_raw_parse_rewrites_group_concat_explicit_separator() {
        let normalized =
            super::normalize_for_raw_parse("SELECT group_concat(name ORDER BY 1 SEPARATOR '|')")
                .expect("normalize should succeed");
        assert_eq!(normalized, "SELECT group_concat(name, '|' ORDER BY 1)");
    }

    #[test]
    fn normalize_for_raw_parse_strips_bare_from_dual() {
        let normalized =
            super::normalize_for_raw_parse("SELECT 1 FROM dual").expect("normalize should succeed");
        assert_eq!(normalized, "SELECT 1");
    }

    #[test]
    fn normalize_for_raw_parse_strips_from_dual_with_trailing_semicolon() {
        let normalized = super::normalize_for_raw_parse("SELECT now() FROM dual;")
            .expect("normalize should succeed");
        assert_eq!(normalized, "SELECT now();");
    }

    #[test]
    fn normalize_for_raw_parse_keeps_from_dual_with_where_clause() {
        let normalized = super::normalize_for_raw_parse("SELECT 1 FROM dual WHERE 1 = 1")
            .expect("normalize should succeed");
        assert!(normalized.contains("FROM dual"));
    }
}
