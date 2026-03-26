pub(crate) mod create_catalog;
pub(crate) mod create_table;
pub(crate) mod drop;

use sqlparser::ast as sqlast;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::sql::ast::{ObjectName, SqlType};

/// Custom StarRocks dialect for sqlparser.
#[derive(Debug)]
pub(crate) struct StarRocksDialect;

impl sqlparser::dialect::Dialect for StarRocksDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_' || ch == '$'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        false
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
        sqlast::DataType::Varbinary(_) => Ok(SqlType::String),
        sqlast::DataType::Binary(_) => Ok(SqlType::String),
        sqlast::DataType::Custom(name, modifiers) => {
            let n = name.to_string().to_lowercase();
            match n.as_str() {
                "string" => Ok(SqlType::String),
                "largeint" => Ok(SqlType::LargeInt),
                "json" | "jsonb" => Ok(SqlType::String),
                "varbinary" => Ok(SqlType::String),
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
    normalize_function_syntax(sql)
}

pub(crate) fn normalize_function_syntax(sql: &str) -> Result<String, String> {
    rewrite_group_concat_separator(sql)
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
            output.push(bytes[idx] as char);
            idx += 1;
        }
    }
    Ok(output)
}

fn rewrite_group_concat_inner(inner: &str) -> Result<String, String> {
    let Some(separator_pos) = find_top_level_keyword(inner, "separator") else {
        return Ok(inner.to_string());
    };
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
}

fn find_matching_paren(sql: &str, open_idx: usize) -> Result<usize, String> {
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
                b'(' => depth += 1,
                b')' => {
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

fn is_identifier_byte(byte: Option<u8>) -> bool {
    byte.is_some_and(|value| value == b'_' || value.is_ascii_alphanumeric())
}
