pub(crate) mod alter_iceberg_ref;
pub(crate) mod create_catalog;
pub(crate) mod create_table;
pub(crate) mod drop;
pub(crate) mod materialized_view;
pub(crate) mod truncate;

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

    fn supports_table_versioning(&self) -> bool {
        true
    }

    /// Accept `IGNORE NULLS` / `RESPECT NULLS` written *inside* the function
    /// argument list, e.g. `first_value(v IGNORE NULLS) OVER (...)`.
    /// The post-args form (`first_value(v) IGNORE NULLS OVER (...)`) is parsed
    /// unconditionally by sqlparser into `Function::null_treatment` and does
    /// not depend on this flag.
    fn supports_window_function_null_treatment_arg(&self) -> bool {
        true
    }

    /// StarRocks follows MySQL string-literal semantics, where backslash is an
    /// escape character: `'\\'` → a single backslash, `'\n'` → newline, etc.
    /// Without this, sqlparser preserves backslashes verbatim and INSERTs such
    /// as `'e\\f'` end up storing 4 bytes instead of 3.
    fn supports_string_literal_backslash_escape(&self) -> bool {
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
        sqlast::DataType::JSON | sqlast::DataType::JSONB => Ok(SqlType::Json),
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
                "json" | "jsonb" => Ok(SqlType::Json),
                "varbinary" | "binary" => Ok(SqlType::Binary),
                "variant" => Ok(SqlType::Variant),
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
pub(crate) fn parse_create_database_name(
    parser: &mut Parser<'_>,
) -> Result<(ObjectName, bool), String> {
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::DATABASE)
        .map_err(|e| e.to_string())?;
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;
    Ok((name, if_not_exists))
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
    let sql = rewrite_version_as_of_string(&sql)?;
    let sql = rewrite_iceberg_metadata_suffix(&sql)?;
    let sql = rewrite_overwrite_partitions(&sql)?;
    let sql = rewrite_inline_null_treatment(&sql);
    let sql = strip_join_hints(&sql);
    Ok(rewrite_create_table_nested_generic_closers(&sql))
}

/// Strip StarRocks-style join hints written as `JOIN [broadcast]`,
/// `JOIN[shuffle]`, `LEFT JOIN [skew|t1.c(1,2,3)]`, etc.
///
/// Standalone NovaRocks has no notion of broadcast/shuffle/bucket/colocate
/// dispatch and no skew hint optimizer, so these hints carry no semantics for
/// the engine. sqlparser does not recognize `[...]` after JOIN at all, so
/// leaving them in produces "Expected: identifier, found: [" or
/// "Expected: joined table, ..." parse errors. Stripping them lets the rest
/// of the JOIN clause parse normally.
///
/// Detection rule: any `JOIN` keyword at a word boundary, followed by
/// optional ASCII whitespace and an opening `[`. We scan to the matching `]`
/// (allowing nested brackets defensively) and drop the bracketed range,
/// emitting a single space in its place.
fn strip_join_hints(sql: &str) -> String {
    if !sql.contains('[') {
        return sql.to_string();
    }
    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len());
    let mut idx = 0usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut backtick = false;

    while idx < bytes.len() {
        let byte = bytes[idx];

        // Pass UTF-8 multi-byte sequences through verbatim.
        if byte >= 0x80 {
            let len = utf8_sequence_len(byte);
            let end = (idx + len).min(bytes.len());
            output.push_str(&sql[idx..end]);
            idx = end;
            continue;
        }

        if single_quote {
            if byte == b'\'' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                single_quote = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        if double_quote {
            if byte == b'"' && bytes.get(idx.wrapping_sub(1)).copied() != Some(b'\\') {
                double_quote = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        if backtick {
            if byte == b'`' {
                backtick = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => {
                single_quote = true;
                output.push('\'');
                idx += 1;
                continue;
            }
            b'"' => {
                double_quote = true;
                output.push('"');
                idx += 1;
                continue;
            }
            b'`' => {
                backtick = true;
                output.push('`');
                idx += 1;
                continue;
            }
            _ => {}
        }

        // Line comment: emit through end of line.
        if byte == b'-' && bytes.get(idx + 1) == Some(&b'-') {
            while idx < bytes.len() && bytes[idx] != b'\n' {
                if bytes[idx] >= 0x80 {
                    let len = utf8_sequence_len(bytes[idx]);
                    let end = (idx + len).min(bytes.len());
                    output.push_str(&sql[idx..end]);
                    idx = end;
                } else {
                    output.push(bytes[idx] as char);
                    idx += 1;
                }
            }
            continue;
        }
        // Block comment: emit through closing `*/`.
        if byte == b'/' && bytes.get(idx + 1) == Some(&b'*') {
            output.push_str("/*");
            idx += 2;
            while idx + 1 < bytes.len() && !(bytes[idx] == b'*' && bytes[idx + 1] == b'/') {
                if bytes[idx] >= 0x80 {
                    let len = utf8_sequence_len(bytes[idx]);
                    let end = (idx + len).min(bytes.len());
                    output.push_str(&sql[idx..end]);
                    idx = end;
                } else {
                    output.push(bytes[idx] as char);
                    idx += 1;
                }
            }
            if idx + 1 < bytes.len() {
                output.push_str("*/");
                idx += 2;
            }
            continue;
        }

        if starts_with_keyword(bytes, idx, "join")
            && !is_identifier_byte(bytes.get(idx.wrapping_sub(1)).copied())
            && !is_identifier_byte(bytes.get(idx + "join".len()).copied())
        {
            // Emit JOIN keyword verbatim, then look for an optional `[...]` hint.
            let join_end = idx + "join".len();
            output.push_str(&sql[idx..join_end]);
            let after_ws = skip_ascii_whitespace(bytes, join_end);
            if bytes.get(after_ws) == Some(&b'[') {
                output.push(' ');
                let mut p = after_ws + 1;
                let mut depth = 1usize;
                while p < bytes.len() {
                    match bytes[p] {
                        b'[' => depth += 1,
                        b']' => {
                            depth -= 1;
                            if depth == 0 {
                                p += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                    p += 1;
                }
                idx = p;
                continue;
            }
            // No hint — preserve any whitespace we already saw.
            output.push_str(&sql[join_end..after_ws]);
            idx = after_ws;
            continue;
        }

        output.push(byte as char);
        idx += 1;
    }
    output
}

/// Move `IGNORE NULLS` / `RESPECT NULLS` from before a comma to the end of
/// its containing function call.
///
/// StarRocks accepts `LEAD(v IGNORE NULLS, 3) OVER (...)` — the modifier sits
/// between the value argument and the offset, before the comma. sqlparser only
/// recognizes the modifier *after* the last argument, so we rewrite
/// `LEAD(v IGNORE NULLS, 3)` to `LEAD(v , 3 IGNORE NULLS)` so the standard
/// post-args parsing path picks it up. Display-name generation is unaffected
/// because the modifier still ends up on `Function::null_treatment` /
/// `FunctionArgumentClause::IgnoreOrRespectNulls` after parsing.
fn rewrite_inline_null_treatment(sql: &str) -> String {
    // Fast path: if neither `ignore` nor `respect` appears (case-insensitive),
    // there is nothing for us to rewrite. Avoids touching UTF-8 in the common
    // case and keeps non-window queries cheap.
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("ignore") && !lower.contains("respect") {
        return sql.to_string();
    }

    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len() + 16);
    // Per-depth slot: when set, inject the captured modifier just before the
    // matching `)`. The first inline modifier encountered in a paren scope
    // wins; later ones at the same depth are left in place (typically already
    // at the end-of-args position, where sqlparser handles them natively).
    let mut pending_at_depth: Vec<Option<&'static str>> = Vec::new();

    let mut idx = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;

    while idx < bytes.len() {
        let byte = bytes[idx];

        // Multi-byte UTF-8: copy the whole code point verbatim. This keeps
        // string and identifier literals (Chinese names, etc.) intact.
        if byte >= 0x80 {
            let len = utf8_sequence_len(byte);
            let end = (idx + len).min(bytes.len());
            output.push_str(&sql[idx..end]);
            idx = end;
            continue;
        }

        if in_single_quote {
            if byte == b'\\' && idx + 1 < bytes.len() {
                output.push(byte as char);
                output.push(bytes[idx + 1] as char);
                idx += 2;
                continue;
            }
            if byte == b'\'' {
                in_single_quote = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        if in_double_quote {
            if byte == b'"' {
                in_double_quote = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        if in_backtick {
            if byte == b'`' {
                in_backtick = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => {
                in_single_quote = true;
                output.push('\'');
                idx += 1;
                continue;
            }
            b'"' => {
                in_double_quote = true;
                output.push('"');
                idx += 1;
                continue;
            }
            b'`' => {
                in_backtick = true;
                output.push('`');
                idx += 1;
                continue;
            }
            _ => {}
        }
        // Line comment.
        if byte == b'-' && idx + 1 < bytes.len() && bytes[idx + 1] == b'-' {
            while idx < bytes.len() && bytes[idx] != b'\n' {
                if bytes[idx] >= 0x80 {
                    let len = utf8_sequence_len(bytes[idx]);
                    let end = (idx + len).min(bytes.len());
                    output.push_str(&sql[idx..end]);
                    idx = end;
                } else {
                    output.push(bytes[idx] as char);
                    idx += 1;
                }
            }
            continue;
        }
        // Block comment.
        if byte == b'/' && idx + 1 < bytes.len() && bytes[idx + 1] == b'*' {
            output.push_str("/*");
            idx += 2;
            while idx + 1 < bytes.len() && !(bytes[idx] == b'*' && bytes[idx + 1] == b'/') {
                if bytes[idx] >= 0x80 {
                    let len = utf8_sequence_len(bytes[idx]);
                    let end = (idx + len).min(bytes.len());
                    output.push_str(&sql[idx..end]);
                    idx = end;
                } else {
                    output.push(bytes[idx] as char);
                    idx += 1;
                }
            }
            if idx + 1 < bytes.len() {
                output.push_str("*/");
                idx += 2;
            }
            continue;
        }

        if byte == b'(' {
            pending_at_depth.push(None);
            output.push('(');
            idx += 1;
            continue;
        }
        if byte == b')' {
            if let Some(slot) = pending_at_depth.pop()
                && let Some(treatment) = slot
            {
                output.push(' ');
                output.push_str(treatment);
            }
            output.push(')');
            idx += 1;
            continue;
        }

        if !pending_at_depth.is_empty() {
            // Only attempt the keyword match at a word boundary so we don't
            // collide with identifiers like `IGNORED_NULLS`.
            let prev = idx.checked_sub(1).map(|p| bytes[p]);
            if !is_identifier_byte(prev) {
                if let Some((treatment, consumed)) = match_inline_null_treatment(bytes, idx) {
                    if let Some(slot) = pending_at_depth.last_mut()
                        && slot.is_none()
                    {
                        *slot = Some(treatment);
                    }
                    idx += consumed;
                    continue;
                }
            }
        }

        output.push(byte as char);
        idx += 1;
    }

    output
}

fn utf8_sequence_len(byte: u8) -> usize {
    if byte & 0x80 == 0 {
        1
    } else if byte & 0xE0 == 0xC0 {
        2
    } else if byte & 0xF0 == 0xE0 {
        3
    } else if byte & 0xF8 == 0xF0 {
        4
    } else {
        1
    }
}

/// Match `IGNORE NULLS` or `RESPECT NULLS` (case-insensitive, with arbitrary
/// inter-word whitespace) followed by optional whitespace and a `,`. Returns
/// the canonical replacement text and the number of bytes that should be
/// skipped (just the keyword tokens, excluding any trailing whitespace and the
/// comma — those stay in the rewritten SQL).
fn match_inline_null_treatment(bytes: &[u8], start: usize) -> Option<(&'static str, usize)> {
    let candidates: [(&[&str], &str); 2] = [
        (&["IGNORE", "NULLS"], "IGNORE NULLS"),
        (&["RESPECT", "NULLS"], "RESPECT NULLS"),
    ];
    for (words, replacement) in candidates {
        let Some(consumed) = match_keyword_sequence_ci(bytes, start, words) else {
            continue;
        };
        let mut p = start + consumed;
        while p < bytes.len() && (bytes[p] as char).is_ascii_whitespace() {
            p += 1;
        }
        if p < bytes.len() && bytes[p] == b',' {
            return Some((replacement, consumed));
        }
    }
    None
}

/// Match a sequence of keywords case-insensitively, with one or more
/// whitespace bytes between consecutive words and a word boundary at the end.
/// Returns the number of bytes consumed if successful.
fn match_keyword_sequence_ci(bytes: &[u8], start: usize, words: &[&str]) -> Option<usize> {
    let mut p = start;
    for (i, word) in words.iter().enumerate() {
        if i > 0 {
            if p >= bytes.len() || !(bytes[p] as char).is_ascii_whitespace() {
                return None;
            }
            while p < bytes.len() && (bytes[p] as char).is_ascii_whitespace() {
                p += 1;
            }
        }
        let kw = word.as_bytes();
        if p + kw.len() > bytes.len() {
            return None;
        }
        for j in 0..kw.len() {
            if !bytes[p + j].eq_ignore_ascii_case(&kw[j]) {
                return None;
            }
        }
        p += kw.len();
    }
    if p < bytes.len() && is_identifier_byte(Some(bytes[p])) {
        return None;
    }
    Some(p - start)
}

/// Rewrite `<ident>$<metatype>` (in unquoted/non-string context) to
/// `<ident>.__nr_meta_<metatype>__`, lowercasing `<metatype>`.
///
/// Iceberg's `t$snapshots` syntax cannot be lexed by sqlparser without dialect
/// hacks. The analyzer detects the `__nr_meta_*__` last-part suffix and
/// dispatches to `IcebergMetadataScanOp`.
///
/// Restricted to the four BE-supported types: snapshots, history, refs,
/// partitions. An unrecognised type errors at normalize time.
fn rewrite_iceberg_metadata_suffix(sql: &str) -> Result<String, String> {
    if !sql.contains('$') {
        return Ok(sql.to_string());
    }

    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len() + 16);
    let mut idx = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;

    while idx < bytes.len() {
        let byte = bytes[idx];
        if in_single_quote {
            if byte == b'\'' {
                in_single_quote = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        if in_double_quote {
            if byte == b'"' {
                in_double_quote = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        if in_backtick {
            if byte == b'`' {
                in_backtick = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => {
                in_single_quote = true;
                output.push('\'');
                idx += 1;
                continue;
            }
            b'"' => {
                in_double_quote = true;
                output.push('"');
                idx += 1;
                continue;
            }
            b'`' => {
                in_backtick = true;
                output.push('`');
                idx += 1;
                continue;
            }
            _ => {}
        }

        if byte == b'$' && idx > 0 && is_identifier_byte(Some(bytes[idx - 1])) {
            // Read the identifier word that follows `$`.
            let mut end = idx + 1;
            while end < bytes.len() && is_identifier_byte(Some(bytes[end])) {
                end += 1;
            }
            if end == idx + 1 {
                // Lone `$` not followed by an identifier — pass through.
                output.push('$');
                idx += 1;
                continue;
            }
            let metatype_raw = &sql[idx + 1..end];
            let metatype = metatype_raw.to_ascii_lowercase();
            // Whitelist the four scope types.
            match metatype.as_str() {
                "snapshots" | "history" | "refs" | "partitions" => {}
                other => {
                    return Err(format!(
                        "unsupported iceberg metadata table type: {other}; \
                         expected one of snapshots/history/refs/partitions"
                    ));
                }
            }
            output.push('.');
            output.push_str("__nr_meta_");
            output.push_str(&metatype);
            output.push_str("__");
            idx = end;
            continue;
        }

        output.push(byte as char);
        idx += 1;
    }
    Ok(output)
}

/// Rewrite `FOR VERSION AS OF '<ref_name>'` → `FOR SYSTEM_TIME AS OF '__nr_ref:<ref_name>'`
///
/// sqlparser 0.61 parses `VERSION AS OF` via `parse_number_value()`, which rejects
/// single-quoted strings.  By normalizing string-valued VERSION clauses to a special
/// `__nr_ref:` prefix on the SYSTEM_TIME path (which uses `parse_expr()`) we round-trip
/// the ref-name through the AST and let `resolve_read_binding` detect the magic prefix
/// and dispatch to branch/tag resolution instead of timestamp resolution.
///
/// Numeric `VERSION AS OF <integer>` is left untouched (already handled by sqlparser).
fn rewrite_version_as_of_string(sql: &str) -> Result<String, String> {
    // Fast path: no VERSION keyword at all.
    let sql_lower = sql.to_ascii_lowercase();
    if !sql_lower.contains("version") {
        return Ok(sql.to_string());
    }

    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len() + 32);
    let mut idx = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;

    while idx < bytes.len() {
        let byte = bytes[idx];

        // Track quoted contexts so we don't rewrite inside string literals.
        if in_single_quote {
            if byte == b'\'' {
                in_single_quote = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        if in_double_quote {
            if byte == b'"' {
                in_double_quote = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        if in_backtick {
            if byte == b'`' {
                in_backtick = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }

        match byte {
            b'\'' => {
                in_single_quote = true;
                output.push('\'');
                idx += 1;
                continue;
            }
            b'"' => {
                in_double_quote = true;
                output.push('"');
                idx += 1;
                continue;
            }
            b'`' => {
                in_backtick = true;
                output.push('`');
                idx += 1;
                continue;
            }
            _ => {}
        }

        // Check for `VERSION` keyword (case-insensitive, word boundary).
        if starts_with_keyword(bytes, idx, "version")
            && !is_identifier_byte(bytes.get(idx.wrapping_sub(1)).copied())
        {
            let after_version = idx + "version".len();
            if is_identifier_byte(bytes.get(after_version).copied()) {
                // Not a standalone keyword — push and continue.
                output.push(byte as char);
                idx += 1;
                continue;
            }

            // Skip whitespace after VERSION.
            let as_start = skip_ascii_whitespace(bytes, after_version);

            // Check for `AS` keyword.
            if !starts_with_keyword(bytes, as_start, "as")
                || is_identifier_byte(bytes.get(as_start + 2).copied())
            {
                output.push(byte as char);
                idx += 1;
                continue;
            }
            let after_as = skip_ascii_whitespace(bytes, as_start + 2);

            // Check for `OF` keyword.
            if !starts_with_keyword(bytes, after_as, "of")
                || is_identifier_byte(bytes.get(after_as + 2).copied())
            {
                output.push(byte as char);
                idx += 1;
                continue;
            }
            let after_of = skip_ascii_whitespace(bytes, after_as + 2);

            // Check for single-quoted string literal.
            if bytes.get(after_of) != Some(&b'\'') {
                // Numeric VERSION AS OF — leave untouched.
                output.push(byte as char);
                idx += 1;
                continue;
            }

            // Find the end of the single-quoted string.
            let quote_start = after_of + 1; // First char inside the quotes.
            let mut qi = quote_start;
            while qi < bytes.len() && bytes[qi] != b'\'' {
                qi += 1;
            }
            if qi >= bytes.len() {
                return Err("unterminated string literal in FOR VERSION AS OF".to_string());
            }
            // bytes[quote_start..qi] is the unquoted ref name.
            let ref_name = &sql[quote_start..qi];

            // Emit: `SYSTEM_TIME AS OF '__nr_ref:<ref_name>'`
            output.push_str("SYSTEM_TIME AS OF '__nr_ref:");
            output.push_str(ref_name);
            output.push('\'');
            idx = qi + 1; // Move past the closing quote.
            continue;
        }

        output.push(byte as char);
        idx += 1;
    }

    Ok(output)
}

/// Rewrite `INSERT OVERWRITE PARTITIONS [TABLE] <name>` so that sqlparser-rs
/// can accept it.
///
/// sqlparser-rs does not recognise the `PARTITIONS` keyword in this position.
/// This rewriter drops the `PARTITIONS` token and prepends the reserved marker
/// identifier `__nr_op_dyn` as a leading name segment on the table object.
/// Downstream code (`convert_sqlparser_insert_to_custom`) detects the marker
/// and sets `OverwriteMode::DynamicPartitions`.
///
/// # Marker convention
///
/// `__nr_op_dyn` (NovaRocks reserved identifier — never a real table name).
/// It carries no meaning to sqlparser; it is purely an out-of-band signal from
/// the normaliser to the AST converter.
///
/// # Examples
///
/// ```text
/// INSERT OVERWRITE PARTITIONS t VALUES (1)
///     → INSERT OVERWRITE __nr_op_dyn.t VALUES (1)
///
/// INSERT OVERWRITE PARTITIONS TABLE x.y.z SELECT ...
///     → INSERT OVERWRITE TABLE __nr_op_dyn.x.y.z SELECT ...
///
/// INSERT OVERWRITE PARTITIONS t.branch_dev VALUES (1)
///     → INSERT OVERWRITE __nr_op_dyn.t.branch_dev VALUES (1)
/// ```
fn rewrite_overwrite_partitions(sql: &str) -> Result<String, String> {
    // Fast path: no PARTITIONS keyword at all.
    let sql_lower = sql.to_ascii_lowercase();
    if !sql_lower.contains("partitions") {
        return Ok(sql.to_string());
    }

    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len() + 16);
    let mut idx = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;

    while idx < bytes.len() {
        let byte = bytes[idx];

        // Track quoted contexts so we never rewrite inside string literals.
        if in_single_quote {
            if byte == b'\'' {
                in_single_quote = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        if in_double_quote {
            if byte == b'"' {
                in_double_quote = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }
        if in_backtick {
            if byte == b'`' {
                in_backtick = false;
            }
            output.push(byte as char);
            idx += 1;
            continue;
        }

        match byte {
            b'\'' => {
                in_single_quote = true;
                output.push('\'');
                idx += 1;
                continue;
            }
            b'"' => {
                in_double_quote = true;
                output.push('"');
                idx += 1;
                continue;
            }
            b'`' => {
                in_backtick = true;
                output.push('`');
                idx += 1;
                continue;
            }
            _ => {}
        }

        // Look for `INSERT` keyword (word boundary on left).
        if starts_with_keyword(bytes, idx, "insert")
            && !is_identifier_byte(bytes.get(idx.wrapping_sub(1)).copied())
            && !is_identifier_byte(bytes.get(idx + "insert".len()).copied())
        {
            let after_insert = skip_ascii_whitespace(bytes, idx + "insert".len());

            // Must be followed by `OVERWRITE`.
            if !starts_with_keyword(bytes, after_insert, "overwrite")
                || is_identifier_byte(bytes.get(after_insert + "overwrite".len()).copied())
            {
                output.push(byte as char);
                idx += 1;
                continue;
            }
            let after_overwrite = skip_ascii_whitespace(bytes, after_insert + "overwrite".len());

            // Must be followed by `PARTITIONS`.
            if !starts_with_keyword(bytes, after_overwrite, "partitions")
                || is_identifier_byte(bytes.get(after_overwrite + "partitions".len()).copied())
            {
                // Not our pattern — emit unchanged.
                output.push(byte as char);
                idx += 1;
                continue;
            }
            let after_partitions =
                skip_ascii_whitespace(bytes, after_overwrite + "partitions".len());

            // Optional `TABLE` keyword.
            let (emit_table_kw, name_start) =
                if starts_with_keyword(bytes, after_partitions, "table")
                    && !is_identifier_byte(bytes.get(after_partitions + "table".len()).copied())
                {
                    (
                        true,
                        skip_ascii_whitespace(bytes, after_partitions + "table".len()),
                    )
                } else {
                    (false, after_partitions)
                };

            // Emit: `INSERT OVERWRITE` (preserving original keyword casing),
            // then optionally ` TABLE`, then ` __nr_op_dyn.<rest>`.
            //
            // `after_insert` points to the first byte of OVERWRITE; emit the
            // original INSERT + whitespace segment, then OVERWRITE itself.
            // Avoid including trailing whitespace in the slice so we can emit
            // exactly one space separator regardless of the original spacing.
            let overwrite_end = after_insert + "overwrite".len();
            output.push_str(&sql[idx..overwrite_end]);

            if emit_table_kw {
                output.push_str(" TABLE ");
            } else {
                output.push(' ');
            }

            output.push_str("__nr_op_dyn.");
            // name_start points at the first byte of the table name — emit the
            // rest of the SQL from there.
            idx = name_start;
            continue;
        }

        output.push(byte as char);
        idx += 1;
    }

    Ok(output)
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
    rewrite_collection_type_generics(target)
}

/// Rewrite CAST target syntax like `map<K, V>` / `array<T>` / `struct<n T, …>`
/// into a form sqlparser's StarRocks dialect can tokenise via its generic
/// `parse_optional_type_modifiers` path: `KEYWORD('item1', 'item2', …)`.
///
/// The angle-bracket forms collapse to `Custom(name, modifiers)` only if each
/// modifier is a single token — but inner types like `array<int>` and struct
/// field specs like `col1 int` are multi-token, so we wrap each top-level
/// comma-separated item in a single-quoted string. Downstream the analyzer's
/// `custom_*_type_to_arrow` re-parses the modifier strings (`parse_custom_type_string`
/// already understands `array<…>`, `map<…>`, `struct<…>`, `decimal(p,s)`, …),
/// so nesting works transparently.
fn rewrite_collection_type_generics(target: &str) -> Result<String, String> {
    let mut output = String::with_capacity(target.len());
    let bytes = target.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        if let Some((keyword, name_len)) = collection_keyword_at(bytes, idx)
            && !is_identifier_byte(bytes.get(idx.wrapping_sub(1)).copied())
        {
            let mut cursor = idx + name_len;
            while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            if cursor < bytes.len() && bytes[cursor] == b'<' {
                let end_idx = find_matching_delimiter(target, cursor, b'<', b'>')?;
                let raw_inner = &target[cursor + 1..end_idx];
                let items = split_top_level_by_comma(raw_inner);
                output.push_str(keyword);
                output.push('(');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        output.push_str(", ");
                    }
                    // Keep nested generics in their original `<…>` form inside
                    // the quoted modifier — `parse_custom_type_string` in
                    // `helpers.rs` understands both `<…>` and `(…)` and will
                    // recursively walk them when we eventually re-parse this
                    // modifier string at type-resolution time.
                    let trimmed = item.trim();
                    output.push('\'');
                    for ch in trimmed.chars() {
                        if ch == '\'' || ch == '\\' {
                            output.push('\\');
                        }
                        output.push(ch);
                    }
                    output.push('\'');
                }
                output.push(')');
                idx = end_idx + 1;
                continue;
            }
        }
        idx = push_original_char(&mut output, target, idx);
    }
    Ok(output)
}

fn collection_keyword_at(bytes: &[u8], idx: usize) -> Option<(&'static str, usize)> {
    // `array<…>` is left intact: sqlparser's `Keyword::ARRAY` path already
    // accepts the angle-bracket form and recursively parses the element type
    // (so nested `map<…>` / `struct<…>` inside an array still get rewritten
    // because they match here on the recursive pass). MAP / STRUCT need the
    // rewrite because sqlparser's StarRocks dialect has no native parser for
    // those generics.
    for (kw, upper) in [("map", "MAP"), ("struct", "STRUCT")] {
        if starts_with_keyword(bytes, idx, kw) {
            return Some((upper, kw.len()));
        }
    }
    None
}

fn split_top_level_by_comma(input: &str) -> Vec<&str> {
    let bytes = input.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth_angle = 0i32;
    let mut depth_paren = 0i32;
    let mut idx = 0usize;
    while idx < bytes.len() {
        match bytes[idx] {
            b'<' => depth_angle += 1,
            b'>' => depth_angle -= 1,
            b'(' => depth_paren += 1,
            b')' => depth_paren -= 1,
            b',' if depth_angle == 0 && depth_paren == 0 => {
                out.push(&input[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
        idx += 1;
    }
    out.push(&input[start..]);
    out
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

pub(crate) fn substitute_user_variables(
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
        if separator_expr.is_empty() {
            return Err("invalid GROUP_CONCAT separator syntax".to_string());
        }
        if before_separator.is_empty() {
            return Err(format!(
                "No viable statement for input 'group_concat(separator {separator_expr}'."
            ));
        }
        if let Some(order_by_pos) = find_top_level_order_by(before_separator) {
            let args = before_separator[..order_by_pos].trim_end();
            let order_by = before_separator[order_by_pos..].trim_start();
            if args.is_empty() || args.eq_ignore_ascii_case("distinct") {
                Ok(inner.to_string())
            } else {
                Ok(format!("{args}, {separator_expr} {order_by}"))
            }
        } else {
            Ok(format!("{before_separator}, {separator_expr}"))
        }
    } else if let Some(order_by_pos) = find_top_level_order_by(inner) {
        let args = inner[..order_by_pos].trim_end();
        let order_by = inner[order_by_pos..].trim_start();
        if args.is_empty() || args.eq_ignore_ascii_case("distinct") {
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
        // Outer / inner generics become single-quoted modifiers so sqlparser's
        // StarRocks dialect can tokenise them; the analyzer's
        // `parse_custom_type_string` re-reads the inner `<…>` form at type
        // resolution time.
        let normalized =
            super::normalize_for_raw_parse("SELECT CAST(NULL AS MAP<INT, MAP<INT, INT>>)")
                .expect("normalize should succeed");
        assert_eq!(
            normalized,
            "SELECT CAST(NULL AS MAP('INT', 'MAP<INT, INT>'))"
        );
    }

    #[test]
    fn normalize_for_raw_parse_rewrites_cast_struct_target_syntax() {
        let normalized = super::normalize_for_raw_parse(
            "SELECT CAST(NULL AS STRUCT<col1 INT, col2 ARRAY<INT>>)",
        )
        .expect("normalize should succeed");
        assert_eq!(
            normalized,
            "SELECT CAST(NULL AS STRUCT('col1 INT', 'col2 ARRAY<INT>'))"
        );
        // ARRAY<…> is left as-is because sqlparser's StarRocks dialect can
        // already tokenise it natively.
        let normalized2 = super::normalize_for_raw_parse("SELECT CAST(NULL AS ARRAY<JSON>)")
            .expect("normalize should succeed");
        assert_eq!(normalized2, "SELECT CAST(NULL AS ARRAY<JSON>)");
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

    #[test]
    fn metadata_suffix_dollar_is_rewritten_for_known_types() {
        let cases = [
            (
                "SELECT * FROM t$snapshots",
                "SELECT * FROM t.__nr_meta_snapshots__",
            ),
            (
                "SELECT * FROM db.t$history",
                "SELECT * FROM db.t.__nr_meta_history__",
            ),
            (
                "SELECT * FROM ice.db.t$refs",
                "SELECT * FROM ice.db.t.__nr_meta_refs__",
            ),
            (
                "select * from t$partitions",
                "select * from t.__nr_meta_partitions__",
            ),
            // Mixed case input still routes — the rewritten metatype is lowercase.
            (
                "SELECT * FROM t$Snapshots",
                "SELECT * FROM t.__nr_meta_snapshots__",
            ),
        ];
        for (input, expected) in cases {
            let got = super::normalize_for_raw_parse(input).expect("normalize");
            assert_eq!(got, expected, "input: {input}");
        }
    }

    #[test]
    fn metadata_suffix_unknown_type_errors() {
        let err = super::normalize_for_raw_parse("SELECT * FROM t$foo").unwrap_err();
        assert!(
            err.contains("unsupported iceberg metadata table type") && err.contains("foo"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn metadata_suffix_inside_string_literal_is_left_alone() {
        let input = "SELECT 'a$snapshots' FROM t";
        let got = super::normalize_for_raw_parse(input).expect("normalize");
        assert_eq!(got, input);
    }

    #[test]
    fn metadata_suffix_with_alias() {
        let input = "SELECT * FROM t$snapshots AS s";
        let got = super::normalize_for_raw_parse(input).expect("normalize");
        assert_eq!(got, "SELECT * FROM t.__nr_meta_snapshots__ AS s");
    }

    // ----- rewrite_overwrite_partitions tests -----

    #[test]
    fn rewrite_overwrite_partitions_injects_marker_no_table_keyword() {
        let normalized =
            super::rewrite_overwrite_partitions("INSERT OVERWRITE PARTITIONS t VALUES (1)")
                .expect("rewrite should succeed");
        assert_eq!(normalized, "INSERT OVERWRITE __nr_op_dyn.t VALUES (1)");
    }

    #[test]
    fn rewrite_overwrite_partitions_injects_marker_with_table_keyword() {
        let normalized = super::rewrite_overwrite_partitions(
            "INSERT OVERWRITE PARTITIONS TABLE t SELECT * FROM s",
        )
        .expect("rewrite should succeed");
        assert_eq!(
            normalized,
            "INSERT OVERWRITE TABLE __nr_op_dyn.t SELECT * FROM s"
        );
    }

    #[test]
    fn rewrite_overwrite_partitions_injects_marker_multi_part_name() {
        let normalized =
            super::rewrite_overwrite_partitions("INSERT OVERWRITE PARTITIONS x.y.z SELECT 1")
                .expect("rewrite should succeed");
        assert_eq!(normalized, "INSERT OVERWRITE __nr_op_dyn.x.y.z SELECT 1");
    }

    #[test]
    fn rewrite_overwrite_partitions_injects_marker_with_branch_suffix() {
        let normalized = super::rewrite_overwrite_partitions(
            "INSERT OVERWRITE PARTITIONS t.branch_dev VALUES (1)",
        )
        .expect("rewrite should succeed");
        assert_eq!(
            normalized,
            "INSERT OVERWRITE __nr_op_dyn.t.branch_dev VALUES (1)"
        );
    }

    #[test]
    fn rewrite_overwrite_partitions_case_insensitive() {
        let normalized =
            super::rewrite_overwrite_partitions("insert overwrite partitions t values (1)")
                .expect("rewrite should succeed");
        assert_eq!(normalized, "insert overwrite __nr_op_dyn.t values (1)");
    }

    #[test]
    fn rewrite_overwrite_partitions_passes_through_plain_overwrite() {
        let sql = "INSERT OVERWRITE TABLE t VALUES (1)";
        assert_eq!(super::rewrite_overwrite_partitions(sql).unwrap(), sql);
    }

    #[test]
    fn rewrite_overwrite_partitions_passes_through_unrelated_sql() {
        let sql = "SELECT 'INSERT OVERWRITE PARTITIONS' AS s";
        assert_eq!(super::rewrite_overwrite_partitions(sql).unwrap(), sql);
    }

    #[test]
    fn rewrite_overwrite_partitions_does_not_rewrite_in_double_quoted_literal() {
        let sql = r#"SELECT "INSERT OVERWRITE PARTITIONS t" AS s"#;
        assert_eq!(super::rewrite_overwrite_partitions(sql).unwrap(), sql);
    }
}
