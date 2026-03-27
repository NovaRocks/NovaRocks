use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::{convert_object_name, convert_sql_type, peek_word_eq};
use crate::sql::ast::{
    CreateTableKind, CreateTableStmt, TableColumnDef, TableKeyDesc, TableKeyKind,
};

/// Parse StarRocks CREATE TABLE statement:
/// CREATE TABLE [IF NOT EXISTS] <name> (
///   col1 type [NOT NULL] [DEFAULT ...] [COMMENT '...'],
///   ...
/// )
/// [ENGINE = OLAP|...]
/// [key_desc]
/// [COMMENT '...']
/// [PARTITION BY ...]
/// [DISTRIBUTED BY HASH(...) [BUCKETS n]]
/// [PROPERTIES (...)]
pub(crate) fn parse_create_table_statement(
    parser: &mut Parser<'_>,
) -> Result<CreateTableStmt, String> {
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(|e| e.to_string())?;
    // Skip EXTERNAL / TEMPORARY
    let _ = parser.parse_keyword(Keyword::EXTERNAL);
    let _ = parser.parse_keyword(Keyword::TEMPORARY);
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| e.to_string())?;

    // IF NOT EXISTS
    let _if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

    let name = convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;

    // Parse column definitions
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| e.to_string())?;
    let columns = parse_column_definitions(parser)?;

    // Parse trailing clauses: ENGINE, KEY type, COMMENT, PARTITION, DISTRIBUTED, ORDER BY, PROPERTIES
    let mut _engine = None;
    let mut key_desc = None;
    let mut properties = Vec::new();

    // Consume all remaining clauses until EOF or semicolon
    loop {
        if parser.peek_token_ref().token == Token::EOF
            || parser.peek_token_ref().token == Token::SemiColon
        {
            break;
        }
        if peek_word_eq(parser, 0, "ENGINE") {
            parser.next_token(); // ENGINE
            let _ = parser.consume_token(&Token::Eq);
            let eng_name = parser
                .parse_identifier()
                .map_err(|e| e.to_string())?
                .value
                .to_lowercase();
            _engine = Some(eng_name);
        } else if peek_word_eq(parser, 0, "DUPLICATE") {
            key_desc = Some(parse_key_desc(parser, TableKeyKind::Duplicate)?);
        } else if peek_word_eq(parser, 0, "AGGREGATE") {
            key_desc = Some(parse_key_desc(parser, TableKeyKind::Aggregate)?);
        } else if peek_word_eq(parser, 0, "UNIQUE") {
            key_desc = Some(parse_key_desc(parser, TableKeyKind::Unique)?);
        } else if peek_word_eq(parser, 0, "PRIMARY") {
            key_desc = Some(parse_key_desc(parser, TableKeyKind::Primary)?);
        } else if peek_word_eq(parser, 0, "COMMENT") {
            parser.next_token(); // COMMENT
            parser.next_token(); // string
        } else if peek_word_eq(parser, 0, "PARTITION") {
            skip_until_keyword_or_eof(parser, &["DISTRIBUTED", "ORDER", "PROPERTIES"]);
        } else if peek_word_eq(parser, 0, "DISTRIBUTED") {
            skip_until_keyword_or_eof(parser, &["ORDER", "PROPERTIES"]);
        } else if parser.parse_keyword(Keyword::ORDER) {
            // ORDER BY (...)
            let _ = parser.parse_keyword(Keyword::BY);
            skip_parenthesized(parser);
        } else if peek_word_eq(parser, 0, "PROPERTIES") {
            parser.next_token(); // PROPERTIES
            properties = parse_kv_properties_vec(parser)?;
        } else {
            // Skip unknown token
            parser.next_token();
        }
    }

    let kind = CreateTableKind::Iceberg {
        columns,
        key_desc,
        properties,
    };

    Ok(CreateTableStmt { name, kind })
}

fn parse_column_definitions(parser: &mut Parser<'_>) -> Result<Vec<TableColumnDef>, String> {
    let mut columns = Vec::new();
    loop {
        if parser.consume_token(&Token::RParen) {
            break;
        }
        if !columns.is_empty() {
            let _ = parser.consume_token(&Token::Comma);
            if parser.consume_token(&Token::RParen) {
                break;
            }
        }
        let col_name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        let data_type = parser.parse_data_type().map_err(|e| e.to_string())?;
        let sql_type = convert_sql_type(data_type)?;

        let mut _nullable = true;
        let mut _comment = None;

        // Parse optional NOT NULL, NULL, DEFAULT, COMMENT, AUTO_INCREMENT, etc.
        loop {
            if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
                _nullable = false;
            } else if parser.parse_keyword(Keyword::NULL) {
                _nullable = true;
            } else if parser.parse_keyword(Keyword::DEFAULT) {
                // Skip the default value expression
                skip_default_value(parser);
            } else if peek_word_eq(parser, 0, "COMMENT") {
                parser.next_token();
                let tok = parser.next_token();
                if let Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) = tok.token {
                    _comment = Some(s);
                }
            } else if peek_word_eq(parser, 0, "AUTO_INCREMENT") {
                parser.next_token();
            } else if peek_word_eq(parser, 0, "AS") {
                // Generated column
                parser.next_token();
                skip_default_value(parser);
            } else {
                break;
            }
        }

        columns.push(TableColumnDef {
            name: col_name,
            data_type: sql_type,
            aggregation: None,
        });
    }
    Ok(columns)
}

fn parse_key_desc(parser: &mut Parser<'_>, kind: TableKeyKind) -> Result<TableKeyDesc, String> {
    parser.next_token(); // DUPLICATE/AGGREGATE/UNIQUE/PRIMARY
    parser
        .expect_keyword(Keyword::KEY)
        .map_err(|e| e.to_string())?;
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| e.to_string())?;
    let mut key_columns = Vec::new();
    loop {
        if parser.consume_token(&Token::RParen) {
            break;
        }
        if !key_columns.is_empty() {
            let _ = parser.consume_token(&Token::Comma);
            if parser.consume_token(&Token::RParen) {
                break;
            }
        }
        let col = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        key_columns.push(col);
    }
    Ok(TableKeyDesc {
        kind,
        columns: key_columns,
    })
}

fn parse_kv_properties_vec(parser: &mut Parser<'_>) -> Result<Vec<(String, String)>, String> {
    let mut props = Vec::new();
    if !parser.consume_token(&Token::LParen) {
        return Ok(props);
    }
    loop {
        if parser.consume_token(&Token::RParen) {
            break;
        }
        if !props.is_empty() {
            let _ = parser.consume_token(&Token::Comma);
            if parser.consume_token(&Token::RParen) {
                break;
            }
        }
        let key = parse_string_or_ident(parser)?;
        let _ = parser.consume_token(&Token::Eq);
        let value = parse_string_or_ident(parser)?;
        props.push((key, value));
    }
    Ok(props)
}

fn parse_string_or_ident(parser: &mut Parser<'_>) -> Result<String, String> {
    let token = parser.next_token();
    match token.token {
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => Ok(s),
        Token::Word(w) => Ok(w.value),
        Token::Number(n, _) => Ok(n),
        other => Err(format!("expected string or identifier, got {other}")),
    }
}

fn skip_until_keyword_or_eof(parser: &mut Parser<'_>, stop_words: &[&str]) {
    loop {
        if parser.peek_token_ref().token == Token::EOF
            || parser.peek_token_ref().token == Token::SemiColon
        {
            break;
        }
        let should_stop = stop_words.iter().any(|w| peek_word_eq(parser, 0, w));
        if should_stop {
            break;
        }
        // Handle parenthesized groups
        if parser.peek_token_ref().token == Token::LParen {
            skip_parenthesized(parser);
        } else {
            parser.next_token();
        }
    }
}

fn skip_parenthesized(parser: &mut Parser<'_>) {
    if !parser.consume_token(&Token::LParen) {
        return;
    }
    let mut depth = 1;
    loop {
        let tok = parser.next_token();
        match tok.token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            Token::EOF => break,
            _ => {}
        }
    }
}

fn skip_default_value(parser: &mut Parser<'_>) {
    // Skip until we hit a comma, RParen, or a known keyword
    let mut depth = 0;
    loop {
        match parser.peek_token_ref().token {
            Token::EOF | Token::SemiColon => break,
            Token::Comma | Token::RParen if depth == 0 => break,
            Token::LParen => {
                depth += 1;
                parser.next_token();
            }
            Token::RParen => {
                depth -= 1;
                parser.next_token();
            }
            _ => {
                if depth == 0 {
                    if peek_word_eq(parser, 0, "COMMENT")
                        || peek_word_eq(parser, 0, "NOT")
                        || peek_word_eq(parser, 0, "NULL")
                        || peek_word_eq(parser, 0, "AUTO_INCREMENT")
                    {
                        break;
                    }
                }
                parser.next_token();
            }
        }
    }
}
