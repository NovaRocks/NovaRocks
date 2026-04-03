use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::peek_word_eq;
use crate::sql::parser::ast::CreateCatalogStmt;

/// Parse: CREATE [EXTERNAL] CATALOG <name> [COMMENT '...'] PROPERTIES ( "key"="value", ... )
pub(crate) fn parse_create_catalog_statement(
    parser: &mut Parser<'_>,
) -> Result<CreateCatalogStmt, String> {
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(|e| e.to_string())?;
    // Skip optional EXTERNAL keyword
    let _ = parser.parse_keyword(Keyword::EXTERNAL);
    if !peek_word_eq(parser, 0, "CATALOG") {
        return Err("expected CATALOG keyword".into());
    }
    parser.next_token(); // consume CATALOG

    // Skip optional IF NOT EXISTS
    let _ = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

    let name = parser.parse_identifier().map_err(|e| e.to_string())?.value;

    // Skip optional COMMENT clause
    if peek_word_eq(parser, 0, "COMMENT") {
        parser.next_token(); // COMMENT
        parser.next_token(); // the string literal
    }

    // Parse PROPERTIES or WITH PROPERTIES
    let properties = if peek_word_eq(parser, 0, "PROPERTIES") || peek_word_eq(parser, 0, "WITH") {
        if peek_word_eq(parser, 0, "WITH") {
            parser.next_token();
        }
        parser.next_token(); // PROPERTIES
        parse_properties(parser)?
    } else {
        vec![]
    };

    Ok(CreateCatalogStmt { name, properties })
}

fn parse_properties(parser: &mut Parser<'_>) -> Result<Vec<(String, String)>, String> {
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| e.to_string())?;
    let mut props = Vec::new();
    loop {
        if parser.consume_token(&Token::RParen) {
            break;
        }
        if !props.is_empty() {
            parser
                .expect_token(&Token::Comma)
                .map_err(|e| e.to_string())?;
            if parser.consume_token(&Token::RParen) {
                break;
            }
        }
        let key = parse_string_value(parser)?;
        parser.expect_token(&Token::Eq).map_err(|e| e.to_string())?;
        let value = parse_string_value(parser)?;
        props.push((key, value));
    }
    Ok(props)
}

fn parse_string_value(parser: &mut Parser<'_>) -> Result<String, String> {
    let token = parser.next_token();
    match token.token {
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => Ok(s),
        Token::Word(w) => Ok(w.value),
        other => Err(format!("expected string or identifier, got {other}")),
    }
}
