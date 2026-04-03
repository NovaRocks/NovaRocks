use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;

use super::{convert_object_name, peek_word_eq};
use crate::sql::parser::ast::{DropCatalogStmt, DropDatabaseStmt, DropTableStmt};

/// Result of parsing a DROP statement.
pub(crate) enum DropResult {
    Table(DropTableStmt),
    Database(DropDatabaseStmt),
    Catalog(DropCatalogStmt),
}

/// Parse DROP TABLE/DATABASE/CATALOG with optional IF EXISTS and FORCE.
pub(crate) fn parse_drop_statement(parser: &mut Parser<'_>) -> Result<DropResult, String> {
    parser
        .expect_keyword(Keyword::DROP)
        .map_err(|e| e.to_string())?;

    if peek_word_eq(parser, 0, "TABLE") {
        parser.next_token();
        let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name =
            convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;
        let force = peek_word_eq(parser, 0, "FORCE") && {
            parser.next_token();
            true
        };
        Ok(DropResult::Table(DropTableStmt {
            name,
            if_exists,
            force,
        }))
    } else if peek_word_eq(parser, 0, "DATABASE") {
        parser.next_token();
        let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name =
            convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;
        let force = peek_word_eq(parser, 0, "FORCE") && {
            parser.next_token();
            true
        };
        Ok(DropResult::Database(DropDatabaseStmt {
            name,
            if_exists,
            force,
        }))
    } else if peek_word_eq(parser, 0, "CATALOG") {
        parser.next_token();
        let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        Ok(DropResult::Catalog(DropCatalogStmt { name, if_exists }))
    } else {
        Err("expected TABLE, DATABASE, or CATALOG after DROP".into())
    }
}
