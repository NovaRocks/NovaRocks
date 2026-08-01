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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::dialect::StarRocksDialect;

    #[test]
    fn drop_database_if_exists_force_preserves_both_modifiers() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql("DROP DATABASE IF EXISTS `stale_db` FORCE")
            .expect("parser");

        let DropResult::Database(statement) = parse_drop_statement(&mut parser).expect("drop")
        else {
            panic!("expected DROP DATABASE");
        };
        assert!(statement.if_exists);
        assert!(statement.force);
    }
}
