use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::{convert_object_name, convert_sql_type, peek_word_eq};
use crate::sql::parser::ast::{
    ColumnAggregation, CreateTableKind, CreateTableStmt, SqlType, TableColumnDef, TableKeyDesc,
    TableKeyKind,
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
    let mut bucket_count = None;
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
            bucket_count = parse_bucket_count(parser)?;
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
        bucket_count,
        properties,
    };

    Ok(CreateTableStmt { name, kind })
}

fn parse_bucket_count(parser: &mut Parser<'_>) -> Result<Option<u32>, String> {
    parser.next_token(); // DISTRIBUTED
    loop {
        if parser.peek_token_ref().token == Token::EOF
            || parser.peek_token_ref().token == Token::SemiColon
            || peek_word_eq(parser, 0, "ORDER")
            || peek_word_eq(parser, 0, "PROPERTIES")
        {
            return Ok(None);
        }
        if peek_word_eq(parser, 0, "BUCKETS") {
            parser.next_token(); // BUCKETS
            let token = parser.next_token();
            return match token.token {
                Token::Number(value, _) => {
                    Ok(Some(value.parse::<u32>().map_err(|e| {
                        format!("invalid BUCKETS value `{value}`: {e}")
                    })?))
                }
                other => Err(format!("expected numeric BUCKETS value, got {other}")),
            };
        }
        if parser.peek_token_ref().token == Token::LParen {
            skip_parenthesized(parser);
        } else {
            parser.next_token();
        }
    }
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
        let sql_type = parse_sql_type_definition(parser)?;

        let mut aggregation = None;
        let mut nullable = true;
        let mut _comment = None;

        // Parse optional NOT NULL, NULL, DEFAULT, COMMENT, AUTO_INCREMENT, etc.
        loop {
            if aggregation.is_none()
                && let Some(parsed) = parse_column_aggregation(parser)
            {
                aggregation = Some(parsed);
            } else if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
                nullable = false;
            } else if parser.parse_keyword(Keyword::NULL) {
                nullable = true;
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
            nullable,
            aggregation,
        });
    }
    Ok(columns)
}

fn parse_column_aggregation(parser: &mut Parser<'_>) -> Option<ColumnAggregation> {
    let aggregation = if peek_word_eq(parser, 0, "SUM") {
        Some(ColumnAggregation::Sum)
    } else if peek_word_eq(parser, 0, "MIN") {
        Some(ColumnAggregation::Min)
    } else if peek_word_eq(parser, 0, "MAX") {
        Some(ColumnAggregation::Max)
    } else if peek_word_eq(parser, 0, "REPLACE") {
        Some(ColumnAggregation::Replace)
    } else {
        None
    }?;
    parser.next_token();
    Some(aggregation)
}

fn parse_sql_type_definition(parser: &mut Parser<'_>) -> Result<SqlType, String> {
    if peek_word_eq(parser, 0, "ARRAY") {
        parse_array_sql_type(parser)
    } else if peek_word_eq(parser, 0, "MAP") {
        parse_map_sql_type(parser)
    } else if peek_word_eq(parser, 0, "STRUCT") {
        parse_struct_sql_type(parser)
    } else {
        let data_type = parser.parse_data_type().map_err(|e| e.to_string())?;
        convert_sql_type(data_type)
    }
}

fn parse_array_sql_type(parser: &mut Parser<'_>) -> Result<SqlType, String> {
    parser.next_token(); // ARRAY
    if parser.consume_token(&Token::Lt) {
        let element_type = parse_sql_type_definition(parser)?;
        parser.expect_token(&Token::Gt).map_err(|e| e.to_string())?;
        Ok(SqlType::Array(Box::new(element_type)))
    } else {
        convert_sql_type(sqlparser::ast::DataType::Array(
            sqlparser::ast::ArrayElemTypeDef::AngleBracket(Box::new(
                parser.parse_data_type().map_err(|e| e.to_string())?,
            )),
        ))
    }
}

fn parse_map_sql_type(parser: &mut Parser<'_>) -> Result<SqlType, String> {
    parser.next_token(); // MAP
    parser.expect_token(&Token::Lt).map_err(|e| e.to_string())?;
    let key_type = parse_sql_type_definition(parser)?;
    parser
        .expect_token(&Token::Comma)
        .map_err(|e| e.to_string())?;
    let value_type = parse_sql_type_definition(parser)?;
    parser.expect_token(&Token::Gt).map_err(|e| e.to_string())?;
    Ok(SqlType::Map(Box::new(key_type), Box::new(value_type)))
}

fn parse_struct_sql_type(parser: &mut Parser<'_>) -> Result<SqlType, String> {
    parser.next_token(); // STRUCT
    parser.expect_token(&Token::Lt).map_err(|e| e.to_string())?;
    let mut fields = Vec::new();
    loop {
        if parser.consume_token(&Token::Gt) {
            break;
        }
        if !fields.is_empty() {
            parser
                .expect_token(&Token::Comma)
                .map_err(|e| e.to_string())?;
        }
        let field_name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        let field_type = parse_sql_type_definition(parser)?;
        fields.push((field_name, field_type));
    }
    Ok(SqlType::Struct(fields))
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

#[cfg(test)]
mod tests {
    use sqlparser::parser::Parser;

    use super::parse_create_table_statement;
    use crate::sql::parser::ast::CreateTableKind;
    use crate::sql::parser::dialect::StarRocksDialect;

    #[test]
    fn parse_create_table_accepts_map_and_struct_columns() {
        let sql = r#"
            CREATE TABLE t1 (
                c12 map<varchar(5), double>,
                c13 struct<a bigint, b string>
            )
            DUPLICATE KEY(c12)
            DISTRIBUTED BY HASH(c12) BUCKETS 3
            PROPERTIES ("replication_num" = "1")
        "#;

        let mut parser = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql(sql)
            .expect("build parser");
        let stmt = parse_create_table_statement(&mut parser);
        assert!(stmt.is_ok(), "expected complex type DDL to parse: {stmt:?}");
    }

    #[test]
    fn parse_create_table_accepts_nested_array_complex_columns() {
        let sql = r#"
            CREATE TABLE t1 (
                c1 array<array<int>>,
                c2 array<map<string, int>>,
                c3 array<struct<f1 int, f2 string>>
            )
            DUPLICATE KEY(c1)
            DISTRIBUTED BY HASH(c1) BUCKETS 3
            PROPERTIES ("replication_num" = "1")
        "#;

        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
            .expect("normalize should succeed");
        let mut parser = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql(&normalized)
            .expect("build parser");
        let stmt = parse_create_table_statement(&mut parser);
        assert!(
            stmt.is_ok(),
            "expected nested complex type DDL to parse: {stmt:?}"
        );
    }

    #[test]
    fn create_table_parser_preserves_bucket_count() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                "create table tbl (id int) duplicate key(id) distributed by hash(id) buckets 3",
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        match stmt.kind {
            CreateTableKind::Iceberg { bucket_count, .. } => {
                assert_eq!(bucket_count, Some(3));
            }
            other => panic!("unexpected create table kind: {other:?}"),
        }
    }

    #[test]
    fn create_table_parser_preserves_column_nullability() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                "create table tbl (id int not null, note string null) duplicate key(id) distributed by hash(id) buckets 3",
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        match stmt.kind {
            CreateTableKind::Iceberg { columns, .. } => {
                assert_eq!(columns.len(), 2);
                assert!(!columns[0].nullable);
                assert!(columns[1].nullable);
            }
            other => panic!("unexpected create table kind: {other:?}"),
        }
    }
}
