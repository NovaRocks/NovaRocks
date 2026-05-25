use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::{StarRocksDialect, convert_object_name, convert_sql_type, peek_word_eq};
use crate::engine::catalog::normalize_identifier;
use crate::sql::analyzer::iceberg_ref::split_ref_suffix;
use crate::sql::catalog::LegacyRangePartition;
use crate::sql::parser::ast::{
    ColumnAggregation, CreateTableKind, CreateTableStmt, DefaultLiteral, IcebergPartitionFieldExpr,
    SqlType, TableColumnDef, TableKeyDesc, TableKeyKind,
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
/// [TBLPROPERTIES (...)]
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
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

    let name = convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;

    // Parse optional column definitions.
    // CTAS (`CREATE TABLE ... AS SELECT`) omits the column list entirely;
    // schema is inferred from the SELECT output at engine time.
    let columns = if parser.peek_token_ref().token == Token::LParen {
        parser
            .expect_token(&Token::LParen)
            .map_err(|e| e.to_string())?;
        parse_column_definitions(parser)?
    } else {
        Vec::new()
    };

    // Parse trailing clauses: ENGINE, KEY type, COMMENT, PARTITION, DISTRIBUTED, ORDER BY, PROPERTIES
    let mut _engine = None;
    let mut key_desc = None;
    let mut bucket_count = None;
    let mut distribution_columns: Vec<String> = Vec::new();
    let mut partition_fields = Vec::new();
    let mut legacy_range_partitions = Vec::new();
    let mut parsed_iceberg_partition_clause = false;
    let mut properties = Vec::new();
    let mut as_select: Option<Box<sqlparser::ast::Query>> = None;

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
            if is_legacy_partition_clause(parser) {
                legacy_range_partitions.extend(parse_legacy_range_partitions_or_skip(parser)?);
            } else if key_desc.is_some() || _engine.as_deref() == Some("olap") {
                skip_until_keyword_or_eof(
                    parser,
                    &["DISTRIBUTED", "ORDER", "PROPERTIES", "TBLPROPERTIES"],
                );
            } else {
                partition_fields = parse_partition_by_clause(parser)?;
                parsed_iceberg_partition_clause = true;
                ensure_partition_clause_boundary(parser)?;
            }
        } else if peek_word_eq(parser, 0, "DISTRIBUTED") {
            let (parsed_bucket_count, parsed_distribution_columns) =
                if parsed_iceberg_partition_clause {
                    parse_bucket_count_strict(parser)?
                } else {
                    parse_bucket_count(parser)?
                };
            bucket_count = parsed_bucket_count;
            distribution_columns = parsed_distribution_columns;
        } else if parser.parse_keyword(Keyword::ORDER) {
            // ORDER BY (...)
            let _ = parser.parse_keyword(Keyword::BY);
            skip_parenthesized(parser);
        } else if peek_word_eq(parser, 0, "PROPERTIES") || peek_word_eq(parser, 0, "TBLPROPERTIES")
        {
            parser.next_token(); // PROPERTIES / TBLPROPERTIES
            properties = parse_kv_properties_vec(parser)?;
        } else if parser.parse_keyword(Keyword::AS) {
            // CTAS: `AS <select>` must be the final clause; nothing follows.
            let query = parser.parse_query().map_err(|e| format!("CTAS: {e}"))?;
            as_select = Some(query);
            break;
        } else {
            if parsed_iceberg_partition_clause {
                return Err(format!(
                    "unexpected token after PARTITION BY clause: {}",
                    parser.peek_token_ref().token
                ));
            }
            // Skip unknown token
            parser.next_token();
        }
    }

    if parsed_iceberg_partition_clause {
        ensure_partitioned_create_table_end(parser)?;
    }

    // Parse-time CTAS rejections — checked after the clause loop so all
    // properties and column lists are fully populated before validation.
    if as_select.is_some() {
        // Reject branch/tag suffix in the target table name.
        if let (_, Some(_)) = split_ref_suffix(&name.parts) {
            return Err("CTAS does not support branch target".to_string());
        }

        // Reject explicit column list — CTAS infers schema from SELECT output.
        if !columns.is_empty() {
            return Err("CTAS with explicit column definitions is not supported; \
                 use CREATE TABLE then INSERT instead"
                .to_string());
        }

        // Validate TBLPROPERTIES constraints.
        for (k, v) in &properties {
            if k.eq_ignore_ascii_case("format-version") && v != "3" {
                return Err(format!("CTAS only supports format-version=3, got '{v}'"));
            }
            if (k.eq_ignore_ascii_case("row-lineage")
                || k.eq_ignore_ascii_case("write.row-lineage"))
                && !v.eq_ignore_ascii_case("true")
            {
                return Err(format!("CTAS requires row-lineage=true, got '{v}'"));
            }
        }
    }

    let kind = CreateTableKind::Iceberg {
        columns,
        key_desc,
        bucket_count,
        distribution_columns,
        partition_fields,
        properties,
    };

    Ok(CreateTableStmt {
        name,
        kind,
        legacy_range_partitions,
        as_select,
        if_not_exists,
    })
}

pub(crate) fn parse_sql_type_string(sql: &str) -> Result<SqlType, String> {
    let normalized = super::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| e.to_string())?;
    let data_type = parse_sql_type_definition(&mut parser)?;
    if parser.peek_token_ref().token != Token::EOF
        && parser.peek_token_ref().token != Token::SemiColon
    {
        return Err(format!(
            "unexpected token after SQL type: {:?}",
            parser.peek_token_ref().token
        ));
    }
    Ok(data_type)
}

fn is_legacy_partition_clause(parser: &Parser<'_>) -> bool {
    if !peek_word_eq(parser, 0, "PARTITION") || !peek_word_eq(parser, 1, "BY") {
        return false;
    }
    let first_field_offset = if parser.peek_nth_token_ref(2).token == Token::LParen {
        3
    } else {
        2
    };
    let next_offset = first_field_offset + 1;
    matches!(
        parser.peek_nth_token_ref(first_field_offset).token,
        Token::Word(ref word)
            if word.quote_style.is_none()
                && parser.peek_nth_token_ref(next_offset).token == Token::LParen
                && matches!(
                    word.value.to_ascii_lowercase().as_str(),
                    "range" | "list" | "date_trunc" | "time_slice"
                )
    )
}

fn parse_legacy_range_partitions_or_skip(
    parser: &mut Parser<'_>,
) -> Result<Vec<LegacyRangePartition>, String> {
    if !peek_word_eq(parser, 0, "PARTITION")
        || !peek_word_eq(parser, 1, "BY")
        || !peek_word_eq(parser, 2, "RANGE")
    {
        skip_until_keyword_or_eof(
            parser,
            &["DISTRIBUTED", "ORDER", "PROPERTIES", "TBLPROPERTIES"],
        );
        return Ok(Vec::new());
    }

    parser.next_token(); // PARTITION
    parser
        .expect_keyword(Keyword::BY)
        .map_err(|e| format!("expected BY after PARTITION: {e}"))?;
    expect_word(parser, "RANGE")?;
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| format!("expected ( after PARTITION BY RANGE: {e}"))?;
    let column = parser
        .parse_identifier()
        .map_err(|e| format!("expected RANGE partition column: {e}"))?
        .value;
    parser
        .expect_token(&Token::RParen)
        .map_err(|e| format!("expected ) after RANGE partition column: {e}"))?;

    parser
        .expect_token(&Token::LParen)
        .map_err(|e| format!("expected partition list after RANGE column: {e}"))?;
    let mut partitions = Vec::new();
    loop {
        if parser.consume_token(&Token::RParen) {
            break;
        }
        expect_word(parser, "PARTITION")?;
        let name = parser
            .parse_identifier()
            .map_err(|e| format!("expected partition name: {e}"))?
            .value;
        expect_word(parser, "VALUES")?;
        if parser.peek_token_ref().token != Token::LBracket {
            skip_until_keyword_or_eof(
                parser,
                &["DISTRIBUTED", "ORDER", "PROPERTIES", "TBLPROPERTIES"],
            );
            return Ok(Vec::new());
        }
        let (lower_sql, upper_sql) = parse_legacy_range_values(parser)?;
        partitions.push(LegacyRangePartition {
            name,
            column: column.clone(),
            lower_sql,
            upper_sql,
        });
        if parser.consume_token(&Token::Comma) {
            continue;
        }
        parser
            .expect_token(&Token::RParen)
            .map_err(|e| format!("expected , or ) after range partition: {e}"))?;
        break;
    }
    Ok(partitions)
}

pub(crate) fn parse_legacy_range_values(
    parser: &mut Parser<'_>,
) -> Result<(String, String), String> {
    parser
        .expect_token(&Token::LBracket)
        .map_err(|e| format!("expected [ before range partition lower bound: {e}"))?;
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| format!("expected ( before range partition lower bound: {e}"))?;
    let lower = parser
        .parse_expr()
        .map_err(|e| format!("parse range partition lower bound: {e}"))?;
    parser
        .expect_token(&Token::RParen)
        .map_err(|e| format!("expected ) after range partition lower bound: {e}"))?;
    parser
        .expect_token(&Token::Comma)
        .map_err(|e| format!("expected , between range partition bounds: {e}"))?;
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| format!("expected ( before range partition upper bound: {e}"))?;
    let upper = parser
        .parse_expr()
        .map_err(|e| format!("parse range partition upper bound: {e}"))?;
    parser
        .expect_token(&Token::RParen)
        .map_err(|e| format!("expected ) after range partition upper bound: {e}"))?;
    parser
        .expect_token(&Token::RParen)
        .map_err(|e| format!("expected ) after range partition interval: {e}"))?;
    Ok((partition_bound_sql(&lower), partition_bound_sql(&upper)))
}

fn partition_bound_sql(expr: &sqlparser::ast::Expr) -> String {
    use sqlparser::ast::{Expr, Value};
    match expr {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                format!("'{}'", s.replace('\'', "''"))
            }
            Value::Number(n, _) => n.clone(),
            _ => expr.to_string(),
        },
        _ => expr.to_string(),
    }
}

fn ensure_partition_clause_boundary(parser: &mut Parser<'_>) -> Result<(), String> {
    if parser.peek_token_ref().token == Token::EOF
        || parser.peek_token_ref().token == Token::SemiColon
        || peek_word_eq(parser, 0, "DISTRIBUTED")
        || peek_word_eq(parser, 0, "ORDER")
        || peek_word_eq(parser, 0, "PROPERTIES")
        || peek_word_eq(parser, 0, "TBLPROPERTIES")
        || peek_word_eq(parser, 0, "AS")
    {
        return Ok(());
    }
    Err(format!(
        "unexpected token after PARTITION BY clause: {}",
        parser.peek_token_ref().token
    ))
}

fn ensure_partitioned_create_table_end(parser: &mut Parser<'_>) -> Result<(), String> {
    if parser.consume_token(&Token::SemiColon) {
        return match parser.peek_token_ref().token {
            Token::EOF => Ok(()),
            ref other => Err(format!(
                "unexpected token after PARTITION BY clause: {other}"
            )),
        };
    }
    match parser.peek_token_ref().token {
        Token::EOF => Ok(()),
        ref other => Err(format!(
            "unexpected token after PARTITION BY clause: {other}"
        )),
    }
}

fn parse_partition_by_clause(
    parser: &mut Parser<'_>,
) -> Result<Vec<IcebergPartitionFieldExpr>, String> {
    parser.next_token(); // PARTITION
    parser
        .expect_keyword(Keyword::BY)
        .map_err(|e| format!("expected BY after PARTITION: {e}"))?;

    if parser.consume_token(&Token::LParen) {
        let mut fields = Vec::new();
        loop {
            if parser.consume_token(&Token::RParen) {
                break;
            }
            if !fields.is_empty() {
                parser
                    .expect_token(&Token::Comma)
                    .map_err(|e| format!("expected , in PARTITION BY: {e}"))?;
            }
            fields.push(parse_partition_field_expr(parser)?);
            if parser.consume_token(&Token::RParen) {
                break;
            }
        }
        if fields.is_empty() {
            return Err("PARTITION BY requires at least one field".to_string());
        }
        return Ok(fields);
    }

    let first = parse_partition_field_expr(parser)?;
    let mut fields = vec![first];
    while parser.consume_token(&Token::Comma) {
        fields.push(parse_partition_field_expr(parser)?);
    }
    Ok(fields)
}

pub(crate) fn parse_partition_field_expr(
    parser: &mut Parser<'_>,
) -> Result<IcebergPartitionFieldExpr, String> {
    let name = parser
        .parse_identifier()
        .map_err(|e| format!("expected partition field column or transform: {e}"))?
        .value;

    if !parser.consume_token(&Token::LParen) {
        return Ok(IcebergPartitionFieldExpr::Identity {
            column: normalize_identifier(&name)?,
        });
    }

    let transform = name.to_ascii_lowercase();

    // identity() is unary: identity(col).  No args and multi-arg forms are rejected.
    if transform == "identity" {
        // Reject identity() — no column arg.
        if parser.consume_token(&Token::RParen) {
            return Err(
                "identity() requires exactly one column argument: identity(column)".to_string(),
            );
        }
        let column = parser
            .parse_identifier()
            .map_err(|e| {
                format!("expected column argument for partition transform `identity`: {e}")
            })?
            .value;
        let column = normalize_identifier(&column)?;
        // Reject identity(a, b) — multiple args.
        if parser.consume_token(&Token::Comma) {
            return Err(
                "identity() requires exactly one column argument: identity(column)".to_string(),
            );
        }
        parser
            .expect_token(&Token::RParen)
            .map_err(|e| format!("expected ) after identity argument: {e}"))?;
        return Ok(IcebergPartitionFieldExpr::Identity { column });
    }

    let column = parser
        .parse_identifier()
        .map_err(|e| format!("expected column argument for partition transform `{name}`: {e}"))?
        .value;
    let column = normalize_identifier(&column)?;

    let field = match transform.as_str() {
        "year" => {
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after year argument: {e}"))?;
            IcebergPartitionFieldExpr::Year { column }
        }
        "month" => {
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after month argument: {e}"))?;
            IcebergPartitionFieldExpr::Month { column }
        }
        "day" => {
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after day argument: {e}"))?;
            IcebergPartitionFieldExpr::Day { column }
        }
        "hour" => {
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after hour argument: {e}"))?;
            IcebergPartitionFieldExpr::Hour { column }
        }
        "void" => {
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after void argument: {e}"))?;
            IcebergPartitionFieldExpr::Void { column }
        }
        "bucket" => {
            parser
                .expect_token(&Token::Comma)
                .map_err(|e| format!("expected bucket column and bucket count: {e}"))?;
            let num_buckets = parse_positive_u32(parser, "bucket count")?;
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after bucket arguments: {e}"))?;
            IcebergPartitionFieldExpr::Bucket {
                column,
                num_buckets,
            }
        }
        "truncate" => {
            parser
                .expect_token(&Token::Comma)
                .map_err(|e| format!("expected truncate column and width: {e}"))?;
            let width = parse_positive_u32(parser, "truncate width")?;
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected ) after truncate arguments: {e}"))?;
            IcebergPartitionFieldExpr::Truncate { column, width }
        }
        _ => {
            return Err(format!("unsupported Iceberg partition transform `{name}`"));
        }
    };

    Ok(field)
}

fn parse_positive_u32(parser: &mut Parser<'_>, label: &str) -> Result<u32, String> {
    let token = parser.next_token();
    match token.token {
        Token::Number(value, _) => {
            let parsed = value
                .parse::<u32>()
                .map_err(|e| format!("invalid {label} `{value}`: {e}"))?;
            if parsed == 0 {
                return Err(format!("{label} must be positive"));
            }
            Ok(parsed)
        }
        other => Err(format!("expected numeric {label}, got {other}")),
    }
}

fn parse_bucket_count(parser: &mut Parser<'_>) -> Result<(Option<u32>, Vec<String>), String> {
    parser.next_token(); // DISTRIBUTED
    let mut distribution_columns = Vec::new();
    loop {
        if parser.peek_token_ref().token == Token::EOF
            || parser.peek_token_ref().token == Token::SemiColon
            || peek_word_eq(parser, 0, "ORDER")
            || peek_word_eq(parser, 0, "PROPERTIES")
            || peek_word_eq(parser, 0, "TBLPROPERTIES")
        {
            return Ok((None, distribution_columns));
        }
        if peek_word_eq(parser, 0, "BUCKETS") {
            parser.next_token(); // BUCKETS
            let token = parser.next_token();
            return match token.token {
                Token::Number(value, _) => Ok((
                    Some(
                        value
                            .parse::<u32>()
                            .map_err(|e| format!("invalid BUCKETS value `{value}`: {e}"))?,
                    ),
                    distribution_columns,
                )),
                other => Err(format!("expected numeric BUCKETS value, got {other}")),
            };
        }
        if parser.peek_token_ref().token == Token::LParen {
            // Capture column names inside `HASH(...)` once. Subsequent parens
            // (none expected today) fall through to the original skip path.
            if distribution_columns.is_empty() {
                distribution_columns = capture_parenthesized_identifiers(parser);
            } else {
                skip_parenthesized(parser);
            }
        } else {
            parser.next_token();
        }
    }
}

fn parse_bucket_count_strict(
    parser: &mut Parser<'_>,
) -> Result<(Option<u32>, Vec<String>), String> {
    parser.next_token(); // DISTRIBUTED
    parser
        .expect_keyword(Keyword::BY)
        .map_err(|e| format!("expected BY after DISTRIBUTED: {e}"))?;
    expect_word(parser, "HASH")?;
    let distribution_columns = capture_parenthesized_identifiers(parser);

    let bucket_count = if peek_word_eq(parser, 0, "BUCKETS") {
        parser.next_token(); // BUCKETS
        let token = parser.next_token();
        match token.token {
            Token::Number(value, _) => Some(
                value
                    .parse::<u32>()
                    .map_err(|e| format!("invalid BUCKETS value `{value}`: {e}"))?,
            ),
            other => return Err(format!("expected numeric BUCKETS value, got {other}")),
        }
    } else {
        None
    };

    if parser.peek_token_ref().token == Token::EOF
        || parser.peek_token_ref().token == Token::SemiColon
        || peek_word_eq(parser, 0, "ORDER")
        || peek_word_eq(parser, 0, "PROPERTIES")
        || peek_word_eq(parser, 0, "TBLPROPERTIES")
    {
        return Ok((bucket_count, distribution_columns));
    }
    Err(format!(
        "unexpected token after DISTRIBUTED clause: {}",
        parser.peek_token_ref().token
    ))
}

/// Consume a `(...)` group and return identifier tokens inside it (with quote
/// characters stripped). Used for `DISTRIBUTED BY HASH(col1, col2, ...)` so
/// the engine can reject BITMAP/HLL distribution columns.  Any non-identifier
/// tokens (commas, whitespace) are silently skipped — the engine validates
/// the resulting names against the declared schema.
fn capture_parenthesized_identifiers(parser: &mut Parser<'_>) -> Vec<String> {
    let mut names = Vec::new();
    if !parser.consume_token(&Token::LParen) {
        return names;
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
            Token::Word(w) if depth == 1 => {
                names.push(w.value);
            }
            Token::EOF => break,
            _ => {}
        }
    }
    names
}

fn expect_word(parser: &mut Parser<'_>, word: &str) -> Result<(), String> {
    let token = parser.next_token();
    match token.token {
        Token::Word(token_word) if token_word.value.eq_ignore_ascii_case(word) => Ok(()),
        other => Err(format!("expected {word}, got {other}")),
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
        let mut default: Option<DefaultLiteral> = None;

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
                if default.is_some() {
                    return Err(format!("duplicate DEFAULT clause for column `{col_name}`"));
                }
                if parser.parse_keyword(Keyword::NULL) {
                    default = Some(DefaultLiteral::Null);
                } else {
                    default = Some(parse_default_literal(parser, &sql_type)?);
                }
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
            default,
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
    } else if peek_word_eq(parser, 0, "REPLACE_IF_NOT_NULL") {
        Some(ColumnAggregation::ReplaceIfNotNull)
    } else if peek_word_eq(parser, 0, "REPLACE") {
        Some(ColumnAggregation::Replace)
    } else if peek_word_eq(parser, 0, "BITMAP_UNION") {
        Some(ColumnAggregation::BitmapUnion)
    } else if peek_word_eq(parser, 0, "HLL_UNION") {
        Some(ColumnAggregation::HllUnion)
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
                if depth == 0
                    && (peek_word_eq(parser, 0, "COMMENT")
                        || peek_word_eq(parser, 0, "NOT")
                        || peek_word_eq(parser, 0, "NULL")
                        || peek_word_eq(parser, 0, "AUTO_INCREMENT"))
                {
                    break;
                }
                parser.next_token();
            }
        }
    }
}

pub(crate) fn parse_default_literal(
    parser: &mut sqlparser::parser::Parser<'_>,
    data_type: &crate::sql::parser::ast::SqlType,
) -> Result<crate::sql::parser::ast::DefaultLiteral, String> {
    use crate::sql::parser::ast::{DefaultLiteral, SqlType};

    // Consumes one token unconditionally. Callers must propagate Err
    // immediately — parser state is not rewound on failure.
    let token = parser.next_token();
    let lit = match token.token {
        sqlparser::tokenizer::Token::Word(w) if w.value.eq_ignore_ascii_case("TRUE") => {
            DefaultLiteral::Bool(true)
        }
        sqlparser::tokenizer::Token::Word(w) if w.value.eq_ignore_ascii_case("FALSE") => {
            DefaultLiteral::Bool(false)
        }
        sqlparser::tokenizer::Token::Number(n, _) => parse_numeric_default(&n, data_type)?,
        sqlparser::tokenizer::Token::SingleQuotedString(s)
        | sqlparser::tokenizer::Token::DoubleQuotedString(s) => {
            parse_string_default(&s, data_type)?
        }
        sqlparser::tokenizer::Token::HexStringLiteral(s) => {
            let bytes = hex::decode(&s)
                .map_err(|e| format!("invalid hex DEFAULT literal `x'{s}'`: {e}"))?;
            // Validate that the column accepts binary
            if !matches!(data_type, SqlType::Binary) {
                return Err(format!(
                    "hex DEFAULT not supported for column type {data_type:?}"
                ));
            }
            DefaultLiteral::Binary(bytes)
        }
        sqlparser::tokenizer::Token::Minus => {
            // Negative numeric literal
            let next = parser.next_token();
            if let sqlparser::tokenizer::Token::Number(n, _) = next.token {
                let mut signed = String::from('-');
                signed.push_str(&n);
                parse_numeric_default(&signed, data_type)?
            } else {
                return Err(format!(
                    "expected number after `-` in DEFAULT, got {next:?}"
                ));
            }
        }
        other => {
            return Err(format!("unsupported DEFAULT value token: {other:?}"));
        }
    };

    // Type-check the literal against the column type up front so the parser
    // fails fast (re-using the conversion helper from default_value.rs).
    crate::connector::iceberg::default_value::default_literal_to_iceberg(&lit, data_type)?;

    Ok(lit)
}

fn parse_numeric_default(
    text: &str,
    data_type: &crate::sql::parser::ast::SqlType,
) -> Result<crate::sql::parser::ast::DefaultLiteral, String> {
    use crate::sql::parser::ast::{DefaultLiteral, SqlType};
    match data_type {
        SqlType::TinyInt | SqlType::SmallInt | SqlType::Int | SqlType::BigInt => {
            let v: i64 = text
                .parse()
                .map_err(|e| format!("invalid integer DEFAULT `{text}`: {e}"))?;
            Ok(DefaultLiteral::Int(v))
        }
        SqlType::Float | SqlType::Double => {
            let v: f64 = text
                .parse()
                .map_err(|e| format!("invalid float DEFAULT `{text}`: {e}"))?;
            Ok(DefaultLiteral::Float(v))
        }
        SqlType::Decimal { scale, .. } => {
            let (unscaled, scanned_scale) = decimal_from_str(text)?;
            if scanned_scale != *scale {
                return Err(format!(
                    "DEFAULT value scale {scanned_scale} does not match column scale {scale}"
                ));
            }
            Ok(DefaultLiteral::Decimal {
                unscaled,
                scale: *scale,
            })
        }
        other => Err(format!(
            "numeric DEFAULT not supported for column type {other:?}"
        )),
    }
}

fn parse_string_default(
    s: &str,
    data_type: &crate::sql::parser::ast::SqlType,
) -> Result<crate::sql::parser::ast::DefaultLiteral, String> {
    use crate::sql::parser::ast::{DefaultLiteral, SqlType};
    match data_type {
        SqlType::String => Ok(DefaultLiteral::String(s.to_string())),
        SqlType::Json => {
            serde_json::from_str::<serde_json::Value>(s)
                .map_err(|e| format!("invalid JSON DEFAULT literal: {e}"))?;
            Ok(DefaultLiteral::String(s.to_string()))
        }
        SqlType::Date => {
            let days = crate::engine::parquet::parse_date_string_to_days(s)?;
            Ok(DefaultLiteral::Date(days))
        }
        SqlType::DateTime => {
            let micros = crate::engine::parquet::parse_datetime_string_to_micros(s)?;
            Ok(DefaultLiteral::DateTime(micros))
        }
        other => Err(format!(
            "string DEFAULT not supported for column type {other:?}"
        )),
    }
}

fn decimal_from_str(text: &str) -> Result<(i128, i8), String> {
    let trimmed = text.trim();
    let (sign, body) = if let Some(rest) = trimmed.strip_prefix('-') {
        (-1i128, rest)
    } else {
        (1, trimmed)
    };
    let (whole, frac) = match body.split_once('.') {
        Some((w, f)) => (w, f),
        None => (body, ""),
    };
    let combined: String = whole.chars().chain(frac.chars()).collect();
    let unscaled: i128 = combined
        .parse()
        .map_err(|e| format!("invalid decimal DEFAULT `{text}`: {e}"))?;
    let scale = i8::try_from(frac.len()).map_err(|_| "decimal scale too large".to_string())?;
    Ok((sign * unscaled, scale))
}

#[cfg(test)]
mod tests {
    use sqlparser::parser::Parser;

    use super::parse_create_table_statement;
    use crate::sql::parser::ast::{
        ColumnAggregation, CreateTableKind, CreateTableStmt, IcebergPartitionFieldExpr, SqlType,
    };
    use crate::sql::parser::dialect::StarRocksDialect;

    /// Parse a single `CREATE TABLE` statement from `sql` and return the result.
    fn parse_create_table_one(sql: &str) -> Result<CreateTableStmt, String> {
        let mut parser = Parser::new(&StarRocksDialect)
            .try_with_sql(sql)
            .map_err(|e| e.to_string())?;
        parse_create_table_statement(&mut parser)
    }

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
    fn parse_create_table_preserves_json_logical_type() {
        let stmt =
            parse_create_table_one("CREATE TABLE t1 (id INT, payload JSON, nested ARRAY<JSON>)")
                .expect("create table with json columns");

        let CreateTableKind::Iceberg { columns, .. } = stmt.kind;
        assert_eq!(columns[1].data_type, SqlType::Json);
        assert_eq!(
            columns[2].data_type,
            SqlType::Array(Box::new(SqlType::Json))
        );
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
        let CreateTableKind::Iceberg { bucket_count, .. } = stmt.kind;
        assert_eq!(bucket_count, Some(3));
    }

    #[test]
    fn create_table_parser_accepts_olap_random_distribution_after_partition() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                "create table tbl (k int, v int) engine=olap duplicate key(k, v) \
                 partition by (`k`) distributed by random properties('replication_num'='1')",
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg {
            partition_fields, ..
        } = stmt.kind;
        assert!(partition_fields.is_empty());
    }

    #[test]
    fn create_table_parser_captures_legacy_range_partitions() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                r#"create table tbl (c0 date, c1 int) duplicate key(c0)
                   partition by range(c0) (
                     partition p202401 values [("2024-01-01"), ("2024-02-01")),
                     partition p202402 values [("2024-02-01"), ("2024-03-01"))
                   )
                   distributed by hash(c1) buckets 3"#,
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");

        assert_eq!(stmt.legacy_range_partitions.len(), 2);
        assert_eq!(stmt.legacy_range_partitions[0].name, "p202401");
        assert_eq!(stmt.legacy_range_partitions[0].column, "c0");
        assert_eq!(stmt.legacy_range_partitions[0].lower_sql, "'2024-01-01'");
        assert_eq!(stmt.legacy_range_partitions[0].upper_sql, "'2024-02-01'");
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
        let CreateTableKind::Iceberg { columns, .. } = stmt.kind;
        assert_eq!(columns.len(), 2);
        assert!(!columns[0].nullable);
        assert!(columns[1].nullable);
    }

    #[test]
    fn create_table_parser_preserves_tblproperties() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                r#"create table tbl (id bigint) partition by(id) tblproperties("format-version"="3","write.row-lineage"="true")"#,
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg { properties, .. } = stmt.kind;
        assert_eq!(
            properties,
            vec![
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ]
        );
    }

    #[test]
    fn create_table_parser_preserves_partition_transforms() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                "create table tbl (id bigint, ts datetime, name string) \
                 partition by (month(ts), bucket(id, 16), truncate(name, 8))",
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg {
            partition_fields, ..
        } = stmt.kind;
        assert_eq!(
            partition_fields,
            vec![
                IcebergPartitionFieldExpr::Month {
                    column: "ts".to_string()
                },
                IcebergPartitionFieldExpr::Bucket {
                    column: "id".to_string(),
                    num_buckets: 16
                },
                IcebergPartitionFieldExpr::Truncate {
                    column: "name".to_string(),
                    width: 8
                },
            ]
        );
    }

    #[test]
    fn create_table_parser_rejects_invalid_partition_transform_args() {
        for sql in [
            "create table tbl (id bigint) partition by bucket(id, 0)",
            "create table tbl (name string) partition by truncate(name, 0)",
            "create table tbl (id bigint) partition by unknown(id)",
            "create table tbl (ts datetime) partition by month(date_trunc(ts))",
            "create table tbl (id bigint) partition by bucket(1, 16)",
            "create table tbl (id bigint) partition by ()",
        ] {
            let dialect = StarRocksDialect;
            let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
            assert!(
                parse_create_table_statement(&mut parser).is_err(),
                "expected partition transform parse failure for {sql}"
            );
        }
    }

    #[test]
    fn create_table_parser_rejects_trailing_junk_after_partition_by() {
        for sql in [
            "create table tbl (id bigint) partition by id bogus",
            "create table tbl (id bigint) partition by bucket(id, 16) unknown_clause",
            "create table tbl (id bigint) partition by (id) bogus",
            "create table tbl (id bigint) partition by id; select 1",
            "create table tbl (id bigint) partition by id;;",
            r#"create table tbl (id bigint) partition by id tblproperties("format-version"="2"); select 1"#,
            r#"create table tbl (id bigint) partition by id tblproperties("format-version"="2");;"#,
            "create table tbl (id bigint) partition by id distributed by hash(id) buckets 3; select 1",
            r#"create table tbl (id bigint) partition by id tblproperties("format-version"="2") select 1"#,
            "create table tbl (id bigint) partition by id distributed by hash(id) buckets 3 bogus",
            "create table tbl (id bigint) partition by id distributed by hash(id) bogus",
        ] {
            let dialect = StarRocksDialect;
            let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
            let err = parse_create_table_statement(&mut parser).expect_err("partition parse error");
            assert!(
                err.contains("unexpected token after PARTITION BY clause")
                    || err.contains("unexpected token after DISTRIBUTED clause"),
                "unexpected error for {sql}: {err}"
            );
        }
    }

    #[test]
    fn create_table_parser_accepts_single_final_semicolon_after_partition_by() {
        for sql in [
            "create table tbl (id bigint) partition by id;",
            r#"create table tbl (id bigint) partition by id tblproperties("format-version"="2");"#,
        ] {
            let dialect = StarRocksDialect;
            let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
            let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
            let CreateTableKind::Iceberg {
                partition_fields, ..
            } = stmt.kind;
            assert_eq!(
                partition_fields,
                vec![IcebergPartitionFieldExpr::Identity {
                    column: "id".to_string()
                }]
            );
        }
    }

    #[test]
    fn create_table_parser_preserves_bucket_count_after_partition_by() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                "create table tbl (id bigint) partition by id distributed by hash(id) buckets 3",
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg {
            partition_fields,
            bucket_count,
            ..
        } = stmt.kind;
        assert_eq!(
            partition_fields,
            vec![IcebergPartitionFieldExpr::Identity {
                column: "id".to_string()
            }]
        );
        assert_eq!(bucket_count, Some(3));
    }

    #[test]
    fn create_table_parser_keeps_tblproperties_after_identity_partition() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                r#"create table tbl (city string) partition by city tblproperties("format-version"="2")"#,
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg {
            partition_fields,
            properties,
            ..
        } = stmt.kind;
        assert_eq!(
            partition_fields,
            vec![IcebergPartitionFieldExpr::Identity {
                column: "city".to_string()
            }]
        );
        assert_eq!(
            properties,
            vec![("format-version".to_string(), "2".to_string())]
        );
    }

    #[test]
    fn create_table_parser_treats_legacy_marker_names_as_identity_columns() {
        for sql in [
            "create table tbl (range bigint) partition by range",
            "create table tbl (`range` bigint) partition by `range`",
        ] {
            let dialect = StarRocksDialect;
            let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
            let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
            let CreateTableKind::Iceberg {
                partition_fields, ..
            } = stmt.kind;
            assert_eq!(
                partition_fields,
                vec![IcebergPartitionFieldExpr::Identity {
                    column: "range".to_string()
                }]
            );
        }
    }

    #[test]
    fn create_table_parser_accepts_identity_function_call_form() {
        // identity(col) is the verbose canonical form of the bare-column shorthand.
        for sql in [
            "create table tbl (p bigint) partition by identity(p)",
            r#"create table tbl (p bigint, k bigint, v bigint) partition by identity(p) tblproperties("format-version"="3")"#,
        ] {
            let dialect = StarRocksDialect;
            let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
            let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
            let CreateTableKind::Iceberg {
                partition_fields, ..
            } = stmt.kind;
            assert_eq!(
                partition_fields,
                vec![IcebergPartitionFieldExpr::Identity {
                    column: "p".to_string()
                }]
            );
        }
    }

    #[test]
    fn create_table_parser_rejects_identity_with_no_args() {
        let sql = "create table tbl (p bigint) partition by identity()";
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
        let err = parse_create_table_statement(&mut parser)
            .expect_err("should reject identity() with no arg");
        assert!(
            err.contains("identity()"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn create_table_parser_rejects_identity_with_multiple_args() {
        let sql = "create table tbl (a bigint, b bigint) partition by identity(a, b)";
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
        let err = parse_create_table_statement(&mut parser)
            .expect_err("should reject identity(a, b) with multiple args");
        assert!(
            err.contains("identity()"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn create_table_parser_skips_legacy_range_partition_clause() {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(
                r#"
                create table tbl (k1 int, v int)
                partition by range(k1) (
                    partition p1 values less than (10),
                    partition p2 values less than (20)
                )
                properties ("replication_num"="1")
                "#,
            )
            .expect("parser");
        let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
        let CreateTableKind::Iceberg {
            partition_fields,
            properties,
            ..
        } = stmt.kind;
        assert!(partition_fields.is_empty());
        assert_eq!(
            properties,
            vec![("replication_num".to_string(), "1".to_string())]
        );
    }

    #[test]
    fn create_table_parser_skips_legacy_list_and_function_partition_clauses() {
        for sql in [
            r#"
            create table tbl (k1 int, v int)
            partition by list(k1) (
                partition p1 values in ("a"),
                partition p2 values in ("b")
            )
            tblproperties ("format-version"="2")
            "#,
            r#"
            create table tbl (ts datetime, v int)
            partition by date_trunc("day", ts)
            properties ("replication_num"="1")
            "#,
        ] {
            let dialect = StarRocksDialect;
            let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
            let stmt = parse_create_table_statement(&mut parser).expect("create table stmt");
            let CreateTableKind::Iceberg {
                partition_fields,
                properties,
                ..
            } = stmt.kind;
            assert!(partition_fields.is_empty());
            assert!(!properties.is_empty());
        }
    }

    #[test]
    fn parse_create_table_captures_int_default() {
        let sql = r#"
            CREATE TABLE ice.ns.t (a INT, b INT DEFAULT 5)
            PROPERTIES ('format-version' = '3')
        "#;
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(sql)
            .expect("build parser");
        let stmt = parse_create_table_statement(&mut parser).expect("parsed");
        let CreateTableKind::Iceberg { columns, .. } = stmt.kind else {
            panic!("expected iceberg create table");
        };
        assert_eq!(
            columns[1].default,
            Some(crate::sql::parser::ast::DefaultLiteral::Int(5))
        );
    }

    #[test]
    fn parse_create_table_captures_default_null() {
        let sql = r#"
            CREATE TABLE ice.ns.t (a INT, b INT DEFAULT NULL)
        "#;
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(sql)
            .expect("build parser");
        let stmt = parse_create_table_statement(&mut parser).expect("parsed");
        let CreateTableKind::Iceberg { columns, .. } = stmt.kind else {
            panic!("expected iceberg create table");
        };
        assert_eq!(columns[0].default, None);
        assert_eq!(
            columns[1].default,
            Some(crate::sql::parser::ast::DefaultLiteral::Null)
        );
    }

    #[test]
    fn parse_create_table_rejects_duplicate_default() {
        let sql =
            "CREATE TABLE ice.ns.t (b INT DEFAULT 5 DEFAULT 6) PROPERTIES ('format-version' = '3')";
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect)
            .try_with_sql(sql)
            .expect("build parser");
        let err = parse_create_table_statement(&mut parser).expect_err("duplicate DEFAULT");
        assert!(err.contains("duplicate DEFAULT"), "unexpected error: {err}");
    }

    // ----- CTAS tests -----

    #[test]
    fn parse_create_table_as_select_basic() {
        let stmt = parse_create_table_one("CREATE TABLE t AS SELECT 1 AS x, 'a' AS y").unwrap();
        assert!(stmt.as_select.is_some());
        let CreateTableKind::Iceberg { columns, .. } = stmt.kind;
        assert!(
            columns.is_empty(),
            "CTAS infers schema from SELECT, no explicit columns"
        );
    }

    #[test]
    fn parse_create_table_as_select_if_not_exists() {
        let stmt = parse_create_table_one("CREATE TABLE IF NOT EXISTS t AS SELECT 1 AS x").unwrap();
        assert!(stmt.as_select.is_some());
    }

    #[test]
    fn parse_create_table_as_select_with_partitioned_by_and_properties() {
        let stmt = parse_create_table_one(
            "CREATE TABLE t PARTITION BY (region) TBLPROPERTIES('format-version'='3') AS SELECT 1 AS region",
        )
        .unwrap();
        assert!(stmt.as_select.is_some());
        let CreateTableKind::Iceberg {
            partition_fields,
            properties,
            ..
        } = stmt.kind;
        assert_eq!(partition_fields.len(), 1);
        assert!(
            properties
                .iter()
                .any(|(k, v)| k == "format-version" && v == "3"),
            "expected format-version=3 in properties"
        );
    }

    #[test]
    fn parse_create_table_as_select_branch_rejected() {
        let err = parse_create_table_one("CREATE TABLE t.branch_dev AS SELECT 1 AS x").unwrap_err();
        assert!(
            err.to_lowercase().contains("branch"),
            "expected branch rejection, got: {err}"
        );
    }

    #[test]
    fn parse_create_table_as_select_format_version_2_rejected() {
        let err = parse_create_table_one(
            "CREATE TABLE t TBLPROPERTIES('format-version'='2') AS SELECT 1 AS x",
        )
        .unwrap_err();
        assert!(
            err.to_lowercase().contains("format-version"),
            "expected format-version rejection, got: {err}"
        );
    }

    #[test]
    fn parse_create_table_as_select_row_lineage_false_rejected() {
        let err = parse_create_table_one(
            "CREATE TABLE t TBLPROPERTIES('write.row-lineage'='false') AS SELECT 1 AS x",
        )
        .unwrap_err();
        assert!(
            err.to_lowercase().contains("row-lineage"),
            "expected row-lineage rejection, got: {err}"
        );
    }

    #[test]
    fn parse_create_table_as_select_with_explicit_columns_rejected() {
        let err =
            parse_create_table_one("CREATE TABLE t (id INT, name VARCHAR(32)) AS SELECT 1, 'a'")
                .unwrap_err();
        assert!(
            err.to_lowercase().contains("column"),
            "expected explicit-column rejection, got: {err}"
        );
    }

    #[test]
    fn parse_create_table_without_as_select_unchanged() {
        let stmt = parse_create_table_one("CREATE TABLE t (id INT) ENGINE=ICEBERG").unwrap();
        assert!(stmt.as_select.is_none());
        let CreateTableKind::Iceberg { columns, .. } = stmt.kind;
        assert_eq!(columns.len(), 1);
    }

    #[test]
    fn parse_create_table_with_bitmap_and_hll_columns() {
        let sql = "CREATE TABLE foo (k INT, bm BITMAP, hv HLL) \
                   DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1";
        let stmt = parse_create_table_one(sql).expect("parse must succeed");
        let CreateTableKind::Iceberg { columns, .. } = stmt.kind;
        assert_eq!(columns[1].data_type, SqlType::Bitmap);
        assert_eq!(columns[2].data_type, SqlType::Hll);
    }

    #[test]
    fn parse_aggregate_key_with_bitmap_hll_state_columns() {
        let sql = "CREATE TABLE foo (
        k INT,
        bm BITMAP BITMAP_UNION,
        hv HLL HLL_UNION
    ) AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1";
        let stmt = parse_create_table_one(sql).expect("parse must succeed");
        let CreateTableKind::Iceberg { columns, .. } = stmt.kind else {
            panic!("expected Iceberg kind");
        };
        assert_eq!(columns[1].aggregation, Some(ColumnAggregation::BitmapUnion));
        assert_eq!(columns[2].aggregation, Some(ColumnAggregation::HllUnion));
    }
}
