// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// Licensed under the Apache License, Version 2.0.

//! Table DDL grammar owned by SQLP-5.

use crate::ast::{AstBox, TypeParameter};

use super::{StatementParser, query};
use crate::{
    ParseError, Span,
    ast::{
        ColumnDefinition, CreateTable, CreateTableAsSelect, DataTypeParameter, DmlStatement,
        LegacyRangePartition, LegacyRangePartitionDefinition, Literal, LiteralKind,
        PartitionTransform, Statement, TableDistribution, TableKey, TableKeyKind, TablePartition,
        TablePartitionTransform, TableProperty, TableStatement, TypeName,
    },
    token::Symbol,
};

/// `CREATE [TEMPORARY | EXTERNAL] TABLE [IF NOT EXISTS] name table-body`.
pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if !looks_like_create_table(parser) {
        return Ok(None);
    }
    let start = parser.consume_word("CREATE")?.start();
    let mut temporary = false;
    let mut external = false;
    loop {
        if parser.consume_if_word("TEMPORARY") {
            if temporary {
                return Err(parser.unexpected("one TEMPORARY modifier"));
            }
            temporary = true;
        } else if parser.consume_if_word("EXTERNAL") {
            if external {
                return Err(parser.unexpected("one EXTERNAL modifier"));
            }
            external = true;
        } else {
            break;
        }
    }
    parser.consume_word("TABLE")?;
    let if_not_exists = if parser.consume_if_word("IF") {
        parser.consume_word("NOT")?;
        parser.consume_word("EXISTS")?;
        true
    } else {
        false
    };
    let name = parser.parse_object_name()?;
    if parser.consume_if_word("LIKE") {
        let like = parser.parse_object_name()?;
        return Ok(Some(Statement::Table(TableStatement::Create(
            CreateTable {
                temporary,
                external,
                if_not_exists,
                name,
                engine: None,
                like: Some(like.clone()),
                columns: vec![],
                key: None,
                distribution: None,
                partition: None,
                order_by: vec![],
                properties: vec![],
                comment: None,
                span: Span::new(start, like.span.end()),
            },
        ))));
    }
    let columns = if parser.current_is_symbol(Symbol::LParen) {
        parse_columns(parser)?
    } else {
        vec![]
    };
    let mut key = None;
    let mut distribution = None;
    let mut partition = None;
    let mut order_by = vec![];
    let mut properties = vec![];
    let mut comment = None;
    let mut engine = None;
    loop {
        if parser.current_is_word("AS") {
            parser.consume_word("AS")?;
            let query = query::parse_query(parser)?;
            let table = TableStatement::Create(CreateTable {
                temporary,
                external,
                if_not_exists,
                name,
                engine,
                like: None,
                columns,
                key,
                distribution,
                partition,
                order_by,
                properties,
                comment,
                span: Span::new(start, query.span.start()),
            });
            return Ok(Some(Statement::Dml(DmlStatement::CreateTableAsSelect(
                CreateTableAsSelect {
                    table,
                    span: Span::new(start, query.span.end()),
                    query,
                },
            ))));
        }
        if parser.current_is_symbol(Symbol::Semicolon)
            || parser
                .current()
                .is_some_and(|token| matches!(token.kind, crate::TokenKind::End))
            || parser.current().is_none()
        {
            break;
        }
        if parser.current_is_word("DUPLICATE") {
            one(
                &mut key,
                parse_key(parser, TableKeyKind::Duplicate)?,
                parser,
            )?;
        } else if parser.current_is_word("UNIQUE") {
            one(&mut key, parse_key(parser, TableKeyKind::Unique)?, parser)?;
        } else if parser.current_is_word("AGGREGATE") {
            one(
                &mut key,
                parse_key(parser, TableKeyKind::Aggregate)?,
                parser,
            )?;
        } else if parser.current_is_word("PRIMARY") {
            one(&mut key, parse_key(parser, TableKeyKind::Primary)?, parser)?;
        } else if parser.current_is_word("DISTRIBUTED") {
            one(&mut distribution, parse_distribution(parser)?, parser)?;
        } else if parser.current_is_word("PARTITION") {
            one(&mut partition, parse_partition(parser)?, parser)?;
        } else if parser.current_is_word("ORDER") {
            if !order_by.is_empty() {
                return Err(parser.unexpected("one ORDER BY clause"));
            }
            parser.consume_word("ORDER")?;
            parser.consume_word("BY")?;
            order_by = ident_list(parser)?;
        } else if parser.current_is_word("PROPERTIES") || parser.current_is_word("TBLPROPERTIES") {
            if !properties.is_empty() {
                return Err(parser.unexpected("one PROPERTIES clause"));
            }
            properties = parse_properties(parser)?;
        } else if parser.consume_if_word("COMMENT") {
            if comment.is_some() {
                return Err(parser.unexpected("one table COMMENT clause"));
            }
            comment = Some(parser.parse_literal()?);
        } else if parser.consume_if_word("ENGINE") {
            parser.consume_if_symbol(Symbol::Eq);
            if engine.is_some() {
                return Err(parser.unexpected("one ENGINE clause"));
            }
            engine = Some(parser.parse_contextual_ident()?);
        } else {
            return Err(parser.unexpected("CREATE TABLE clause or AS"));
        }
    }
    let end = comment.as_ref().map_or_else(
        || columns.last().map_or(name.span.end(), |v| v.span.end()),
        |v| v.span.end(),
    );
    let table = CreateTable {
        temporary,
        external,
        if_not_exists,
        name,
        engine,
        like: None,
        columns,
        key,
        distribution,
        partition,
        order_by,
        properties,
        comment,
        span: Span::new(start, end),
    };
    Ok(Some(Statement::Table(TableStatement::Create(table))))
}

fn looks_like_create_table(parser: &StatementParser<'_, '_>) -> bool {
    if !parser.current_is_word("CREATE") {
        return false;
    }
    let mut offset = 1;
    while parser.peek_word(offset, "TEMPORARY") || parser.peek_word(offset, "EXTERNAL") {
        offset += 1;
    }
    parser.peek_word(offset, "TABLE")
}

/// `("(" column-definition { "," column-definition } ")")`.
fn parse_columns(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<ColumnDefinition>, ParseError> {
    parser.consume_symbol(Symbol::LParen)?;
    let mut values = vec![];
    loop {
        if parser.current_is_symbol(Symbol::RParen) {
            if values.is_empty() {
                return Err(parser.unexpected("column definition"));
            }
            parser.consume_symbol(Symbol::RParen)?;
            return Ok(values);
        }
        values.push(parse_column(parser)?);
        if parser.consume_if_symbol(Symbol::RParen) {
            return Ok(values);
        }
        parser.consume_symbol(Symbol::Comma)?;
    }
}

/// `identifier type-name [aggregation] [NULL | NOT NULL] [DEFAULT literal] [COMMENT literal]`.
fn parse_column(parser: &mut StatementParser<'_, '_>) -> Result<ColumnDefinition, ParseError> {
    let name = parser.parse_ident()?;
    let start = name.span.start();
    let data_type = parse_type_name(parser)?;
    let mut nullable = None;
    let mut aggregation = None;
    let mut default = None;
    let mut comment = None;
    let mut end = data_type.span.end();
    loop {
        if aggregation.is_none() && aggregation_word(parser) {
            let value = parser.parse_contextual_ident()?;
            end = value.span.end();
            aggregation = Some(value);
        } else if parser.consume_if_word("NOT") {
            let span = parser.consume_word("NULL")?;
            if nullable.replace(false).is_some() {
                return Err(parser.unexpected("one NULLability clause"));
            }
            end = span.end();
        } else if parser.consume_if_word("NULL") {
            if nullable.replace(true).is_some() {
                return Err(parser.unexpected("one NULLability clause"));
            }
        } else if parser.consume_if_word("DEFAULT") {
            if default.is_some() {
                return Err(parser.unexpected("one DEFAULT clause"));
            }
            let value = parser.parse_literal()?;
            end = value.span.end();
            default = Some(value);
        } else if parser.consume_if_word("COMMENT") {
            if comment.is_some() {
                return Err(parser.unexpected("one column COMMENT clause"));
            }
            let value = parser.parse_literal()?;
            end = value.span.end();
            comment = Some(value);
        } else {
            break;
        }
    }
    Ok(ColumnDefinition {
        name,
        data_type,
        nullable,
        aggregation,
        default,
        comment,
        span: Span::new(start, end),
    })
}

/// `type-name ::= object-name [ "(" number { "," number } ")" | "<" type-argument { "," type-argument } ">" ]`.
fn parse_type_name(parser: &mut StatementParser<'_, '_>) -> Result<TypeName, ParseError> {
    let first = parser.parse_contextual_ident()?;
    let start = first.span.start();
    let mut end = first.span.end();
    let mut parts = vec![first];
    while parser.consume_if_symbol(Symbol::Dot) {
        let part = parser.parse_contextual_ident()?;
        end = part.span.end();
        parts.push(part);
    }
    if parts.len() != 1 {
        return Err(parser.unexpected("unqualified type name"));
    }
    let name = parts.pop().expect("first type identifier");
    let mut arguments = vec![];
    let mut fields = None;
    if parser.consume_if_symbol(Symbol::LParen) {
        loop {
            let literal = parser.parse_literal()?;
            if !matches!(literal.kind, LiteralKind::Long { .. }) {
                return Err(parser.unexpected("numeric type parameter"));
            }
            arguments.push(super::numeric_type_parameter(literal)?);
            if parser.current_is_symbol(Symbol::RParen) {
                end = parser.consume_symbol(Symbol::RParen)?.end();
                break;
            }
            parser.consume_symbol(Symbol::Comma)?;
        }
    } else if parser.consume_if_symbol(Symbol::Lt) {
        let struct_type = name.value.eq_ignore_ascii_case("STRUCT");
        loop {
            if struct_type {
                let field_start = parser.current_span().start();
                let field_name = parser.parse_contextual_ident()?;
                let data_type = parse_type_name(parser)?;
                let field_end = data_type.span.end();
                fields
                    .get_or_insert_with(Vec::new)
                    .push(crate::ast::RowDataTypeField {
                        name: Some(field_name),
                        data_type,
                        span: Span::new(field_start, field_end),
                    });
            } else {
                let data_type = parse_type_name(parser)?;
                arguments.push(DataTypeParameter::Type(TypeParameter {
                    data_type: AstBox::new(data_type),
                }));
            }
            if parser.has_pending_type_gt() || !parser.consume_if_symbol(Symbol::Comma) {
                end = parser.consume_type_gt()?.end();
                break;
            }
        }
    }
    super::finish_type_name(name, arguments, fields, Span::new(start, end))
}

fn aggregation_word(parser: &StatementParser<'_, '_>) -> bool {
    [
        "SUM",
        "MIN",
        "MAX",
        "REPLACE",
        "REPLACE_IF_NOT_NULL",
        "BITMAP_UNION",
        "HLL_UNION",
    ]
    .into_iter()
    .any(|word| parser.current_is_word(word))
}
fn parse_key(
    parser: &mut StatementParser<'_, '_>,
    kind: TableKeyKind,
) -> Result<TableKey, ParseError> {
    let start = parser.current_span().start();
    parser.advance();
    parser.skip_trivia();
    parser.consume_word("KEY")?;
    let columns = ident_list(parser)?;
    let end = columns.last().map_or(start, |v| v.span.end());
    Ok(TableKey {
        kind,
        columns,
        span: Span::new(start, end),
    })
}

/// `DISTRIBUTED BY HASH "(" identifiers ")" [BUCKETS number]`.
fn parse_distribution(
    parser: &mut StatementParser<'_, '_>,
) -> Result<TableDistribution, ParseError> {
    let start = parser.consume_word("DISTRIBUTED")?.start();
    parser.consume_word("BY")?;
    if parser.consume_if_word("RANDOM") {
        return Ok(TableDistribution {
            columns: vec![],
            random: true,
            buckets: None,
            span: Span::new(start, parser.current_offset()),
        });
    }
    parser.consume_word("HASH")?;
    let columns = ident_list(parser)?;
    let mut end = columns.last().map_or(start, |v| v.span.end());
    let buckets = if parser.consume_if_word("BUCKETS") {
        let literal = parser.parse_literal()?;
        let LiteralKind::Long {
            spelling: value, ..
        } = &literal.kind
        else {
            return Err(parser.unexpected("BUCKETS number"));
        };
        let value = value
            .parse::<u64>()
            .map_err(|_| parser.unexpected("valid BUCKETS number"))?;
        if value == 0 {
            return Err(parser.unexpected("positive BUCKETS number"));
        }
        end = literal.span.end();
        Some(value)
    } else {
        None
    };
    Ok(TableDistribution {
        columns,
        random: false,
        buckets,
        span: Span::new(start, end),
    })
}

/// `PARTITION BY transform-list` or parser-owned legacy `RANGE` partition syntax.
fn parse_partition(parser: &mut StatementParser<'_, '_>) -> Result<TablePartition, ParseError> {
    let start = parser.consume_word("PARTITION")?.start();
    parser.consume_word("BY")?;
    if parser.current_is_word("RANGE") {
        parser.consume_word("RANGE")?;
        if parser.current_is_symbol(Symbol::LParen) {
            return legacy_range(parser, start);
        }
        return Err(parser.unexpected("'(' after RANGE"));
    }
    let expressions = if parser.consume_if_symbol(Symbol::LParen) {
        let mut values = vec![];
        loop {
            values.push(transform(parser)?);
            if parser.consume_if_symbol(Symbol::RParen) {
                break values;
            }
            parser.consume_symbol(Symbol::Comma)?;
        }
    } else {
        let mut values = vec![transform(parser)?];
        while parser.consume_if_symbol(Symbol::Comma) {
            values.push(transform(parser)?);
        }
        values
    };
    let end = transform_span(expressions.last().expect("nonempty transform")).end();
    Ok(TablePartition::Transform(TablePartitionTransform {
        expressions,
        span: Span::new(start, end),
    }))
}

fn legacy_range(
    parser: &mut StatementParser<'_, '_>,
    start: usize,
) -> Result<TablePartition, ParseError> {
    let columns = ident_list(parser)?;
    parser.consume_symbol(Symbol::LParen)?;
    let mut definitions = vec![];
    loop {
        parser.consume_word("PARTITION")?;
        let definition_start = parser.current_span().start();
        let name = parser.parse_ident()?;
        parser.consume_word("VALUES")?;
        let (values, end) = range_values(parser)?;
        definitions.push(LegacyRangePartitionDefinition {
            name,
            values,
            span: Span::new(definition_start, end),
        });
        if parser.consume_if_symbol(Symbol::RParen) {
            let end = definitions.last().expect("definition").span.end();
            return Ok(TablePartition::LegacyRange(LegacyRangePartition {
                columns,
                definitions,
                span: Span::new(start, end),
            }));
        }
        parser.consume_symbol(Symbol::Comma)?;
    }
}
/// `LESS THAN (literal, ...)` or `[ (literal, ...), (literal, ...) )`.
fn range_values(parser: &mut StatementParser<'_, '_>) -> Result<(String, usize), ParseError> {
    let start = parser.current_span().start();
    let end = if parser.consume_if_word("LESS") {
        parser.consume_word("THAN")?;
        range_tuple(parser)?
    } else {
        parser.consume_symbol(Symbol::LBracket)?;
        range_tuple(parser)?;
        parser.consume_symbol(Symbol::Comma)?;
        range_tuple(parser)?;
        parser.consume_symbol(Symbol::RParen)?.end()
    };
    Ok((parser.source_slice(Span::new(start, end)).to_owned(), end))
}
fn range_tuple(parser: &mut StatementParser<'_, '_>) -> Result<usize, ParseError> {
    parser.consume_symbol(Symbol::LParen)?;
    loop {
        if parser.current_is_word("MAXVALUE") {
            parser.consume_word("MAXVALUE")?;
        } else {
            parser.parse_literal()?;
        }
        if parser.consume_if_symbol(Symbol::RParen) {
            return Ok(parser.current_offset());
        }
        parser.consume_symbol(Symbol::Comma)?;
    }
}

/// `identifier | transform "(" identifier ["," positive-number] ")"`.
fn transform(parser: &mut StatementParser<'_, '_>) -> Result<PartitionTransform, ParseError> {
    let name = parser.parse_contextual_ident()?;
    let start = name.span.start();
    if !parser.consume_if_symbol(Symbol::LParen) {
        return Ok(PartitionTransform::Identity {
            column: name.clone(),
            span: name.span,
        });
    }
    let word = name.value.to_ascii_uppercase();
    let column = parser.parse_ident()?;
    if word == "BUCKET" || word == "TRUNCATE" {
        parser.consume_symbol(Symbol::Comma)?;
        let literal = parser.parse_literal()?;
        let LiteralKind::Long {
            spelling: value, ..
        } = literal.kind
        else {
            return Err(parser.unexpected("positive transform width"));
        };
        let value = value
            .parse::<u64>()
            .map_err(|_| parser.unexpected("valid transform width"))?;
        if value == 0 {
            return Err(parser.unexpected("positive transform width"));
        }
        let end = parser.consume_symbol(Symbol::RParen)?.end();
        return Ok(if word == "BUCKET" {
            PartitionTransform::Bucket {
                buckets: value,
                column,
                span: Span::new(start, end),
            }
        } else {
            PartitionTransform::Truncate {
                width: value,
                column,
                span: Span::new(start, end),
            }
        });
    }
    let end = parser.consume_symbol(Symbol::RParen)?.end();
    let span = Span::new(start, end);
    match word.as_str() {
        "IDENTITY" => Ok(PartitionTransform::Identity { column, span }),
        "YEAR" => Ok(PartitionTransform::Year { column, span }),
        "MONTH" => Ok(PartitionTransform::Month { column, span }),
        "DAY" => Ok(PartitionTransform::Day { column, span }),
        "HOUR" => Ok(PartitionTransform::Hour { column, span }),
        "VOID" => Ok(PartitionTransform::Void { column, span }),
        _ => Err(parser.unexpected("supported partition transform")),
    }
}

/// `PROPERTIES "(" property { "," property } ")"`.
fn parse_properties(
    parser: &mut StatementParser<'_, '_>,
) -> Result<Vec<TableProperty>, ParseError> {
    parser.advance();
    parser.skip_trivia();
    parser.consume_symbol(Symbol::LParen)?;
    let mut values = vec![];
    loop {
        let start = parser.current_span().start();
        let key = property_literal(parser)?;
        parser.consume_symbol(Symbol::Eq)?;
        let value = property_literal(parser)?;
        let end = value.span.end();
        values.push(TableProperty {
            key,
            value,
            span: Span::new(start, end),
        });
        if parser.consume_if_symbol(Symbol::RParen) {
            return Ok(values);
        }
        parser.consume_symbol(Symbol::Comma)?;
    }
}
fn property_literal(parser: &mut StatementParser<'_, '_>) -> Result<Literal, ParseError> {
    if parser.current_is_word("NULL")
        || parser.current_is_word("TRUE")
        || parser.current_is_word("FALSE")
        || parser.current().is_some_and(|v| {
            matches!(
                v.kind,
                crate::TokenKind::Number | crate::TokenKind::HexNumber | crate::TokenKind::String
            )
        })
    {
        return parser.parse_literal();
    }
    let value = parser.parse_contextual_ident()?;
    Ok(Literal {
        kind: LiteralKind::String(value.value),
        span: value.span,
    })
}
fn ident_list(parser: &mut StatementParser<'_, '_>) -> Result<Vec<crate::ast::Ident>, ParseError> {
    parser.consume_symbol(Symbol::LParen)?;
    let mut values = vec![];
    loop {
        values.push(parser.parse_ident()?);
        if parser.consume_if_symbol(Symbol::RParen) {
            return Ok(values);
        }
        parser.consume_symbol(Symbol::Comma)?;
    }
}
fn one<T>(
    slot: &mut Option<T>,
    value: T,
    parser: &StatementParser<'_, '_>,
) -> Result<(), ParseError> {
    if slot.replace(value).is_some() {
        return Err(parser.unexpected("one CREATE TABLE clause"));
    }
    Ok(())
}
fn transform_span(value: &PartitionTransform) -> Span {
    match value {
        PartitionTransform::Identity { span, .. }
        | PartitionTransform::Year { span, .. }
        | PartitionTransform::Month { span, .. }
        | PartitionTransform::Day { span, .. }
        | PartitionTransform::Hour { span, .. }
        | PartitionTransform::Bucket { span, .. }
        | PartitionTransform::Truncate { span, .. }
        | PartitionTransform::Void { span, .. } => *span,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ast::{DmlStatement, Statement, TablePartition, TableStatement},
        parse,
        printer::Printer,
    };

    fn one(sql: &str) -> Statement {
        parse(sql)
            .expect("statement must parse")
            .pop()
            .expect("one statement")
    }

    #[test]
    fn create_table_owns_ddl_and_ctas_shapes() {
        let statement = one(
            "CREATE EXTERNAL TABLE IF NOT EXISTS db.t (id BIGINT NOT NULL, tags ARRAY<STRUCT<k INT, v VARCHAR(8)>> NULL COMMENT 'tags') DUPLICATE KEY (id) PARTITION BY (MONTH(id), BUCKET(id, 8)) DISTRIBUTED BY HASH (id) BUCKETS 3 COMMENT 'table' PROPERTIES ('format-version' = '3')",
        );
        let Statement::Table(TableStatement::Create(table)) = statement else {
            panic!("table statement")
        };
        assert!(table.external && table.if_not_exists);
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.distribution.and_then(|value| value.buckets), Some(3));
        assert!(matches!(
            table.partition,
            Some(TablePartition::Transform(_))
        ));

        assert!(matches!(
            one("CREATE TABLE dst LIKE src"),
            Statement::Table(_)
        ));
        let Statement::Dml(DmlStatement::CreateTableAsSelect(ctas)) =
            one("CREATE TEMPORARY TABLE IF NOT EXISTS dst AS SELECT 1 AS id")
        else {
            panic!("ctas")
        };
        assert_eq!(crate::printer::print_query(&ctas.query), "SELECT 1 AS id");
    }

    #[test]
    fn legacy_range_and_printer_roundtrip() {
        let statement = one(
            "CREATE TABLE t (d DATE) PARTITION BY RANGE (d) (PARTITION p1 VALUES [('2024-01-01'), ('2024-02-01')))",
        );
        let rendered = Printer::new().statement(&statement);
        assert_eq!(one(&rendered), statement);
    }

    #[test]
    fn nested_type_closers_preserve_following_column_separators() {
        let statement = one(
            "CREATE TABLE t (a ARRAY<ARRAY<INT>>, b ARRAY<MAP<STRING, INT>>, c ARRAY<STRUCT<f1 INT, f2 STRING>>)",
        );
        let Statement::Table(TableStatement::Create(table)) = statement else {
            panic!("table statement");
        };
        assert_eq!(table.columns.len(), 3);
    }

    #[test]
    fn malformed_owned_productions_do_not_fall_through() {
        for sql in [
            "CREATE TABLE t (id INT DEFAULT)",
            "CREATE TABLE t (id INT) DISTRIBUTED BY HASH ()",
            "CREATE TABLE t (id INT) PARTITION BY BUCKET(id, 0)",
            "CREATE TABLE t (id INT) PROPERTIES ('a' 'b')",
        ] {
            assert!(parse(sql).is_err(), "{sql}");
        }
    }
}
