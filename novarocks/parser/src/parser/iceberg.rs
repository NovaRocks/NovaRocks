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

//! Iceberg table DDL grammar.

use crate::{
    ParseError, Span, TokenKind,
    ast::iceberg::{
        AddFiles, AlterIcebergTable, ColumnPath, ColumnPosition, IcebergColumnAction,
        IcebergPartitionChange, IcebergPartitionField, IcebergPropertiesAction,
        IcebergReferenceAction, IcebergReferenceKind, IcebergSchemaChange, IcebergStatement,
        IcebergTableAction, RawReferenceOptions, ReferenceAnchor,
    },
    ast::{
        Ident, Literal, LiteralKind, Property, PropertyKeyValue, Statement, StructField, TypeName,
        TypeNameArgument,
    },
    token::Symbol,
};

use super::StatementParser;

/// Parses the Iceberg `ALTER TABLE` command family.
///
/// Unrelated `ALTER TABLE` forms, including equality deletes, are not claimed
/// here: their legacy frontier remains intact until a separately scoped cut.
pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if !looks_like_iceberg_command(parser) {
        return Ok(None);
    }

    let start = parser.consume_word("ALTER")?.start();
    parser.consume_word("TABLE")?;
    let table = parser.parse_object_name()?;
    let action = if parser.consume_if_word("ADD") {
        parse_add_action(parser)?
    } else if parser.consume_if_word("DROP") {
        parse_drop_action(parser)?
    } else if parser.consume_if_word("RENAME") {
        parser.consume_word("COLUMN")?;
        let from = parse_column_path(parser)?;
        parser.consume_word("TO")?;
        let to = parse_column_path(parser)?;
        IcebergTableAction::Schema(IcebergSchemaChange::RenameColumn { from, to })
    } else if parser.consume_if_word("MODIFY") {
        parser.consume_word("COLUMN")?;
        let path = parse_column_path(parser)?;
        let data_type = parse_type_name(parser)?;
        IcebergTableAction::Schema(IcebergSchemaChange::ModifyColumn { path, data_type })
    } else if parser.consume_if_word("ALTER") {
        parser.consume_word("COLUMN")?;
        let path = parse_column_path(parser)?;
        IcebergTableAction::Schema(IcebergSchemaChange::AlterColumn {
            path,
            action: parse_alter_column_action(parser)?,
        })
    } else if parser.consume_if_word("SET") {
        IcebergTableAction::Properties(parse_set_properties(parser)?)
    } else if parser.consume_if_word("UNSET") {
        IcebergTableAction::Properties(parse_unset_properties(parser)?)
    } else if parser.consume_if_word("COMMENT") {
        IcebergTableAction::Properties(IcebergPropertiesAction::Comment {
            value: parse_string_literal(parser)?,
        })
    } else if parser.consume_if_word("CREATE") {
        IcebergTableAction::Reference(parse_create_reference(parser)?)
    } else {
        IcebergTableAction::Reference(parse_drop_reference(parser)?)
    };

    let span = Span::new(start, parser.current_offset());
    Ok(Some(Statement::Iceberg(IcebergStatement::AlterTable(
        AlterIcebergTable {
            table,
            action,
            span,
        },
    ))))
}

fn parse_add_action(
    parser: &mut StatementParser<'_, '_>,
) -> Result<IcebergTableAction, ParseError> {
    if parser.consume_if_word("COLUMN") {
        return Ok(IcebergTableAction::Schema(parse_add_column(parser)?));
    }
    if parser.consume_if_word("PARTITION") {
        parser.consume_word("COLUMN")?;
        return Ok(IcebergTableAction::Partition(IcebergPartitionChange::Add {
            field: parse_partition_field(parser)?,
        }));
    }
    parser.consume_word("FILES")?;
    parser.consume_word("FROM")?;
    let location = parse_string_literal(parser)?;
    Ok(IcebergTableAction::AddFiles(AddFiles {
        span: location.span,
        location,
    }))
}

fn parse_drop_action(
    parser: &mut StatementParser<'_, '_>,
) -> Result<IcebergTableAction, ParseError> {
    if parser.consume_if_word("COLUMN") {
        return Ok(IcebergTableAction::Schema(
            IcebergSchemaChange::DropColumn {
                path: parse_column_path(parser)?,
            },
        ));
    }
    if parser.consume_if_word("PARTITION") {
        parser.consume_word("COLUMN")?;
        return Ok(IcebergTableAction::Partition(
            IcebergPartitionChange::Drop {
                field: parse_partition_field(parser)?,
            },
        ));
    }
    Ok(IcebergTableAction::Reference(
        parse_drop_reference_after_drop(parser)?,
    ))
}

fn parse_add_column(
    parser: &mut StatementParser<'_, '_>,
) -> Result<IcebergSchemaChange, ParseError> {
    let path = parse_column_path(parser)?;
    let data_type = parse_type_name(parser)?;
    let mut nullable = None;
    let mut default = None;
    let mut position = ColumnPosition::Default;

    loop {
        if parser.consume_if_word("NULL") {
            if nullable.replace(true).is_some() {
                return Err(parser.unexpected("one NULLability clause"));
            }
        } else if parser.consume_if_word("NOT") {
            parser.consume_word("NULL")?;
            if nullable.replace(false).is_some() {
                return Err(parser.unexpected("one NULLability clause"));
            }
        } else if parser.consume_if_word("DEFAULT") {
            if default.is_some() {
                return Err(parser.unexpected("one DEFAULT clause"));
            }
            default = Some(parse_default_literal(parser)?);
        } else if parser.consume_if_word("FIRST") {
            if !matches!(position, ColumnPosition::Default) {
                return Err(parser.unexpected("one column position clause"));
            }
            position = ColumnPosition::First;
        } else if parser.consume_if_word("AFTER") {
            if !matches!(position, ColumnPosition::Default) {
                return Err(parser.unexpected("one column position clause"));
            }
            position = ColumnPosition::After(parse_column_path(parser)?);
        } else if parser.consume_if_word("BEFORE") {
            if !matches!(position, ColumnPosition::Default) {
                return Err(parser.unexpected("one column position clause"));
            }
            position = ColumnPosition::Before(parse_column_path(parser)?);
        } else {
            break;
        }
    }

    Ok(IcebergSchemaChange::AddColumn {
        path,
        data_type,
        nullable,
        default,
        position,
    })
}

fn parse_default_literal(parser: &mut StatementParser<'_, '_>) -> Result<Literal, ParseError> {
    if parser.current_is_symbol(Symbol::Minus) {
        let start = parser.current_span().start();
        parser.consume_symbol(Symbol::Minus)?;
        if !matches!(
            parser.current().map(|token| &token.kind),
            Some(TokenKind::Number)
        ) {
            let literal = parser.parse_literal()?;
            return Err(ParseError::UnexpectedToken {
                expected: "numeric literal after `-` in DEFAULT",
                found: "non-numeric literal".to_string(),
                span: literal.span,
            });
        }
        let number_span = parser.current_span();
        let span = Span::new(start, number_span.end());
        let kind =
            super::number_literal_kind(format!("-{}", parser.source_slice(number_span)), span)?;
        parser.advance();
        parser.skip_trivia();
        return Ok(Literal { kind, span });
    }
    parser.parse_literal()
}

fn parse_alter_column_action(
    parser: &mut StatementParser<'_, '_>,
) -> Result<IcebergColumnAction, ParseError> {
    if parser.consume_if_word("FIRST") {
        return Ok(IcebergColumnAction::Reorder(ColumnPosition::First));
    }
    if parser.consume_if_word("AFTER") {
        return Ok(IcebergColumnAction::Reorder(ColumnPosition::After(
            parse_column_path(parser)?,
        )));
    }
    if parser.consume_if_word("BEFORE") {
        return Ok(IcebergColumnAction::Reorder(ColumnPosition::Before(
            parse_column_path(parser)?,
        )));
    }
    if parser.consume_if_word("SET") {
        parser.consume_word("NOT")?;
        parser.consume_word("NULL")?;
        return Ok(IcebergColumnAction::SetNullable(false));
    }
    if parser.consume_if_word("DROP") {
        parser.consume_word("NOT")?;
        parser.consume_word("NULL")?;
        return Ok(IcebergColumnAction::SetNullable(true));
    }
    parser.consume_word("COMMENT")?;
    Ok(IcebergColumnAction::Comment(parse_string_literal(parser)?))
}

fn parse_set_properties(
    parser: &mut StatementParser<'_, '_>,
) -> Result<IcebergPropertiesAction, ParseError> {
    parser.consume_if_word("TBLPROPERTIES");
    parser.consume_symbol(Symbol::LParen)?;
    let mut entries = Vec::new();
    loop {
        let key = parse_property_key(parser)?;
        let start = key.span.start();
        parser.consume_symbol(Symbol::Eq)?;
        let value = parser.parse_literal()?;
        entries.push(PropertyKeyValue {
            key,
            span: Span::new(start, value.span.end()),
            value,
        });
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    parser.consume_symbol(Symbol::RParen)?;
    Ok(IcebergPropertiesAction::Set { entries })
}

fn parse_unset_properties(
    parser: &mut StatementParser<'_, '_>,
) -> Result<IcebergPropertiesAction, ParseError> {
    parser.consume_word("TBLPROPERTIES")?;
    let if_exists = parser.consume_if_word("IF");
    if if_exists {
        parser.consume_word("EXISTS")?;
    }
    parser.consume_symbol(Symbol::LParen)?;
    let mut keys = Vec::new();
    loop {
        let key = parse_property_key(parser)?;
        keys.push(Property {
            span: key.span,
            key,
        });
        if !parser.consume_if_symbol(Symbol::Comma) {
            break;
        }
    }
    parser.consume_symbol(Symbol::RParen)?;
    Ok(IcebergPropertiesAction::Unset { keys, if_exists })
}

fn parse_property_key(parser: &mut StatementParser<'_, '_>) -> Result<Ident, ParseError> {
    let literal = parse_string_literal(parser)?;
    let LiteralKind::String(value) = literal.kind else {
        unreachable!("parse_string_literal guards this variant")
    };
    Ok(Ident {
        value,
        quoted: false,
        quote_style: None,
        span: literal.span,
    })
}

fn parse_partition_field(
    parser: &mut StatementParser<'_, '_>,
) -> Result<IcebergPartitionField, ParseError> {
    let start = parser.current_offset();
    let transform = parse_transform_ident(parser)?;
    if !parser.consume_if_symbol(Symbol::LParen) {
        return Ok(IcebergPartitionField::Identity {
            column: ColumnPath {
                span: transform.span,
                parts: vec![transform],
            },
            span: Span::new(start, parser.current_offset()),
        });
    }
    let column = parse_column_path(parser)?;
    if transform.value.eq_ignore_ascii_case("identity") {
        parser.consume_symbol(Symbol::RParen)?;
        return Ok(IcebergPartitionField::Identity {
            column,
            span: Span::new(start, parser.current_offset()),
        });
    }
    if transform.value.eq_ignore_ascii_case("year") {
        parser.consume_symbol(Symbol::RParen)?;
        return Ok(IcebergPartitionField::Year {
            column,
            span: Span::new(start, parser.current_offset()),
        });
    }
    if transform.value.eq_ignore_ascii_case("month") {
        parser.consume_symbol(Symbol::RParen)?;
        return Ok(IcebergPartitionField::Month {
            column,
            span: Span::new(start, parser.current_offset()),
        });
    }
    if transform.value.eq_ignore_ascii_case("day") {
        parser.consume_symbol(Symbol::RParen)?;
        return Ok(IcebergPartitionField::Day {
            column,
            span: Span::new(start, parser.current_offset()),
        });
    }
    if transform.value.eq_ignore_ascii_case("hour") {
        parser.consume_symbol(Symbol::RParen)?;
        return Ok(IcebergPartitionField::Hour {
            column,
            span: Span::new(start, parser.current_offset()),
        });
    }
    if transform.value.eq_ignore_ascii_case("void") {
        parser.consume_symbol(Symbol::RParen)?;
        return Ok(IcebergPartitionField::Void {
            column,
            span: Span::new(start, parser.current_offset()),
        });
    }
    if transform.value.eq_ignore_ascii_case("bucket") {
        parser.consume_symbol(Symbol::Comma)?;
        let buckets = parser.parse_literal()?;
        parser.consume_symbol(Symbol::RParen)?;
        return Ok(IcebergPartitionField::Bucket {
            column,
            buckets,
            span: Span::new(start, parser.current_offset()),
        });
    }
    if transform.value.eq_ignore_ascii_case("truncate") {
        parser.consume_symbol(Symbol::Comma)?;
        let width = parser.parse_literal()?;
        parser.consume_symbol(Symbol::RParen)?;
        return Ok(IcebergPartitionField::Truncate {
            column,
            width,
            span: Span::new(start, parser.current_offset()),
        });
    }
    Err(ParseError::UnexpectedToken {
        expected: "supported Iceberg partition transform",
        found: format!("`{}`", transform.value),
        span: transform.span,
    })
}

fn parse_create_reference(
    parser: &mut StatementParser<'_, '_>,
) -> Result<IcebergReferenceAction, ParseError> {
    let or_replace = if parser.consume_if_word("OR") {
        parser.consume_word("REPLACE")?;
        true
    } else {
        false
    };
    let kind = parse_reference_kind(parser)?;
    let if_not_exists = if parser.consume_if_word("IF") {
        parser.consume_word("NOT")?;
        parser.consume_word("EXISTS")?;
        true
    } else {
        false
    };
    let name = parser.parse_ident()?;
    let anchor = if parser.consume_if_word("AS") {
        parser.consume_word("OF")?;
        parser.consume_word("VERSION")?;
        ReferenceAnchor::Version(parser.parse_literal()?)
    } else {
        ReferenceAnchor::CurrentMain
    };
    let options = parse_reference_options(parser);
    Ok(IcebergReferenceAction::Create {
        kind,
        name,
        if_not_exists,
        or_replace,
        anchor,
        options,
    })
}

fn parse_drop_reference(
    parser: &mut StatementParser<'_, '_>,
) -> Result<IcebergReferenceAction, ParseError> {
    parser.consume_word("DROP")?;
    parse_drop_reference_after_drop(parser)
}

fn parse_drop_reference_after_drop(
    parser: &mut StatementParser<'_, '_>,
) -> Result<IcebergReferenceAction, ParseError> {
    let kind = parse_reference_kind(parser)?;
    let if_exists = if parser.consume_if_word("IF") {
        parser.consume_word("EXISTS")?;
        true
    } else {
        false
    };
    let name = parser.parse_ident()?;
    Ok(IcebergReferenceAction::Drop {
        kind,
        name,
        if_exists,
    })
}

fn parse_reference_kind(
    parser: &mut StatementParser<'_, '_>,
) -> Result<IcebergReferenceKind, ParseError> {
    if parser.consume_if_word("BRANCH") {
        Ok(IcebergReferenceKind::Branch)
    } else {
        parser.consume_word("TAG")?;
        Ok(IcebergReferenceKind::Tag)
    }
}

fn parse_reference_options(parser: &mut StatementParser<'_, '_>) -> Option<RawReferenceOptions> {
    if parser.current_is_symbol(Symbol::Semicolon)
        || parser
            .current()
            .is_none_or(|token| matches!(token.kind, TokenKind::End))
    {
        return None;
    }
    let start = parser.current_offset();
    let mut end = start;
    while !parser.current_is_symbol(Symbol::Semicolon)
        && parser
            .current()
            .is_some_and(|token| !matches!(token.kind, TokenKind::End))
    {
        end = parser.current_span().end();
        parser.advance();
    }
    let span = Span::new(start, end);
    Some(RawReferenceOptions {
        text: parser.source_slice(span).trim().to_owned(),
        span,
    })
}

fn parse_column_path(parser: &mut StatementParser<'_, '_>) -> Result<ColumnPath, ParseError> {
    let name = parser.parse_object_name()?;
    Ok(ColumnPath {
        parts: name.parts,
        span: name.span,
    })
}

fn parse_type_name(parser: &mut StatementParser<'_, '_>) -> Result<TypeName, ParseError> {
    let name = parser.parse_object_name()?;
    let type_word = name
        .parts
        .last()
        .expect("object names always contain one part")
        .value
        .to_ascii_uppercase();
    let mut arguments = Vec::new();
    let end = if matches!(type_word.as_str(), "ARRAY" | "MAP" | "STRUCT")
        && parser.consume_if_symbol(Symbol::Lt)
    {
        if type_word == "STRUCT" {
            loop {
                let field_name = parser.parse_ident()?;
                let data_type = parse_type_name(parser)?;
                let field_span = Span::new(field_name.span.start(), data_type.span.end());
                arguments.push(TypeNameArgument::Field(StructField {
                    name: field_name,
                    data_type,
                    span: field_span,
                }));
                if parser.consume_if_symbol(Symbol::Comma) {
                    continue;
                }
                break parser.consume_type_gt()?;
            }
        } else {
            loop {
                arguments.push(TypeNameArgument::Type(parse_type_name(parser)?));
                if parser.consume_if_symbol(Symbol::Comma) {
                    continue;
                }
                break parser.consume_type_gt()?;
            }
        }
    } else if parser.consume_if_symbol(Symbol::LParen) {
        loop {
            arguments.push(TypeNameArgument::Literal(parser.parse_literal()?));
            if parser.consume_if_symbol(Symbol::Comma) {
                continue;
            }
            break parser.consume_symbol(Symbol::RParen)?;
        }
    } else {
        name.span
    };
    Ok(TypeName {
        span: Span::new(name.span.start(), end.end()),
        name,
        arguments,
        argument_separator_spaces: Vec::new(),
    })
}

/// Partition-transform names are grammar words, so a keyword such as
/// `TRUNCATE` is valid here even though it is not a general identifier.
fn parse_transform_ident(parser: &mut StatementParser<'_, '_>) -> Result<Ident, ParseError> {
    let token = parser
        .current()
        .ok_or_else(|| parser.unexpected("partition transform"))?;
    if !matches!(
        token.kind,
        TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
    ) {
        return Err(parser.unexpected("partition transform"));
    }
    let source = parser.source_slice(token.span);
    let (value, quoted, quote_style) = if matches!(token.kind, TokenKind::QuotedIdent) {
        (
            source[1..source.len() - 1].replace("``", "`"),
            true,
            Some('`'),
        )
    } else {
        (source.to_owned(), false, None)
    };
    let ident = Ident {
        value,
        quoted,
        quote_style,
        span: token.span,
    };
    parser.advance();
    parser.skip_trivia();
    Ok(ident)
}

fn parse_string_literal(parser: &mut StatementParser<'_, '_>) -> Result<Literal, ParseError> {
    let literal = parser.parse_literal()?;
    if matches!(literal.kind, LiteralKind::String(_)) {
        Ok(literal)
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "string literal",
            found: format!("`{}`", parser.source_slice(literal.span)),
            span: literal.span,
        })
    }
}

fn looks_like_iceberg_command(parser: &StatementParser<'_, '_>) -> bool {
    let mut index = next_significant(parser, parser.position);
    if !word_at(parser, index, "ALTER") {
        return false;
    }
    index = next_significant(parser, index + 1);
    if !word_at(parser, index, "TABLE") {
        return false;
    }
    index = next_significant(parser, index + 1);
    if !identifier_at(parser, index) {
        return false;
    }
    index = next_significant(parser, index + 1);
    while symbol_at(parser, index, Symbol::Dot) {
        index = next_significant(parser, index + 1);
        if !identifier_at(parser, index) {
            return false;
        }
        index = next_significant(parser, index + 1);
    }
    if word_at(parser, index, "ADD") {
        let next = next_significant(parser, index + 1);
        return word_at(parser, next, "COLUMN")
            || word_at(parser, next, "PARTITION")
            || word_at(parser, next, "FILES");
    }
    if word_at(parser, index, "DROP") {
        let next = next_significant(parser, index + 1);
        return word_at(parser, next, "COLUMN")
            || word_at(parser, next, "PARTITION")
            || word_at(parser, next, "BRANCH")
            || word_at(parser, next, "TAG");
    }
    if word_at(parser, index, "RENAME")
        || word_at(parser, index, "MODIFY")
        || word_at(parser, index, "ALTER")
    {
        return word_at(parser, next_significant(parser, index + 1), "COLUMN");
    }
    if word_at(parser, index, "SET")
        || word_at(parser, index, "UNSET")
        || word_at(parser, index, "COMMENT")
    {
        return true;
    }
    if word_at(parser, index, "CREATE") {
        let mut next = next_significant(parser, index + 1);
        if word_at(parser, next, "OR") {
            next = next_significant(parser, next + 1);
            if !word_at(parser, next, "REPLACE") {
                return false;
            }
            next = next_significant(parser, next + 1);
        }
        return word_at(parser, next, "BRANCH") || word_at(parser, next, "TAG");
    }
    false
}

fn next_significant(parser: &StatementParser<'_, '_>, mut index: usize) -> usize {
    while parser
        .tokens
        .get(index)
        .is_some_and(|token| matches!(token.kind, TokenKind::Trivia(_)))
    {
        index += 1;
    }
    index
}

fn word_at(parser: &StatementParser<'_, '_>, index: usize, word: &str) -> bool {
    parser.tokens.get(index).is_some_and(|token| {
        matches!(
            token.kind,
            TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
        ) && parser.source[token.span.start()..token.span.end()].eq_ignore_ascii_case(word)
    })
}

fn identifier_at(parser: &StatementParser<'_, '_>, index: usize) -> bool {
    parser
        .tokens
        .get(index)
        .is_some_and(|token| matches!(token.kind, TokenKind::Ident | TokenKind::QuotedIdent))
}

fn symbol_at(parser: &StatementParser<'_, '_>, index: usize, expected: Symbol) -> bool {
    parser
        .tokens
        .get(index)
        .is_some_and(|token| matches!(token.kind, TokenKind::Symbol(found) if found == expected))
}
