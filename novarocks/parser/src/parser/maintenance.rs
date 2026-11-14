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

//! Procedure and table-maintenance grammar.

use std::collections::BTreeSet;

use crate::{
    ParseError, Span, TokenKind,
    ast::{
        MaintenanceStatement, Statement,
        maintenance::{
            CallStatement, ExpireSnapshots, ExpireSnapshotsOption, MaintenanceValue, OptimizeTable,
            ProcedureArgument, ProcedureArgumentMode, ProcedureMap, ProcedureMapEntry,
            RemoveOrphanFiles, RewriteManifests,
        },
    },
    token::Symbol,
};

use super::StatementParser;

pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if parser.current_is_word("CALL") {
        return parse_call(parser).map(|statement| Some(Statement::Maintenance(statement)));
    }
    if parser.current_is_word("ALTER") && parser.peek_word(1, "TABLE") {
        return parse_alter_table(parser).map(|statement| Some(Statement::Maintenance(statement)));
    }
    Ok(None)
}

fn parse_call(parser: &mut StatementParser<'_, '_>) -> Result<MaintenanceStatement, ParseError> {
    let start = parser.consume_word("CALL")?.start();
    let procedure = parser.parse_object_name()?;
    parser.consume_symbol(Symbol::LParen)?;

    let mut arguments = Vec::new();
    let end = if parser.current_is_symbol(Symbol::RParen) {
        parser.consume_symbol(Symbol::RParen)?.end()
    } else {
        loop {
            arguments.push(parse_procedure_argument(parser)?);
            if parser.consume_if_symbol(Symbol::Comma) {
                if parser.current_is_symbol(Symbol::RParen) {
                    return Err(parser.unexpected("procedure argument"));
                }
                continue;
            }
            break parser.consume_symbol(Symbol::RParen)?.end();
        }
    };

    let argument_mode = classify_argument_mode(&arguments, parser)?;
    Ok(MaintenanceStatement::Call(CallStatement {
        procedure,
        arguments,
        argument_mode,
        span: Span::new(start, end),
    }))
}

fn parse_procedure_argument(
    parser: &mut StatementParser<'_, '_>,
) -> Result<ProcedureArgument, ParseError> {
    let start = parser.current_offset();
    let name = if named_argument_ahead(parser) {
        let name = parser.parse_contextual_ident()?;
        if !parser.consume_if_symbol(Symbol::FatArrow) {
            parser.consume_symbol(Symbol::Eq)?;
            parser.consume_symbol(Symbol::Gt)?;
        }
        Some(name)
    } else {
        None
    };
    let value = parse_value(parser)?;
    Ok(ProcedureArgument {
        name,
        span: Span::new(start, value.span().end()),
        value,
    })
}

fn named_argument_ahead(parser: &StatementParser<'_, '_>) -> bool {
    let mut tokens = parser
        .tokens
        .iter()
        .skip(parser.position)
        .filter(|token| !matches!(token.kind, TokenKind::Trivia(_)));
    let first = tokens.next();
    let second = tokens.next();
    let third = tokens.next();
    matches!(
        (first, second, third),
        (
            Some(first),
            Some(crate::Token {
                kind: TokenKind::Symbol(Symbol::Eq),
                ..
            }),
            Some(crate::Token {
                kind: TokenKind::Symbol(Symbol::Gt),
                ..
            }),
        ) if matches!(first.kind, TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_))
    ) || matches!(
        (first, second),
        (
            Some(first),
            Some(crate::Token { kind: TokenKind::Symbol(Symbol::FatArrow), .. }),
        ) if matches!(first.kind, TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_))
    )
}

fn parse_value(parser: &mut StatementParser<'_, '_>) -> Result<MaintenanceValue, ParseError> {
    if parser.current_is_word("TIMESTAMP") {
        let start = parser.consume_word("TIMESTAMP")?.start();
        let value = parser.parse_literal()?;
        return Ok(MaintenanceValue::Timestamp {
            span: Span::new(start, value.span.end()),
            value,
        });
    }
    if parser.current_is_word("MAP") {
        return parse_map(parser).map(MaintenanceValue::Map);
    }
    parser.parse_literal().map(MaintenanceValue::Literal)
}

fn parse_map(parser: &mut StatementParser<'_, '_>) -> Result<ProcedureMap, ParseError> {
    let start = parser.consume_word("MAP")?.start();
    parser.consume_symbol(Symbol::LParen)?;
    let mut entries = Vec::new();
    let end = if parser.current_is_symbol(Symbol::RParen) {
        parser.consume_symbol(Symbol::RParen)?.end()
    } else {
        loop {
            let key = parser.parse_literal()?;
            parser.consume_symbol(Symbol::Comma)?;
            let value = parser.parse_literal()?;
            let entry_span = Span::new(key.span.start(), value.span.end());
            entries.push(ProcedureMapEntry {
                key,
                value,
                span: entry_span,
            });
            if parser.consume_if_symbol(Symbol::Comma) {
                if parser.current_is_symbol(Symbol::RParen) {
                    return Err(parser.unexpected("map key"));
                }
                continue;
            }
            break parser.consume_symbol(Symbol::RParen)?.end();
        }
    };
    Ok(ProcedureMap {
        entries,
        span: Span::new(start, end),
    })
}

fn classify_argument_mode(
    arguments: &[ProcedureArgument],
    parser: &StatementParser<'_, '_>,
) -> Result<ProcedureArgumentMode, ParseError> {
    let has_named = arguments.iter().any(|argument| argument.name.is_some());
    let has_positional = arguments.iter().any(|argument| argument.name.is_none());
    if has_named && has_positional {
        return Err(parser.unexpected("only named or only positional procedure arguments"));
    }

    let mut seen = BTreeSet::new();
    for name in arguments
        .iter()
        .filter_map(|argument| argument.name.as_ref())
    {
        if !seen.insert(name.value.to_ascii_lowercase()) {
            return Err(ParseError::UnexpectedToken {
                expected: "unique procedure argument name",
                found: format!("`{}`", name.value),
                span: name.span,
            });
        }
    }

    Ok(match (has_named, has_positional) {
        (false, false) => ProcedureArgumentMode::Empty,
        (true, false) => ProcedureArgumentMode::Named,
        (false, true) => ProcedureArgumentMode::Positional,
        (true, true) => unreachable!("mixed procedure arguments returned above"),
    })
}

fn parse_alter_table(
    parser: &mut StatementParser<'_, '_>,
) -> Result<MaintenanceStatement, ParseError> {
    let start = parser.consume_word("ALTER")?.start();
    parser.consume_word("TABLE")?;
    let table = parser.parse_object_name()?;

    if parser.current_is_word("OPTIMIZE") {
        let end = parser.consume_word("OPTIMIZE")?.end();
        return Ok(MaintenanceStatement::Optimize(OptimizeTable {
            table,
            span: Span::new(start, end),
        }));
    }
    if parser.consume_if_word("REWRITE") {
        let end = parser.consume_word("MANIFESTS")?.end();
        if !parser.current_is_symbol(Symbol::Semicolon) && !parser.is_end() {
            return Err(parser.unexpected("REWRITE MANIFESTS without unsupported trailing clauses"));
        }
        return Ok(MaintenanceStatement::RewriteManifests(RewriteManifests {
            table,
            span: Span::new(start, end),
        }));
    }
    if parser.consume_if_word("EXPIRE") {
        parser.consume_word("SNAPSHOTS")?;
        return parse_expire_snapshots(parser, start, table);
    }
    if parser.consume_if_word("REMOVE") {
        parser.consume_word("ORPHAN")?;
        parser.consume_word("FILES")?;
        if !parser.current_is_word("OLDER") {
            return Err(parser.unexpected("REMOVE ORPHAN FILES requires OLDER THAN"));
        }
        parser.consume_word("OLDER")?;
        parser.consume_word("THAN")?;
        let older_than = parse_value(parser)?;
        if !parser.current_is_symbol(Symbol::Semicolon) && !parser.is_end() {
            return Err(
                parser.unexpected("REMOVE ORPHAN FILES without unsupported trailing clauses")
            );
        }
        return Ok(MaintenanceStatement::RemoveOrphanFiles(RemoveOrphanFiles {
            table,
            span: Span::new(start, older_than.span().end()),
            older_than,
        }));
    }
    Err(parser.unexpected("OPTIMIZE, REWRITE MANIFESTS, EXPIRE SNAPSHOTS, or REMOVE ORPHAN FILES"))
}

fn parse_expire_snapshots(
    parser: &mut StatementParser<'_, '_>,
    start: usize,
    table: crate::ast::ObjectName,
) -> Result<MaintenanceStatement, ParseError> {
    let mut options = Vec::new();
    let mut older_than = false;
    let mut retain_last = false;
    while parser.current_is_word("OLDER") || parser.current_is_word("RETAIN") {
        if parser.current_is_word("OLDER") {
            if older_than {
                return Err(parser.unexpected("duplicate OLDER THAN clause"));
            }
            let option_start = parser.consume_word("OLDER")?.start();
            parser.consume_word("THAN")?;
            let value = parse_value(parser)?;
            options.push(ExpireSnapshotsOption::OlderThan {
                span: Span::new(option_start, value.span().end()),
                value,
            });
            older_than = true;
        } else {
            if retain_last {
                return Err(parser.unexpected("duplicate RETAIN LAST clause"));
            }
            let option_start = parser.consume_word("RETAIN")?.start();
            parser.consume_word("LAST")?;
            let value = parse_value(parser)?;
            options.push(ExpireSnapshotsOption::RetainLast {
                span: Span::new(option_start, value.span().end()),
                value,
            });
            retain_last = true;
        }
    }
    if options.is_empty() {
        return Err(parser.unexpected(
            "EXPIRE SNAPSHOTS requires at least one OLDER THAN or RETAIN LAST clause",
        ));
    }
    if !parser.current_is_symbol(Symbol::Semicolon) && !parser.is_end() {
        return Err(parser.unexpected("EXPIRE SNAPSHOTS without unsupported trailing clauses"));
    }
    let end = options.last().expect("non-empty options").span().end();
    Ok(MaintenanceStatement::ExpireSnapshots(ExpireSnapshots {
        table,
        options,
        span: Span::new(start, end),
    }))
}
