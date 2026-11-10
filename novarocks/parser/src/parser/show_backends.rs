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

use crate::{
    ParseError,
    ast::{ShowBackends, Statement},
    token::Keyword,
};

use super::StatementParser;

/// `show-backends ::= SHOW BACKENDS`
pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if !parser.current_is_word("SHOW") {
        return Ok(None);
    }
    if !parser.peek_word(1, "BACKENDS")
        && ["ANALYZE", "TABLE", "CREATE", "ALTER", "MATERIALIZED"]
            .iter()
            .any(|word| parser.peek_word(1, word))
    {
        return Ok(None);
    }
    let start = parser.current_span().start();
    parser.advance(); // SHOW
    skip_trivia(parser);
    if !parser.current_is_keyword(Keyword::Backends) {
        return Err(parser.unexpected("BACKENDS"));
    }
    let end = parser.current_span().end();
    parser.advance();
    Ok(Some(Statement::ShowBackends(ShowBackends {
        span: crate::Span::new(start, end),
    })))
}

fn skip_trivia(parser: &mut StatementParser<'_, '_>) {
    while matches!(
        parser.current().map(|token| &token.kind),
        Some(crate::TokenKind::Trivia(_))
    ) {
        parser.advance();
    }
}
