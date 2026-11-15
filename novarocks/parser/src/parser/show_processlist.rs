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

//! `SHOW [FULL] PROCESSLIST`.

use crate::{
    ParseError,
    ast::{ShowProcessList, Statement},
};

use super::StatementParser;

/// `show-processlist ::= SHOW [ FULL ] PROCESSLIST`
///
/// `PROCESSLIST` and `FULL` are matched as words rather than promoted to
/// keywords: reserving them would take two ordinary identifiers away from
/// every other statement for the sake of this one.
pub(super) fn parse(parser: &mut StatementParser<'_, '_>) -> Result<Option<Statement>, ParseError> {
    if !parser.current_is_word("SHOW") {
        return Ok(None);
    }
    let full = parser.peek_word(1, "FULL");
    let subject = if full { 2 } else { 1 };
    if !parser.peek_word(subject, "PROCESSLIST") {
        return Ok(None);
    }
    let start = parser.current_span().start();
    parser.advance(); // SHOW
    skip_trivia(parser);
    if full {
        parser.advance(); // FULL
        skip_trivia(parser);
    }
    let end = parser.current_span().end();
    parser.advance(); // PROCESSLIST
    Ok(Some(Statement::ShowProcessList(ShowProcessList {
        full,
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

#[cfg(test)]
mod tests {
    use crate::ast::{ShowProcessList, Statement};
    use crate::parser::parse;
    use crate::printer::Printer;

    #[test]
    fn both_processlist_forms_parse_and_round_trip() {
        assert_eq!(
            parse("SHOW PROCESSLIST").expect("plain form parses"),
            vec![Statement::ShowProcessList(ShowProcessList {
                full: false,
                span: crate::Span::new(0, 16),
            })]
        );
        assert_eq!(
            Printer::new().statements(&parse("SHOW PROCESSLIST").unwrap()),
            "SHOW PROCESSLIST"
        );

        let full = parse("SHOW FULL PROCESSLIST").expect("full form parses");
        assert!(matches!(
            full.as_slice(),
            [Statement::ShowProcessList(ShowProcessList {
                full: true,
                ..
            })]
        ));
        assert_eq!(Printer::new().statements(&full), "SHOW FULL PROCESSLIST");
    }

    #[test]
    fn processlist_does_not_shadow_its_neighbours() {
        assert!(matches!(
            parse("SHOW STATS FOR t").unwrap().as_slice(),
            [Statement::Statistics(_)]
        ));
        // PROCESSLIST is matched as a word, so it remains usable as an alias.
        assert!(matches!(
            parse("SELECT 1 AS processlist")
                .expect("processlist is not reserved")
                .as_slice(),
            [Statement::Query(_)]
        ));
    }
}
