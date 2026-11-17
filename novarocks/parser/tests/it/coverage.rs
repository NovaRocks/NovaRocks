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

use novarocks_parser::trino_lexer::lex;
use novarocks_parser::trino_token::TokenKind;
use std::borrow::Cow;

/// Loads SQL bytes that cannot fit in one TSV cell, including dollar-body LF.
pub(super) fn sample(raw: &str) -> Cow<'_, str> {
    match raw.strip_prefix("@file:") {
        None => Cow::Borrowed(raw),
        Some(relative) => {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/it")
                .join(relative);
            Cow::Owned(
                std::fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("{}: {error}", path.display())),
            )
        }
    }
}

#[test]
fn lexical_rule_samples_match_their_token_kind_and_reject_neighbours() {
    for line in include_str!("lexical-cases.tsv")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 4, "{line}");
        let [rule, expected, source, rejected] = fields[..] else {
            unreachable!()
        };
        if expected == "reject" {
            assert!(lex(source).is_err(), "{rule}: {source:?}");
        } else {
            let tokens = lex(source).unwrap_or_else(|error| panic!("{rule}: {error:?}"));
            assert_eq!(tokens.len(), 2, "{rule}: {source:?}");
            assert_eq!(format!("{:?}", tokens[0].kind), expected, "{rule}");
            assert_eq!(tokens[0].span.start(), 0, "{rule}");
            assert_eq!(tokens[0].span.end(), source.len(), "{rule}");
            assert_eq!(tokens[1].kind, TokenKind::End, "{rule}");
        }
        assert!(lex(rejected).is_err(), "{rule}: {rejected:?}");
    }
}

#[test]
fn grammar_sample_tokens_reconstruct_the_complete_input() {
    for line in include_str!("grammar-coverage.tsv")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 10, "{line}");
        if !matches!(fields[0], "alternative" | "optional") || fields[5].is_empty() {
            continue;
        }
        let source = sample(fields[5]);
        if fields[7] == "syntax-error" {
            assert!(lex(&source).is_err(), "{}: {source}", fields[1]);
            continue;
        }
        let tokens =
            lex(&source).unwrap_or_else(|error| panic!("{}: {source}: {error:?}", fields[1]));
        let reconstructed: String = tokens
            .iter()
            .map(|token| &source[token.span.start()..token.span.end()])
            .collect();
        assert_eq!(reconstructed, source.as_ref(), "{}", fields[1]);
        for adjacent in tokens.windows(2) {
            assert_eq!(adjacent[0].span.end(), adjacent[1].span.start());
        }
    }
}
