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

use novarocks_parser::lexer::lex;
use novarocks_parser::token::TokenKind;
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

#[test]
fn grammar_samples_use_their_actual_entry() {
    use novarocks_parser::parser;
    let mut failures = Vec::new();
    for line in include_str!("grammar-coverage.tsv")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 10, "{line}");
        if !matches!(fields[0], "alternative" | "optional") {
            continue;
        }
        assert!(!fields[5].is_empty(), "missing grammar source: {line}");
        assert!(
            matches!(
                fields[6],
                "supported" | "parsed-not-supported" | "syntax-rejection"
            ),
            "missing grammar ownership: {line}"
        );
        let source = sample(fields[5]);
        let result = match fields[1] {
            "standaloneExpression" => parser::parse_expression(&source).map(|_| ()),
            "standaloneType" => parser::parse_type(&source).map(|_| ()),
            "standaloneRowPattern" => parser::parse_row_pattern(&source).map(|_| ()),
            "standaloneFunctionSpecification" => {
                parser::parse_function_specification(&source).map(|_| ())
            }
            "standalonePathSpecification" => parser::parse_path_specification(&source).map(|_| ()),
            _ => parser::parse_statement(&source).map(|_| ()),
        };
        match fields[7] {
            "accept" if result.is_err() => {
                failures.push(format!("{line}\nsource: {source}\n{result:?}"))
            }
            "reject" | "syntax-error" if result.is_ok() => {
                failures.push(format!("{line}\nsource: {source}\nunexpected acceptance"))
            }
            "accept" | "reject" | "syntax-error" => {}
            unexpected => panic!("unknown grammar expectation {unexpected}: {line}"),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

#[test]
fn keyword_samples_obey_reserved_and_nonreserved_identifier_grammar() {
    for line in include_str!("grammar-coverage.tsv")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let fields: Vec<_> = line.split('\t').collect();
        if fields[0] != "keyword" {
            continue;
        }
        let source = sample(fields[5]);
        let result = novarocks_parser::parser::parse_query(&source);
        match fields[7] {
            "nonreserved" => assert!(result.is_ok(), "{line}\n{result:?}"),
            "reserved" => assert!(result.is_err(), "{line}"),
            unexpected => panic!("unknown keyword expectation {unexpected}: {line}"),
        }
    }
}
