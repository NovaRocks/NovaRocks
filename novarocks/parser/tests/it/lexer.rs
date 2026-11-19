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

use novarocks_parser::keyword::{KEYWORDS, class, lookup};
use novarocks_parser::lexer::lex;
use novarocks_parser::token::{Keyword, Symbol, TokenKind, TriviaKind};
use novarocks_parser::{KeywordClass, LexError, ParserError};

fn kinds(sql: &str) -> Vec<TokenKind> {
    lex(sql)
        .unwrap_or_else(|error| panic!("{sql:?}: {error:?}"))
        .into_iter()
        .map(|token| token.kind)
        .filter(|kind| !matches!(kind, TokenKind::Trivia(_) | TokenKind::End))
        .collect()
}

#[test]
fn identifiers_preserve_quoting_and_case_insensitive_keyword_facts() {
    assert_eq!(
        kinds(r#"select _a1, "MiXeD", "a""b", "select", "t$snapshots" from table_name"#),
        vec![
            TokenKind::Keyword(Keyword::Select),
            TokenKind::Identifier,
            TokenKind::Symbol(Symbol::Comma),
            TokenKind::QuotedIdentifier,
            TokenKind::Symbol(Symbol::Comma),
            TokenKind::QuotedIdentifier,
            TokenKind::Symbol(Symbol::Comma),
            TokenKind::QuotedIdentifier,
            TokenKind::Symbol(Symbol::Comma),
            TokenKind::QuotedIdentifier,
            TokenKind::Keyword(Keyword::From),
            TokenKind::Identifier,
        ]
    );
    for source in ["`x`", "1abc", "0xG", "1_", "1__2", "1_2e3", r#""""#] {
        assert!(
            matches!(lex(source), Err(ParserError::Parse(_))),
            "{source}"
        );
    }
}

#[test]
fn strings_have_distinct_lexical_kinds_and_no_backslash_escapes() {
    for (source, expected) in [
        ("'a''b'", TokenKind::String),
        (r"'a\tb'", TokenKind::String),
        (r"'a\'", TokenKind::String),
        ("'a\nb'", TokenKind::String),
        (r"U&'a\0009b'", TokenKind::UnicodeString),
        (r"u&'\+01F600'", TokenKind::UnicodeString),
        ("X'00 ff'", TokenKind::BinaryLiteral),
        ("x'not-hex'", TokenKind::BinaryLiteral),
        ("$$a'\n;b$$", TokenKind::DollarString),
        ("$$$$", TokenKind::DollarString),
    ] {
        assert_eq!(kinds(source), vec![expected], "{source:?}");
        let tokens = lex(source).unwrap();
        assert_eq!(tokens[0].span.start(), 0);
        assert_eq!(tokens[0].span.end(), source.len());
    }
    assert_eq!(
        kinds("U&'a!0009b' UESCAPE '!'"),
        vec![
            TokenKind::UnicodeString,
            TokenKind::Keyword(Keyword::Uescape),
            TokenKind::String,
        ]
    );
    assert_eq!(
        kinds("X'a''b'"),
        vec![TokenKind::BinaryLiteral, TokenKind::String]
    );
    // A type name followed by a string is handled by the grammar, not a lexical prefix.
    assert_eq!(
        kinds("N'abc'"),
        vec![TokenKind::Identifier, TokenKind::String]
    );
}

#[test]
fn numbers_use_longest_matching_with_grammar_priority() {
    for source in ["0", "123", "1_234", "0x1F", "0X_Ff", "0o_17", "0b_10_01"] {
        assert_eq!(kinds(source), vec![TokenKind::Integer], "{source}");
    }
    for source in ["1.", ".5", "1.25", "1_2.3_4", ".1_2"] {
        assert_eq!(kinds(source), vec![TokenKind::Decimal], "{source}");
    }
    for source in ["1e2", "1.E2", "1.5e-2", ".5e+3", "0E0"] {
        assert_eq!(kinds(source), vec![TokenKind::Double], "{source}");
    }
    assert_eq!(
        kinds("0xFF.0"),
        vec![TokenKind::Integer, TokenKind::Decimal]
    );
    for source in ["0b2", "0o8", "0x_", "123abc", "1E", "1e2_3"] {
        assert!(lex(source).is_err(), "{source}");
    }
    assert_eq!(
        kinds("-1"),
        vec![TokenKind::Symbol(Symbol::Minus), TokenKind::Integer]
    );
}

#[test]
fn comments_are_lossless_non_nested_and_have_no_hint_kind() {
    let source =
        "\t--no space\r\n/*+ SET_VAR(x=1) */SELECT /* outer /* inner */ 'é\\'; --尾\rSELECT 1";
    let tokens = lex(source).unwrap();
    let reconstructed: String = tokens
        .iter()
        .map(|token| &source[token.span.start()..token.span.end()])
        .collect();
    assert_eq!(reconstructed, source);
    assert_eq!(
        tokens.first().unwrap().kind,
        TokenKind::Trivia(TriviaKind::Whitespace)
    );
    assert_eq!(tokens[1].kind, TokenKind::Trivia(TriviaKind::LineComment));
    assert_eq!(
        &source[tokens[1].span.start()..tokens[1].span.end()],
        "--no space\r\n"
    );
    assert_eq!(tokens[2].kind, TokenKind::Trivia(TriviaKind::BlockComment));
    assert_eq!(tokens.last().unwrap().kind, TokenKind::End);
    assert_eq!(tokens.last().unwrap().span.start(), source.len());
    for pair in tokens.windows(2) {
        assert_eq!(pair[0].span.end(), pair[1].span.start());
    }
    assert_eq!(
        kinds("--x\rSELECT 1"),
        vec![TokenKind::Keyword(Keyword::Select), TokenKind::Integer]
    );
}

#[test]
fn whitespace_and_unterminated_input_report_existing_typed_errors() {
    for source in ["#comment", "a\u{a0}b", "a\u{b}b", "名称", "\\"] {
        assert!(
            matches!(
                lex(source),
                Err(ParserError::Lex(LexError::UnexpectedCharacter { .. }))
            ),
            "{source:?}"
        );
    }
    for source in ["'abc", "U&'abc", "X'abc"] {
        assert!(matches!(
            lex(source),
            Err(ParserError::Lex(LexError::UnterminatedString { .. }))
        ));
    }
    for source in ["\"abc", "`abc"] {
        assert!(matches!(
            lex(source),
            Err(ParserError::Lex(
                LexError::UnterminatedQuotedIdentifier { .. }
            ))
        ));
    }
    assert!(matches!(
        lex("/*abc"),
        Err(ParserError::Lex(LexError::UnterminatedComment { .. }))
    ));
}

#[test]
fn grammar_punctuation_includes_routine_and_row_pattern_tokens() {
    let source = "= <> != < <= > >= + - * / % || ? ; . ( ) , -> {- -} : :: => @ [ ] ^ { } | $";
    let expected = [
        Symbol::Eq,
        Symbol::Neq,
        Symbol::Neq,
        Symbol::Lt,
        Symbol::Lte,
        Symbol::Gt,
        Symbol::Gte,
        Symbol::Plus,
        Symbol::Minus,
        Symbol::Star,
        Symbol::Slash,
        Symbol::Percent,
        Symbol::DoublePipe,
        Symbol::Question,
        Symbol::Semicolon,
        Symbol::Dot,
        Symbol::LParen,
        Symbol::RParen,
        Symbol::Comma,
        Symbol::Arrow,
        Symbol::ExcludeStart,
        Symbol::ExcludeEnd,
        Symbol::Colon,
        Symbol::DoubleColon,
        Symbol::FatArrow,
        Symbol::At,
        Symbol::LBracket,
        Symbol::RBracket,
        Symbol::Caret,
        Symbol::LBrace,
        Symbol::RBrace,
        Symbol::Pipe,
        Symbol::Dollar,
    ];
    assert_eq!(
        kinds(source),
        expected
            .into_iter()
            .map(TokenKind::Symbol)
            .collect::<Vec<_>>()
    );
}

#[test]
fn keyword_samples_retain_identifier_reservedness() {
    let data = include_str!("keyword-cases.tsv");
    let mut seen = Vec::new();
    for line in data.lines().filter(|line| !line.starts_with('#')) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 4, "{line}");
        let spelling = fields[0];
        let keyword = lookup(spelling).unwrap();
        let expected = match fields[1] {
            "reserved" => KeywordClass::Reserved,
            "nonreserved" => KeywordClass::NonReserved,
            other => panic!("unknown keyword classification: {other}"),
        };
        assert_eq!(class(keyword), expected, "{spelling}");
        assert_eq!(keyword.sql(), spelling);
        assert_eq!(lookup(&spelling.to_ascii_lowercase()), Some(keyword));
        assert_eq!(kinds(spelling), vec![TokenKind::Keyword(keyword)]);
        assert_eq!(
            kinds(&format!("\"{spelling}\"")),
            vec![TokenKind::QuotedIdentifier]
        );
        assert!(lex(fields[2]).is_ok());
        assert!(lex(fields[3]).is_ok());
        seen.push(spelling);
    }
    assert_eq!(
        seen,
        KEYWORDS.iter().map(|(word, _)| *word).collect::<Vec<_>>()
    );
    assert_eq!(class(lookup("SKIP").unwrap()), KeywordClass::Reserved);
    assert_eq!(lookup("STRING"), Some(Keyword::TextString));
    assert_eq!(lookup("UTF16"), Some(Keyword::Utf16));
    assert_eq!(lookup("not_a_keyword"), None);
}
