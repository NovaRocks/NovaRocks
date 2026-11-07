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

//! Lossless MySQL/StarRocks lexical scanning.

use crate::{LexError, Span, Symbol, Token, TokenKind, TriviaKind, keyword};

/// Lexes SQL source into a lossless, source-ordered token stream.
///
/// Every token's span is a byte range into `source`; concatenating the spans
/// of all non-`End` tokens reconstructs `source` exactly. Trivia is represented
/// as ordinary source-ordered tokens. A trivia run, including a `/*+ ... */`
/// hint, is leading trivia for the next non-trivia token; at end-of-input it is
/// leading trivia for `End`. This adjacency is the token-level attachment rule
/// until the AST owns a richer trivia container.
pub fn lex(source: &str) -> Result<Vec<Token>, LexError> {
    Lexer::new(source).run()
}

struct Lexer<'source> {
    source: &'source str,
    position: usize,
    tokens: Vec<Token>,
}

impl<'source> Lexer<'source> {
    fn new(source: &'source str) -> Self {
        Self {
            source,
            position: 0,
            tokens: Vec::new(),
        }
    }

    fn run(mut self) -> Result<Vec<Token>, LexError> {
        while let Some(character) = self.current() {
            let start = self.position;
            if character.is_whitespace() {
                self.consume_while(char::is_whitespace);
                self.push(TokenKind::Trivia(TriviaKind::Whitespace), start);
            } else if (self.starts_with("--") && self.mysql_dash_comment()) || character == '#' {
                self.consume_line_comment();
                self.push(TokenKind::Trivia(TriviaKind::LineComment), start);
            } else if self.starts_with("/*") {
                let hint = self.starts_with("/*+");
                self.consume_block_comment(start)?;
                self.push(
                    TokenKind::Trivia(if hint {
                        TriviaKind::HintComment
                    } else {
                        TriviaKind::BlockComment
                    }),
                    start,
                );
            } else if character == '`' {
                self.consume_quoted_identifier(start)?;
                self.push(TokenKind::QuotedIdent, start);
            } else if character == '\'' || character == '"' {
                self.consume_string(start, character)?;
                self.push(TokenKind::String, start);
            } else if self.has_string_prefix() {
                self.advance(); // literal prefix
                let quote = self.current().expect("prefixed string has a quote");
                self.consume_string(start, quote)?;
                self.push(TokenKind::String, start);
            } else if character.is_ascii_digit()
                || (character == '.' && self.next().is_some_and(|next| next.is_ascii_digit()))
            {
                let kind = self.consume_number();
                self.push(kind, start);
            } else if is_identifier_start(character) {
                self.advance();
                self.consume_while(is_identifier_part);
                let word = &self.source[start..self.position];
                self.push(
                    keyword::lookup(word).map_or(TokenKind::Ident, TokenKind::Keyword),
                    start,
                );
            } else if let Some(symbol) = self.consume_symbol() {
                self.push(TokenKind::Symbol(symbol), start);
            } else {
                self.advance();
                return Err(LexError::UnexpectedCharacter {
                    character,
                    span: Span::new(start, self.position),
                });
            }
        }
        self.tokens.push(Token::new(
            TokenKind::End,
            Span::new(self.position, self.position),
        ));
        Ok(self.tokens)
    }

    fn current(&self) -> Option<char> {
        self.source[self.position..].chars().next()
    }

    fn next(&self) -> Option<char> {
        let current = self.current()?;
        self.source[self.position + current.len_utf8()..]
            .chars()
            .next()
    }

    fn starts_with(&self, prefix: &str) -> bool {
        self.source[self.position..].starts_with(prefix)
    }

    fn advance(&mut self) -> char {
        let character = self
            .current()
            .expect("advance is only called at a character");
        self.position += character.len_utf8();
        character
    }

    fn consume_while(&mut self, predicate: impl Fn(char) -> bool) {
        while self.current().is_some_and(&predicate) {
            self.advance();
        }
    }

    fn push(&mut self, kind: TokenKind, start: usize) {
        self.tokens
            .push(Token::new(kind, Span::new(start, self.position)));
    }

    fn mysql_dash_comment(&self) -> bool {
        self.source[self.position + 2..]
            .chars()
            .next()
            .is_none_or(|character| character.is_whitespace())
    }

    fn consume_line_comment(&mut self) {
        while let Some(character) = self.current() {
            self.advance();
            if character == '\n' {
                break;
            }
        }
    }

    fn consume_block_comment(&mut self, start: usize) -> Result<(), LexError> {
        self.position += 2; // `/*`
        while self.current().is_some() {
            if self.starts_with("*/") {
                self.position += 2;
                return Ok(());
            }
            self.advance();
        }
        Err(LexError::UnterminatedComment {
            span: Span::new(start, self.position),
        })
    }

    fn consume_quoted_identifier(&mut self, start: usize) -> Result<(), LexError> {
        self.advance(); // opening backtick
        while let Some(character) = self.current() {
            self.advance();
            if character == '`' {
                if self.current() == Some('`') {
                    self.advance();
                } else {
                    return Ok(());
                }
            }
        }
        Err(LexError::UnterminatedQuotedIdentifier {
            span: Span::new(start, self.position),
        })
    }

    fn consume_string(&mut self, start: usize, quote: char) -> Result<(), LexError> {
        self.advance(); // opening quote
        while let Some(character) = self.current() {
            self.advance();
            if character == '\\' {
                if self.current().is_some() {
                    self.advance();
                }
            } else if character == quote {
                if self.current() == Some(quote) {
                    self.advance();
                } else {
                    return Ok(());
                }
            }
        }
        Err(LexError::UnterminatedString {
            span: Span::new(start, self.position),
        })
    }

    fn has_string_prefix(&self) -> bool {
        matches!(self.current(), Some('x' | 'X' | 'b' | 'B' | 'n' | 'N'))
            && matches!(self.next(), Some('\'' | '"'))
    }

    fn consume_number(&mut self) -> TokenKind {
        if self.starts_with("0x") || self.starts_with("0X") {
            self.position += 2;
            self.consume_while(|character| character.is_ascii_hexdigit());
            return TokenKind::HexNumber;
        }

        self.consume_while(|character| character.is_ascii_digit());
        if self.current() == Some('.') && !self.starts_with("..") {
            self.advance();
            self.consume_while(|character| character.is_ascii_digit());
        }
        if matches!(self.current(), Some('e' | 'E')) {
            let exponent_start = self.position;
            self.advance();
            if matches!(self.current(), Some('+' | '-')) {
                self.advance();
            }
            if self
                .current()
                .is_some_and(|character| character.is_ascii_digit())
            {
                self.consume_while(|character| character.is_ascii_digit());
            } else {
                self.position = exponent_start;
            }
        }
        TokenKind::Number
    }

    fn consume_symbol(&mut self) -> Option<Symbol> {
        let (prefix, symbol) = if self.starts_with("->>") {
            (3, Symbol::LongArrow)
        } else if self.starts_with("<=>") {
            (3, Symbol::NullSafeEq)
        } else if self.starts_with(">=") {
            (2, Symbol::Gte)
        } else if self.starts_with("<=") {
            (2, Symbol::Lte)
        } else if self.starts_with("<>") || self.starts_with("!=") {
            (2, Symbol::Neq)
        } else if self.starts_with("<<") {
            (2, Symbol::ShiftLeft)
        } else if self.starts_with(">>") {
            (2, Symbol::ShiftRight)
        } else if self.starts_with("||") {
            (2, Symbol::DoublePipe)
        } else if self.starts_with("&&") {
            (2, Symbol::DoubleAmpersand)
        } else if self.starts_with(":=") {
            (2, Symbol::ColonEq)
        } else if self.starts_with("::") {
            (2, Symbol::DoubleColon)
        } else if self.starts_with("=>") {
            (2, Symbol::FatArrow)
        } else if self.starts_with("->") {
            (2, Symbol::Arrow)
        } else if self.starts_with("..") {
            (2, Symbol::DoubleDot)
        } else if self.starts_with("?&") {
            (2, Symbol::QuestionAmpersand)
        } else {
            match self.current()? {
                ',' => (1, Symbol::Comma),
                '\\' => (1, Symbol::Backslash),
                ':' => (1, Symbol::Colon),
                '.' => (1, Symbol::Dot),
                '=' => (1, Symbol::Eq),
                '>' => (1, Symbol::Gt),
                '{' => (1, Symbol::LBrace),
                '[' => (1, Symbol::LBracket),
                '(' => (1, Symbol::LParen),
                '<' => (1, Symbol::Lt),
                '-' => (1, Symbol::Minus),
                '%' => (1, Symbol::Percent),
                '|' => (1, Symbol::Pipe),
                '+' => (1, Symbol::Plus),
                '?' => (1, Symbol::Question),
                '}' => (1, Symbol::RBrace),
                ']' => (1, Symbol::RBracket),
                ')' => (1, Symbol::RParen),
                ';' => (1, Symbol::Semicolon),
                '/' => (1, Symbol::Slash),
                '*' => (1, Symbol::Star),
                '~' => (1, Symbol::Tilde),
                '!' => (1, Symbol::Bang),
                '&' => (1, Symbol::Ampersand),
                '^' => (1, Symbol::Caret),
                _ => return None,
            }
        };
        self.position += prefix;
        Some(symbol)
    }
}

fn is_identifier_start(character: char) -> bool {
    // SQL test corpus files intentionally retain runner placeholders such as
    // `${case_db}` before substitution. Tokenizing `$` here keeps the lexer
    // lossless; grammar/admission decides whether a resulting identifier is
    // legal in a statement position.
    character.is_alphabetic() || matches!(character, '_' | '$')
}

fn is_identifier_part(character: char) -> bool {
    character.is_alphanumeric() || matches!(character, '_' | '$')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Keyword;

    fn kinds(source: &str) -> Vec<TokenKind> {
        lex(source)
            .expect("source should lex")
            .into_iter()
            .map(|token| token.kind)
            .collect()
    }

    fn reconstruct(source: &str) -> String {
        lex(source)
            .expect("source should lex")
            .into_iter()
            .filter(|token| token.kind != TokenKind::End)
            .map(|token| source[token.span.start()..token.span.end()].to_owned())
            .collect()
    }

    #[test]
    fn preserves_quoted_identifiers_numbers_and_string_escapes() {
        let source = "SELECT `odd``name`, t$snapshots, 'e\\\\f', \"x\\\"y\", 1.2e-3, .5, 0xDeAd";
        let tokens = lex(source).expect("source should lex");
        assert_eq!(reconstruct(source), source);
        assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Select));
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::QuotedIdent)
        );
        assert_eq!(
            tokens
                .iter()
                .filter(|token| token.kind == TokenKind::Number)
                .count(),
            2
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::HexNumber)
        );
        assert_eq!(tokens.last().expect("End token").kind, TokenKind::End);
    }

    #[test]
    fn classifies_keywords_without_changing_identifier_spelling() {
        let source = "sHoW BACKENDS as from TRUE false t$snapshots";
        assert_eq!(
            kinds(source),
            vec![
                TokenKind::Keyword(Keyword::Show),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Keyword(Keyword::Backends),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Keyword(Keyword::As),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Keyword(Keyword::From),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Keyword(Keyword::True),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Keyword(Keyword::False),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Ident,
                TokenKind::End,
            ]
        );
        assert_eq!(reconstruct(source), source);
    }

    #[test]
    fn classifies_query_vocabulary_and_preserves_quoted_and_non_reserved_words() {
        let source = "SeLeCt `select`, window, TRY_CAST(x AS INT) FROM t";
        assert_eq!(
            kinds(source),
            vec![
                TokenKind::Keyword(Keyword::Select),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::QuotedIdent,
                TokenKind::Symbol(Symbol::Comma),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Keyword(Keyword::Window),
                TokenKind::Symbol(Symbol::Comma),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Keyword(Keyword::TryCast),
                TokenKind::Symbol(Symbol::LParen),
                TokenKind::Ident,
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Keyword(Keyword::As),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Ident,
                TokenKind::Symbol(Symbol::RParen),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Keyword(Keyword::From),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Ident,
                TokenKind::End,
            ]
        );
        assert_eq!(reconstruct(source), source);
    }

    #[test]
    fn preserves_prefixed_string_literals_as_single_lossless_tokens() {
        let source = "X'4D79' b\"0101\" N'local text'";
        assert_eq!(
            kinds(source),
            vec![
                TokenKind::String,
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::String,
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::String,
                TokenKind::End,
            ]
        );
        assert_eq!(reconstruct(source), source);
    }

    #[test]
    fn preserves_comments_and_keeps_hints_as_leading_trivia() {
        let source = "-- line\n# hash\n/* block */ /*+ SET_VAR(x = 'a\\\\b') */\nSHOW BACKENDS";
        let tokens = lex(source).expect("source should lex");
        assert_eq!(reconstruct(source), source);
        let hint_index = tokens
            .iter()
            .position(|token| token.kind == TokenKind::Trivia(TriviaKind::HintComment))
            .expect("hint trivia");
        let next_significant = tokens[hint_index + 1..]
            .iter()
            .find(|token| !matches!(token.kind, TokenKind::Trivia(_)))
            .expect("hint is followed by a token");
        assert_eq!(next_significant.kind, TokenKind::Keyword(Keyword::Show));
    }

    #[test]
    fn mysql_dash_requires_whitespace_but_hash_does_not() {
        assert_eq!(
            kinds("1--2 # comment"),
            vec![
                TokenKind::Number,
                TokenKind::Symbol(Symbol::Minus),
                TokenKind::Symbol(Symbol::Minus),
                TokenKind::Number,
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Trivia(TriviaKind::LineComment),
                TokenKind::End,
            ]
        );
    }

    #[test]
    fn reports_unterminated_multiline_constructs_with_full_source_span() {
        assert_eq!(
            lex("'unterminated").expect_err("must fail"),
            LexError::UnterminatedString {
                span: Span::new(0, 13)
            }
        );
        assert_eq!(
            lex("/* unterminated\ncomment").expect_err("must fail"),
            LexError::UnterminatedComment {
                span: Span::new(0, 23)
            }
        );
        assert_eq!(
            lex("`unterminated").expect_err("must fail"),
            LexError::UnterminatedQuotedIdentifier {
                span: Span::new(0, 13)
            }
        );
    }

    #[test]
    fn recognizes_the_foundation_punctuation_set() {
        let source = ";:{}[]%|?~!&^";
        assert_eq!(
            kinds(source),
            vec![
                TokenKind::Symbol(Symbol::Semicolon),
                TokenKind::Symbol(Symbol::Colon),
                TokenKind::Symbol(Symbol::LBrace),
                TokenKind::Symbol(Symbol::RBrace),
                TokenKind::Symbol(Symbol::LBracket),
                TokenKind::Symbol(Symbol::RBracket),
                TokenKind::Symbol(Symbol::Percent),
                TokenKind::Symbol(Symbol::Pipe),
                TokenKind::Symbol(Symbol::Question),
                TokenKind::Symbol(Symbol::Tilde),
                TokenKind::Symbol(Symbol::Bang),
                TokenKind::Symbol(Symbol::Ampersand),
                TokenKind::Symbol(Symbol::Caret),
                TokenKind::End,
            ]
        );
        assert_eq!(reconstruct(source), source);
    }

    #[test]
    fn prefers_longest_query_operator_matches_and_keeps_number_boundaries() {
        let source = "<=> <= <> != << >> || && := :: => ->> -> ?& .. 1..2";
        assert_eq!(
            kinds(source),
            vec![
                TokenKind::Symbol(Symbol::NullSafeEq),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::Lte),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::Neq),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::Neq),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::ShiftLeft),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::ShiftRight),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::DoublePipe),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::DoubleAmpersand),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::ColonEq),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::DoubleColon),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::FatArrow),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::LongArrow),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::Arrow),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::QuestionAmpersand),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Symbol(Symbol::DoubleDot),
                TokenKind::Trivia(TriviaKind::Whitespace),
                TokenKind::Number,
                TokenKind::Symbol(Symbol::DoubleDot),
                TokenKind::Number,
                TokenKind::End,
            ]
        );
        assert_eq!(reconstruct(source), source);
    }

    #[test]
    fn uses_byte_spans_for_multibyte_identifiers() {
        let source = "名$s";
        let tokens = lex(source).expect("source should lex");
        assert_eq!(tokens[0].span, Span::new(0, source.len()));
        assert_eq!(reconstruct(source), source);
    }
}
