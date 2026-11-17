// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::{GrammarError as ParserError, Parser};
use crate::ast::{Ident, ObjectName};
use crate::trino_token::{Keyword, Symbol, Token, TokenKind};
use crate::{KeywordClass, ParseError, Span, trino_keyword};

impl Parser<'_> {
    pub(super) fn peek(&self, offset: usize) -> Token {
        self.tokens[self.position..]
            .iter()
            .filter(|token| !matches!(token.kind, TokenKind::Trivia(_)))
            .nth(offset)
            .copied()
            .unwrap_or_else(|| *self.tokens.last().expect("lexer supplies End"))
    }

    pub(super) fn current(&mut self) -> Token {
        while matches!(self.tokens[self.position].kind, TokenKind::Trivia(_)) {
            self.position += 1;
        }
        self.tokens[self.position]
    }

    pub(super) fn bump(&mut self) -> Token {
        let token = self.current();
        if token.kind != TokenKind::End {
            self.position += 1;
        }
        token
    }

    pub(super) fn at_keyword(&self, keyword: Keyword) -> bool {
        self.peek(0).kind == TokenKind::Keyword(keyword)
    }

    pub(super) fn at_symbol(&self, symbol: Symbol) -> bool {
        self.peek(0).kind == TokenKind::Symbol(symbol)
    }

    pub(super) fn eat_keyword(&mut self, keyword: Keyword) -> Option<Span> {
        self.at_keyword(keyword).then(|| self.bump().span)
    }

    pub(super) fn eat_symbol(&mut self, symbol: Symbol) -> Option<Span> {
        self.at_symbol(symbol).then(|| self.bump().span)
    }

    pub(super) fn expect_keyword(&mut self, keyword: Keyword) -> Result<Span, ParserError> {
        self.eat_keyword(keyword)
            .ok_or_else(|| self.error(keyword.sql()))
    }

    pub(super) fn expect_symbol(&mut self, symbol: Symbol) -> Result<Span, ParserError> {
        self.eat_symbol(symbol)
            .ok_or_else(|| self.error(symbol.sql()))
    }

    pub(super) fn error(&self, expected: &'static str) -> ParserError {
        let token = self.peek(0);
        ParseError::UnexpectedToken {
            expected,
            found: if token.kind == TokenKind::End {
                "end of input".to_owned()
            } else {
                self.source[token.span.start()..token.span.end()].to_owned()
            },
            span: token.span,
        }
        .into()
    }

    pub(super) fn last_span(&self) -> Span {
        self.tokens[..self.position]
            .iter()
            .rev()
            .find(|token| !matches!(token.kind, TokenKind::Trivia(_)))
            .expect("a grammar token has been consumed")
            .span
    }

    pub(super) fn span_since(&self, start: Span) -> Span {
        Span::new(start.start(), self.last_span().end())
    }

    pub(super) fn is_identifier(&self, offset: usize) -> bool {
        match self.peek(offset).kind {
            TokenKind::Identifier | TokenKind::QuotedIdentifier => true,
            TokenKind::Keyword(keyword) => {
                trino_keyword::class(keyword) == KeywordClass::NonReserved
            }
            _ => false,
        }
    }

    pub(super) fn ident(&mut self) -> Result<Ident, ParserError> {
        if !self.is_identifier(0) {
            return Err(self.error("an identifier"));
        }
        let token = self.bump();
        let text = &self.source[token.span.start()..token.span.end()];
        let quoted = token.kind == TokenKind::QuotedIdentifier;
        Ok(Ident {
            value: if quoted {
                text[1..text.len() - 1].replace("\"\"", "\"")
            } else {
                text.to_owned()
            },
            quoted,
            span: token.span,
        })
    }

    pub(super) fn qualified_name(&mut self) -> Result<ObjectName, ParserError> {
        let first = self.ident()?;
        let start = first.span;
        let mut parts = vec![first];
        while self.at_symbol(Symbol::Dot) && self.is_identifier(1) {
            self.bump();
            parts.push(self.ident()?);
        }
        Ok(ObjectName {
            parts,
            span: self.span_since(start),
        })
    }
}
