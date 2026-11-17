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
use crate::ast::*;
use crate::trino_token::{Keyword, Symbol, TokenKind};

impl Parser<'_> {
    pub(super) fn parse_order_by(&mut self) -> Result<OrderBy, ParserError> {
        let start = self.expect_keyword(Keyword::Order)?;
        self.expect_keyword(Keyword::By)?;
        let mut items = vec![self.parse_sort_item()?];
        while self.eat_symbol(Symbol::Comma).is_some() {
            items.push(self.parse_sort_item()?);
        }
        Ok(OrderBy {
            items: items.into(),
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_sort_item(&mut self) -> Result<SortItem, ParserError> {
        let start = self.current().span;
        let sort_key = self.parse_expression()?;
        let ordering = if self.eat_keyword(Keyword::Desc).is_some() {
            SortOrdering::Descending
        } else {
            self.eat_keyword(Keyword::Asc);
            SortOrdering::Ascending
        };
        let null_ordering = if self.eat_keyword(Keyword::Nulls).is_some() {
            if self.eat_keyword(Keyword::First).is_some() {
                NullOrdering::First
            } else {
                self.expect_keyword(Keyword::Last)?;
                NullOrdering::Last
            }
        } else {
            NullOrdering::Undefined
        };
        Ok(SortItem {
            sort_key,
            ordering,
            null_ordering,
            span: self.span_since(start),
        })
    }

    #[recursive::recursive]
    pub(super) fn parse_window(&mut self) -> Result<Window, ParserError> {
        self.expect_keyword(Keyword::Over)?;
        if self.eat_symbol(Symbol::LParen).is_some() {
            let specification = self.parse_window_specification()?;
            self.expect_symbol(Symbol::RParen)?;
            Ok(Window::Specification(AstBox::new(specification)))
        } else {
            let name = self.ident()?;
            let span = name.span;
            Ok(Window::Reference(WindowReference { name, span }))
        }
    }

    #[recursive::recursive]
    pub(super) fn parse_window_specification(
        &mut self,
    ) -> Result<WindowSpecification, ParserError> {
        let start = self.current().span;
        let existing_window_name = if self.is_identifier(0)
            && matches!(
                self.peek(1).kind,
                TokenKind::Symbol(Symbol::RParen)
                    | TokenKind::Keyword(
                        Keyword::Partition
                            | Keyword::Order
                            | Keyword::Rows
                            | Keyword::Range
                            | Keyword::Groups
                            | Keyword::Measures
                    )
            ) {
            Some(self.ident()?)
        } else {
            None
        };
        let mut partition_by = Vec::new();
        if self.eat_keyword(Keyword::Partition).is_some() {
            self.expect_keyword(Keyword::By)?;
            partition_by.push(self.parse_expression()?);
            while self.eat_symbol(Symbol::Comma).is_some() {
                partition_by.push(self.parse_expression()?);
            }
        }
        let order_by = if self.at_keyword(Keyword::Order) {
            Some(self.parse_order_by()?)
        } else {
            None
        };
        let window_frame = if matches!(
            self.peek(0).kind,
            TokenKind::Keyword(
                Keyword::Measures | Keyword::Rows | Keyword::Range | Keyword::Groups
            )
        ) {
            Some(self.parse_pattern_window_frame()?)
        } else {
            None
        };
        // Empty specifications have an actual zero-width content extent at ).
        let span = if self.last_span().end() < start.start() {
            crate::Span::new(start.start(), start.start())
        } else {
            self.span_since(start)
        };
        Ok(WindowSpecification {
            existing_window_name,
            partition_by: partition_by.into(),
            order_by,
            window_frame,
            span,
        })
    }

    pub(super) fn parse_pattern_frame_exclusion(
        &mut self,
    ) -> Result<WindowFrameExclusion, ParserError> {
        Ok(if self.eat_keyword(Keyword::Exclude).is_some() {
            if self.eat_keyword(Keyword::Current).is_some() {
                self.expect_keyword(Keyword::Row)?;
                WindowFrameExclusion::CurrentRow
            } else if self.eat_keyword(Keyword::Group).is_some() {
                WindowFrameExclusion::Group
            } else if self.eat_keyword(Keyword::Ties).is_some() {
                WindowFrameExclusion::Ties
            } else {
                self.expect_keyword(Keyword::No)?;
                self.expect_keyword(Keyword::Others)?;
                WindowFrameExclusion::NoOthers
            }
        } else {
            WindowFrameExclusion::NoOthers
        })
    }

    pub(super) fn parse_window_bound(&mut self) -> Result<WindowFrameBound, ParserError> {
        let start = self.current().span;
        if self.at_keyword(Keyword::Current)
            && self.peek(1).kind == TokenKind::Keyword(Keyword::Row)
        {
            self.bump();
            self.bump();
            return Ok(WindowFrameBound::CurrentRow(self.span_since(start)));
        }
        let value = if self.at_keyword(Keyword::Unbounded)
            && matches!(
                self.peek(1).kind,
                TokenKind::Keyword(Keyword::Preceding | Keyword::Following)
            ) {
            self.bump();
            None
        } else {
            Some(self.parse_expression()?)
        };
        if self.eat_keyword(Keyword::Preceding).is_some() {
            Ok(WindowFrameBound::Preceding(value, self.span_since(start)))
        } else {
            self.expect_keyword(Keyword::Following)?;
            Ok(WindowFrameBound::Following(value, self.span_since(start)))
        }
    }
}
