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
    #[recursive::recursive]
    pub(super) fn parse_root_query_with_session(&mut self) -> Result<Query, ParserError> {
        let start = self.current().span;
        let parameter_start = self.next_parameter;
        let mut properties = Vec::new();
        // SESSION is also an identifier. A CTE named SESSION has AS or a
        // column-alias list here, not the start of a session property name.
        if self.at_keyword(Keyword::With)
            && self.peek(1).kind == TokenKind::Keyword(Keyword::Session)
            && self.is_identifier(2)
        {
            self.bump();
            self.bump();
            loop {
                let property_start = self.current().span;
                let name = self.qualified_name()?;
                self.expect_symbol(Symbol::Eq)?;
                let value = self.parse_expression()?;
                properties.push(SessionProperty {
                    name,
                    value,
                    span: self.span_since(property_start),
                });
                if self.eat_symbol(Symbol::Comma).is_none() {
                    break;
                }
            }
        }
        let properties_end = self.next_parameter;
        let mut query = self.parse_root_query()?;
        let parameter_end = self.next_parameter;
        self.reorder_parameter_groups(
            parameter_start,
            &[
                properties_end..parameter_end,
                parameter_start..properties_end,
            ],
        );
        query.session_properties = properties.into();
        query.span = self.span_since(start);
        Ok(query)
    }

    #[recursive::recursive]
    pub(super) fn parse_root_query(&mut self) -> Result<Query, ParserError> {
        if self.at_keyword(Keyword::With)
            && self.peek(1).kind == TokenKind::Keyword(Keyword::Function)
            && self.is_identifier(2)
        {
            return Err(self.error("inline function specification (next parser slice)"));
        }
        let parameter_start = self.next_parameter;
        let query = self.parse_query()?;
        let parameter_end = self.next_parameter;
        // This slice has no inline function declarations. Keep the actual
        // rootQuery visit boundary explicit for the shared parameter author.
        self.reorder_parameter_groups(
            parameter_start,
            &[
                parameter_start..parameter_end,
                parameter_start..parameter_start,
            ],
        );
        Ok(query)
    }

    #[recursive::recursive]
    pub(super) fn parse_query(&mut self) -> Result<Query, ParserError> {
        let start = self.current().span;
        let parameter_start = self.next_parameter;
        let with = if let Some(with_start) = self.eat_keyword(Keyword::With) {
            let recursive = self.eat_keyword(Keyword::Recursive).is_some();
            let mut ctes = Vec::new();
            loop {
                let cte_start = self.current().span;
                let name = self.ident()?;
                let columns = if self.at_symbol(Symbol::LParen) {
                    Some(self.parse_column_aliases()?)
                } else {
                    None
                };
                self.expect_keyword(Keyword::As)?;
                self.expect_symbol(Symbol::LParen)?;
                let query = self.parse_query()?;
                self.expect_symbol(Symbol::RParen)?;
                ctes.push(Cte {
                    name,
                    columns,
                    query: AstBox::new(query),
                    span: self.span_since(cte_start),
                });
                if self.eat_symbol(Symbol::Comma).is_none() {
                    break;
                }
            }
            Some(With {
                recursive,
                ctes: ctes.into(),
                span: self.span_since(with_start),
            })
        } else {
            None
        };
        let with_end = self.next_parameter;
        let mut query = self.parse_query_no_with()?;
        let parameter_end = self.next_parameter;
        // AstBuilder visits queryNoWith before the preceding WITH queries.
        self.reorder_parameter_groups(
            parameter_start,
            &[with_end..parameter_end, parameter_start..with_end],
        );
        query.with = with;
        query.span = self.span_since(start);
        Ok(query)
    }

    #[recursive::recursive]
    pub(super) fn parse_query_no_with(&mut self) -> Result<Query, ParserError> {
        let start = self.current().span;
        let mut body = self.parse_set_expression(0)?;
        let modifiers = self.parse_query_modifiers()?;
        let modifiers = if let SetExpr::Select(select) = &mut body {
            select.modifiers = modifiers;
            select.span = self.span_since(start);
            QueryModifiers::default()
        } else {
            modifiers
        };
        Ok(Query {
            session_properties: Vec::new().into(),
            functions: Vec::new().into(),
            with: None,
            body: AstBox::new(body),
            modifiers,
            span: self.span_since(start),
        })
    }

    #[recursive::recursive]
    fn parse_set_expression(&mut self, minimum: u8) -> Result<SetExpr, ParserError> {
        let mut left = self.parse_query_primary()?;
        loop {
            let (operator, precedence) = match self.peek(0).kind {
                TokenKind::Keyword(Keyword::Union) => (SetOperator::Union, 1),
                TokenKind::Keyword(Keyword::Except) => (SetOperator::Except, 1),
                TokenKind::Keyword(Keyword::Intersect) => (SetOperator::Intersect, 2),
                _ => break,
            };
            if precedence < minimum {
                break;
            }
            self.bump();
            let distinct = if self.eat_keyword(Keyword::All).is_some() {
                false
            } else {
                self.eat_keyword(Keyword::Distinct);
                true
            };
            let corresponding = if let Some(start) = self.eat_keyword(Keyword::Corresponding) {
                let columns = if self.eat_keyword(Keyword::By).is_some() {
                    self.parse_column_aliases()?
                } else {
                    Vec::new()
                };
                Some(Corresponding {
                    columns,
                    span: self.span_since(start),
                })
            } else {
                None
            };
            let right = self.parse_set_expression(precedence + 1)?;
            let span = crate::Span::new(left.span().start(), self.last_span().end());
            let inputs = match left {
                SetExpr::SetOperation(previous)
                    if operator == SetOperator::Union
                        && !distinct
                        && corresponding.is_none()
                        && previous.operator == SetOperator::Union
                        && !previous.distinct
                        && previous.corresponding.is_none() =>
                {
                    let mut inputs = previous.inputs.into_inner();
                    inputs.push(right);
                    inputs
                }
                other => vec![other, right],
            };
            // Parentheses are real TableSubquery nodes, so this cannot flatten
            // through their grammar boundary.
            left = SetExpr::SetOperation(SetOperation {
                inputs: inputs.into(),
                operator,
                distinct,
                corresponding,
                span,
            });
        }
        Ok(left)
    }

    #[recursive::recursive]
    fn parse_query_primary(&mut self) -> Result<SetExpr, ParserError> {
        if self.at_keyword(Keyword::Select) {
            return Ok(SetExpr::Select(AstBox::new(self.parse_select()?)));
        }
        if let Some(start) = self.eat_keyword(Keyword::Values) {
            let mut rows = vec![self.parse_expression()?];
            while self.eat_symbol(Symbol::Comma).is_some() {
                rows.push(self.parse_expression()?);
            }
            return Ok(SetExpr::Values(Values {
                rows: rows.into(),
                span: self.span_since(start),
            }));
        }
        if let Some(start) = self.eat_keyword(Keyword::Table) {
            let name = self.qualified_name()?;
            return Ok(SetExpr::Table(AstBox::new(Table {
                name,
                query_period: None,
                branch: None,
                span: self.span_since(start),
            })));
        }
        if let Some(start) = self.eat_symbol(Symbol::LParen) {
            let query = self.parse_query_no_with()?;
            self.expect_symbol(Symbol::RParen)?;
            return Ok(SetExpr::Query(AstBox::new(TableSubquery {
                query: AstBox::new(query),
                span: self.span_since(start),
            })));
        }
        Err(self.error("SELECT, VALUES, TABLE or a parenthesized query"))
    }

    fn parse_select(&mut self) -> Result<Select, ParserError> {
        let start = self.expect_keyword(Keyword::Select)?;
        let distinct = self.parse_optional_distinct();
        let mut projection = vec![self.parse_select_item()?];
        while self.eat_symbol(Symbol::Comma).is_some() {
            projection.push(self.parse_select_item()?);
        }
        let mut from = if self.eat_keyword(Keyword::From).is_some() {
            Some(self.parse_relation()?)
        } else {
            None
        };
        if from.is_some() {
            while self.eat_symbol(Symbol::Comma).is_some() {
                let right = self.parse_relation()?;
                from = Some(Relation::Join(AstBox::new(Join {
                    left: AstBox::new(from.take().expect("FROM relation already parsed")),
                    right: AstBox::new(right),
                    kind: JoinType::Implicit,
                    criteria: None,
                    span: self.span_since(start),
                })));
            }
        }
        let selection = if self.eat_keyword(Keyword::Where).is_some() {
            Some(self.parse_expression()?)
        } else {
            None
        };
        let group_by = if self.eat_keyword(Keyword::Group).is_some() {
            self.expect_keyword(Keyword::By)?;
            Some(self.parse_group_by()?)
        } else {
            None
        };
        let having = if self.eat_keyword(Keyword::Having).is_some() {
            Some(self.parse_expression()?)
        } else {
            None
        };
        let mut windows = Vec::new();
        if self.eat_keyword(Keyword::Window).is_some() {
            loop {
                let definition_start = self.current().span;
                let name = self.ident()?;
                self.expect_keyword(Keyword::As)?;
                self.expect_symbol(Symbol::LParen)?;
                let specification = self.parse_window_specification()?;
                self.expect_symbol(Symbol::RParen)?;
                windows.push(WindowDefinition {
                    name,
                    specification,
                    span: self.span_since(definition_start),
                });
                if self.eat_symbol(Symbol::Comma).is_none() {
                    break;
                }
            }
        }
        Ok(Select {
            distinct,
            projection: projection.into(),
            from,
            selection,
            group_by,
            having,
            legacy: Default::default(),
            windows: windows.into(),
            modifiers: QueryModifiers::default(),
            span: self.span_since(start),
        })
    }

    fn parse_select_item(&mut self) -> Result<SelectItem, ParserError> {
        let start = self.current().span;
        if self.eat_symbol(Symbol::Star).is_some() {
            return Ok(SelectItem::AllColumns(AstBox::new(AllColumns {
                target: None,
                aliases: Vec::new(),
                span: self.span_since(start),
            })));
        }
        let (expression, primary) = self.parse_projection_expression()?;
        if self.at_symbol(Symbol::Dot) && self.peek(1).kind == TokenKind::Symbol(Symbol::Star) {
            if !primary {
                return Err(self.error("a primary expression before .*"));
            }
            self.bump();
            self.bump();
            let aliases = if self.eat_keyword(Keyword::As).is_some() {
                self.parse_column_aliases()?
            } else {
                Vec::new()
            };
            return Ok(SelectItem::AllColumns(AstBox::new(AllColumns {
                target: Some(expression),
                aliases,
                span: self.span_since(start),
            })));
        }
        let alias = if self.eat_keyword(Keyword::As).is_some()
            || (self.is_identifier(0) && !self.query_clause_follows_alias())
        {
            Some(self.ident()?)
        } else {
            None
        };
        Ok(SelectItem::SingleColumn(AstBox::new(SingleColumn {
            expression,
            alias,
            span: self.span_since(start),
        })))
    }

    pub(super) fn parse_column_aliases(&mut self) -> Result<Vec<Ident>, ParserError> {
        self.expect_symbol(Symbol::LParen)?;
        let mut columns = vec![self.ident()?];
        while self.eat_symbol(Symbol::Comma).is_some() {
            columns.push(self.ident()?);
        }
        self.expect_symbol(Symbol::RParen)?;
        Ok(columns)
    }

    pub(super) fn query_clause_follows_alias(&self) -> bool {
        let count = |kind| {
            matches!(
                kind,
                TokenKind::Integer | TokenKind::Symbol(Symbol::Question)
            )
        };
        match self.peek(0).kind {
            TokenKind::Keyword(Keyword::Offset) => count(self.peek(1).kind),
            TokenKind::Keyword(Keyword::Limit) => {
                count(self.peek(1).kind) || self.peek(1).kind == TokenKind::Keyword(Keyword::All)
            }
            TokenKind::Keyword(Keyword::Fetch) => matches!(
                self.peek(1).kind,
                TokenKind::Keyword(Keyword::First | Keyword::Next)
            ),
            TokenKind::Keyword(Keyword::Window) => {
                self.is_identifier(1) && self.peek(2).kind == TokenKind::Keyword(Keyword::As)
            }
            _ => false,
        }
    }

    fn parse_optional_distinct(&mut self) -> bool {
        if self.eat_keyword(Keyword::Distinct).is_some() {
            true
        } else {
            self.eat_keyword(Keyword::All);
            false
        }
    }

    fn parse_group_by(&mut self) -> Result<GroupBy, ParserError> {
        let start = self.current().span;
        let distinct = self.parse_optional_distinct();
        let mut elements = vec![self.parse_grouping_element()?];
        while self.eat_symbol(Symbol::Comma).is_some() {
            elements.push(self.parse_grouping_element()?);
        }
        Ok(GroupBy {
            distinct,
            elements: elements.into(),
            span: self.span_since(start),
        })
    }

    fn parse_grouping_element(&mut self) -> Result<GroupingElement, ParserError> {
        let start = self.current().span;
        if self.eat_keyword(Keyword::Auto).is_some() {
            return Ok(GroupingElement::Auto(AutoGroupBy { span: start }));
        }
        let kind = if self.eat_keyword(Keyword::Rollup).is_some() {
            Some(GroupingSetsKind::Rollup)
        } else if self.eat_keyword(Keyword::Cube).is_some() {
            Some(GroupingSetsKind::Cube)
        } else if self.at_keyword(Keyword::Grouping)
            && self.peek(1).kind == TokenKind::Keyword(Keyword::Sets)
        {
            self.bump();
            self.bump();
            Some(GroupingSetsKind::Explicit)
        } else {
            None
        };
        if let Some(kind) = kind {
            self.expect_symbol(Symbol::LParen)?;
            // The pinned GroupingSets constructor rejects an empty outer list.
            if self.at_symbol(Symbol::RParen) {
                return Err(if kind == GroupingSetsKind::Explicit {
                    self.error("at least one grouping set")
                } else {
                    self.construction_error("at least one grouping set")
                });
            }
            let mut sets = vec![self.parse_grouping_set()?.into()];
            while self.eat_symbol(Symbol::Comma).is_some() {
                sets.push(self.parse_grouping_set()?.into());
            }
            self.expect_symbol(Symbol::RParen)?;
            return Ok(GroupingElement::Sets(GroupingSets {
                kind,
                sets: sets.into(),
                span: self.span_since(start),
            }));
        }
        let columns = self.parse_grouping_set()?;
        Ok(GroupingElement::Simple(SimpleGroupBy {
            columns: columns.into(),
            span: self.span_since(start),
        }))
    }

    fn parse_grouping_set(&mut self) -> Result<Vec<Expr>, ParserError> {
        if self.grouping_parentheses_are_list() {
            self.expect_symbol(Symbol::LParen)?;
            let mut columns = Vec::new();
            if !self.at_symbol(Symbol::RParen) {
                columns.push(self.parse_expression()?);
                while self.eat_symbol(Symbol::Comma).is_some() {
                    columns.push(self.parse_expression()?);
                }
            }
            self.expect_symbol(Symbol::RParen)?;
            Ok(columns)
        } else {
            Ok(vec![self.parse_expression()?])
        }
    }

    fn grouping_parentheses_are_list(&self) -> bool {
        if !self.at_symbol(Symbol::LParen) {
            return false;
        }
        let mut depth = 0;
        let mut index = 0;
        loop {
            match self.peek(index).kind {
                TokenKind::Symbol(Symbol::LParen) => depth += 1,
                TokenKind::Symbol(Symbol::RParen) => {
                    depth -= 1;
                    if depth == 0 {
                        return matches!(
                            self.peek(index + 1).kind,
                            TokenKind::End
                                | TokenKind::Symbol(
                                    Symbol::Comma | Symbol::RParen | Symbol::Semicolon
                                )
                                | TokenKind::Keyword(
                                    Keyword::Having
                                        | Keyword::Window
                                        | Keyword::Order
                                        | Keyword::Offset
                                        | Keyword::Limit
                                        | Keyword::Fetch
                                        | Keyword::Union
                                        | Keyword::Except
                                        | Keyword::Intersect
                                )
                        );
                    }
                }
                TokenKind::End => return false,
                _ => {}
            }
            index += 1;
        }
    }

    fn parse_query_modifiers(&mut self) -> Result<QueryModifiers, ParserError> {
        let order_by = if self.at_keyword(Keyword::Order) {
            Some(self.parse_order_by()?)
        } else {
            None
        };
        let offset = if let Some(start) = self.eat_keyword(Keyword::Offset) {
            let row_count = self.parse_row_count()?;
            if self.eat_keyword(Keyword::Row).is_none() {
                self.eat_keyword(Keyword::Rows);
            }
            Some(Offset {
                row_count,
                span: self.span_since(start),
            })
        } else {
            None
        };
        let limit = if let Some(start) = self.eat_keyword(Keyword::Limit) {
            let row_count = if let Some(span) = self.eat_keyword(Keyword::All) {
                Expr::AllRows(AllRows { span })
            } else {
                self.parse_row_count()?
            };
            Some(QueryLimit::Limit(Limit {
                row_count,
                span: self.span_since(start),
            }))
        } else if let Some(start) = self.eat_keyword(Keyword::Fetch) {
            if self.eat_keyword(Keyword::First).is_none() {
                self.expect_keyword(Keyword::Next)?;
            }
            let row_count = if matches!(
                self.peek(0).kind,
                TokenKind::Integer | TokenKind::Symbol(Symbol::Question)
            ) {
                Some(self.parse_row_count()?)
            } else {
                None
            };
            if self.eat_keyword(Keyword::Row).is_none() {
                self.expect_keyword(Keyword::Rows)?;
            }
            let with_ties = if self.eat_keyword(Keyword::Only).is_some() {
                false
            } else {
                self.expect_keyword(Keyword::With)?;
                self.expect_keyword(Keyword::Ties)?;
                true
            };
            Some(QueryLimit::FetchFirst(FetchFirst {
                row_count,
                with_ties,
                span: self.span_since(start),
            }))
        } else {
            None
        };
        Ok(QueryModifiers {
            order_by,
            offset,
            limit,
        })
    }

    fn parse_row_count(&mut self) -> Result<Expr, ParserError> {
        if let Some(span) = self.eat_symbol(Symbol::Question) {
            return Ok(Expr::Parameter(self.new_parameter(span)));
        }
        if self.peek(0).kind != TokenKind::Integer {
            return Err(self.error("an integer or parameter row count"));
        }
        Ok(Expr::Literal(self.parse_literal()?))
    }
}
