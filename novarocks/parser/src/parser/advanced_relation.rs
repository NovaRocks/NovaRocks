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
use crate::Span;
use crate::ast::*;
use crate::token::{Keyword, Symbol, TokenKind};

impl Parser<'_> {
    #[recursive::recursive]
    pub(super) fn parse_table_function_relation(&mut self) -> Result<Relation, ParserError> {
        self.expect_keyword(Keyword::Table)?;
        self.expect_symbol(Symbol::LParen)?;
        // AstBuilder returns tableFunctionCall, whose location starts at the
        // function name rather than the enclosing TABLE token.
        let start = self.current().span;
        let name = self.qualified_name()?;
        self.expect_symbol(Symbol::LParen)?;
        let mut arguments = Vec::new();
        if !self.at_symbol(Symbol::RParen) {
            arguments.push(self.parse_table_function_argument()?);
            while self.eat_symbol(Symbol::Comma).is_some() {
                arguments.push(self.parse_table_function_argument()?);
            }
        }
        let mut copartitioning = Vec::new();
        if self.eat_keyword(Keyword::Copartition).is_some() {
            loop {
                self.expect_symbol(Symbol::LParen)?;
                let first = self.qualified_name()?;
                self.expect_symbol(Symbol::Comma)?;
                let mut tables = vec![first, self.qualified_name()?];
                while self.eat_symbol(Symbol::Comma).is_some() {
                    tables.push(self.qualified_name()?);
                }
                self.expect_symbol(Symbol::RParen)?;
                copartitioning.push(tables);
                if self.eat_symbol(Symbol::Comma).is_none() {
                    break;
                }
            }
        }
        self.expect_symbol(Symbol::RParen)?;
        let span = self.span_since(start);
        self.expect_symbol(Symbol::RParen)?;
        Ok(Relation::TableFunction(AstBox::new(
            TableFunctionInvocation {
                name,
                arguments: arguments.into(),
                copartitioning,
                span,
            },
        )))
    }

    #[recursive::recursive]
    fn parse_table_function_argument(&mut self) -> Result<TableFunctionArgument, ParserError> {
        let start = self.current().span;
        let name =
            if self.is_identifier(0) && self.peek(1).kind == TokenKind::Symbol(Symbol::FatArrow) {
                let name = self.ident()?;
                self.expect_symbol(Symbol::FatArrow)?;
                Some(name)
            } else {
                None
            };
        let value = if self.at_keyword(Keyword::Table)
            && self.peek(1).kind == TokenKind::Symbol(Symbol::LParen)
        {
            TableFunctionArgumentValue::Table(self.parse_table_function_table_argument()?)
        } else if (self.at_keyword(Keyword::Descriptor)
            && self.peek(1).kind == TokenKind::Symbol(Symbol::LParen))
            || self.null_descriptor_ahead()
        {
            let checkpoint = self.checkpoint();
            match self.parse_table_function_descriptor() {
                Ok(descriptor) if self.table_function_argument_end() => {
                    TableFunctionArgumentValue::Descriptor(descriptor)
                }
                Ok(_) | Err(ParserError::Syntax(_)) => {
                    self.restore(checkpoint);
                    TableFunctionArgumentValue::Expression(self.parse_expression()?)
                }
                Err(error @ ParserError::Construction(_)) => return Err(error),
            }
        } else {
            TableFunctionArgumentValue::Expression(self.parse_expression()?)
        };
        Ok(TableFunctionArgument {
            name,
            value,
            span: self.span_since(start),
        })
    }

    fn table_function_argument_end(&self) -> bool {
        self.at_symbol(Symbol::Comma)
            || self.at_symbol(Symbol::RParen)
            || self.at_keyword(Keyword::Copartition)
    }

    fn null_descriptor_ahead(&self) -> bool {
        self.at_keyword(Keyword::Cast)
            && self.peek(1).kind == TokenKind::Symbol(Symbol::LParen)
            && self.peek(2).kind == TokenKind::Keyword(Keyword::Null)
            && self.peek(3).kind == TokenKind::Keyword(Keyword::As)
            && self.peek(4).kind == TokenKind::Keyword(Keyword::Descriptor)
            && self.peek(5).kind == TokenKind::Symbol(Symbol::RParen)
    }

    fn parse_table_function_descriptor(
        &mut self,
    ) -> Result<TableFunctionDescriptorArgument, ParserError> {
        let start = self.current().span;
        let descriptor = if self.null_descriptor_ahead() {
            self.expect_keyword(Keyword::Cast)?;
            self.expect_symbol(Symbol::LParen)?;
            self.expect_keyword(Keyword::Null)?;
            self.expect_keyword(Keyword::As)?;
            self.expect_keyword(Keyword::Descriptor)?;
            self.expect_symbol(Symbol::RParen)?;
            None
        } else {
            self.expect_keyword(Keyword::Descriptor)?;
            self.expect_symbol(Symbol::LParen)?;
            let mut fields = vec![self.parse_descriptor_field()?];
            while self.eat_symbol(Symbol::Comma).is_some() {
                fields.push(self.parse_descriptor_field()?);
            }
            self.expect_symbol(Symbol::RParen)?;
            Some(Descriptor {
                fields,
                span: self.span_since(start),
            })
        };
        Ok(TableFunctionDescriptorArgument {
            descriptor,
            span: self.span_since(start),
        })
    }

    fn parse_descriptor_field(&mut self) -> Result<DescriptorField, ParserError> {
        let start = self.current().span;
        let name = self.ident()?;
        let data_type = if self.at_symbol(Symbol::Comma) || self.at_symbol(Symbol::RParen) {
            None
        } else {
            Some(self.parse_type()?)
        };
        Ok(DescriptorField {
            name,
            data_type,
            span: self.span_since(start),
        })
    }

    #[recursive::recursive]
    fn parse_table_function_table_argument(
        &mut self,
    ) -> Result<TableFunctionTableArgument, ParserError> {
        let start = self.expect_keyword(Keyword::Table)?;
        self.expect_symbol(Symbol::LParen)?;
        // Both alternatives use the same original token cursor. Only a syntax
        // failure may restore the query alternative's parameter events.
        let checkpoint = self.checkpoint();
        let table_attempt = (|| {
            let name = self.qualified_name()?;
            self.expect_symbol(Symbol::RParen)?;
            Ok::<_, ParserError>(name)
        })();
        let table = match table_attempt {
            Ok(name) => Relation::Table(AstBox::new(Table {
                name,
                query_period: None,
                branch: None,
                span: self.span_since(start),
            })),
            Err(ParserError::Syntax(_)) => {
                self.restore(checkpoint);
                let query = self.parse_query()?;
                self.expect_symbol(Symbol::RParen)?;
                Relation::TableSubquery(AstBox::new(TableSubquery {
                    query: AstBox::new(query),
                    span: self.span_since(start),
                }))
            }
            Err(error) => return Err(error),
        };
        let explicit = self.eat_keyword(Keyword::As).is_some();
        let table = if explicit || (self.is_identifier(0) && !self.table_argument_clause_ahead()) {
            let alias = self.ident()?;
            if !explicit && !alias.quoted && alias.value.eq_ignore_ascii_case("COPARTITION") {
                return Err(ParserError::Construction(
                    crate::ParseError::UnexpectedToken {
                        expected: "AS before an unquoted COPARTITION argument alias",
                        found: self.source[alias.span.start()..alias.span.end()].to_owned(),
                        span: alias.span,
                    }
                    .into(),
                ));
            }
            let column_names = if self.at_symbol(Symbol::LParen) {
                Some(self.parse_column_aliases()?)
            } else {
                None
            };
            Relation::Aliased(AstBox::new(AliasedRelation {
                relation: AstBox::new(table),
                alias,
                column_names,
                span: self.span_since(start),
            }))
        } else {
            table
        };
        let partition_by = if self.eat_keyword(Keyword::Partition).is_some() {
            self.expect_keyword(Keyword::By)?;
            Some(self.parse_table_argument_partition()?.into())
        } else {
            None
        };
        let empty_table_treatment = if let Some(treatment_start) = self.eat_keyword(Keyword::Prune)
        {
            self.expect_keyword(Keyword::When)?;
            self.expect_keyword(Keyword::Empty)?;
            Some(EmptyTableTreatment {
                treatment: EmptyTableTreatmentKind::Prune,
                span: self.span_since(treatment_start),
            })
        } else if let Some(treatment_start) = self.eat_keyword(Keyword::Keep) {
            self.expect_keyword(Keyword::When)?;
            self.expect_keyword(Keyword::Empty)?;
            Some(EmptyTableTreatment {
                treatment: EmptyTableTreatmentKind::Keep,
                span: self.span_since(treatment_start),
            })
        } else {
            None
        };
        let order_by = if let Some(order_start) = self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            let items = self.parse_table_argument_order()?;
            Some(OrderBy {
                items: items.into(),
                span: self.span_since(order_start),
            })
        } else {
            None
        };
        Ok(TableFunctionTableArgument {
            table: AstBox::new(table),
            partition_by,
            order_by,
            empty_table_treatment,
            span: self.span_since(start),
        })
    }

    fn table_argument_clause_ahead(&self) -> bool {
        (self.at_keyword(Keyword::Partition)
            && self.peek(1).kind == TokenKind::Keyword(Keyword::By))
            || ((self.at_keyword(Keyword::Keep) || self.at_keyword(Keyword::Prune))
                && self.peek(1).kind == TokenKind::Keyword(Keyword::When))
            || self.at_keyword(Keyword::Order)
    }

    fn table_argument_partition_end(&self) -> bool {
        self.at_symbol(Symbol::Comma)
            || self.at_symbol(Symbol::RParen)
            || self.at_keyword(Keyword::Copartition)
            || self.at_keyword(Keyword::Order)
            || ((self.at_keyword(Keyword::Keep) || self.at_keyword(Keyword::Prune))
                && self.peek(1).kind == TokenKind::Keyword(Keyword::When))
    }

    fn parse_table_argument_partition(&mut self) -> Result<Vec<Expr>, ParserError> {
        if self.at_symbol(Symbol::LParen) {
            let checkpoint = self.checkpoint();
            let attempt = (|| {
                self.expect_symbol(Symbol::LParen)?;
                let mut expressions = Vec::new();
                if !self.at_symbol(Symbol::RParen) {
                    expressions.push(self.parse_expression()?);
                    while self.eat_symbol(Symbol::Comma).is_some() {
                        expressions.push(self.parse_expression()?);
                    }
                }
                self.expect_symbol(Symbol::RParen)?;
                if !self.table_argument_partition_end() {
                    return Err(self.error("table argument partition suffix"));
                }
                Ok(expressions)
            })();
            match attempt {
                Ok(expressions) => return Ok(expressions),
                Err(ParserError::Syntax(_)) => self.restore(checkpoint),
                Err(error) => return Err(error),
            }
        }
        Ok(vec![self.parse_expression()?])
    }

    fn parse_table_argument_order(&mut self) -> Result<Vec<SortItem>, ParserError> {
        if self.at_symbol(Symbol::LParen) {
            let checkpoint = self.checkpoint();
            let attempt = (|| {
                self.expect_symbol(Symbol::LParen)?;
                let mut items = vec![self.parse_sort_item()?];
                while self.eat_symbol(Symbol::Comma).is_some() {
                    items.push(self.parse_sort_item()?);
                }
                self.expect_symbol(Symbol::RParen)?;
                if !self.at_symbol(Symbol::Comma)
                    && !self.at_symbol(Symbol::RParen)
                    && !self.at_keyword(Keyword::Copartition)
                {
                    return Err(self.error("table argument ordering suffix"));
                }
                Ok(items)
            })();
            match attempt {
                Ok(items) => return Ok(items),
                Err(ParserError::Syntax(_)) => self.restore(checkpoint),
                Err(error) => return Err(error),
            }
        }
        Ok(vec![self.parse_sort_item()?])
    }

    #[recursive::recursive]
    pub(super) fn parse_pivot(
        &mut self,
        input: Relation,
        input_extent: Span,
    ) -> Result<Relation, ParserError> {
        self.expect_keyword(Keyword::Pivot)?;
        self.expect_symbol(Symbol::LParen)?;
        let mut aggregations = vec![self.parse_pivot_aggregation()?];
        while self.eat_symbol(Symbol::Comma).is_some() {
            aggregations.push(self.parse_pivot_aggregation()?);
        }
        self.expect_keyword(Keyword::For)?;
        let parenthesized = self.eat_symbol(Symbol::LParen).is_some();
        let mut pivot_columns = vec![super::expression::name_expression(self.qualified_name()?)];
        if parenthesized {
            while self.eat_symbol(Symbol::Comma).is_some() {
                pivot_columns.push(super::expression::name_expression(self.qualified_name()?));
            }
            self.expect_symbol(Symbol::RParen)?;
        }
        self.expect_keyword(Keyword::In)?;
        self.expect_symbol(Symbol::LParen)?;
        let mut value_groups = vec![self.parse_pivot_value_group()?];
        while self.eat_symbol(Symbol::Comma).is_some() {
            value_groups.push(self.parse_pivot_value_group()?);
        }
        self.expect_symbol(Symbol::RParen)?;
        let group_by = if self.eat_keyword(Keyword::Group).is_some() {
            self.expect_keyword(Keyword::By)?;
            Some(self.parse_group_by()?)
        } else {
            None
        };
        self.expect_symbol(Symbol::RParen)?;
        let pivot = Relation::Pivot(AstBox::new(Pivot {
            input: AstBox::new(input),
            aggregations: aggregations.into(),
            pivot_columns: pivot_columns.into(),
            value_groups: value_groups.into(),
            group_by,
            span: self.span_since(input_extent),
        }));
        let explicit = self.eat_keyword(Keyword::As).is_some();
        if explicit
            || (self.is_identifier(0)
                && !self.grammar_boundary_at_current()
                && !self.query_clause_follows_alias()
                && !self.relation_clause_follows_alias())
        {
            let alias = self.ident()?;
            let column_names = if self.at_symbol(Symbol::LParen) {
                Some(self.parse_column_aliases()?)
            } else {
                None
            };
            Ok(Relation::Aliased(AstBox::new(AliasedRelation {
                relation: AstBox::new(pivot),
                alias,
                column_names,
                span: self.span_since(input_extent),
            })))
        } else {
            Ok(pivot)
        }
    }

    fn parse_pivot_aggregation(&mut self) -> Result<PivotAggregation, ParserError> {
        let start = self.current().span;
        let (expression, _) = self.parse_projection_expression()?;
        let alias = self.parse_advanced_expression_alias()?;
        Ok(PivotAggregation {
            expression,
            alias,
            span: self.span_since(start),
        })
    }

    fn parse_advanced_expression_alias(&mut self) -> Result<Option<Ident>, ParserError> {
        if self.eat_keyword(Keyword::As).is_some() || self.is_identifier(0) {
            Ok(Some(self.ident()?))
        } else {
            Ok(None)
        }
    }

    fn parse_pivot_value_group(&mut self) -> Result<PivotValueGroup, ParserError> {
        let start = self.current().span;
        if self.at_symbol(Symbol::LParen) {
            let checkpoint = self.checkpoint();
            let attempt = (|| {
                self.expect_symbol(Symbol::LParen)?;
                let first = self.parse_expression()?;
                self.expect_symbol(Symbol::Comma)?;
                let mut values = vec![first, self.parse_expression()?];
                while self.eat_symbol(Symbol::Comma).is_some() {
                    values.push(self.parse_expression()?);
                }
                self.expect_symbol(Symbol::RParen)?;
                let alias = self.parse_advanced_expression_alias()?;
                if !self.at_symbol(Symbol::Comma) && !self.at_symbol(Symbol::RParen) {
                    return Err(self.error("pivot value group suffix"));
                }
                Ok((values, alias))
            })();
            match attempt {
                Ok((values, alias)) => {
                    return Ok(PivotValueGroup {
                        values: values.into(),
                        alias,
                        span: self.span_since(start),
                    });
                }
                Err(ParserError::Syntax(_)) => self.restore(checkpoint),
                Err(error) => return Err(error),
            }
        }
        let (expression, _) = self.parse_projection_expression()?;
        let alias = self.parse_advanced_expression_alias()?;
        Ok(PivotValueGroup {
            values: vec![expression].into(),
            alias,
            span: self.span_since(start),
        })
    }

    #[recursive::recursive]
    pub(super) fn parse_nearest_relation(&mut self) -> Result<Relation, ParserError> {
        let start = self.expect_keyword(Keyword::Nearest)?;
        self.expect_symbol(Symbol::LParen)?;
        let mut delimiters = Vec::new();
        let mut candidates = Vec::new();
        // These are indices into the one original token vector. A delimiter
        // candidate must belong to this relation context, not a child query,
        // expression container, quoted identifier, string or comment.
        for (index, token) in self.tokens.iter().enumerate().skip(self.position) {
            match token.kind {
                TokenKind::Symbol(Symbol::LParen) => delimiters.push(Symbol::RParen),
                TokenKind::Symbol(Symbol::LBracket) => delimiters.push(Symbol::RBracket),
                TokenKind::Symbol(Symbol::LBrace) => delimiters.push(Symbol::RBrace),
                TokenKind::Symbol(close @ (Symbol::RParen | Symbol::RBracket | Symbol::RBrace)) => {
                    if delimiters.pop() != Some(close) {
                        break;
                    }
                }
                TokenKind::Keyword(Keyword::Match) if delimiters.is_empty() => {
                    candidates.push(index)
                }
                TokenKind::End => break,
                _ => {}
            }
        }
        let mut syntax_error = self.error("a MATCH clause in NEAREST");
        for boundary in candidates.into_iter().rev() {
            let checkpoint = self.checkpoint();
            let attempt = (|| {
                let (relation, where_clause) = self.with_grammar_boundary(boundary, |parser| {
                    parser.expect_keyword(Keyword::From)?;
                    let relation = parser.parse_relation()?;
                    let where_clause = if parser.eat_keyword(Keyword::Where).is_some() {
                        Some(parser.parse_expression()?)
                    } else {
                        None
                    };
                    if !parser.grammar_boundary_at_current() {
                        return Err(parser.error("the NEAREST MATCH delimiter"));
                    }
                    Ok((relation, where_clause))
                })?;
                self.expect_keyword(Keyword::Match)?;
                let match_expression = self.parse_expression()?;
                self.expect_symbol(Symbol::RParen)?;
                Ok(Relation::Nearest(AstBox::new(Nearest {
                    relation: AstBox::new(relation),
                    where_clause,
                    match_expression,
                    span: self.span_since(start),
                })))
            })();
            match attempt {
                Ok(relation) => return Ok(relation),
                Err(error @ ParserError::Syntax(_)) => {
                    self.restore(checkpoint);
                    syntax_error = error;
                }
                Err(error) => return Err(error),
            }
        }
        Err(syntax_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_copartition_alias_is_a_construction_failure() {
        let source = "TABLE(f(TABLE(t) copartition(a,b)))";
        let mut parser = Parser::new(source).unwrap();
        let error = parser.parse_table_function_relation().unwrap_err();
        let ParserError::Construction(crate::ParserError::Parse(
            crate::ParseError::UnexpectedToken { span, found, .. },
        )) = error
        else {
            panic!("expected a construction failure at the alias")
        };
        let start = source.find("copartition").unwrap();
        assert_eq!(span, Span::new(start, start + "copartition".len()));
        assert_eq!(found, "copartition");
    }

    #[test]
    fn syntax_alternatives_restore_only_their_actual_parameter_events() {
        let mut parser =
            Parser::new("TABLE(f(TABLE(t) PARTITION BY (?) + ? ORDER BY (?) DESC))").unwrap();
        parser.parse_table_function_relation().unwrap();
        parser.finish().unwrap();
        assert_eq!(parser.next_parameter, 3);
        assert_eq!(parser.parameter_events.len(), 3);
        let spans = parser.parameter_events;
        assert!(
            spans
                .windows(2)
                .all(|pair| pair[0].start() < pair[1].start())
        );
    }
}
