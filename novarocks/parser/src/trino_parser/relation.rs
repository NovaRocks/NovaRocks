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
    pub(super) fn parse_relation(&mut self) -> Result<Relation, ParserError> {
        let start = self.current().span;
        let mut left = self.parse_sampled_relation()?;
        loop {
            if self.eat_keyword(Keyword::Cross).is_some() {
                self.expect_keyword(Keyword::Join)?;
                let right = self.parse_sampled_relation()?;
                left = Relation::Join(AstBox::new(Join {
                    kind: JoinType::Cross,
                    left: AstBox::new(left),
                    right: AstBox::new(right),
                    criteria: None,
                    span: self.span_since(start),
                }));
                continue;
            }
            let natural = self.eat_keyword(Keyword::Natural);
            let kind = if self.eat_keyword(Keyword::Left).is_some() {
                self.eat_keyword(Keyword::Outer);
                JoinType::Left
            } else if self.eat_keyword(Keyword::Right).is_some() {
                self.eat_keyword(Keyword::Outer);
                JoinType::Right
            } else if self.eat_keyword(Keyword::Full).is_some() {
                self.eat_keyword(Keyword::Outer);
                JoinType::Full
            } else if self.eat_keyword(Keyword::Inner).is_some()
                || natural.is_some()
                || self.at_keyword(Keyword::Join)
            {
                JoinType::Inner
            } else {
                break;
            };
            self.expect_keyword(Keyword::Join)?;
            // The grammar's explicit-criteria right operand is relation,
            // whereas CROSS and NATURAL take sampledRelation.
            let right = if natural.is_some() {
                self.parse_sampled_relation()?
            } else {
                self.parse_relation()?
            };
            let criteria = if let Some(span) = natural {
                JoinCriteria::Natural(NaturalJoin { span })
            } else if let Some(on) = self.eat_keyword(Keyword::On) {
                let expression = self.parse_expression()?;
                JoinCriteria::On(JoinOn {
                    expression,
                    span: self.span_since(on),
                })
            } else {
                let using = self.expect_keyword(Keyword::Using)?;
                let columns = self.parse_column_aliases()?;
                JoinCriteria::Using(JoinUsing {
                    columns,
                    span: self.span_since(using),
                })
            };
            left = Relation::Join(AstBox::new(Join {
                kind,
                left: AstBox::new(left),
                right: AstBox::new(right),
                criteria: Some(criteria),
                span: self.span_since(start),
            }));
        }
        Ok(left)
    }

    fn parse_sampled_relation(&mut self) -> Result<Relation, ParserError> {
        let start = self.current().span;
        let mut relation = self.parse_aliased_relation()?;
        if self.at_keyword(Keyword::MatchRecognize) {
            let input_extent = self.span_since(start);
            relation = self.parse_pattern_recognition(relation, input_extent)?;
        }
        if self.at_keyword(Keyword::Pivot) {
            return Err(self.error("pivot syntax (next parser slice)"));
        }
        if self.eat_keyword(Keyword::Tablesample).is_some() {
            let kind = if self.eat_keyword(Keyword::Bernoulli).is_some() {
                SamplingType::Bernoulli
            } else {
                self.expect_keyword(Keyword::System)?;
                SamplingType::System
            };
            self.expect_symbol(Symbol::LParen)?;
            let sample_percentage = self.parse_expression()?;
            self.expect_symbol(Symbol::RParen)?;
            relation = Relation::Sampled(AstBox::new(SampledRelation {
                relation: AstBox::new(relation),
                kind,
                sample_percentage,
                span: self.span_since(start),
            }));
        }
        Ok(relation)
    }

    fn parse_aliased_relation(&mut self) -> Result<Relation, ParserError> {
        let start = self.current().span;
        let relation = self.parse_relation_primary()?;
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
                relation: AstBox::new(relation),
                alias,
                column_names,
                span: self.span_since(start),
            })))
        } else {
            Ok(relation)
        }
    }

    pub(super) fn relation_clause_follows_alias(&self) -> bool {
        match self.peek(0).kind {
            TokenKind::Keyword(Keyword::Tablesample) => {
                matches!(
                    self.peek(1).kind,
                    TokenKind::Keyword(Keyword::Bernoulli | Keyword::System)
                ) && self.peek(2).kind == TokenKind::Symbol(Symbol::LParen)
            }
            TokenKind::Keyword(Keyword::Pivot | Keyword::MatchRecognize)
                if self.peek(1).kind == TokenKind::Symbol(Symbol::LParen) =>
            {
                // These non-reserved words can also be real aliases with a
                // column-name list. Keep that grammar alternative intact.
                !self.relation_alias_columns_ahead(1)
            }
            _ => false,
        }
    }

    fn relation_alias_columns_ahead(&self, opening: usize) -> bool {
        let mut index = opening + 1;
        if !self.is_identifier(index) {
            return false;
        }
        index += 1;
        while self.peek(index).kind == TokenKind::Symbol(Symbol::Comma) {
            index += 1;
            if !self.is_identifier(index) {
                return false;
            }
            index += 1;
        }
        self.peek(index).kind == TokenKind::Symbol(Symbol::RParen)
    }

    #[recursive::recursive]
    fn parse_relation_primary(&mut self) -> Result<Relation, ParserError> {
        let start = self.current().span;
        if self.eat_keyword(Keyword::Unnest).is_some() {
            self.expect_symbol(Symbol::LParen)?;
            let mut expressions = vec![self.parse_expression()?];
            while self.eat_symbol(Symbol::Comma).is_some() {
                expressions.push(self.parse_expression()?);
            }
            self.expect_symbol(Symbol::RParen)?;
            let with_ordinality = if self.eat_keyword(Keyword::With).is_some() {
                self.expect_keyword(Keyword::Ordinality)?;
                true
            } else {
                false
            };
            return Ok(Relation::Unnest(AstBox::new(Unnest {
                expressions: expressions.into(),
                with_ordinality,
                span: self.span_since(start),
            })));
        }
        if self.at_keyword(Keyword::Lateral)
            && self.peek(1).kind == TokenKind::Symbol(Symbol::LParen)
        {
            self.bump();
            self.bump();
            let query = self.parse_query()?;
            self.expect_symbol(Symbol::RParen)?;
            return Ok(Relation::Lateral(AstBox::new(Lateral {
                query: AstBox::new(query),
                span: self.span_since(start),
            })));
        }
        if self.at_keyword(Keyword::JsonTable) {
            return Ok(Relation::JsonTable(AstBox::new(self.parse_json_table()?)));
        }
        if self.at_keyword(Keyword::Table) {
            return Err(self.error("table-function syntax (next parser slice)"));
        }
        if self.at_keyword(Keyword::Nearest)
            && self.peek(1).kind == TokenKind::Symbol(Symbol::LParen)
        {
            return Err(self.error("NEAREST syntax (next parser slice)"));
        }
        if self.eat_symbol(Symbol::LParen).is_some() {
            if self.relation_query_ahead() {
                let checkpoint = self.checkpoint();
                let query = (|| {
                    let query = self.parse_query()?;
                    self.expect_symbol(Symbol::RParen)?;
                    Ok::<_, ParserError>(query)
                })();
                match query {
                    Ok(query) => {
                        return Ok(Relation::TableSubquery(AstBox::new(TableSubquery {
                            query: AstBox::new(query),
                            span: self.span_since(start),
                        })));
                    }
                    Err(ParserError::Syntax(_)) => self.restore(checkpoint),
                    Err(error @ ParserError::Construction(_)) => return Err(error),
                }
            }
            let relation = self.parse_relation()?;
            self.expect_symbol(Symbol::RParen)?;
            return Ok(relation);
        }
        let name = self.qualified_name()?;
        let query_period = if let Some(period_start) = self.eat_keyword(Keyword::For) {
            let range_type = if self.eat_keyword(Keyword::Timestamp).is_some() {
                QueryPeriodRangeType::Timestamp
            } else {
                self.expect_keyword(Keyword::Version)?;
                QueryPeriodRangeType::Version
            };
            self.expect_keyword(Keyword::As)?;
            self.expect_keyword(Keyword::Of)?;
            let end = self.parse_value_expression()?;
            Some(QueryPeriod {
                start: None,
                end: Some(AstBox::new(end)),
                range_type,
                span: self.span_since(period_start),
            })
        } else {
            None
        };
        Ok(Relation::Table(AstBox::new(Table {
            name,
            query_period,
            branch: None,
            span: self.span_since(start),
        })))
    }

    fn relation_query_ahead(&self) -> bool {
        let mut index = 0;
        while self.peek(index).kind == TokenKind::Symbol(Symbol::LParen) {
            index += 1;
        }
        matches!(
            self.peek(index).kind,
            TokenKind::Keyword(Keyword::Select | Keyword::Values | Keyword::With)
        ) || (self.peek(index).kind == TokenKind::Keyword(Keyword::Table)
            && self.peek(index + 1).kind != TokenKind::Symbol(Symbol::LParen))
    }
}
