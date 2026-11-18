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
use crate::trino_token::{Keyword, Symbol, TokenKind};

fn concatenate(left: Option<RowPattern>, right: RowPattern, span: Span) -> RowPattern {
    match left {
        Some(left) => RowPattern::Concatenation {
            patterns: vec![left, right].into(),
            span,
        },
        None => right,
    }
}

impl Parser<'_> {
    #[recursive::recursive]
    pub(super) fn parse_row_pattern(&mut self) -> Result<RowPattern, ParserError> {
        let start = self.current().span;
        let mut left = self.parse_pattern_concatenation()?;
        while self.eat_symbol(Symbol::Pipe).is_some() {
            let right = self.parse_pattern_concatenation()?;
            // AstBuilder preserves each binary parse context. Row patterns
            // have no Logical/UNION flattening exception.
            left = RowPattern::Alternation {
                patterns: vec![left, right].into(),
                span: self.span_since(start),
            };
        }
        Ok(left)
    }

    #[recursive::recursive]
    fn parse_pattern_concatenation(&mut self) -> Result<RowPattern, ParserError> {
        let start = self.current().span;
        let mut left = None;
        loop {
            let (prefix, right) = self.parse_quantified_pattern()?;
            if let Some(prefix) = prefix {
                let extent = Span::new(start.start(), prefix.span().end());
                left = Some(concatenate(left, prefix, extent));
            }
            let result = concatenate(left, right, self.span_since(start));
            if !self.pattern_primary_ahead() {
                return Ok(result);
            }
            left = Some(result);
        }
    }

    fn pattern_primary_ahead(&self) -> bool {
        self.is_identifier(0)
            || matches!(
                self.peek(0).kind,
                TokenKind::Symbol(
                    Symbol::LParen | Symbol::Caret | Symbol::Dollar | Symbol::ExcludeStart
                )
            )
    }

    #[recursive::recursive]
    fn parse_quantified_pattern(
        &mut self,
    ) -> Result<(Option<RowPattern>, RowPattern), ParserError> {
        let start = self.current().span;
        let (prefix, primary, start) = if self.at_keyword(Keyword::Permute)
            && self.peek(1).kind == TokenKind::Symbol(Symbol::LParen)
        {
            self.parse_permute_or_variable(start)?
        } else {
            (None, self.parse_pattern_primary()?, start)
        };
        let token = self.current();
        let quantifier = if self.eat_symbol(Symbol::Star).is_some() {
            let greedy = self.eat_symbol(Symbol::Question).is_none();
            Some(PatternQuantifier::ZeroOrMore {
                greedy,
                span: self.span_since(token.span),
            })
        } else if self.eat_symbol(Symbol::Plus).is_some() {
            let greedy = self.eat_symbol(Symbol::Question).is_none();
            Some(PatternQuantifier::OneOrMore {
                greedy,
                span: self.span_since(token.span),
            })
        } else if self.eat_symbol(Symbol::Question).is_some() {
            let greedy = self.eat_symbol(Symbol::Question).is_none();
            Some(PatternQuantifier::ZeroOrOne {
                greedy,
                span: self.span_since(token.span),
            })
        } else if self.eat_symbol(Symbol::LBrace).is_some() {
            let lower = if self.peek(0).kind == TokenKind::Integer {
                Some(self.parse_pattern_integer()?)
            } else {
                None
            };
            let (at_least, at_most) = if self.eat_symbol(Symbol::Comma).is_some() {
                let upper = if self.peek(0).kind == TokenKind::Integer {
                    Some(self.parse_pattern_integer()?)
                } else {
                    None
                };
                (lower, upper)
            } else {
                let Some(exact) = lower else {
                    return Err(self.error("an integer or comma in a pattern range"));
                };
                // The reference constructs two LongLiteral nodes from the
                // same real token, retaining its exact spelling and location.
                let upper = PatternInteger {
                    lexeme: self.source[exact.span.start()..exact.span.end()].to_owned(),
                    parsed_value: exact.parsed_value,
                    span: exact.span,
                };
                (Some(exact), Some(upper))
            };
            self.expect_symbol(Symbol::RBrace)?;
            let greedy = self.eat_symbol(Symbol::Question).is_none();
            // RangeQuantifier checks only non-null Optional values. Neither
            // {,} nor a descending range is rejected by its constructor.
            Some(PatternQuantifier::Range {
                greedy,
                at_least,
                at_most,
                span: self.span_since(token.span),
            })
        } else {
            None
        };
        let result = match quantifier {
            Some(quantifier) => RowPattern::Quantified {
                pattern: AstBox::new(primary),
                quantifier,
                span: self.span_since(start),
            },
            None => primary,
        };
        Ok((prefix, result))
    }

    #[recursive::recursive]
    fn parse_pattern_primary(&mut self) -> Result<RowPattern, ParserError> {
        let start = self.current().span;
        if self.eat_symbol(Symbol::LParen).is_some() {
            if self.eat_symbol(Symbol::RParen).is_some() {
                return Ok(RowPattern::Empty {
                    span: self.span_since(start),
                });
            }
            let pattern = self.parse_row_pattern()?;
            self.expect_symbol(Symbol::RParen)?;
            return Ok(pattern);
        }
        if self.eat_symbol(Symbol::Caret).is_some() {
            return Ok(RowPattern::Anchor {
                kind: PatternAnchorKind::PartitionStart,
                span: start,
            });
        }
        if self.eat_symbol(Symbol::Dollar).is_some() {
            return Ok(RowPattern::Anchor {
                kind: PatternAnchorKind::PartitionEnd,
                span: start,
            });
        }
        if self.eat_symbol(Symbol::ExcludeStart).is_some() {
            let pattern = self.parse_row_pattern()?;
            self.expect_symbol(Symbol::ExcludeEnd)?;
            return Ok(RowPattern::Excluded {
                pattern: AstBox::new(pattern),
                span: self.span_since(start),
            });
        }
        let name = self.ident()?;
        let span = name.span;
        Ok(RowPattern::Variable { name, span })
    }

    fn parse_permute_or_variable(
        &mut self,
        start: Span,
    ) -> Result<(Option<RowPattern>, RowPattern, Span), ParserError> {
        let name = self.ident()?;
        let group = self.expect_symbol(Symbol::LParen)?;
        let first = if self.eat_symbol(Symbol::RParen).is_some() {
            RowPattern::Empty {
                span: self.span_since(group),
            }
        } else {
            let pattern = self.parse_row_pattern()?;
            if self.eat_symbol(Symbol::Comma).is_some() {
                let mut patterns = vec![pattern, self.parse_row_pattern()?];
                while self.eat_symbol(Symbol::Comma).is_some() {
                    patterns.push(self.parse_row_pattern()?);
                }
                self.expect_symbol(Symbol::RParen)?;
                return Ok((
                    None,
                    RowPattern::Permutation {
                        patterns: patterns.into(),
                        span: self.span_since(start),
                    },
                    start,
                ));
            }
            self.expect_symbol(Symbol::RParen)?;
            pattern
        };
        // When the parenthesized pattern has no list separator, the reference
        // chooses its earlier identifier alternative. The variable belongs to
        // the outer concatenation; any quantifier attaches to the grouped term.
        let variable = RowPattern::Variable {
            span: name.span,
            name,
        };
        Ok((Some(variable), first, group))
    }

    fn parse_pattern_integer(&mut self) -> Result<PatternInteger, ParserError> {
        if self.peek(0).kind != TokenKind::Integer {
            return Err(self.error("a pattern integer"));
        }
        let literal = self.parse_literal()?;
        let LiteralKind::Long {
            spelling,
            parsed_value,
        } = literal.kind
        else {
            unreachable!("Integer uses the shared LongLiteral constructor")
        };
        Ok(PatternInteger {
            lexeme: spelling,
            parsed_value,
            span: literal.span,
        })
    }

    #[recursive::recursive]
    pub(super) fn parse_pattern_recognition(
        &mut self,
        input: Relation,
        input_extent: Span,
    ) -> Result<Relation, ParserError> {
        self.expect_keyword(Keyword::MatchRecognize)?;
        self.expect_symbol(Symbol::LParen)?;
        // The already parsed input is AstBuilder's first visit. Reorder only
        // this production's new children, leaving the input prefix untouched.
        let parameter_start = self.next_parameter;
        let mut partition_by = Vec::new();
        if self.eat_keyword(Keyword::Partition).is_some() {
            self.expect_keyword(Keyword::By)?;
            partition_by.push(self.parse_expression()?);
            while self.eat_symbol(Symbol::Comma).is_some() {
                partition_by.push(self.parse_expression()?);
            }
        }
        let partition_end = self.next_parameter;
        let order_by = if self.at_keyword(Keyword::Order) {
            Some(self.parse_order_by()?)
        } else {
            None
        };
        let order_end = self.next_parameter;
        let measures = self.parse_pattern_measures()?;
        let measures_end = self.next_parameter;
        let rows_per_match = self.parse_pattern_rows_per_match()?;
        let after_match_skip_to = self.parse_pattern_after_match()?;
        let pattern_search_mode = self.parse_pattern_search_mode();
        self.expect_keyword(Keyword::Pattern)?;
        self.expect_symbol(Symbol::LParen)?;
        let pattern = self.parse_row_pattern()?;
        self.expect_symbol(Symbol::RParen)?;
        let subsets = self.parse_pattern_subsets()?;
        self.expect_keyword(Keyword::Define)?;
        let variable_definitions = self.parse_pattern_definitions()?;
        self.expect_symbol(Symbol::RParen)?;
        let parameter_end = self.next_parameter;
        self.reorder_parameter_groups(
            parameter_start,
            &[
                partition_end..order_end,
                parameter_start..partition_end,
                order_end..measures_end,
                measures_end..parameter_end,
            ],
        );
        let relation = Relation::PatternRecognition(AstBox::new(PatternRecognitionRelation {
            input: AstBox::new(input),
            partition_by: partition_by.into(),
            order_by,
            measures: measures.into(),
            rows_per_match,
            after_match_skip_to,
            pattern_search_mode,
            pattern,
            subsets: subsets.into(),
            variable_definitions: variable_definitions.into(),
            span: self.span_since(input_extent),
        }));
        let explicit_alias = self.eat_keyword(Keyword::As).is_some();
        if explicit_alias
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
                span: self.span_since(input_extent),
            })))
        } else {
            Ok(relation)
        }
    }

    #[recursive::recursive]
    pub(super) fn parse_pattern_window_frame(&mut self) -> Result<WindowFrame, ParserError> {
        let start = self.current().span;
        let parameter_start = self.next_parameter;
        let measures = self.parse_pattern_measures()?;
        let measures_end = self.next_parameter;
        let units = if self.eat_keyword(Keyword::Rows).is_some() {
            WindowFrameUnits::Rows
        } else if self.eat_keyword(Keyword::Range).is_some() {
            WindowFrameUnits::Range
        } else {
            self.expect_keyword(Keyword::Groups)?;
            WindowFrameUnits::Groups
        };
        let between = self.eat_keyword(Keyword::Between).is_some();
        let start_bound = self.parse_window_bound()?;
        let end_bound = if between {
            self.expect_keyword(Keyword::And)?;
            Some(self.parse_window_bound()?)
        } else {
            None
        };
        let extent_end = self.next_parameter;
        let exclusion = self.parse_pattern_frame_exclusion()?;
        let after_match_skip_to = self.parse_pattern_after_match()?;
        let pattern_search_mode = self.parse_pattern_search_mode();
        let pattern = if self.eat_keyword(Keyword::Pattern).is_some() {
            self.expect_symbol(Symbol::LParen)?;
            let pattern = self.parse_row_pattern()?;
            self.expect_symbol(Symbol::RParen)?;
            Some(pattern)
        } else {
            None
        };
        let subsets = self.parse_pattern_subsets()?;
        let variable_definitions = if self.eat_keyword(Keyword::Define).is_some() {
            self.parse_pattern_definitions()?
        } else {
            Vec::new()
        };
        let parameter_end = self.next_parameter;
        self.reorder_parameter_groups(
            parameter_start,
            &[
                measures_end..extent_end,
                parameter_start..measures_end,
                extent_end..parameter_end,
            ],
        );
        Ok(WindowFrame {
            units,
            start_bound,
            end_bound,
            exclusion,
            measures: measures.into(),
            after_match_skip_to,
            pattern_search_mode,
            pattern,
            subsets: subsets.into(),
            variable_definitions: variable_definitions.into(),
            span: self.span_since(start),
        })
    }

    fn parse_pattern_measures(&mut self) -> Result<Vec<MeasureDefinition>, ParserError> {
        let mut measures = Vec::new();
        if self.eat_keyword(Keyword::Measures).is_some() {
            loop {
                let start = self.current().span;
                let expression = self.parse_expression()?;
                self.expect_keyword(Keyword::As)?;
                let name = self.ident()?;
                measures.push(MeasureDefinition {
                    expression,
                    name,
                    span: self.span_since(start),
                });
                if self.eat_symbol(Symbol::Comma).is_none() {
                    break;
                }
            }
        }
        Ok(measures)
    }

    fn parse_pattern_rows_per_match(&mut self) -> Result<Option<RowsPerMatch>, ParserError> {
        if self.eat_keyword(Keyword::One).is_some() {
            self.expect_keyword(Keyword::Row)?;
            self.expect_keyword(Keyword::Per)?;
            self.expect_keyword(Keyword::Match)?;
            return Ok(Some(RowsPerMatch::One));
        }
        if self.eat_keyword(Keyword::All).is_none() {
            return Ok(None);
        }
        self.expect_keyword(Keyword::Rows)?;
        self.expect_keyword(Keyword::Per)?;
        self.expect_keyword(Keyword::Match)?;
        let rows = if self.eat_keyword(Keyword::Omit).is_some() {
            self.expect_keyword(Keyword::Empty)?;
            self.expect_keyword(Keyword::Matches)?;
            RowsPerMatch::AllOmitEmpty
        } else if self.eat_keyword(Keyword::With).is_some() {
            self.expect_keyword(Keyword::Unmatched)?;
            self.expect_keyword(Keyword::Rows)?;
            RowsPerMatch::AllWithUnmatched
        } else {
            if self.eat_keyword(Keyword::Show).is_some() {
                self.expect_keyword(Keyword::Empty)?;
                self.expect_keyword(Keyword::Matches)?;
            }
            RowsPerMatch::AllShowEmpty
        };
        Ok(Some(rows))
    }

    fn parse_pattern_after_match(&mut self) -> Result<Option<SkipTo>, ParserError> {
        if self.eat_keyword(Keyword::After).is_none() {
            return Ok(None);
        }
        self.expect_keyword(Keyword::Match)?;
        let start = self.expect_keyword(Keyword::Skip)?;
        let skip = if self.eat_keyword(Keyword::Past).is_some() {
            self.expect_keyword(Keyword::Last)?;
            self.expect_keyword(Keyword::Row)?;
            SkipTo::PastLast {
                span: self.span_since(start),
            }
        } else {
            self.expect_keyword(Keyword::To)?;
            if self.at_keyword(Keyword::Next)
                && self.peek(1).kind == TokenKind::Keyword(Keyword::Row)
            {
                self.bump();
                self.bump();
                SkipTo::Next {
                    span: self.span_since(start),
                }
            } else if self.at_keyword(Keyword::First)
                && self.is_identifier(1)
                && !self.pattern_tail_clause_ahead(1)
            {
                self.bump();
                let identifier = self.ident()?;
                SkipTo::First {
                    identifier,
                    span: self.span_since(start),
                }
            } else {
                if self.at_keyword(Keyword::Last)
                    && self.is_identifier(1)
                    && !self.pattern_tail_clause_ahead(1)
                {
                    self.bump();
                }
                let identifier = self.ident()?;
                SkipTo::Last {
                    identifier,
                    span: self.span_since(start),
                }
            }
        };
        Ok(Some(skip))
    }

    fn pattern_tail_clause_ahead(&self, offset: usize) -> bool {
        match self.peek(offset).kind {
            TokenKind::Keyword(Keyword::Pattern) => {
                self.peek(offset + 1).kind == TokenKind::Symbol(Symbol::LParen)
            }
            TokenKind::Keyword(Keyword::Subset) => {
                self.is_identifier(offset + 1)
                    && self.peek(offset + 2).kind == TokenKind::Symbol(Symbol::Eq)
            }
            TokenKind::Keyword(Keyword::Define) => {
                self.is_identifier(offset + 1)
                    && self.peek(offset + 2).kind == TokenKind::Keyword(Keyword::As)
            }
            _ => false,
        }
    }

    fn parse_pattern_search_mode(&mut self) -> Option<PatternSearchMode> {
        if let Some(span) = self.eat_keyword(Keyword::Initial) {
            Some(PatternSearchMode {
                mode: PatternSearchModeKind::Initial,
                span,
            })
        } else {
            self.eat_keyword(Keyword::Seek)
                .map(|span| PatternSearchMode {
                    mode: PatternSearchModeKind::Seek,
                    span,
                })
        }
    }

    fn parse_pattern_subsets(&mut self) -> Result<Vec<SubsetDefinition>, ParserError> {
        let mut subsets = Vec::new();
        if self.eat_keyword(Keyword::Subset).is_some() {
            loop {
                let start = self.current().span;
                let name = self.ident()?;
                self.expect_symbol(Symbol::Eq)?;
                let identifiers = self.parse_column_aliases()?;
                subsets.push(SubsetDefinition {
                    name,
                    identifiers,
                    span: self.span_since(start),
                });
                if self.eat_symbol(Symbol::Comma).is_none() {
                    break;
                }
            }
        }
        Ok(subsets)
    }

    fn parse_pattern_definitions(&mut self) -> Result<Vec<VariableDefinition>, ParserError> {
        let mut definitions = Vec::new();
        loop {
            let start = self.current().span;
            let name = self.ident()?;
            self.expect_keyword(Keyword::As)?;
            let expression = self.parse_expression()?;
            definitions.push(VariableDefinition {
                name,
                expression,
                span: self.span_since(start),
            });
            if self.eat_symbol(Symbol::Comma).is_none() {
                break;
            }
        }
        Ok(definitions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trino_parser::GrammarError;

    #[test]
    fn pattern_constructor_errors_are_not_alternative_syntax_errors() {
        let mut parser = Parser::new("A{9223372036854775808}").unwrap();
        assert!(matches!(
            parser.parse_row_pattern(),
            Err(GrammarError::Construction(_))
        ));
        for source in ["A{}", "PERMUTE(, A)", "A{1,,2}"] {
            let mut parser = Parser::new(source).unwrap();
            assert!(
                matches!(parser.parse_row_pattern(), Err(GrammarError::Syntax(_))),
                "{source}"
            );
        }
    }
}
