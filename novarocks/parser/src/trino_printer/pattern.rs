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

use super::*;

impl Writer {
    #[recursive::recursive]
    pub(super) fn pattern_recognition(&mut self, value: &PatternRecognitionRelation) {
        if matches!(value.input.as_ref(), Relation::Join(join) if join.kind == JoinType::Implicit) {
            unsupported("an implicit comma join as MATCH_RECOGNIZE input");
        }
        // A parenthesized relation returns its actual child in AstBuilder.
        // This preserves nested recognition, aliases and sampled inputs.
        self.character('(');
        self.relation(&value.input);
        self.text(") MATCH_RECOGNIZE (");
        if !value.partition_by.is_empty() {
            self.text("PARTITION BY ");
            self.expressions(&value.partition_by);
            self.character(' ');
        }
        if let Some(order_by) = &value.order_by {
            self.order_by(order_by);
            self.character(' ');
        }
        self.pattern_measures(&value.measures);
        match value.rows_per_match {
            None => {}
            Some(RowsPerMatch::One) => self.text("ONE ROW PER MATCH "),
            Some(RowsPerMatch::AllShowEmpty) => self.text("ALL ROWS PER MATCH SHOW EMPTY MATCHES "),
            Some(RowsPerMatch::AllOmitEmpty) => self.text("ALL ROWS PER MATCH OMIT EMPTY MATCHES "),
            Some(RowsPerMatch::AllWithUnmatched) => {
                self.text("ALL ROWS PER MATCH WITH UNMATCHED ROWS ")
            }
            Some(RowsPerMatch::Window) => {
                unsupported("the WINDOW-only RowsPerMatch value in a relation")
            }
        }
        self.pattern_after_match(value.after_match_skip_to.as_ref());
        self.pattern_search(value.pattern_search_mode.as_ref());
        self.text("PATTERN (");
        self.row_pattern(&value.pattern);
        self.text(") ");
        self.pattern_subsets(&value.subsets);
        if value.variable_definitions.is_empty() {
            unsupported("MATCH_RECOGNIZE without its required variable definitions");
        }
        self.pattern_definitions(&value.variable_definitions);
        self.character(')');
    }

    pub(super) fn pattern_measures(&mut self, values: &[MeasureDefinition]) {
        if values.is_empty() {
            return;
        }
        self.text("MEASURES ");
        for (index, value) in values.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            self.expression(&value.expression);
            self.text(" AS ");
            self.identifier(&value.name);
        }
        self.character(' ');
    }

    pub(super) fn window_pattern_tail(&mut self, value: &WindowFrame) {
        if let Some(skip) = &value.after_match_skip_to {
            self.character(' ');
            self.pattern_after_match(Some(skip));
        }
        if let Some(mode) = &value.pattern_search_mode {
            if value.after_match_skip_to.is_none() {
                self.character(' ');
            }
            self.pattern_search(Some(mode));
        }
        if let Some(pattern) = &value.pattern {
            if value.after_match_skip_to.is_none() && value.pattern_search_mode.is_none() {
                self.character(' ');
            }
            self.text("PATTERN (");
            self.row_pattern(pattern);
            self.text(") ");
        }
        if !value.subsets.is_empty() {
            if value.after_match_skip_to.is_none()
                && value.pattern_search_mode.is_none()
                && value.pattern.is_none()
            {
                self.character(' ');
            }
            self.pattern_subsets(&value.subsets);
        }
        if !value.variable_definitions.is_empty() {
            if value.after_match_skip_to.is_none()
                && value.pattern_search_mode.is_none()
                && value.pattern.is_none()
                && value.subsets.is_empty()
            {
                self.character(' ');
            }
            self.pattern_definitions(&value.variable_definitions);
        }
    }

    fn pattern_after_match(&mut self, value: Option<&SkipTo>) {
        let Some(value) = value else { return };
        self.text("AFTER MATCH SKIP ");
        match value {
            SkipTo::PastLast { .. } => self.text("PAST LAST ROW"),
            SkipTo::Next { .. } => self.text("TO NEXT ROW"),
            SkipTo::First { identifier, .. } => {
                self.text("TO FIRST ");
                self.identifier(identifier);
            }
            SkipTo::Last { identifier, .. } => {
                self.text("TO LAST ");
                self.identifier(identifier);
            }
        }
        self.character(' ');
    }

    fn pattern_search(&mut self, value: Option<&PatternSearchMode>) {
        if let Some(value) = value {
            self.text(match value.mode {
                PatternSearchModeKind::Initial => "INITIAL ",
                PatternSearchModeKind::Seek => "SEEK ",
            });
        }
    }

    fn pattern_subsets(&mut self, values: &[SubsetDefinition]) {
        if values.is_empty() {
            return;
        }
        self.text("SUBSET ");
        for (index, value) in values.iter().enumerate() {
            if value.identifiers.is_empty() {
                unsupported("an empty row-pattern subset");
            }
            if index != 0 {
                self.text(", ");
            }
            self.identifier(&value.name);
            self.text(" = (");
            self.identifiers(&value.identifiers);
            self.character(')');
        }
        self.character(' ');
    }

    fn pattern_definitions(&mut self, values: &[VariableDefinition]) {
        self.text("DEFINE ");
        for (index, value) in values.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            self.identifier(&value.name);
            self.text(" AS ");
            self.expression(&value.expression);
        }
    }

    #[recursive::recursive]
    fn row_pattern(&mut self, value: &RowPattern) {
        match value {
            RowPattern::Variable { name, .. } => self.identifier(name),
            RowPattern::Empty { .. } => self.text("()"),
            RowPattern::Alternation { patterns, .. }
            | RowPattern::Concatenation { patterns, .. } => {
                // The reference builder keeps every binary parse context.
                // Parentheses preserve either child's grouping. An n-ary or
                // singleton constructor has no corresponding grammar image.
                if patterns.len() != 2 {
                    unsupported("a non-binary row-pattern alternation or concatenation");
                }
                self.character('(');
                self.row_pattern(&patterns[0]);
                self.text(if matches!(value, RowPattern::Alternation { .. }) {
                    " | "
                } else {
                    " "
                });
                self.row_pattern(&patterns[1]);
                self.character(')');
            }
            RowPattern::Permutation { patterns, .. } => {
                if patterns.is_empty() {
                    unsupported("an empty PERMUTE pattern");
                }
                self.text("PERMUTE(");
                for (index, pattern) in patterns.iter().enumerate() {
                    if index != 0 {
                        self.text(", ");
                    }
                    self.row_pattern(pattern);
                }
                self.character(')');
            }
            RowPattern::Anchor { kind, .. } => self.character(match kind {
                PatternAnchorKind::PartitionStart => '^',
                PatternAnchorKind::PartitionEnd => '$',
            }),
            RowPattern::Excluded { pattern, .. } => {
                self.text("{- ");
                self.row_pattern(pattern);
                self.text(" -}");
            }
            RowPattern::Quantified {
                pattern,
                quantifier,
                ..
            } => {
                self.character('(');
                self.row_pattern(pattern);
                self.character(')');
                self.pattern_quantifier(quantifier);
            }
        }
    }

    fn pattern_quantifier(&mut self, value: &PatternQuantifier) {
        let greedy = match value {
            PatternQuantifier::ZeroOrMore { greedy, .. } => {
                self.character('*');
                *greedy
            }
            PatternQuantifier::OneOrMore { greedy, .. } => {
                self.character('+');
                *greedy
            }
            PatternQuantifier::ZeroOrOne { greedy, .. } => {
                self.character('?');
                *greedy
            }
            PatternQuantifier::Range {
                greedy,
                at_least,
                at_most,
                ..
            } => {
                self.character('{');
                if let Some(bound) = at_least {
                    self.text(&bound.lexeme);
                }
                self.character(',');
                if let Some(bound) = at_most {
                    self.text(&bound.lexeme);
                }
                self.character('}');
                *greedy
            }
        };
        if !greedy {
            self.character('?');
        }
    }
}
