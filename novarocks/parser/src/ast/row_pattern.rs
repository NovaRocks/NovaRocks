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

//! Typed row-pattern companions shared by complete window frames.

use super::{AstBox, AstVec, Expr, Fold, Ident, SyntaxEq, Visit};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeasureDefinition {
    pub expression: Expr,
    pub name: Ident,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubsetDefinition {
    pub name: Ident,
    pub identifiers: Vec<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VariableDefinition {
    pub name: Ident,
    pub expression: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PatternSearchMode {
    pub mode: PatternSearchModeKind,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PatternSearchModeKind {
    Initial,
    Seek,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PatternInteger {
    pub lexeme: String,
    pub parsed_value: i64,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PatternAnchorKind {
    PartitionStart,
    PartitionEnd,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SkipTo {
    PastLast { span: Span },
    Next { span: Span },
    First { identifier: Ident, span: Span },
    Last { identifier: Ident, span: Span },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowPattern {
    Variable {
        name: Ident,
        span: Span,
    },
    Empty {
        span: Span,
    },
    Alternation {
        patterns: AstVec<RowPattern>,
        span: Span,
    },
    Concatenation {
        patterns: AstVec<RowPattern>,
        span: Span,
    },
    Permutation {
        patterns: AstVec<RowPattern>,
        span: Span,
    },
    Anchor {
        kind: PatternAnchorKind,
        span: Span,
    },
    Excluded {
        pattern: AstBox<RowPattern>,
        span: Span,
    },
    Quantified {
        pattern: AstBox<RowPattern>,
        quantifier: PatternQuantifier,
        span: Span,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PatternQuantifier {
    ZeroOrMore {
        greedy: bool,
        span: Span,
    },
    OneOrMore {
        greedy: bool,
        span: Span,
    },
    ZeroOrOne {
        greedy: bool,
        span: Span,
    },
    Range {
        greedy: bool,
        at_least: Option<PatternInteger>,
        at_most: Option<PatternInteger>,
        span: Span,
    },
}

impl MeasureDefinition {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SubsetDefinition {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl VariableDefinition {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl PatternSearchMode {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl PatternInteger {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SkipTo {
    pub const fn span(&self) -> Span {
        match self {
            Self::PastLast { span }
            | Self::Next { span }
            | Self::First { span, .. }
            | Self::Last { span, .. } => *span,
        }
    }
}

impl RowPattern {
    pub const fn span(&self) -> Span {
        match self {
            Self::Variable { span, .. }
            | Self::Empty { span }
            | Self::Alternation { span, .. }
            | Self::Concatenation { span, .. }
            | Self::Permutation { span, .. }
            | Self::Anchor { span, .. }
            | Self::Excluded { span, .. }
            | Self::Quantified { span, .. } => *span,
        }
    }
}

impl PatternQuantifier {
    pub const fn span(&self) -> Span {
        match self {
            Self::ZeroOrMore { span, .. }
            | Self::OneOrMore { span, .. }
            | Self::ZeroOrOne { span, .. }
            | Self::Range { span, .. } => *span,
        }
    }
}

#[recursive::recursive]
pub fn walk_measure_definition<V: Visit + ?Sized>(visitor: &mut V, node: &MeasureDefinition) {
    visitor.visit_expr(&node.expression);
    visitor.visit_ident(&node.name);
}

#[recursive::recursive]
pub fn walk_subset_definition<V: Visit + ?Sized>(visitor: &mut V, node: &SubsetDefinition) {
    visitor.visit_ident(&node.name);
    for child in &node.identifiers {
        visitor.visit_ident(child);
    }
}

#[recursive::recursive]
pub fn walk_variable_definition<V: Visit + ?Sized>(visitor: &mut V, node: &VariableDefinition) {
    visitor.visit_ident(&node.name);
    visitor.visit_expr(&node.expression);
}

#[recursive::recursive]
pub fn walk_pattern_search_mode<V: Visit + ?Sized>(visitor: &mut V, node: &PatternSearchMode) {
    let _ = (visitor, node);
}

#[recursive::recursive]
pub fn walk_pattern_integer<V: Visit + ?Sized>(visitor: &mut V, node: &PatternInteger) {
    let _ = (visitor, node);
}

#[recursive::recursive]
pub fn walk_skip_to<V: Visit + ?Sized>(visitor: &mut V, node: &SkipTo) {
    match node {
        SkipTo::First { identifier, .. } | SkipTo::Last { identifier, .. } => {
            visitor.visit_ident(identifier)
        }
        SkipTo::PastLast { .. } | SkipTo::Next { .. } => {}
    }
}

#[recursive::recursive]
pub fn walk_row_pattern<V: Visit + ?Sized>(visitor: &mut V, node: &RowPattern) {
    match node {
        RowPattern::Variable { name, .. } => visitor.visit_ident(name),
        RowPattern::Empty { .. } | RowPattern::Anchor { .. } => {}
        RowPattern::Alternation { patterns, .. }
        | RowPattern::Concatenation { patterns, .. }
        | RowPattern::Permutation { patterns, .. } => {
            for pattern in patterns {
                visitor.visit_row_pattern(pattern);
            }
        }
        RowPattern::Excluded { pattern, .. } => visitor.visit_row_pattern(pattern),
        RowPattern::Quantified {
            pattern,
            quantifier,
            ..
        } => {
            visitor.visit_row_pattern(pattern);
            visitor.visit_pattern_quantifier(quantifier);
        }
    }
}

#[recursive::recursive]
pub fn walk_pattern_quantifier<V: Visit + ?Sized>(visitor: &mut V, node: &PatternQuantifier) {
    match node {
        PatternQuantifier::Range {
            at_least, at_most, ..
        } => {
            if let Some(bound) = at_least {
                visitor.visit_pattern_integer(bound);
            }
            if let Some(bound) = at_most {
                visitor.visit_pattern_integer(bound);
            }
        }
        PatternQuantifier::ZeroOrMore { .. }
        | PatternQuantifier::OneOrMore { .. }
        | PatternQuantifier::ZeroOrOne { .. } => {}
    }
}

#[recursive::recursive]
pub fn fold_measure_definition<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: MeasureDefinition,
) -> MeasureDefinition {
    node.expression = folder.fold_expr(node.expression);
    node.name = folder.fold_ident(node.name);
    node
}

#[recursive::recursive]
pub fn fold_subset_definition<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: SubsetDefinition,
) -> SubsetDefinition {
    node.name = folder.fold_ident(node.name);
    node.identifiers = node
        .identifiers
        .into_iter()
        .map(|child| folder.fold_ident(child))
        .collect();
    node
}

#[recursive::recursive]
pub fn fold_variable_definition<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: VariableDefinition,
) -> VariableDefinition {
    node.name = folder.fold_ident(node.name);
    node.expression = folder.fold_expr(node.expression);
    node
}

#[recursive::recursive]
pub fn fold_pattern_search_mode<F: Fold + ?Sized>(
    folder: &mut F,
    node: PatternSearchMode,
) -> PatternSearchMode {
    let _ = folder;
    node
}

#[recursive::recursive]
pub fn fold_pattern_integer<F: Fold + ?Sized>(
    folder: &mut F,
    node: PatternInteger,
) -> PatternInteger {
    let _ = folder;
    node
}

#[recursive::recursive]
pub fn fold_skip_to<F: Fold + ?Sized>(folder: &mut F, node: SkipTo) -> SkipTo {
    match node {
        SkipTo::First { identifier, span } => SkipTo::First {
            identifier: folder.fold_ident(identifier),
            span,
        },
        SkipTo::Last { identifier, span } => SkipTo::Last {
            identifier: folder.fold_ident(identifier),
            span,
        },
        other @ (SkipTo::PastLast { .. } | SkipTo::Next { .. }) => other,
    }
}

#[recursive::recursive]
pub fn fold_row_pattern<F: Fold + ?Sized>(folder: &mut F, node: RowPattern) -> RowPattern {
    match node {
        RowPattern::Variable { name, span } => RowPattern::Variable {
            name: folder.fold_ident(name),
            span,
        },
        RowPattern::Alternation { patterns, span } => RowPattern::Alternation {
            patterns: patterns
                .into_iter()
                .map(|p| folder.fold_row_pattern(p))
                .collect(),
            span,
        },
        RowPattern::Concatenation { patterns, span } => RowPattern::Concatenation {
            patterns: patterns
                .into_iter()
                .map(|p| folder.fold_row_pattern(p))
                .collect(),
            span,
        },
        RowPattern::Permutation { patterns, span } => RowPattern::Permutation {
            patterns: patterns
                .into_iter()
                .map(|p| folder.fold_row_pattern(p))
                .collect(),
            span,
        },
        RowPattern::Excluded { pattern, span } => RowPattern::Excluded {
            pattern: AstBox::new(folder.fold_row_pattern(pattern.into_inner())),
            span,
        },
        RowPattern::Quantified {
            pattern,
            quantifier,
            span,
        } => RowPattern::Quantified {
            pattern: AstBox::new(folder.fold_row_pattern(pattern.into_inner())),
            quantifier: folder.fold_pattern_quantifier(quantifier),
            span,
        },
        other @ (RowPattern::Empty { .. } | RowPattern::Anchor { .. }) => other,
    }
}

#[recursive::recursive]
pub fn fold_pattern_quantifier<F: Fold + ?Sized>(
    folder: &mut F,
    node: PatternQuantifier,
) -> PatternQuantifier {
    match node {
        PatternQuantifier::Range {
            greedy,
            at_least,
            at_most,
            span,
        } => PatternQuantifier::Range {
            greedy,
            at_least: at_least.map(|n| folder.fold_pattern_integer(n)),
            at_most: at_most.map(|n| folder.fold_pattern_integer(n)),
            span,
        },
        other @ (PatternQuantifier::ZeroOrMore { .. }
        | PatternQuantifier::OneOrMore { .. }
        | PatternQuantifier::ZeroOrOne { .. }) => other,
    }
}

fn eq_option<T: SyntaxEq>(left: &Option<T>, right: &Option<T>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.syntax_eq(right),
        (None, None) => true,
        _ => false,
    }
}

fn eq_slice<T: SyntaxEq>(left: &[T], right: &[T]) -> bool {
    left.len() == right.len() && left.iter().zip(right).all(|(l, r)| l.syntax_eq(r))
}

impl SyntaxEq for MeasureDefinition {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expression.syntax_eq(&other.expression) && self.name.syntax_eq(&other.name)
    }
}

impl SyntaxEq for SubsetDefinition {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && eq_slice(&self.identifiers, &other.identifiers)
    }
}

impl SyntaxEq for VariableDefinition {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && self.expression.syntax_eq(&other.expression)
    }
}

impl SyntaxEq for PatternSearchMode {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.mode == other.mode
    }
}

impl SyntaxEq for PatternInteger {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.lexeme == other.lexeme && self.parsed_value == other.parsed_value
    }
}

impl SyntaxEq for SkipTo {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::PastLast { .. }, Self::PastLast { .. })
            | (Self::Next { .. }, Self::Next { .. }) => true,
            (Self::First { identifier: l, .. }, Self::First { identifier: r, .. })
            | (Self::Last { identifier: l, .. }, Self::Last { identifier: r, .. }) => {
                l.syntax_eq(r)
            }
            _ => false,
        }
    }
}

impl SyntaxEq for RowPattern {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Variable { name: l, .. }, Self::Variable { name: r, .. }) => l.syntax_eq(r),
            (Self::Empty { .. }, Self::Empty { .. }) => true,
            (Self::Alternation { patterns: l, .. }, Self::Alternation { patterns: r, .. })
            | (Self::Concatenation { patterns: l, .. }, Self::Concatenation { patterns: r, .. })
            | (Self::Permutation { patterns: l, .. }, Self::Permutation { patterns: r, .. }) => {
                eq_slice(l, r)
            }
            (Self::Anchor { kind: l, .. }, Self::Anchor { kind: r, .. }) => l == r,
            (Self::Excluded { pattern: l, .. }, Self::Excluded { pattern: r, .. }) => {
                l.syntax_eq(r)
            }
            (
                Self::Quantified {
                    pattern: lp,
                    quantifier: lq,
                    ..
                },
                Self::Quantified {
                    pattern: rp,
                    quantifier: rq,
                    ..
                },
            ) => lp.syntax_eq(rp) && lq.syntax_eq(rq),
            _ => false,
        }
    }
}

impl SyntaxEq for PatternQuantifier {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ZeroOrMore { greedy: l, .. }, Self::ZeroOrMore { greedy: r, .. })
            | (Self::OneOrMore { greedy: l, .. }, Self::OneOrMore { greedy: r, .. })
            | (Self::ZeroOrOne { greedy: l, .. }, Self::ZeroOrOne { greedy: r, .. }) => l == r,
            (
                Self::Range {
                    greedy: lg,
                    at_least: ll,
                    at_most: lu,
                    ..
                },
                Self::Range {
                    greedy: rg,
                    at_least: rl,
                    at_most: ru,
                    ..
                },
            ) => lg == rg && eq_option(ll, rl) && eq_option(lu, ru),
            _ => false,
        }
    }
}
