// SPDX-License-Identifier: Apache-2.0
//! Predicate nodes preserve the pinned builder's value/predicate separation.
use super::{AstVec, Expr, Fold, SyntaxEq, Visit};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PredicatedExpr {
    pub value: Expr,
    pub predicate: Predicate,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComparisonPredicate {
    pub operator: ComparisonOperator,
    pub right: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistinctFromPredicate {
    pub negated: bool,
    pub right: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BetweenPredicate {
    pub negated: bool,
    pub symmetry: Option<BetweenSymmetry>,
    pub min: Expr,
    pub max: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BooleanTestPredicate {
    pub negated: bool,
    pub truth_value: TruthValue,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IsNullPredicate {
    pub negated: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LikePredicate {
    pub negated: bool,
    pub pattern: Expr,
    pub escape: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InPredicate {
    pub negated: bool,
    pub value_list: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OverlapsPredicate {
    pub right: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuantifiedComparisonPredicate {
    pub operator: ComparisonOperator,
    pub quantifier: ComparisonQuantifier,
    pub subquery: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatchPredicate {
    pub unique: bool,
    pub match_type: MatchType,
    pub subquery: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InListExpr {
    pub values: AstVec<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Predicate {
    Comparison(ComparisonPredicate),
    DistinctFrom(DistinctFromPredicate),
    Between(BetweenPredicate),
    BooleanTest(BooleanTestPredicate),
    IsNull(IsNullPredicate),
    Like(LikePredicate),
    In(InPredicate),
    Overlaps(OverlapsPredicate),
    QuantifiedComparison(QuantifiedComparisonPredicate),
    Match(MatchPredicate),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BetweenSymmetry {
    Symmetric,
    Asymmetric,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TruthValue {
    True,
    False,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonQuantifier {
    All,
    Any,
    Some,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchType {
    Simple,
    Partial,
    Full,
}

#[recursive::recursive]
pub fn walk_predicated_expr<V: Visit + ?Sized>(visitor: &mut V, node: &PredicatedExpr) {
    visitor.visit_expr(&node.value);
    visitor.visit_predicate(&node.predicate);
}

#[recursive::recursive]
pub fn fold_predicated_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: PredicatedExpr,
) -> PredicatedExpr {
    node.value = folder.fold_expr(node.value);
    node.predicate = folder.fold_predicate(node.predicate);
    node
}

impl PredicatedExpr {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for PredicatedExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.value.syntax_eq(&other.value) && self.predicate.syntax_eq(&other.predicate)
    }
}

pub fn walk_comparison_predicate<V: Visit + ?Sized>(visitor: &mut V, node: &ComparisonPredicate) {
    visitor.visit_expr(&node.right);
}

pub fn fold_comparison_predicate<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: ComparisonPredicate,
) -> ComparisonPredicate {
    node.right = folder.fold_expr(node.right);
    node
}

impl ComparisonPredicate {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for ComparisonPredicate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.operator == other.operator && self.right.syntax_eq(&other.right)
    }
}

pub fn walk_distinct_from_predicate<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &DistinctFromPredicate,
) {
    visitor.visit_expr(&node.right);
}

pub fn fold_distinct_from_predicate<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: DistinctFromPredicate,
) -> DistinctFromPredicate {
    node.right = folder.fold_expr(node.right);
    node
}

impl DistinctFromPredicate {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for DistinctFromPredicate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.negated == other.negated && self.right.syntax_eq(&other.right)
    }
}

pub fn walk_between_predicate<V: Visit + ?Sized>(visitor: &mut V, node: &BetweenPredicate) {
    visitor.visit_expr(&node.min);
    visitor.visit_expr(&node.max);
}

pub fn fold_between_predicate<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: BetweenPredicate,
) -> BetweenPredicate {
    node.min = folder.fold_expr(node.min);
    node.max = folder.fold_expr(node.max);
    node
}

impl BetweenPredicate {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for BetweenPredicate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.negated == other.negated
            && self.symmetry == other.symmetry
            && self.min.syntax_eq(&other.min)
            && self.max.syntax_eq(&other.max)
    }
}

pub fn walk_boolean_test_predicate<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &BooleanTestPredicate,
) {
    let _ = (visitor, node);
}

pub fn fold_boolean_test_predicate<F: Fold + ?Sized>(
    folder: &mut F,
    node: BooleanTestPredicate,
) -> BooleanTestPredicate {
    let _ = folder;
    node
}

impl BooleanTestPredicate {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for BooleanTestPredicate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.negated == other.negated && self.truth_value == other.truth_value
    }
}

pub fn walk_is_null_predicate<V: Visit + ?Sized>(visitor: &mut V, node: &IsNullPredicate) {
    let _ = (visitor, node);
}

pub fn fold_is_null_predicate<F: Fold + ?Sized>(
    folder: &mut F,
    node: IsNullPredicate,
) -> IsNullPredicate {
    let _ = folder;
    node
}

impl IsNullPredicate {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for IsNullPredicate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.negated == other.negated
    }
}

pub fn walk_like_predicate<V: Visit + ?Sized>(visitor: &mut V, node: &LikePredicate) {
    visitor.visit_expr(&node.pattern);
    if let Some(child) = &node.escape {
        visitor.visit_expr(child);
    }
}

pub fn fold_like_predicate<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: LikePredicate,
) -> LikePredicate {
    node.pattern = folder.fold_expr(node.pattern);
    node.escape = node.escape.map(|child| folder.fold_expr(child));
    node
}

impl LikePredicate {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for LikePredicate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.negated == other.negated
            && self.pattern.syntax_eq(&other.pattern)
            && eq_option(&self.escape, &other.escape)
    }
}

pub fn walk_in_predicate<V: Visit + ?Sized>(visitor: &mut V, node: &InPredicate) {
    visitor.visit_expr(&node.value_list);
}

pub fn fold_in_predicate<F: Fold + ?Sized>(folder: &mut F, mut node: InPredicate) -> InPredicate {
    node.value_list = folder.fold_expr(node.value_list);
    node
}

impl InPredicate {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for InPredicate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.negated == other.negated && self.value_list.syntax_eq(&other.value_list)
    }
}

pub fn walk_overlaps_predicate<V: Visit + ?Sized>(visitor: &mut V, node: &OverlapsPredicate) {
    visitor.visit_expr(&node.right);
}

pub fn fold_overlaps_predicate<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: OverlapsPredicate,
) -> OverlapsPredicate {
    node.right = folder.fold_expr(node.right);
    node
}

impl OverlapsPredicate {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for OverlapsPredicate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.right.syntax_eq(&other.right)
    }
}

pub fn walk_quantified_comparison_predicate<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &QuantifiedComparisonPredicate,
) {
    visitor.visit_expr(&node.subquery);
}

pub fn fold_quantified_comparison_predicate<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: QuantifiedComparisonPredicate,
) -> QuantifiedComparisonPredicate {
    node.subquery = folder.fold_expr(node.subquery);
    node
}

impl QuantifiedComparisonPredicate {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for QuantifiedComparisonPredicate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.operator == other.operator
            && self.quantifier == other.quantifier
            && self.subquery.syntax_eq(&other.subquery)
    }
}

pub fn walk_match_predicate<V: Visit + ?Sized>(visitor: &mut V, node: &MatchPredicate) {
    visitor.visit_expr(&node.subquery);
}

pub fn fold_match_predicate<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: MatchPredicate,
) -> MatchPredicate {
    node.subquery = folder.fold_expr(node.subquery);
    node
}

impl MatchPredicate {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for MatchPredicate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.unique == other.unique
            && self.match_type == other.match_type
            && self.subquery.syntax_eq(&other.subquery)
    }
}

pub fn walk_in_list_expr<V: Visit + ?Sized>(visitor: &mut V, node: &InListExpr) {
    for child in &node.values {
        visitor.visit_expr(child);
    }
}

pub fn fold_in_list_expr<F: Fold + ?Sized>(folder: &mut F, mut node: InListExpr) -> InListExpr {
    node.values = node
        .values
        .into_iter()
        .map(|child| folder.fold_expr(child))
        .collect();
    node
}

impl InListExpr {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for InListExpr {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.values, &other.values)
    }
}

pub fn walk_predicate<V: Visit + ?Sized>(visitor: &mut V, node: &Predicate) {
    match node {
        Predicate::Comparison(child) => visitor.visit_comparison_predicate(child),
        Predicate::DistinctFrom(child) => visitor.visit_distinct_from_predicate(child),
        Predicate::Between(child) => visitor.visit_between_predicate(child),
        Predicate::BooleanTest(child) => visitor.visit_boolean_test_predicate(child),
        Predicate::IsNull(child) => visitor.visit_is_null_predicate(child),
        Predicate::Like(child) => visitor.visit_like_predicate(child),
        Predicate::In(child) => visitor.visit_in_predicate(child),
        Predicate::Overlaps(child) => visitor.visit_overlaps_predicate(child),
        Predicate::QuantifiedComparison(child) => {
            visitor.visit_quantified_comparison_predicate(child)
        }
        Predicate::Match(child) => visitor.visit_match_predicate(child),
    }
}

pub fn fold_predicate<F: Fold + ?Sized>(folder: &mut F, node: Predicate) -> Predicate {
    match node {
        Predicate::Comparison(child) => {
            Predicate::Comparison(folder.fold_comparison_predicate(child))
        }
        Predicate::DistinctFrom(child) => {
            Predicate::DistinctFrom(folder.fold_distinct_from_predicate(child))
        }
        Predicate::Between(child) => Predicate::Between(folder.fold_between_predicate(child)),
        Predicate::BooleanTest(child) => {
            Predicate::BooleanTest(folder.fold_boolean_test_predicate(child))
        }
        Predicate::IsNull(child) => Predicate::IsNull(folder.fold_is_null_predicate(child)),
        Predicate::Like(child) => Predicate::Like(folder.fold_like_predicate(child)),
        Predicate::In(child) => Predicate::In(folder.fold_in_predicate(child)),
        Predicate::Overlaps(child) => Predicate::Overlaps(folder.fold_overlaps_predicate(child)),
        Predicate::QuantifiedComparison(child) => {
            Predicate::QuantifiedComparison(folder.fold_quantified_comparison_predicate(child))
        }
        Predicate::Match(child) => Predicate::Match(folder.fold_match_predicate(child)),
    }
}

impl Predicate {
    pub const fn span(&self) -> Span {
        match self {
            Self::Comparison(node) => node.span(),
            Self::DistinctFrom(node) => node.span(),
            Self::Between(node) => node.span(),
            Self::BooleanTest(node) => node.span(),
            Self::IsNull(node) => node.span(),
            Self::Like(node) => node.span(),
            Self::In(node) => node.span(),
            Self::Overlaps(node) => node.span(),
            Self::QuantifiedComparison(node) => node.span(),
            Self::Match(node) => node.span(),
        }
    }
}

impl SyntaxEq for Predicate {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Comparison(a), Self::Comparison(b)) => a.syntax_eq(b),
            (Self::DistinctFrom(a), Self::DistinctFrom(b)) => a.syntax_eq(b),
            (Self::Between(a), Self::Between(b)) => a.syntax_eq(b),
            (Self::BooleanTest(a), Self::BooleanTest(b)) => a.syntax_eq(b),
            (Self::IsNull(a), Self::IsNull(b)) => a.syntax_eq(b),
            (Self::Like(a), Self::Like(b)) => a.syntax_eq(b),
            (Self::In(a), Self::In(b)) => a.syntax_eq(b),
            (Self::Overlaps(a), Self::Overlaps(b)) => a.syntax_eq(b),
            (Self::QuantifiedComparison(a), Self::QuantifiedComparison(b)) => a.syntax_eq(b),
            (Self::Match(a), Self::Match(b)) => a.syntax_eq(b),
            _ => false,
        }
    }
}
fn eq_slice<T: SyntaxEq>(left: &[T], right: &[T]) -> bool {
    left.len() == right.len() && left.iter().zip(right).all(|(a, b)| a.syntax_eq(b))
}
fn eq_option<T: SyntaxEq>(left: &Option<T>, right: &Option<T>) -> bool {
    match (left, right) {
        (Some(a), Some(b)) => a.syntax_eq(b),
        (None, None) => true,
        _ => false,
    }
}

#[cfg(test)]
#[path = "predicate_tests.rs"]
mod tests;
