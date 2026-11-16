// SPDX-License-Identifier: Apache-2.0
//! Control expressions and their original, ordered syntax children.
use super::{AstVec, Expr, Fold, Ident, Predicate, SyntaxEq, TypeName, Visit};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExistsExpr {
    pub subquery: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UniqueExpr {
    pub subquery: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CastExpr {
    pub expression: Expr,
    pub data_type: TypeName,
    pub safe: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LambdaExpr {
    pub arguments: AstVec<LambdaArgumentDeclaration>,
    pub body: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LambdaArgumentDeclaration {
    pub name: Ident,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchedCaseExpr {
    pub when_clauses: AstVec<WhenClause>,
    pub default_value: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimpleCaseExpr {
    pub operand: Expr,
    pub when_clauses: AstVec<WhenClause>,
    pub default_value: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WhenClause {
    pub match_value: WhenMatch,
    pub result: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WhenMatch {
    Operand(Expr),
    Partial(Predicate),
}

#[recursive::recursive]
pub fn walk_exists_expr<V: Visit + ?Sized>(visitor: &mut V, node: &ExistsExpr) {
    visitor.visit_expr(&node.subquery);
}

#[recursive::recursive]
pub fn fold_exists_expr<F: Fold + ?Sized>(folder: &mut F, mut node: ExistsExpr) -> ExistsExpr {
    node.subquery = folder.fold_expr(node.subquery);
    node
}

impl ExistsExpr {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for ExistsExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.subquery.syntax_eq(&other.subquery)
    }
}

#[recursive::recursive]
pub fn walk_unique_expr<V: Visit + ?Sized>(visitor: &mut V, node: &UniqueExpr) {
    visitor.visit_expr(&node.subquery);
}

#[recursive::recursive]
pub fn fold_unique_expr<F: Fold + ?Sized>(folder: &mut F, mut node: UniqueExpr) -> UniqueExpr {
    node.subquery = folder.fold_expr(node.subquery);
    node
}

impl UniqueExpr {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for UniqueExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.subquery.syntax_eq(&other.subquery)
    }
}

#[recursive::recursive]
pub fn walk_cast_expr<V: Visit + ?Sized>(visitor: &mut V, node: &CastExpr) {
    visitor.visit_expr(&node.expression);
    visitor.visit_type_name(&node.data_type);
}

#[recursive::recursive]
pub fn fold_cast_expr<F: Fold + ?Sized>(folder: &mut F, mut node: CastExpr) -> CastExpr {
    node.expression = folder.fold_expr(node.expression);
    node.data_type = folder.fold_type_name(node.data_type);
    node
}

impl CastExpr {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for CastExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expression.syntax_eq(&other.expression)
            && self.data_type.syntax_eq(&other.data_type)
            && self.safe == other.safe
    }
}

#[recursive::recursive]
pub fn walk_lambda_expr<V: Visit + ?Sized>(visitor: &mut V, node: &LambdaExpr) {
    for child in &node.arguments {
        visitor.visit_lambda_argument_declaration(child);
    }
    visitor.visit_expr(&node.body);
}

#[recursive::recursive]
pub fn fold_lambda_expr<F: Fold + ?Sized>(folder: &mut F, mut node: LambdaExpr) -> LambdaExpr {
    node.arguments = node
        .arguments
        .into_iter()
        .map(|child| folder.fold_lambda_argument_declaration(child))
        .collect();
    node.body = folder.fold_expr(node.body);
    node
}

impl LambdaExpr {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for LambdaExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.arguments, &other.arguments) && self.body.syntax_eq(&other.body)
    }
}

#[recursive::recursive]
pub fn walk_lambda_argument_declaration<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &LambdaArgumentDeclaration,
) {
    visitor.visit_ident(&node.name);
}

#[recursive::recursive]
pub fn fold_lambda_argument_declaration<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: LambdaArgumentDeclaration,
) -> LambdaArgumentDeclaration {
    node.name = folder.fold_ident(node.name);
    node
}

impl LambdaArgumentDeclaration {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for LambdaArgumentDeclaration {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
    }
}

#[recursive::recursive]
pub fn walk_searched_case_expr<V: Visit + ?Sized>(visitor: &mut V, node: &SearchedCaseExpr) {
    for child in &node.when_clauses {
        visitor.visit_when_clause(child);
    }
    if let Some(child) = &node.default_value {
        visitor.visit_expr(child);
    }
}

#[recursive::recursive]
pub fn fold_searched_case_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: SearchedCaseExpr,
) -> SearchedCaseExpr {
    node.when_clauses = node
        .when_clauses
        .into_iter()
        .map(|child| folder.fold_when_clause(child))
        .collect();
    node.default_value = node.default_value.map(|child| folder.fold_expr(child));
    node
}

impl SearchedCaseExpr {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for SearchedCaseExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.when_clauses, &other.when_clauses)
            && eq_option(&self.default_value, &other.default_value)
    }
}

#[recursive::recursive]
pub fn walk_simple_case_expr<V: Visit + ?Sized>(visitor: &mut V, node: &SimpleCaseExpr) {
    visitor.visit_expr(&node.operand);
    for child in &node.when_clauses {
        visitor.visit_when_clause(child);
    }
    if let Some(child) = &node.default_value {
        visitor.visit_expr(child);
    }
}

#[recursive::recursive]
pub fn fold_simple_case_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: SimpleCaseExpr,
) -> SimpleCaseExpr {
    node.operand = folder.fold_expr(node.operand);
    node.when_clauses = node
        .when_clauses
        .into_iter()
        .map(|child| folder.fold_when_clause(child))
        .collect();
    node.default_value = node.default_value.map(|child| folder.fold_expr(child));
    node
}

impl SimpleCaseExpr {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for SimpleCaseExpr {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.operand.syntax_eq(&other.operand)
            && eq_slice(&self.when_clauses, &other.when_clauses)
            && eq_option(&self.default_value, &other.default_value)
    }
}

#[recursive::recursive]
pub fn walk_when_clause<V: Visit + ?Sized>(visitor: &mut V, node: &WhenClause) {
    visitor.visit_when_match(&node.match_value);
    visitor.visit_expr(&node.result);
}

#[recursive::recursive]
pub fn fold_when_clause<F: Fold + ?Sized>(folder: &mut F, mut node: WhenClause) -> WhenClause {
    node.match_value = folder.fold_when_match(node.match_value);
    node.result = folder.fold_expr(node.result);
    node
}

impl WhenClause {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl SyntaxEq for WhenClause {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.match_value.syntax_eq(&other.match_value) && self.result.syntax_eq(&other.result)
    }
}

#[recursive::recursive]
pub fn walk_when_match<V: Visit + ?Sized>(visitor: &mut V, node: &WhenMatch) {
    match node {
        WhenMatch::Operand(child) => visitor.visit_expr(child),
        WhenMatch::Partial(child) => visitor.visit_predicate(child),
    }
}

#[recursive::recursive]
pub fn fold_when_match<F: Fold + ?Sized>(folder: &mut F, node: WhenMatch) -> WhenMatch {
    match node {
        WhenMatch::Operand(child) => WhenMatch::Operand(folder.fold_expr(child)),
        WhenMatch::Partial(child) => WhenMatch::Partial(folder.fold_predicate(child)),
    }
}

impl WhenMatch {
    pub fn span(&self) -> Span {
        match self {
            Self::Operand(node) => node.span(),
            Self::Partial(node) => node.span(),
        }
    }
}

impl SyntaxEq for WhenMatch {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Operand(a), Self::Operand(b)) => a.syntax_eq(b),
            (Self::Partial(a), Self::Partial(b)) => a.syntax_eq(b),
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
