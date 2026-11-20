// SPDX-License-Identifier: Apache-2.0
//! Predicate fields, child custody, and real recursive storage contracts.
use super::*;
use crate::{SYNTHETIC, Span, build};

fn span() -> Span {
    Span::new(10, 40)
}
fn id(name: &str) -> Expr {
    build::identifier(build::ident_at(name, false, span()))
}
fn subquery(name: &str) -> Expr {
    let query = crate::parser::parse_query(&format!("SELECT {name}")).unwrap();
    Expr::Subquery(crate::ast::SubqueryExpr {
        query: crate::ast::AstBox::new(query),
        span: span(),
    })
}
fn wrap(predicate: Predicate) -> Expr {
    Expr::Predicated(crate::ast::AstBox::new(PredicatedExpr {
        value: id("subject"),
        predicate,
        span: span(),
    }))
}
fn cases() -> Vec<(Predicate, Vec<&'static str>)> {
    let s = span();
    vec![
        (
            Predicate::Comparison(ComparisonPredicate {
                operator: ComparisonOperator::LessThan,
                right: id("right"),
                span: s,
            }),
            vec!["subject", "right"],
        ),
        (
            Predicate::DistinctFrom(DistinctFromPredicate {
                negated: true,
                right: id("distinct"),
                span: s,
            }),
            vec!["subject", "distinct"],
        ),
        (
            Predicate::Between(BetweenPredicate {
                negated: true,
                symmetry: Some(BetweenSymmetry::Symmetric),
                min: id("minimum"),
                max: id("maximum"),
                span: s,
            }),
            vec!["subject", "minimum", "maximum"],
        ),
        (
            Predicate::BooleanTest(BooleanTestPredicate {
                negated: true,
                truth_value: TruthValue::Unknown,
                span: s,
            }),
            vec!["subject"],
        ),
        (
            Predicate::IsNull(IsNullPredicate {
                negated: true,
                span: s,
            }),
            vec!["subject"],
        ),
        (
            Predicate::Like(LikePredicate {
                negated: true,
                pattern: id("pattern"),
                escape: Some(id("escape")),
                span: s,
            }),
            vec!["subject", "pattern", "escape"],
        ),
        (
            Predicate::In(InPredicate {
                negated: true,
                value_list: Expr::InList(crate::ast::AstBox::new(InListExpr {
                    values: vec![id("first"), id("second")].into(),
                    span: s,
                })),
                span: s,
            }),
            vec!["subject", "first", "second"],
        ),
        (
            Predicate::Overlaps(OverlapsPredicate {
                right: id("overlap"),
                span: s,
            }),
            vec!["subject", "overlap"],
        ),
        (
            Predicate::QuantifiedComparison(QuantifiedComparisonPredicate {
                operator: ComparisonOperator::GreaterThan,
                quantifier: ComparisonQuantifier::Any,
                subquery: subquery("quantified"),
                span: s,
            }),
            vec!["subject", "quantified"],
        ),
        (
            Predicate::Match(MatchPredicate {
                unique: true,
                match_type: MatchType::Partial,
                subquery: subquery("matched"),
                span: s,
            }),
            vec!["subject", "matched"],
        ),
    ]
}
#[derive(Default)]
struct Names(Vec<String>);
impl Visit for Names {
    fn visit_ident(&mut self, node: &crate::ast::Ident) {
        self.0.push(node.value.clone());
    }
}
struct Rename;
impl Fold for Rename {
    fn fold_ident(&mut self, mut node: crate::ast::Ident) -> crate::ast::Ident {
        node.value = format!("changed_{}", node.value);
        node
    }
}
struct Respan;
macro_rules! relocate {
    ($method:ident,$walk:ident,$ty:ty) => {
        fn $method(&mut self, node: $ty) -> $ty {
            let mut node = $walk(self, node);
            node.span = SYNTHETIC;
            node
        }
    };
}
impl Fold for Respan {
    fn fold_ident(&mut self, mut node: crate::ast::Ident) -> crate::ast::Ident {
        node.span = SYNTHETIC;
        node
    }
    relocate!(fold_predicated_expr, fold_predicated_expr, PredicatedExpr);
    relocate!(fold_in_list_expr, fold_in_list_expr, InListExpr);
    relocate!(
        fold_comparison_predicate,
        fold_comparison_predicate,
        ComparisonPredicate
    );
    relocate!(
        fold_distinct_from_predicate,
        fold_distinct_from_predicate,
        DistinctFromPredicate
    );
    relocate!(
        fold_between_predicate,
        fold_between_predicate,
        BetweenPredicate
    );
    relocate!(
        fold_boolean_test_predicate,
        fold_boolean_test_predicate,
        BooleanTestPredicate
    );
    relocate!(
        fold_is_null_predicate,
        fold_is_null_predicate,
        IsNullPredicate
    );
    relocate!(fold_like_predicate, fold_like_predicate, LikePredicate);
    relocate!(fold_in_predicate, fold_in_predicate, InPredicate);
    relocate!(
        fold_overlaps_predicate,
        fold_overlaps_predicate,
        OverlapsPredicate
    );
    relocate!(
        fold_quantified_comparison_predicate,
        fold_quantified_comparison_predicate,
        QuantifiedComparisonPredicate
    );
    relocate!(fold_match_predicate, fold_match_predicate, MatchPredicate);
}
#[test]
fn predicate_visit_and_fold_reach_each_actual_child_in_field_order() {
    for (predicate, expected) in cases() {
        let original = wrap(predicate);
        let mut names = Names::default();
        names.visit_expr(&original);
        assert_eq!(names.0, expected);
        let folded = Rename.fold_expr(original.clone());
        let mut names = Names::default();
        names.visit_expr(&folded);
        assert_eq!(
            names.0,
            expected
                .iter()
                .map(|name| format!("changed_{name}"))
                .collect::<Vec<_>>()
        );
        assert!(!original.syntax_eq(&folded));
        let relocated = Respan.fold_expr(original.clone());
        assert_ne!(original, relocated);
        assert!(original.syntax_eq(&relocated));
    }
}
#[test]
fn predicate_scalar_flags_and_order_remain_stored_syntax_facts() {
    for (original, _) in cases() {
        let mut changed = original.clone();
        match &mut changed {
            Predicate::Comparison(node) => node.operator = ComparisonOperator::Equal,
            Predicate::DistinctFrom(node) => node.negated = false,
            Predicate::Between(node) => node.symmetry = Some(BetweenSymmetry::Asymmetric),
            Predicate::BooleanTest(node) => node.truth_value = TruthValue::False,
            Predicate::IsNull(node) => node.negated = false,
            Predicate::Like(node) => node.escape = None,
            Predicate::In(node) => {
                let Expr::InList(list) = &mut node.value_list else {
                    unreachable!()
                };
                list.values.swap(0, 1);
            }
            Predicate::Overlaps(node) => node.right = id("other"),
            Predicate::QuantifiedComparison(node) => node.quantifier = ComparisonQuantifier::Some,
            Predicate::Match(node) => node.unique = false,
        }
        assert!(!original.syntax_eq(&changed));
    }
    let s = span();
    let a = Predicate::Match(MatchPredicate {
        unique: true,
        match_type: MatchType::Partial,
        subquery: subquery("same"),
        span: s,
    });
    let b = Predicate::Match(MatchPredicate {
        unique: true,
        match_type: MatchType::Full,
        subquery: subquery("same"),
        span: s,
    });
    assert!(!a.syntax_eq(&b));
}
#[test]
fn predicate_owning_cycle_and_all_synchronous_traversals_grow_the_actual_stack() {
    let mut node = build::boolean(true);
    for _ in 0..20_000 {
        node = build::comparison(node, ComparisonOperator::Equal, build::boolean(false));
    }
    let cloned = node.clone();
    assert_eq!(node, cloned);
    assert!(node.syntax_eq(&cloned));
    struct Identity;
    impl Fold for Identity {}
    let folded = Identity.fold_expr(cloned);
    assert!(node.syntax_eq(&folded));
    struct Count(usize);
    impl Visit for Count {
        fn visit_predicated_expr(&mut self, node: &PredicatedExpr) {
            self.0 += 1;
            walk_predicated_expr(self, node);
        }
    }
    let mut count = Count(0);
    count.visit_expr(&node);
    assert_eq!(count.0, 20_000);
    let rendered = crate::printer::print_expression(&node).unwrap();
    assert!(!rendered.is_empty());
    let mut query = crate::parser::parse_query("SELECT TRUE").unwrap();
    let crate::ast::SetExpr::Select(select) = query.body.as_mut() else {
        unreachable!()
    };
    select.projection[0] = crate::build::item(folded, None);
    crate::ast::validate_statement(&crate::ast::TrinoStatement::Query(query)).unwrap();
    drop(node);
}
