// SPDX-License-Identifier: Apache-2.0
//! Control-expression syntax facts and custody through actual owning edges.
use super::*;
use crate::{SYNTHETIC, Span, build};

fn query_with(expression: Expr) -> Query {
    let mut query = crate::parser::parse_query("SELECT TRUE").unwrap();
    let SetExpr::Select(select) = query.body.as_mut() else {
        unreachable!()
    };
    select.projection[0] = crate::build::item(expression, None);
    query
}
fn id(value: &str) -> Expr {
    build::identifier(build::ident_at(value, false, Span::new(11, 17)))
}
fn clause(match_value: WhenMatch, result: Expr) -> WhenClause {
    WhenClause {
        match_value,
        result,
        span: Span::new(20, 35),
    }
}
fn simple() -> Expr {
    Expr::SimpleCase(AstBox::new(SimpleCaseExpr {
        operand: id("operand"),
        when_clauses: vec![
            clause(WhenMatch::Operand(id("label")), id("result")),
            clause(
                WhenMatch::Partial(Predicate::Between(BetweenPredicate {
                    negated: true,
                    symmetry: Some(BetweenSymmetry::Symmetric),
                    min: id("minimum"),
                    max: id("maximum"),
                    span: Span::new(21, 29),
                })),
                id("partial_result"),
            ),
        ]
        .into(),
        default_value: Some(id("default")),
        span: Span::new(0, 55),
    }))
}
#[derive(Default)]
struct Names(Vec<String>);
impl Visit for Names {
    fn visit_ident(&mut self, ident: &Ident) {
        self.0.push(ident.value.clone());
    }
}
struct Rename;
impl Fold for Rename {
    fn fold_ident(&mut self, mut ident: Ident) -> Ident {
        ident.value = format!("new_{}", ident.value);
        ident
    }
}
struct Relocate;
macro_rules! relocate {
    ($hook:ident,$walk:ident,$ty:ty) => {
        fn $hook(&mut self, node: $ty) -> $ty {
            let mut node = $walk(self, node);
            node.span = SYNTHETIC;
            node
        }
    };
}
impl Fold for Relocate {
    fn fold_ident(&mut self, mut node: Ident) -> Ident {
        node.span = SYNTHETIC;
        node
    }
    relocate!(fold_exists_expr, fold_exists_expr, ExistsExpr);
    relocate!(fold_unique_expr, fold_unique_expr, UniqueExpr);
    relocate!(fold_cast_expr, fold_cast_expr, CastExpr);
    relocate!(fold_lambda_expr, fold_lambda_expr, LambdaExpr);
    relocate!(
        fold_lambda_argument_declaration,
        fold_lambda_argument_declaration,
        LambdaArgumentDeclaration
    );
    relocate!(
        fold_searched_case_expr,
        fold_searched_case_expr,
        SearchedCaseExpr
    );
    relocate!(fold_simple_case_expr, fold_simple_case_expr, SimpleCaseExpr);
    relocate!(fold_when_clause, fold_when_clause, WhenClause);
    relocate!(
        fold_between_predicate,
        fold_between_predicate,
        BetweenPredicate
    );
}
#[test]
fn control_children_keep_declared_order_and_partial_predicate_identity() {
    let original = simple();
    let expected = [
        "operand",
        "label",
        "result",
        "minimum",
        "maximum",
        "partial_result",
        "default",
    ];
    let mut names = Names::default();
    names.visit_expr(&original);
    assert_eq!(names.0, expected);
    let changed = Rename.fold_expr(original.clone());
    let mut names = Names::default();
    names.visit_expr(&changed);
    assert_eq!(names.0, expected.map(|name| format!("new_{name}")));
    assert!(!original.syntax_eq(&changed));
    let moved = Relocate.fold_expr(original.clone());
    assert_ne!(original, moved);
    assert!(original.syntax_eq(&moved));
    let mut reordered = original.clone();
    let Expr::SimpleCase(node) = &mut reordered else {
        unreachable!()
    };
    node.when_clauses.reverse();
    assert!(!original.syntax_eq(&reordered));
    let mut no_default = original.clone();
    let Expr::SimpleCase(node) = &mut no_default else {
        unreachable!()
    };
    node.default_value = None;
    assert!(!original.syntax_eq(&no_default));
    let mut null_default = no_default.clone();
    let Expr::SimpleCase(node) = &mut null_default else {
        unreachable!()
    };
    node.default_value = Some(build::null());
    assert!(!no_default.syntax_eq(&null_default));
    assert_eq!(
        crate::printer::print_expression(&original).unwrap(),
        "CASE operand WHEN label THEN result WHEN NOT BETWEEN SYMMETRIC minimum AND maximum THEN partial_result ELSE default END"
    );
    let Expr::SimpleCase(node) = &mut reordered else {
        unreachable!()
    };
    node.when_clauses.clear();
    assert!(validate_statement(&TrinoStatement::Query(query_with(reordered))).is_err());
}
#[test]
fn cast_lambda_and_subquery_wrappers_visit_all_stored_facts() {
    let body = build::cast(id("input"), build::simple_type("integer"));
    let original = Expr::Lambda(AstBox::new(LambdaExpr {
        arguments: vec![LambdaArgumentDeclaration {
            name: build::ident_at("Arg", true, Span::new(1, 6)),
            span: Span::new(1, 6),
        }]
        .into(),
        body,
        span: Span::new(0, 40),
    }));
    let mut names = Names::default();
    names.visit_expr(&original);
    assert_eq!(names.0, ["Arg", "input", "integer"]);
    let relocated = Relocate.fold_expr(original.clone());
    assert_ne!(original, relocated);
    assert!(original.syntax_eq(&relocated));
    let mut changed = original.clone();
    let Expr::Lambda(node) = &mut changed else {
        unreachable!()
    };
    let Expr::Cast(cast) = &mut node.body else {
        unreachable!()
    };
    cast.safe = true;
    assert!(!original.syntax_eq(&changed));
    let mut changed = original.clone();
    let Expr::Lambda(node) = &mut changed else {
        unreachable!()
    };
    node.arguments[0].name.quoted = false;
    assert!(!original.syntax_eq(&changed));
    for wrapped in [
        Expr::Exists(AstBox::new(ExistsExpr {
            subquery: id("wrapped"),
            span: Span::new(0, 20),
        })),
        Expr::Unique(AstBox::new(UniqueExpr {
            subquery: id("wrapped"),
            span: Span::new(0, 20),
        })),
    ] {
        let mut names = Names::default();
        names.visit_expr(&wrapped);
        assert_eq!(names.0, ["wrapped"]);
        let renamed = Rename.fold_expr(wrapped.clone());
        assert!(!wrapped.syntax_eq(&renamed));
        let relocated = Relocate.fold_expr(wrapped.clone());
        assert_ne!(wrapped, relocated);
        assert!(wrapped.syntax_eq(&relocated));
    }
}
#[test]
fn actual_control_owning_cycles_and_synchronous_walks_grow_the_stack() {
    let mut node = build::boolean(true);
    for index in 0..20_000 {
        node = match index % 4 {
            0 => build::cast(node, build::simple_type("boolean")),
            1 => build::case_when(vec![(build::boolean(true), node)], None).unwrap(),
            2 => Expr::SimpleCase(AstBox::new(SimpleCaseExpr {
                operand: node,
                when_clauses: vec![clause(
                    WhenMatch::Operand(build::boolean(true)),
                    build::boolean(false),
                )]
                .into(),
                default_value: None,
                span: SYNTHETIC,
            })),
            _ => Expr::Lambda(AstBox::new(LambdaExpr {
                arguments: vec![LambdaArgumentDeclaration {
                    name: build::ident("x", false),
                    span: SYNTHETIC,
                }]
                .into(),
                body: node,
                span: SYNTHETIC,
            })),
        };
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
        fn visit_expr(&mut self, node: &Expr) {
            self.0 += 1;
            walk_expr(self, node);
        }
    }
    let mut count = Count(0);
    count.visit_expr(&node);
    assert_eq!(count.0, 35_001);
    let query = query_with(node);
    validate_statement(&TrinoStatement::Query(query.clone())).unwrap();
    let rendered = crate::printer::print_statement(&TrinoStatement::Query(query)).unwrap();
    assert!(!rendered.is_empty());
    drop(folded);
}
