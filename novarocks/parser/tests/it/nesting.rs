// SPDX-License-Identifier: Apache-2.0
//! Source-sized syntax uses the ordinary test thread through the whole lifecycle.
use novarocks_parser::ast::*;
use novarocks_parser::{Span, trino_parser, trino_printer};

#[derive(Default)]
struct Count {
    expressions: usize,
    queries: usize,
}
impl Visit for Count {
    fn visit_expr(&mut self, expression: &Expr) {
        self.expressions += 1;
        walk_expr(self, expression);
    }
    fn visit_query(&mut self, query: &Query) {
        self.queries += 1;
        walk_query(self, query);
    }
}

fn expression_lifecycle(original: Expr, expected: usize) {
    let printed = trino_printer::print_expression(&original).unwrap();
    let reparsed = trino_parser::parse_expression(&printed).unwrap();
    let cloned = reparsed.clone();
    assert!(original.syntax_eq(&reparsed));
    assert!(reparsed.syntax_eq(&cloned));
    let mut count = Count::default();
    count.visit_expr(&reparsed);
    assert_eq!(count.expressions, expected);
    drop(cloned);
    drop(reparsed);
    drop(original);
}

#[test]
fn parentheses_and_function_calls_complete_the_full_lifecycle_on_the_default_thread() {
    for depth in [1_000, 10_000] {
        let source = format!("{}1{}", "(".repeat(depth), ")".repeat(depth));
        let parsed = trino_parser::parse_expression(&source).unwrap();
        assert!(
            matches!(&parsed, Expr::Literal(value) if value.span == Span::new(depth, depth + 1))
        );
        expression_lifecycle(parsed, 1);
    }
    let depth = 5_000;
    let source = format!("{}1{}", "f(".repeat(depth), ")".repeat(depth));
    let parsed = trino_parser::parse_expression(&source).unwrap();
    let mut cursor = &parsed;
    for index in 0..depth {
        let Expr::FunctionCall(call) = cursor else {
            panic!("missing call at {index}")
        };
        assert_eq!(call.name.parts[0].value, "f");
        assert_eq!(call.arguments.len(), 1);
        assert_eq!(call.span, Span::new(2 * index, source.len() - index));
        cursor = &call.arguments[0].value;
    }
    assert!(matches!(
        cursor,
        Expr::Literal(Literal {
            kind: LiteralKind::Long {
                parsed_value: 1,
                ..
            },
            ..
        })
    ));
    expression_lifecycle(parsed, depth + 1);
}

#[test]
fn nested_subqueries_keep_every_real_query_through_print_walk_clone_and_drop() {
    let depth = 200;
    let source = format!("{}SELECT 1{}", "SELECT (".repeat(depth), ")".repeat(depth));
    let original = trino_parser::parse_query(&source).unwrap();
    let printed = trino_printer::print_query(&original).unwrap();
    let reparsed = trino_parser::parse_query(&printed).unwrap();
    let cloned = reparsed.clone();
    assert!(syntax_eq_query(&original, &reparsed));
    assert!(syntax_eq_query(&reparsed, &cloned));
    let mut count = Count::default();
    count.visit_query(&reparsed);
    assert_eq!(count.queries, depth + 1);
    assert_eq!(count.expressions, depth + 1);
    drop(cloned);
    drop(reparsed);
    drop(original);
}

#[test]
fn long_or_source_remains_one_ordered_multi_term_node_through_the_full_lifecycle() {
    let terms = 20_000;
    let source = std::iter::repeat_n("x", terms)
        .collect::<Vec<_>>()
        .join(" OR ");
    let original = trino_parser::parse_expression(&source).unwrap();
    let Expr::Logical(logical) = &original else {
        panic!()
    };
    assert_eq!(logical.operator, LogicalOperator::Or);
    assert_eq!(logical.terms.len(), terms);
    expression_lifecycle(original, terms + 1);
}

#[test]
fn long_union_all_source_remains_flat_and_ordered_through_the_full_lifecycle() {
    let inputs = 20_000;
    let source = (0..inputs)
        .map(|index| format!("SELECT {index}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let original = trino_parser::parse_query(&source).unwrap();
    let SetExpr::SetOperation(union) = original.body.as_ref() else {
        panic!()
    };
    assert_eq!(union.operator, SetOperator::Union);
    assert!(!union.distinct);
    assert_eq!(union.inputs.len(), inputs);
    for (index, input) in union.inputs.iter().enumerate() {
        let SetExpr::Select(select) = input else {
            panic!()
        };
        let SelectItem::SingleColumn(column) = &select.projection[0] else {
            panic!()
        };
        assert!(
            matches!(&column.expression, Expr::Literal(Literal { kind: LiteralKind::Long { parsed_value, .. }, .. }) if *parsed_value == index as i64)
        );
    }
    let printed = trino_printer::print_query(&original).unwrap();
    let reparsed = trino_parser::parse_query(&printed).unwrap();
    let cloned = reparsed.clone();
    assert!(syntax_eq_query(&original, &reparsed));
    assert!(syntax_eq_query(&reparsed, &cloned));
    let mut count = Count::default();
    count.visit_query(&reparsed);
    assert_eq!(count.queries, 1);
    assert_eq!(count.expressions, inputs);
    drop(cloned);
    drop(reparsed);
    drop(original);
}
