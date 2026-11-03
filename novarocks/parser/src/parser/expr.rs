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

//! Public entry point for the foundation expression grammar.

use crate::{ParseError, Token, ast::Expr};

use super::pratt::PrattParser;

/// `expression ::= prefix-expression { infix-operator prefix-expression } EOF`
///
/// Parses one complete expression from a lexer token stream. Trivia is ignored
/// syntactically while every AST node retains the exact byte span of its source.
pub fn parse_expression(source: &str, tokens: &[Token]) -> Result<Expr, ParseError> {
    PrattParser::new(source, tokens).parse()
}

#[cfg(test)]
mod tests {
    use crate::{
        ParseError, ParserError, Span,
        ast::{BinaryOperator, Expr, FunctionQuantifier, LiteralKind, UnaryOperator},
        lex,
    };

    use super::parse_expression;

    fn parse(source: &str) -> Expr {
        let tokens = lex(source).expect("test source must lex");
        parse_expression(source, &tokens).expect("test expression must parse")
    }

    #[test]
    fn precedence_and_left_associativity_are_table_driven() {
        let expression = parse("1 + 2 * 3 - 4");
        let Expr::Binary(subtract) = expression else {
            panic!("expected subtraction");
        };
        assert_eq!(subtract.operator, BinaryOperator::Subtract);
        let Expr::Binary(add) = *subtract.left else {
            panic!("expected addition on the left");
        };
        assert_eq!(add.operator, BinaryOperator::Add);
        let Expr::Binary(multiply) = *add.right else {
            panic!("expected multiplication to bind most tightly");
        };
        assert_eq!(multiply.operator, BinaryOperator::Multiply);

        let expression = parse("8 - 3 - 1");
        let Expr::Binary(outer) = expression else {
            panic!("expected subtraction");
        };
        assert_eq!(outer.operator, BinaryOperator::Subtract);
        assert!(matches!(*outer.left, Expr::Binary(_)));

        let expression = parse("NOT a = b AND c");
        let Expr::Binary(and) = expression else {
            panic!("expected conjunction");
        };
        assert_eq!(and.operator, BinaryOperator::And);
        let Expr::Unary(not) = *and.left else {
            panic!("expected NOT to bind below comparison and above AND");
        };
        assert_eq!(not.operator, UnaryOperator::Not);
        assert!(matches!(*not.expression, Expr::Binary(_)));
    }

    #[test]
    fn parentheses_unary_and_function_arguments_retain_precise_spans() {
        let source = "NOT (a = b) AND coalesce(-c, 'it''s')";
        let expression = parse(source);
        assert_eq!(expression.span(), Span::new(0, source.len()));

        let Expr::Binary(and) = expression else {
            panic!("expected conjunction");
        };
        assert_eq!(and.operator, BinaryOperator::And);
        let Expr::Unary(not) = *and.left else {
            panic!("expected unary NOT");
        };
        assert_eq!(not.operator, UnaryOperator::Not);
        assert_eq!(not.span, Span::new(0, 11));
        assert!(matches!(*not.expression, Expr::Nested(_)));

        let Expr::FunctionCall(call) = *and.right else {
            panic!("expected function call");
        };
        assert_eq!(call.span, Span::new(16, source.len()));
        assert_eq!(call.arguments.len(), 2);
        assert!(matches!(call.arguments[0], Expr::Unary(_)));
        let Expr::Literal(literal) = &call.arguments[1] else {
            panic!("expected string literal");
        };
        assert_eq!(literal.kind, LiteralKind::String("it's".to_owned()));
    }

    #[test]
    fn malformed_expressions_return_typed_code_and_found_token_span() {
        let source = "1 + * 2";
        let tokens = lex(source).expect("test source must lex");
        let error = parse_expression(source, &tokens).expect_err("expression must fail");
        assert_eq!(
            error,
            ParseError::UnexpectedToken {
                expected: "expression",
                found: "`*`".to_owned(),
                span: Span::new(4, 5),
            }
        );
        let user_error = ParserError::from(error).to_user_error(source);
        assert_eq!(user_error.code().as_str(), "sql.parse.unexpected_token");
        assert_eq!(
            user_error.to_string(),
            "[sql.parse.unexpected_token] expected expression, found `*` at line 1 column 5"
        );

        let source = "f(1, )";
        let tokens = lex(source).expect("test source must lex");
        let error = parse_expression(source, &tokens).expect_err("function argument must fail");
        assert_eq!(
            error,
            ParseError::UnexpectedToken {
                expected: "expression after ','",
                found: "`)`".to_owned(),
                span: Span::new(5, 6),
            }
        );
    }

    #[test]
    fn comparison_special_forms_keep_their_dedicated_ast_nodes() {
        assert!(matches!(parse("a NOT BETWEEN 1 AND 3"), Expr::Between(_)));
        assert!(matches!(parse("a IN (1, 2, 3)"), Expr::InList(_)));
        assert!(matches!(parse("a NOT LIKE 'x%' ESCAPE '_'"), Expr::Like(_)));
        assert!(matches!(parse("a IS NOT NULL"), Expr::IsPredicate(_)));

        let Expr::Binary(expression) = parse("a IS DISTINCT FROM b") else {
            panic!("expected binary IS DISTINCT FROM");
        };
        assert_eq!(expression.operator, BinaryOperator::IsDistinctFrom);

        assert!(matches!(
            parse("CASE WHEN a THEN CAST(b AS DECIMAL(10, 2)) ELSE TRY_CAST(c AS INT) END"),
            Expr::Case(_)
        ));

        let Expr::FunctionCall(call) = parse(
            "sum(v) OVER (PARTITION BY k ORDER BY ts DESC NULLS LAST ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        ) else {
            panic!("expected window function");
        };
        assert!(call.over.is_some());

        assert!(matches!(parse("EXISTS (SELECT 1)"), Expr::Exists(_)));
        assert!(matches!(
            parse("a NOT IN (SELECT b FROM t)"),
            Expr::InSubquery(_)
        ));
        assert!(matches!(parse("(SELECT 1) + 2"), Expr::Binary(_)));
        assert!(matches!(parse("[1, 2, 3]"), Expr::Array(_)));
        let Expr::Array(typed_array) = parse("ARRAY<double>[0.25, 0.5]") else {
            panic!("expected typed array literal");
        };
        assert_eq!(typed_array.elements.len(), 2);
        assert_eq!(
            typed_array
                .element_type
                .as_ref()
                .and_then(|element_type| element_type.name.parts.last())
                .map(|part| part.value.as_str()),
            Some("double")
        );
        assert_eq!(
            crate::printer::print_expr(&Expr::Array(typed_array.clone())),
            "ARRAY<double>[0.25, 0.5]"
        );
        let hex = parse("X'AB01'");
        let Expr::Literal(hex_literal) = &hex else {
            panic!("expected hex literal");
        };
        assert_eq!(hex_literal.kind, LiteralKind::HexString("AB01".to_owned()));
        assert_eq!(crate::printer::print_expr(&hex), "X'AB01'");
        assert!(matches!(parse("map{1: [2, 3], NULL: 4}"), Expr::Map(_)));
        assert!(matches!(parse("(a, b)"), Expr::Tuple(_)));
        assert!(matches!(
            parse("CAST(map{1: NULL} AS MAP<INT, ARRAY<INT>>)"),
            Expr::Cast(_)
        ));
        assert!(matches!(
            parse(
                "CAST(row(1, [row(1, 1)], map{1: 1}) AS STRUCT<s1 INT, s2 ARRAY<STRUCT<a INT, b INT>>, s3 MAP<INT, INT>>)"
            ),
            Expr::Cast(_)
        ));
        assert!(matches!(parse("items[1]"), Expr::Access(_)));
        assert!(matches!(parse("items[1].field"), Expr::Access(_)));
        assert!(matches!(parse("left('value', 2)"), Expr::FunctionCall(_)));
        assert!(matches!(parse("right('value', 2)"), Expr::FunctionCall(_)));
        assert!(matches!(parse("DATE '2024-01-10'"), Expr::TypedString(_)));
        assert!(matches!(parse("JSON '1.00'"), Expr::TypedString(_)));
        assert!(matches!(
            parse("EXTRACT(YEAR FROM created_at)"),
            Expr::FunctionCall(_)
        ));
        assert!(matches!(parse("payload->>'$.name'"), Expr::Access(_)));
        let Expr::FunctionCall(call) = parse("array_map((x, y) -> x + y, input)") else {
            panic!("expected lambda function argument");
        };
        assert!(matches!(call.arguments[0], Expr::Lambda(_)));
        let Expr::FunctionCall(call) = parse(
            "group_concat(DISTINCT a ORDER BY b DESC SEPARATOR ',') FILTER (WHERE a IS NOT NULL)",
        ) else {
            panic!("expected function modifiers");
        };
        assert_eq!(call.quantifier, FunctionQuantifier::Distinct);
        assert_eq!(call.order_by.len(), 1);
        assert!(call.separator.is_some());
        assert!(call.filter.is_some());
        let Expr::FunctionCall(call) = parse("lead(v IGNORE NULLS, 1) OVER (ORDER BY x)") else {
            panic!("expected null-treatment function");
        };
        assert_eq!(
            call.null_treatment,
            Some(crate::ast::NullTreatment::IgnoreNulls)
        );
    }
}
