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

//! Independent syntax migration oracles through the real analyzer and catalog.
use super::AnalyzerContext;
use crate::analysis::{ExprKind, TypedExpr};
use crate::analyze_error::AnalyzeErrorKind;
use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
use crate::compiler::SqlCompileControl;
use arrow::datatypes::DataType;
use novarocks_parser::{Span, ast, build};
use novarocks_type_contract::{FunctionValueType, OperatorFunction};

struct NoTables;
impl PlannerTableProvider for NoTables {
    fn resolve_table_for_analysis(
        &self,
        _: Option<&str>,
        _: &str,
        _: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        panic!("predicate capability must be checked before table discovery")
    }
}
fn context(control: &SqlCompileControl) -> AnalyzerContext<'_> {
    AnalyzerContext {
        purpose: crate::analyzer::QueryAnalysisPurpose::Ordinary,
        effects: Default::default(),
        grouping_scope: Default::default(),
        view_stack: Default::default(),
        control,
        constant_policy: crate::constant::test_constant_policy(),
        materialized_values: None,
        statement_relation: None,
        prepared_parameters: None,
        table_sequence: None,
        catalog: &NoTables,
        current_database: "default",
        function_catalog: crate::functions::builtin_sql_function_catalog(),
        sql_semantics: Default::default(),
        factory: std::rc::Rc::new(std::cell::RefCell::new(
            crate::column_id::ColumnRefFactory::new(),
        )),
        ctes: Default::default(),
        pending_ctes: Default::default(),
        next_subquery_id: Default::default(),
        collected_subqueries: Default::default(),
        cte_registry: std::cell::RefCell::new(crate::analysis::cte::CTERegistry::new()),
    }
}
fn query(sql: &str) -> ast::Query {
    let query = &novarocks_parser::parser::parse_query(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    query.clone()
}
fn projection(query: &ast::Query) -> &ast::Expr {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("select fixture")
    };
    let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
        panic!("projection fixture")
    };
    assert!(projection_column.alias.is_none(), "anonymous projection");
    &projection_column.expression
}
fn same_typed(actual: &TypedExpr, expected: &TypedExpr, control: &SqlCompileControl) {
    assert_eq!(actual.value_type, expected.value_type);
    assert!(
        crate::analysis::expr_identity::typed_expr_semantically_eq(actual, expected, control)
            .unwrap(),
        "complete typed subtree, binding, conversion policy and arguments must agree"
    );
}

#[test]
fn distinct_from_retains_null_safe_operands_and_exact_operator_identities() {
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for (left, right) in [
        ("NULL", "NULL"),
        ("NULL", "1"),
        ("1", "NULL"),
        ("1", "1"),
        ("[1,NULL]", "[1,NULL]"),
        ("ROW(1, NULL)", "ROW(1, 2)"),
    ] {
        let equal = query(&format!("SELECT {left} IS NOT DISTINCT FROM {right}"));
        let unequal = query(&format!("SELECT {left} IS DISTINCT FROM {right}"));
        let actual_equal = ctx.analyze_expr(projection(&equal), &scope).unwrap();
        let actual_unequal = ctx.analyze_expr(projection(&unequal), &scope).unwrap();
        for actual in [&actual_equal, &actual_unequal] {
            assert_eq!(
                actual.value_type,
                FunctionValueType::new(DataType::Boolean, false)
            );
        }
        let operator = crate::common::binary_operator::typed_operands;
        let (equal, operands) = operator(&actual_equal).unwrap();
        let (unequal, compared) = operator(&actual_unequal).unwrap();
        assert_eq!(equal, OperatorFunction::Identical);
        assert_eq!(unequal, OperatorFunction::Distinct);
        for (actual, expected) in compared.into_iter().zip(operands) {
            same_typed(actual, expected, &control);
        }
    }
}

#[test]
fn unsupported_predicate_profiles_precede_original_children_and_catalog_discovery() {
    let span = Span::new(7, 20);
    let unknown = || build::identifier(build::ident_at("must_not_resolve", false, span));
    let mut cases = vec![
        (
            ast::Predicate::Overlaps(ast::OverlapsPredicate {
                right: unknown(),
                span,
            }),
            "OVERLAPS predicates are not supported",
        ),
        (
            ast::Predicate::QuantifiedComparison(ast::QuantifiedComparisonPredicate {
                operator: ast::ComparisonOperator::Equal,
                quantifier: ast::ComparisonQuantifier::Any,
                subquery: unknown(),
                span,
            }),
            "quantified comparison predicates are not supported",
        ),
        (
            ast::Predicate::Match(ast::MatchPredicate {
                unique: true,
                match_type: ast::MatchType::Full,
                subquery: unknown(),
                span,
            }),
            "MATCH predicates are not supported",
        ),
        (
            ast::Predicate::In(ast::InPredicate {
                negated: false,
                value_list: unknown(),
                span,
            }),
            "IN predicates require a nonempty value list or a subquery",
        ),
        (
            ast::Predicate::In(ast::InPredicate {
                negated: false,
                value_list: ast::Expr::InList(ast::AstBox::new(ast::InListExpr {
                    values: vec![].into(),
                    span,
                })),
                span,
            }),
            "IN predicates require a nonempty value list or a subquery",
        ),
    ];
    for symmetry in [
        ast::BetweenSymmetry::Symmetric,
        ast::BetweenSymmetry::Asymmetric,
    ] {
        cases.push((
            ast::Predicate::Between(ast::BetweenPredicate {
                negated: false,
                symmetry: Some(symmetry),
                min: unknown(),
                max: unknown(),
                span,
            }),
            "explicit BETWEEN symmetry is not supported",
        ));
    }
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for (predicate, expected) in cases {
        let mut query = query("SELECT 1 FROM missing_table");
        let ast::SetExpr::Select(select) = query.body.as_mut() else {
            panic!("select")
        };
        select.projection[0] = novarocks_parser::build::item(
            ast::Expr::Predicated(ast::AstBox::new(ast::PredicatedExpr {
                value: unknown(),
                predicate,
                span,
            })),
            None,
        );
        let direct = ctx.analyze_expr(projection(&query), &scope).unwrap_err();
        assert_eq!(direct.kind(), AnalyzeErrorKind::UnsupportedExpression);
        assert_eq!(direct.message(), expected);
        assert_eq!(direct.span(), Some(span));
        let whole = super::analyze(&query, &NoTables, "default").unwrap_err();
        assert_eq!(whole.kind(), AnalyzeErrorKind::UnsupportedExpression);
        assert_eq!(whole.message(), expected);
        assert_eq!(whole.span(), Some(span));
    }
}

#[test]
fn admitted_predicates_keep_original_child_bindings_and_three_valued_declarations() {
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for sql in [
        "SELECT coalesce(1,2) = coalesce(3,4)",
        "SELECT coalesce(1,2) BETWEEN coalesce(3,4) AND coalesce(5,6)",
        "SELECT coalesce(1,2) IN (coalesce(3,4), coalesce(5,6))",
        "SELECT coalesce(CAST('a' AS VARCHAR),CAST('b' AS VARCHAR)) LIKE coalesce(CAST('c' AS VARCHAR),CAST('d' AS VARCHAR))",
    ] {
        let query = query(sql);
        let ast::Expr::Predicated(node) = projection(&query) else {
            panic!("predicated")
        };
        let expected_probe = ctx.analyze_expr(&node.value, &scope).unwrap();
        let actual = ctx.analyze_expr(projection(&query), &scope).unwrap();
        let (probe, pairs): (&TypedExpr, Vec<(&TypedExpr, &ast::Expr)>) =
            match (&actual.kind, &node.predicate) {
                (ExprKind::FunctionCall { binding, args, .. }, ast::Predicate::Comparison(p))
                    if binding.function_id == OperatorFunction::Equal.function_id() =>
                {
                    (&args[0], vec![(&args[1], &p.right)])
                }
                (
                    ExprKind::Let {
                        initializer,
                        body,
                        binding,
                    },
                    ast::Predicate::Between(p),
                ) => {
                    let ExprKind::BooleanConnective { left, right, .. } = &body.kind else {
                        panic!("BETWEEN uses the original ordered pair of comparisons");
                    };
                    let Some((OperatorFunction::LessThanOrEqual, [minimum, first_probe])) =
                        crate::common::binary_operator::typed_operands(left)
                    else {
                        panic!("the minimum starts the first comparison");
                    };
                    let Some((OperatorFunction::LessThanOrEqual, [second_probe, maximum])) =
                        crate::common::binary_operator::typed_operands(right)
                    else {
                        panic!("the maximum ends the second comparison");
                    };
                    for probe in [first_probe, second_probe] {
                        assert!(matches!(probe.kind, ExprKind::LocalValue(id) if id == *binding));
                    }
                    (initializer, vec![(minimum, &p.min), (maximum, &p.max)])
                }
                (ExprKind::FunctionCall { binding, args, .. }, ast::Predicate::In(p))
                    if binding.function_id == OperatorFunction::InList.function_id() =>
                {
                    let ast::Expr::InList(values) = &p.value_list else {
                        panic!("list");
                    };
                    (&args[0], args[1..].iter().zip(&values.values).collect())
                }
                (ExprKind::FunctionCall { binding, args, .. }, ast::Predicate::Like(p))
                    if binding.function_id == OperatorFunction::Like.function_id() =>
                {
                    (&args[0], vec![(&args[1], &p.pattern)])
                }
                _ => panic!("source predicate kind and admitted typed shape must agree"),
            };
        same_typed(probe, &expected_probe, &control);
        for (actual, syntax) in pairs {
            same_typed(actual, &ctx.analyze_expr(syntax, &scope).unwrap(), &control);
        }
    }
    for (sql, nullable) in [
        ("SELECT NULL = 1", true),
        ("SELECT 1 IS NULL", false),
        ("SELECT NULL IS TRUE", false),
        ("SELECT 1 BETWEEN NULL AND 2", true),
        ("SELECT 1 IN (NULL,2)", true),
        ("SELECT NULL LIKE 'a'", true),
    ] {
        let query = query(sql);
        let actual = ctx.analyze_expr(projection(&query), &scope).unwrap();
        assert_eq!(
            actual.value_type,
            FunctionValueType::new(DataType::Boolean, nullable),
            "{sql}"
        );
    }
    let like = query("SELECT 'ab' LIKE 'a%'");
    let typed = ctx.analyze_expr(projection(&like), &scope).unwrap();
    assert!(matches!(
        typed.kind,
        ExprKind::FunctionCall { binding, args, .. }
            if binding.function_id == novarocks_type_contract::OperatorFunction::Like.function_id()
                && args.len() == 2
    ));
}
