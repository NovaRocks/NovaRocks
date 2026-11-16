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
use crate::analysis::{BinOp, ExprKind, TypedExpr, UnOp};
use crate::analyze_error::AnalyzeErrorKind;
use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
use crate::compiler::SqlCompileControl;
use arrow::datatypes::DataType;
use novarocks_parser::{Span, ast, build};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};

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
        grouping_scope: Default::default(),
        view_stack: Default::default(),
        control,
        constant_policy: crate::constant::test_constant_policy(),
        materialized_values: None,
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
        next_lambda_slot_id: Default::default(),
        collected_subqueries: Default::default(),
        cte_registry: std::cell::RefCell::new(crate::analysis::cte::CTERegistry::new()),
    }
}
fn query(sql: &str) -> ast::Query {
    let statements =
        novarocks_parser::parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("query fixture")
    };
    query.clone()
}
fn projection(query: &ast::Query) -> &ast::Expr {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("select fixture")
    };
    let ast::SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
        panic!("projection fixture")
    };
    expr
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
fn distinct_from_retains_original_null_safe_equality_and_not_results() {
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for (left, right) in [
        ("NULL", "NULL"),
        ("NULL", "1"),
        ("1", "NULL"),
        ("1", "1"),
        ("[1,NULL]", "[1,NULL]"),
    ] {
        let original = query(&format!("SELECT {left} <=> {right}"));
        let old_equal = ctx.analyze_expr(projection(&original), &scope).unwrap();
        let standard = query(&format!("SELECT {left} IS NOT DISTINCT FROM {right}"));
        let actual_equal = ctx.analyze_expr(projection(&standard), &scope).unwrap();
        same_typed(&actual_equal, &old_equal, &control);
        assert_eq!(
            actual_equal.value_type,
            FunctionValueType::new(DataType::Boolean, false)
        );
        assert!(matches!(
            actual_equal.kind,
            ExprKind::BinaryOp {
                op: BinOp::EqForNull,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                ..
            }
        ));
        let old_not = query(&format!("SELECT NOT ({left} <=> {right})"));
        let old_not = ctx.analyze_expr(projection(&old_not), &scope).unwrap();
        let standard = query(&format!("SELECT {left} IS DISTINCT FROM {right}"));
        let actual_not = ctx.analyze_expr(projection(&standard), &scope).unwrap();
        assert_eq!(actual_not.value_type, old_not.value_type);
        assert_eq!(
            actual_not.value_type,
            FunctionValueType::new(DataType::Boolean, false)
        );
        let ExprKind::UnaryOp {
            op: UnOp::Not,
            expr: actual_inner,
        } = &actual_not.kind
        else {
            panic!("typed NOT")
        };
        let ExprKind::UnaryOp {
            op: UnOp::Not,
            expr: original_inner,
        } = &old_not.kind
        else {
            panic!("original typed NOT")
        };
        let ExprKind::Nested(original_equal) = &original_inner.kind else {
            panic!("original parentheses")
        };
        same_typed(actual_inner, original_equal, &control);
    }
}

#[test]
fn unsupported_predicate_profiles_precede_original_children_and_catalog_discovery() {
    let span = Span::new(7, 20);
    let unknown = || build::identifier(build::ident_at("must_not_resolve", None, span));
    let mut cases = vec![
        (
            ast::Predicate::BooleanTest(ast::BooleanTestPredicate {
                negated: false,
                truth_value: ast::TruthValue::Unknown,
                span,
            }),
            "IS UNKNOWN predicates are not supported",
        ),
        (
            ast::Predicate::BooleanTest(ast::BooleanTestPredicate {
                negated: true,
                truth_value: ast::TruthValue::Unknown,
                span,
            }),
            "IS UNKNOWN predicates are not supported",
        ),
        (
            ast::Predicate::Like(ast::LikePredicate {
                negated: false,
                pattern: unknown(),
                escape: Some(unknown()),
                span,
            }),
            "LIKE ESCAPE is not supported",
        ),
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
        select.projection[0] = ast::SelectItem::UnnamedExpr(ast::Expr::Predicated(
            ast::AstBox::new(ast::PredicatedExpr {
                value: unknown(),
                predicate,
                span,
            }),
        ));
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
        "SELECT coalesce('a','b') LIKE coalesce('c','d')",
    ] {
        let query = query(sql);
        let ast::Expr::Predicated(node) = projection(&query) else {
            panic!("predicated")
        };
        let expected_probe = ctx.analyze_expr(&node.value, &scope).unwrap();
        let actual = ctx.analyze_expr(projection(&query), &scope).unwrap();
        let probe = match &actual.kind {
            ExprKind::BinaryOp { left, .. }
            | ExprKind::Between { expr: left, .. }
            | ExprKind::InList { expr: left, .. }
            | ExprKind::Like { expr: left, .. } => left,
            _ => panic!("the admitted profile keeps its original typed IR kind"),
        };
        same_typed(probe, &expected_probe, &control);
        let pairs: Vec<(&TypedExpr, &ast::Expr)> = match (&actual.kind, &node.predicate) {
            (ExprKind::BinaryOp { right, .. }, ast::Predicate::Comparison(p)) => {
                vec![(right, &p.right)]
            }
            (ExprKind::Between { low, high, .. }, ast::Predicate::Between(p)) => {
                vec![(low, &p.min), (high, &p.max)]
            }
            (ExprKind::InList { list, .. }, ast::Predicate::In(p)) => {
                let ast::Expr::InList(values) = &p.value_list else {
                    panic!("list")
                };
                list.iter().zip(&values.values).collect()
            }
            (ExprKind::Like { pattern, .. }, ast::Predicate::Like(p)) => {
                vec![(pattern, &p.pattern)]
            }
            _ => panic!("source predicate kind and admitted typed shape must agree"),
        };
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
    // The legacy RLIKE spelling currently has exactly the same admitted LIKE
    // owner. This test grants no regex function or implementation capability.
    let legacy = query("SELECT 'ab' RLIKE 'a%'");
    let plain = query("SELECT 'ab' LIKE 'a%'");
    same_typed(
        &ctx.analyze_expr(projection(&legacy), &scope).unwrap(),
        &ctx.analyze_expr(projection(&plain), &scope).unwrap(),
        &control,
    );
}

#[test]
fn in_predicate_children_retain_actual_source_and_write_conversion_loans() {
    use crate::compiler::SqlWriteAssignmentSite;
    use crate::compiler::materialized_values::{
        MaterializedValues, SqlConstantExpressionSite, SqlConstantExpressionStep,
        SqlExpressionConstant,
    };
    use arrow::array::{Array, Int64Array};
    use novarocks_functions::ConstantPool;
    use novarocks_type_contract::CompilePhase;
    use std::{borrow::Cow, sync::Arc};
    let control = SqlCompileControl::unbounded();
    let field = Arc::new(arrow::datatypes::Field::new(
        "predicate_source",
        DataType::Int64,
        false,
    ));
    let value_type = FunctionValueType::new(DataType::Int64, false);
    let pool = ConstantPool::try_new(
        field.clone(),
        value_type.clone(),
        Int64Array::from(vec![99, 2]).to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    let query = query("SELECT only_original_probe IN (1,2)");
    let ast::Expr::Predicated(node) = projection(&query) else {
        panic!("predicate")
    };
    let ast::Predicate::In(predicate) = &node.predicate else {
        panic!("IN")
    };
    let ast::Expr::InList(list) = &predicate.value_list else {
        panic!("list")
    };
    let constants = vec![
        SqlExpressionConstant::new(
            vec![],
            SqlConstantExpressionSite::SelectProjection { column: 0 },
            value.clone(),
        )
        .with_expression_path(vec![SqlConstantExpressionStep::PredicatedValue]),
    ]
    .into();
    let source = MaterializedValues::try_new_expressions(&query, constants, &control).unwrap();
    let mut original = source.loan(&query, &control).unwrap();
    original
        .insert_write_assignment(
            &list.values[0],
            Cow::Owned(
                SqlWriteAssignmentSite::try_new(0, value_type.clone(), None, &control)
                    .unwrap()
                    .with_source_span(list.values[0].span()),
            ),
            &control,
        )
        .unwrap();
    assert!(original.view().constant_at(&node.value.clone()).is_none());
    assert!(
        original
            .view()
            .write_assignment_at(&list.values[0].clone())
            .is_none()
    );
    let mut ctx = context(&control);
    ctx.materialized_values = Some(original.view());
    let scope = ctx.new_scope();
    let expected_probe = ctx.analyze_expr(&node.value, &scope).unwrap();
    let expected_assignment = ctx.analyze_expr(&list.values[0], &scope).unwrap();
    let actual = ctx.analyze_expr(projection(&query), &scope).unwrap();
    assert_eq!(
        actual.value_type,
        FunctionValueType::new(DataType::Boolean, true)
    );
    let ExprKind::InList {
        expr,
        list: actual_values,
        negated: false,
    } = &actual.kind
    else {
        panic!("original IN owner")
    };
    same_typed(expr, &expected_probe, &control);
    same_typed(&actual_values[0], &expected_assignment, &control);
    let ExprKind::Constant(retained) = &expr.kind else {
        panic!("retained source")
    };
    assert_eq!(retained.ordinal(), 1);
    assert_eq!(retained.pool().backing_identity(), pool.backing_identity());
    assert_eq!(retained.field(), field.as_ref());
    assert_eq!(retained.value_type(), &value_type);
    assert_eq!(
        original
            .view()
            .write_assignment_at(&list.values[0])
            .unwrap()
            .input(),
        &value_type
    );
    assert_eq!(
        actual_values[0].value_type,
        FunctionValueType::new(DataType::Int64, true)
    );
    let ExprKind::Cast {
        expr: input,
        target,
        decimal_overflow_policy,
    } = &actual_values[0].kind
    else {
        panic!("original nullable source conversion")
    };
    assert_eq!(target, value_type.data_type());
    assert_eq!(*decimal_overflow_policy, DecimalOverflowPolicy::OutputNull);
    assert!(matches!(
        input.kind,
        ExprKind::Literal(crate::analysis::LiteralValue::Int(1))
    ));
    assert_eq!(
        ctx.analyze_expr(&projection(&query).clone(), &scope)
            .unwrap_err()
            .kind(),
        AnalyzeErrorKind::UnknownColumn
    );
}
