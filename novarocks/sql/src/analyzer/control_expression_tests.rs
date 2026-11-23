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
use novarocks_type_contract::FunctionValueType;

struct NoTables;
impl PlannerTableProvider for NoTables {
    fn resolve_table_for_analysis(
        &self,
        _: Option<&str>,
        _: &str,
        _: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        panic!("control expression capability must be checked before table discovery")
    }
}
fn context(control: &SqlCompileControl) -> AnalyzerContext<'_> {
    AnalyzerContext {
        purpose: crate::analyzer::QueryAnalysisPurpose::Ordinary,
        grouping_scope: Default::default(),
        view_stack: Default::default(),
        control,
        constant_policy: crate::constant::test_constant_policy(),
        materialized_values: None,
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
fn case_clauses_keep_original_order_bindings_and_full_branch_declarations() {
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for sql in [
        "SELECT CASE WHEN coalesce(TRUE,FALSE) THEN coalesce(1,2) WHEN coalesce(FALSE,TRUE) THEN coalesce(3,4) ELSE coalesce(5,6) END",
        "SELECT CASE coalesce(1,2) WHEN coalesce(3,4) THEN coalesce(5,6) WHEN coalesce(7,8) THEN coalesce(9,10) ELSE coalesce(11,12) END",
    ] {
        let query = query(sql);
        let (original_operand, clauses, default) = match projection(&query) {
            ast::Expr::SearchedCase(case) => {
                (None, &case.when_clauses, case.default_value.as_ref())
            }
            ast::Expr::SimpleCase(case) => (
                Some(&case.operand),
                &case.when_clauses,
                case.default_value.as_ref(),
            ),
            _ => panic!("CASE fixture"),
        };
        let actual = ctx.analyze_expr(projection(&query), &scope).unwrap();
        assert_eq!(
            actual.value_type,
            FunctionValueType::new(DataType::Int32, true)
        );
        let ExprKind::Case {
            operand,
            when_then,
            else_expr,
            ..
        } = &actual.kind
        else {
            panic!("original CASE IR")
        };
        assert_eq!(operand.is_some(), original_operand.is_some());
        if let (Some(actual), Some(original)) = (operand, original_operand) {
            same_typed(
                actual,
                &ctx.analyze_expr(original, &scope).unwrap(),
                &control,
            );
        }
        let check_result = |actual: &TypedExpr, original: &ast::Expr| {
            let expected = ctx.analyze_expr(original, &scope).unwrap();
            if operand.is_some() {
                same_typed(actual, &expected, &control);
                return;
            }
            assert_eq!(
                actual.value_type,
                expected.value_type.with_nullable(true).unwrap()
            );
            let ExprKind::Cast {
                expr,
                target,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                cast_mode: None,
            } = &actual.kind
            else {
                panic!("searched CASE must explicitly convert to the nullable result type");
            };
            assert_eq!(target, expected.value_type.data_type());
            same_typed(expr, &expected, &control);
        };
        assert_eq!(when_then.len(), 2);
        for ((condition, result), original) in when_then.iter().zip(clauses) {
            let ast::WhenMatch::Operand(original_condition) = &original.match_value else {
                panic!("operand WHEN")
            };
            same_typed(
                condition,
                &ctx.analyze_expr(original_condition, &scope).unwrap(),
                &control,
            );
            check_result(result, &original.result);
        }
        check_result(else_expr.as_ref().unwrap(), default.unwrap());
    }
    for sql in [
        "SELECT CASE WHEN TRUE THEN NULL END",
        "SELECT CASE 1 WHEN 2 THEN NULL ELSE NULL END",
    ] {
        let query = query(sql);
        let actual = ctx.analyze_expr(projection(&query), &scope).unwrap();
        assert_eq!(
            actual.value_type,
            FunctionValueType::new(DataType::Null, true),
            "an all-NULL CASE has an unknown result type"
        );
    }
}

#[test]
fn partial_case_unique_and_invalid_exists_fail_before_inputs_and_table_lookup() {
    let span = Span::new(7, 24);
    let unknown = || build::identifier(build::ident_at("must_not_resolve", false, span));
    let partial = || ast::WhenClause {
        match_value: ast::WhenMatch::Partial(ast::Predicate::Comparison(
            ast::ComparisonPredicate {
                operator: ast::ComparisonOperator::Equal,
                right: unknown(),
                span,
            },
        )),
        result: unknown(),
        span,
    };
    let cases = vec![
        (
            ast::Expr::SearchedCase(ast::AstBox::new(ast::SearchedCaseExpr {
                when_clauses: vec![partial()].into(),
                default_value: Some(unknown()),
                span,
            })),
            "partial CASE predicates are not supported",
        ),
        (
            ast::Expr::SimpleCase(ast::AstBox::new(ast::SimpleCaseExpr {
                operand: unknown(),
                when_clauses: vec![partial()].into(),
                default_value: Some(unknown()),
                span,
            })),
            "partial CASE predicates are not supported",
        ),
        (
            ast::Expr::Unique(ast::AstBox::new(ast::UniqueExpr {
                subquery: unknown(),
                span,
            })),
            "UNIQUE subquery predicates are not supported",
        ),
        (
            ast::Expr::Exists(ast::AstBox::new(ast::ExistsExpr {
                subquery: unknown(),
                span,
            })),
            "EXISTS requires a subquery expression",
        ),
    ];
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for (expression, reason) in cases {
        let direct = ctx.analyze_expr(&expression, &scope).unwrap_err();
        assert_eq!(direct.kind(), AnalyzeErrorKind::UnsupportedExpression);
        assert_eq!(direct.message(), reason);
        assert_eq!(direct.span(), Some(span));
        let mut query = query("SELECT 1 FROM missing_table");
        let ast::SetExpr::Select(select) = query.body.as_mut() else {
            panic!("select")
        };
        select.projection[0] = novarocks_parser::build::item(expression, None);
        let whole = super::analyze(&query, &NoTables, "default").unwrap_err();
        assert_eq!(whole.kind(), AnalyzeErrorKind::UnsupportedExpression);
        assert_eq!(whole.message(), reason);
        assert_eq!(whole.span(), Some(span));
    }
}

#[test]
fn cast_safe_syntax_distinguishes_conversion_modes_and_preserves_display_contract() {
    use novarocks_type_contract::TrinoCastMode;

    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for (operand, strict_nullable, try_nullable) in [
        ("1", false, false),
        ("'invalid'", false, true),
        ("NULL", true, true),
    ] {
        let ordinary = query(&format!("SELECT CAST({operand} AS INT)"));
        let safe = query(&format!("SELECT TRY_CAST({operand} AS INT)"));
        let ast::Expr::Cast(ordinary_node) = projection(&ordinary) else {
            panic!("CAST")
        };
        let ast::Expr::Cast(safe_node) = projection(&safe) else {
            panic!("TRY_CAST")
        };
        assert!(!ordinary_node.safe);
        assert!(safe_node.safe);
        let source = ctx.analyze_expr(&ordinary_node.expression, &scope).unwrap();
        for (query, mode, nullable) in [
            (&ordinary, TrinoCastMode::Explicit, strict_nullable),
            (&safe, TrinoCastMode::Try, try_nullable),
        ] {
            let actual = ctx.analyze_expr(projection(query), &scope).unwrap();
            assert_eq!(actual.value_type.data_type(), &DataType::Int32);
            assert_eq!(
                actual.value_type.nullable(),
                nullable,
                "{operand}: {mode:?}"
            );
            let ExprKind::Cast {
                expr,
                target,
                cast_mode,
                ..
            } = &actual.kind
            else {
                panic!("conversion retains its original operand: {operand}");
            };
            same_typed(expr, &source, &control);
            assert_eq!(target, &DataType::Int32);
            assert_eq!(*cast_mode, Some(mode));
        }
        assert_eq!(
            super::display_expr_for_test(projection(&safe)),
            super::display_expr_for_test(projection(&ordinary))
        );
    }
    for (sql, expected) in [
        (
            "SELECT CASE WHEN TRUE THEN 1 ELSE 2 END",
            "CASE WHEN TRUE THEN 1 ELSE 2 END",
        ),
        (
            "SELECT CASE 1 WHEN 2 THEN 3 WHEN 4 THEN 5 END",
            "CASE 1 WHEN 2 THEN 3 WHEN 4 THEN 5 END",
        ),
        ("SELECT EXISTS (SELECT 1)", "EXISTS (SELECT 1)"),
        ("SELECT NOT EXISTS (SELECT 1)", "(NOT (EXISTS (SELECT 1)))"),
        ("SELECT x -> CAST(x AS INT)", "x -> CAST(x AS INT)"),
    ] {
        assert_eq!(
            super::display_expr_for_test(projection(&query(sql))),
            expected,
            "{sql}"
        );
    }
}

#[test]
fn array_item_borrow_retains_original_constant_ordinal_through_coercion() {
    use crate::compiler::materialized_values::MaterializedSourceBorrow;
    use arrow::array::{Array, Int32Array};
    use novarocks_functions::ConstantPool;
    use novarocks_type_contract::CompilePhase;
    use std::sync::Arc;
    let control = SqlCompileControl::unbounded();
    let field = Arc::new(arrow::datatypes::Field::new(
        "original_array_item",
        DataType::Int32,
        false,
    ));
    let pool = ConstantPool::try_new(
        field.clone(),
        FunctionValueType::new(DataType::Int32, false),
        Int32Array::from(vec![99, 5]).to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    let query = query("SELECT ARRAY[only_original_body, CAST(1 AS BIGINT)]");
    let ast::Expr::Array(array) = projection(&query) else {
        panic!("array literal")
    };
    let original = &array.values[0];
    let mut source = MaterializedSourceBorrow::empty(&query);
    source
        .insert_context_constant(original, value.clone(), &control)
        .unwrap();
    assert!(source.view().constant_at(&original.clone()).is_none());
    let mut ctx = context(&control);
    ctx.materialized_values = Some(source.view());
    let scope = ctx.new_scope();
    let expected = ctx.analyze_expr(original, &scope).unwrap();
    let actual = ctx.analyze_expr(projection(&query), &scope).unwrap();
    let ExprKind::FunctionCall { args, .. } = &actual.kind else {
        panic!("original array function binding")
    };
    let body = &args[0];
    assert_eq!(
        body.value_type,
        FunctionValueType::new(DataType::Int64, false)
    );
    let ExprKind::Cast { expr, target, .. } = &body.kind else {
        panic!("original write conversion")
    };
    assert_eq!(target, &DataType::Int64);
    same_typed(expr, &expected, &control);
    let ExprKind::Constant(retained) = &expr.kind else {
        panic!("original ARRAY item source")
    };
    assert_eq!(retained.pool().backing_identity(), pool.backing_identity());
    assert_eq!(retained.ordinal(), 1);
    assert_eq!(retained.field(), field.as_ref());
    assert_eq!(retained.value_type(), value.value_type());
}

#[test]
fn case_result_cast_path_preserves_original_constant_ordinal() {
    use crate::compiler::materialized_values::{
        MaterializedValues, SqlConstantExpressionSite, SqlConstantExpressionStep,
        SqlExpressionConstant,
    };
    use arrow::array::{Array, Int32Array};
    use novarocks_functions::ConstantPool;
    use novarocks_type_contract::CompilePhase;
    use std::sync::Arc;
    let control = SqlCompileControl::unbounded();
    let field = Arc::new(arrow::datatypes::Field::new(
        "original_case_source",
        DataType::Int32,
        false,
    ));
    let pool = ConstantPool::try_new(
        field.clone(),
        FunctionValueType::new(DataType::Int32, false),
        Int32Array::from(vec![99, 5]).to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    let query = query("SELECT CASE WHEN TRUE THEN CAST(only_original_result AS BIGINT) ELSE 2 END");
    let ast::Expr::SearchedCase(case) = projection(&query) else {
        panic!("searched CASE")
    };
    let ast::Expr::Cast(cast) = &case.when_clauses[0].result else {
        panic!("original CAST")
    };
    let source = MaterializedValues::try_new_expressions(
        &query,
        vec![
            SqlExpressionConstant::new(
                vec![],
                SqlConstantExpressionSite::SelectProjection { column: 0 },
                value.clone(),
            )
            .with_expression_path(vec![
                SqlConstantExpressionStep::CaseResult(0),
                SqlConstantExpressionStep::CastExpression,
            ]),
        ]
        .into(),
        &control,
    )
    .unwrap();
    let original = source.loan(&query, &control).unwrap();
    let mut ctx = context(&control);
    ctx.materialized_values = Some(original.view());
    let scope = ctx.new_scope();
    let expected = ctx
        .analyze_expr(&case.when_clauses[0].result, &scope)
        .unwrap();
    let actual = ctx.analyze_expr(projection(&query), &scope).unwrap();
    let ExprKind::Case { when_then, .. } = &actual.kind else {
        panic!("original CASE IR")
    };
    let branch = &when_then[0].1;
    assert_eq!(
        branch.value_type,
        FunctionValueType::new(DataType::Int64, true)
    );
    let ExprKind::Cast {
        expr: original_conversion,
        target,
        cast_mode: None,
        ..
    } = &branch.kind
    else {
        panic!("CASE result nullability conversion")
    };
    assert_eq!(target, &DataType::Int64);
    same_typed(original_conversion, &expected, &control);
    let ExprKind::Cast {
        expr,
        target,
        cast_mode: Some(novarocks_type_contract::TrinoCastMode::Explicit),
        ..
    } = &original_conversion.kind
    else {
        panic!("original source conversion")
    };
    assert_eq!(target, &DataType::Int64);
    let ExprKind::Constant(retained) = &expr.kind else {
        panic!("original CASE result source")
    };
    assert_eq!(retained.pool().backing_identity(), pool.backing_identity());
    assert_eq!(retained.ordinal(), 1);
    assert_eq!(retained.field(), field.as_ref());
    assert_eq!(retained.value_type(), value.value_type());
    assert!(
        original
            .view()
            .constant_at(&cast.expression.clone())
            .is_none()
    );
}

#[test]
fn named_arguments_and_processing_modes_refuse_before_ordinary_lookup() {
    let source = crate::planning::catalog::PlannerMemoryCatalog::default();
    let functions = crate::compiler::build_builtin_engine_function_catalog().unwrap();
    let control = crate::compiler::SqlCompileControl::unbounded();
    for (sql, expected) in [
        (
            "SELECT missing_function(named => missing_value) FROM missing_table",
            "named function arguments",
        ),
        (
            "SELECT RUNNING missing_function(missing_value) FROM missing_table",
            "processing modes",
        ),
    ] {
        let query = novarocks_parser::parser::parse_query(sql).unwrap();
        let error = super::analyze_with_function_catalog_and_sql_semantics(
            &query,
            &source,
            "default",
            &functions,
            &crate::sql_mode::SqlSemanticSettings::default(),
            crate::constant::test_constant_policy(),
            &control,
        )
        .map(|result| result.0)
        .unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::UnsupportedExpression
        );
        assert!(error.to_string().contains(expected), "{sql}: {error}");
    }
}

#[test]
fn dedicated_controls_keep_original_bindings_display_and_direct_refusals() {
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for (sql, name, arguments) in [
        (
            "SELECT if(TRUE, 1, 2)",
            "if",
            vec![build::boolean(true), build::integer(1), build::integer(2)],
        ),
        (
            "SELECT nullif(1, 2)",
            "nullif",
            vec![build::integer(1), build::integer(2)],
        ),
        (
            "SELECT coalesce(1, 2)",
            "coalesce",
            vec![build::integer(1), build::integer(2)],
        ),
    ] {
        let parsed = query(sql);
        let actual = ctx.analyze_expr(projection(&parsed), &scope).unwrap();
        let ordinary = build::function(
            ast::ObjectName {
                parts: vec![build::ident_at(name, false, novarocks_parser::SYNTHETIC)],
                span: novarocks_parser::SYNTHETIC,
            },
            arguments,
        );
        let expected = ctx.analyze_expr(&ordinary, &scope).unwrap();
        same_typed(&actual, &expected, &control);
        assert_eq!(
            super::helpers::expr_display_name(projection(&parsed)),
            super::helpers::expr_display_name(&ordinary)
        );
    }
    for (sql, kind) in [
        (
            "SELECT if(TRUE, missing_value)",
            AnalyzeErrorKind::UnsupportedExpression,
        ),
        ("SELECT try(missing_value)", AnalyzeErrorKind::UnknownColumn),
        (
            "SELECT format(missing_format, missing_value)",
            AnalyzeErrorKind::UnknownColumn,
        ),
    ] {
        let parsed = query(sql);
        let error = ctx.analyze_expr(projection(&parsed), &scope).unwrap_err();
        assert_eq!(error.kind(), kind, "{sql}: {error}");
    }
    for sql in [
        "SELECT typeof(CAST(if(TRUE, missing_value) AS INT))",
        "SELECT typeof(CAST(try(missing_value) AS INT))",
        "SELECT typeof(CAST(format(missing_format, missing_value) AS INT))",
    ] {
        let parsed = query(sql);
        let actual = ctx.analyze_expr(projection(&parsed), &scope).unwrap();
        assert!(
            matches!(&actual.kind, ExprKind::Literal(crate::analysis::LiteralValue::String(value)) if value == "int"),
            "{sql}: {actual:?}"
        );
    }
}
