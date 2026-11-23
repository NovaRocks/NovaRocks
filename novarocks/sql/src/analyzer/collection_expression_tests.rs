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

//! Exact collection conversion boundaries before syntax normalization.
use super::AnalyzerContext;
use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
use crate::compiler::SqlCompileControl;
use arrow::datatypes::DataType;
use novarocks_parser::ast;

struct NoTables;
impl PlannerTableProvider for NoTables {
    fn resolve_table_for_analysis(
        &self,
        _: Option<&str>,
        _: &str,
        _: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        panic!("collection conversion must not discover a table")
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
        panic!("SELECT fixture");
    };
    let [ast::SelectItem::SingleColumn(projection_column)] = select.projection.as_slice() else {
        panic!("one expression fixture");
    };
    assert!(projection_column.alias.is_none(), "anonymous projection");
    &projection_column.expression
}
fn json_list_target() -> novarocks_type_contract::FunctionValueType {
    use arrow::datatypes::{DataType, Field};
    use novarocks_type_contract::{
        FunctionValueType, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    let item =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let value = ValueTypeRef::try_new(
        ValueKind::List(item.value_type().clone()),
        true,
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let layout = ValueLayout::try_from_normalized_arrow(
        DataType::List(std::sync::Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))),
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    FunctionValueType::try_from_value(value, layout).unwrap()
}
fn same_typed(
    actual: &crate::analysis::TypedExpr,
    expected: &crate::analysis::TypedExpr,
    control: &SqlCompileControl,
) {
    assert_eq!(actual.value_type, expected.value_type);
    assert!(
        crate::analysis::expr_identity::typed_expr_semantically_eq(actual, expected, control)
            .unwrap(),
        "original typed source, binding, values and order must remain unchanged"
    );
}

#[test]
fn null_child_arrays_cast_to_the_complete_json_list_declaration() {
    use crate::analysis::{ExprKind, LiteralValue};
    use arrow::datatypes::DataType;
    use novarocks_type_contract::ValueLogicalType;
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    let expected = json_list_target().with_nullable(false).unwrap();
    for (sql, len) in [
        ("SELECT CAST([] AS ARRAY<JSON>)", 0),
        ("SELECT CAST([NULL,NULL] AS ARRAY<JSON>)", 2),
    ] {
        let query = query(sql);
        let ast::Expr::Cast(cast) = projection(&query) else {
            panic!("ordinary CAST");
        };
        let original = ctx.analyze_expr(&cast.expression, &scope).unwrap();
        let source_item = original.value_type.child_at(0).unwrap();
        assert_eq!(source_item.data_type(), &DataType::Null);
        assert_eq!(source_item.logical_type(), ValueLogicalType::Physical);
        let ExprKind::FunctionCall { name, args, .. } = &original.kind else {
            panic!("original array constructor");
        };
        assert_eq!(name, "__array_literal");
        assert_eq!(args.len(), len);
        assert!(
            args.iter()
                .all(|value| matches!(value.kind, ExprKind::Literal(LiteralValue::Null)))
        );
        let actual = ctx.analyze_expr(projection(&query), &scope).unwrap();
        assert_eq!(actual.value_type, expected);
        assert_eq!(
            actual.value_type.try_to_field("result").unwrap(),
            expected.try_to_field("result").unwrap()
        );
        let item = actual.value_type.child_at(0).unwrap();
        assert_eq!(item.logical_type(), ValueLogicalType::Json);
        assert_eq!(item.data_type(), &DataType::Utf8);
        assert!(item.nullable());
        assert!(!actual.value_type.nullable());
        let ExprKind::Cast { expr, target, .. } = &actual.kind else {
            panic!("original intrinsic CAST");
        };
        assert_eq!(target, expected.data_type());
        same_typed(expr, &original, &control);
    }
}

#[test]
fn full_json_list_conversion_does_not_admit_nonempty_utf8_lists() {
    use crate::analyze_error::AnalyzeErrorKind;
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    let source = ctx
        .analyze_expr(projection(&query("SELECT ['not json']")), &scope)
        .unwrap();
    let error = super::value_conversion::cast_to_value_type(
        source,
        json_list_target(),
        novarocks_parser::Span::new(7, 27),
        ctx.function_catalog,
        &ctx.sql_semantics,
        ctx.constant_policy,
        &control,
    )
    .unwrap_err();
    assert_eq!(error.kind(), AnalyzeErrorKind::TypeMismatch);
    assert_eq!(error.message(), "no matching declared overload");
}

#[test]
fn root_null_keeps_the_explicit_array_boundary_and_original_null_source() {
    use crate::analysis::{ExprKind, LiteralValue};
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    let actual = ctx
        .analyze_expr(
            projection(&query("SELECT CAST(NULL AS ARRAY<JSON>)")),
            &scope,
        )
        .unwrap();
    assert_eq!(actual.value_type, json_list_target());
    let ExprKind::Cast {
        expr,
        cast_mode,
        target,
        ..
    } = actual.kind
    else {
        panic!("explicit array conversion retains its source before optional folding")
    };
    assert_eq!(
        cast_mode,
        Some(novarocks_type_contract::TrinoCastMode::Explicit)
    );
    assert_eq!(target, json_list_target().data_type().clone());
    assert!(matches!(expr.kind, ExprKind::Literal(LiteralValue::Null)));
    assert_eq!(expr.value_type.data_type(), &DataType::Null);
}

#[test]
fn map_constructor_keeps_its_exact_overload_and_ordered_array_sources() {
    use crate::analysis::ExprKind;
    use novarocks_functions::{FunctionArgumentType, FunctionResultType};

    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for (sql, overload, sources, child_types) in [
        (
            "SELECT map()",
            "builtin.scalar/map/empty-v1",
            &[][..],
            [DataType::Null, DataType::Null],
        ),
        (
            "SELECT map(ARRAY[1, 2], ARRAY['one', 'two'])",
            "builtin.scalar/map/array-keys-array-values-v1",
            &["SELECT ARRAY[1, 2]", "SELECT ARRAY['one', 'two']"][..],
            [DataType::Int32, DataType::Utf8],
        ),
    ] {
        let actual = ctx.analyze_expr(projection(&query(sql)), &scope).unwrap();
        let ExprKind::FunctionCall {
            name,
            args,
            binding,
            ..
        } = &actual.kind
        else {
            panic!("selected MAP constructor");
        };
        assert_eq!(name, "map");
        assert_eq!(binding.function_id.as_str(), "builtin.scalar/map/v1");
        assert_eq!(binding.selected.overload.as_str(), overload);
        assert_eq!(args.len(), sources.len());
        assert_eq!(binding.selected.argument_types.len(), sources.len());
        for ((argument, selected), source) in args
            .iter()
            .zip(binding.selected.argument_types.iter())
            .zip(sources)
        {
            let original = ctx
                .analyze_expr(projection(&query(source)), &scope)
                .unwrap();
            same_typed(argument, &original, &control);
            assert_eq!(
                selected,
                &FunctionArgumentType::Value(argument.value_type.clone())
            );
        }
        assert_eq!(
            binding.selected.result_type,
            FunctionResultType::Scalar(actual.value_type.clone())
        );
        assert!(matches!(actual.value_type.data_type(), DataType::Map(..)));
        for (index, data_type) in child_types.iter().enumerate() {
            assert_eq!(
                actual.value_type.child_at(index).unwrap().data_type(),
                data_type
            );
        }
    }
}

#[test]
fn null_list_conversion_preserves_each_original_control_refusal() {
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::sync::Mutex;
    struct Control {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        stop: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            if let Some((at, _)) = &self.stop {
                assert!(trace.len() < *at, "no callback after the original refusal");
            }
            trace.push((phase, units));
            match &self.stop {
                Some((at, error)) if trace.len() == *at => Err(error.clone()),
                _ => Ok(()),
            }
        }
    }
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    let source = ctx
        .analyze_expr(projection(&query("SELECT [NULL,NULL]")), &scope)
        .unwrap();
    let run = |control: &Control| {
        super::value_conversion::cast_to_value_type(
            source.clone(),
            json_list_target(),
            novarocks_parser::Span::new(7, 18),
            ctx.function_catalog,
            &ctx.sql_semantics,
            ctx.constant_policy,
            control,
        )
    };
    let successful = Control {
        trace: Default::default(),
        stop: None,
    };
    run(&successful).unwrap();
    let trace = successful.trace.into_inner().unwrap();
    assert!(!trace.is_empty());
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=trace.len() {
            let control = Control {
                trace: Default::default(),
                stop: Some((at, error.clone())),
            };
            let actual = run(&control).unwrap_err();
            assert_eq!(actual.control_error(), Some(error.clone()));
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}

#[test]
fn null_json_list_conversion_plan_retains_the_explicit_carrier_step() {
    use crate::compiler::selected_scalar_call::{PlannedConversion, plan_conversion};
    use novarocks_type_contract::DecimalOverflowPolicy;
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let source = ctx
        .analyze_expr(projection(&query("SELECT []")), &ctx.new_scope())
        .unwrap();
    let target = json_list_target();
    let conversion = plan_conversion(
        ctx.function_catalog,
        &source.value_type,
        &target,
        DecimalOverflowPolicy::OutputNull,
        None,
        &control,
    )
    .unwrap();
    let PlannedConversion::Carrier(step) = conversion else {
        panic!("the declared NULL lifting must remain an original carrier CAST")
    };
    assert_eq!(step.target(), &target);
    assert_eq!(
        step.cast_mode(),
        novarocks_type_contract::TrinoCastMode::Implicit
    );
    let text = ctx
        .analyze_expr(projection(&query("SELECT ['not json']")), &ctx.new_scope())
        .unwrap();
    assert!(
        plan_conversion(
            ctx.function_catalog,
            &text.value_type,
            &target,
            DecimalOverflowPolicy::OutputNull,
            None,
            &control
        )
        .is_err()
    );
}

#[test]
fn explicit_array_cast_modes_preserve_independent_source_and_full_declared_target() {
    use crate::analysis::ExprKind;
    use novarocks_type_contract::TrinoCastMode;
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for (sql, mode, nullable) in [
        (
            "SELECT CAST([] AS ARRAY<JSON>)",
            TrinoCastMode::Explicit,
            false,
        ),
        (
            "SELECT TRY_CAST([NULL,NULL] AS ARRAY<JSON>)",
            TrinoCastMode::Try,
            false,
        ),
        (
            "SELECT TRY_CAST(['invalid'] AS ARRAY<INT>)",
            TrinoCastMode::Try,
            true,
        ),
        (
            "SELECT CAST([1,2] AS ARRAY<INT>)",
            TrinoCastMode::Explicit,
            false,
        ),
    ] {
        let query = query(sql);
        let ast::Expr::Cast(cast) = projection(&query) else {
            panic!("explicit CAST")
        };
        let original = ctx.analyze_expr(&cast.expression, &scope).unwrap();
        let declared =
            super::helpers::sql_type_to_value_type(&cast.data_type, nullable, &control).unwrap();
        let actual = ctx.analyze_expr(projection(&query), &scope).unwrap();
        assert_eq!(actual.value_type, declared);
        assert_eq!(
            actual.value_type.try_to_field("result").unwrap(),
            declared.try_to_field("result").unwrap()
        );
        let ExprKind::Cast {
            expr,
            target,
            cast_mode,
            ..
        } = &actual.kind
        else {
            panic!("explicit boundary survives analysis")
        };
        assert_eq!(*cast_mode, Some(mode));
        assert_eq!(target, declared.data_type());
        same_typed(expr, &original, &control);
        if sql.contains("[1,2]") {
            assert_eq!(
                expr.value_type.child_at(0).unwrap().data_type(),
                &DataType::Int32,
                "the independent integer ARRAY keeps its original common type, even when the target happens to be the same type"
            );
        }
    }
}

#[test]
fn array_cast_modes_have_distinct_semantic_and_scalar_identity_and_survive_rebuild() {
    use crate::analysis::{ExprKind, TypedExpr};
    use crate::optimizer::scalar::ScalarArena;
    use crate::planner::optimizer_bridge::scalar::{intern_typed, materialize};
    use novarocks_type_contract::{DecimalOverflowPolicy, TrinoCastMode};
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    let source = ctx
        .analyze_expr(projection(&query("SELECT [NULL]")), &scope)
        .unwrap();
    let target = source.value_type.clone().with_nullable(true).unwrap();
    let mut arena = ScalarArena::new();
    let mut ids = Vec::new();
    let mut typed = Vec::new();
    for mode in [
        None,
        Some(TrinoCastMode::Explicit),
        Some(TrinoCastMode::Try),
    ] {
        let actual = TypedExpr {
            kind: ExprKind::Cast {
                expr: novarocks_parser::ast::AstBox::new(source.clone()),
                target: target.data_type().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                cast_mode: mode,
            },
            value_type: target.clone(),
        };
        let id = intern_typed(&mut arena, &actual, &control).unwrap();
        let rebuilt = materialize(&arena, id);
        same_typed(&actual, &rebuilt, &control);
        assert_eq!(id, intern_typed(&mut arena, &rebuilt, &control).unwrap());
        let display =
            crate::analysis::expr_display::typed_expr_display_name(&actual, &control).unwrap();
        if mode == Some(TrinoCastMode::Try) {
            assert!(display.starts_with("try_cast("));
        } else if mode.is_some() {
            assert!(display.starts_with("cast("));
        }
        ids.push(id);
        typed.push(actual);
    }
    let nested_try = TypedExpr {
        kind: ExprKind::Cast {
            expr: novarocks_parser::ast::AstBox::new(typed[1].clone()),
            target: target.data_type().clone(),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            cast_mode: Some(TrinoCastMode::Try),
        },
        value_type: target.clone(),
    };
    let nested_id = intern_typed(&mut arena, &nested_try, &control).unwrap();
    assert!(
        crate::optimizer::scalar_expr::can_fail(&arena, ids[1]),
        "strict conversion retains its own Data failure possibility"
    );
    assert!(
        crate::optimizer::scalar_expr::can_fail(&arena, nested_id),
        "outer TRY cannot suppress a source expression's failure"
    );
    for left in 0..ids.len() {
        for right in left + 1..ids.len() {
            assert_ne!(ids[left], ids[right]);
            assert!(
                !crate::analysis::expr_identity::typed_expr_semantically_eq(
                    &typed[left],
                    &typed[right],
                    &control
                )
                .unwrap()
            );
        }
    }
}

#[test]
fn ordinary_array_common_targets_preserve_original_operands_and_use_trino_rules() {
    use crate::analysis::ExprKind;
    use novarocks_type_contract::TrinoCastMode;
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for (sql, expected) in [
        (
            "SELECT [CAST(1 AS INT),CAST(2 AS SMALLINT)]",
            DataType::Int32,
        ),
        ("SELECT [CAST(1 AS REAL),CAST(2 AS INT)]", DataType::Float32),
        ("SELECT [9007199254740993,1e0]", DataType::Float64),
        (
            "SELECT [CAST(0 AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))]",
            DataType::Decimal128(38, 38),
        ),
        ("SELECT []", DataType::Null),
        ("SELECT [NULL,NULL]", DataType::Null),
    ] {
        let parsed = query(sql);
        let ast::Expr::Array(source_array) = projection(&parsed) else {
            panic!("original array")
        };
        let originals = source_array
            .values
            .iter()
            .map(|item| ctx.analyze_expr(item, &scope).unwrap())
            .collect::<Vec<_>>();
        let actual = ctx
            .analyze_expr(projection(&parsed), &scope)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert!(!actual.value_type.nullable());
        let item = actual.value_type.child_at(0).unwrap();
        assert_eq!(item.data_type(), &expected, "{sql}");
        assert!(item.nullable());
        let ExprKind::FunctionCall { binding, args, .. } = &actual.kind else {
            panic!("original constructor")
        };
        assert_eq!(
            binding.function_id.as_str(),
            "builtin.scalar/__array_literal/v1"
        );
        assert_eq!(args.len(), originals.len());
        assert_eq!(binding.selected.argument_types.len(), originals.len());
        for (argument, original) in args.iter().zip(&originals) {
            // An existing explicit CAST is part of the original source.
            if original.value_type.data_type() == item.data_type()
                || original.value_type.data_type() == &DataType::Null
            {
                same_typed(argument, original, &control);
                continue;
            }
            let ExprKind::Cast {
                cast_mode: Some(TrinoCastMode::Implicit),
                expr,
                target,
                ..
            } = &argument.kind
            else {
                panic!("the original array item requires a common-type conversion: {sql}");
            };
            same_typed(expr, original, &control);
            assert_eq!(target, item.data_type());
            assert_eq!(
                argument.value_type.nullable(),
                original.value_type.nullable()
            );
        }
    }
}

#[test]
fn ordinary_array_common_does_not_coerce_numeric_or_json_values_to_text() {
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for sql in [
        "SELECT [1,'2']",
        "SELECT [JSON '1','2']",
        "SELECT [[1],['2']]",
    ] {
        let parsed = query(sql);
        let error = ctx.analyze_expr(projection(&parsed), &scope).unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::TypeMismatch,
            "{sql}: {error}"
        );
    }
    for sql in ["SELECT [[NULL],NULL]", "SELECT [JSON '1',NULL]"] {
        let parsed = query(sql);
        let actual = ctx
            .analyze_expr(projection(&parsed), &scope)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert!(!actual.value_type.nullable());
    }
}

#[test]
fn multi_column_in_decomposes_only_anonymous_row_fields() {
    use crate::analyze_error::AnalyzeErrorKind;
    use novarocks_parser::build;

    let control = SqlCompileControl::unbounded();
    let positional = query("SELECT 1 WHERE (1, 2) IN (SELECT 1, 2)");
    context(&control)
        .analyze_query(&positional)
        .expect("anonymous fields retain positional IN analysis");

    let mut named = positional;
    let ast::SetExpr::Select(select) = named.body.as_mut() else {
        panic!("SELECT fixture");
    };
    let Some(ast::Expr::Predicated(predicate)) = select.selection.as_mut() else {
        panic!("WHERE IN fixture");
    };
    let ast::Expr::Row(row) = &mut predicate.value else {
        panic!("positional row fixture");
    };
    // A named field has no positional IN capability, even if all the other
    // fields are anonymous. Its value must not be resolved as a scalar first.
    row.fields[0].name = Some(build::ident("First", true));
    row.fields[0].expression = build::identifier(build::ident("missing", false));
    let ctx = context(&control);
    let error = ctx.analyze_query(&named).unwrap_err();
    assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
    assert!(ctx.collected_subqueries.borrow().is_empty());
}

#[test]
fn unsupported_access_profiles_refuse_before_base_or_table_resolution() {
    use crate::analyze_error::AnalyzeErrorKind;
    use novarocks_parser::{Span, build};
    let span = Span::new(7, 31);
    for expression in [
        ast::Expr::Dereference(ast::AstBox::new(ast::DereferenceExpr {
            base: build::identifier(build::ident("unknown_base", false)),
            field: None,
            span,
        })),
        ast::Expr::ArrayWildcardSubscript(ast::AstBox::new(ast::ArrayWildcardSubscriptExpr {
            base: build::identifier(build::ident("unknown_base", false)),
            span,
        })),
    ] {
        let mut parsed = query("SELECT 1 FROM unknown_table");
        let ast::SetExpr::Select(select) = parsed.body.as_mut() else {
            panic!("SELECT fixture");
        };
        select.projection[0] = novarocks_parser::build::item(expression, None);
        // NoTables panics on lookup, so this also protects public prepass order.
        let error = crate::analyzer::analyze(&parsed, &NoTables, "default").unwrap_err();
        assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
        assert_eq!(error.span(), Some(span));
    }
}

#[test]
fn anonymous_row_uses_original_scalar_binding_and_named_fields_refuse_directly() {
    use crate::analysis::ExprKind;
    use novarocks_parser::{ast, build};
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for (sql, type_name) in [
        ("SELECT ROW(7)", "struct<col1 int>"),
        (
            "SELECT ROW(CAST(1.25 AS DECIMAL(5,2)), [1, NULL])",
            "struct<col1 decimal128(5, 2), col2 array<int>>",
        ),
    ] {
        let parsed = query(sql);
        let ast::Expr::Row(row) = projection(&parsed) else {
            panic!("row source");
        };
        let ordinary = build::function(
            ast::ObjectName {
                parts: vec![build::ident_at("row", false, novarocks_parser::SYNTHETIC)],
                span: novarocks_parser::SYNTHETIC,
            },
            row.fields
                .iter()
                .map(|field| field.expression.clone())
                .collect(),
        );
        let actual = ctx.analyze_expr(projection(&parsed), &scope).unwrap();
        let error = ctx.analyze_expr(&ordinary, &scope).unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::UnknownFunction
        );
        let ExprKind::FunctionCall {
            name,
            binding,
            args,
            ..
        } = &actual.kind
        else {
            panic!("row binding");
        };
        assert_eq!(name, "row");
        assert_eq!(binding.function_id.as_str(), "builtin.scalar/row/v1");
        assert_eq!(
            binding.selected.overload.as_str(),
            "builtin.scalar/row/dynamic-v1"
        );
        assert_eq!(args.len(), row.fields.len());
        assert_eq!(binding.selected.argument_types.len(), row.fields.len());
        for ((argument, field), selected) in args
            .iter()
            .zip(&row.fields)
            .zip(binding.selected.argument_types.iter())
        {
            let original = ctx.analyze_expr(&field.expression, &scope).unwrap();
            same_typed(argument, &original, &control);
            assert_eq!(
                selected,
                &novarocks_functions::FunctionArgumentType::Value(argument.value_type.clone())
            );
        }
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("scalar");
        };
        assert_eq!(result, &actual.value_type);
        let typeof_call = |value| {
            build::function(
                ast::ObjectName {
                    parts: vec![build::ident_at(
                        "typeof",
                        false,
                        novarocks_parser::SYNTHETIC,
                    )],
                    span: novarocks_parser::SYNTHETIC,
                },
                vec![value],
            )
        };
        let inspected = ctx
            .analyze_expr(&typeof_call(projection(&parsed).clone()), &scope)
            .unwrap();
        assert_eq!(
            inspected.value_type,
            novarocks_functions::FunctionValueType::new(DataType::Utf8, false)
        );
        assert!(matches!(
            inspected.kind,
            ExprKind::Literal(crate::analysis::LiteralValue::String(value)) if value == type_name
        ));
    }
    let names = ctx
        .analyze_query(&query("SELECT ROW(7), map(), ROW(7) AS Kept"))
        .unwrap();
    assert_eq!(
        names
            .output_columns
            .iter()
            .map(|field| field.field_name.as_deref())
            .collect::<Vec<_>>(),
        vec![None, None, Some("Kept")]
    );
    let derived = ctx
        .analyze_query(&query("SELECT q.Kept FROM (SELECT ROW(7) AS Kept) q"))
        .unwrap();
    assert_eq!(
        derived.output_columns[0].field_name.as_deref(),
        Some("Kept")
    );
    let mut parsed = query("SELECT ROW(1)");
    let ast::SetExpr::Select(select) = parsed.body.as_mut() else {
        panic!("select");
    };
    let ast::SelectItem::SingleColumn(projection_column) = &mut select.projection[0] else {
        panic!("row");
    };
    assert!(projection_column.alias.is_none(), "anonymous projection");
    let ast::Expr::Row(row) = &mut projection_column.expression else {
        panic!("row");
    };
    row.fields[0].name = Some(build::ident("Named", true));
    row.fields[0].expression = build::identifier(build::ident("must_not_resolve", false));
    let error = ctx.analyze_expr(projection(&parsed), &scope).unwrap_err();
    assert_eq!(
        error.kind(),
        crate::analyze_error::AnalyzeErrorKind::UnsupportedExpression
    );
}
