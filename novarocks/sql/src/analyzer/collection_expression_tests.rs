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
        panic!("one query fixture");
    };
    query.clone()
}
fn projection(query: &ast::Query) -> &ast::Expr {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("SELECT fixture");
    };
    let [ast::SelectItem::UnnamedExpr(expression)] = select.projection.as_slice() else {
        panic!("one expression fixture");
    };
    expression
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
    let expected = json_list_target();
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
        assert!(actual.value_type.nullable());
        let ExprKind::Cast { expr, target, .. } = &actual.kind else {
            panic!("original intrinsic CAST");
        };
        assert_eq!(target, expected.data_type());
        same_typed(expr, &original, &control);
    }
}

#[test]
fn null_list_cast_retains_selected_constant_field_and_write_assignment_source() {
    use crate::{
        analysis::ExprKind,
        compiler::{SqlWriteAssignmentSite, materialized_values::MaterializedSourceBorrow},
    };
    use arrow::{
        array::{Array, ListArray, NullArray},
        buffer::OffsetBuffer,
        datatypes::{DataType, Field},
    };
    use novarocks_functions::ConstantPool;
    use novarocks_type_contract::{CompilePhase, FunctionValueType};
    use std::{borrow::Cow, sync::Arc};
    let control = SqlCompileControl::unbounded();
    let item = Arc::new(Field::new("original_null_item", DataType::Null, true));
    let array = ListArray::try_new(
        item,
        OffsetBuffer::new(vec![0_i32, 0, 2].into()),
        Arc::new(NullArray::new(2)),
        None,
    )
    .unwrap();
    let field = Arc::new(Field::new(
        "original_null_list",
        array.data_type().clone(),
        false,
    ));
    let source_type = FunctionValueType::new(array.data_type().clone(), false);
    let pool = ConstantPool::try_new(
        field.clone(),
        source_type.clone(),
        array.to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    let query = query("SELECT CAST(original_null_list AS ARRAY<JSON>)");
    let ast::Expr::Cast(cast) = projection(&query) else {
        panic!("ordinary CAST");
    };
    let mut source = MaterializedSourceBorrow::empty(&query);
    source
        .insert_context_constant(&cast.expression, value.clone(), &control)
        .unwrap();
    source
        .insert_write_assignment(
            &cast.expression,
            Cow::Owned(
                SqlWriteAssignmentSite::try_new(0, source_type.clone(), None, &control)
                    .unwrap()
                    .with_source_span(cast.expression.span()),
            ),
            &control,
        )
        .unwrap();
    assert!(
        source
            .view()
            .constant_at(&cast.expression.clone())
            .is_none()
    );
    let mut ctx = context(&control);
    ctx.materialized_values = Some(source.view());
    let scope = ctx.new_scope();
    let actual = ctx.analyze_expr(projection(&query), &scope).unwrap();
    assert_eq!(actual.value_type, json_list_target());
    let ExprKind::Cast { expr, .. } = &actual.kind else {
        panic!("intrinsic outer cast");
    };
    assert_eq!(expr.value_type, source_type);
    let ExprKind::Constant(retained) = &expr.kind else {
        panic!("original materialized source");
    };
    assert_eq!(retained.ordinal(), 1);
    assert_eq!(retained.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(retained.pool().field_ref(), &field));
    assert_eq!(retained.field(), field.as_ref());
    assert_eq!(retained.value_type(), value.value_type());
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
fn root_null_uses_typed_null_author_for_a_json_list_target() {
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
    assert!(matches!(actual.kind, ExprKind::Literal(LiteralValue::Null)));
}

#[test]
fn map_literals_keep_the_existing_alternating_argument_binding() {
    let control = SqlCompileControl::unbounded();
    let ctx = context(&control);
    let scope = ctx.new_scope();
    for (literal, call) in [
        ("SELECT map{}", "SELECT map()"),
        ("SELECT map{1:'one',2:'two'}", "SELECT map(1,'one',2,'two')"),
    ] {
        let literal = ctx
            .analyze_expr(projection(&query(literal)), &scope)
            .unwrap();
        let call = ctx.analyze_expr(projection(&query(call)), &scope).unwrap();
        same_typed(&literal, &call, &control);
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
    for allow in [false, true] {
        let conversion = plan_conversion(
            ctx.function_catalog,
            &source.value_type,
            &target,
            DecimalOverflowPolicy::OutputNull,
            allow,
            &control,
        )
        .unwrap();
        let PlannedConversion::Carrier(step) = conversion else {
            panic!("the declared NULL lifting must remain an original carrier CAST")
        };
        assert_eq!(step.target(), &target);
        assert_eq!(
            step.decimal_overflow_policy(),
            DecimalOverflowPolicy::OutputNull
        );
        assert_eq!(step.allow_throw_exception(), allow);
    }
    let text = ctx
        .analyze_expr(projection(&query("SELECT ['not json']")), &ctx.new_scope())
        .unwrap();
    assert!(
        plan_conversion(
            ctx.function_catalog,
            &text.value_type,
            &target,
            DecimalOverflowPolicy::OutputNull,
            false,
            &control
        )
        .is_err()
    );
}
