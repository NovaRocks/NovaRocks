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

use super::*;
use crate::analysis::{
    AnalyzedOutputField as OutputColumn, AnalyzedProjectItem as ProjectItem, ExprKind,
    LiteralValue, ResolvedSelect, TypedExpr,
};
fn ty(data: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(data, nullable)
}
fn query(expr: TypedExpr) -> (ResolvedQuery, ColumnRefFactory) {
    let mut factory = ColumnRefFactory::new();
    let id = factory.create(None, "__nr_sel_0".into(), expr.value_type.clone());
    let output = OutputColumn {
        field_name: Some("v".into()),
        column_id: id,
        name: "__nr_sel_0".into(),
        value_type: expr.value_type.clone(),
        is_internal: false,
    };
    (
        ResolvedQuery {
            body: QueryBody::Select(ResolvedSelect {
                from: None,
                filter: None,
                group_by: vec![],
                having: None,
                projection: vec![ProjectItem {
                    field_name: Some("v".into()),
                    expr,
                    output_name: output.name.clone(),
                    output_column_id: id,
                }],
                has_aggregation: false,
                distinct: false,
                repeat: None,
                apply_specs: vec![],
                predicate_apply_specs: vec![],
            }),
            order_by: vec![],
            limit: None,
            offset: None,
            output_columns: vec![output],
            local_cte_ids: vec![],
        },
        factory,
    )
}
fn apply(
    query: &mut ResolvedQuery,
    factory: &mut ColumnRefFactory,
    target: &[FunctionValueType],
) -> Result<(), SqlCompileError> {
    apply_root_output_declaration(
        query,
        factory,
        Some(target),
        crate::functions::builtin_sql_function_catalog(),
        &Default::default(),
        crate::constant::test_constant_policy(),
        &SqlCompileControl::unbounded(),
    )
}
fn projection(query: &ResolvedQuery) -> &ProjectItem {
    let QueryBody::Select(select) = &query.body else {
        panic!("expected SELECT")
    };
    &select.projection[0]
}
#[test]
fn root_output_fixed_self_assignment_and_case_keep_exact_width() {
    for data in [
        DataType::FixedSizeBinary(16),
        DataType::Struct(vec![Field::new("f", DataType::FixedSizeBinary(4), false)].into()),
    ] {
        let declared = ty(data, false);
        let actual = widen_declaration(&declared, &SqlCompileControl::unbounded()).unwrap();
        let reference = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: crate::column_id::ColumnId(91),
                qualifier: None,
                column: "v".into(),
            },
            value_type: actual.clone(),
        };
        for expr in [
            reference.clone(),
            TypedExpr {
                kind: ExprKind::Case {
                    operand: None,
                    when_then: vec![(
                        TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::Bool(true)),
                            value_type: ty(DataType::Boolean, false),
                        },
                        reference.clone(),
                    )],
                    else_expr: Some(Box::new(reference.clone())),
                },
                value_type: actual.clone(),
            },
        ] {
            let (mut query, mut factory) = query(expr);
            let old = query.output_columns[0].column_id;
            apply(&mut query, &mut factory, std::slice::from_ref(&declared)).unwrap();
            assert_eq!(projection(&query).expr.value_type, actual);
            assert_eq!(query.output_columns[0].column_id, old);
            assert!(!matches!(
                projection(&query).expr.kind,
                ExprKind::Cast { .. }
            ));
        }
    }
}
#[test]
fn root_output_carrier_conversion_mints_independent_output_column() {
    for data in [DataType::Int8, DataType::Int32] {
        let source = ty(DataType::Int64, false);
        let (mut query, mut factory) = query(TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: crate::column_id::ColumnId(91),
                qualifier: None,
                column: "v".into(),
            },
            value_type: source.clone(),
        });
        let old = query.output_columns[0].column_id;
        apply(&mut query, &mut factory, &[ty(data.clone(), false)]).unwrap();
        assert_ne!(query.output_columns[0].column_id, old);
        assert_eq!(factory.get(old).value_type, source);
        assert_eq!(projection(&query).expr.value_type.data_type(), &data);
        assert!(projection(&query).expr.value_type.nullable());
        assert!(matches!(
            projection(&query).expr.kind,
            ExprKind::Cast { .. }
        ));
    }
}
#[test]
fn root_output_literal_null_is_typed_without_evaluating_conversion() {
    let (mut query, mut factory) = query(TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Null),
        value_type: ty(DataType::Null, true),
    });
    apply(
        &mut query,
        &mut factory,
        &[ty(DataType::FixedSizeBinary(4), false)],
    )
    .unwrap();
    assert_eq!(
        projection(&query).expr.value_type.data_type(),
        &DataType::FixedSizeBinary(4)
    );
    assert!(projection(&query).expr.value_type.nullable());
    assert!(!matches!(
        projection(&query).expr.kind,
        ExprKind::Cast { .. } | ExprKind::FunctionCall { .. }
    ));
}
#[test]
fn root_output_largeint_uses_selected_value_conversion_owner() {
    let source = TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: crate::column_id::ColumnId(91),
            qualifier: None,
            column: "v".into(),
        },
        value_type: ty(DataType::Int64, false),
    };
    let target = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        novarocks_type_contract::ValueLogicalType::LargeInt,
    )
    .unwrap();
    let (mut query, mut factory) = query(source);
    apply(&mut query, &mut factory, &[target]).unwrap();
    let mut expr = &projection(&query).expr;
    if let ExprKind::Cast { expr: child, .. } = &expr.kind {
        expr = child;
    }
    let ExprKind::FunctionCall { name, .. } = &expr.kind else {
        panic!("nominal establishment requires its selected owner")
    };
    assert_eq!(
        name,
        novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_NAME
    );
    assert_eq!(
        projection(&query).expr.value_type.logical_type(),
        novarocks_type_contract::ValueLogicalType::LargeInt
    );
}
#[test]
fn root_output_width_mismatch_is_invalid_request() {
    let (mut query, mut factory) = query(TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(1)),
        value_type: ty(DataType::Int64, false),
    });
    assert!(matches!(
        apply(&mut query, &mut factory, &[]),
        Err(SqlCompileError::InvalidRequest(_))
    ));
}
#[test]
fn root_output_widening_keeps_map_structural_roles_required() {
    let map = ty(
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        false,
    );
    let widened = widen_declaration(&map, &SqlCompileControl::unbounded()).unwrap();
    assert!(widened.nullable());
    let DataType::Map(entries, _) = widened.data_type() else {
        panic!("expected MAP")
    };
    assert!(!entries.is_nullable());
    let DataType::Struct(fields) = entries.data_type() else {
        panic!("expected entries")
    };
    assert!(!fields[0].is_nullable());
    assert!(fields[1].is_nullable());
    assert!(root_output_type_matches(&widened, &map, &SqlCompileControl::unbounded()).unwrap());
    assert!(
        !root_output_type_matches(
            &ty(DataType::FixedSizeBinary(4), true),
            &ty(DataType::FixedSizeBinary(16), true),
            &SqlCompileControl::unbounded()
        )
        .unwrap()
    );
}

#[test]
fn root_output_request_is_internal_only_and_applied_by_real_analyzer() {
    use crate::compiler::{
        SqlAnalyzeRequest, SqlCompileIntent, SqlCompiler, SqlPlannerTableSnapshot,
        SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    };
    let catalog = crate::catalog::memory::PlannerMemoryCatalog::default();
    let snapshot = SqlPlannerTableSnapshot::new(&catalog);
    let request = |intent| {
        SqlAnalyzeRequest::new(
            SqlStatementInput::sql("SELECT 7 AS __nr_sel_0"),
            intent,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: None,
                current_database: "default".into(),
                optimizer_settings: Default::default(),
            },
            SqlPlanningEnvironment::Distributed,
            &snapshot,
            crate::functions::builtin_sql_function_catalog(),
            crate::compiler::noop_constant_evaluator(),
            None,
            crate::constant::test_constant_policy(),
            SqlCompileControl::unbounded(),
        )
    };
    assert!(matches!(
        request(SqlCompileIntent::Query)
            .with_root_output_declaration(vec![ty(DataType::Int8, false)]),
        Err(SqlCompileError::InvalidRequest(_))
    ));
    let internal = request(SqlCompileIntent::DmlInternalRead)
        .with_root_output_declaration(vec![ty(DataType::Int8, false)])
        .unwrap();
    SqlCompiler::analyze(internal).unwrap();
    let wrong_width = request(SqlCompileIntent::DmlInternalRead)
        .with_root_output_declaration(vec![])
        .unwrap();
    assert!(matches!(
        SqlCompiler::analyze(wrong_width),
        Err(SqlCompileError::InvalidRequest(_))
    ));
}
