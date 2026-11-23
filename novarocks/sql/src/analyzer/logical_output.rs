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

//! SQL semantic output provenance for physically ambiguous scalar values.

#[cfg(test)]
use arrow::datatypes::{DataType, Field};
#[cfg(test)]
use novarocks_parser::Span;
use novarocks_parser::ast;
#[cfg(test)]
use novarocks_types::logical::LogicalType;
#[cfg(test)]
use novarocks_types::logical::field_with_logical_type;
use novarocks_types::schema::SqlType;

use super::{AnalyzerContext, scope::AnalyzerScope};
#[cfg(test)]
use crate::analysis::ExprKind;
use crate::analysis::TypedExpr;
use crate::analyze_error::AnalyzeError;

#[cfg(test)]
#[path = "logical_container.rs"]
mod container;

enum OutputProjectionError {
    Source(novarocks_types::ExecutionColumnTypeError),
    Control(novarocks_type_contract::CompileControlError),
}
impl From<novarocks_types::ExecutionColumnTypeError> for OutputProjectionError {
    fn from(error: novarocks_types::ExecutionColumnTypeError) -> Self {
        Self::Source(error)
    }
}
impl AnalyzerContext<'_> {
    pub(super) fn logical_output_type(
        &self,
        _source: Option<&ast::Expr>,
        expression: &TypedExpr,
        _scope: &AnalyzerScope,
    ) -> Result<Option<SqlType>, AnalyzeError> {
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Validate)
            .map_err(AnalyzeError::control)?;
        let result = novarocks_types::sql_type_from_execution_value_observed(
            expression.value_type.typed_binding(),
            |units| {
                for _ in 0..units {
                    work.step().map_err(OutputProjectionError::Control)?;
                }
                // The projector calls before each actual name/container copy.
                work.flush().map_err(OutputProjectionError::Control)
            },
        );
        if let Err(OutputProjectionError::Control(error)) = &result {
            return Err(AnalyzeError::control(error.clone()));
        }
        work.finish().map_err(AnalyzeError::control)?;
        match result {
            Ok(projected) => Ok(Some(projected)),
            // SQL declarations have no NULL/Union/unsigned leaf vocabulary.
            // Absence of presentation never changes the complete bound type.
            Err(OutputProjectionError::Source(
                novarocks_types::ExecutionColumnTypeError::UnrepresentableValue(_)
                | novarocks_types::ExecutionColumnTypeError::UnrepresentableCarrier(_),
            )) => Ok(None),
            Err(OutputProjectionError::Source(error)) => {
                Err(AnalyzeError::internal(error.to_string()))
            }
            Err(OutputProjectionError::Control(_)) => {
                unreachable!("original control already returned")
            }
        }
    }

    pub(super) fn json_list_provenance(
        &self,
        _source: Option<&ast::Expr>,
        expression: &TypedExpr,
        _scope: &AnalyzerScope,
    ) -> bool {
        use novarocks_type_contract::{ValueKind, ValueLogicalType};
        matches!(expression.value_type.value_type().kind(), ValueKind::List(item) if item.domain() == Some(ValueLogicalType::Json))
    }
}

#[cfg(test)]
mod scalar_domain_tests {
    use super::*;
    use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
    use crate::column_id::ColumnRefFactory;
    use crate::planner::table::{SqlScanKind, TableDef};
    use novarocks_types::schema::ColumnDef;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::Arc;

    struct Catalog;
    fn declared(name: &str, data_type: DataType, logical_type: SqlType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            data_type,
            nullable: true,
            write_default: None,
            logical_type: Some(logical_type),
        }
    }
    impl PlannerTableProvider for Catalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<ResolvedAnalyzerTable, String> {
            let json_item = field_with_logical_type(
                Field::new("item", DataType::Utf8, true),
                LogicalType::Json,
            );
            let planner = TableDef {
                name: table.into(),
                columns: vec![
                    declared("j", DataType::Utf8, SqlType::Json),
                    declared("h", DataType::Binary, SqlType::Hll),
                    declared("b", DataType::Binary, SqlType::Bitmap),
                    declared("s", DataType::Utf8, SqlType::String),
                    declared("v", DataType::LargeBinary, SqlType::Variant),
                    declared("o", DataType::Binary, SqlType::Object),
                    declared("p", DataType::Binary, SqlType::Percentile),
                    declared(
                        "a",
                        DataType::List(Arc::new(json_item)),
                        SqlType::Array(Box::new(SqlType::Json)),
                    ),
                    declared(
                        "m",
                        DataType::Map(
                            Arc::new(Field::new(
                                "entries",
                                DataType::Struct(
                                    vec![
                                        Field::new("key", DataType::Utf8, false),
                                        Field::new("value", DataType::Binary, true),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            false,
                        ),
                        SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::Hll)),
                    ),
                    declared(
                        "r",
                        DataType::Struct(
                            vec![
                                Field::new("json", DataType::Utf8, true),
                                Field::new("bitmap", DataType::Binary, true),
                            ]
                            .into(),
                        ),
                        SqlType::Struct(vec![
                            ("json".into(), SqlType::Json),
                            ("bitmap".into(), SqlType::Bitmap),
                        ]),
                    ),
                ],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source_for(
                    "ice",
                    database,
                    table,
                    SqlScanKind::ConnectorRead,
                ),
            };
            Ok(ResolvedAnalyzerTable::from_planner(
                catalog, database, planner,
            ))
        }
    }
    fn query(sql: &str) -> ast::Query {
        let query = &mut novarocks_parser::parser::parse_query(sql).unwrap();
        query.clone()
    }
    fn output_domains(sql: &str) -> Vec<Option<SqlType>> {
        let (query, _, factory) = super::super::analyze(&query(sql), &Catalog, "db").unwrap();
        query
            .output_columns
            .iter()
            .map(|column| factory.logical_type(column.column_id))
            .collect()
    }
    fn assert_public_variant_cast_unsupported(sql: &str) {
        let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
        let offset = sql.find(" as variant").unwrap() + " as ".len();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::UnsupportedExpression,
            "{sql}: {error}"
        );
        assert_eq!(error.message(), "SQL type variant is not supported");
        assert_eq!(
            error.span(),
            Some(novarocks_parser::Span::new(
                offset,
                offset + "variant".len()
            ))
        );
    }
    fn fixture_control() -> &'static crate::compiler::SqlCompileControl {
        static CONTROL: std::sync::OnceLock<crate::compiler::SqlCompileControl> =
            std::sync::OnceLock::new();
        CONTROL.get_or_init(crate::compiler::SqlCompileControl::unbounded)
    }
    fn context(factory: Rc<RefCell<ColumnRefFactory>>) -> AnalyzerContext<'static> {
        context_with_control(factory, fixture_control())
    }
    fn context_with_control<'a>(
        factory: Rc<RefCell<ColumnRefFactory>>,
        control: &'a crate::compiler::SqlCompileControl,
    ) -> AnalyzerContext<'a> {
        AnalyzerContext {
            purpose: crate::analyzer::QueryAnalysisPurpose::Ordinary,
            grouping_scope: std::cell::RefCell::new(None),
            materialized_values: None,
            prepared_parameters: None,
            table_sequence: None,
            constant_policy: crate::constant::test_constant_policy(),
            control,
            catalog: &Catalog,
            current_database: "db",
            function_catalog: crate::functions::builtin_sql_function_catalog(),
            sql_semantics: Default::default(),
            factory,
            view_stack: Default::default(),
            ctes: Default::default(),
            pending_ctes: Default::default(),
            next_subquery_id: Cell::new(0),
            collected_subqueries: RefCell::new(Vec::new()),
            cte_registry: RefCell::new(Default::default()),
        }
    }

    #[test]
    fn m07_json_list_explicit_literal_target_keeps_its_source_decision() {
        assert_eq!(
            output_domains("select CAST([] AS ARRAY<json>),CAST([null] AS ARRAY<json>)"),
            vec![Some(SqlType::Array(Box::new(SqlType::Json))); 2],
        );
        assert_eq!(
            output_domains(
                "with q as (select CAST([] AS ARRAY<json>) as j) select concat(j, CAST(ARRAY[] AS ARRAY(JSON))) from q"
            ),
            vec![Some(SqlType::Array(Box::new(SqlType::Json)))],
        );
        assert_eq!(
            output_domains(
                "select CAST([json_object('k':1)] AS ARRAY<varchar>),CAST(['plain'] AS ARRAY<json>)"
            ),
            vec![
                Some(SqlType::Array(Box::new(SqlType::String))),
                Some(SqlType::Array(Box::new(SqlType::Json)))
            ],
        );
        assert_eq!(
            output_domains("select cast(['plain'] as array<json>)"),
            vec![Some(SqlType::Array(Box::new(SqlType::Json)))],
        );
        for (sql, mode) in [
            (
                "select cast(['plain'] as array<json>)",
                novarocks_type_contract::TrinoCastMode::Explicit,
            ),
            (
                "select try_cast(['plain'] as array<json>)",
                novarocks_type_contract::TrinoCastMode::Try,
            ),
        ] {
            let expressions = output_expressions(sql);
            let expression = &expressions[0];
            assert_eq!(
                expression.value_type,
                crate::test_value_types::string_list(true, true, false)
            );
            let ExprKind::Cast {
                expr, cast_mode, ..
            } = &expression.kind
            else {
                panic!("explicit array CAST");
            };
            assert_eq!(*cast_mode, Some(mode));
            assert_eq!(domain_at(expr, &[0]), None);
            let ExprKind::FunctionCall { binding, args, .. } = &expr.kind else {
                panic!("original array literal owner");
            };
            assert_eq!(
                binding.function_id.as_str(),
                "builtin.scalar/__array_literal/v1"
            );
            assert_eq!(args.len(), 1);
            assert_eq!(
                args[0].value_type.logical_type(),
                novarocks_type_contract::ValueLogicalType::Physical
            );
            let novarocks_functions::FunctionResultType::Scalar(result) =
                &binding.selected.result_type
            else {
                panic!("scalar");
            };
            assert_eq!(result, &expr.value_type);
        }
        // Both declarations belong to the ordinary outer CAST. Their source
        // constructors retain the original text and JSON domains separately.
        let sql = "select CAST(['{}'] AS ARRAY<json>),CAST([parse_json('{}')] AS ARRAY<json>)";
        assert_eq!(
            output_domains(sql),
            vec![Some(SqlType::Array(Box::new(SqlType::Json))); 2]
        );
        let expressions = output_expressions(sql);
        for (expression, source_domain) in expressions
            .into_iter()
            .zip([None, Some(novarocks_type_contract::ValueLogicalType::Json)])
        {
            assert_eq!(
                domain_at(&expression, &[0]),
                Some(novarocks_type_contract::ValueLogicalType::Json)
            );
            let ExprKind::Cast {
                expr, cast_mode, ..
            } = &expression.kind
            else {
                panic!("ordinary outer array CAST");
            };
            assert_eq!(
                *cast_mode,
                Some(novarocks_type_contract::TrinoCastMode::Explicit)
            );
            assert_eq!(domain_at(expr, &[0]), source_domain);
            let ExprKind::FunctionCall { binding, .. } = &expr.kind else {
                panic!("original independent constructor");
            };
            let novarocks_functions::FunctionResultType::Scalar(result) =
                &binding.selected.result_type
            else {
                panic!("scalar")
            };
            assert_eq!(result, &expr.value_type);
            let DataType::List(item) = expression.value_type.data_type() else {
                panic!("list")
            };
            assert_eq!(item.data_type(), &DataType::Utf8);
            assert!(item.metadata().as_arc().is_none());
        }
    }

    fn domain_at(
        expression: &TypedExpr,
        path: &[usize],
    ) -> Option<novarocks_type_contract::ValueLogicalType> {
        let mut value = expression.value_type.value_type();
        for &index in path {
            value = value.child_type_at(index).expect("actual semantic child");
        }
        value.domain()
    }

    fn typed_row_fixture(value: TypedExpr) -> (AnalyzerContext<'static>, AnalyzerScope, TypedExpr) {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        let call = context
            .type_scalar_call(
                novarocks_expression::typing::NodeShape::Row,
                vec![value],
                Span::new(0, 0),
            )
            .unwrap();
        let bound = TypedExpr {
            value_type: call.value_type().clone(),
            kind: ExprKind::FunctionCall {
                name: "row".to_owned(),
                args: call.args.into(),
                distinct: false,
                volatility: call.binding.semantics.volatility,
                binding: call.binding,
            },
        };
        (context, scope, bound)
    }
    fn typed_list_source(json: bool, item_nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: crate::column_id::ColumnRefFactory::new().create(
                    None,
                    "source".into(),
                    crate::test_value_types::string_list(item_nullable, json, false),
                ),
                qualifier: None,
                column: "source".into(),
            },
            value_type: crate::test_value_types::string_list(item_nullable, json, false),
        }
    }

    #[test]
    fn typed_wrapper_selected_full_binding_is_the_only_domain_author() {
        for json in [false, true] {
            let source = typed_list_source(json, false);
            let (context, scope, bound) = typed_row_fixture(source.clone());
            let before = bound.clone();
            let adapted = context
                .adapt_bound_output_domains(bound, None, &scope, Span::new(0, 0))
                .unwrap();
            assert_eq!(adapted.value_type, before.value_type);
            let ExprKind::FunctionCall { binding, args, .. } = &adapted.kind else {
                panic!("original call");
            };
            let ExprKind::FunctionCall {
                binding: original, ..
            } = &before.kind
            else {
                panic!("original selection");
            };
            assert_eq!(binding.resolved(), original.resolved());
            assert_eq!(args[0].value_type, source.value_type);
            let child = adapted
                .value_type
                .value_type()
                .child_type_at(0)
                .unwrap()
                .child_type_at(0)
                .unwrap();
            assert_eq!(
                child.domain(),
                json.then_some(novarocks_type_contract::ValueLogicalType::Json)
            );
            assert!(!child.nullable());
            let DataType::Struct(fields) = adapted.value_type.data_type() else {
                panic!("row");
            };
            let DataType::List(item) = fields[0].data_type() else {
                panic!("list");
            };
            assert_eq!(item.name(), "item");
            assert!(item.metadata().as_arc().is_none());
        }
    }

    #[test]
    fn typed_wrapper_rejects_actual_domain_child_null_and_selected_result_drift() {
        let (context, scope, bound) = typed_row_fixture(typed_list_source(true, false));
        for replacement in [
            crate::test_value_types::string_list(false, false, false),
            crate::test_value_types::string_list(true, true, false),
        ] {
            let mut changed = bound.clone();
            let ExprKind::FunctionCall { args, .. } = &mut changed.kind else {
                panic!("row");
            };
            args[0].value_type = replacement;
            assert!(
                context
                    .adapt_bound_output_domains(changed, None, &scope, Span::new(0, 0))
                    .unwrap_err()
                    .to_string()
                    .contains("complete selected binding")
            );
        }
        let mut changed = bound.clone();
        changed.value_type = changed
            .value_type
            .with_nullable(!changed.value_type.nullable())
            .unwrap();
        assert!(
            context
                .adapt_bound_output_domains(changed, None, &scope, Span::new(0, 0))
                .unwrap_err()
                .to_string()
                .contains("wrapper result")
        );
    }

    #[test]
    fn m07_scalar_catalog_domains_survive_projection_cte_and_union() {
        assert_eq!(
            output_domains("select j,h,b,s,v,a,m,r from t"),
            vec![
                Some(SqlType::Json),
                Some(SqlType::Hll),
                Some(SqlType::Bitmap),
                Some(SqlType::String),
                Some(SqlType::Variant),
                Some(SqlType::Array(Box::new(SqlType::Json))),
                Some(SqlType::Map(
                    Box::new(SqlType::String),
                    Box::new(SqlType::Hll)
                )),
                Some(SqlType::Struct(vec![
                    ("json".into(), SqlType::Json),
                    ("bitmap".into(), SqlType::Bitmap),
                ])),
            ]
        );
        assert_eq!(
            output_domains("with q as (select h as x from t) select x from q"),
            vec![Some(SqlType::Hll)]
        );
        assert_eq!(
            output_domains("select h from t union all select h from t"),
            vec![Some(SqlType::Hll)]
        );
        assert_eq!(
            output_domains("select null union all select b from t"),
            vec![Some(SqlType::Bitmap)]
        );
        let error = super::super::analyze(
            &query("select h from t union all select b from t"),
            &Catalog,
            "db",
        )
        .unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::TypeMismatch
        );
    }

    #[test]
    fn m07_scalar_bound_conditionals_keep_only_homogeneous_returned_domains() {
        assert_eq!(
            output_domains(
                "select coalesce(j,null),ifnull(null,h),ifnull(b,null),if(true,h,null),case when true then h else null end from t"
            ),
            vec![
                Some(SqlType::Json),
                Some(SqlType::Hll),
                Some(SqlType::Bitmap),
                Some(SqlType::Hll),
                Some(SqlType::Hll)
            ]
        );
        assert_eq!(
            output_domains(
                "select coalesce(v,v),ifnull(a,null),case when true then b else b end,coalesce(m,m),if(true,r,r) from t"
            ),
            vec![
                Some(SqlType::Variant),
                Some(SqlType::Array(Box::new(SqlType::Json))),
                Some(SqlType::Bitmap),
                Some(SqlType::Map(
                    Box::new(SqlType::String),
                    Box::new(SqlType::Hll)
                )),
                Some(SqlType::Struct(vec![
                    ("json".into(), SqlType::Json),
                    ("bitmap".into(), SqlType::Bitmap),
                ])),
            ]
        );
        assert_eq!(
            output_domains(
                "select coalesce(null,parse_json('null')),if(false,null,json_object('k':1)),case when true then null else parse_json('null') end"
            ),
            vec![Some(SqlType::Json); 3]
        );
        assert_eq!(
            output_domains("select coalesce(null,null),case when true then null else null end"),
            vec![None, None]
        );
    }

    #[test]
    fn m07_scalar_mixed_domains_refuse_coercion_and_keep_explicit_json_identity() {
        use crate::analyze_error::AnalyzeErrorKind;
        let error = super::super::analyze(&query("select if(true,h,b) from t"), &Catalog, "db")
            .unwrap_err();
        assert!(
            error.to_string().contains("no matching declared overload"),
            "{error}"
        );
        for sql in [
            "select case when true then h else b end from t",
            "select coalesce(j,s) from t",
            "select cast(h as varbinary) from t",
        ] {
            let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
            assert_eq!(
                error.kind(),
                AnalyzeErrorKind::TypeMismatch,
                "{sql}: {error}"
            );
        }
        assert_eq!(
            output_domains(
                "select cast(j as varchar),cast(s as json),cast(j as json),coalesce(cast(j as json),null) from t"
            ),
            vec![
                Some(SqlType::String),
                Some(SqlType::Json),
                Some(SqlType::Json),
                Some(SqlType::Json)
            ]
        );

        let value = output_expressions("select cast('plain' as json)").remove(0);
        assert_eq!(
            value.value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Json
        );
        let ExprKind::Cast {
            expr, cast_mode, ..
        } = &value.kind
        else {
            panic!("expected the explicitly authored JSON conversion");
        };
        assert_eq!(
            *cast_mode,
            Some(novarocks_type_contract::TrinoCastMode::Explicit)
        );
        assert_eq!(
            expr.value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Physical
        );

        let parsed = output_expressions("select parse_json('{}')").remove(0);
        assert_eq!(
            parsed.value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Json
        );
        let ExprKind::FunctionCall { binding, .. } = &parsed.kind else {
            panic!("real JSON parser owner");
        };
        assert_eq!(binding.function_id.as_str(), "builtin.scalar/parse_json/v1");
    }

    #[test]
    fn m07_scalar_nullif_comparator_does_not_supply_the_returned_domain() {
        for sql in [
            "select nullif(h,b) from t",
            "select nullif(j,s) from t",
            "select nullif(s,j) from t",
        ] {
            let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
            assert!(
                error.to_string().contains("no matching declared overload"),
                "{sql}: {error}"
            );
        }
        assert_eq!(
            output_domains("select nullif(h,h),nullif(j,j),nullif(null,j) from t"),
            vec![Some(SqlType::Hll), Some(SqlType::Json), Some(SqlType::Json)]
        );
        let expression = output_expressions("select nullif(null,j) from t").remove(0);
        let ExprKind::FunctionCall { binding, args, .. } = &expression.kind else {
            panic!("actual nullif owner");
        };
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("scalar");
        };
        assert_eq!(result, &expression.value_type);
        assert_eq!(
            result.logical_type(),
            novarocks_type_contract::ValueLogicalType::Json
        );
        assert!(args[0].value_type.nullable());
        assert!(
            super::super::value_conversion::constant_is_null(
                &args[0],
                crate::constant::test_constant_policy(),
                fixture_control()
            )
            .unwrap()
        );
        assert_eq!(
            args[1].value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Json
        );
    }

    #[test]
    fn m07_scalar_shadowed_spelling_cannot_use_builtin_value_preservation() {
        let mut expression = output_expressions("select coalesce(j,null) from t").remove(0);
        let ExprKind::FunctionCall { binding, args, .. } = &mut expression.kind else {
            panic!("call");
        };
        let original = binding.resolved().clone();
        let mut changed = original.clone();
        changed.function_id =
            novarocks_functions::FunctionId::try_new("test.shadow/coalesce/v1").unwrap();
        let control = fixture_control();
        let arguments = args
            .iter()
            .map(|arg| {
                crate::analysis::function_argument(
                    arg,
                    crate::constant::test_constant_policy(),
                    control,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let request = novarocks_functions::FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: arguments.len(),
            expected_result_type: None,
        };
        assert!(
            crate::functions::build_builtin_engine_function_catalog()
                .unwrap()
                .validate_bound(&changed, request, control)
                .is_err()
        );
        assert!(
            crate::functions::build_builtin_engine_function_catalog()
                .unwrap()
                .validate_bound(&original, request, control)
                .is_ok()
        );
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let scope = AnalyzerScope::new(factory.clone());
        assert_eq!(
            context(factory)
                .logical_output_type(None, &expression, &scope)
                .unwrap(),
            Some(SqlType::Json)
        );
    }

    #[test]
    fn m07_scalar_implicit_offset_adaptation_preserves_authority_but_family_change_clears_it() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        let json = novarocks_type_contract::FunctionValueType::try_with_logical_type(
            DataType::Utf8,
            true,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap();
        let expression = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: crate::column_id::ColumnId::UNSET,
                qualifier: None,
                column: "j".into(),
            },
            value_type: json,
        };
        assert_eq!(
            context
                .logical_output_type(None, &expression, &scope)
                .unwrap(),
            Some(SqlType::Json)
        );
        // JSON's installed profile remains Utf8-only. Ordinary String offsets
        // have one semantic declaration without widening that capability.
        assert!(
            novarocks_type_contract::FunctionValueType::try_with_logical_type(
                DataType::LargeUtf8,
                true,
                novarocks_type_contract::ValueLogicalType::Json
            )
            .is_err()
        );
        for target in [DataType::Utf8, DataType::LargeUtf8] {
            let expression = TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: crate::column_id::ColumnId::UNSET,
                    qualifier: None,
                    column: "s".into(),
                },
                value_type: novarocks_type_contract::FunctionValueType::new(target, true),
            };
            assert_eq!(
                context
                    .logical_output_type(None, &expression, &scope)
                    .unwrap(),
                Some(SqlType::String)
            );
        }
        assert_eq!(
            output_domains("select cast(j as varchar),j from t"),
            vec![Some(SqlType::String), Some(SqlType::Json)]
        );
        assert_opaque_binary_cast_rejected("select cast(h as varbinary) from t");
    }

    fn assert_opaque_binary_cast_rejected(sql: &str) {
        let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::TypeMismatch,
            "{sql}: {error}"
        );
        assert!(
            error
                .message()
                .contains("source and target have no legal Trino conversion"),
            "{sql}: {error}"
        );
    }

    fn output_expressions(sql: &str) -> Vec<TypedExpr> {
        let (query, _, _) = super::super::analyze(&query(sql), &Catalog, "db").unwrap();
        let crate::analysis::QueryBody::Select(select) = query.body else {
            panic!("expected select")
        };
        select
            .projection
            .into_iter()
            .map(|item| item.expr)
            .collect()
    }

    fn original_window(expression: &TypedExpr) -> &TypedExpr {
        match &expression.kind {
            ExprKind::Cast { expr, .. } => original_window(expr),
            ExprKind::WindowCall { .. } => expression,
            _ => panic!("expected selected window"),
        }
    }

    #[test]
    fn m07_window_value_domains_follow_all_actual_value_suppliers() {
        assert_eq!(
            output_domains(
                "select first_value(j) over (),last_value(h) over (),first_value(b) over (),last_value(v) over (),first_value(o) over (),last_value(p) over (),lead(j,1,j) over (),lag(j,1,null) over () from t"
            ),
            vec![
                Some(SqlType::Json),
                Some(SqlType::Hll),
                Some(SqlType::Bitmap),
                Some(SqlType::Variant),
                Some(SqlType::Object),
                Some(SqlType::Percentile),
                Some(SqlType::Json),
                Some(SqlType::Json)
            ]
        );
        assert_eq!(
            output_domains(
                "select lead(j,1,s) over (),lag(s,1,j) over (),lead(h,1,b) over (),lag(b,1,h) over (),lead(j,1,cast(null as varchar)) over (),first_value(cast(j as varchar)) over (),lag(cast(j as varchar),1,j) over () from t"
            ),
            vec![
                Some(SqlType::Json),
                Some(SqlType::String),
                Some(SqlType::Hll),
                Some(SqlType::Bitmap),
                Some(SqlType::Json),
                Some(SqlType::String),
                Some(SqlType::String)
            ]
        );
        // The offset is a control operand, not a supplier of the result value.
        assert_eq!(
            output_domains("select lead(j,2) over (),lag(h,2) over () from t"),
            vec![Some(SqlType::Json), Some(SqlType::Hll)]
        );
    }

    #[test]
    fn m07_window_nested_value_identity_preserves_shape_and_null_siblings() {
        let sql = "with q as (select [b] as b from t) select first_value(b) over (),last_value(b) over (),lead(b,1,b) over (),lag(b,1,null) over () from q";
        assert_eq!(
            output_domains(sql),
            vec![Some(SqlType::Array(Box::new(SqlType::Bitmap))); 4]
        );
        for expression in output_expressions(sql) {
            let selected = original_window(&expression);
            let ExprKind::WindowCall {
                binding,
                args,
                aggregate_binding,
                ..
            } = &selected.kind
            else {
                unreachable!()
            };
            assert!(aggregate_binding.is_none());
            assert_eq!(binding.kind, novarocks_functions::FunctionKind::Window);
            let novarocks_functions::FunctionResultType::Scalar(result) =
                &binding.selected.result_type
            else {
                panic!("scalar selection")
            };
            assert_eq!(result.data_type(), selected.value_type.data_type());
            let DataType::List(output) = &expression.value_type.data_type() else {
                panic!("list output")
            };
            let DataType::List(input) = &args[0].value_type.data_type() else {
                panic!("list input")
            };
            assert_eq!(output.name(), input.name());
            assert_eq!(output.is_nullable(), input.is_nullable());
            assert_eq!(output.data_type(), input.data_type());
            assert!(output.metadata().as_arc().is_none());
            assert_eq!(
                domain_at(&expression, &[0]),
                Some(novarocks_type_contract::ValueLogicalType::Bitmap)
            );
        }
        let expression =
            output_expressions("select first_value(row(null,b)) over () from t").remove(0);
        let DataType::Struct(fields) = &expression.value_type.data_type() else {
            panic!("struct output")
        };
        assert_eq!(fields[0].data_type(), &DataType::Null);
        assert_eq!(fields[0].name(), "col1");
        assert_eq!(fields[1].name(), "col2");
        assert!(fields[1].metadata().as_arc().is_none());
        assert_eq!(
            domain_at(&expression, &[1]),
            Some(novarocks_type_contract::ValueLogicalType::Bitmap)
        );
        assert_eq!(
            output_domains("select first_value(row(null,b)) over () from t"),
            vec![None]
        );
        let sql = "with q as (select [parse_json('{}')] as j,['plain'] as s) select lead(j,1,s) over () from q";
        let expression = output_expressions(sql).remove(0);
        let ExprKind::WindowCall { binding, args, .. } = &original_window(&expression).kind else {
            panic!("actual window");
        };
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("scalar");
        };
        assert_eq!(&expression.value_type, result);
        assert_eq!(
            domain_at(&expression, &[0]),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        for (actual, formal) in args.iter().zip(binding.selected.argument_types.iter()) {
            let novarocks_functions::FunctionArgumentType::Value(formal) = formal else {
                panic!("actual value argument");
            };
            assert!(
                actual
                    .value_type
                    .fits_physical_binding_observed(formal, || Ok::<(), std::convert::Infallible>(
                        ()
                    ))
                    .unwrap()
            );
        }
        assert_eq!(
            output_domains(sql),
            vec![Some(SqlType::Array(Box::new(SqlType::Json)))]
        );
    }

    #[test]
    fn m07_window_json_defaults_keep_identity_and_variant_casts_are_rejected() {
        assert_eq!(
            output_domains(
                "select lead(j,1,cast(null as json)) over (),lag(j,1,cast(null as json)) over () from t"
            ),
            vec![Some(SqlType::Json); 2]
        );
        assert_eq!(
            output_domains(
                "select lead(j,1,cast(j as json)) over (),lag(j,1,cast(j as json)) over (),first_value(cast(j as json)) over () from t"
            ),
            vec![Some(SqlType::Json); 3]
        );
        for sql in [
            "select lead(v,1,cast(null as variant)) over () from t",
            "select lag(v,1,cast(v as variant)) over () from t",
            "select first_value(cast(X'AB01' as variant)) over ()",
            "select first_value(cast(cast(X'AB01' as varbinary) as variant)) over ()",
        ] {
            assert_public_variant_cast_unsupported(sql);
        }
        // A NULL default cannot erase a known sibling in a partial Struct or
        // a nested Map value. SqlType has no complete Null-bearing counterpart.
        let sql = "with q as (select row(null,map(ARRAY['k'],ARRAY[b])) as r from t) select lead(r,1,null) over (),lag(r,1,null) over () from q";
        for expression in output_expressions(sql) {
            let ExprKind::WindowCall { args, .. } = &original_window(&expression).kind else {
                unreachable!()
            };
            let DataType::Struct(fields) = &expression.value_type.data_type() else {
                panic!("struct result")
            };
            assert_eq!(fields[0].data_type(), &DataType::Null);
            let DataType::Map(entries, sorted) = fields[1].data_type() else {
                panic!("map sibling")
            };
            let DataType::Struct(input) = &args[0].value_type.data_type() else {
                panic!("struct input")
            };
            let DataType::Map(input_entries, input_sorted) = input[1].data_type() else {
                panic!("map input")
            };
            assert_eq!(sorted, input_sorted);
            assert_eq!(entries.name(), input_entries.name());
            assert_eq!(entries.is_nullable(), input_entries.is_nullable());
            let DataType::Struct(map_fields) = entries.data_type() else {
                panic!("map entries")
            };
            let DataType::Struct(input_fields) = input_entries.data_type() else {
                panic!("input entries")
            };
            assert_eq!(map_fields.len(), 2);
            for (actual, input) in map_fields.iter().zip(input_fields) {
                assert_eq!(actual.name(), input.name());
                assert_eq!(actual.is_nullable(), input.is_nullable());
                assert_eq!(actual.data_type(), input.data_type());
            }
            assert!(map_fields[1].metadata().as_arc().is_none());
            assert_eq!(
                domain_at(&expression, &[1, 1]),
                Some(novarocks_type_contract::ValueLogicalType::Bitmap)
            );
        }
        assert_eq!(output_domains(sql), vec![None; 2]);
    }

    #[test]
    fn m07_public_variant_cast_is_refused_even_with_a_known_input_domain() {
        for sql in [
            "select cast(v as variant) from t",
            "select cast((v) as variant) from t",
            "select cast(s as variant) from t",
            "select cast(j as variant) from t",
            "select cast(X'AB01' as variant)",
            "select cast(cast(X'AB01' as varbinary) as variant)",
        ] {
            assert_public_variant_cast_unsupported(sql);
        }
        let sql = "select cast(cast(v as varbinary) as variant) from t";
        let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::TypeMismatch
        );
        let inner = "cast(v as varbinary)";
        let offset = sql.find(inner).unwrap();
        assert_eq!(
            error.span(),
            Some(novarocks_parser::Span::new(offset, offset + inner.len()))
        );
        assert_eq!(
            output_domains("select v,cast(j as json),cast(s as json) from t"),
            vec![
                Some(SqlType::Variant),
                Some(SqlType::Json),
                Some(SqlType::Json)
            ]
        );
    }

    #[test]
    fn m07_window_value_domains_require_exact_selected_binding_and_carrier() {
        let original = output_expressions("select first_value(parse_json('{}')) over ()").remove(0);
        let (binding, args) = match &original.kind {
            ExprKind::FunctionCall { binding, args, .. }
            | ExprKind::WindowCall { binding, args, .. } => (binding, args),
            _ => panic!("complete call"),
        };
        let selection = binding.resolved().clone();
        let control = fixture_control();
        let arguments = args
            .iter()
            .map(|arg| {
                crate::analysis::function_argument(
                    arg,
                    crate::constant::test_constant_policy(),
                    control,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let request = novarocks_functions::FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: arguments.len(),
            expected_result_type: None,
        };
        assert!(
            crate::functions::build_builtin_engine_function_catalog()
                .unwrap()
                .validate_bound(&selection, request, control)
                .is_ok()
        );
        for change in 0..5 {
            let mut changed = selection.clone();
            match change {
                0 => {
                    changed.function_id =
                        novarocks_functions::FunctionId::try_new("test.shadow/complete/v1").unwrap()
                }
                1 => changed.kind = novarocks_functions::FunctionKind::Aggregate,
                2 => {
                    changed.selected.result_type = novarocks_functions::FunctionResultType::Scalar(
                        novarocks_type_contract::FunctionValueType::new(DataType::Int32, true),
                    )
                }
                3 => changed.logical_argument_count = 0,
                _ => {
                    let novarocks_functions::FunctionResultType::Scalar(result) =
                        &mut changed.selected.result_type
                    else {
                        panic!("scalar");
                    };
                    *result = result.with_nullable(!result.nullable()).unwrap();
                }
            }
            assert!(
                crate::functions::build_builtin_engine_function_catalog()
                    .unwrap()
                    .validate_bound(&changed, request, control)
                    .is_err(),
                "exact selected alteration {change}"
            );
        }
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let scope = AnalyzerScope::new(factory.clone());
        let adapted = context(factory)
            .adapt_bound_output_domains(original.clone(), None, &scope, Span::new(0, 0))
            .unwrap();
        assert_eq!(adapted.value_type, original.value_type);
        assert_eq!(
            format!("{:?}", adapted.kind),
            format!("{:?}", original.kind)
        );
    }

    #[test]
    fn typed_wrapper_keeps_original_formal_null_widening_and_refuses_inverse() {
        let mut nullable = typed_list_source(true, false);
        nullable.value_type = nullable.value_type.with_nullable(true).unwrap();
        let (context, scope, mut bound) = typed_row_fixture(nullable);
        let before = bound.value_type.clone();
        let ExprKind::FunctionCall { args, .. } = &mut bound.kind else {
            panic!("row");
        };
        args[0].value_type = args[0].value_type.with_nullable(false).unwrap();
        assert_eq!(
            context
                .adapt_bound_output_domains(bound, None, &scope, Span::new(0, 0))
                .unwrap()
                .value_type,
            before
        );
        let (context, scope, mut bound) = typed_row_fixture(typed_list_source(true, false));
        let ExprKind::FunctionCall { args, .. } = &mut bound.kind else {
            panic!("row");
        };
        args[0].value_type = args[0].value_type.with_nullable(true).unwrap();
        assert!(
            context
                .adapt_bound_output_domains(bound, None, &scope, Span::new(0, 0))
                .unwrap_err()
                .to_string()
                .contains("complete selected binding")
        );
    }

    #[test]
    fn typed_wrapper_keeps_actual_sql_cancellation_and_deadline_without_tail() {
        use crate::compiler::{SqlCancellationObservation, SqlCompileControl};
        use novarocks_type_contract::CompileControlError;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Observation {
            polls: AtomicUsize,
            cancel_at: Option<usize>,
        }
        impl SqlCancellationObservation for Observation {
            fn is_cancelled(&self) -> bool {
                let poll = self.polls.fetch_add(1, Ordering::SeqCst);
                self.cancel_at.is_some_and(|at| poll >= at)
            }
        }
        let (_, scope, bound) = typed_row_fixture(typed_list_source(true, false));
        let observation = Arc::new(Observation {
            polls: AtomicUsize::new(0),
            cancel_at: None,
        });
        let control = SqlCompileControl::new(None, observation.clone());
        let context =
            context_with_control(Rc::new(RefCell::new(ColumnRefFactory::new())), &control);
        context
            .adapt_bound_output_domains(bound.clone(), None, &scope, Span::new(0, 0))
            .unwrap();
        let baseline = observation.polls.load(Ordering::SeqCst);
        assert!(baseline >= 2);
        for at in 0..baseline {
            let observation = Arc::new(Observation {
                polls: AtomicUsize::new(0),
                cancel_at: Some(at),
            });
            let control = SqlCompileControl::new(None, observation.clone());
            let context =
                context_with_control(Rc::new(RefCell::new(ColumnRefFactory::new())), &control);
            let error = context
                .adapt_bound_output_domains(bound.clone(), None, &scope, Span::new(0, 0))
                .unwrap_err();
            assert_eq!(error.control_error(), Some(CompileControlError::Cancelled));
            assert_eq!(observation.polls.load(Ordering::SeqCst), at + 1);
        }
        let observation = Arc::new(Observation {
            polls: AtomicUsize::new(0),
            cancel_at: None,
        });
        let control = SqlCompileControl::new(
            Some(std::time::Instant::now() - std::time::Duration::from_millis(1)),
            observation.clone(),
        );
        let context =
            context_with_control(Rc::new(RefCell::new(ColumnRefFactory::new())), &control);
        let error = context
            .adapt_bound_output_domains(bound, None, &scope, Span::new(0, 0))
            .unwrap_err();
        assert_eq!(
            error.control_error(),
            Some(CompileControlError::DeadlineExceeded)
        );
        assert_eq!(observation.polls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn typed_display_projection_keeps_actual_sql_cancellation_and_deadline_without_tail() {
        use crate::compiler::{SqlCancellationObservation, SqlCompileControl};
        use novarocks_type_contract::CompileControlError;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Observation {
            polls: AtomicUsize,
            cancel_at: Option<usize>,
        }
        impl SqlCancellationObservation for Observation {
            fn is_cancelled(&self) -> bool {
                let poll = self.polls.fetch_add(1, Ordering::SeqCst);
                self.cancel_at.is_some_and(|at| poll >= at)
            }
        }
        let (_, scope, bound) = typed_row_fixture(typed_list_source(true, false));
        let observation = Arc::new(Observation {
            polls: AtomicUsize::new(0),
            cancel_at: None,
        });
        let control = SqlCompileControl::new(None, observation.clone());
        let context =
            context_with_control(Rc::new(RefCell::new(ColumnRefFactory::new())), &control);
        context.logical_output_type(None, &bound, &scope).unwrap();
        let baseline = observation.polls.load(Ordering::SeqCst);
        assert!(baseline >= 2);
        for at in 0..baseline {
            let observation = Arc::new(Observation {
                polls: AtomicUsize::new(0),
                cancel_at: Some(at),
            });
            let control = SqlCompileControl::new(None, observation.clone());
            let context =
                context_with_control(Rc::new(RefCell::new(ColumnRefFactory::new())), &control);
            let error = context
                .logical_output_type(None, &bound, &scope)
                .unwrap_err();
            assert_eq!(error.control_error(), Some(CompileControlError::Cancelled));
            assert_eq!(observation.polls.load(Ordering::SeqCst), at + 1);
        }
        let observation = Arc::new(Observation {
            polls: AtomicUsize::new(0),
            cancel_at: None,
        });
        let control = SqlCompileControl::new(
            Some(std::time::Instant::now() - std::time::Duration::from_millis(1)),
            observation.clone(),
        );
        let context =
            context_with_control(Rc::new(RefCell::new(ColumnRefFactory::new())), &control);
        let error = context
            .logical_output_type(None, &bound, &scope)
            .unwrap_err();
        assert_eq!(
            error.control_error(),
            Some(CompileControlError::DeadlineExceeded)
        );
        assert_eq!(observation.polls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn typed_wrapper_transparent_cast_cannot_wash_original_nominal_child() {
        let original = typed_list_source(true, false);
        let target = crate::test_value_types::string_list(false, false, false);
        let coerced = TypedExpr {
            kind: ExprKind::Cast {
                cast_mode: None,
                expr: novarocks_parser::ast::AstBox::new(original),
                target: target.data_type().clone(),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: target,
        };
        let (context, scope, bound) = typed_row_fixture(coerced);
        let error = context
            .adapt_bound_output_domains(bound, None, &scope, Span::new(0, 0))
            .unwrap_err();
        assert!(
            error.to_string().contains("original nominal domain"),
            "{error}"
        );
    }

    #[test]
    fn typed_wrapper_keeps_genuine_explicit_json_to_text_conversion() {
        let expression =
            output_expressions("select row(cast(parse_json('{}') as varchar))").remove(0);
        let ExprKind::FunctionCall { binding, args, .. } = &expression.kind else {
            panic!("row");
        };
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("scalar");
        };
        assert_eq!(result, &expression.value_type);
        assert_eq!(
            args[0].value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Physical
        );
        assert_eq!(
            expression
                .value_type
                .value_type()
                .child_type_at(0)
                .unwrap()
                .domain(),
            None
        );
        let ExprKind::Cast {
            expr: original,
            cast_mode: Some(novarocks_type_contract::TrinoCastMode::Explicit),
            target: DataType::Utf8,
            ..
        } = &args[0].kind
        else {
            panic!("expected explicit JSON to VARCHAR CAST");
        };
        assert_eq!(
            original.value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Json
        );
        assert!(matches!(
            &original.kind,
            ExprKind::FunctionCall { binding, .. }
                if binding.function_id.as_str() == "builtin.scalar/parse_json/v1"
        ));
    }

    #[test]
    fn m07_window_partial_null_value_keeps_known_origin_through_internal_cast() {
        let original =
            output_expressions("select first_value(row(null,b)) over () from t").remove(0);
        let ExprKind::WindowCall { args, binding, .. } = &original_window(&original).kind else {
            panic!("window");
        };
        let before = binding.resolved().clone();
        let source = args[0].value_type.clone();
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("scalar");
        };
        assert_eq!(result, &original.value_type);
        let value = TypedExpr {
            kind: ExprKind::Cast {
                cast_mode: None,
                expr: novarocks_parser::ast::AstBox::new(original.clone()),
                target: original.value_type.data_type().clone(),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: original.value_type.clone(),
        };
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        let adapted = context
            .adapt_bound_output_domains(value, None, &scope, Span::new(0, 0))
            .unwrap();
        assert_eq!(
            context.logical_output_type(None, &adapted, &scope).unwrap(),
            None
        );
        let DataType::Struct(fields) = adapted.value_type.data_type() else {
            panic!("struct");
        };
        assert_eq!(fields[0].data_type(), &DataType::Null);
        assert_eq!(fields[1].name(), "col2");
        assert!(fields[1].metadata().as_arc().is_none());
        assert_eq!(
            domain_at(&adapted, &[1]),
            Some(novarocks_type_contract::ValueLogicalType::Bitmap)
        );
        let ExprKind::WindowCall { args, binding, .. } = &original_window(&adapted).kind else {
            panic!("window");
        };
        assert_eq!(binding.resolved(), &before);
        assert_eq!(args[0].value_type, source);
    }

    fn compile_scalar_proof(sql: &str, uses_catalog: bool) -> crate::compiler::SqlCompletedPlan {
        use crate::compiler::{
            DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl,
            SqlCompileIntent, SqlCompileProgress, SqlCompiler, SqlFinalPlanCompileRequest,
            SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
            builtin_sql_function_catalog,
        };
        use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId};

        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([17; 16]).unwrap(),
            SqlStatementInput::parsed_query(Box::new(query(sql))),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: Some("iceberg".to_owned()),
                current_database: "db".to_owned(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            crate::constant::test_constant_policy(),
            SqlCompileControl::unbounded(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            DEFAULT_COMPLETION_LIMITS,
        );
        if uses_catalog {
            return crate::compiler::complete_with_catalog_for_test(
                request,
                |need| {
                    let relation = need.relation();
                    let mut resolved = Catalog
                        .resolve_table_for_analysis(
                            Some(&relation.catalog),
                            &relation.namespace,
                            &relation.table,
                        )
                        .expect("signed fixture declaration");
                    // Retain the exact Catalog column types and supply a current
                    // data scan identity that can reach provider completion.
                    resolved.planner.source =
                        crate::compiler::mv_rewrite::test_data_scan_source_for(
                            &relation.catalog,
                            &relation.namespace,
                            &relation.table,
                        );
                    resolved
                },
                sql,
            );
        }
        let SqlCompileProgress::Complete(completed) =
            SqlCompiler::start(request.try_into_completion().unwrap(), fixture_control())
                .unwrap_or_else(|error| panic!("{sql}: {error}"))
        else {
            panic!("source-free scalar proof unexpectedly needs observations: {sql}");
        };
        completed
    }

    #[test]
    fn m07_window_final_compiler_scalar_proof_matches_the_frozen_output() {
        // Variant comes from the exact fixture catalog declaration. JSON
        // and unbounded VARCHAR retain their source-free ScalarValueV1 paths.
        use novarocks_physical_plan::ResultValueDomain as D;
        use novarocks_result_contract::ScalarValueType as V;
        for (sql, uses_catalog, domain, scalar) in [
            (
                "select first_value(parse_json('{}')) over ()",
                false,
                D::Json,
                V::Json,
            ),
            (
                "select lead(parse_json('{}'),1,null) over ()",
                false,
                D::Json,
                V::Json,
            ),
            (
                "select lag(parse_json('{}'),1,null) over ()",
                false,
                D::Json,
                V::Json,
            ),
            (
                "select lead(cast('plain' as varchar),1,cast(7 as bigint)) over ()",
                false,
                D::Plain,
                V::String,
            ),
            (
                "select lag(cast(7 as bigint),1,'2') over ()",
                false,
                D::Plain,
                V::SignedInteger(64),
            ),
            (
                "select first_value(v) over () from t",
                true,
                D::Variant,
                V::Variant,
            ),
            (
                "with q as (select v from t) select lead(v,1) over () from q",
                true,
                D::Variant,
                V::Variant,
            ),
            (
                "with q as (select v from t) select lag(v,1) over () from q",
                true,
                D::Variant,
                V::Variant,
            ),
        ] {
            let completed = compile_scalar_proof(sql, uses_catalog);
            let result = completed.plan().result_port().unwrap();
            assert_eq!(result.fields[0].domain, domain, "{sql}");
            let expected_domain = match domain {
                D::Json => novarocks_type_contract::ValueLogicalType::Json,
                D::Bitmap => novarocks_type_contract::ValueLogicalType::Bitmap,
                D::Hll => novarocks_type_contract::ValueLogicalType::Hll,
                D::Variant => novarocks_type_contract::ValueLogicalType::Variant,
                D::Plain => novarocks_type_contract::ValueLogicalType::Physical,
                _ => panic!("unexpected regression domain"),
            };
            assert_eq!(
                result.fields[0].ty.logical_type(),
                expected_domain,
                "complete frozen Window output: {sql}"
            );
            let schema = completed
                .scalar_schema()
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_eq!(schema.field().value_type, scalar, "{sql}");
            assert_eq!(
                schema.field().nullable,
                result.fields[0].ty.nullable(),
                "{sql}"
            );
            assert_eq!(schema.source_slot(), None);
            novarocks_physical_plan::validate_plan(completed.plan()).unwrap();
        }
    }

    #[test]
    fn m07_projection_occurrences_retain_exact_binary_producer_witnesses() {
        for sql in [
            "select cast(b as varbinary) from t",
            "with q as (select p from t) select cast(p as varbinary) from q",
        ] {
            assert_opaque_binary_cast_rejected(sql);
        }
        for (sql, expected) in [
            (
                "select p,b,h from t",
                vec![
                    Some(SqlType::Percentile),
                    Some(SqlType::Bitmap),
                    Some(SqlType::Hll),
                ],
            ),
            (
                "select p as p,b as b,h as h from t",
                vec![
                    Some(SqlType::Percentile),
                    Some(SqlType::Bitmap),
                    Some(SqlType::Hll),
                ],
            ),
            (
                "with q as (select p as p from t) select p,p as again from q",
                vec![Some(SqlType::Percentile); 2],
            ),
            (
                "select b as b,b as again,X'AB01' as raw from t",
                vec![
                    Some(SqlType::Bitmap),
                    Some(SqlType::Bitmap),
                    Some(SqlType::Binary),
                ],
            ),
            (
                "with q as (select p as p,X'AB01' as raw from t) select raw,p from q",
                vec![Some(SqlType::Binary), Some(SqlType::Percentile)],
            ),
        ] {
            let (query, _, factory) = super::super::analyze(&query(sql), &Catalog, "db").unwrap();
            assert_eq!(
                query
                    .output_columns
                    .iter()
                    .map(|column| factory.borrowed_logical_type(column.column_id).cloned())
                    .collect::<Vec<_>>(),
                expected,
                "{sql}",
            );
            for (column, expected) in query.output_columns.iter().zip(&expected) {
                let projected = novarocks_types::sql_type_from_execution_value(
                    column.value_type.typed_binding(),
                )
                .unwrap();
                assert_eq!(
                    Some(projected),
                    *expected,
                    "complete authored result: {sql}"
                );
            }
        }
    }

    #[test]
    fn m07_scalar_exact_aggregate_and_window_producers_forward_domains() {
        assert_eq!(
            output_domains("select any_value(b),any_value(h),any_value(p),any_value(b) from t"),
            vec![
                Some(SqlType::Bitmap),
                Some(SqlType::Hll),
                Some(SqlType::Percentile),
                Some(SqlType::Bitmap)
            ]
        );
        assert_eq!(
            output_domains("select first_value(b) over () from t"),
            vec![Some(SqlType::Bitmap)]
        );
        assert_eq!(
            output_domains("select array_agg(b),map_agg(cast(1 as bigint),h) from t"),
            vec![
                Some(SqlType::Array(Box::new(SqlType::Bitmap))),
                Some(SqlType::Map(
                    Box::new(SqlType::BigInt),
                    Box::new(SqlType::Hll)
                ))
            ]
        );
    }

    #[test]
    fn m07_scalar_nested_wrappers_and_selectors_keep_exact_value_origins() {
        assert_eq!(
            output_domains(
                "select element_at([b,null],1),element_at(map(ARRAY[1], ARRAY[h]),1),element_at(map_values(map(ARRAY[1], ARRAY[p])),1),r.bitmap from t"
            ),
            vec![
                Some(SqlType::Bitmap),
                Some(SqlType::Hll),
                Some(SqlType::Percentile),
                Some(SqlType::Bitmap)
            ]
        );
        assert_eq!(
            output_domains(
                "select element_at(slice(ARRAY[h],1,1),1),element_at(ARRAY[b],1),element_at(map(ARRAY[cast(1 as bigint)],ARRAY[p]),1) from t"
            ),
            vec![
                Some(SqlType::Hll),
                Some(SqlType::Bitmap),
                Some(SqlType::Percentile)
            ]
        );
        assert_eq!(
            output_domains("select element_at([o],1),element_at([p],1),element_at([v],1) from t"),
            vec![
                Some(SqlType::Object),
                Some(SqlType::Percentile),
                Some(SqlType::Variant)
            ]
        );
    }

    #[test]
    fn m07_scalar_null_companion_retains_actual_null_and_independent_marker() {
        let expressions = output_expressions("select ROW(NULL,b),ROW(NULL,p) from t");
        for (expression, marker, names) in [
            (
                &expressions[0],
                novarocks_type_contract::ValueLogicalType::Bitmap,
                ["col1", "col2"],
            ),
            (
                &expressions[1],
                novarocks_type_contract::ValueLogicalType::Percentile,
                ["col1", "col2"],
            ),
        ] {
            let DataType::Struct(fields) = &expression.value_type.data_type() else {
                panic!("expected struct")
            };
            assert_eq!(fields[0].data_type(), &DataType::Null);
            assert_eq!(fields[0].name(), names[0]);
            assert_eq!(fields[1].name(), names[1]);
            assert!(fields[1].metadata().as_arc().is_none());
            assert_eq!(domain_at(expression, &[1]), Some(marker));
        }
        // SqlType has no Null member. The exact actual fields carry this proof,
        // rather than a fabricated all-members SqlType or a plain opaque value.
        assert_eq!(output_domains("select row(null,b) from t"), vec![None]);
        assert_eq!(
            output_domains("select r.bitmap from t"),
            vec![Some(SqlType::Bitmap)]
        );
    }

    #[test]
    fn typed_scalar_wrapper_preserves_selected_full_result_without_output_adapter() {
        let expression = output_expressions("select [b,null] from t").remove(0);
        let ExprKind::FunctionCall { binding, args, .. } = &expression.kind else {
            panic!("selected original function");
        };
        assert_eq!(
            binding.function_id.as_str(),
            "builtin.scalar/__array_literal/v1"
        );
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("scalar result");
        };
        assert_eq!(result, &expression.value_type);
        let item = expression.value_type.value_type().child_type_at(0).unwrap();
        assert_eq!(
            item.domain(),
            Some(novarocks_type_contract::ValueLogicalType::Bitmap)
        );
        assert!(item.nullable());
        assert_eq!(
            args[0].value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Bitmap
        );
        assert!(args[1].value_type.nullable());
        let DataType::List(field) = expression.value_type.data_type() else {
            panic!("list");
        };
        assert!(field.metadata().as_arc().is_none());
    }

    #[test]
    fn m07_scalar_wrapper_mixed_domains_clear_and_missing_source_markers_refuse() {
        let error =
            super::super::analyze(&query("select element_at([b,h],1) from t"), &Catalog, "db")
                .unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::TypeMismatch
        );
        let mixed_error = super::super::analyze(
            &query("select element_at([parse_json('{}'),'plain'],1)"),
            &Catalog,
            "db",
        )
        .unwrap_err();
        assert_eq!(
            mixed_error.kind(),
            crate::analyze_error::AnalyzeErrorKind::TypeMismatch
        );
        // The catalog's explicit recursive declaration is the author even when
        // its external Arrow fields carry no execution-domain annotations.
        assert_eq!(
            output_domains("select map_values(m),row(r) from t"),
            vec![
                Some(SqlType::Array(Box::new(SqlType::Hll))),
                Some(SqlType::Struct(vec![(
                    "col1".into(),
                    SqlType::Struct(vec![
                        ("json".into(), SqlType::Json),
                        ("bitmap".into(), SqlType::Bitmap)
                    ])
                )]))
            ]
        );
        let error = super::super::analyze(
            &query("select cast(element_at([b],1) as varbinary) from t"),
            &Catalog,
            "db",
        )
        .unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::TypeMismatch
        );
    }

    #[test]
    fn m07_scalar_shadowed_wrapper_binding_cannot_project_child_domain() {
        let mut expression = output_expressions("select [b] from t").remove(0);
        let ExprKind::FunctionCall { binding, args, .. } = &mut expression.kind else {
            panic!("real complete call");
        };
        let original = binding.resolved().clone();
        let mut changed = original.clone();
        changed.function_id =
            novarocks_functions::FunctionId::try_new("test.shadow/__array_literal/v1").unwrap();
        let control = fixture_control();
        let arguments = args
            .iter()
            .map(|arg| {
                crate::analysis::function_argument(
                    arg,
                    crate::constant::test_constant_policy(),
                    control,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let request = novarocks_functions::FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: arguments.len(),
            expected_result_type: None,
        };
        assert!(
            crate::functions::build_builtin_engine_function_catalog()
                .unwrap()
                .validate_bound(&changed, request, control)
                .is_err()
        );
        assert!(
            crate::functions::build_builtin_engine_function_catalog()
                .unwrap()
                .validate_bound(&original, request, control)
                .is_ok()
        );
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let scope = AnalyzerScope::new(factory.clone());
        assert_eq!(
            context(factory)
                .logical_output_type(None, &expression, &scope)
                .unwrap(),
            Some(SqlType::Array(Box::new(SqlType::Bitmap)))
        );
        assert_eq!(
            domain_at(&expression, &[0]),
            Some(novarocks_type_contract::ValueLogicalType::Bitmap)
        );
    }

    fn original_transform(expression: &TypedExpr) -> &TypedExpr {
        let mut original = expression;
        while let ExprKind::Cast { expr, .. } = &original.kind {
            original = expr;
        }
        original
    }

    #[test]
    fn m07_container_transforms_forward_exact_leaf_domains() {
        assert_eq!(
            output_domains(
                "select element_at(flatten([[b]]),1),element_at(repeat(parse_json('{}'),2),1),element_at(__array_struct_subfield(ARRAY[r],'BITMAP'),1) from t"
            ),
            vec![
                Some(SqlType::Bitmap),
                Some(SqlType::Json),
                Some(SqlType::Bitmap)
            ]
        );
        let expressions = output_expressions(
            "select flatten([[parse_json('{}')]]),repeat(b,2),__array_struct_subfield(ARRAY[r],'JSON') from t",
        );
        for (expression, id, marker) in [
            (
                &expressions[0],
                "builtin.scalar/flatten/v1",
                novarocks_type_contract::ValueLogicalType::Json,
            ),
            (
                &expressions[1],
                "builtin.scalar/repeat/v1",
                novarocks_type_contract::ValueLogicalType::Bitmap,
            ),
            (
                &expressions[2],
                "builtin.scalar/__array_struct_subfield/v1",
                novarocks_type_contract::ValueLogicalType::Json,
            ),
        ] {
            let ExprKind::FunctionCall { binding, .. } = &original_transform(expression).kind
            else {
                panic!("selected call");
            };
            assert_eq!(binding.function_id.as_str(), id);
            let novarocks_functions::FunctionResultType::Scalar(selected) =
                &binding.selected.result_type
            else {
                panic!("scalar selection");
            };
            assert_eq!(
                &selected.data_type(),
                &original_transform(expression).value_type.data_type()
            );
            let (DataType::List(actual), DataType::List(selected)) =
                (&expression.value_type.data_type(), &selected.data_type())
            else {
                panic!("List result");
            };
            assert_eq!(actual.name(), selected.name());
            assert_eq!(actual.is_nullable(), selected.is_nullable());
            assert!(actual.metadata().as_arc().is_none());
            assert_eq!(domain_at(expression, &[0]), Some(marker));
        }
    }

    #[test]
    fn m07_container_zip_preserves_selected_names_nullability_and_partial_null() {
        assert_eq!(
            output_domains("select zip([b],[parse_json('{}')]) from t"),
            vec![Some(SqlType::Array(Box::new(SqlType::Struct(vec![
                ("col1".to_owned(), SqlType::Bitmap),
                ("col2".to_owned(), SqlType::Json),
            ]))))]
        );
        let sql = "select zip([row(null,b)],null) from t";
        assert_eq!(output_domains(sql), vec![None]);
        let expression = output_expressions(sql).remove(0);
        let ExprKind::FunctionCall { binding, .. } = &original_transform(&expression).kind else {
            panic!("zip call");
        };
        let novarocks_functions::FunctionResultType::Scalar(selected) =
            &binding.selected.result_type
        else {
            panic!("scalar");
        };
        let (DataType::List(item), DataType::List(selected_item)) =
            (&expression.value_type.data_type(), &selected.data_type())
        else {
            panic!("List");
        };
        assert_eq!(item.name(), selected_item.name());
        assert_eq!(item.is_nullable(), selected_item.is_nullable());
        let (DataType::Struct(fields), DataType::Struct(selected_fields)) =
            (item.data_type(), selected_item.data_type())
        else {
            panic!("Struct");
        };
        assert_eq!(fields.len(), 2);
        for (i, (field, selected)) in fields.iter().zip(selected_fields).enumerate() {
            assert_eq!(field.name(), &format!("col{}", i + 1));
            assert_eq!(field.name(), selected.name());
            assert_eq!(field.is_nullable(), selected.is_nullable());
            assert!(field.is_nullable());
        }
        assert_eq!(fields[1].data_type(), &DataType::Null);
        let DataType::Struct(children) = fields[0].data_type() else {
            panic!("partial source");
        };
        assert_eq!(children[0].data_type(), &DataType::Null);
        assert!(children[1].metadata().as_arc().is_none());
        assert_eq!(
            domain_at(&expression, &[0, 0, 1]),
            Some(novarocks_type_contract::ValueLogicalType::Bitmap)
        );
    }

    #[test]
    fn m07_container_transforms_preserve_null_siblings_and_ordinary_values() {
        for sql in [
            "select flatten([[row(null,b)]]) from t",
            "select repeat(row(null,b),2) from t",
            "select ARRAY[ROW(NULL,b)] from t",
        ] {
            assert_eq!(output_domains(sql), vec![None]);
            let expression = output_expressions(sql).remove(0);
            let DataType::List(item) = &expression.value_type.data_type() else {
                panic!("List");
            };
            let DataType::Struct(fields) = item.data_type() else {
                panic!("Struct");
            };
            assert_eq!(fields[0].data_type(), &DataType::Null);
            assert!(fields[1].metadata().as_arc().is_none());
            assert_eq!(
                domain_at(&expression, &[0, 1]),
                Some(novarocks_type_contract::ValueLogicalType::Bitmap)
            );
        }
        for sql in [
            "select flatten([[1,2]])",
            "select repeat('plain',2)",
            "select zip([1],[2])",
        ] {
            let expression = output_expressions(sql).remove(0);
            let DataType::List(item) = &expression.value_type.data_type() else {
                panic!("ordinary List");
            };
            assert_eq!(novarocks_types::logical::logical_type_of_field(item), None);
        }
        let expression = output_expressions("select repeat(null,2)").remove(0);
        let DataType::List(item) = &expression.value_type.data_type() else {
            panic!("NULL List");
        };
        assert_eq!(item.data_type(), &DataType::Null);
    }

    #[test]
    fn m07_container_keeps_json_casts_and_rejects_public_variant_casts() {
        let expression = output_expressions("select repeat(cast(null as json),2)").remove(0);
        let DataType::List(item) = expression.value_type.data_type() else {
            panic!("typed NULL List");
        };
        assert_eq!(item.data_type(), &DataType::Utf8);
        assert!(item.metadata().as_arc().is_none());
        assert_eq!(
            domain_at(&expression, &[0]),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        assert_eq!(
            output_domains(
                "select repeat(cast(j as json),2),element_at(repeat(cast(j as json),2),1) from t"
            ),
            vec![
                Some(SqlType::Array(Box::new(SqlType::Json))),
                Some(SqlType::Json)
            ],
        );
        for sql in [
            "select repeat(cast(null as variant),2)",
            "select repeat(cast(v as variant),2) from t",
            "select repeat(cast('x' as variant),2)",
        ] {
            assert_public_variant_cast_unsupported(sql);
        }
    }

    #[test]
    fn m07_container_transform_requires_selected_identity_kind_and_result() {
        let original = output_expressions("select repeat(b,2) from t").remove(0);
        let (binding, args) = match &original.kind {
            ExprKind::FunctionCall { binding, args, .. }
            | ExprKind::WindowCall { binding, args, .. } => (binding, args),
            _ => panic!("complete call"),
        };
        let selection = binding.resolved().clone();
        let control = fixture_control();
        let arguments = args
            .iter()
            .map(|arg| {
                crate::analysis::function_argument(
                    arg,
                    crate::constant::test_constant_policy(),
                    control,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let request = novarocks_functions::FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: arguments.len(),
            expected_result_type: None,
        };
        assert!(
            crate::functions::build_builtin_engine_function_catalog()
                .unwrap()
                .validate_bound(&selection, request, control)
                .is_ok()
        );
        for change in 0..5 {
            let mut changed = selection.clone();
            match change {
                0 => {
                    changed.function_id =
                        novarocks_functions::FunctionId::try_new("test.shadow/complete/v1").unwrap()
                }
                1 => changed.kind = novarocks_functions::FunctionKind::Aggregate,
                2 => {
                    changed.selected.result_type = novarocks_functions::FunctionResultType::Scalar(
                        novarocks_type_contract::FunctionValueType::new(DataType::Int32, true),
                    )
                }
                3 => changed.logical_argument_count = 0,
                _ => {
                    let novarocks_functions::FunctionResultType::Scalar(result) =
                        &mut changed.selected.result_type
                    else {
                        panic!("scalar");
                    };
                    *result = result.with_nullable(!result.nullable()).unwrap();
                }
            }
            assert!(
                crate::functions::build_builtin_engine_function_catalog()
                    .unwrap()
                    .validate_bound(&changed, request, control)
                    .is_err(),
                "exact selected alteration {change}"
            );
        }
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let scope = AnalyzerScope::new(factory.clone());
        let adapted = context(factory)
            .adapt_bound_output_domains(original.clone(), None, &scope, Span::new(0, 0))
            .unwrap();
        assert_eq!(adapted.value_type, original.value_type);
        assert_eq!(
            format!("{:?}", adapted.kind),
            format!("{:?}", original.kind)
        );
    }

    #[test]
    fn array_subfield_keeps_the_exact_selected_source_and_nominal_children() {
        let (resolved, _, factory) = super::super::analyze(
            &query("SELECT __array_struct_subfield(ARRAY[r], 'JSON') FROM t"),
            &Catalog,
            "db",
        )
        .unwrap();
        let crate::analysis::QueryBody::Select(select) = resolved.body else {
            panic!("select")
        };
        let output = select.projection[0].expr.clone();
        let ExprKind::FunctionCall { binding, args, .. } = &output.kind else {
            panic!("selected subfield")
        };
        assert_eq!(
            binding.function_id.as_str(),
            "builtin.scalar/__array_struct_subfield/v1"
        );
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("scalar subfield result")
        };
        assert_eq!(result, &output.value_type);
        assert_eq!(
            domain_at(&output, &[0]),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        assert_eq!(
            domain_at(&args[0], &[0, 0]),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        assert_eq!(
            domain_at(&args[0], &[0, 1]),
            Some(novarocks_type_contract::ValueLogicalType::Bitmap)
        );
        let original = binding.resolved().clone();
        let factory = Rc::new(RefCell::new(factory));
        let scope = AnalyzerScope::new(factory.clone());
        let validated = context(factory)
            .adapt_bound_output_domains(output.clone(), None, &scope, Span::new(0, 0))
            .unwrap();
        assert_eq!(validated.value_type, output.value_type);
        let ExprKind::FunctionCall { binding, .. } = &validated.kind else {
            panic!("original subfield")
        };
        assert_eq!(binding.resolved(), &original);
    }

    #[test]
    fn m07_container_final_compiler_scalar_proof_matches_exact_domains() {
        // The semantic transform tests above own flatten/repeat/zip rules.
        // Final compiler proofs use installed suppliers of the same exact
        // domains; declaration-only functions do not imply pure CPU presence.
        use novarocks_physical_plan::ResultValueDomain as D;
        use novarocks_result_contract::{
            NamedScalarField as N, ScalarField as F, ScalarOpaqueType as O, ScalarValueType as V,
        };
        for (sql, uses_catalog, domain, scalar) in [
            (
                "select element_at([parse_json('{}')],1)",
                false,
                D::Json,
                V::Json,
            ),
            (
                "select [parse_json('{}')]",
                false,
                D::Plain,
                V::List(Box::new(F {
                    nullable: true,
                    value_type: V::Json,
                })),
            ),
            (
                "select [v] from t",
                true,
                D::Plain,
                V::List(Box::new(F {
                    nullable: true,
                    value_type: V::Variant,
                })),
            ),
            (
                "select element_at([v],1) from t",
                true,
                D::Variant,
                V::Variant,
            ),
            (
                "select [r] from t",
                true,
                D::Plain,
                V::List(Box::new(F {
                    nullable: true,
                    value_type: V::Struct(vec![
                        N {
                            name: "json".to_owned(),
                            field: F {
                                nullable: true,
                                value_type: V::Json,
                            },
                        },
                        N {
                            name: "bitmap".to_owned(),
                            field: F {
                                nullable: true,
                                value_type: V::Opaque(O::Bitmap),
                            },
                        },
                    ]),
                })),
            ),
        ] {
            let completed = compile_scalar_proof(sql, uses_catalog);
            let result = completed.plan().result_port().unwrap();
            assert_eq!(result.fields[0].domain, domain, "{sql}");
            let schema = completed
                .scalar_schema()
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_eq!(schema.field().value_type, scalar, "{sql}");
            assert_eq!(
                schema.field().nullable,
                result.fields[0].ty.nullable(),
                "{sql}"
            );
            assert_eq!(schema.source_slot(), None);
            novarocks_physical_plan::validate_plan(completed.plan()).unwrap();
        }
    }
    #[test]
    fn structure_projection_case_collisions_fail_at_analysis_and_unique_case_keeps_domain() {
        for sql in [
            r#"SELECT s.id FROM (SELECT CAST(ROW(1,2) AS ROW("ID" INTEGER,"id" INTEGER)) s) t"#,
            r#"SELECT __array_struct_subfield(ARRAY[CAST(ROW(1,2) AS ROW("ID" INTEGER,"id" INTEGER))],'id')"#,
        ] {
            let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
            assert!(error.to_string().contains("ambiguous"), "{sql}: {error}");
        }
        let expression =
            output_expressions("SELECT __array_struct_subfield(ARRAY[r],'bItMaP') FROM t")
                .remove(0);
        assert_eq!(
            domain_at(&expression, &[0]),
            Some(novarocks_type_contract::ValueLogicalType::Bitmap)
        );
    }
}
