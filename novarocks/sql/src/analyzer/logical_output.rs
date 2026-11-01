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
use novarocks_parser::{Span, ast};
#[cfg(test)]
use novarocks_types::logical::LogicalType;
#[cfg(test)]
use novarocks_types::logical::field_with_logical_type;
use novarocks_types::schema::SqlType;

use super::{AnalyzerContext, scope::AnalyzerScope};
use crate::analysis::{ExprKind, TypedExpr};
use crate::analyze_error::AnalyzeError;

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
        let mut statements = novarocks_parser::parse(sql).unwrap();
        let [ast::Statement::Query(query)] = statements.as_mut_slice() else {
            panic!("expected query")
        };
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
            materialized_values: None,
            constant_policy: crate::constant::test_constant_policy(),
            control,
            catalog: &Catalog,
            current_database: "db",
            function_catalog: crate::functions::builtin_sql_function_catalog(),
            sql_semantics: Default::default(),
            factory,
            ctes: Default::default(),
            pending_ctes: Default::default(),
            next_subquery_id: Cell::new(0),
            next_lambda_slot_id: Cell::new(0),
            collected_subqueries: RefCell::new(Vec::new()),
            cte_registry: RefCell::new(Default::default()),
        }
    }

    #[test]
    fn m07_json_list_explicit_literal_target_keeps_its_source_decision() {
        assert_eq!(
            output_domains("select array<json>[],array<json>[null]"),
            vec![Some(SqlType::Array(Box::new(SqlType::Json))); 2],
        );
        assert_eq!(
            output_domains(
                "with q as (select array<json>[] as j) select array_sortby(j,[1]) from q"
            ),
            vec![Some(SqlType::Array(Box::new(SqlType::Json)))],
        );
        assert_eq!(
            output_domains("select array<varchar>[json_object('k',1)],array<json>['plain']"),
            vec![
                Some(SqlType::Array(Box::new(SqlType::String))),
                Some(SqlType::Array(Box::new(SqlType::String)))
            ],
        );
        assert_eq!(
            output_domains("select cast(['plain'] as array<json>)"),
            vec![Some(SqlType::Array(Box::new(SqlType::String)))],
        );
        let sql = "select array<json>['{}'],array<json>[parse_json('{}')]";
        assert_eq!(
            output_domains(sql),
            vec![
                Some(SqlType::Array(Box::new(SqlType::String))),
                Some(SqlType::Array(Box::new(SqlType::Json)))
            ]
        );
        let expressions = output_expressions(sql);
        assert_eq!(domain_at(&expressions[0], &[0]), None);
        assert_eq!(
            domain_at(&expressions[1], &[0]),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        for expression in expressions {
            let ExprKind::FunctionCall { binding, .. } = &expression.kind else {
                panic!("literal owner");
            };
            let novarocks_functions::FunctionResultType::Scalar(result) =
                &binding.selected.result_type
            else {
                panic!("scalar");
            };
            assert_eq!(result, &expression.value_type);
            let DataType::List(item) = expression.value_type.data_type() else {
                panic!("list");
            };
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
        let bound = super::super::resolve_expr::resolved_scalar_call_at(
            context.function_catalog,
            "row",
            vec![value],
            Span::new(0, 0),
            context.sql_semantics.sql_mode().decimal_overflow_policy(),
            context.constant_policy,
            context.control,
        )
        .unwrap();
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
        assert_eq!(
            output_domains("select h from t union all select b from t"),
            vec![None]
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
                "select coalesce(null,parse_json('null')),if(false,null,json_object('k',1)),case when true then null else parse_json('null') end"
            ),
            vec![Some(SqlType::Json); 3]
        );
        assert_eq!(
            output_domains("select coalesce(null,null),case when true then null else null end"),
            vec![None, Some(SqlType::String)]
        );
    }

    #[test]
    fn m07_scalar_mixed_values_and_explicit_string_cast_never_guess_json_or_opaque() {
        // IF's installed selection still refuses distinct nominal arguments;
        // the CASE syntax owner independently selects explicit byte erasure.
        let error = super::super::analyze(&query("select if(true,h,b) from t"), &Catalog, "db")
            .unwrap_err();
        assert!(
            error.to_string().contains("no matching declared overload"),
            "{error}"
        );
        let sql = "select case when true then h else b end from t";
        assert_eq!(output_domains(sql), vec![Some(SqlType::Binary)]);
        let expression = output_expressions(sql).remove(0);
        assert_eq!(
            expression.value_type,
            novarocks_type_contract::FunctionValueType::new(DataType::Binary, true)
        );
        let ExprKind::Case {
            when_then,
            else_expr,
            ..
        } = &expression.kind
        else {
            panic!("actual CASE owner");
        };
        for (converted, original_domain) in [
            (
                &when_then[0].1,
                novarocks_type_contract::ValueLogicalType::Hll,
            ),
            (
                else_expr.as_deref().unwrap(),
                novarocks_type_contract::ValueLogicalType::Bitmap,
            ),
        ] {
            let ExprKind::FunctionCall { binding, args, .. } = &converted.kind else {
                panic!("real selected erasure");
            };
            assert_eq!(
                binding.function_id.as_str(),
                novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_FUNCTION_ID
            );
            assert_eq!(
                binding.selected.overload.as_str(),
                "builtin.scalar/value_domain_conversion/opaque_to_binary_same_structure/v1"
            );
            let novarocks_functions::FunctionResultType::Scalar(result) =
                &binding.selected.result_type
            else {
                panic!("scalar");
            };
            assert_eq!(result, &converted.value_type);
            assert_eq!(converted.value_type, expression.value_type);
            assert_eq!(args[0].value_type.logical_type(), original_domain);
            let novarocks_functions::FunctionArgumentType::Value(formal) =
                &binding.selected.argument_types[0]
            else {
                panic!("value formal");
            };
            assert_eq!(&args[0].value_type, formal);
            let original_arguments = args
                .iter()
                .map(|argument| {
                    crate::analysis::function_argument(
                        argument,
                        crate::constant::test_constant_policy(),
                        fixture_control(),
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            crate::functions::build_builtin_engine_function_catalog()
                .unwrap()
                .validate_bound(
                    binding.resolved(),
                    novarocks_functions::FunctionBindingRequest {
                        arguments: &original_arguments,
                        logical_argument_count: original_arguments.len(),
                        expected_result_type: Some(result),
                    },
                    fixture_control(),
                )
                .unwrap();
        }
        assert_eq!(
            output_domains(
                "select coalesce(j,s),ifnull(j,'plain'),cast(j as varchar),cast(h as varbinary),cast(s as json) from t"
            ),
            vec![
                Some(SqlType::String),
                Some(SqlType::String),
                Some(SqlType::String),
                Some(SqlType::Binary),
                Some(SqlType::String)
            ]
        );
        assert_eq!(
            output_domains("select cast(j as json),coalesce(cast(j as json),null) from t"),
            vec![Some(SqlType::Json); 2]
        );
        // A JSON target declaration alone does not validate ordinary text.
        // The actual existing parse_json owner is the independently checked
        // author of a JSON value; no carrier Cast may sign that domain.
        let value = output_expressions("select cast('plain' as json)").remove(0);
        assert_eq!(
            value.value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Physical
        );
        assert_eq!(
            output_domains("select cast('plain' as json)"),
            vec![Some(SqlType::String)]
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
        let error =
            super::super::analyze(&query("select nullif(h,b) from t"), &Catalog, "db").unwrap_err();
        assert!(
            error.to_string().contains("no matching declared overload"),
            "{error}"
        );
        assert_eq!(
            output_domains("select nullif(h,h),nullif(j,s),nullif(s,j),nullif(null,j) from t"),
            vec![
                Some(SqlType::Hll),
                Some(SqlType::String),
                Some(SqlType::String),
                Some(SqlType::Json)
            ]
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
            output_domains("select cast(j as varchar),cast(h as varbinary) from t"),
            vec![Some(SqlType::String), Some(SqlType::Binary)]
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
        let sql = "with q as (select [to_bitmap(1)] as b) select first_value(b) over (),last_value(b) over (),lead(b,1,b) over (),lag(b,1,null) over () from q";
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
            output_expressions("select first_value(row(null,to_bitmap(1))) over ()").remove(0);
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
            output_domains("select first_value(row(null,to_bitmap(1))) over ()"),
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
    fn m07_window_variant_typed_null_defaults_are_neutral_without_admitting_unproven_values() {
        assert_eq!(
            output_domains(
                "select lead(v,1,cast(null as variant)) over (),lag(v,1,cast(null as variant)) over () from t"
            ),
            vec![Some(SqlType::Variant); 2]
        );
        // Same-domain public CAST is an actual LargeBinary identity operation,
        // not an unproven payload. Both value and default contributions retain
        // the independently established catalog Variant fact.
        assert_eq!(
            output_domains(
                "select lead(v,1,cast(v as variant)) over (),lag(v,1,cast(v as variant)) over (),first_value(cast(v as variant)) over () from t"
            ),
            vec![Some(SqlType::Variant); 3]
        );
        for sql in [
            "select lead(v,1,cast(s as variant)) over () from t",
            "select lag(v,1,cast(s as variant)) over () from t",
            "select first_value(cast(X'AB01' as variant)) over ()",
            "select first_value(cast(cast(v as varbinary) as variant)) over () from t",
        ] {
            let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
            assert!(
                error.to_string().contains("no matching declared overload"),
                "{error}"
            );
        }
        // A NULL default cannot erase a known sibling in a partial Struct or
        // a nested Map value. SqlType has no complete Null-bearing counterpart.
        let sql = "with q as (select row(null,map('k',to_bitmap(1))) as r) select lead(r,1,null) over (),lag(r,1,null) over () from q";
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
    fn m07_variant_public_identity_cast_requires_independent_exact_source_fact() {
        assert_eq!(
            output_domains(
                "select cast(v as variant),cast(cast(v as variant) as variant),cast((v) as variant) from t"
            ),
            vec![Some(SqlType::Variant); 3]
        );
        for sql in [
            "select cast(s as variant) from t",
            "select cast(j as variant) from t",
            "select cast(X'AB01' as variant)",
            "select cast(cast(v as varbinary) as variant) from t",
        ] {
            let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
            assert_eq!(
                error.kind(),
                crate::analyze_error::AnalyzeErrorKind::TypeMismatch
            );
        }
        assert_eq!(
            output_domains("select cast(v as varbinary) from t"),
            vec![Some(SqlType::Binary)]
        );
        assert_eq!(
            output_domains(
                "select first_value(cast(variant_get(v,'$.a') as variant)) over (),last_value(cast(variant_get(v,'$.a') as variant)) over () from t"
            ),
            vec![Some(SqlType::Variant); 2]
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
                expr: Box::new(original),
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
        let mut conversion = &args[0];
        while let ExprKind::Cast { expr, .. } = &conversion.kind {
            conversion = expr;
        }
        let ExprKind::FunctionCall {
            binding: conversion_binding,
            args: original,
            ..
        } = &conversion.kind
        else {
            panic!("selected explicit conversion");
        };
        assert_eq!(
            conversion_binding.function_id.as_str(),
            novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_FUNCTION_ID
        );
        assert_eq!(
            conversion_binding.selected.overload.as_str(),
            "builtin.scalar/value_domain_conversion/json_text_same_structure/v1"
        );
        assert_eq!(
            original[0].value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Json
        );
        assert_eq!(
            conversion.value_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Physical
        );
    }

    #[test]
    fn m07_window_partial_null_value_keeps_known_origin_through_internal_cast() {
        let original =
            output_expressions("select first_value(row(null,to_bitmap(1))) over ()").remove(0);
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
                expr: Box::new(original.clone()),
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

    #[test]
    fn m07_window_final_compiler_scalar_proof_matches_the_frozen_output() {
        // This oracle checks the window output domain and frozen scalar proof, not
        // to_bitmap or variant_get availability. Installed bitmap_agg and an exact
        // NULL Variant declaration supply the same opaque/Variant input domains.
        // Pure LEAD/LAG defaults support only existing numeric/Utf8 profiles:
        // Json uses a neutral NULL; mixed physical carriers exercise both default
        // conversions, and Variant uses the installed two-argument source path.
        // The semantic-only value-supplier and Variant-default tests above retain
        // the mixed nominal/default rules independently of pure CPU admission.
        use crate::compiler::{
            DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl,
            SqlCompileIntent, SqlCompileProgress, SqlCompiler, SqlFinalPlanCompileRequest,
            SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
            builtin_sql_function_catalog, noop_constant_evaluator,
        };
        use novarocks_physical_plan::{
            MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId,
            ResultValueDomain as D, ScanReadBudget,
        };
        use novarocks_result_contract::{ScalarOpaqueType as O, ScalarValueType as V};
        for (sql, domain, scalar) in [
            (
                "select first_value(parse_json('{}')) over ()",
                D::Json,
                V::Json,
            ),
            (
                "with q as (select bitmap_agg(1) b) select last_value(b) over () from q",
                D::Bitmap,
                V::Opaque(O::Bitmap),
            ),
            (
                "select first_value(hll_hash('x')) over ()",
                D::Hll,
                V::Opaque(O::Hll),
            ),
            (
                "select lead(parse_json('{}'),1,null) over ()",
                D::Json,
                V::Json,
            ),
            (
                "select lag(parse_json('{}'),1,null) over ()",
                D::Json,
                V::Json,
            ),
            (
                "select lead('plain',1,cast(7 as bigint)) over ()",
                D::Plain,
                V::String,
            ),
            (
                "select lag(cast(7 as bigint),1,'2') over ()",
                D::Plain,
                V::SignedInteger(64),
            ),
            (
                "select first_value(cast(null as variant)) over ()",
                D::Variant,
                V::Variant,
            ),
            (
                "with q as (select cast(null as variant) as v) select lead(v,1) over () from q",
                D::Variant,
                V::Variant,
            ),
            (
                "with q as (select cast(null as variant) as v) select lag(v,1) over () from q",
                D::Variant,
                V::Variant,
            ),
        ] {
            let request = SqlFinalPlanCompileRequest::new(
                PlanVersionId::try_new([17; 16]).unwrap(),
                SqlStatementInput::sql(sql),
                SqlCompileIntent::Query,
                SqlSessionContext {
                    sql_semantics: Default::default(),
                    current_catalog: Some("iceberg".to_owned()),
                    current_database: "db".to_owned(),
                    optimizer_settings: SessionOptimizerSettings::default(),
                },
                SqlPlanningEnvironment::Distributed,
                builtin_sql_function_catalog().snapshot(),
                noop_constant_evaluator(),
                crate::constant::test_constant_policy(),
                SqlCompileControl::unbounded(),
                PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                ScanReadBudget {
                    max_batch_rows: MAX_SCAN_BATCH_ROWS,
                    max_batch_bytes: MAX_SCAN_BATCH_BYTES,
                },
                DEFAULT_COMPLETION_LIMITS,
            );
            let SqlCompileProgress::Complete(completed) =
                SqlCompiler::start(request.try_into_completion().unwrap(), fixture_control())
                    .unwrap()
            else {
                panic!("source-free window unexpectedly needs observations: {sql}");
            };
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
            let schema = completed.scalar_schema().unwrap();
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
        for (sql, expected) in [
            (
                "select percentile_hash(cast(1 as double)),to_bitmap(1),hll_hash('x')",
                vec![
                    Some(SqlType::Percentile),
                    Some(SqlType::Bitmap),
                    Some(SqlType::Hll),
                ],
            ),
            (
                "select percentile_hash(cast(1 as double)) as p,to_bitmap(1) as b,hll_hash('x') as h",
                vec![
                    Some(SqlType::Percentile),
                    Some(SqlType::Bitmap),
                    Some(SqlType::Hll),
                ],
            ),
            (
                "with q as (select percentile_hash(cast(1 as double)) as p) select p,p as again from q",
                vec![Some(SqlType::Percentile); 2],
            ),
            (
                "select to_bitmap(1) as b,b as again,bitmap_to_binary(b) as raw",
                vec![
                    Some(SqlType::Bitmap),
                    Some(SqlType::Bitmap),
                    Some(SqlType::Binary),
                ],
            ),
            (
                "with q as (select percentile_hash(cast(1 as double)) as p) select cast(p as varbinary) as raw,p from q",
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
            output_domains(
                "select bitmap_union(to_bitmap(1)),hll_union(hll_hash('x')),percentile_union(percentile_hash(1.0)),any_value(to_bitmap(1))"
            ),
            vec![
                Some(SqlType::Bitmap),
                Some(SqlType::Hll),
                Some(SqlType::Percentile),
                Some(SqlType::Bitmap)
            ]
        );
        assert_eq!(
            output_domains("select bitmap_union(to_bitmap(1)) over ()"),
            vec![Some(SqlType::Bitmap)]
        );
        assert_eq!(
            output_domains(
                "select array_agg(to_bitmap(1)),map_agg(cast(1 as bigint),hll_hash('x'))"
            ),
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
                "select element_at([to_bitmap(1),null],1),element_at(map(1,hll_hash('x')),1),element_at(map_values(map(1,percentile_hash(1.0))),1),named_struct('value',to_bitmap(1)).value"
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
                "select element_at(array_slice([hll_hash('x')],1),1),element_at(array_sortby([to_bitmap(1)],[1]),1),element_at(map_from_arrays([cast(1 as bigint)],[percentile_hash(1.0)]),1)"
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
        let expressions = output_expressions(
            "select row(null,to_bitmap(1)),named_struct('missing',null,'value',percentile_hash(1.0))",
        );
        for (expression, marker, names) in [
            (
                &expressions[0],
                novarocks_type_contract::ValueLogicalType::Bitmap,
                ["col1", "col2"],
            ),
            (
                &expressions[1],
                novarocks_type_contract::ValueLogicalType::Percentile,
                ["missing", "value"],
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
        assert_eq!(output_domains("select row(null,to_bitmap(1))"), vec![None]);
        assert_eq!(
            output_domains(
                "select named_struct('missing',null,'value',percentile_hash(1.0)).value"
            ),
            vec![Some(SqlType::Percentile)]
        );
    }

    #[test]
    fn typed_scalar_wrapper_preserves_selected_full_result_without_output_adapter() {
        let expression = output_expressions("select [to_bitmap(1),null]").remove(0);
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
        let error = super::super::analyze(
            &query("select element_at([to_bitmap(1),hll_hash('x')],1)"),
            &Catalog,
            "db",
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("no matching declared overload"),
            "{error}"
        );
        assert_eq!(
            output_domains("select element_at([parse_json('{}'),'plain'],1)"),
            vec![Some(SqlType::String)]
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
        assert_eq!(
            output_domains("select cast(element_at([to_bitmap(1)],1) as varbinary)"),
            vec![Some(SqlType::Binary)]
        );
    }

    #[test]
    fn m07_scalar_shadowed_wrapper_binding_cannot_project_child_domain() {
        let mut expression = output_expressions("select [to_bitmap(1)]").remove(0);
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
                "select element_at(array_flatten([[to_bitmap(1)]]),1),element_at(array_repeat(parse_json('{}'),2),1),element_at(__array_struct_subfield([named_struct('payload',to_bitmap(1))],'PAYLOAD'),1)"
            ),
            vec![
                Some(SqlType::Bitmap),
                Some(SqlType::Json),
                Some(SqlType::Bitmap)
            ]
        );
        let expressions = output_expressions(
            "select array_flatten([[parse_json('{}')]]),array_repeat(to_bitmap(1),2),__array_struct_subfield([named_struct('payload',parse_json('{}'))],'PAYLOAD')",
        );
        for (expression, id, marker) in [
            (
                &expressions[0],
                "builtin.scalar/array_flatten/v1",
                novarocks_type_contract::ValueLogicalType::Json,
            ),
            (
                &expressions[1],
                "builtin.scalar/array_repeat/v1",
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
            output_domains("select arrays_zip([to_bitmap(1)],[parse_json('{}')])"),
            vec![Some(SqlType::Array(Box::new(SqlType::Struct(vec![
                ("col1".to_owned(), SqlType::Bitmap),
                ("col2".to_owned(), SqlType::Json),
            ]))))]
        );
        let sql = "select arrays_zip([row(null,to_bitmap(1))],null)";
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
            "select array_flatten([[row(null,to_bitmap(1))]])",
            "select array_repeat(row(null,to_bitmap(1)),2)",
            "select __array_struct_subfield([named_struct('v',row(null,to_bitmap(1)))],'v')",
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
            "select array_flatten([[1,2]])",
            "select array_repeat('plain',2)",
            "select arrays_zip([1],[2])",
        ] {
            let expression = output_expressions(sql).remove(0);
            let DataType::List(item) = &expression.value_type.data_type() else {
                panic!("ordinary List");
            };
            assert_eq!(novarocks_types::logical::logical_type_of_field(item), None);
        }
        let expression = output_expressions("select array_repeat(null,2)").remove(0);
        let DataType::List(item) = &expression.value_type.data_type() else {
            panic!("NULL List");
        };
        assert_eq!(item.data_type(), &DataType::Null);
    }

    #[test]
    fn m07_container_trusted_and_null_variant_preserve_identity_unknown_cast_is_rejected() {
        let expression =
            output_expressions("select array_repeat(cast(null as variant),2)").remove(0);
        let DataType::List(item) = &expression.value_type.data_type() else {
            panic!("typed NULL List");
        };
        assert_eq!(item.data_type(), &DataType::LargeBinary);
        assert!(item.metadata().as_arc().is_none());
        assert_eq!(
            domain_at(&expression, &[0]),
            Some(novarocks_type_contract::ValueLogicalType::Variant)
        );
        // Same-carrier CAST preserves an independently established Variant
        // identity. Target spelling alone does not establish serialized bytes.
        assert_eq!(
            output_domains(
                "select array_repeat(cast(v as variant),2),element_at(array_repeat(cast(v as variant),2),1) from t"
            ),
            vec![
                Some(SqlType::Array(Box::new(SqlType::Variant))),
                Some(SqlType::Variant)
            ],
        );
        {
            let sql = "select array_repeat(cast('x' as variant),2)";
            let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
            assert!(
                error.to_string().contains("no matching declared overload"),
                "{error}"
            );
        }
    }

    #[test]
    fn m07_container_transform_requires_selected_identity_kind_and_result() {
        let original = output_expressions("select array_repeat(to_bitmap(1),2)").remove(0);
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
    fn typed_container_sortby_subfield_keeps_selected_source_and_real_conversion_owner() {
        let expression = output_expressions(
            "select array_sortby((x)->x.key,[named_struct('key',1,'payload',to_bitmap(1))])",
        )
        .remove(0);
        assert_eq!(
            expression
                .value_type
                .value_type()
                .child_type_at(0)
                .unwrap()
                .child_type_at(1)
                .unwrap()
                .domain(),
            Some(novarocks_type_contract::ValueLogicalType::Bitmap)
        );
        let (resolved, _, factory) = super::super::analyze(
            &query("select array_sortby((x)->x.key,[named_struct('key',parse_json('{}'),'payload',1)])"), &Catalog, "db",
        ).unwrap();
        let crate::analysis::QueryBody::Select(select) = resolved.body else {
            panic!("select");
        };
        let output = select.projection[0].expr.clone();
        let ExprKind::FunctionCall { binding, args, .. } = &output.kind else {
            panic!("original sortby");
        };
        let selection = binding.resolved().clone();
        let original_arguments = format!("{args:?}");
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("selected result");
        };
        assert_eq!(result, &output.value_type);
        assert_eq!(
            output
                .value_type
                .value_type()
                .child_type_at(0)
                .unwrap()
                .child_type_at(0)
                .unwrap()
                .domain(),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        let novarocks_functions::FunctionArgumentType::Value(key) =
            &binding.selected.argument_types[1]
        else {
            panic!("selected key");
        };
        assert_eq!(&args[1].value_type, key);
        assert_eq!(
            key.value_type().child_type_at(0).unwrap().domain(),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        let subfield = original_transform(&args[1]);
        let ExprKind::FunctionCall {
            binding: subfield_binding,
            args: source_args,
            ..
        } = &subfield.kind
        else {
            panic!("selected subfield");
        };
        assert_eq!(
            subfield_binding.function_id.as_str(),
            "builtin.scalar/__array_struct_subfield/v1"
        );
        let novarocks_functions::FunctionResultType::Scalar(subfield_result) =
            &subfield_binding.selected.result_type
        else {
            panic!("subfield result");
        };
        assert_eq!(&subfield.value_type, subfield_result);
        assert_eq!(
            subfield_result
                .value_type()
                .child_type_at(0)
                .unwrap()
                .domain(),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        assert_eq!(
            source_args[0]
                .value_type
                .value_type()
                .child_type_at(0)
                .unwrap()
                .child_type_at(0)
                .unwrap()
                .domain(),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        assert!(
            matches!(&source_args[1].kind, ExprKind::Literal(crate::analysis::LiteralValue::String(name)) if name == "key")
        );
        let DataType::List(item) = source_args[0].value_type.data_type() else {
            panic!("source List");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("source Struct");
        };
        assert_eq!(fields[0].name(), "key");
        assert!(fields[0].metadata().as_arc().is_none());
        let factory = Rc::new(RefCell::new(factory));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        let validated = context
            .adapt_bound_output_domains(output.clone(), None, &scope, Span::new(0, 0))
            .unwrap();
        assert_eq!(validated.value_type, output.value_type);
        let ExprKind::FunctionCall { binding, args, .. } = &validated.kind else {
            panic!("same original sortby");
        };
        assert_eq!(binding.resolved(), &selection);
        assert_eq!(format!("{args:?}"), original_arguments);
    }

    #[test]
    fn m07_container_final_compiler_scalar_proof_matches_exact_domains() {
        // The semantic transform tests above own flatten/repeat/zip rules.
        // Final compiler proofs use installed suppliers of the same exact
        // domains; declaration-only functions do not imply pure CPU presence.
        use crate::compiler::{
            DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl,
            SqlCompileIntent, SqlCompileProgress, SqlCompiler, SqlFinalPlanCompileRequest,
            SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
            builtin_sql_function_catalog, noop_constant_evaluator,
        };
        use novarocks_physical_plan::{
            MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId,
            ResultValueDomain as D, ScanReadBudget,
        };
        use novarocks_result_contract::{
            NamedScalarField as N, ScalarField as F, ScalarOpaqueType as O, ScalarValueType as V,
        };
        for (sql, domain, scalar) in [
            (
                "select element_at([bitmap_agg(1)],1)",
                D::Bitmap,
                V::Opaque(O::Bitmap),
            ),
            ("select element_at([parse_json('{}')],1)", D::Json, V::Json),
            (
                "select element_at(element_at([[bitmap_agg(1)]],1),1)",
                D::Bitmap,
                V::Opaque(O::Bitmap),
            ),
            (
                "select [parse_json('{}')]",
                D::Plain,
                V::List(Box::new(F {
                    nullable: true,
                    value_type: V::Json,
                })),
            ),
            (
                "select [cast(null as variant)]",
                D::Plain,
                V::List(Box::new(F {
                    nullable: true,
                    value_type: V::Variant,
                })),
            ),
            (
                "select element_at([cast(null as variant)],1)",
                D::Variant,
                V::Variant,
            ),
            (
                "select [cast(null as struct<col1 bitmap,col2 json>)]",
                D::Plain,
                V::List(Box::new(F {
                    nullable: true,
                    value_type: V::Struct(vec![
                        N {
                            name: "col1".to_owned(),
                            field: F {
                                nullable: true,
                                value_type: V::Opaque(O::Bitmap),
                            },
                        },
                        N {
                            name: "col2".to_owned(),
                            field: F {
                                nullable: true,
                                value_type: V::Json,
                            },
                        },
                    ]),
                })),
            ),
        ] {
            let request = SqlFinalPlanCompileRequest::new(
                PlanVersionId::try_new([17; 16]).unwrap(),
                SqlStatementInput::sql(sql),
                SqlCompileIntent::Query,
                SqlSessionContext {
                    sql_semantics: Default::default(),
                    current_catalog: Some("iceberg".to_owned()),
                    current_database: "db".to_owned(),
                    optimizer_settings: SessionOptimizerSettings::default(),
                },
                SqlPlanningEnvironment::Distributed,
                builtin_sql_function_catalog().snapshot(),
                noop_constant_evaluator(),
                crate::constant::test_constant_policy(),
                SqlCompileControl::unbounded(),
                PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                ScanReadBudget {
                    max_batch_rows: MAX_SCAN_BATCH_ROWS,
                    max_batch_bytes: MAX_SCAN_BATCH_BYTES,
                },
                DEFAULT_COMPLETION_LIMITS,
            );
            let SqlCompileProgress::Complete(completed) =
                SqlCompiler::start(request.try_into_completion().unwrap(), fixture_control())
                    .unwrap()
            else {
                panic!("source-free transform unexpectedly needs observations: {sql}");
            };
            let result = completed.plan().result_port().unwrap();
            assert_eq!(result.fields[0].domain, domain, "{sql}");
            let schema = completed.scalar_schema().unwrap();
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
            "select s.id from (select named_struct('ID',1,'id',2) s) t",
            "select __array_struct_subfield([named_struct('ID',1,'id',2)],'id')",
            "select array_sortby((x)->x.id,[named_struct('ID',1,'id',2)])",
        ] {
            let result = super::super::analyze(&query(sql), &Catalog, "db");
            match result {
                Err(source) => assert!(source.to_string().contains("ambiguous"), "{sql}: {source}"),
                Ok(_) => panic!("case collision accepted: {sql}"),
            }
        }
        let expression = output_expressions(
            "select __array_struct_subfield([named_struct('Payload',to_bitmap(1))],'pAYLOAD')",
        )
        .remove(0);
        assert_eq!(
            expression
                .value_type
                .value_type()
                .child_type_at(0)
                .unwrap()
                .domain(),
            Some(novarocks_type_contract::ValueLogicalType::Bitmap)
        );
        let expression = output_expressions(
            "select array_sortby((x)->x.kEY,[named_struct('Key',1,'payload',to_bitmap(1))])",
        )
        .remove(0);
        assert_eq!(
            expression
                .value_type
                .value_type()
                .child_type_at(0)
                .unwrap()
                .child_type_at(1)
                .unwrap()
                .domain(),
            Some(novarocks_type_contract::ValueLogicalType::Bitmap)
        );
    }
}
