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

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_functions::{ConstantPolicy, FunctionArgumentType, FunctionResultType};
use novarocks_parser::ast;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType,
    PureCompileControl, ValueLogicalType,
};
use novarocks_types::schema::{ColumnDef, SqlType};

use crate::analysis::{ExprKind, QueryBody, ResolvedQuery, TypedExpr};
use crate::analyze_error::{AnalyzeError, AnalyzeErrorKind};
use crate::catalog::PlannerTableProvider;

struct SourceCatalog {
    carrier: DataType,
    logical: Option<SqlType>,
    nullable: bool,
}

impl PlannerTableProvider for SourceCatalog {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
        use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
        use crate::planner::table::{
            ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector,
            TableDef,
        };
        let planner = TableDef {
            name: table.into(),
            columns: vec![ColumnDef {
                name: "x".into(),
                data_type: self.carrier.clone(),
                nullable: self.nullable,
                write_default: None,
                logical_type: self.logical.clone(),
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::Sql(SqlScanSource::new(
                SqlTableBindingId::new(
                    SqlTableBindingScopeId::new(NonZeroU64::new(817).unwrap()),
                    NonZeroU32::new(1).unwrap(),
                ),
                SqlTableIdentity {
                    catalog: catalog.unwrap_or("default_catalog").into(),
                    namespace: database.into(),
                    table: table.into(),
                },
                SqlScanKind::Data {
                    version: SqlTableVersionSelector::Current,
                },
            )),
        };
        Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
            catalog, database, planner,
        ))
    }
}

fn analyze(
    sql: &str,
    source: &SourceCatalog,
    policy: ConstantPolicy,
) -> Result<ResolvedQuery, AnalyzeError> {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("query");
    };
    super::analyze_with_function_catalog_and_sql_semantics(
        query,
        source,
        "default",
        crate::functions::builtin_sql_function_catalog(),
        &crate::sql_mode::SqlSemanticSettings::default(),
        policy,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .map(|result| result.0)
}

fn project(query: &ResolvedQuery, ordinal: usize) -> &TypedExpr {
    let QueryBody::Select(select) = &query.body else {
        panic!("select");
    };
    &select.projection[ordinal].expr
}

fn call(expr: &TypedExpr) -> (&str, &crate::binding::SqlFunctionBinding, &[TypedExpr]) {
    let ExprKind::FunctionCall {
        name,
        binding,
        args,
        distinct,
        ..
    } = &expr.kind
    else {
        panic!("bound scalar");
    };
    assert!(!distinct);
    (name, binding, args)
}

fn assert_bound(
    expr: &TypedExpr,
    name: &str,
    arguments: &[FunctionValueType],
    result: &FunctionValueType,
) {
    let (actual_name, binding, args) = call(expr);
    assert_eq!(actual_name, name);
    assert_eq!(
        binding.function_id.as_str(),
        format!("builtin.scalar/{name}/v1")
    );
    assert_eq!(args.len(), arguments.len());
    for ((arg, selected), expected) in args
        .iter()
        .zip(binding.selected.argument_types.iter())
        .zip(arguments)
    {
        assert_eq!(&arg.value_type, expected);
        assert_eq!(selected, &FunctionArgumentType::Value(expected.clone()));
    }
    assert_eq!(&expr.value_type, result);
    assert_eq!(
        binding.selected.result_type,
        FunctionResultType::Scalar(result.clone())
    );
}

#[test]
fn canonical_day_arithmetic_and_ordinary_calls_keep_four_units_zones_and_source_fvt() {
    let mut carriers = vec![DataType::Date32];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [None, Some("UTC".into())] {
            carriers.push(DataType::Timestamp(unit, zone));
        }
    }
    for carrier in carriers {
        for nullable in [false, true] {
            let source = SourceCatalog {
                carrier: carrier.clone(),
                logical: None,
                nullable,
            };
            let target = if carrier == DataType::Date32 {
                DataType::Date32
            } else {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            };
            let args = [
                FunctionValueType::new(
                    target.clone(),
                    nullable
                        || matches!(
                            carrier,
                            DataType::Timestamp(TimeUnit::Second | TimeUnit::Millisecond, _)
                        ),
                ),
                // Explicit SQL CAST authors nullable=true before this helper.
                FunctionValueType::new(DataType::Int64, true),
            ];
            let result = FunctionValueType::new(target.clone(), true);
            let sql = "SELECT x+CAST(5 AS INT), CAST(5 AS INT)+x, x-CAST(5 AS INT), days_add(x,CAST(5 AS INT)), days_sub(x,CAST(5 AS INT)) FROM temporal_source";
            let query = analyze(sql, &source, crate::constant::test_constant_policy()).unwrap();
            for (ordinal, name) in [
                (0, "days_add"),
                (1, "days_add"),
                (2, "days_sub"),
                (3, "days_add"),
                (4, "days_sub"),
            ] {
                let expression = project(&query, ordinal);
                assert_bound(expression, name, &args, &result);
                let date = &call(expression).2[0];
                if carrier != target {
                    let ExprKind::Cast {
                        expr,
                        target: cast_target,
                        decimal_overflow_policy,
                    } = &date.kind
                    else {
                        panic!("canonical temporal cast");
                    };
                    assert_eq!(
                        expr.value_type,
                        FunctionValueType::new(carrier.clone(), nullable)
                    );
                    assert!(matches!(expr.kind, ExprKind::ColumnRef { .. }));
                    assert_eq!(cast_target, &target);
                    assert_eq!(
                        *decimal_overflow_policy,
                        call(expression).1.decimal_overflow_policy()
                    );
                } else {
                    assert!(matches!(date.kind, ExprKind::ColumnRef { .. }));
                }
            }
            assert_eq!(
                call(project(&query, 0)).1.selected,
                call(project(&query, 3)).1.selected
            );
            assert_eq!(
                call(project(&query, 2)).1.selected,
                call(project(&query, 4)).1.selected
            );
        }
    }
}

#[test]
fn canonical_day_arithmetic_keeps_lexical_policy_and_required_nullable_sources() {
    use DecimalOverflowPolicy::{OutputNull, ReportError};
    let source = SourceCatalog {
        carrier: DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        logical: None,
        nullable: true,
    };
    for (outer, inner, outer_policy, inner_policy) in [
        ("ERROR_IF_OVERFLOW", "32", ReportError, OutputNull),
        ("32", "ERROR_IF_OVERFLOW", OutputNull, ReportError),
    ] {
        let sql = format!(
            "SELECT /*+ SET_VAR(sql_mode='{outer}') */ q.x+CAST(1 AS INT), days_add(q.x,CAST(1 AS INT)) FROM (SELECT /*+ SET_VAR(sql_mode='{inner}') */ x,x-CAST(1 AS INT) AS shifted FROM temporal_source) q"
        );
        let query = analyze(&sql, &source, crate::constant::test_constant_policy()).unwrap();
        for ordinal in [0, 1] {
            let (_, binding, args) = call(project(&query, ordinal));
            assert_eq!(binding.decimal_overflow_policy(), outer_policy);
            let ExprKind::Cast {
                expr,
                decimal_overflow_policy,
                ..
            } = &args[0].kind
            else {
                panic!("source cast");
            };
            assert_eq!(*decimal_overflow_policy, outer_policy);
            assert_eq!(
                expr.value_type,
                FunctionValueType::new(source.carrier.clone(), true)
            );
        }
        let QueryBody::Select(select) = &query.body else {
            panic!("select");
        };
        let Some(crate::analysis::Relation::Subquery {
            query: inner_query, ..
        }) = &select.from
        else {
            panic!("subquery");
        };
        let (_, binding, args) = call(project(inner_query, 1));
        assert_eq!(binding.decimal_overflow_policy(), inner_policy);
        let ExprKind::Cast {
            decimal_overflow_policy,
            ..
        } = &args[0].kind
        else {
            panic!("source cast");
        };
        assert_eq!(*decimal_overflow_policy, inner_policy);
    }
    // Typed temporal NULL is a legitimate source; it does not authorize a
    // bare untyped NULL arithmetic domain or an extra raw carrier profile.
    let query = analyze(
        "SELECT CAST(NULL AS DATETIME)+1, days_add(CAST(NULL AS DATETIME),1)",
        &source,
        crate::constant::test_constant_policy(),
    )
    .unwrap();
    for ordinal in [0, 1] {
        assert_bound(
            project(&query, ordinal),
            "days_add",
            &[
                FunctionValueType::new(DataType::Timestamp(TimeUnit::Microsecond, None), true),
                FunctionValueType::new(DataType::Int64, false),
            ],
            &FunctionValueType::new(DataType::Timestamp(TimeUnit::Microsecond, None), true),
        );
    }
}

#[test]
fn canonical_hidden_array_literal_materializes_common_width_and_keeps_null_and_nominal_domains() {
    for nullable in [false, true] {
        let source = SourceCatalog {
            carrier: DataType::Int8,
            logical: None,
            nullable,
        };
        let query = analyze(
            "SELECT [x,CAST(300 AS SMALLINT)], [x,NULL] FROM temporal_source",
            &source,
            crate::constant::test_constant_policy(),
        )
        .unwrap();
        let first = project(&query, 0);
        assert_bound(
            first,
            "__array_literal",
            &[
                FunctionValueType::new(DataType::Int16, nullable),
                // The original explicit SQL CAST permits NULL.
                FunctionValueType::new(DataType::Int16, true),
            ],
            &FunctionValueType::new(
                DataType::List(Arc::new(Field::new("item", DataType::Int16, true))),
                false,
            ),
        );
        let ExprKind::Cast { expr, target, .. } = &call(first).2[0].kind else {
            panic!("common width source cast");
        };
        assert_eq!(*target, DataType::Int16);
        assert_eq!(
            expr.value_type,
            FunctionValueType::new(DataType::Int8, nullable)
        );
        assert!(matches!(expr.kind, ExprKind::ColumnRef { .. }));
        let second = project(&query, 1);
        assert_bound(
            second,
            "__array_literal",
            &[
                FunctionValueType::new(DataType::Int8, nullable),
                FunctionValueType::new(DataType::Int8, true),
            ],
            &FunctionValueType::new(
                DataType::List(Arc::new(Field::new("item", DataType::Int8, true))),
                false,
            ),
        );
        assert!(matches!(call(second).2[0].kind, ExprKind::ColumnRef { .. }));
    }
    let source = SourceCatalog {
        carrier: DataType::Utf8,
        logical: Some(SqlType::Json),
        nullable: false,
    };
    let query = analyze(
        "SELECT [x,x] FROM temporal_source",
        &source,
        crate::constant::test_constant_policy(),
    )
    .unwrap();
    let (_, binding, args) = call(project(&query, 0));
    for argument in args {
        assert_eq!(argument.value_type.logical_type(), ValueLogicalType::Json);
        assert!(matches!(argument.kind, ExprKind::ColumnRef { .. }));
    }
    let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
        panic!("scalar");
    };
    let DataType::List(item) = &result.data_type() else {
        panic!("list");
    };
    assert_eq!(
        result,
        &crate::test_value_types::string_list(true, true, false)
    );
    assert!(item.metadata().as_arc().is_none());
    assert_eq!(&project(&query, 0).value_type, result);
}

#[test]
fn canonical_higher_order_helper_keeps_original_lambda_and_nested_source_metadata() {
    let field = Arc::new(
        Field::new("source_element", DataType::Int64, false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider".to_string(),
                "exact original field".to_string(),
            )]),
        ),
    );
    let source = SourceCatalog {
        carrier: DataType::List(field),
        logical: None,
        nullable: true,
    };
    let query = analyze(
        "SELECT array_map(v -> v,x) FROM temporal_source",
        &source,
        crate::constant::test_constant_policy(),
    )
    .unwrap();
    let expression = project(&query, 0);
    let (_, binding, args) = call(expression);
    assert_eq!(binding.function_id.as_str(), "builtin.scalar/array_map/v1");
    let ExprKind::LambdaFunction { params, body } = &args[0].kind else {
        panic!("lambda");
    };
    assert_eq!(params.len(), 1);
    let expected = FunctionValueType::new(DataType::Int64, false);
    assert_eq!(params[0].value_type, expected);
    assert_eq!(body.value_type, expected);
    assert!(matches!(body.kind, ExprKind::LambdaParamRef { .. }));
    assert_eq!(
        binding.selected.argument_types[0],
        FunctionArgumentType::Lambda {
            parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                vec![expected.clone()].into_boxed_slice()
            ),
            result_type: expected,
        }
    );
    assert_eq!(
        args[1].value_type,
        FunctionValueType::new(source.carrier.clone(), true)
    );
    assert!(matches!(args[1].kind, ExprKind::ColumnRef { .. }));
    assert_eq!(
        binding.selected.argument_types[1],
        FunctionArgumentType::Value(args[1].value_type.clone())
    );
    let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
        panic!("scalar");
    };
    assert_eq!(&expression.value_type, result);
    let DataType::List(item) = &result.data_type() else {
        panic!("list");
    };
    assert_eq!(item.data_type(), &DataType::Int64);
    assert!(item.is_nullable());
    assert_eq!(item.name(), "item");
    assert!(item.metadata().as_arc().is_none());
    let DataType::List(source_item) = &source.carrier else {
        panic!("expected original source list");
    };
    assert_eq!(source_item.name(), "source_element");
    assert_eq!(
        source_item.metadata().get("provider").map(String::as_str),
        Some("exact original field")
    );
}

#[derive(Default)]
struct Trace {
    callbacks: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.callbacks.lock().unwrap();
        let at = trace.len();
        trace.push((phase, units));
        if let Some((refused, cause)) = self.refusal.clone()
            && at == refused
        {
            return Err(cause);
        }
        Ok(())
    }
}

#[test]
fn canonical_resolved_helper_materializes_text_and_preserves_success_error_control_prefixes() {
    // These values come from the full analyzer's source author. Injection is
    // scoped to its actual shared helper, not a new whole-analyzer wallet.
    for carrier in [DataType::LargeUtf8, DataType::Binary] {
        let source = SourceCatalog {
            carrier: carrier.clone(),
            logical: None,
            nullable: true,
        };
        let query = analyze(
            "SELECT x FROM temporal_source",
            &source,
            crate::constant::test_constant_policy(),
        )
        .unwrap();
        let argument = project(&query, 0).clone();
        let span = novarocks_parser::Span::new(7, 21);
        let execute = |control: &dyn PureCompileControl| {
            super::resolve_expr::resolved_scalar_call_at(
                crate::functions::builtin_sql_function_catalog(),
                "date",
                vec![argument.clone()],
                span,
                DecimalOverflowPolicy::ReportError,
                crate::constant::test_constant_policy(),
                control,
            )
        };
        let good = Trace::default();
        let result = execute(&good);
        if carrier == DataType::LargeUtf8 {
            let expression = result.unwrap();
            assert_bound(
                &expression,
                "date",
                &[FunctionValueType::new(DataType::Utf8, true)],
                &FunctionValueType::new(DataType::Date32, true),
            );
            let ExprKind::Cast {
                expr,
                target,
                decimal_overflow_policy,
            } = &call(&expression).2[0].kind
            else {
                panic!("canonical text cast");
            };
            assert_eq!(expr.value_type, argument.value_type);
            assert!(matches!(expr.kind, ExprKind::ColumnRef { .. }));
            assert_eq!(*target, DataType::Utf8);
            assert_eq!(*decimal_overflow_policy, DecimalOverflowPolicy::ReportError);
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.kind(), AnalyzeErrorKind::TypeMismatch);
            assert_eq!(error.span(), Some(span));
        }
        let trace = good.callbacks.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Trace {
                    callbacks: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let error = execute(&control).unwrap_err();
                assert_eq!(error.control_error(), Some(cause));
                assert_eq!(*control.callbacks.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
