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

use arrow::datatypes::{DataType, Field};
use novarocks_functions::{ConstantPolicy, FunctionArgumentType, FunctionResultType};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType,
    PureCompileControl, TemporalPrecision, TemporalType, ValueLogicalType,
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
    let query = &novarocks_parser::parser::parse_query(sql).unwrap();
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
fn canonical_date_add_retains_the_precise_source_type() {
    let mut sources = vec![(DataType::Date32, None)];
    for precision in [0, 3, 6, 9, 12] {
        let precision = TemporalPrecision::new(precision).unwrap();
        for temporal in [
            TemporalType::Time(precision),
            TemporalType::TimeWithTimeZone(precision),
            TemporalType::Timestamp(precision),
            TemporalType::TimestampWithTimeZone(precision),
        ] {
            sources.push((temporal.carrier(), Some(SqlType::Temporal(temporal))));
        }
    }
    for (carrier, logical) in sources {
        for nullable in [false, true] {
            let source = SourceCatalog {
                carrier: carrier.clone(),
                logical: logical.clone(),
                nullable,
            };
            let query = analyze(
                "SELECT x, date_add('day', CAST(5 AS INTEGER), x) FROM temporal_source",
                &source,
                crate::constant::test_constant_policy(),
            )
            .unwrap();
            let input = &project(&query, 0).value_type;
            let shifted = project(&query, 1);
            let (_, _, args) = call(shifted);
            assert_eq!(input.data_type(), &carrier);
            assert_eq!(input.nullable(), nullable);
            assert!(matches!(args[2].kind, ExprKind::ColumnRef { .. }));
            assert_bound(
                shifted,
                "date_add",
                &[
                    args[0].value_type.clone(),
                    FunctionValueType::new(DataType::Int64, true),
                    input.clone(),
                ],
                &input.with_nullable(true).unwrap(),
            );
        }
    }
}

#[test]
fn canonical_date_add_inherits_root_policy_and_preserves_nullable_sources() {
    use DecimalOverflowPolicy::{OutputNull, ReportError};
    let temporal = TemporalType::TimestampWithTimeZone(TemporalPrecision::new(9).unwrap());
    let source = SourceCatalog {
        carrier: temporal.carrier(),
        logical: Some(SqlType::Temporal(temporal)),
        nullable: true,
    };
    for (outer, outer_policy) in [("ERROR_IF_OVERFLOW", ReportError), ("32", OutputNull)] {
        let sql = format!(
            "WITH SESSION sql_mode = '{outer}' SELECT date_add('day', CAST(1 AS BIGINT), q.x) FROM (SELECT x,date_add('day', CAST(-1 AS BIGINT), x) AS shifted FROM temporal_source) q"
        );
        let query = analyze(&sql, &source, crate::constant::test_constant_policy()).unwrap();
        let QueryBody::Select(select) = &query.body else {
            panic!("select");
        };
        let Some(crate::analysis::Relation::Subquery {
            query: inner_query, ..
        }) = &select.from
        else {
            panic!("subquery");
        };
        for expression in [project(&query, 0), project(inner_query, 1)] {
            let (_, binding, args) = call(expression);
            assert_eq!(binding.decimal_overflow_policy(), outer_policy);
            let ExprKind::Cast {
                decimal_overflow_policy,
                ..
            } = &args[1].kind
            else {
                panic!("original explicit offset cast");
            };
            assert_eq!(*decimal_overflow_policy, outer_policy);
            assert_eq!(args[2].value_type.data_type(), &source.carrier);
            assert!(args[2].value_type.nullable());
            assert_eq!(expression.value_type, args[2].value_type);
        }
    }

    let query = analyze(
        "SELECT CAST(NULL AS TIMESTAMP(6)), date_add('day', 1, CAST(NULL AS TIMESTAMP(6)))",
        &source,
        crate::constant::test_constant_policy(),
    )
    .unwrap();
    let null = &project(&query, 0).value_type;
    let shifted = project(&query, 1);
    assert!(null.nullable());
    assert_eq!(&call(shifted).2[2].value_type, null);
    assert_eq!(&shifted.value_type, null);
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
                FunctionValueType::new(DataType::Int16, false),
            ],
            &FunctionValueType::new(
                DataType::List(Arc::new(Field::new("item", DataType::Int16, true))),
                false,
            ),
        );
        let coercion = &call(first).2[0];
        let ExprKind::Cast {
            expr: original,
            cast_mode: Some(novarocks_type_contract::TrinoCastMode::Implicit),
            target: DataType::Int16,
            ..
        } = &coercion.kind
        else {
            panic!("ordered common width conversion");
        };
        assert_eq!(
            coercion.value_type,
            FunctionValueType::new(DataType::Int16, nullable)
        );
        assert_eq!(
            original.value_type,
            FunctionValueType::new(DataType::Int8, nullable)
        );
        assert!(matches!(original.kind, ExprKind::ColumnRef { .. }));
        let explicit = &call(first).2[1];
        assert!(matches!(
            explicit.kind,
            ExprKind::Cast {
                cast_mode: Some(novarocks_type_contract::TrinoCastMode::Explicit),
                ..
            }
        ));
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
fn canonical_cardinality_keeps_original_nested_source_metadata() {
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
    let selected = analyze(
        "SELECT cardinality(x) FROM temporal_source",
        &source,
        crate::constant::test_constant_policy(),
    )
    .unwrap();
    let expression = project(&selected, 0);
    let (_, binding, args) = call(expression);
    assert_eq!(
        binding.function_id.as_str(),
        "builtin.scalar/cardinality/v1"
    );
    assert_eq!(
        expression.value_type,
        FunctionValueType::new(DataType::Int64, true)
    );
    assert_eq!(
        args[0].value_type,
        FunctionValueType::new(source.carrier.clone(), true)
    );
    assert_eq!(
        binding.selected.argument_types[0],
        FunctionArgumentType::Value(args[0].value_type.clone())
    );
    let DataType::List(item) = args[0].value_type.data_type() else {
        panic!("canonical source ARRAY")
    };
    assert_eq!(item.name(), "item");
    assert!(item.metadata().as_arc().is_none());
    let DataType::List(provider_item) = &source.carrier else {
        panic!("original provider ARRAY")
    };
    assert_eq!(provider_item.name(), "source_element");
    assert_eq!(
        provider_item.metadata().get("provider").map(String::as_str),
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
                cast_mode: None,
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
            assert_eq!(error.kind(), AnalyzeErrorKind::UnknownFunction);
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
