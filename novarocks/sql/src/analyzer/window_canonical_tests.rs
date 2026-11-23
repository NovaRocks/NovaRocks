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
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    FunctionArgument, FunctionArgumentType, FunctionBindingError, FunctionResolutionError,
    FunctionResultType, FunctionVolatility, ResolvedAggregateSignature, ResolvedFunctionBinding,
    ResolvedFunctionSignature,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionKind, FunctionValueType,
    PureCompileControl, ValueLogicalType,
};
use novarocks_types::schema::{ColumnDef, SqlType};

use crate::analysis::{
    ExprKind, QueryBody, ResolvedQuery, TypedExpr, WindowBound, WindowFrameType,
};
use crate::analyze_error::{AnalyzeError, AnalyzeErrorKind};
use crate::catalog::PlannerTableProvider;
use crate::compiler::SqlFunctionCatalog;

struct SourceCatalog {
    offset_nullable: bool,
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
        let column = |name: &str, data_type, nullable, logical_type| ColumnDef {
            name: name.into(),
            data_type,
            nullable,
            logical_type,
            write_default: None,
        };
        let nested = DataType::LargeList(Arc::new(
            Field::new("actual_item", DataType::Utf8, false).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "provider".into(),
                    "original".into(),
                )]),
            ),
        ));
        let mut columns = vec![
            column("v", DataType::Int64, false, None),
            column("off", DataType::Int32, self.offset_nullable, None),
            column("off16", DataType::Int16, false, None),
            column("off64", DataType::Int64, self.offset_nullable, None),
            column("d", DataType::Int32, true, None),
            column("text", DataType::Utf8, true, None),
            column("j", DataType::Utf8, true, Some(SqlType::Json)),
            column("nested", nested, true, None),
        ];
        if table == "wide_source" {
            let fields = (0..320)
                .map(|ordinal| {
                    Field::new(
                        format!("authored_{ordinal}"),
                        DataType::Int64,
                        ordinal % 2 == 0,
                    )
                    .with_metadata(
                        std::collections::HashMap::<String, String>::from([(
                            "provider".into(),
                            format!("source-{ordinal}"),
                        )]),
                    )
                })
                .collect::<Vec<_>>();
            columns.push(column("wide", DataType::Struct(fields.into()), true, None));
        }
        let planner = TableDef {
            name: table.into(),
            columns,
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::Sql(SqlScanSource::new(
                SqlTableBindingId::new(
                    SqlTableBindingScopeId::new(NonZeroU64::new(829).unwrap()),
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
    functions: &dyn SqlFunctionCatalog,
) -> Result<ResolvedQuery, AnalyzeError> {
    let query = &novarocks_parser::parser::parse_query(sql).unwrap();
    super::analyze_with_function_catalog_and_sql_semantics(
        query,
        source,
        "default",
        functions,
        &crate::sql_mode::SqlSemanticSettings::default(),
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .map(|result| result.0)
}
fn projection(query: &ResolvedQuery, index: usize) -> &TypedExpr {
    let QueryBody::Select(select) = &query.body else {
        panic!("select");
    };
    &select.projection[index].expr
}
fn args(expr: &TypedExpr) -> &[TypedExpr] {
    let ExprKind::WindowCall {
        args,
        binding,
        aggregate_binding,
        ..
    } = &expr.kind
    else {
        panic!("actual selected window");
    };
    assert!(aggregate_binding.is_none());
    assert_eq!(binding.kind, FunctionKind::Window);
    assert_eq!(args.len(), binding.selected.argument_types.len());
    for (arg, selected) in args.iter().zip(binding.selected.argument_types.iter()) {
        assert_eq!(
            selected,
            &FunctionArgumentType::Value(arg.value_type.clone())
        );
    }
    assert_eq!(
        binding.selected.result_type,
        FunctionResultType::Scalar(expr.value_type.clone())
    );
    args
}
fn source_column(expr: &TypedExpr) -> (&crate::column_id::ColumnId, &str) {
    let ExprKind::ColumnRef {
        column_id, column, ..
    } = &expr.kind
    else {
        panic!("source column");
    };
    (column_id, column)
}

#[test]
fn window_canonical_lead_lag_offsets_preserve_original_sources_and_policy() {
    for nullable in [false, true] {
        for mode in ["32", "ERROR_IF_OVERFLOW"] {
            let sql = format!(
                "WITH SESSION sql_mode='{mode}' SELECT lead(v,off64) OVER (PARTITION BY text ORDER BY v), lag(v,off64) OVER (ORDER BY v), off64, off64 FROM source"
            );
            let query = analyze(
                &sql,
                &SourceCatalog {
                    offset_nullable: nullable,
                },
                crate::functions::builtin_sql_function_catalog(),
            )
            .unwrap();
            for (index, name) in [(0, "lead"), (1, "lag")] {
                let expr = projection(&query, index);
                let ExprKind::WindowCall {
                    name: actual,
                    binding,
                    partition_by,
                    order_by,
                    ..
                } = &expr.kind
                else {
                    panic!("window");
                };
                assert_eq!(actual, name);
                assert_eq!(
                    binding.function_id.as_str(),
                    format!("builtin.window/{name}/v1")
                );
                let policy = if mode == "32" {
                    DecimalOverflowPolicy::OutputNull
                } else {
                    DecimalOverflowPolicy::ReportError
                };
                assert_eq!(binding.decimal_overflow_policy(), policy);
                let args = args(expr);
                assert_eq!(
                    args[0].value_type,
                    FunctionValueType::new(DataType::Int64, false)
                );
                assert_eq!(
                    args[1].value_type,
                    FunctionValueType::new(DataType::Int64, nullable)
                );
                // The built-in binder admits exact I64 offsets; no widening
                // capability is inferred from the helper's coercion stage.
                assert!(matches!(args[1].kind, ExprKind::ColumnRef { .. }));
                assert_eq!(
                    source_column(&args[1]),
                    source_column(projection(&query, index + 2))
                );
                assert_eq!(partition_by.len(), usize::from(index == 0));
                assert_eq!(order_by.len(), 1);
                assert_eq!(
                    expr.value_type,
                    FunctionValueType::new(DataType::Int64, true)
                );
                assert_eq!(query.output_columns[index].value_type, expr.value_type);
            }
        }
    }
}

#[test]
fn window_canonical_default_parameter_keeps_common_domain_and_original_refusal() {
    let source = SourceCatalog {
        offset_nullable: false,
    };
    let functions = crate::functions::builtin_sql_function_catalog();
    let query = analyze("SELECT lead(v,off64,d) OVER (ORDER BY v), lag(v,off64,NULL) OVER (ORDER BY v), lag(v,off64,CAST(NULL AS BIGINT)) OVER (ORDER BY v), d FROM source", &source, functions).unwrap();
    let default = &args(projection(&query, 0))[2];
    assert_eq!(
        default.value_type,
        FunctionValueType::new(DataType::Int64, true)
    );
    let ExprKind::Cast { expr, .. } = &default.kind else {
        panic!("INTEGER default must be widened to BIGINT");
    };
    assert_eq!(source_column(expr), source_column(projection(&query, 3)));
    for index in [1, 2] {
        let default = &args(projection(&query, index))[2];
        assert_eq!(
            default.value_type,
            FunctionValueType::new(DataType::Int64, true)
        );
    }
    let error = analyze(
        "SELECT lead(text,off64,v) OVER (ORDER BY v) FROM source",
        &source,
        functions,
    )
    .unwrap_err();
    assert_eq!(error.kind(), AnalyzeErrorKind::TypeMismatch);
    assert!(error.span().is_some());
    assert_eq!(error.control_error(), None);
}

#[test]
fn window_canonical_arithmetic_default_retains_the_selected_shape_and_refusals() {
    use novarocks_type_contract::OperatorFunction;

    let source = SourceCatalog {
        offset_nullable: false,
    };
    let functions = crate::functions::builtin_sql_function_catalog();
    let query = analyze(
        "SELECT lag(d, 5, 1+2+3) OVER (ORDER BY v) FROM source",
        &source,
        functions,
    )
    .unwrap();
    let default = &args(projection(&query, 0))[2];
    assert_eq!(
        default.value_type,
        FunctionValueType::new(DataType::Int32, false)
    );
    let (outer, [left, _]) =
        crate::common::binary_operator::typed_operands(default).expect("outer default addition");
    assert_eq!(outer, OperatorFunction::Add);
    let (inner, _) =
        crate::common::binary_operator::typed_operands(left).expect("inner default addition");
    assert_eq!(inner, OperatorFunction::Add);

    for sql in [
        "SELECT lag(d, 5, abs(1)+2) OVER (ORDER BY v) FROM source",
        "SELECT lag(d, 5, d+1) OVER (ORDER BY v) FROM source",
        "SELECT lag(CAST(v AS REAL), 5, 1+2+3) OVER (ORDER BY v) FROM source",
    ] {
        analyze(sql, &source, functions).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let error = analyze(
        "SELECT lag(d, 5, abs(1)) OVER (ORDER BY missing) FROM source",
        &source,
        functions,
    )
    .unwrap_err();
    assert_eq!(error.kind(), AnalyzeErrorKind::UnknownColumn);
}

#[test]
fn window_canonical_first_last_normalization_keeps_complete_selected_result() {
    let query = analyze("SELECT first_value(j) IGNORE NULLS OVER (ORDER BY v ASC NULLS LAST ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), last_value(nested) OVER (ORDER BY v DESC NULLS FIRST ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), nested FROM source",
        &SourceCatalog { offset_nullable: false }, crate::functions::builtin_sql_function_catalog()).unwrap();
    for (index, expected_name, expected_asc, expected_nulls) in [
        (0, "last_value", false, true),
        (1, "first_value", true, false),
    ] {
        let expr = projection(&query, index);
        let ExprKind::WindowCall {
            name,
            binding,
            order_by,
            window_frame,
            ignore_nulls,
            ..
        } = &expr.kind
        else {
            panic!("window");
        };
        assert_eq!(name, expected_name);
        assert_eq!(
            binding.function_id.as_str(),
            format!("builtin.window/{expected_name}/v1")
        );
        assert_eq!(*ignore_nulls, index == 0);
        assert_eq!(order_by.len(), 1);
        assert_eq!(order_by[0].asc, expected_asc);
        assert_eq!(order_by[0].nulls_first, expected_nulls);
        let frame = window_frame.as_ref().unwrap();
        assert_eq!(frame.frame_type, WindowFrameType::Rows);
        assert_eq!(frame.start, WindowBound::UnboundedPreceding);
        assert_eq!(frame.end, WindowBound::CurrentRow);
        let child = &args(expr)[0];
        assert!(matches!(child.kind, ExprKind::ColumnRef { .. }));
        assert_eq!(child.value_type, expr.value_type);
        assert_eq!(query.output_columns[index].value_type, expr.value_type);
    }
    assert_eq!(
        projection(&query, 0).value_type.logical_type(),
        ValueLogicalType::Json
    );
    assert_eq!(
        projection(&query, 1).value_type,
        projection(&query, 2).value_type
    );
    let DataType::LargeList(item) = &projection(&query, 1).value_type.data_type() else {
        panic!("source LargeList");
    };
    assert_eq!(item.name(), "item");
    assert!(!item.is_nullable());
    assert!(item.metadata().as_arc().is_none());
}

#[derive(Default)]
struct Trace {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Trace {
    fn recorded(&self) -> Vec<(CompilePhase, u32)> {
        self.calls.lock().unwrap().clone()
    }
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        calls.push((phase, units));
        if let Some((index, cause)) = self.refusal.clone()
            && at == index
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn bind(
    functions: &dyn SqlFunctionCatalog,
    name: &str,
    arguments: Vec<TypedExpr>,
    control: &dyn PureCompileControl,
) -> Result<(Vec<TypedExpr>, crate::binding::SqlFunctionBinding), AnalyzeError> {
    super::resolve_expr::bind_window_function_call_with_catalog(
        functions,
        name,
        arguments,
        novarocks_parser::Span::new(7, 31),
        DecimalOverflowPolicy::ReportError,
        crate::constant::test_constant_policy(),
        control,
    )
}

#[test]
fn window_canonical_sole_helper_keeps_success_and_ordinary_control_prefixes() {
    // The whole analyzer's control is concrete. Exercise its same production
    // window helper with real analyzer-authored source expressions instead.
    let query = analyze(
        "SELECT v,NULL,d FROM source",
        &SourceCatalog {
            offset_nullable: true,
        },
        crate::functions::builtin_sql_function_catalog(),
    )
    .unwrap();
    for (name, count, succeeds) in [("lead", 2, true), ("lag", 3, true), ("lead", 0, false)] {
        let arguments = (0..count)
            .map(|index| projection(&query, index).clone())
            .collect::<Vec<_>>();
        let control = Trace::default();
        let result = bind(
            crate::functions::builtin_sql_function_catalog(),
            name,
            arguments.clone(),
            &control,
        );
        assert_eq!(result.is_ok(), succeeds);
        if let Ok((actual, binding)) = result {
            assert_eq!(
                actual[1].value_type,
                FunctionValueType::new(DataType::Int64, true)
            );
            // typed_null_conversion preserves genuine NULL syntax while
            // authoring the selected exact I64(true) type; it is not a Cast.
            assert!(matches!(
                actual[1].kind,
                ExprKind::Literal(crate::analysis::LiteralValue::Null)
            ));
            assert_eq!(
                projection(&query, 1).value_type,
                FunctionValueType::new(DataType::Null, true)
            );
            assert_eq!(binding.kind, FunctionKind::Window);
            assert_eq!(
                binding.decimal_overflow_policy(),
                DecimalOverflowPolicy::ReportError
            );
            if count == 3 {
                assert_eq!(
                    actual[2].value_type,
                    FunctionValueType::new(DataType::Int32, true)
                );
            }
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.kind(), AnalyzeErrorKind::TypeMismatch);
            assert_eq!(error.span(), Some(novarocks_parser::Span::new(7, 31)));
            assert!(
                error
                    .message()
                    .starts_with("cannot bind window function `lead`")
            );
            assert_eq!(error.control_error(), None);
        }
        let trace = control.recorded();
        assert!(trace.len() >= 2);
        assert_eq!(
            trace.last().unwrap().0,
            CompilePhase::FunctionSpecialization
        );
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Trace {
                    refusal: Some((at, cause.clone())),
                    ..Trace::default()
                };
                let error = bind(
                    crate::functions::builtin_sql_function_catalog(),
                    name,
                    arguments.clone(),
                    &control,
                )
                .unwrap_err();
                assert_eq!(error.control_error(), Some(cause));
                assert_eq!(error.span(), None);
                assert_eq!(control.recorded(), trace[..=at]);
            }
        }
    }
}

#[derive(Clone, Debug)]
enum WindowFault {
    Control(usize, CompileControlError),
    StaleRebound,
    ReboundArgument,
    ReboundArity,
}
#[derive(Clone, Debug)]
struct FaultCatalog {
    fault: WindowFault,
    windows: Arc<AtomicUsize>,
}
impl FaultCatalog {
    fn new(fault: WindowFault) -> Self {
        Self {
            fault,
            windows: Arc::new(AtomicUsize::new(0)),
        }
    }
}
impl SqlFunctionCatalog for FaultCatalog {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(self.clone())
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        crate::functions::builtin_sql_function_catalog()
            .resolve_scalar_signature(name, args, control)
    }
    fn resolve_scalar_binding(
        &self,
        name: &str,
        args: &[FunctionArgument],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        crate::functions::builtin_sql_function_catalog().resolve_scalar_binding(name, args, control)
    }
    fn resolve_value_conversion_binding(
        &self,
        arg: &FunctionArgument,
        target: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        crate::functions::builtin_sql_function_catalog()
            .resolve_value_conversion_binding(arg, target, control)
    }
    fn resolve_window_binding(
        &self,
        name: &str,
        args: &[FunctionArgument],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        let at = self.windows.fetch_add(1, Ordering::SeqCst);
        if let WindowFault::Control(index, cause) = self.fault.clone()
            && at == index
        {
            // A retry delegates successfully, exposing a swallowed refusal.
            return Err(FunctionBindingError::Control(cause));
        }
        let mut binding = crate::functions::builtin_sql_function_catalog()
            .resolve_window_binding(name, args, control)?;
        if at == 1 {
            match self.fault {
                WindowFault::StaleRebound => {
                    binding.function_id =
                        novarocks_functions::FunctionId::try_new("test.window/stale-rebound/v1")
                            .unwrap();
                }
                WindowFault::ReboundArgument => {
                    // Preserve identity and overload, but contradict the actual
                    // already-coerced offset's complete value type.
                    let mut arguments = binding.selected.argument_types.to_vec();
                    arguments[1] =
                        FunctionArgumentType::Value(FunctionValueType::new(DataType::Int32, false));
                    binding.selected.argument_types =
                        novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                            arguments.into_boxed_slice(),
                        );
                }
                WindowFault::ReboundArity => {
                    binding.selected.argument_types =
                        novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                            binding.selected.argument_types[..1]
                                .to_vec()
                                .into_boxed_slice(),
                        );
                }
                WindowFault::Control(..) => {}
            }
        }
        Ok(binding)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        crate::functions::builtin_sql_function_catalog().contains_aggregate(name)
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        crate::functions::builtin_sql_function_catalog()
            .resolve_aggregate_signature(name, args, control)
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        crate::functions::builtin_sql_function_catalog()
            .resolve_aggregate_trusted(name, args, control)
    }
    fn volatility(&self, name: &str) -> FunctionVolatility {
        crate::functions::builtin_sql_function_catalog().volatility(name)
    }
}

#[test]
fn window_canonical_exact_rebind_and_catalog_first_control_cause_never_retry() {
    for at in [0, 1] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let functions = FaultCatalog::new(WindowFault::Control(at, cause.clone()));
            let error = analyze(
                "SELECT lead(v,off64) OVER (ORDER BY v) FROM source",
                &SourceCatalog {
                    offset_nullable: false,
                },
                &functions,
            )
            .unwrap_err();
            assert_eq!(error.control_error(), Some(cause));
            assert_eq!(functions.windows.load(Ordering::SeqCst), at + 1);
        }
    }
    for fault in [
        WindowFault::StaleRebound,
        WindowFault::ReboundArgument,
        WindowFault::ReboundArity,
    ] {
        let functions = FaultCatalog::new(fault);
        let error = analyze(
            "SELECT lead(v,off64) OVER (ORDER BY v) FROM source",
            &SourceCatalog {
                offset_nullable: false,
            },
            &functions,
        )
        .unwrap_err();
        assert_eq!(error.kind(), AnalyzeErrorKind::TypeMismatch);
        assert!(error.span().is_some());
        assert_eq!(error.control_error(), None);
        assert_eq!(functions.windows.load(Ordering::SeqCst), 2);
    }
}

#[test]
fn window_builtin_narrow_offsets_keep_original_columns_and_nullable_canonical_casts() {
    for (name, offset, source_type, nullable) in [
        ("lead", "off", DataType::Int32, true),
        ("lag", "off16", DataType::Int16, false),
    ] {
        let query = analyze(
            &format!("SELECT {name}(v,{offset}) OVER (ORDER BY v) FROM source"),
            &SourceCatalog {
                offset_nullable: true,
            },
            crate::functions::builtin_sql_function_catalog(),
        )
        .unwrap();
        let expr = projection(&query, 0);
        let arguments = args(expr);
        assert_eq!(
            arguments[1].value_type,
            FunctionValueType::new(DataType::Int64, nullable)
        );
        let ExprKind::Cast {
            expr: source,
            target,
            ..
        } = &arguments[1].kind
        else {
            panic!("canonical column offset conversion");
        };
        assert_eq!(target, &DataType::Int64);
        assert_eq!(
            source.value_type,
            FunctionValueType::new(source_type, nullable)
        );
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &source.kind
        else {
            panic!("original offset column");
        };
        assert_eq!(column, offset);
        let QueryBody::Select(select) = &query.body else {
            panic!("select");
        };
        let Some(crate::analysis::Relation::Scan(scan)) = &select.from else {
            panic!("original scan");
        };
        let ordinal = scan
            .table
            .columns
            .iter()
            .position(|column| column.name == offset)
            .unwrap();
        assert_eq!(*column_id, scan.column_ids[ordinal]);
        assert_eq!(query.output_columns[0].value_type, expr.value_type);
    }
}

#[test]
fn window_builtin_bare_null_offset_selects_and_materializes_exact_i64_domain() {
    let query = analyze(
        "SELECT lead(v,NULL) OVER (ORDER BY v), lag(v,NULL) OVER (ORDER BY v) FROM source",
        &SourceCatalog {
            offset_nullable: false,
        },
        crate::functions::builtin_sql_function_catalog(),
    )
    .unwrap();
    for index in 0..2 {
        let expr = projection(&query, index);
        let arguments = args(expr);
        assert_eq!(
            arguments[1].value_type,
            FunctionValueType::new(DataType::Int64, true)
        );
        assert!(matches!(
            arguments[1].kind,
            ExprKind::Literal(crate::analysis::LiteralValue::Null)
        ));
        assert_eq!(
            expr.value_type,
            FunctionValueType::new(DataType::Int64, true)
        );
        assert_eq!(query.output_columns[index].value_type, expr.value_type);
    }
    // Check the initial declaration separately from the helper's final AST:
    // the actual built-in binding chooses I64(true) for a genuine Null source.
    let source = analyze(
        "SELECT v,NULL FROM source",
        &SourceCatalog {
            offset_nullable: false,
        },
        crate::functions::builtin_sql_function_catalog(),
    )
    .unwrap();
    let original = [projection(&source, 0), projection(&source, 1)]
        .into_iter()
        .map(|expr| {
            crate::analysis::function_argument(
                expr,
                crate::constant::test_constant_policy(),
                &Trace::default(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let selected = crate::functions::builtin_sql_function_catalog()
        .resolve_window_binding("lead", &original, &Trace::default())
        .unwrap();
    assert_eq!(
        selected.selected.argument_types[1],
        FunctionArgumentType::Value(FunctionValueType::new(DataType::Int64, true))
    );
    assert_eq!(
        projection(&source, 1).value_type,
        FunctionValueType::new(DataType::Null, true)
    );
}

#[test]
fn window_wide_rebound_gate_observes_original_nested_source_and_terminal_control() {
    let source = analyze(
        "SELECT wide FROM wide_source",
        &SourceCatalog {
            offset_nullable: false,
        },
        crate::functions::builtin_sql_function_catalog(),
    )
    .unwrap();
    let original = projection(&source, 0);
    let DataType::Struct(fields) = &original.value_type.data_type() else {
        panic!("actual provider Struct");
    };
    assert_eq!(fields.len(), 320);
    for (ordinal, field) in fields.iter().enumerate() {
        assert_eq!(field.name(), &format!("authored_{ordinal}"));
        assert_eq!(field.is_nullable(), ordinal % 2 == 0);
        assert!(field.metadata().as_arc().is_none());
    }
    for name in ["first_value", "last_value"] {
        let control = Trace::default();
        let (actual, binding) = bind(
            crate::functions::builtin_sql_function_catalog(),
            name,
            vec![original.clone()],
            &control,
        )
        .unwrap();
        assert_eq!(actual[0].value_type, original.value_type);
        assert_eq!(source_column(&actual[0]), source_column(original));
        assert_eq!(
            binding.selected.argument_types[0],
            FunctionArgumentType::Value(original.value_type.clone())
        );
        assert_eq!(
            binding.selected.result_type,
            FunctionResultType::Scalar(original.value_type.clone())
        );
        let trace = control.recorded();

        // The final zero-unit entry is the actual shared rebound gate's
        // scope. Earlier selection/shape/conversion scopes remain in the
        // full trace and every refusal prefix below; no synthetic two-call
        // prefix is assumed as shared gates evolve.
        assert!(trace.last().unwrap().1 > 0);
        let gate_entry = trace
            .iter()
            .rposition(|(phase, units)| {
                *phase == CompilePhase::FunctionSpecialization && *units == 0
            })
            .expect("actual final shared gate entry");
        let gate = &trace[gate_entry..];
        assert_eq!(
            gate.first(),
            Some(&(CompilePhase::FunctionSpecialization, 0))
        );
        assert!(gate.iter().any(|(_, units)| *units == 256));
        assert!(
            gate.last().unwrap().1 > 0,
            "positive completed exact-FVT tail before publication"
        );
        assert!(
            trace.iter().all(
                |(phase, units)| *phase == CompilePhase::FunctionSpecialization && *units <= 256
            )
        );
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refused = Trace {
                    refusal: Some((at, cause.clone())),
                    ..Trace::default()
                };
                let error = bind(
                    crate::functions::builtin_sql_function_catalog(),
                    name,
                    vec![original.clone()],
                    &refused,
                )
                .unwrap_err();
                assert_eq!(error.control_error(), Some(cause));
                assert_eq!(error.span(), None);
                assert_eq!(refused.recorded(), trace[..=at]);
            }
        }
    }
}

#[test]
fn complete_window_metadata_refuses_before_missing_names_or_catalog_lookup() {
    use novarocks_parser::{ast::*, build};
    let functions = crate::compiler::build_builtin_engine_function_catalog().unwrap();
    let source = crate::planning::catalog::PlannerMemoryCatalog::default();
    let control = crate::compiler::SqlCompileControl::unbounded();
    for profile in 0..11 {
        let sql = if profile >= 8 {
            "SELECT row_number() OVER (ROWS CURRENT ROW)"
        } else {
            "SELECT missing_function(missing_argument) OVER (ROWS CURRENT ROW) FROM missing_table"
        };
        let query = &novarocks_parser::parser::parse_query(sql).unwrap();
        let mut query = query.clone();
        let span = query.span;
        let SetExpr::Select(select) = query.body.as_mut() else {
            panic!("select");
        };
        let SelectItem::SingleColumn(projection_column) = &mut select.projection[0] else {
            panic!("expression");
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let expression = &mut projection_column.expression;
        let Expr::FunctionCall(call) = expression else {
            panic!("function");
        };
        let mut window = call.window.take().unwrap();
        let ident = |name| build::ident_at(name, false, span);
        if profile == 8 {
            window = Window::Reference(WindowReference {
                name: ident("missing_window"),
                span,
            });
        } else {
            let Window::Specification(specification) = &mut window else {
                panic!("specification");
            };
            if profile == 9 {
                specification.existing_window_name = Some(ident("missing_inherited_window"));
            } else {
                let frame = specification.window_frame.as_mut().unwrap();
                match profile {
                    0 => frame.exclusion = WindowFrameExclusion::CurrentRow,
                    1 => {
                        frame.measures = vec![MeasureDefinition {
                            expression: build::identifier(ident("missing_measure")),
                            name: ident("m"),
                            span,
                        }]
                        .into()
                    }
                    2 => frame.after_match_skip_to = Some(SkipTo::Next { span }),
                    3 => {
                        frame.pattern_search_mode = Some(PatternSearchMode {
                            mode: PatternSearchModeKind::Seek,
                            span,
                        })
                    }
                    4 => {
                        frame.pattern = Some(RowPattern::Variable {
                            name: ident("A"),
                            span,
                        })
                    }
                    5 => {
                        frame.subsets = vec![SubsetDefinition {
                            name: ident("U"),
                            identifiers: vec![ident("A")],
                            span,
                        }]
                        .into()
                    }
                    6 => {
                        frame.variable_definitions = vec![VariableDefinition {
                            name: ident("A"),
                            expression: build::identifier(ident("missing_definition")),
                            span,
                        }]
                        .into()
                    }
                    7 => {}
                    10 => frame.units = WindowFrameUnits::Groups,
                    _ => unreachable!("bounded metadata profiles"),
                }
            }
        }
        call.window = Some(window);
        if profile == 7 {
            let window = call.window.take().unwrap();
            *expression = Expr::WindowOperation(AstBox::new(WindowOperation {
                name: ident("missing_measure"),
                window,
                span,
            }));
        }
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
            crate::analyze_error::AnalyzeErrorKind::UnsupportedExpression,
            "profile {profile}: {error}"
        );
        let message = error.to_string();
        let expected = match profile {
            0 => "EXCLUDE",
            1..=6 => "pattern recognition",
            7 => "window operations",
            8 | 9 => "named window references",
            10 => "GROUPS",
            _ => unreachable!(),
        };
        assert!(message.contains(expected), "profile {profile}: {message}");
    }
}

#[test]
fn original_typeof_cast_window_metadata_surface_keeps_its_declared_type() {
    let source = SourceCatalog {
        offset_nullable: false,
    };
    let functions = crate::compiler::build_builtin_engine_function_catalog().unwrap();
    for sql in [
        "SELECT typeof(CAST(row_number() OVER missing_window AS INT))",
        "SELECT typeof(CAST(row_number() OVER (missing_window) AS INT))",
        "SELECT typeof(CAST(row_number() OVER (GROUPS CURRENT ROW) AS INT))",
    ] {
        let resolved = analyze(sql, &source, &functions).unwrap();
        assert!(
            matches!(&projection(&resolved, 0).kind,
            ExprKind::Literal(crate::analysis::LiteralValue::String(value)) if value == "int"),
            "{sql}"
        );
    }
}
