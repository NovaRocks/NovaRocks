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

use arrow::datatypes::DataType;
use novarocks_functions::{ConstantPolicy, FunctionArgumentType, FunctionResultType};
use novarocks_parser::ast;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType,
    PureCompileControl, TemporalPrecision, TemporalType,
};
use novarocks_types::schema::{ColumnDef, SqlType};

use crate::analysis::{ExprKind, QueryBody, ResolvedQuery, TypedExpr};
use crate::analyze_error::{AnalyzeError, AnalyzeErrorKind};
use crate::catalog::PlannerTableProvider;

struct TemporalCatalog {
    carrier: DataType,
    logical: Option<SqlType>,
    nullable: bool,
}

impl PlannerTableProvider for TemporalCatalog {
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
    source: &TemporalCatalog,
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

fn assert_selected(expr: &TypedExpr, name: &str, expected: &FunctionValueType) {
    let (actual_name, binding, args) = call(expr);
    assert_eq!(actual_name, name);
    assert_eq!(
        binding.function_id.as_str(),
        format!("builtin.scalar/{name}/v1")
    );
    assert_eq!(args.len(), 1);
    assert_eq!(
        binding.selected.argument_types.as_ref(),
        &[FunctionArgumentType::Value(expected.clone())]
    );
    assert_eq!(&args[0].value_type, expected);
    let result = FunctionValueType::new(DataType::Int64, expected.nullable());
    assert_eq!(
        binding.selected.result_type,
        FunctionResultType::Scalar(result.clone())
    );
    assert_eq!(expr.value_type, result);
}

#[test]
fn extract_binding_retains_exact_temporal_types_and_matches_named_calls() {
    let mut sources = vec![(DataType::Date32, None)];
    for precision in [0, 3, 6, 9, 12] {
        let precision = TemporalPrecision::new(precision).unwrap();
        for temporal in [
            TemporalType::Timestamp(precision),
            TemporalType::TimestampWithTimeZone(precision),
        ] {
            sources.push((temporal.carrier(), Some(SqlType::Temporal(temporal))));
        }
    }
    for (carrier, logical) in sources {
        for nullable in [false, true] {
            let catalog = TemporalCatalog {
                carrier: carrier.clone(),
                logical: logical.clone(),
                nullable,
            };
            for (field, name) in [
                ("YEAR", "year"),
                ("MONTH", "month"),
                ("DAY", "day"),
                ("QUARTER", "quarter"),
                ("WEEK", "week"),
                ("DAY_OF_WEEK", "day_of_week"),
            ] {
                let sql =
                    format!("SELECT x, EXTRACT({field} FROM x), {name}(x) FROM temporal_source");
                let query =
                    analyze(&sql, &catalog, crate::constant::test_constant_policy()).unwrap();
                let original = &project(&query, 0).value_type;
                assert_eq!(original.data_type(), &carrier);
                assert_eq!(original.nullable(), nullable);
                for ordinal in [1, 2] {
                    let expression = project(&query, ordinal);
                    assert_selected(expression, name, original);
                    assert!(matches!(
                        call(expression).2[0].kind,
                        ExprKind::ColumnRef { .. }
                    ));
                }
            }
        }
    }
}

#[test]
fn extract_binding_rejects_non_temporal_carriers_and_invalid_fields_at_original_span() {
    for logical in [None, Some(SqlType::Uuid)] {
        let catalog = TemporalCatalog {
            carrier: DataType::FixedSizeBinary(16),
            logical,
            nullable: false,
        };
        for sql in [
            "SELECT EXTRACT(YEAR FROM x) FROM temporal_source",
            "SELECT year(x) FROM temporal_source",
        ] {
            let error =
                analyze(sql, &catalog, crate::constant::test_constant_policy()).unwrap_err();
            let expected = if sql.contains("EXTRACT") {
                AnalyzeErrorKind::TypeMismatch
            } else {
                AnalyzeErrorKind::UnknownFunction
            };
            assert_eq!(error.kind(), expected);
            assert!(error.control_error().is_none());
        }
    }
    let catalog = TemporalCatalog {
        carrier: DataType::Date32,
        logical: None,
        nullable: false,
    };
    let sql = "SELECT EXTRACT(HOUR FROM x) FROM temporal_source";
    let query = &novarocks_parser::parser::parse_query(sql).unwrap();
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("select");
    };
    let ast::SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
        panic!("extract");
    };
    assert!(projection_column.alias.is_none(), "anonymous projection");
    let ast::Expr::Extract(extract) = &projection_column.expression else {
        panic!("extract");
    };
    let error = analyze(sql, &catalog, crate::constant::test_constant_policy()).unwrap_err();
    assert_eq!(error.kind(), AnalyzeErrorKind::TypeMismatch);
    assert_eq!(error.span(), Some(extract.span));
}

#[test]
fn extract_binding_inherits_root_policy_null_sources_and_explicit_constant_admission() {
    use DecimalOverflowPolicy::{OutputNull, ReportError};
    let temporal = TemporalType::TimestampWithTimeZone(TemporalPrecision::WHOLE_SECONDS);
    let catalog = TemporalCatalog {
        carrier: temporal.carrier(),
        logical: Some(SqlType::Temporal(temporal)),
        nullable: false,
    };
    for (outer, outer_policy) in [("ERROR_IF_OVERFLOW", ReportError), ("32", OutputNull)] {
        let sql = format!(
            "WITH SESSION sql_mode = '{outer}' SELECT EXTRACT(YEAR FROM q.x), year(q.x) FROM (SELECT x, EXTRACT(MONTH FROM x) AS m, month(x) AS n FROM temporal_source) q"
        );
        // The admitted root policy also governs expressions in the derived query.
        let query = analyze(&sql, &catalog, crate::constant::test_constant_policy()).unwrap();
        for ordinal in [0, 1] {
            assert_eq!(
                call(project(&query, ordinal)).1.decimal_overflow_policy(),
                outer_policy
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
        for ordinal in [1, 2] {
            assert_eq!(
                call(project(inner_query, ordinal))
                    .1
                    .decimal_overflow_policy(),
                outer_policy
            );
        }
        for (query, ordinals) in [(&query, [0, 1]), (inner_query.as_ref(), [1, 2])] {
            for ordinal in ordinals {
                let argument = &call(project(query, ordinal)).2[0];
                assert!(matches!(argument.kind, ExprKind::ColumnRef { .. }));
                assert_eq!(argument.value_type.data_type(), &catalog.carrier);
            }
        }
    }
    let query = analyze(
        "SELECT EXTRACT(YEAR FROM CAST(NULL AS DATE)), year(CAST(NULL AS DATE))",
        &catalog,
        crate::constant::test_constant_policy(),
    )
    .unwrap();
    let (.., special_args) = call(project(&query, 0));
    let (.., ordinary_args) = call(project(&query, 1));
    assert_eq!(
        call(project(&query, 0)).1.selected,
        call(project(&query, 1)).1.selected
    );
    assert!(special_args[0].value_type.nullable());
    assert_eq!(special_args[0].value_type, ordinary_args[0].value_type);
    // Literal admission must consume the supplied policy, not a helper default.
    let mut policy = crate::constant::test_constant_policy();
    policy.max_rows = 0;
    for sql in [
        "SELECT EXTRACT(YEAR FROM DATE '2024-02-29')",
        "SELECT year(DATE '2024-02-29')",
    ] {
        let error = analyze(sql, &catalog, policy).unwrap_err();
        assert!(error.control_error().is_none());
        let source = error
            .source_failure()
            .expect("retain the actual constant author");
        assert!(matches!(
            source.downcast_ref::<novarocks_functions::FunctionBindingError>(),
            Some(novarocks_functions::FunctionBindingError::Constant(
                novarocks_constant_contract::ConstantError::Limit(
                    "constant factory row limit exceeded"
                )
            ))
        ));
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(
            source.failure_fact().class(),
            novarocks_failure_contract::FailureClass::Resource
        );
        assert_eq!(
            source.failure_fact().reason().domain(),
            novarocks_failure_contract::domain::CONSTANT.get()
        );
        assert_eq!(source.failure_fact().reason().code(), 2);
    }
}

#[derive(Default)]
struct Trace {
    callbacks: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut callbacks = self.callbacks.lock().unwrap();
        let ordinal = callbacks.len();
        callbacks.push((phase, units));
        if let Some((at, cause)) = self.refusal.clone()
            && at == ordinal
        {
            return Err(cause);
        }
        Ok(())
    }
}

#[test]
fn extract_binding_sole_canonical_helper_preserves_every_original_control_prefix() {
    // The whole analyzer uses concrete deadline/cancellation control. This
    // trace tests its same production binder with actual analyzed source facts.
    for carrier in [DataType::Date32, DataType::FixedSizeBinary(16)] {
        let catalog = TemporalCatalog {
            carrier: carrier.clone(),
            logical: None,
            nullable: true,
        };
        let query = analyze(
            "SELECT x FROM temporal_source",
            &catalog,
            crate::constant::test_constant_policy(),
        )
        .unwrap();
        let argument = project(&query, 0).clone();
        let good = Trace::default();
        let result = super::resolve_expr::bind_scalar_function_call_with_catalog(
            crate::functions::builtin_sql_function_catalog(),
            "year",
            vec![argument.clone()],
            DecimalOverflowPolicy::ReportError,
            crate::constant::test_constant_policy(),
            &good,
        );
        assert_eq!(result.is_ok(), carrier == DataType::Date32);
        let trace = good.callbacks.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Trace {
                    callbacks: Mutex::new(Vec::new()),
                    refusal: Some((at, cause.clone())),
                };
                let error = match super::resolve_expr::bind_scalar_function_call_with_catalog(
                    crate::functions::builtin_sql_function_catalog(),
                    "year",
                    vec![argument.clone()],
                    DecimalOverflowPolicy::ReportError,
                    crate::constant::test_constant_policy(),
                    &control,
                ) {
                    Ok(_) => panic!("refused binder"),
                    Err(error) => error,
                };
                assert_eq!(error.control_error(), Some(cause.clone()));
                assert_eq!(
                    error
                        .at_type_mismatch(novarocks_parser::Span::new(7, 23))
                        .control_error(),
                    Some(cause)
                );
                assert_eq!(*control.callbacks.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

// This declaration-only fixture proves the analyzer respects the selected
// result owner. It is not an installed calendar CPU implementation.
struct CustomYear;
impl CustomYear {
    const OVERLOAD: &str = "test.scalar/custom_year/0/v1";

    fn select(
        request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<
        novarocks_functions::FunctionBindingSelection,
        novarocks_functions::FunctionBindingError,
    > {
        use novarocks_functions::{
            FunctionArgument, FunctionBindingError, FunctionBindingSelection, FunctionOverloadId,
        };
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            CompilePhase::FunctionSpecialization,
        )?;
        let result = (|| {
            let [FunctionArgument::Value { value_type, .. }] = request.arguments else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            work.step()?;
            if request.logical_argument_count != 1
                || request.expected_result_type.is_some()
                || value_type != &FunctionValueType::new(DataType::Date32, false)
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            Ok(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new(Self::OVERLOAD).unwrap(),
                argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    vec![FunctionArgumentType::Value(value_type.clone())].into_boxed_slice(),
                ),
                result_type: FunctionResultType::Scalar(FunctionValueType::new(
                    DataType::Int64,
                    false,
                )),
                aggregate: None,
            })
        })();
        if result
            .as_ref()
            .err()
            .is_some_and(|error| error.control_error().is_some())
        {
            return result;
        }
        work.finish()?;
        result
    }
}
impl novarocks_functions::FunctionBindingResolver for CustomYear {
    fn resolve(
        &self,
        request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<
        novarocks_functions::FunctionBindingSelection,
        novarocks_functions::FunctionBindingError,
    > {
        Self::select(request, control)
    }

    fn select_at_overload_observed(
        &self,
        overload: &novarocks_functions::FunctionOverloadId,
        request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<
        novarocks_functions::FunctionBindingSelection,
        novarocks_functions::FunctionBindingError,
    > {
        control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
        if overload.as_str() != Self::OVERLOAD {
            return Err(novarocks_functions::FunctionBindingError::NoMatchingOverload);
        }
        Self::select(request, control)
    }

    fn validate_selected(
        &self,
        selected: &novarocks_functions::FunctionBindingSelection,
        request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        if selected == &Self::select(request, control)? {
            Ok(())
        } else {
            Err(novarocks_functions::FunctionBindingError::NoMatchingOverload)
        }
    }
}

#[test]
fn extract_binding_uses_the_exact_scalar_identity_even_when_a_custom_name_exists() {
    use novarocks_functions::{
        EngineFunctionCatalogBuilder, FunctionBindingDeclaration, FunctionDefinition,
        FunctionFailureBehavior, FunctionId, FunctionKind, FunctionOverloadDeclaration,
        FunctionOverloadId, FunctionVisibility, FunctionVolatility,
    };
    use novarocks_type_contract::{
        ArgumentControl, FunctionEffectDeclaration, FunctionInstanceState,
        FunctionIntrinsicRowError, FunctionNullBehavior, ObservableEffects,
    };
    let identity = FunctionId::try_new("test.scalar/custom_year/v1").unwrap();
    let declaration = FunctionBindingDeclaration::try_new_complete(
        identity.clone(),
        FunctionKind::Scalar,
        [FunctionOverloadDeclaration::from_effects(
            FunctionOverloadId::try_new(CustomYear::OVERLOAD).unwrap(),
            "Physical Date32 nonnullable",
            "Physical Int64 nonnullable",
            None,
            FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NoRowError,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::Strict,
                argument_control: ArgumentControl::Eager,
                instance_state: FunctionInstanceState::None,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
        )],
    )
    .unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            FunctionDefinition::try_new_bound(
                "year",
                FunctionVisibility::Public,
                declaration,
                Arc::new(CustomYear),
            )
            .unwrap(),
        )
        .unwrap();
    let functions = builder.seal_bound().unwrap();
    let source = TemporalCatalog {
        carrier: DataType::Date32,
        logical: None,
        nullable: false,
    };
    let analyze_sql = |sql| {
        let query = novarocks_parser::parser::parse_query(sql).unwrap();
        super::analyze_with_function_catalog(
            &query,
            &source,
            "default",
            &functions,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .map(|(query, _)| query)
    };
    let ordinary = analyze_sql("SELECT year(x) FROM temporal_source").unwrap();
    let expression = project(&ordinary, 0);
    let (_, binding, args) = call(expression);
    assert_eq!(binding.function_id, identity);
    assert_eq!(
        args[0].value_type,
        FunctionValueType::new(DataType::Date32, false)
    );
    assert!(matches!(args[0].kind, ExprKind::ColumnRef { .. }));
    let expected = FunctionValueType::new(DataType::Int64, false);
    assert_eq!(expression.value_type, expected);
    assert_eq!(
        binding.selected.result_type,
        FunctionResultType::Scalar(expected)
    );

    let error = analyze_sql("SELECT EXTRACT(YEAR FROM x) FROM temporal_source").unwrap_err();
    assert_eq!(error.kind(), AnalyzeErrorKind::UnknownFunction);
}

#[test]
fn extract_keeps_typeof_cast_hint_without_analyzing_input() {
    let catalog = TemporalCatalog {
        carrier: DataType::Date32,
        logical: None,
        nullable: false,
    };
    let query = analyze(
        "SELECT typeof(CAST(EXTRACT(HOUR FROM must_not_resolve) AS INT))",
        &catalog,
        crate::constant::test_constant_policy(),
    )
    .unwrap();
    assert!(
        matches!(&project(&query, 0).kind, ExprKind::Literal(crate::analysis::LiteralValue::String(value)) if value == "int")
    );
}
