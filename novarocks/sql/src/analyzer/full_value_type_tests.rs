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

use std::cell::RefCell;
use std::num::{NonZeroU32, NonZeroU64};
use std::rc::Rc;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};
use novarocks_parser::ast;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use novarocks_types::schema::{ColumnDef, SqlType};

use super::{helpers, scope::AnalyzerScope};
use crate::analysis::{ExprKind, QueryBody};
use crate::catalog::PlannerTableProvider;
use crate::column_id::ColumnRefFactory;

fn column(name: &str, carrier: DataType, logical: Option<SqlType>) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type: carrier,
        nullable: false,
        write_default: None,
        logical_type: logical,
    }
}

struct SourceCatalog;
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
        let columns = vec![
            column("j", DataType::Utf8, Some(SqlType::Json)),
            column("h", DataType::Binary, Some(SqlType::Hll)),
            column("b", DataType::Binary, Some(SqlType::Bitmap)),
            column("v", DataType::LargeBinary, Some(SqlType::Variant)),
            column("i", DataType::FixedSizeBinary(16), Some(SqlType::LargeInt)),
            column("raw", DataType::FixedSizeBinary(16), None),
            column(
                "ja",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                Some(SqlType::Array(Box::new(SqlType::Json))),
            ),
            column(
                "ja_meta",
                DataType::List(Arc::new(
                    Field::new("source_item", DataType::Utf8, false).with_metadata(
                        [("provider".to_string(), "retained-source".to_string())].into(),
                    ),
                )),
                Some(SqlType::Array(Box::new(SqlType::Json))),
            ),
        ];
        let planner = TableDef {
            name: table.into(),
            columns,
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::Sql(SqlScanSource::new(
                SqlTableBindingId::new(
                    SqlTableBindingScopeId::new(NonZeroU64::new(811).unwrap()),
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

fn analyze(sql: &str) -> crate::analysis::ResolvedQuery {
    let statements = novarocks_parser::parse(sql).unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("expected query");
    };
    super::analyze(query, &SourceCatalog, "default").unwrap().0
}

#[test]
fn catalog_root_identity_survives_projection_alias_derived_cte_and_outer_padding() {
    let expected = [
        ValueLogicalType::Json,
        ValueLogicalType::Hll,
        ValueLogicalType::Bitmap,
        ValueLogicalType::Variant,
        ValueLogicalType::LargeInt,
        ValueLogicalType::Physical,
    ];
    for sql in [
        "SELECT j,h,b,v,i,raw FROM source",
        "SELECT q.j,q.h,q.b,q.v,q.i,q.raw FROM (SELECT j,h,b,v,i,raw FROM source) q",
        "WITH q AS (SELECT j,h,b,v,i,raw FROM source) SELECT j,h,b,v,i,raw FROM q",
        "SELECT r.j,r.h,r.b,r.v,r.i,r.raw FROM source l LEFT JOIN source r ON 1=1",
    ] {
        let resolved = analyze(sql);
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select");
        };
        for ((output, project), logical) in resolved
            .output_columns
            .iter()
            .zip(&select.projection)
            .zip(expected)
        {
            assert_eq!(output.value_type.logical_type(), logical, "{sql}");
            assert_eq!(project.expr.value_type, output.value_type, "{sql}");
            if sql.contains("LEFT JOIN") {
                assert!(output.value_type.nullable());
            }
        }
    }
}

#[test]
fn actual_field_domain_reaches_lambda_parameter_body_and_bound_result() {
    let resolved = analyze("SELECT array_map(x -> x, ja) FROM source");
    let QueryBody::Select(select) = resolved.body else {
        panic!("expected select");
    };
    let expression = &select.projection[0].expr;
    let ExprKind::FunctionCall { args, binding, .. } = &expression.kind else {
        panic!("expected actual call");
    };
    let ExprKind::LambdaFunction { params, body } = &args[0].kind else {
        panic!("expected lambda");
    };
    assert_eq!(params[0].value_type.logical_type(), ValueLogicalType::Json);
    assert_eq!(body.value_type, params[0].value_type);
    let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
    else {
        panic!("expected scalar");
    };
    assert_eq!(&expression.value_type, result);
    let DataType::List(item) = &result.data_type() else {
        panic!("expected list");
    };
    let expected =
        crate::test_value_types::string_list(item.is_nullable(), true, result.nullable());
    assert_eq!(result, &expected);
    assert_eq!(item.metadata().capacity(), 0);
}

#[test]
fn unnest_uses_complete_selected_relation_domain() {
    let resolved = analyze("SELECT u.* FROM source, UNNEST(ja) u");
    assert_eq!(
        resolved.output_columns[0].value_type.logical_type(),
        ValueLogicalType::Json
    );
}

#[test]
fn explicit_largeint_and_typed_null_are_authored_by_syntax_not_carrier() {
    for sql in [
        "SELECT 9223372036854775808",
        "SELECT CAST(NULL AS LARGEINT)",
    ] {
        let resolved = analyze(sql);
        assert_eq!(
            resolved.output_columns[0].value_type.logical_type(),
            ValueLogicalType::LargeInt
        );
    }
    assert_eq!(
        analyze("SELECT raw FROM source").output_columns[0]
            .value_type
            .logical_type(),
        ValueLogicalType::Physical
    );
    assert_eq!(
        analyze("SELECT CAST(NULL AS JSON)").output_columns[0]
            .value_type
            .logical_type(),
        ValueLogicalType::Json
    );
    assert_eq!(
        analyze("SELECT CAST('not JSON' AS JSON)").output_columns[0]
            .value_type
            .logical_type(),
        ValueLogicalType::Physical
    );
}

#[test]
fn empty_array_declares_expected_result_only_to_its_exact_owner() {
    let resolved = analyze("SELECT ARRAY<JSON>[]");
    let QueryBody::Select(select) = resolved.body else {
        panic!("expected select");
    };
    let expression = &select.projection[0].expr;
    assert!(!expression.value_type.nullable());
    let ExprKind::FunctionCall { args, binding, .. } = &expression.kind else {
        panic!("expected direct bound literal owner");
    };
    assert!(args.is_empty());
    let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
    else {
        panic!("expected scalar");
    };
    assert_eq!(&expression.value_type, result);
    let DataType::List(item) = &result.data_type() else {
        panic!("expected list");
    };
    let expected =
        crate::test_value_types::string_list(item.is_nullable(), true, result.nullable());
    assert_eq!(result, &expected);
    assert_eq!(item.metadata().capacity(), 0);
    assert_eq!(item.data_type(), &DataType::Utf8);
}

struct StopAt {
    error: CompileControlError,
    at_entry: bool,
}
impl PureCompileControl for StopAt {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        if self.at_entry || units == 256 {
            Err(self.error)
        } else {
            Ok(())
        }
    }
}

#[test]
fn declared_source_validation_preserves_typed_control_at_entry_and_inside_fields() {
    let source = column(
        "wide",
        DataType::Struct(Fields::from(
            (0..320)
                .map(|i| Field::new(format!("f{i}"), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        )),
        None,
    );
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at_entry in [true, false] {
            let failure =
                helpers::column_value_type(&source, &StopAt { error, at_entry }).unwrap_err();
            assert_eq!(failure.control_error(), Some(error));
        }
    }
    let validated =
        helpers::column_value_type(&source, crate::optimizer::test_optimizer_control()).unwrap();
    assert_eq!(validated.data_type().clone(), source.data_type);
}

#[test]
fn conflicting_catalog_tag_is_rejected_without_carrier_guess() {
    let source = column("j", DataType::FixedSizeBinary(16), Some(SqlType::Json));
    assert!(
        helpers::column_value_type(&source, crate::optimizer::test_optimizer_control()).is_err()
    );
    let source = column("plain", DataType::FixedSizeBinary(16), None);
    assert_eq!(
        helpers::column_value_type(&source, crate::optimizer::test_optimizer_control())
            .unwrap()
            .logical_type(),
        ValueLogicalType::Physical
    );
}

#[test]
fn scope_padding_preserves_source_and_parameter_domains_without_retagging_factory() {
    let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
    let mut scope = AnalyzerScope::new(factory.clone());
    let ty =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let id = scope.add_column(Some("t"), "j", ty.clone());
    scope.mark_all_nullable();
    assert_eq!(
        scope.resolve_value_type(Some("t"), "j").unwrap().1,
        helpers::with_nullability(ty.clone(), true)
    );
    assert_eq!(factory.borrow().value_type(id), Some(&ty));
}

#[test]
fn assignment_common_owner_preserves_complete_same_domain_and_null_sources() {
    let control = crate::optimizer::test_optimizer_control();
    for (carrier, logical) in [
        (DataType::Utf8, ValueLogicalType::Json),
        (DataType::Binary, ValueLogicalType::Hll),
        (DataType::Binary, ValueLogicalType::Bitmap),
        (DataType::LargeBinary, ValueLogicalType::Variant),
        (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
        (DataType::FixedSizeBinary(16), ValueLogicalType::Physical),
    ] {
        let source = FunctionValueType::try_with_logical_type(carrier, false, logical).unwrap();
        let nil = FunctionValueType::new(DataType::Null, true);
        assert_eq!(
            helpers::assignment_common_value_type(&source, &nil, control).unwrap(),
            helpers::with_nullability(source.clone(), true)
        );
        assert_eq!(
            helpers::assignment_common_value_type(&nil, &source, control).unwrap(),
            helpers::with_nullability(source.clone(), true)
        );
        assert_eq!(
            helpers::assignment_common_value_type(&source, &source, control).unwrap(),
            source
        );
    }
    let left = column(
        "nested",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
        Some(SqlType::Array(Box::new(SqlType::Json))),
    );
    let right = column(
        "nested",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        Some(SqlType::Array(Box::new(SqlType::Json))),
    );
    let left = helpers::column_value_type(&left, control).unwrap();
    let right = helpers::column_value_type(&right, control).unwrap();
    let common = helpers::assignment_common_value_type(&left, &right, control).unwrap();
    let DataType::List(item) = &common.data_type() else {
        panic!("expected list");
    };
    assert!(item.is_nullable());
    assert_eq!(
        common,
        crate::test_value_types::string_list(true, true, false)
    );
    assert_eq!(item.metadata().capacity(), 0);
}

#[test]
fn case_union_and_values_use_the_same_authored_source_domains() {
    for sql in [
        "SELECT CASE WHEN TRUE THEN j ELSE NULL END FROM source",
        "SELECT j FROM source UNION ALL SELECT NULL",
        "VALUES (parse_json('{}')),(NULL)",
    ] {
        assert_eq!(
            analyze(sql).output_columns[0].value_type.logical_type(),
            ValueLogicalType::Json,
            "{sql}"
        );
    }
}

#[test]
fn scalar_aggregate_and_window_outputs_are_complete_selected_owner_facts() {
    for sql in [
        "SELECT parse_json('{}')",
        "SELECT array_agg(j) FROM source",
        "SELECT first_value(j) OVER () FROM source",
    ] {
        let query = analyze(sql);
        let QueryBody::Select(select) = query.body else {
            panic!("expected select");
        };
        let expression = &select.projection[0].expr;
        let binding = match &expression.kind {
            ExprKind::FunctionCall { binding, .. } | ExprKind::WindowCall { binding, .. } => {
                binding
            }
            ExprKind::AggregateCall { resolved, .. } => resolved,
            _ => panic!("expected exact bound call"),
        };
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("expected scalar");
        };
        assert_eq!(&expression.value_type, result, "{sql}");
        assert_eq!(&query.output_columns[0].value_type, result, "{sql}");
    }
}

fn projection(sql: &str) -> crate::analysis::TypedExpr {
    let QueryBody::Select(mut select) = analyze(sql).body else {
        panic!("expected select");
    };
    select.projection.remove(0).expr
}

#[test]
fn round_and_truncate_decimal_result_is_the_exact_selected_binding() {
    use novarocks_functions::FunctionResultType;

    for name in ["round", "truncate"] {
        for (digits, expected_scale) in [
            (Some("2"), 2),
            (Some("-1"), 0),
            (Some("128"), 0),
            (Some("256"), 0),
            (Some("digits"), 6),
            (Some("CAST(NULL AS BIGINT)"), 6),
            (None, 6),
        ] {
            let arguments = match digits {
                Some(digits) => format!("decimal_value, {digits}"),
                None => "decimal_value".to_string(),
            };
            let sql = format!(
                "SELECT round({arguments}) FROM \
                 (SELECT CAST(1.234 AS DECIMAL(18, 6)) AS decimal_value, 2 AS digits) source"
            );
            let mut statements = novarocks_parser::parse(&sql).unwrap();
            let [ast::Statement::Query(query)] = statements.as_mut_slice() else {
                panic!("expected the decimal call query");
            };
            if name == "truncate" {
                // TRUNCATE is a statement keyword in the current parser. Rename
                // only this call AST to isolate the analyzer contract regression.
                let ast::SetExpr::Select(select) = query.body.as_mut() else {
                    panic!("expected the decimal call projection");
                };
                let Some(ast::SelectItem::UnnamedExpr(ast::Expr::FunctionCall(call))) =
                    select.projection.first_mut()
                else {
                    panic!("expected the ordinary scalar call AST");
                };
                call.name.parts.last_mut().unwrap().value = "truncate".to_string();
            }
            let resolved = super::analyze(query, &SourceCatalog, "default").unwrap().0;
            let QueryBody::Select(mut select) = resolved.body else {
                panic!("expected the analyzed decimal call projection");
            };
            let expression = select.projection.remove(0).expr;
            let ExprKind::FunctionCall { binding, args, .. } = &expression.kind else {
                panic!("expected the actual selected decimal call: {sql}");
            };
            assert_eq!(
                args[0].value_type.data_type().clone(),
                DataType::Decimal128(18, 6)
            );
            assert_eq!(
                args[0].value_type.logical_type(),
                ValueLogicalType::Physical
            );
            if digits == Some("digits") {
                assert!(matches!(args[1].kind, ExprKind::ColumnRef { .. }));
            }
            let expected = FunctionValueType::new(DataType::Decimal128(38, expected_scale), true);
            let FunctionResultType::Scalar(selected) = &binding.selected.result_type else {
                panic!("decimal scalar selected a relation result: {sql}");
            };
            assert_eq!(selected, &expected, "independent decimal shape: {sql}");
            assert_eq!(
                &expression.value_type, selected,
                "exact result owner: {sql}"
            );
            assert_eq!(
                binding.function_id.as_str(),
                format!("builtin.scalar/{name}/v1"),
                "the real builtin must author the selected result: {sql}"
            );
        }
    }
}

/// This fixture's declared scalar preserves the source scale and ignores the
/// digits value. Its spelling does not grant the builtin ROUND result policy.
struct ScalePreservingRoundResolver;

impl ScalePreservingRoundResolver {
    fn selection(
        request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<
        novarocks_functions::FunctionBindingSelection,
        novarocks_functions::FunctionBindingError,
    > {
        use novarocks_functions::{
            FunctionArgument, FunctionArgumentType, FunctionBindingError, FunctionBindingSelection,
            FunctionOverloadId, FunctionResultType,
        };
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            CompilePhase::FunctionSpecialization,
        )?;
        let result = (|| {
            let [
                FunctionArgument::Value {
                    value_type: source, ..
                },
                FunctionArgument::Value {
                    value_type: digits, ..
                },
            ] = request.arguments
            else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            work.step()?;
            if request.logical_argument_count != 2
                || request.expected_result_type.is_some()
                || source.logical_type() != ValueLogicalType::Physical
                || *source.data_type() != DataType::Decimal128(18, 6)
                || digits.logical_type() != ValueLogicalType::Physical
                || !matches!(
                    digits.data_type(),
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                )
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            work.step()?;
            Ok(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("test.scalar/scale_preserving_round/0/v1")
                    .unwrap(),
                argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    vec![
                        FunctionArgumentType::Value(source.clone()),
                        FunctionArgumentType::Value(digits.clone()),
                    ]
                    .into_boxed_slice(),
                ),
                result_type: FunctionResultType::Scalar(FunctionValueType::new(
                    DataType::Decimal128(38, 6),
                    true,
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

impl novarocks_functions::FunctionBindingResolver for ScalePreservingRoundResolver {
    fn resolve(
        &self,
        request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<
        novarocks_functions::FunctionBindingSelection,
        novarocks_functions::FunctionBindingError,
    > {
        Self::selection(request, control)
    }

    fn validate_selected(
        &self,
        selected: &novarocks_functions::FunctionBindingSelection,
        request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        if selected == &Self::selection(request, control)? {
            Ok(())
        } else {
            Err(novarocks_functions::FunctionBindingError::NoMatchingOverload)
        }
    }
}

#[test]
fn same_spelling_custom_round_result_is_not_reauthored_by_analyzer() {
    use novarocks_functions::{
        EngineFunctionCatalogBuilder, FunctionBindingDeclaration, FunctionDefinition,
        FunctionFailureBehavior, FunctionId, FunctionKind, FunctionOverloadDeclaration,
        FunctionOverloadId, FunctionResultType, FunctionVisibility, FunctionVolatility,
    };
    use novarocks_type_contract::{
        ArgumentControl, FunctionEffectDeclaration, FunctionInstanceState,
        FunctionIntrinsicRowError, FunctionNullBehavior, ObservableEffects,
    };

    let identity = FunctionId::try_new("test.scalar/scale_preserving_round/v1").unwrap();
    let declaration = FunctionBindingDeclaration::try_new_complete(
        identity.clone(),
        FunctionKind::Scalar,
        [FunctionOverloadDeclaration::from_effects(
            FunctionOverloadId::try_new("test.scalar/scale_preserving_round/0/v1").unwrap(),
            "Physical Decimal128(18,6), Physical signed integer",
            "Physical Decimal128(38,6), nullable; preserve the source scale",
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
                "round",
                FunctionVisibility::Public,
                declaration,
                Arc::new(ScalePreservingRoundResolver),
            )
            .unwrap(),
        )
        .unwrap();
    let catalog = builder.seal_bound().unwrap();
    let statements =
        novarocks_parser::parse("SELECT round(CAST(1.234 AS DECIMAL(18, 6)), 2)").unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("expected the custom round query");
    };
    let resolved = super::analyze_with_function_catalog(
        query,
        &SourceCatalog,
        "default",
        &catalog,
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap()
    .0;
    let QueryBody::Select(select) = resolved.body else {
        panic!("expected the custom round projection");
    };
    let expression = &select.projection[0].expr;
    let ExprKind::FunctionCall { binding, args, .. } = &expression.kind else {
        panic!("expected the custom selected round call");
    };
    assert_eq!(binding.function_id, identity);
    assert!(matches!(
        args[1].kind,
        ExprKind::Literal(crate::analysis::LiteralValue::Int(2))
    ));
    let FunctionResultType::Scalar(selected) = &binding.selected.result_type else {
        panic!("custom round must select a scalar");
    };
    assert_eq!(
        selected,
        &FunctionValueType::new(DataType::Decimal128(38, 6), true)
    );
    assert_eq!(&expression.value_type, selected);
}

fn assert_selected_conversion(expression: &crate::analysis::TypedExpr, suffix: &str) {
    let ExprKind::FunctionCall { binding, args, .. } = &expression.kind else {
        panic!(
            "expected selected value conversion, got {:?}",
            expression.kind
        );
    };
    assert_eq!(
        binding.function_id.as_str(),
        novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_FUNCTION_ID
    );
    assert!(binding.selected.overload.as_str().ends_with(suffix));
    let [novarocks_functions::FunctionArgumentType::Value(source)] =
        binding.selected.argument_types.as_ref()
    else {
        panic!("expected exact value argument");
    };
    assert_eq!(source, &args[0].value_type);
    let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
    else {
        panic!("expected scalar result");
    };
    assert_eq!(result, &expression.value_type);
}

#[test]
fn explicit_logical_casts_use_exact_selected_conversion_bindings() {
    for (sql, suffix, logical) in [
        (
            "SELECT CAST(j AS VARCHAR) FROM source",
            "json_text_same_structure/v1",
            ValueLogicalType::Physical,
        ),
        (
            "SELECT CAST(i AS BIGINT) FROM source",
            "largeint_to_signed_null_overflow/v1",
            ValueLogicalType::Physical,
        ),
        (
            "SELECT CAST(i AS DOUBLE) FROM source",
            "largeint_to_float_round/v1",
            ValueLogicalType::Physical,
        ),
        (
            "SELECT CAST(1 AS LARGEINT)",
            "signed_to_largeint/v1",
            ValueLogicalType::LargeInt,
        ),
    ] {
        let expression = projection(sql);
        assert_selected_conversion(&expression, suffix);
        assert_eq!(expression.value_type.logical_type(), logical);
    }
    assert!(matches!(
        projection("SELECT CAST(j AS JSON) FROM source").kind,
        ExprKind::Cast { .. }
    ));
    assert!(matches!(
        projection("SELECT CAST(1 AS BIGINT)").kind,
        ExprKind::Cast { .. }
    ));
}

#[test]
fn mixed_case_and_typed_array_convert_actual_json_sources() {
    let expression = projection("SELECT CASE WHEN TRUE THEN j ELSE 'text' END FROM source");
    let ExprKind::Case { when_then, .. } = expression.kind else {
        panic!("expected CASE");
    };
    assert_selected_conversion(&when_then[0].1, "json_text_same_structure/v1");
    let expression = projection("SELECT ARRAY<VARCHAR>[j] FROM source");
    let ExprKind::FunctionCall { args, .. } = expression.kind else {
        panic!("expected array literal owner");
    };
    assert_selected_conversion(&args[0], "json_text_same_structure/v1");
}

#[test]
fn only_literal_null_is_declared_without_running_its_source() {
    let literal = projection("SELECT CAST(NULL AS JSON)");
    assert!(matches!(
        literal.kind,
        ExprKind::Literal(crate::analysis::LiteralValue::Null)
    ));
    assert_eq!(literal.value_type.logical_type(), ValueLogicalType::Json);
    assert!(literal.value_type.nullable());
    let expression = projection("SELECT CAST(element_at([], 1) AS JSON)");
    assert_selected_conversion(&expression, "null_to_typed_nullable/v1");
    let ExprKind::FunctionCall { args, .. } = expression.kind else {
        panic!("expected evaluated NULL lift");
    };
    assert!(matches!(args[0].kind, ExprKind::FunctionCall { .. }));
    assert_eq!(args[0].value_type.data_type().clone(), DataType::Null);
}

#[test]
fn hidden_conversion_cannot_be_selected_as_a_user_function() {
    let statements =
        novarocks_parser::parse("SELECT __value_domain_conversion(j) FROM source").unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("expected query");
    };
    assert!(super::analyze(query, &SourceCatalog, "default").is_err());
}

#[derive(Debug)]
struct MissingConversionPort;
impl crate::compiler::SqlFunctionCatalog for MissingConversionPort {
    fn snapshot(&self) -> Arc<dyn crate::compiler::SqlFunctionCatalog> {
        Arc::new(Self)
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        crate::functions::builtin_sql_function_catalog()
            .resolve_scalar_signature(name, args, control)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        crate::functions::builtin_sql_function_catalog().contains_aggregate(name)
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        crate::functions::builtin_sql_function_catalog()
            .resolve_aggregate_signature(name, args, control)
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        crate::functions::builtin_sql_function_catalog()
            .resolve_aggregate_trusted(name, args, control)
    }
    fn volatility(&self, name: &str) -> novarocks_functions::FunctionVolatility {
        crate::functions::builtin_sql_function_catalog().volatility(name)
    }
}

#[test]
fn actual_request_catalog_missing_conversion_port_has_no_fallback() {
    let statements = novarocks_parser::parse("SELECT CAST(j AS VARCHAR) FROM source").unwrap();
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("expected query");
    };
    let error = super::analyze_with_function_catalog(
        query,
        &SourceCatalog,
        "default",
        &MissingConversionPort,
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("binding declaration"), "{error}");
}

#[test]
fn selected_scalar_text_argument_converts_source_json_before_rebinding() {
    for sql in [
        "SELECT parse_json(j) FROM source",
        "SELECT length(j) FROM source",
    ] {
        let expression = projection(sql);
        let ExprKind::FunctionCall { binding, args, .. } = expression.kind else {
            panic!("expected scalar owner");
        };
        assert_ne!(
            binding.function_id.as_str(),
            novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_FUNCTION_ID
        );
        assert_selected_conversion(&args[0], "json_text_same_structure/v1");
        assert_eq!(
            args[0].value_type.logical_type(),
            ValueLogicalType::Physical
        );
    }
}

#[test]
fn nested_json_conversion_preserves_source_facts_before_same_domain_cast() {
    let expression = projection(
        "SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ CAST(ja_meta AS ARRAY<VARCHAR>) FROM source",
    );
    let ExprKind::Cast {
        expr,
        decimal_overflow_policy,
        ..
    } = &expression.kind
    else {
        panic!("expected final carrier cast");
    };
    assert_eq!(
        *decimal_overflow_policy,
        novarocks_type_contract::DecimalOverflowPolicy::ReportError
    );
    assert_selected_conversion(expr, "json_text_same_structure/v1");
    let DataType::List(item) = &expr.value_type.data_type() else {
        panic!("expected exact intermediate list");
    };
    assert_eq!(item.name(), "item");
    assert!(!item.is_nullable());
    assert_eq!(item.metadata().capacity(), 0);
    assert_eq!(
        expr.value_type,
        crate::test_value_types::string_list(false, false, false),
    );
    let source = SourceCatalog
        .resolve_table_for_analysis(None, "default", "source")
        .unwrap();
    let declared = source
        .catalog
        .columns
        .iter()
        .find(|column| column.name == "ja_meta")
        .unwrap();
    let DataType::List(source_item) = &declared.data_type else {
        panic!("expected original source list");
    };
    assert_eq!(source_item.name(), "source_item");
    assert_eq!(
        source_item.metadata().get("provider").map(String::as_str),
        Some("retained-source")
    );
    let DataType::List(target) = &expression.value_type.data_type() else {
        panic!("expected final list");
    };
    assert!(target.is_nullable());
    assert_eq!(target.name(), "item");
    assert_eq!(target.metadata().capacity(), 0);
    assert_eq!(
        expression.value_type,
        crate::test_value_types::string_list(true, false, true)
    );
}

fn control_binding_arguments(count: usize) -> Vec<crate::analysis::TypedExpr> {
    (0..count)
        .map(|index| crate::analysis::TypedExpr {
            kind: ExprKind::Literal(crate::analysis::LiteralValue::Int(index as i64)),
            value_type: FunctionValueType::new(DataType::Int64, false),
        })
        .collect()
}

#[test]
fn function_binding_adapters_preserve_control_at_entry_and_inside_actual_arguments() {
    let catalog = crate::functions::builtin_sql_function_catalog();
    let arguments = control_binding_arguments(320);
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at_entry in [true, false] {
            let control = StopAt { error, at_entry };
            let failure = crate::analysis::resolve_function_binding(
                catalog,
                "coalesce",
                &arguments,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                crate::constant::test_constant_policy(),
                &control,
            )
            .unwrap_err();
            assert!(
                matches!(failure, novarocks_functions::FunctionBindingError::Control(actual) if actual == error)
            );
            let failure = match super::resolve_expr::bind_scalar_function_call_with_catalog(
                catalog,
                "coalesce",
                arguments.clone(),
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                crate::constant::test_constant_policy(),
                &control,
            ) {
                Ok(_) => panic!("stopped argument binding must fail"),
                Err(failure) => failure,
            };
            assert_eq!(failure.control_error(), Some(error));
            assert_eq!(
                failure
                    .at_type_mismatch(novarocks_parser::Span::new(3, 9))
                    .control_error(),
                Some(error)
            );
        }
    }
    let binding = crate::analysis::resolve_function_binding(
        catalog,
        "coalesce",
        &arguments,
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap();
    assert_eq!(binding.selected.argument_types.len(), 320);
    let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
    else {
        panic!("coalesce must have a scalar result");
    };
    assert_eq!(result.data_type().clone(), DataType::Int64);
    assert_eq!(result.logical_type(), ValueLogicalType::Physical);
}

#[test]
fn legacy_type_inspection_never_falls_back_after_request_control_failure() {
    let catalog = crate::functions::builtin_sql_function_catalog();
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        // Unknown ordinary names and known dynamic fallback names must both
        // retain a failure from the actual signature owner.
        for name in ["unregistered_control_probe", "map_keys"] {
            let failure = super::functions::legacy_scalar_return_type_with_catalog(
                catalog,
                name,
                &[DataType::Int64],
                &StopAt {
                    error,
                    at_entry: true,
                },
            )
            .unwrap_err();
            assert_eq!(failure.control_error(), Some(error));
            let failure = super::functions::infer_scalar_return_type_with_catalog(
                catalog,
                name,
                &[DataType::Int64],
                &StopAt {
                    error,
                    at_entry: true,
                },
            )
            .unwrap_err();
            assert_eq!(failure.control_error(), Some(error));
        }
    }
    assert_eq!(
        super::functions::legacy_scalar_return_type_with_catalog(
            catalog,
            "unregistered_control_probe",
            &[DataType::Int64],
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap(),
        None
    );
}

#[test]
fn aggregate_and_value_conversion_adapters_preserve_binding_control() {
    let catalog = crate::functions::builtin_sql_function_catalog();
    let source = crate::analysis::TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: crate::column_id::ColumnId::UNSET,
            qualifier: None,
            column: "json_source".into(),
        },
        value_type: FunctionValueType::try_with_logical_type(
            DataType::Utf8,
            true,
            ValueLogicalType::Json,
        )
        .unwrap(),
    };
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = StopAt {
            error,
            at_entry: true,
        };
        let failure = super::resolve_expr::resolve_aggregate_function_call(
            catalog,
            "sum",
            &control_binding_arguments(1),
            novarocks_parser::Span::new(2, 8),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &control,
        )
        .unwrap_err();
        assert_eq!(failure.control_error(), Some(error));
        let failure = super::value_conversion::convert_value_domain_with_catalog(
            catalog,
            source.clone(),
            FunctionValueType::new(DataType::Utf8, true),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &control,
        )
        .unwrap_err();
        assert_eq!(failure.control_error(), Some(error));
    }
}

#[test]
fn exact_value_conversion_call_preserves_nested_sql_mode_policy() {
    use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
    let strict = "/*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */";
    let relaxed = "/*+ SET_VAR(sql_mode=32) */";
    for (outer, inner, outer_policy, inner_policy) in [
        (strict, relaxed, ReportError, OutputNull),
        (relaxed, strict, OutputNull, ReportError),
    ] {
        let sql = format!(
            "SELECT {outer} CAST(q.j AS VARCHAR) AS x FROM (SELECT {inner} j,CAST(j AS VARCHAR) AS y FROM source) q"
        );
        let query = analyze(&sql);
        let QueryBody::Select(select) = &query.body else {
            panic!("select");
        };
        let ExprKind::FunctionCall { binding: outer, .. } = &select.projection[0].expr.kind else {
            panic!("exact conversion");
        };
        let Some(crate::analysis::Relation::Subquery { query: inner, .. }) = &select.from else {
            panic!("derived query");
        };
        let QueryBody::Select(inner) = &inner.body else {
            panic!("inner select");
        };
        let ExprKind::FunctionCall { binding: inner, .. } = &inner.projection[1].expr.kind else {
            panic!("inner exact conversion");
        };
        assert_eq!(
            outer.function_id.as_str(),
            novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_FUNCTION_ID
        );
        assert_eq!(outer.function_id, inner.function_id);
        assert_eq!(outer.selected, inner.selected);
        assert_eq!(outer.decimal_overflow_policy(), outer_policy, "{sql}");
        assert_eq!(inner.decimal_overflow_policy(), inner_policy, "{sql}");
    }
}

#[test]
fn variant_get_text_admission_preserves_parse_json_and_original_union_common_type() {
    use novarocks_functions::FunctionArgumentType;
    for (sql, expected_source) in [
        (
            r#"SELECT variant_get(parse_json('{"a":1}'),'$.a','bigint')"#,
            ValueLogicalType::Json,
        ),
        (
            r#"SELECT try_variant_get('{"a":99}','$.a','bigint')"#,
            ValueLogicalType::Physical,
        ),
        (
            r#"WITH source_values AS (
            SELECT 1 AS id, parse_json('{"a":1,"b":"x"}') AS v
            UNION ALL SELECT 2, parse_json('{"a":99,"b":"y"}')
            UNION ALL SELECT 3, parse_json('{"b":"no-a"}')
            UNION ALL SELECT 4, parse_json('{"a":1.5}')
            UNION ALL SELECT 5, CAST(NULL AS VARIANT)
            UNION ALL SELECT 6, parse_json('{"a":"abc"}')
        ) SELECT id, try_variant_get(v, '$.a', 'bigint') FROM source_values ORDER BY id"#,
            ValueLogicalType::Physical,
        ),
    ] {
        let query = analyze(sql);
        let QueryBody::Select(select) = &query.body else {
            panic!("original outer SELECT")
        };
        let expression = &select.projection.last().unwrap().expr;
        let ExprKind::FunctionCall { args, binding, .. } = &expression.kind else {
            panic!("original scalar")
        };
        assert_eq!(args[0].value_type.data_type(), &DataType::Utf8);
        assert_eq!(args[0].value_type.logical_type(), expected_source);
        assert_eq!(
            binding.selected.argument_types[0],
            FunctionArgumentType::Value(args[0].value_type.clone())
        );
        assert_eq!(expression.value_type.data_type(), &DataType::Int64);
        assert_eq!(
            expression.value_type.logical_type(),
            ValueLogicalType::Physical
        );
        assert!(expression.value_type.nullable());
    }
}
