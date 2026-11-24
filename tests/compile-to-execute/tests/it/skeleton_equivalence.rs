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

//! Compare the actual scalar units authored by SQL and the text entry.

use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use novarocks_expression::{
    EvaluationEnvironment,
    testkit::{
        HarnessControl,
        expression_text::{TextColumn, TextContext, TextError, TypedText, type_expression_text},
    },
};
use novarocks_expression_contract::{
    BoundFunction, ExprKind, ExpressionCallClaim, PhysicalCallRequest, RootScope, ValueId,
    materialize_call_request_observed,
};
use novarocks_functions::{
    PureEngineFunctionCatalog, testkit::shared_installed_builtin_owner_catalogue,
};
use novarocks_physical_plan::{ConstantPools, ConstantReference, ConstantValue};
use novarocks_sql::{
    analyze_error::AnalyzeErrorKind,
    compiler::{
        SqlAuthoredScalarUnit, SqlCompileControl, SqlCompileError, SqlPlannerTableSnapshot,
        SqlStandaloneScalarColumn, SqlStandaloneScalarRequest, author_standalone_scalar_for_test,
        builtin_sql_function_catalog,
    },
    planning::catalog::PlannerMemoryCatalog,
    sql_mode::{SqlMode, SqlSemanticSettings},
};
use novarocks_sql_testkit::sql_fixture_constant_policy;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, FunctionValueType,
    SemanticParameterId, SemanticParameterKey, SemanticParameterRef, SemanticParameterValue,
    SemanticParameters,
};

const ALLOW_THROW: SemanticParameterRef = SemanticParameterRef {
    id: SemanticParameterId::new(17),
    expected_key: SemanticParameterKey::AllowThrowException,
};

struct Fixture {
    functions: Arc<PureEngineFunctionCatalog>,
    parameters: SemanticParameters,
    sql_semantics: SqlSemanticSettings,
}

impl Fixture {
    fn new() -> Self {
        Self {
            functions: shared_installed_builtin_owner_catalogue(),
            parameters: SemanticParameters::try_new([(
                ALLOW_THROW.id,
                SemanticParameterValue::AllowThrowException(true),
            )])
            .unwrap(),
            sql_semantics: SqlSemanticSettings::default()
                .with_sql_mode(SqlMode::from_assignment("ALLOW_THROW_EXCEPTION")),
        }
    }

    fn sql(
        &self,
        text: &str,
        columns: &[TextColumn],
    ) -> Result<SqlAuthoredScalarUnit, SqlCompileError> {
        let columns = columns
            .iter()
            .map(|column| SqlStandaloneScalarColumn {
                name: &column.name,
                value: column.value,
                value_type: column.value_type.clone(),
            })
            .collect::<Vec<_>>();
        let catalog = PlannerMemoryCatalog::default();
        author_standalone_scalar_for_test(SqlStandaloneScalarRequest {
            text,
            columns: &columns,
            catalog: &SqlPlannerTableSnapshot::new(&catalog),
            current_database: "default",
            functions: builtin_sql_function_catalog().snapshot(),
            sealed_functions: self.functions.clone(),
            sql_semantics: &self.sql_semantics,
            constant_policy: sql_fixture_constant_policy(),
            control: &SqlCompileControl::unbounded(),
        })
    }

    fn text(&self, text: &str, columns: &[TextColumn]) -> Result<TypedText<'_>, TextError> {
        type_expression_text(
            text,
            columns,
            &TextContext {
                functions: &self.functions,
                constant_policy: sql_fixture_constant_policy(),
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                parameters: &self.parameters,
                allow_throw_exception: ALLOW_THROW,
            },
            &HarnessControl,
        )
    }

    fn equivalent(&self, text: &str, columns: &[TextColumn]) -> TypedText<'_> {
        let sql = self
            .sql(text, columns)
            .unwrap_or_else(|cause| panic!("SQL {text}: {cause}"));
        let typed = self
            .text(text, columns)
            .unwrap_or_else(|cause| panic!("text {text}: {cause}"));
        compare_units(&sql, &typed).unwrap_or_else(|difference| panic!("{text}: {difference}"));
        typed
    }
}

fn column(name: &str, value: u32, data_type: DataType, nullable: bool) -> TextColumn {
    TextColumn {
        name: name.into(),
        value: ValueId::new(value),
        value_type: FunctionValueType::new(data_type, nullable),
    }
}

fn compare_units(sql: &SqlAuthoredScalarUnit, text: &TypedText<'_>) -> Result<(), String> {
    let left = sql.unit();
    let right = text.unit();
    let left_env = EvaluationEnvironment {
        constants: sql.constants(),
        parameters: sql.parameters(),
    };
    let right_env = text.environment();
    // Value IDs come from the same declared layout. Definition IDs are
    // offsets in each unit's ordered table. The common control author
    // numbers uses and regions from the ordered roots.
    same("ordered ports", left.ports(), right.ports())?;
    same("roots", left.roots(), right.roots())?;
    same("root uses", left.root_use_ids(), right.root_use_ids())?;
    same("control flow", left.control_flow(), right.control_flow())?;
    same(
        "definition count",
        &left.definitions().len(),
        &right.definitions().len(),
    )?;
    for (l, r) in left.definitions().iter().zip(right.definitions()) {
        same("lambda scope", &l.lambda_scope, &r.lambda_scope)?;
        same(&format!("definition {:?} type", l.id), &l.ty, &r.ty)?;
        match (&l.kind, &r.kind) {
            (ExprKind::Constant(l), ExprKind::Constant(r)) => {
                let left_value = resolve_constant(left_env.constants, *l)?;
                let right_value = resolve_constant(right_env.constants, *r)?;
                if !left_value
                    .equals_observed(&right_value, CompilePhase::Validate, &HarnessControl)
                    .map_err(|cause| cause.to_string())?
                {
                    let diagnostic = |value: &ConstantValue| {
                        value
                            .format_diagnostic_observed(CompilePhase::Validate, &HarnessControl)
                            .map_err(|cause| cause.to_string())
                    };
                    return Err(format!(
                        "constant value differs: {} versus {}",
                        diagnostic(&left_value)?,
                        diagnostic(&right_value)?,
                    ));
                }
            }
            _ => same(
                &format!("definition {:?} shape and parameters", l.id),
                &resolved_definition(&l.kind, left_env.parameters)?,
                &resolved_definition(&r.kind, right_env.parameters)?,
            )?,
        }
    }

    same("claim count", &left.claims().len(), &right.claims().len())?;
    for (use_id, left_claim) in left.claims() {
        let right_claim = right
            .claims()
            .get(use_id)
            .ok_or_else(|| format!("missing claim {use_id:?}"))?;
        same(
            &format!("claim {use_id:?}"),
            &resolved_claim(left_claim, left_env.parameters)?,
            &resolved_claim(right_claim, right_env.parameters)?,
        )?;
    }
    same(
        "request count",
        &left.requests().len(),
        &right.requests().len(),
    )?;
    for (definition, left_request) in left.requests() {
        let right_request = right
            .requests()
            .get(definition)
            .ok_or_else(|| format!("missing request {definition:?}"))?;
        compare_requests(
            left_request,
            left_env.constants,
            right_request,
            right_env.constants,
        )
        .map_err(|detail| format!("request {definition:?}: {detail}"))?;
    }

    // The table can bind multiple local scopes to the same stable key.
    // Preserve multiplicity while ignoring the producer's sparse IDs.
    let mut actual = right_env.parameters.entries().values().collect::<Vec<_>>();
    for value in left_env.parameters.entries().values() {
        let pos = actual
            .iter()
            .position(|other| *other == value)
            .ok_or_else(|| format!("missing semantic parameter value: {value:?}"))?;
        actual.swap_remove(pos);
    }
    same("extra semantic parameter values", actual.as_slice(), &[])
}

fn same<T: PartialEq + std::fmt::Debug + ?Sized>(
    site: &str,
    left: &T,
    right: &T,
) -> Result<(), String> {
    if left == right {
        Ok(())
    } else {
        Err(format!("{site} differs: {left:?} versus {right:?}"))
    }
}

fn resolve_constant(
    pools: &ConstantPools,
    reference: ConstantReference,
) -> Result<ConstantValue, String> {
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate)
        .map_err(|cause| cause.to_string())?;
    let result = pools
        .resolve_source_observed(reference, &mut work)
        .map_err(|cause| cause.to_string())?;
    work.finish().map_err(|cause| cause.to_string())?;
    Ok(result)
}

fn resolved_definition(
    kind: &ExprKind,
    parameters: &SemanticParameters,
) -> Result<(ExprKind, Vec<SemanticParameterValue>), String> {
    let mut kind = kind.clone();
    let mut values = Vec::new();
    match &mut kind {
        ExprKind::Binary {
            allow_throw_exception: Some(allow_throw_exception),
            ..
        } => {
            resolve_parameter(allow_throw_exception, parameters, &mut values)?;
        }
        ExprKind::FunctionCall {
            function:
                BoundFunction {
                    legacy_metadata: Some(legacy),
                    ..
                },
            ..
        } => {
            for reference in &mut legacy.semantic_parameters {
                resolve_parameter(reference, parameters, &mut values)?;
            }
        }
        _ => {}
    }
    Ok((kind, values))
}

fn resolved_claim(
    claim: &ExpressionCallClaim,
    parameters: &SemanticParameters,
) -> Result<(ExpressionCallClaim, Vec<SemanticParameterValue>), String> {
    let mut claim = claim.clone();
    let mut values = Vec::new();
    for reference in &mut claim.effects.environment {
        resolve_parameter(reference, parameters, &mut values)?;
    }
    Ok((claim, values))
}

fn resolve_parameter(
    reference: &mut SemanticParameterRef,
    parameters: &SemanticParameters,
    values: &mut Vec<SemanticParameterValue>,
) -> Result<(), String> {
    values.push(
        parameters
            .require(*reference)
            .map_err(|cause| cause.to_string())?
            .clone(),
    );
    reference.id = SemanticParameterId::new(0);
    Ok(())
}

fn compare_requests(
    left: &PhysicalCallRequest,
    left_pools: &ConstantPools,
    right: &PhysicalCallRequest,
    right_pools: &ConstantPools,
) -> Result<(), String> {
    same(
        "constant policy",
        &left.constant_policy,
        &right.constant_policy,
    )?;
    same(
        "logical argument count",
        &left.logical_argument_count,
        &right.logical_argument_count,
    )?;
    same(
        "expected result type",
        &left.expected_result_type,
        &right.expected_result_type,
    )?;
    same(
        "static argument count",
        &left.arguments.len(),
        &right.arguments.len(),
    )?;

    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate)
        .map_err(|cause| cause.to_string())?;
    let left = materialize_call_request_observed(left, left_pools, &mut work, left.arguments.len())
        .map_err(|cause| cause.to_string())?;
    let right =
        materialize_call_request_observed(right, right_pools, &mut work, right.arguments.len())
            .map_err(|cause| cause.to_string())?;
    work.finish().map_err(|cause| cause.to_string())?;
    for (ordinal, (left, right)) in left
        .request()
        .arguments
        .iter()
        .zip(right.request().arguments)
        .enumerate()
    {
        if !left
            .equals_observed(right, CompilePhase::Validate, &HarnessControl)
            .map_err(|cause| cause.to_string())?
        {
            return Err(format!(
                "static argument {ordinal} type or constant value differs"
            ));
        }
    }
    Ok(())
}

#[test]
fn migrated_calls_share_the_declared_port_and_preserve_source_occurrences() {
    let fixture = Fixture::new();
    let columns = [
        column("x", 11, DataType::Int32, false),
        column("unused", 12, DataType::Boolean, false),
        column("y", 14, DataType::Int32, true),
    ];
    let text = fixture.equivalent("COALESCE(y, NULLIF(x, x), abs(x))", &columns);
    let [port] = text.unit().ports() else {
        panic!("expected a single referenced input port");
    };
    assert_eq!(
        port.values.as_ref(),
        &[
            (columns[0].value, columns[0].value_type.clone()),
            (columns[2].value, columns[2].value_type.clone()),
        ],
    );
    assert_eq!(text.unit().roots()[0].scope, RootScope::Port(0));
    assert_eq!(
        text.unit()
            .definitions()
            .iter()
            .filter_map(|definition| match definition.kind {
                ExprKind::Value(id) => Some(id),
                _ => None,
            })
            .collect::<Vec<_>>(),
        [
            columns[2].value,
            columns[0].value,
            columns[0].value,
            columns[0].value
        ],
    );
    assert_eq!(text.unit().requests().len(), 3);
}

#[test]
fn migrated_literals_control_and_conversions_share_the_full_unit() {
    let fixture = Fixture::new();
    let columns = [column("unused", 11, DataType::Boolean, false)];
    for source in [
        "NULL",
        "TRUE",
        "2147483648",
        "12.30",
        "1.5E1",
        "'text'",
        "X'036AFF'",
        "DATE '1970-01-03'",
        "TIME '01:02:03'",
        "TIMESTAMP '1970-01-02 00:00:00'",
        "COALESCE(TIME '01:02:03', TIME '04:05:06')",
        "CASE WHEN TRUE THEN TIME '01:02:03' ELSE TIME '04:05:06' END",
        "NULL IS NULL",
        "TRUE OR (NULL AND FALSE)",
        "CASE WHEN TRUE THEN 2147483648 ELSE 1 END",
        "-(2147483648)",
    ] {
        let text = fixture.equivalent(source, &columns);
        assert!(text.unit().ports().is_empty(), "{source}");
        assert_eq!(
            text.unit().roots()[0].scope,
            RootScope::AllPorts,
            "{source}"
        );
    }
    let text = fixture.equivalent("TIME '01:02:03'", &[]);
    assert!(text.environment().parameters.entries().is_empty());
}

#[test]
fn extract_matches_the_unit_and_has_an_independent_result() {
    let fixture = Fixture::new();
    let text = fixture.equivalent("EXTRACT(YEAR FROM DATE '2025-09-02')", &[]);
    let root = text
        .unit()
        .definition(text.unit().roots()[0].definition)
        .unwrap();
    assert!(matches!(
        &root.kind,
        ExprKind::FunctionCall { function, .. } if function.function_id.as_str() == "builtin.scalar/year/v1"
    ));
    let rows = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let result = text
        .check_rows(&rows, &HarnessControl, &HarnessControl)
        .unwrap()
        .batch;
    assert!(result.errors.is_empty());
    assert_eq!(result.shape.port_count, 0);
    assert_eq!(result.shape.input_rows, 1);
    assert_eq!(
        result.values.to_data(),
        Int64Array::from(vec![2025]).to_data()
    );
}

#[test]
fn identical_local_constant_addresses_do_not_hide_different_values() {
    let fixture = Fixture::new();
    let sql = fixture.sql("5", &[]).unwrap();
    let text = fixture.text("6", &[]).unwrap();
    assert_eq!(
        sql.unit().definitions()[0].kind,
        text.unit().definitions()[0].kind
    );
    assert!(matches!(
        sql.unit().definitions()[0].kind,
        ExprKind::Constant(_)
    ));
    assert_eq!(
        compare_units(&sql, &text).unwrap_err(),
        "constant value differs: 5 versus 6"
    );

    let sql = fixture.sql("ABS(5)", &[]).unwrap();
    let text = fixture.text("ABS(6)", &[]).unwrap();
    let (_, sql_request) = sql.unit().requests().first_key_value().unwrap();
    let (_, text_request) = text.unit().requests().first_key_value().unwrap();
    assert_eq!(
        compare_requests(
            sql_request,
            sql.constants(),
            text_request,
            text.environment().constants
        )
        .unwrap_err(),
        "static argument 0 type or constant value differs",
    );
}

#[test]
fn actual_cast_modes_are_checked() {
    let fixture = Fixture::new();
    let columns = [column("input_text", 34, DataType::Utf8, true)];
    let sql = fixture
        .sql("CAST(input_text AS INTEGER)", &columns)
        .unwrap();
    let text = fixture
        .text("TRY_CAST(input_text AS INTEGER)", &columns)
        .unwrap();
    let error = compare_units(&sql, &text).unwrap_err();
    assert!(error.contains("shape and parameters differs"), "{error}");
    assert!(error.contains("cast_mode: Explicit"), "{error}");
    assert!(error.contains("cast_mode: Try"), "{error}");
}

#[test]
fn trim_shares_the_catalogue_binding_and_result_type() {
    let fixture = Fixture::new();
    let columns = [column("input_text", 34, DataType::Utf8, false)];
    let text = fixture.equivalent("TRIM(input_text)", &columns);
    let root = text
        .unit()
        .definition(text.unit().roots()[0].definition)
        .unwrap();
    assert!(matches!(
        &root.kind,
        ExprKind::FunctionCall { function, .. } if function.function_id.as_str() == "builtin.scalar/trim/v1"
    ));
    // The original TRIM signature declares a conservatively nullable result.
    assert_eq!(root.ty, FunctionValueType::new(DataType::Utf8, true));
}

#[test]
fn row_syntax_freezes_distinct_names_and_executes_the_same_selected_field() {
    let fixture = Fixture::new();
    let typed = fixture.equivalent(r#"ROW(7 AS id, NULL AS "Note", 9)"#, &[]);
    let root = typed
        .unit()
        .definition(typed.unit().roots()[0].definition)
        .unwrap();
    let ExprKind::FunctionCall { function, .. } = &root.kind else {
        panic!("compiled ROW constructor");
    };
    assert_eq!(function.function_id.as_str(), "builtin.scalar/row/v1");
    let novarocks_type_contract::ValueKind::Struct(fields) = root.ty.value_type().kind() else {
        panic!("declared ROW");
    };
    assert_eq!(
        fields
            .iter()
            .map(|field| field.row_name())
            .collect::<Vec<_>>(),
        vec![Some("ID"), Some("Note"), None],
    );
    assert!(!root.ty.nullable());
    assert!(!fields[0].value_type().nullable());
    assert!(fields[1].value_type().nullable());

    let rows = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let row = typed
        .check_rows(&rows, &HarnessControl, &HarnessControl)
        .unwrap()
        .batch;
    let data = row
        .values
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .unwrap();
    assert!(data.is_valid(0));
    assert_eq!(data.column(1).logical_null_count(), 1);
    for (source, expected) in [
        ("(ROW(7 AS id, 9)).id", Some(7)),
        ("(ROW(7 AS id, 9))[2]", Some(9)),
        ("(CAST(NULL AS ROW(id INTEGER)))[1]", None),
        ("(ROW(7 AS x, 9 AS x))[2]", Some(9)),
    ] {
        let typed = fixture.equivalent(source, &[]);
        let output = typed
            .check_rows(&rows, &HarnessControl, &HarnessControl)
            .unwrap()
            .batch;
        assert!(output.errors.is_empty(), "{source}");
        assert_eq!(
            output.values.to_data(),
            arrow::array::Int32Array::from(vec![expected]).to_data(),
            "{source}"
        );
    }
}

#[test]
fn derived_char_concat_width_keeps_the_catalogue_type_error() {
    let fixture = Fixture::new();
    let result = fixture.sql(
        "typeof(concat(CAST('a' AS CHAR(32768)), CAST('b' AS CHAR(32769))))",
        &[],
    );
    assert!(matches!(
        result,
        Err(SqlCompileError::Analyze(error))
            if error.kind() == AnalyzeErrorKind::UnknownType
                && error.message() == "Unknown type: char(65537)"
    ));
}
