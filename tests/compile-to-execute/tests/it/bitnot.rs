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
//! Actual source journal/effects, original package author, LocalCompiler and
//! Frame. The shared SQL fixture deliberately disables folding; native whole
//! cases and production FE folding remain separate acceptance obligations.
use arrow::array::{Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_expression::testkit::{PassThroughCallHost, fixture_scope};
use novarocks_expression::{
    CompiledExpressionInstance, EvaluationEnvironment, EvaluationInput, ScopeLayouts,
    compile_expr_set_pure,
};
use novarocks_functions::{
    FunctionArgumentType, FunctionResultType, FunctionValueType, KernelEvaluationControl,
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_local_program::{ProgramNodeKind, ProgramRootRef, ProgramUnitId};
use novarocks_sql::analyze_error::AnalyzeErrorKind;
use novarocks_sql::compiler::{
    SqlCompileControl, SqlCompileError, SqlCompileProgressError, SqlPlannerTableSnapshot,
    SqlStandaloneScalarColumn, SqlStandaloneScalarRequest, author_standalone_scalar_for_test,
    builtin_sql_function_catalog,
};
use novarocks_sql::planning::catalog::PlannerMemoryCatalog;
use novarocks_sql_testkit::{
    sql_fixture_constant_policy, sql_source, try_sql_source_with_declared_fields,
};
use novarocks_test_compile_to_execute::programs::{batch, programs_with_catalogue};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, ValueLogicalType};

use novarocks_expression_contract::{RootOrdinal, ValueId};
use std::sync::Arc;
use std::time::Duration;
struct Control;
impl novarocks_type_contract::PureCompileControl for Control {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        n: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("BitwiseNot frame never waits")
    }
}
// Loan the already installed catalogue author; the old unary fixture's
// deliberately IF/COALESCE-only wrapper is unchanged.
fn programs(
    source: &novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
) -> std::collections::BTreeMap<
    novarocks_physical_plan::FragmentId,
    Arc<novarocks_local_program::LocalProgram>,
> {
    let functions = novarocks_functions::testkit::installed_builtin_owner_catalogue();
    programs_with_catalogue(source, &functions)
}
fn bitnot_call_root(
    plans: &std::collections::BTreeMap<
        novarocks_physical_plan::FragmentId,
        Arc<novarocks_local_program::LocalProgram>,
    >,
    argument: &FunctionValueType,
    result: &FunctionValueType,
) -> (Arc<novarocks_local_program::LocalProgram>, ProgramRootRef) {
    let mut found = None;
    for program in plans.values() {
        for (unit_ordinal, unit) in program.units().iter().enumerate() {
            for (root_ordinal, root) in unit.roots().iter().enumerate() {
                if let Some(call) = unit.calls().get(&root.use_id)
                    && call.call_contract().function_id().as_str() == "builtin.scalar/bitnot/v1"
                {
                    assert!(found.is_none(), "the SQL has exactly one named bitnot root");
                    let contract = call.call_contract();
                    assert_eq!(contract.logical_argument_count(), 1);
                    assert_eq!(
                        contract.selected().argument_types.as_slice(),
                        &[FunctionArgumentType::Value(argument.clone())]
                    );
                    assert_eq!(
                        contract.selected().result_type,
                        FunctionResultType::Scalar(result.clone())
                    );
                    assert_eq!(
                        unit.definitions()
                            .node(root.definition)
                            .unwrap()
                            .value_type(),
                        Some(result)
                    );
                    found = Some((
                        program.clone(),
                        ProgramRootRef {
                            unit: ProgramUnitId::new(unit_ordinal),
                            root: RootOrdinal::new(u32::try_from(root_ordinal).unwrap()),
                        },
                    ));
                }
            }
        }
    }
    found.expect("the actual SQL output retains the bound bitnot call")
}
#[test]
fn named_bitnot_actual_sql_signed_four_widths_preserve_full_fvt_and_frame_results() {
    // For a fixed signed two's-complement width, complement swaps MIN/MAX,
    // maps -1 to zero, zero to -1, and one to -2. These are independent inputs
    // and fixed expected arrays, not an evaluator over the supplied input.
    let cases: Vec<(ArrayRef, ArrayRef)> = vec![
        (
            Arc::new(Int8Array::from(vec![i8::MIN, -1, 0, 1, i8::MAX])),
            Arc::new(Int8Array::from(vec![i8::MAX, 0, -1, -2, i8::MIN])),
        ),
        (
            Arc::new(Int16Array::from(vec![i16::MIN, -1, 0, 1, i16::MAX])),
            Arc::new(Int16Array::from(vec![i16::MAX, 0, -1, -2, i16::MIN])),
        ),
        (
            Arc::new(Int32Array::from(vec![i32::MIN, -1, 0, 1, i32::MAX])),
            Arc::new(Int32Array::from(vec![i32::MAX, 0, -1, -2, i32::MIN])),
        ),
        (
            Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, 1, i64::MAX])),
            Arc::new(Int64Array::from(vec![i64::MAX, 0, -1, -2, i64::MIN])),
        ),
    ];
    for (input, expected) in cases {
        let source = sql_source(
            "SELECT bitnot(k) AS original_inverted FROM fixture.source",
            input.data_type().clone(),
        );
        let public = source
            .checked_original_result_declaration_observed(&Control)
            .unwrap()
            .unwrap();
        let computed = source.plan().result_port().unwrap();
        assert_eq!(public.original_port(), computed);
        let argument = FunctionValueType::new(input.data_type().clone(), false);
        let result = FunctionValueType::new(input.data_type().clone(), true);
        assert_eq!(public.fields()[0].ty, result);
        let plans = programs(&source);
        let (program, root) = bitnot_call_root(&plans, &argument, &result);
        let data = batch(&program, root, input.clone());
        let mut frame = CompiledExpressionInstance::try_new(
            program.unit(root.unit).unwrap().clone(),
            root.root,
            &Control,
        )
        .unwrap();
        let all = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                {
                    let selection = Selection::all(input.len());
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(all.errors().is_empty());
        assert_eq!(all.values().to_data(), expected.to_data());
        let rows = [0, 3, 4];
        let sparse = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(input.len(), &rows).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(sparse.errors().is_empty());
        let wanted = arrow::compute::take(
            expected.as_ref(),
            &arrow::array::UInt64Array::from(vec![0, 3, 4]),
            None,
        )
        .unwrap();
        assert_eq!(sparse.values().to_data(), wanted.to_data());
        assert_eq!(
            frame
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &data,
                        source: None
                    },
                    {
                        let selection = Selection::try_sparse(input.len(), &[]).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap()
                .values()
                .len(),
            0
        );
        assert_eq!(
            frame
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &data.slice(1, 0),
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(0), Selection::all(0)),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap()
                .values()
                .data_type(),
            input.data_type()
        );
    }
}
#[test]
fn named_bitnot_actual_sql_nullable_values_preserve_nulls_and_sparse_results() {
    let source = sql_source(
        "SELECT bitnot(i) AS original_inverted FROM (VALUES (CAST(1 AS BIGINT)),(CAST(NULL AS BIGINT)),(CAST(-2 AS BIGINT))) AS source(i)",
        DataType::Int64,
    );
    let nullable_bigint = FunctionValueType::new(DataType::Int64, true);
    let plans = programs(&source);
    let (program, root) = bitnot_call_root(&plans, &nullable_bigint, &nullable_bigint);
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(-2)]));
    let data = batch(&program, root, input.clone());
    let mut frame = CompiledExpressionInstance::try_new(
        program.unit(root.unit).unwrap().clone(),
        root.root,
        &Control,
    )
    .unwrap();
    let out = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &data,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(out.errors().is_empty());
    // Complement keeps NULL and maps 1 -> -2, -2 -> 1.
    assert_eq!(
        out.values().to_data(),
        Int64Array::from(vec![Some(-2), None, Some(1)]).to_data()
    );
    let rows = [0, 2];
    let sparse = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &data,
                source: None,
            },
            {
                let selection = Selection::try_sparse(3, &rows).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        sparse.values().to_data(),
        Int64Array::from(vec![-2, 1]).to_data()
    );
    assert_eq!(
        source.plan().result_port().unwrap().fields[0].ty,
        nullable_bigint
    );
}
fn constant_outputs(sql: &str) -> Vec<ArrayRef> {
    let source = sql_source(sql, DataType::Int64);
    let plans = programs(&source);
    let mut actual = Vec::new();
    for program in plans.values() {
        for node in program.graph().nodes() {
            let ProgramNodeKind::Project { input, exprs, .. } = node.kind() else {
                continue;
            };
            let parent = &program.graph().nodes()[input.index()];
            let ProgramNodeKind::Values { values } = parent.kind() else {
                panic!("this actual scalar source must retain its original VALUES invocation")
            };
            let data = values
                .batch()
                .expect("the original table-free input is a constant VALUES batch");
            for &root in exprs {
                let mut frame = CompiledExpressionInstance::try_new(
                    program.unit(root.unit).unwrap().clone(),
                    root.root,
                    &Control,
                )
                .unwrap();
                let out = frame
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: data,
                            source: None,
                        },
                        {
                            let selection = Selection::all(data.num_rows());
                            ScalarInvocationScope::new(selection, selection)
                        },
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert!(out.errors().is_empty());
                assert_eq!(out.values().len(), 1);
                actual.push(out.values().clone());
            }
        }
    }
    actual
}
#[test]
fn native_bitnot_intrinsic_actual_project_sql_unchanged_constants_execute_each_root() {
    let sql = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/sql/correctness/project/sql/project_bitnot_operator_semantics.sql"
    ));
    let outputs = constant_outputs(sql);
    let actual = outputs
        .iter()
        .map(|out| out.as_any().downcast_ref::<Int64Array>().unwrap().value(0))
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![0, -1, -2, -1025]);
}
fn nominal_largeint_type() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}

#[test]
fn named_bitnot_sql_on_declared_largeint_keeps_nominal_binding_and_frame_results() {
    let ty = nominal_largeint_type();
    // Historical data can already declare this nominal type. The standalone
    // SQL author consumes that declaration as an input to the scalar expression.
    let columns = [SqlStandaloneScalarColumn {
        name: "k",
        value: ValueId::new(7),
        value_type: ty.clone(),
    }];
    let functions = novarocks_functions::testkit::shared_installed_builtin_owner_catalogue();
    let catalog = PlannerMemoryCatalog::default();
    let source = author_standalone_scalar_for_test(SqlStandaloneScalarRequest {
        text: "bitnot(k)",
        columns: &columns,
        catalog: &SqlPlannerTableSnapshot::new(&catalog),
        current_database: "fixture",
        functions: builtin_sql_function_catalog().snapshot(),
        sealed_functions: functions.clone(),
        sql_semantics: &Default::default(),
        constant_policy: sql_fixture_constant_policy(),
        control: &SqlCompileControl::unbounded(),
    })
    .unwrap();
    assert_eq!(source.value_type(), &ty);
    let [port] = source.unit().ports() else {
        panic!("one original scalar input port");
    };
    assert_eq!(port.values.as_ref(), &[(columns[0].value, ty.clone())]);
    let schema = Arc::new(Schema::new(vec![ty.try_to_field("k").unwrap()]));
    let scope = fixture_scope(port, schema.clone());
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::LowerProgram).unwrap();
    let compiled = compile_expr_set_pure(
        source.unit(),
        EvaluationEnvironment {
            constants: source.constants(),
            parameters: source.parameters(),
        },
        ScopeLayouts {
            ports: &[scope],
            all_ports: None,
        },
        source.constant_policy(),
        &functions,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let root = compiled.root(source.root()).unwrap();
    let call = compiled.calls()[&root.use_id].call_contract();
    assert_eq!(call.function_id().as_str(), "builtin.scalar/bitnot/v1");
    assert_eq!(call.logical_argument_count(), 1);
    assert_eq!(
        call.selected().argument_types.as_slice(),
        &[FunctionArgumentType::Value(ty.clone())]
    );
    assert_eq!(call.selected().result_type, FunctionResultType::Scalar(ty));

    let input = novarocks_types::largeint::array_from_i128(&[
        Some(i64::MIN as i128),
        Some(-1),
        Some(0),
        Some(i64::MAX as i128),
        Some(i128::MIN),
        Some(i128::MAX),
        Some((1_i128 << 100) + 5),
        None,
    ])
    .unwrap();
    let data = RecordBatch::try_new(schema, vec![Arc::new(input)]).unwrap();
    let mut frame = CompiledExpressionInstance::try_new(compiled, source.root(), &Control).unwrap();
    let selection = Selection::all(data.num_rows());
    let out = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &data,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let expected = novarocks_types::largeint::array_from_i128(&[
        Some(9_223_372_036_854_775_807),
        Some(0),
        Some(-1),
        Some(-9_223_372_036_854_775_808),
        Some(i128::MAX),
        Some(i128::MIN),
        Some(-1_267_650_600_228_229_401_496_703_205_382_i128),
        None,
    ])
    .unwrap();
    assert!(out.errors().is_empty());
    assert_eq!(out.values().to_data(), expected.to_data());
}

#[test]
fn public_sql_bitnot_reports_the_retired_largeint_target() {
    let fields = [Field::new("k", DataType::Int64, false)];
    for sql in [
        "SELECT bitnot(CAST(k AS LARGEINT)) AS original_inverted FROM fixture.source",
        "SELECT bitnot(CAST('-170141183460469231731687303715884105728' AS LARGEINT))",
        "SELECT bitnot(CAST('170141183460469231731687303715884105727' AS LARGEINT))",
        "SELECT bitnot(CAST(NULL AS LARGEINT))",
    ] {
        let result = try_sql_source_with_declared_fields(sql, &fields);
        let Err(SqlCompileProgressError::Compile(SqlCompileError::Analyze(error))) = result else {
            panic!("expected the original analysis error for {sql}");
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::TypeMismatch, "{sql}");
        assert_eq!(error.code().as_str(), "sql.analyze.type_mismatch", "{sql}");
        assert_eq!(error.message(), "Unknown type: largeint", "{sql}");
        let span = error.span().expect("the retired declaration has a span");
        assert_eq!(&sql[span.start()..span.end()], "LARGEINT", "{sql}");
    }
}

#[test]
fn internal_largeint_conversion_preserves_utf8_and_rejects_equal_width_carriers() {
    use novarocks_functions::builtin::value_conversion::resolve_value_conversion;

    let expected_type = nominal_largeint_type();
    let utf8 = FunctionValueType::new(DataType::Utf8, false);
    let binding = resolve_value_conversion(&utf8, &expected_type, &Control).unwrap();
    assert_eq!(
        binding.overload.as_str(),
        "builtin.scalar/value_domain_conversion/utf8_to_largeint_null_invalid/v1"
    );
    assert_eq!(
        binding.argument_types.as_slice(),
        &[FunctionArgumentType::Value(utf8)]
    );
    assert_eq!(
        binding.result_type,
        FunctionResultType::Scalar(expected_type.clone())
    );

    // The value conversion owner must not authorize a nominal LARGEINT
    // conversion from another equal-width byte carrier.
    for source in [
        FunctionValueType::new(DataType::FixedSizeBinary(16), true),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        assert!(resolve_value_conversion(&source, &expected_type, &Control).is_err());
    }
}
